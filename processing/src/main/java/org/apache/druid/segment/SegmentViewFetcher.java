/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment;

import org.apache.druid.common.asyncresource.AsyncResource;
import org.apache.druid.common.asyncresource.AsyncResources;
import org.apache.druid.segment.incremental.IncrementalIndexRowSelector;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

/**
 * A segment-level capability, requested via {@link Segment#as}, that resolves the <b>views</b> of a segment a query
 * needs and hands them back for the caller to read columns from directly.
 *
 * <p>A <b>view</b> is a self-contained piece of a segment that can be read on its own, as if it were a whole segment: it
 * exposes the same column-reading surface (a {@link QueryableIndex} for a historical / partial segment, an
 * {@link IncrementalIndexRowSelector} for a realtime one) and is processed independently of its sibling views, sharing
 * no query state with them. A single segment resolves to zero or more views depending on how it is physically organized,
 * and the caller need not know which: {@link #acquireViews} returns one view per surviving cluster group for a clustered
 * base table, a single matching aggregate-projection view when one satisfies the query, or the plain base table as one
 * view otherwise. Together the returned views cover every row the query could match in the segment, so reading all of
 * them is equivalent to reading the whole segment for that query. Callers that do not know the spec their cursors
 * will use, such as segment walkers, pass null, and get views that can answer any spec: every cluster group, or else
 * the base table. Then the first cluster group of a segment with aggregate projections serves them; see
 * {@link ClusterGroupView.ProjectionRole}.
 *
 * <p>This is for callers that read columns directly off a {@link QueryableIndex} / {@link IncrementalIndexRowSelector}
 * rather than through a {@link CursorHolder}; it mirrors what {@link CursorFactory#makeCursorHolderAsync} does
 * internally, but hands the resident views back instead of building a cursor over them. Resolution is metadata-only;
 * only the views that survive the query's filters are downloaded.
 *
 * <p>Each view is returned as an {@link AcquiredView}: an {@link AsyncResource} of a resident-view {@link Segment}
 * paired with the on-demand {@link AcquiredView#fetchedBytes() byte volume} its fetch planned to pull. The resource
 * becomes ready once the view's columns are resident, {@link AsyncResource#get()} yields a {@link Segment} whose
 * {@link Segment#as} exposes the concrete view ({@link QueryableIndex} for historical/partial segments,
 * {@link IncrementalIndexRowSelector} for realtime segments) and, for a per-cluster-group view, a
 * {@link ClusterGroupView} describing the clustering columns it was scoped by; and {@link AsyncResource#close()}
 * releases any cache-layer hold taken to keep those columns resident. Callers must <b>not</b> close the view itself
 * (it shares the underlying segment's file mapper / in-memory facts); closing the {@link AsyncResource} is the only
 * required cleanup.
 */
public interface SegmentViewFetcher
{
  /**
   * The resident views needed to answer {@code spec} by reading columns directly, one per surviving cluster group / the
   * matching aggregate projection / the plain base table, or that can answer any spec if it is null. Group pruning and
   * view resolution are eager and metadata-only, so the returned list's size is exactly the number of views, and each
   * {@link AcquiredView}'s {@link AsyncResource} has already had its on-demand download kicked off (a no-op for an
   * already-resident view), with the planned download volume for that view carried alongside as
   * {@link AcquiredView#fetchedBytes()}. A caller that just needs to wait for every view to become resident can collect
   * the resources via {@link AsyncResources#collect}.
   * <p>
   * All-or-nothing: if constructing any view fails, the views created so far are closed before the failure propagates,
   * so the caller receives either a fully-live list or an exception with nothing leaked. See {@link SegmentViewFetcher}
   * for the per-view lifecycle contract once the list is returned.
   */
  List<AcquiredView> acquireViews(@Nullable CursorBuildSpec spec);

  /**
   * Returns a fetcher for a segment that wraps the segment whose fetcher is {@code delegate}. The returned fetcher
   * rewrites each non-null spec using {@code specFn}, the same way the wrapper rewrites specs before building cursors,
   * and wraps each view using {@code viewFn}, so views keep the wrapper's behavior. Wrapped views generally hide
   * {@link QueryableIndex} and {@link IncrementalIndexRowSelector}, since reading their columns directly would skip
   * what the wrapper does.
   */
  static SegmentViewFetcher wrap(
      SegmentViewFetcher delegate,
      UnaryOperator<CursorBuildSpec> specFn,
      UnaryOperator<Segment> viewFn
  )
  {
    return spec -> {
      final List<AcquiredView> views = delegate.acquireViews(spec == null ? null : specFn.apply(spec));
      final List<AcquiredView> retVal = new ArrayList<>(views.size());
      try {
        for (AcquiredView view : views) {
          retVal.add(new AcquiredView(AsyncResources.transform(view.view(), viewFn::apply), view.fetchedBytes()));
        }
        return retVal;
      }
      catch (Throwable t) {
        // Wrapped views own their sources; the rest are still unwrapped.
        final List<AsyncResource<Segment>> toClose = new ArrayList<>(views.size());
        retVal.forEach(view -> toClose.add(view.view()));
        views.subList(retVal.size(), views.size()).forEach(view -> toClose.add(view.view()));
        throw CloseableUtils.closeAndWrapInCatch(t, CloseableUtils.forIterable(toClose));
      }
    };
  }
}
