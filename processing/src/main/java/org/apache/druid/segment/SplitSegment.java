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

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.filter.SegmentPruner;
import org.apache.druid.segment.loading.AcquireMode;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Segment mapped by a {@link SegmentMapFunction}, along with each of its cluster groups mapped by the same function, so
 * segment walkers can process each group as a separate segment; see {@link ClusterGroupSegment}. Otherwise behaves like
 * the mapped segment. Closing it also closes the mapped groups.
 */
public class SplitSegment extends WrappedSegment
{
  private final List<MappedClusterGroup> clusterGroups;
  private final boolean servesProjections;
  private final Closer clusterGroupCloser;

  private SplitSegment(
      Segment delegate,
      List<MappedClusterGroup> clusterGroups,
      boolean servesProjections,
      Closer clusterGroupCloser
  )
  {
    super(delegate);
    this.clusterGroups = clusterGroups;
    this.servesProjections = servesProjections;
    this.clusterGroupCloser = clusterGroupCloser;
  }

  /**
   * Returns a function that maps segments using {@code mapFn}, and returns a {@link SplitSegment} for each clustered
   * segment, holding its cluster groups that {@code pruner} does not rule out, also mapped using {@code mapFn}. The
   * group that serves aggregate projections is never ruled out; see {@link ClusterGroupView.ProjectionRole}. Segments
   * must be fully downloaded, such as by acquiring them with {@link AcquireMode#FULL}, since views acquired without a
   * spec need every column.
   * <p>
   * Like any {@link SegmentMapFunction}, the returned function owns its input. It returns empty if {@code mapFn}
   * returns empty for the segment or for any of its groups.
   */
  public static SegmentMapFunction mapFunction(SegmentMapFunction mapFn, @Nullable SegmentPruner pruner)
  {
    return segment -> {
      final SegmentViewFetcher fetcher = segment.map(s -> s.as(SegmentViewFetcher.class)).orElse(null);
      if (fetcher == null) {
        return mapFn.apply(segment);
      }

      final Segment input = segment.get();
      final Closer closer = Closer.create();
      // Whether we must close input on failure, because it has not been passed to mapFn or closed already.
      boolean ownsInput = true;
      try {
        final List<AcquiredView> views = fetcher.acquireViews(null);
        views.forEach(view -> closer.register(view.view()));
        if (views.stream().anyMatch(view -> !view.view().isReady())) {
          throw DruidException.defensive(
              "Cannot split segment[%s], since it is not fully downloaded; acquire it with AcquireMode[%s]",
              input.getId(),
              AcquireMode.FULL
          );
        }

        // There are no views when the segment is clustered and has no cluster groups yet. Then process no groups,
        // rather than the segment as a whole, since a realtime segment may gain groups before its cursor is built.
        final List<MappedClusterGroup> clusterGroups = new ArrayList<>();
        boolean servesProjections = false;
        for (AcquiredView acquiredView : views) {
          final Segment view = acquiredView.view().get();
          final ClusterGroupView clusterGroup = view.as(ClusterGroupView.class);
          if (clusterGroup == null) {
            // Not a clustered segment: its only view is the whole segment.
            closer.close();
            ownsInput = false;
            return mapFn.apply(segment);
          }

          servesProjections |= clusterGroup.servesProjections();
          if (clusterGroup.servesProjections()
              || pruner == null
              || pruner.includeClusterGroups(clusterGroup.toClusterGroupTuples())) {
            final Optional<Segment> mappedGroup = mapFn.apply(Optional.of(view));
            if (mappedGroup.isEmpty()) {
              // Return empty, as mapFn would for the whole segment, so walkers report the segment as missing.
              closer.close();
              ownsInput = false;
              input.close();
              return Optional.empty();
            }
            clusterGroups.add(new MappedClusterGroup(clusterGroup.groupNumber(), closer.register(mappedGroup.get())));
          }
        }

        ownsInput = false;
        final Optional<Segment> mapped = mapFn.apply(segment);
        if (mapped.isEmpty()) {
          closer.close();
          return Optional.empty();
        }
        return Optional.of(new SplitSegment(mapped.get(), clusterGroups, servesProjections, closer));
      }
      catch (Throwable t) {
        // Close input last, since views and mapped groups may read from it.
        final Closer onFailure = Closer.create();
        if (ownsInput) {
          onFailure.register(input);
        }
        onFailure.register(closer);
        throw CloseableUtils.closeAndWrapInCatch(t, onFailure);
      }
    };
  }

  /**
   * Returns the cache identifier for one cluster group of the segment or subsegment whose cache identifier is
   * {@code cacheIdentifier}. The suffix keeps it from colliding with the identifier of the whole segment.
   */
  public static String makeClusterGroupCacheIdentifier(String cacheIdentifier, int groupNumber)
  {
    return cacheIdentifier + "_G" + groupNumber;
  }

  /**
   * Returns the mapped cluster groups to process in place of this segment. Callers must not close them; closing this
   * segment closes them.
   */
  public List<MappedClusterGroup> getClusterGroups()
  {
    return clusterGroups;
  }

  /**
   * Whether the results of each cluster group can be cached on their own. They cannot if the segment has aggregate
   * projections, since a group's results then depend on whether the query matches a projection, which per-segment
   * cache keys do not capture.
   */
  public boolean isClusterGroupCacheable()
  {
    return !servesProjections;
  }

  @Nullable
  @Override
  public <T> T as(@Nonnull Class<T> clazz)
  {
    return delegate.as(clazz);
  }

  @Override
  public String getDebugString()
  {
    return "split->" + delegate.getDebugString();
  }

  @Override
  public void close() throws IOException
  {
    final Closer closer = Closer.create();
    closer.register(delegate);
    closer.register(clusterGroupCloser);
    closer.close();
  }

  /**
   * A cluster group mapped by a {@link SegmentMapFunction}, along with its position among the cluster groups of its
   * segment (see {@link ClusterGroupView#groupNumber()}).
   */
  public record MappedClusterGroup(int groupNumber, Segment segment)
  {
  }
}
