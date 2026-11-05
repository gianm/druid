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

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.common.asyncresource.AsyncResource;
import org.apache.druid.common.asyncresource.AsyncResources;
import org.apache.druid.common.asyncresource.SettableAsyncResource;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.Order;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.file.PartialSegmentFileMapperV10;
import org.apache.druid.segment.projections.ClusterGroupQueryPlan;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.segment.projections.QueryableProjection;
import org.apache.druid.segment.projections.TableClusterGroupSpec;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Partial-aware {@link CursorFactory} for {@link PartialQueryableIndex}.
 * <p>
 * <b>Sync vs async contract.</b> {@link #makeCursorHolder} requires the segment to already be fully downloaded,
 * intended for callers that acquired the segment via the eager (download-everything-up-front) path, so by the time
 * they ask for a cursor every internal file is on disk. If anything is missing it throws
 * {@link DruidException#defensive} so that we never trigger downloads on the sync path, since processing threads must
 * not block on deep-storage I/O. {@link #makeCursorHolderAsync} is the only path that performs downloads on demand;
 * callers acknowledge that by opting into the async variant when they acquire a partial segment.
 * <p>
 * <b>Async download granularity.</b> {@link PartialQueryableIndex#planCursorPrefetch} resolves what the spec needs.
 * When the segment's metadata records per-column file lists
 * ({@link org.apache.druid.segment.file.SegmentFileMetadata#getColumnFiles()}), the plan covers the required columns'
 * complete internal-file sets up front (including serde sub-files such as nested-column field files) as coalesced
 * range reads; for older segments without file lists it degrades to whole-bundle granularity, losing column-level
 * pruning within the bundle but keeping the matched-bundle pruning and covering serde sub-files that can't be
 * enumerated without the lists. Either way the plan's runs are coalesced up to the configured gap tolerance and split
 * at the configured run-size cap (see {@link PartialSegmentFileMapperV10#planParallelFetch}), and the cursor holder
 * constructed afterward sees the already-materialized holders via the same memoized suppliers, so no further
 * downloads happen at cursor-read time.
 * <p>
 * If a projection matches, the required columns are looked up against the projection's row selector and its rewritten
 * {@link CursorBuildSpec} (which carries physical columns in the projection's namespace). When
 * {@link CursorBuildSpec#getPhysicalColumns()} is {@code null}, every column on the chosen row selector is pre-fetched
 * as required by the contract of {@link CursorBuildSpec}.
 * <p>
 * <b>Parallelism.</b> Every planned range read is its own task on the download executor, so a bundle's independent
 * deep-storage requests proceed concurrently rather than paying their round trips sequentially. Once every run is
 * resident, the ready callback materializes each bundle's required columns and the cursor holder is constructed.
 * <p>
 * <b>Restricted serving.</b> When constructed with a {@code servableBundles} predicate (see
 * {@link org.apache.druid.segment.loading.AcquireMode#PARTIAL_RULE_HELD}), both paths plan only over the bundles it
 * accepts, and a spec that nothing servable can answer gets an {@link EmptyCursorHolder}. Accepted bundles that are not
 * resident are still downloaded by the async path as usual.
 */
public class PartialQueryableIndexCursorFactory implements CursorFactory
{
  private final PartialQueryableIndex index;
  private final QueryableIndexCursorFactory delegate;
  private final PartialBundleAcquirer bundleAcquirer;
  // when non-null, only the bundles this accepts are served, as if they were the whole segment; see
  // PartialQueryableIndex#planCursorPrefetch(CursorBuildSpec, Predicate)
  @Nullable
  private final Predicate<String> servableBundles;

  public PartialQueryableIndexCursorFactory(
      PartialQueryableIndex index,
      TimeBoundaryInspector timeBoundaryInspector,
      PartialBundleAcquirer bundleAcquirer
  )
  {
    this(index, timeBoundaryInspector, bundleAcquirer, null);
  }

  public PartialQueryableIndexCursorFactory(
      PartialQueryableIndex index,
      TimeBoundaryInspector timeBoundaryInspector,
      PartialBundleAcquirer bundleAcquirer,
      @Nullable Predicate<String> servableBundles
  )
  {
    this(index, new QueryableIndexCursorFactory(index, timeBoundaryInspector), bundleAcquirer, servableBundles);
  }

  private PartialQueryableIndexCursorFactory(
      PartialQueryableIndex index,
      QueryableIndexCursorFactory delegate,
      PartialBundleAcquirer bundleAcquirer,
      @Nullable Predicate<String> servableBundles
  )
  {
    this.index = index;
    this.delegate = delegate;
    this.bundleAcquirer = bundleAcquirer;
    this.servableBundles = servableBundles;
  }

  /**
   * Returns the views of {@code segment}, a segment over this factory's index, that are needed to answer {@code spec},
   * or any spec if it is null; see {@link SegmentViewFetcher}. Each view downloads the columns of its own rows that
   * {@code spec} needs, or all of them if it is null, unless the whole segment is already downloaded. When restricted
   * to {@code servableBundles}, only servable projections and cluster groups have views.
   */
  public List<AcquiredView> acquireViews(Segment segment, @Nullable CursorBuildSpec spec)
  {
    final QueryableIndexCursorFactory.ViewPlan plan =
        servableBundles == null ? delegate.planViews(spec) : planServableViews(spec);
    final CursorBuildSpec prefetchSpec = spec == null ? CursorBuildSpec.FULL_SCAN : spec;
    // Restricted factories always plan downloads, since the base table bundle that a resident view would hold may not
    // be servable.
    if (servableBundles == null && plan.projection() == null && index.isFullyDownloaded()) {
      final List<AcquiredView> residentViews = acquireResidentViews(segment, plan);
      if (residentViews != null) {
        return residentViews;
      }
    }

    if (plan.clusterGroups() == null) {
      // A matched projection or the base table. planCursorPrefetch matches projections the same way.
      final Segment view =
          plan.projection() != null
          ? new AggregateProjectionSegment(segment, QueryableIndex.class, plan.projection().getRowSelector())
          : new ReferenceCountedSegmentProvider.UnmanagedReference(segment);
      return List.of(prefetchView(index.planCursorPrefetch(prefetchSpec, servableBundles).bundles(), view));
    }

    final TimeBoundaryInspector timeBoundaryInspector = segment.as(TimeBoundaryInspector.class);
    final List<AcquiredView> retVal = new ArrayList<>(plan.clusterGroups().size());
    try {
      for (ClusterGroupView clusterGroup : plan.clusterGroups()) {
        final PartialQueryableIndexCursorFactory groupFactory =
            makeClusterGroupFactory(timeBoundaryInspector, clusterGroup);
        // Plan nothing for an unservable group, which may still have a view, since it serves projections.
        final List<PartialQueryableIndex.PrefetchBundle> groupBundles =
            isServable(clusterGroup.group())
            ? index.planClusterGroupCursorPrefetch(
                prefetchSpec,
                groupFactory.delegate.planClusterGroups(prefetchSpec)
            ).bundles()
            : List.of();
        retVal.add(prefetchView(groupBundles, makeClusterGroupView(segment, clusterGroup, groupFactory)));
      }
      return retVal;
    }
    catch (Throwable t) {
      throw CloseableUtils.closeAndWrapInCatch(t, closeViews(retVal));
    }
  }

  /**
   * Like {@link QueryableIndexCursorFactory#planViews}, but plans only over {@link #servableBundles}, which must be
   * non-null. If {@code spec} is null, the group that serves projections keeps its view even if its own bundle is not
   * servable, since a servable projection may match the spec its cursors use.
   */
  private QueryableIndexCursorFactory.ViewPlan planServableViews(@Nullable CursorBuildSpec spec)
  {
    if (spec == null) {
      final QueryableIndexCursorFactory.ViewPlan plan = delegate.planViews(null);
      if (plan.clusterGroups() == null) {
        return plan;
      }
      final List<ClusterGroupView> clusterGroups = new ArrayList<>(plan.clusterGroups().size());
      for (ClusterGroupView clusterGroup : plan.clusterGroups()) {
        if (clusterGroup.servesProjections() || isServable(clusterGroup.group())) {
          clusterGroups.add(clusterGroup);
        }
      }
      return new QueryableIndexCursorFactory.ViewPlan(null, clusterGroups);
    }

    final PartialQueryableIndex.CursorPrefetchPlan plan = index.planCursorPrefetch(spec, servableBundles);
    if (plan.clusterGroupPlan() != null) {
      return new QueryableIndexCursorFactory.ViewPlan(
          null,
          ClusterGroupView.forSurvivingGroups(index.getClusterGroupSchemas(), plan.clusterGroupPlan().survivingGroups())
      );
    } else {
      return new QueryableIndexCursorFactory.ViewPlan(plan.matchedProjection(), null);
    }
  }

  /**
   * Whether {@code group} is servable, meaning there is no {@link #servableBundles} restriction, or it accepts the
   * group's bundle.
   */
  private boolean isServable(TableClusterGroupSpec group)
  {
    return servableBundles == null
           || servableBundles.test(Projections.getClusterGroupBundleName(group.getClusteringValueIds()));
  }

  /**
   * Returns the views of {@code plan}, which must not match a projection, without planning downloads, since the whole
   * segment is downloaded. Each view holds its bundle until it is closed, so the bundle cannot be evicted while the
   * view is in use. Returns null if a bundle was evicted before its hold was taken.
   */
  @Nullable
  private List<AcquiredView> acquireResidentViews(Segment segment, QueryableIndexCursorFactory.ViewPlan plan)
  {
    final List<AcquiredView> retVal = new ArrayList<>();
    try {
      if (plan.clusterGroups() == null) {
        final Segment view = new ReferenceCountedSegmentProvider.UnmanagedReference(segment);
        retVal.add(heldView(view, Projections.BASE_TABLE_PROJECTION_NAME));
      } else {
        final TimeBoundaryInspector timeBoundaryInspector = segment.as(TimeBoundaryInspector.class);
        for (ClusterGroupView clusterGroup : plan.clusterGroups()) {
          final PartialQueryableIndexCursorFactory groupFactory =
              makeClusterGroupFactory(timeBoundaryInspector, clusterGroup);
          retVal.add(
              heldView(
                  makeClusterGroupView(segment, clusterGroup, groupFactory),
                  Projections.getClusterGroupBundleName(clusterGroup.group().getClusteringValueIds())
              )
          );
        }
      }

      // Check again, now that the holds are taken. If a bundle was evicted first, taking its hold mounted it empty.
      if (index.isFullyDownloaded()) {
        return retVal;
      }
    }
    catch (Throwable t) {
      throw CloseableUtils.closeAndWrapInCatch(t, closeViews(retVal));
    }

    CloseableUtils.closeAndWrapExceptions(closeViews(retVal));
    return null;
  }

  /**
   * Returns a factory that reads only {@code clusterGroup} of this factory's index, restricted to the same
   * {@link #servableBundles}.
   */
  @VisibleForTesting
  PartialQueryableIndexCursorFactory makeClusterGroupFactory(
      TimeBoundaryInspector timeBoundaryInspector,
      ClusterGroupView clusterGroup
  )
  {
    return new PartialQueryableIndexCursorFactory(
        index,
        QueryableIndexCursorFactory.forClusterGroup(
            index,
            timeBoundaryInspector,
            clusterGroup.group(),
            clusterGroup.projectionRole()
        ),
        bundleAcquirer,
        servableBundles
    );
  }

  /**
   * Returns a view of {@code clusterGroup} of {@code segment}, a segment over this factory's index, that reads rows
   * through {@code groupFactory}.
   */
  private ClusterGroupSegment makeClusterGroupView(
      Segment segment,
      ClusterGroupView clusterGroup,
      PartialQueryableIndexCursorFactory groupFactory
  )
  {
    return new ClusterGroupSegment(
        segment,
        clusterGroup,
        groupFactory,
        // Only inspect the group once the whole segment is downloaded, since inspecting must not block on deep
        // storage.
        () -> index.isFullyDownloaded()
              ? new QueryableIndexPhysicalSegmentInspector(
                  index.getClusterGroupQueryableIndex(clusterGroup.group(), true)
              )
              : null,
        QueryableIndex.class,
        () -> index.getClusterGroupQueryableIndex(clusterGroup.group(), true)
    );
  }

  /**
   * Returns {@code view}, which reads only the bundle {@code bundleName}, as a resident view that holds the bundle
   * until it is closed.
   */
  private AcquiredView heldView(Segment view, String bundleName)
  {
    final Closeable hold = bundleAcquirer.acquire(bundleName);
    final SettableAsyncResource<Segment> resource = new SettableAsyncResource<>();
    resource.set(view, hold);
    return new AcquiredView(resource, 0);
  }

  private static Closeable closeViews(List<AcquiredView> views)
  {
    final List<AsyncResource<Segment>> resources = new ArrayList<>(views.size());
    views.forEach(view -> resources.add(view.view()));
    return CloseableUtils.forIterable(resources);
  }

  @Override
  public CursorHolder makeCursorHolder(CursorBuildSpec spec)
  {
    // refuse to download here so we never accidentally block a processing thread
    if (!index.isFullyDownloaded()) {
      throw DruidException.defensive(
          "Sync makeCursorHolder requires the segment to be fully downloaded; use makeCursorHolderAsync for "
          + "on-demand loading, or acquire the segment via the eager path so all files are loaded up front."
      );
    }
    if (servableBundles == null) {
      return delegate.makeCursorHolder(spec);
    }
    // restricted: dispatch from the restricted plan rather than letting the delegate match over the whole segment
    final PartialQueryableIndex.CursorPrefetchPlan plan = planPrefetch(spec);
    if (plan == null || plan.bundles().isEmpty()) {
      return EmptyCursorHolder.forSpec(spec, delegate);
    }
    if (plan.clusterGroupPlan() != null) {
      return delegate.makeClusteredCursorHolder(spec, plan.clusterGroupPlan());
    }
    return delegate.makeCursorHolderForProjection(spec, plan.matchedProjection());
  }

  @Override
  public AsyncCursorHolder makeCursorHolderAsync(CursorBuildSpec spec)
  {
    // The index resolves what the spec needs (projection match / surviving cluster groups / base table, required
    // columns, planned range reads); this factory purely orchestrates the downloads and cursor wiring.
    final PartialQueryableIndex.CursorPrefetchPlan plan = planPrefetch(spec);
    if (plan == null || plan.bundles().isEmpty()) {
      // This factory reads no rows for the spec: another group serves its projection, the filter rules out every
      // cluster group, or nothing servable can answer it. No bundle to acquire, nothing to download.
      return AsyncCursorHolder.completed(EmptyCursorHolder.forSpec(spec, delegate));
    }
    if (plan.clusterGroupPlan() != null) {
      if (!delegate.isClusterGroup() && index.getClusterGroupSchemas().size() > 1) {
        // Fail before downloading anything, since makeClusteredCursorHolder would refuse anyway.
        throw DruidException.defensive(
            "Cannot build a cursor over a segment with [%d] cluster groups; split it into cluster groups first",
            index.getClusterGroupSchemas().size()
        );
      }
      // reuse the plan's cluster resolution
      return buildAsyncCursorHolder(
          plan.bundles(),
          () -> delegate.makeClusteredCursorHolder(spec, plan.clusterGroupPlan())
      );
    }
    return buildAsyncCursorHolder(
        plan.bundles(),
        () -> delegate.makeCursorHolderForProjection(spec, plan.matchedProjection())
    );
  }

  /**
   * Returns the downloads that the rows of this factory need to answer {@code spec}, or null if this factory is for a
   * cluster group that reads no rows for {@code spec}, because another group serves the projection it matches, or
   * because its own bundle is not servable.
   */
  @Nullable
  private PartialQueryableIndex.CursorPrefetchPlan planPrefetch(CursorBuildSpec spec)
  {
    if (delegate.isClusterGroup()) {
      final QueryableProjection<QueryableIndex> projection;
      if (delegate.getProjectionRole() == ClusterGroupView.ProjectionRole.IGNORES) {
        projection = null;
      } else if (servableBundles == null) {
        projection = delegate.matchProjection(spec);
      } else {
        projection = index.getServableProjection(spec, servableBundles);
      }

      if (projection == null) {
        final ClusterGroupQueryPlan groupPlan = delegate.planClusterGroups(spec);
        for (TableClusterGroupSpec group : groupPlan.survivingGroups()) {
          if (!isServable(group)) {
            return null;
          }
        }
        return index.planClusterGroupCursorPrefetch(spec, groupPlan);
      } else if (delegate.getProjectionRole() == ClusterGroupView.ProjectionRole.DEFERS) {
        // Another group of this index serves the projection, which spans every group.
        return null;
      }
    }
    return index.planCursorPrefetch(spec, servableBundles);
  }

  /**
   * Generalized async-holder build over one or more bundles. Each {@link PartialQueryableIndex.PrefetchBundle}
   * mounts its cache-layer bundle (its own {@link BundleHoldRelease}, since eviction is per-bundle) and submits one
   * download task per planned range read. Once every run of every bundle is resident, {@link #produceHolder} runs as
   * its own download-executor task and materializes each bundle's required columns and builds the cursor holder, all
   * inside every bundle's hold handshake (deserialization and holder construction both touch the container mmaps, see
   * {@link #buildHolderUnderHolds}); the produced holder takes ownership of {@code inner} plus every bundle hold.
   */
  private AsyncCursorHolder buildAsyncCursorHolder(
      List<PartialQueryableIndex.PrefetchBundle> bundles,
      Supplier<CursorHolder> innerBuilder
  )
  {
    final List<BundleHoldRelease> holdReleases = new ArrayList<>(bundles.size());
    try {
      final AsyncResource<List<String>> downloaded = startDownloads(bundles, holdReleases).downloaded();

      // Canceler runs if the awaiter closes this holder before it's ready (e.g. query cancel/timeout). Close the
      // collected resource to cancel every run download that hasn't begun its deep-storage read yet (queued tasks
      // are skipped; tasks parked on the download executor's permit are interrupted out of the interruptible wait
      // before doing any I/O), then request the hold release on every bundle through the handshake. Once the holder is
      // produced and handed to set(), ownership transfers to the awaiter, which drains it via close() (cancel) or
      // release() (success); each bundle's once-guard keeps its hold release safe across all of these paths.
      final AsyncCursorHolder asyncHolder = new AsyncCursorHolder(() -> {
        CloseableUtils.closeAndSuppressExceptions(downloaded, ignored -> {});
        holdReleases.forEach(BundleHoldRelease::requestRelease);
      });
      // This callback can run on ANY thread: the download-executor thread that completed the last run, or, whenever
      // every run finished before registration (always true when there was nothing to download, since an empty
      // collect completes in its constructor), synchronously on THIS thread, because addReadyCallback on an
      // already-ready resource fires inline. So do no real work here: propagate failure/cancellation inline, and hop
      // the success path to the download executor, where the body deserializes columns and may even hit deep storage,
      // neither of which may run on a processing thread.
      downloaded.addReadyCallback(() -> {
        try {
          // ready, so never blocks; throws the run-download failure (or cancellation) if any. Failing here rather
          // than in the continuation keeps a canceled build from queueing a pointless continuation task.
          downloaded.get();
        }
        catch (Throwable t) {
          failHolder(holdReleases, asyncHolder, t);
          return;
        }
        try {
          bundleAcquirer.submitDownload(() -> {
            produceHolder(downloaded, holdReleases, bundles, innerBuilder, asyncHolder);
            return "produce";
          });
        }
        catch (Throwable t) {
          // the continuation couldn't even be submitted (e.g. executor shutdown rejection)
          failHolder(holdReleases, asyncHolder, t);
        }
      });
      return asyncHolder;
    }
    catch (Throwable t) {
      // Failure while submitting downloads / wiring up the holder (e.g. submitDownload shutdown rejection). A run
      // download submitted before the failure may already be in flight, so release every bundle hold through the
      // handshake (requestRelease defers to the last in-flight body) rather than dropping it directly mid-mapFile().
      throw CloseableUtils.closeAndWrapInCatch(t, () -> holdReleases.forEach(BundleHoldRelease::requestRelease));
    }
  }

  /**
   * Acquires a hold on each of {@code bundles}, adding its release to {@code holdReleases}, and submits one download
   * task per planned range read. On failure, the caller must release the holds already in {@code holdReleases}.
   */
  private Downloads startDownloads(
      List<PartialQueryableIndex.PrefetchBundle> bundles,
      List<BundleHoldRelease> holdReleases
  )
  {
    final List<AsyncResource<String>> runDownloads = new ArrayList<>();
    long bytes = 0;
    for (PartialQueryableIndex.PrefetchBundle bundle : bundles) {
      final BundleHoldRelease holdRelease = new BundleHoldRelease(bundleAcquirer.acquire(bundle.bundleName()));
      holdReleases.add(holdRelease);
      // Plan the bundle's range reads only now, under its freshly-acquired hold
      for (PartialSegmentFileMapperV10.PlannedFetch fetch : bundle.planFetches()) {
        runDownloads.add(submitRunFetch(bundle.bundleName(), fetch, holdRelease));
        bytes += fetch.run().length();
      }
    }
    return new Downloads(AsyncResources.collect(runDownloads), bytes);
  }

  /**
   * Starts downloading {@code bundles} for a view from {@link #acquireViews}. The view's resource yields {@code view}
   * once every download is done, and holds the bundles until it is closed.
   */
  private AcquiredView prefetchView(List<PartialQueryableIndex.PrefetchBundle> bundles, Segment view)
  {
    final List<BundleHoldRelease> holdReleases = new ArrayList<>(bundles.size());
    try {
      final Downloads downloads = startDownloads(bundles, holdReleases);
      final AsyncResource<Segment> resource = new AsyncResource<>()
      {
        @Override
        public boolean isReady()
        {
          return downloads.downloaded().isReady();
        }

        @Override
        public void addReadyCallback(Runnable callback)
        {
          downloads.downloaded().addReadyCallback(callback);
        }

        @Override
        public Segment get()
        {
          // throws the download failure, if any
          downloads.downloaded().get();
          return view;
        }

        @Override
        public void close()
        {
          // Cancels downloads that have not started, then releases each hold once its in-flight download is done.
          CloseableUtils.closeAndSuppressExceptions(downloads.downloaded(), ignored -> {});
          holdReleases.forEach(BundleHoldRelease::requestRelease);
        }
      };
      return new AcquiredView(resource, downloads.bytes());
    }
    catch (Throwable t) {
      throw CloseableUtils.closeAndWrapInCatch(t, () -> holdReleases.forEach(BundleHoldRelease::requestRelease));
    }
  }

  /**
   * Continuation of {@link #buildAsyncCursorHolder} once every run download is ready, always running as its own task
   * on the download executor (never on the thread that observed readiness): surface any download failure, materialize
   * columns and build the cursor holder under every hold, and hand the result to the async holder.
   */
  private void produceHolder(
      AsyncResource<List<String>> downloaded,
      List<BundleHoldRelease> holdReleases,
      List<PartialQueryableIndex.PrefetchBundle> bundles,
      Supplier<CursorHolder> innerBuilder,
      AsyncCursorHolder asyncHolder
  )
  {
    final CursorHolder holder;
    try {
      // the ready callback only submits this continuation on success, but the awaiter may close the resource in the
      // window before this task runs; re-checking surfaces that cancellation instead of building a doomed holder
      downloaded.get();
      holder = buildHolderUnderHolds(holdReleases, 0, bundles, innerBuilder);
      if (holder == null) {
        // a concurrent cancel requested a hold release before we could open the handshake on every bundle; the
        // awaiter is gone, so release the rest of the holds and surface the cancellation
        failHolder(
            holdReleases,
            asyncHolder,
            DruidException.defensive("Async cursor holder was closed before the cursor holder could be built")
        );
        return;
      }
    }
    catch (Throwable t) {
      failHolder(holdReleases, asyncHolder, t);
      return;
    }
    if (!asyncHolder.set(holder)) {
      // wrapper was closed (awaiter canceled) while we were producing the holder; close it ourselves so the
      // holder, its inner, and the bundle holds don't leak.
      holder.close();
    }
  }

  /**
   * Shared failure path: request every bundle hold's release and fail the async holder. Releases go through the
   * {@link BundleHoldRelease} handshake (never dropped directly) because failure can be observed while a sibling
   * download is still mid-{@code mapFile()}; setException on an already-closed async holder is a silent no-op, which
   * is the desired behavior when the awaiter canceled.
   */
  private static void failHolder(List<BundleHoldRelease> holdReleases, AsyncCursorHolder asyncHolder, Throwable t)
  {
    holdReleases.forEach(BundleHoldRelease::requestRelease);
    asyncHolder.setException(t);
  }

  @Override
  public RowSignature getRowSignature()
  {
    return delegate.getRowSignature();
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return delegate.getColumnCapabilities(column);
  }

  /**
   * Submit one planned range read as its own download task, running under the bundle-hold handshake (see
   * {@link BundleHoldRelease#runDownloadBody}) so the bundle can't be evicted while the run is streaming into its
   * container (bundle cache entries own their external-resident containers too, so the hold covers runs on external
   * mappers just the same). The returned token (the bundle name) is unused; the task's effect is making the run's
   * files resident in the mapper that planned it.
   */
  private AsyncResource<String> submitRunFetch(
      String bundleName,
      PartialSegmentFileMapperV10.PlannedFetch fetch,
      BundleHoldRelease holdRelease
  )
  {
    return bundleAcquirer.submitDownload(() -> {
      holdRelease.runDownloadBody(() -> {
        try {
          fetch.fetch();
        }
        catch (IOException e) {
          throw DruidException.defensive(e, "Failed to fetch files for bundle[%s]", bundleName);
        }
      });
      return bundleName;
    });
  }

  /**
   * Wrap a {@link CursorHolder} so that closing it also closes {@code bundleHold}, releasing the cache-layer
   * reservation hold on the bundle's containers.
   */
  private static CursorHolder wrapWithBundleRelease(CursorHolder inner, Closeable bundleHold)
  {
    return new CursorHolder()
    {
      @Override
      public Cursor asCursor()
      {
        return inner.asCursor();
      }

      @Override
      public VectorCursor asVectorCursor()
      {
        return inner.asVectorCursor();
      }

      @Override
      public boolean canVectorize()
      {
        return inner.canVectorize();
      }

      @Override
      public boolean isPreAggregated()
      {
        return inner.isPreAggregated();
      }

      @Override
      @Nullable
      public List<AggregatorFactory> getAggregatorsForPreAggregated()
      {
        return inner.getAggregatorsForPreAggregated();
      }

      @Override
      public List<OrderBy> getOrdering()
      {
        return inner.getOrdering();
      }

      @Override
      public Order getTimeOrder()
      {
        return inner.getTimeOrder();
      }

      @Override
      public void close()
      {
        final Closer closer = Closer.create();
        closer.register(bundleHold);
        closer.register(inner);
        CloseableUtils.closeAndWrapExceptions(closer);
      }
    };
  }

  /**
   * Compose every bundle's success-path {@link BundleHoldRelease#releaser} into one {@link Closeable} that the produced
   * cursor holder owns: closing the holder releases all the bundle holds together.
   */
  private static Closeable releasers(List<BundleHoldRelease> holdReleases)
  {
    final Closer closer = Closer.create();
    holdReleases.forEach(holdRelease -> closer.register(holdRelease.releaser()));
    return closer;
  }

  /**
   * Recursively nest {@link BundleHoldRelease#runDownloadBody(Supplier)} across every hold, then (with all handshakes
   * open) materialize each bundle's required columns, build the inner cursor holder, and wrap it with ownership of
   * every hold. Returns {@code null} if any hold had already been requested released (concurrent cancel), the mmaps
   * it guards may be about to unmap, so nothing may be read and no holder can be built.
   */
  @Nullable
  private static CursorHolder buildHolderUnderHolds(
      List<BundleHoldRelease> holdReleases,
      int nextHold,
      List<PartialQueryableIndex.PrefetchBundle> bundles,
      Supplier<CursorHolder> innerBuilder
  )
  {
    if (nextHold < holdReleases.size()) {
      return holdReleases.get(nextHold)
                         .runDownloadBody(() -> buildHolderUnderHolds(holdReleases, nextHold + 1, bundles, innerBuilder));
    }
    for (PartialQueryableIndex.PrefetchBundle bundle : bundles) {
      for (String column : bundle.requiredColumns()) {
        bundle.rowSelector().getColumnHolder(column);
      }
    }
    final CursorHolder inner = innerBuilder.get();
    // The produced holder takes ownership of inner and every bundle hold; closing it releases all of them.
    return wrapWithBundleRelease(inner, releasers(holdReleases));
  }

  /**
   * Owns a bundle hold's release for one {@link #makeCursorHolderAsync} build, guarding the file mapper's
   * evict-vs-mapFile contract: releasing the hold makes the bundle entry eligible for eviction, which unmaps the
   * bundle's containers, so a column download still inside {@code mapFile()} would then read (and slice a ByteBuffer
   * from) an unmapped region resulting in a JVM SIGSEGV. The hold is released exactly once (CAS-guarded) and only once
   * a release has been {@link #requestRelease() requested} (the awaiter canceled, or a download failed) AND no
   * download body is executing, so an in-flight {@code mapFile()} always finishes under the hold. On the success path
   * the produced cursor holder takes ownership of the hold via {@link #releaser()} and the request/in-flight handshake
   * is unused.
   */
  private static final class BundleHoldRelease
  {
    private final Closeable releaseHoldOnce;
    private final AtomicInteger bodiesInFlight = new AtomicInteger(0);
    private final AtomicBoolean releaseRequested = new AtomicBoolean(false);

    BundleHoldRelease(Closeable bundleHold)
    {
      // Release the bundle hold at most once: the canceler, the success-path holder close, the failure callback, and
      // the last download body can all race to release it.
      final AtomicBoolean released = new AtomicBoolean(false);
      this.releaseHoldOnce = () -> {
        if (released.compareAndSet(false, true)) {
          bundleHold.close();
        }
      };
    }

    /**
     * Run one mmap-touching body under the in-flight count. {@code incrementAndGet}-before-read pairs with
     * {@code releaseRequested.set(true)}-before-read in {@link #requestRelease()}: whichever thread observes the
     * other's write takes responsibility for the hold release, so it happens exactly once and never while a
     * {@code mapFile()} is in flight. A body that observes a release request before starting is skipped entirely,
     * returning {@code null} (its containers are about to be released and may be evicted), so the body itself must
     * never return {@code null}; the last body to finish after a release request releases the hold.
     */
    @Nullable
    <T> T runDownloadBody(Supplier<T> read)
    {
      bodiesInFlight.incrementAndGet();
      try {
        if (!releaseRequested.get()) {
          return read.get();
        }
        return null;
      }
      finally {
        if (bodiesInFlight.decrementAndGet() == 0 && releaseRequested.get()) {
          CloseableUtils.closeAndSuppressExceptions(releaseHoldOnce, ignored -> {});
        }
      }
    }

    /**
     * {@link #runDownloadBody(Supplier)} for bodies with no result; the caller can't distinguish ran-from-skipped.
     */
    void runDownloadBody(Runnable read)
    {
      runDownloadBody(() -> {
        read.run();
        return Boolean.TRUE;
      });
    }

    /**
     * Request the hold release because the awaiter cancelled or a download failed. Releases now only if no download
     * body is executing; otherwise the last body out (see {@link #runDownloadBody}) releases it once its
     * {@code mapFile()} has finished. Idempotent.
     */
    void requestRelease()
    {
      releaseRequested.set(true);
      if (bodiesInFlight.get() == 0) {
        CloseableUtils.closeAndSuppressExceptions(releaseHoldOnce, ignored -> {});
      }
    }

    /**
     * The raw, idempotent bundle-hold close. Handed to the produced cursor holder on the success path so that closing
     * the holder releases the hold; it bypasses the in-flight handshake, which is safe there because the holder is only
     * built after every download has completed (nothing is mid-{@code mapFile()}). Cancel/failure paths must release
     * through {@link #requestRelease()} instead; the once-guard keeps the hold release exactly-once across whichever
     * path fires.
     */
    Closeable releaser()
    {
      return releaseHoldOnce;
    }
  }

  /**
   * Downloads started by {@link #startDownloads}, and the total bytes they read.
   */
  private record Downloads(AsyncResource<List<String>> downloaded, long bytes)
  {
  }
}
