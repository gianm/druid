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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.common.asyncresource.AsyncResource;
import org.apache.druid.common.asyncresource.AsyncResources;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.ClusteredValueGroupsBaseTableProjectionSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.metadata.SegmentAnalyzer;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.file.CountingRangeReader;
import org.apache.druid.segment.file.PartialSegmentFileMapperV10;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.testing.TemporaryFolderExtension;
import org.apache.druid.timeline.SegmentId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Partial (on-demand) download coverage for a clustered base-table V10 segment. Cluster groups are resolved from
 * metadata only ({@code planClusterGroupQuery}); the async cursor factory downloads one bundle ({@code __base$<ids>})
 * per surviving group.
 */
class PartialQueryableIndexCursorFactoryClusteredTest extends PartialQueryableIndexCursorFactoryTestBase
{
  private static final long T0 = DateTimes.of("2025-01-01").getMillis();

  // tenants sort to dictionary ids acme=0, globex=1 → bundles __base$0 (2 rows) and __base$1 (1 row)
  private static final String ACME_BUNDLE = "__base$0";
  private static final String GLOBEX_BUNDLE = "__base$1";

  @RegisterExtension
  public static final TemporaryFolderExtension SHARED_TEMPORARY_FOLDER = TemporaryFolderExtension.classScoped();

  private static File segmentDir;

  // segment whose groups disagree about column shapes; see buildMixedSegment
  private static File mixedSegmentDir;

  @BeforeAll
  static void buildSegment() throws IOException
  {
    final ClusteredValueGroupsBaseTableProjectionSpec clusterSpec =
        ClusteredValueGroupsBaseTableProjectionSpec.builder()
            .columns(
                new StringDimensionSchema("tenant"),
                new StringDimensionSchema("region"),
                new LongDimensionSchema("__time")
            )
            .clusteringColumns("tenant")
            .build();
    final IncrementalIndexSchema schema =
        IncrementalIndexSchema.builder()
                              .withMinTimestamp(T0)
                              .withTimestampSpec(new TimestampSpec("ts", "millis", null))
                              .withQueryGranularity(Granularities.NONE)
                              .withDimensionsSpec(clusterSpec.getDimensionsSpec())
                              .withRollup(false)
                              .withClusterSpec(clusterSpec)
                              .build();
    final File tmpDir = SHARED_TEMPORARY_FOLDER.newFolder("build_" + ThreadLocalRandom.current().nextInt());
    segmentDir = IndexBuilder.create()
                             .useV10()
                             .tmpDir(tmpDir)
                             .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                             .schema(schema)
                             .indexSpec(IndexSpec.builder().withMetadataCompression(CompressionStrategy.NONE).build())
                             // ingest out of clustering order; the writer sorts groups by clustering value
                             .rows(List.of(
                                 row(T0 + 2, "globex", "eu-west-1"),
                                 row(T0, "acme", "us-east-1"),
                                 row(T0 + 1, "acme", "us-west-2")
                             ))
                             .buildMMappedIndexFile();
    mixedSegmentDir = buildMixedSegment();
  }

  /**
   * Builds a segment whose groups disagree about two columns: {@code tags} is multi-valued only in the second group
   * (globex), and the {@code auto} column {@code num} is LONG in the first group (acme) but DOUBLE in the second.
   */
  private static File buildMixedSegment() throws IOException
  {
    final ClusteredValueGroupsBaseTableProjectionSpec clusterSpec =
        ClusteredValueGroupsBaseTableProjectionSpec.builder()
            .columns(
                new StringDimensionSchema("tenant"),
                new LongDimensionSchema("__time"),
                new StringDimensionSchema("tags"),
                AutoTypeColumnSchema.of("num")
            )
            .clusteringColumns("tenant")
            .build();
    final IncrementalIndexSchema schema =
        IncrementalIndexSchema.builder()
                              .withMinTimestamp(T0)
                              .withTimestampSpec(new TimestampSpec("ts", "millis", null))
                              .withQueryGranularity(Granularities.NONE)
                              .withDimensionsSpec(clusterSpec.getDimensionsSpec())
                              .withRollup(false)
                              .withClusterSpec(clusterSpec)
                              .build();
    final File tmpDir = SHARED_TEMPORARY_FOLDER.newFolder("build_mixed_" + ThreadLocalRandom.current().nextInt());
    return IndexBuilder.create()
                       .useV10()
                       .tmpDir(tmpDir)
                       .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                       .schema(schema)
                       .indexSpec(IndexSpec.builder().withMetadataCompression(CompressionStrategy.NONE).build())
                       .rows(List.of(
                           mixedRow(T0, "acme", "a", 1L),
                           mixedRow(T0 + 1, "globex", List.of("b", "c"), 2.5),
                           mixedRow(T0 + 2, "initech", "d", 3L)
                       ))
                       .buildMMappedIndexFile();
  }

  @Test
  void testWholeSegmentFactoryRefusesEvenIfFilterLeavesOneGroup() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "filter_acme")) {
      final PartialQueryableIndexCursorFactory factory = factory(opened.index());
      final CursorBuildSpec spec = CursorBuildSpec.builder()
                                                  .setFilter(new EqualityFilter("tenant", ColumnType.STRING, "acme", null))
                                                  .build();

      // A cursor cannot span groups, so the whole-segment factory refuses, before downloading anything, even though
      // only the acme group survives the filter. Callers read the acme group through its view instead.
      Assertions.assertThrows(DruidException.class, () -> factory.makeCursorHolderAsync(spec));
      Assertions.assertTrue(opened.mapper().getDownloadedFiles().isEmpty());
    }
  }

  @Test
  void testFilterMatchingNoGroupDownloadsNothing() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "filter_none")) {
      final PartialQueryableIndexCursorFactory factory = factory(opened.index());
      final CursorBuildSpec spec = CursorBuildSpec.builder()
                                                  .setFilter(new EqualityFilter("tenant", ColumnType.STRING, "nobody", null))
                                                  .build();

      try (AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(spec);
           CursorHolder holder = asyncHolder.release()) {
        Assertions.assertTrue(scanTenantRegion(holder).isEmpty(), "no group should survive");
        Assertions.assertTrue(
            opened.mapper().getDownloadedFiles().isEmpty(),
            "no group bundle should be downloaded when nothing survives; got: " + opened.mapper().getDownloadedFiles()
        );
      }
    }
  }

  @Test
  void testFullScanReadsEachGroup() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "full_scan")) {
      // A cursor cannot span groups, so the whole-segment factory refuses, before downloading anything.
      Assertions.assertThrows(
          DruidException.class,
          () -> factory(opened.index()).makeCursorHolderAsync(CursorBuildSpec.FULL_SCAN)
      );
      Assertions.assertTrue(opened.mapper().getDownloadedFiles().isEmpty());

      // Groups come in clustering (dictionary id) order: acme then globex, clustering constant injected.
      final List<List<String>> rows = new ArrayList<>();
      for (CursorFactory groupFactory : groupFactories(opened.index(), noOpAcquirer(directExec()))) {
        try (AsyncCursorHolder asyncHolder = groupFactory.makeCursorHolderAsync(CursorBuildSpec.FULL_SCAN);
             CursorHolder holder = asyncHolder.release()) {
          rows.addAll(scanTenantRegion(holder));
        }
      }
      Assertions.assertEquals(
          List.of(
              List.of("acme", "us-east-1"),
              List.of("acme", "us-west-2"),
              List.of("globex", "eu-west-1")
          ),
          rows
      );

      final Set<String> downloaded = opened.mapper().getDownloadedFiles();
      Assertions.assertTrue(downloaded.contains(ACME_BUNDLE + "/region"), "got: " + downloaded);
      Assertions.assertTrue(downloaded.contains(GLOBEX_BUNDLE + "/region"), "got: " + downloaded);
    }
  }

  @Test
  void testServableBundlesDropsUnservedGroups() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "servable_acme")) {
      final PartialSegmentFileMapperV10 mapper = opened.mapper();
      final Segment segment = new PartialQueryableIndexSegment(
          opened.index(),
          SegmentId.dummy("test"),
          () -> {},
          noOpAcquirer(directExec()),
          ACME_BUNDLE::equals
      );

      // the unserved globex group is left out entirely, rather than nil-ed
      final List<AcquiredView> views = segment.as(SegmentViewFetcher.class).acquireViews(CursorBuildSpec.FULL_SCAN);
      Assertions.assertEquals(1, views.size());
      final List<List<String>> rows = new ArrayList<>();
      for (AcquiredView view : views) {
        try (AsyncCursorHolder asyncHolder =
                 view.view().get().as(CursorFactory.class).makeCursorHolderAsync(CursorBuildSpec.FULL_SCAN);
             CursorHolder holder = asyncHolder.release()) {
          rows.addAll(scanTenantRegion(holder));
        }
      }
      Assertions.assertEquals(List.of(List.of("acme", "us-east-1"), List.of("acme", "us-west-2")), rows);
      final Set<String> downloaded = mapper.getDownloadedFiles();
      Assertions.assertTrue(
          downloaded.stream().noneMatch(f -> f.startsWith(GLOBEX_BUNDLE + "/")),
          "unserved globex group must not be downloaded; got: " + downloaded
      );
      views.forEach(view -> view.view().close());

      // sync path drops the unserved group too, even once it is resident
      mapper.fetchFiles(mapper.getSegmentFileMetadata().getFiles().keySet());
      final List<AcquiredView> residentViews =
          segment.as(SegmentViewFetcher.class).acquireViews(CursorBuildSpec.FULL_SCAN);
      Assertions.assertEquals(1, residentViews.size());
      final List<List<String>> residentRows = new ArrayList<>();
      for (AcquiredView view : residentViews) {
        try (CursorHolder holder =
                 view.view().get().as(CursorFactory.class).makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
          residentRows.addAll(scanTenantRegion(holder));
        }
      }
      Assertions.assertEquals(List.of(List.of("acme", "us-east-1"), List.of("acme", "us-west-2")), residentRows);
      residentViews.forEach(view -> view.view().close());
    }
  }

  @Test
  void testNoServableGroupsDownloadsNothing() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "servable_none")) {
      final PartialQueryableIndexCursorFactory factory = new PartialQueryableIndexCursorFactory(
          opened.index(),
          QueryableIndexTimeBoundaryInspector.create(opened.index()),
          noOpAcquirer(directExec()),
          bundle -> false
      );
      rangeReader.resetCount();
      try (AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(CursorBuildSpec.FULL_SCAN);
           CursorHolder holder = asyncHolder.release()) {
        Assertions.assertTrue(scanTenantRegion(holder).isEmpty());
      }
      Assertions.assertEquals(0, rangeReader.getReadCount());
      Assertions.assertTrue(opened.mapper().getDownloadedFiles().isEmpty());
    }
  }

  @Test
  void testSyncMakeCursorHolderAfterFullDownload() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "sync_full")) {
      final PartialSegmentFileMapperV10 mapper = opened.mapper();
      final List<CursorFactory> groupFactories = groupFactories(opened.index(), noOpAcquirer(directExec()));

      // Sync path refuses until everything is resident.
      Assertions.assertThrows(
          DruidException.class,
          () -> groupFactories.get(0).makeCursorHolder(CursorBuildSpec.FULL_SCAN)
      );

      // Eager-acquire equivalent: materialize every internal file, then the sync clustered path works via the delegate.
      mapper.fetchFiles(mapper.getSegmentFileMetadata().getFiles().keySet());
      Assertions.assertTrue(mapper.isFullyDownloaded());
      final List<List<String>> rows = new ArrayList<>();
      for (CursorFactory groupFactory : groupFactories) {
        try (CursorHolder holder = groupFactory.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
          rows.addAll(scanTenantRegion(holder));
        }
      }
      Assertions.assertEquals(
          List.of(
              List.of("acme", "us-east-1"),
              List.of("acme", "us-west-2"),
              List.of("globex", "eu-west-1")
          ),
          rows
      );
    }
  }

  @Test
  void testAsyncGroupsDeferDownloadUntilExecutorRuns() throws Exception
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    final CountDownLatch gate = new CountDownLatch(1);
    final ExecutorService rawExec = Execs.singleThreaded("partial-clustered-defer-%d");
    final ListeningExecutorService gatedExec = MoreExecutors.listeningDecorator(rawExec);
    try (IndexAndMapper opened = openIndex(rangeReader, "async_defer")) {
      final List<CursorFactory> groupFactories = groupFactories(opened.index(), noOpAcquirer(gatedExec));
      rangeReader.resetCount();

      // Block the download executor so no group download can start yet.
      @SuppressWarnings("unused")
      ListenableFuture<?> unused = gatedExec.submit(() -> {
        try {
          gate.await();
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      // FULL_SCAN survives both groups → each group's bundle must download before its holder is ready.
      final List<AsyncCursorHolder> asyncHolders = new ArrayList<>();
      for (CursorFactory groupFactory : groupFactories) {
        asyncHolders.add(groupFactory.makeCursorHolderAsync(CursorBuildSpec.FULL_SCAN));
      }
      final CountDownLatch ready = new CountDownLatch(asyncHolders.size());
      for (AsyncCursorHolder asyncHolder : asyncHolders) {
        Assertions.assertFalse(asyncHolder.isReady(), "must wait while the download executor is blocked");
        asyncHolder.addReadyCallback(ready::countDown);
      }
      Assertions.assertEquals(0, rangeReader.getReadCount(), "no group download should have started yet");

      gate.countDown();
      Assertions.assertTrue(ready.await(15, TimeUnit.SECONDS), "ready callbacks must fire once downloads run");

      final List<List<String>> rows = new ArrayList<>();
      for (AsyncCursorHolder asyncHolder : asyncHolders) {
        Assertions.assertTrue(asyncHolder.isReady());
        try (CursorHolder holder = asyncHolder.release()) {
          rows.addAll(scanTenantRegion(holder));
        }
      }
      Assertions.assertEquals(
          List.of(
              List.of("acme", "us-east-1"),
              List.of("acme", "us-west-2"),
              List.of("globex", "eu-west-1")
          ),
          rows
      );
    }
    finally {
      gatedExec.shutdownNow();
      rawExec.shutdownNow();
    }
  }

  @Test
  void testGroupSuccessCloseReleasesOnlyItsBundleHold() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    final CountingBundleAcquirer acquirer = new CountingBundleAcquirer(directExec());
    try (IndexAndMapper opened = openIndex(rangeReader, "release_success")) {
      final List<CursorFactory> groupFactories = groupFactories(opened.index(), acquirer);
      final List<String> bundles = List.of(ACME_BUNDLE, GLOBEX_BUNDLE);

      for (int i = 0; i < groupFactories.size(); i++) {
        final String bundle = bundles.get(i);
        final String otherBundle = bundles.get(1 - i);

        // With the direct executor the download runs inline, so the holder is immediately ready. Each group acquires
        // only its own bundle.
        try (AsyncCursorHolder asyncHolder = groupFactories.get(i).makeCursorHolderAsync(CursorBuildSpec.FULL_SCAN)) {
          Assertions.assertTrue(asyncHolder.isReady());
          Assertions.assertEquals(1, acquirer.acquiredCount(bundle), bundle + " acquired");
          Assertions.assertEquals(i == 0 ? 0 : 1, acquirer.acquiredCount(otherBundle), otherBundle + " acquired");

          final CursorHolder holder = asyncHolder.release();
          // The produced holder owns the group's hold; nothing is released until it closes.
          Assertions.assertEquals(0, acquirer.releasedCount(bundle));

          holder.close();
          Assertions.assertEquals(1, acquirer.releasedCount(bundle), bundle + " hold released on holder close");
        }
      }
    }
  }

  @Test
  void testGroupCancelBeforeReadyReleasesItsBundleHold() throws Exception
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    final CountDownLatch gate = new CountDownLatch(1);
    final ExecutorService rawExec = Execs.singleThreaded("partial-clustered-cancel-%d");
    final ListeningExecutorService gatedExec = MoreExecutors.listeningDecorator(rawExec);
    final CountingBundleAcquirer acquirer = new CountingBundleAcquirer(gatedExec);
    try (IndexAndMapper opened = openIndex(rangeReader, "release_cancel")) {
      final List<CursorFactory> groupFactories = groupFactories(opened.index(), acquirer);

      // Block the download executor so the per-column download bodies stay queued and the holders never become ready.
      @SuppressWarnings("unused")
      ListenableFuture<?> unused = gatedExec.submit(() -> {
        try {
          gate.await();
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      final List<AsyncCursorHolder> asyncHolders = new ArrayList<>();
      for (CursorFactory groupFactory : groupFactories) {
        asyncHolders.add(groupFactory.makeCursorHolderAsync(CursorBuildSpec.FULL_SCAN));
      }
      // Each group's bundle is acquired synchronously during its build, before any download runs.
      Assertions.assertEquals(1, acquirer.acquiredCount(ACME_BUNDLE), "acme group bundle acquired");
      Assertions.assertEquals(1, acquirer.acquiredCount(GLOBEX_BUNDLE), "globex group bundle acquired");
      for (AsyncCursorHolder asyncHolder : asyncHolders) {
        Assertions.assertFalse(asyncHolder.isReady(), "must wait while the download executor is blocked");
      }

      // Cancel before the holders are ready (query cancel/timeout). No download body has run, so the per-bundle
      // handshake releases each hold immediately.
      asyncHolders.get(0).close();
      Assertions.assertEquals(1, acquirer.releasedCount(ACME_BUNDLE), "acme hold released on cancel");
      Assertions.assertEquals(0, acquirer.releasedCount(GLOBEX_BUNDLE), "globex hold still held");
      asyncHolders.get(1).close();
      Assertions.assertEquals(1, acquirer.releasedCount(GLOBEX_BUNDLE), "globex hold released on cancel");
    }
    finally {
      gate.countDown();
      gatedExec.shutdownNow();
      rawExec.shutdownNow();
    }
  }

  @Test
  void testViewsDownloadTheirOwnColumnsAndHoldTheirBundles() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    final CountingBundleAcquirer acquirer = new CountingBundleAcquirer(directExec());
    try (IndexAndMapper opened = openIndex(rangeReader, "views_globex")) {
      final Segment segment =
          new PartialQueryableIndexSegment(opened.index(), SegmentId.dummy("test"), () -> {}, acquirer);
      final CursorBuildSpec spec =
          CursorBuildSpec.builder()
                         .setFilter(new EqualityFilter("tenant", ColumnType.STRING, "globex", null))
                         .build();

      // The filter rules out the acme group, which has nothing to download. The globex group downloads its columns
      // while its view is acquired.
      final List<AcquiredView> views = segment.as(SegmentViewFetcher.class).acquireViews(spec);
      Assertions.assertEquals(1, views.size());
      Assertions.assertTrue(views.get(0).fetchedBytes() > 0);
      Assertions.assertTrue(views.get(0).view().isReady());
      Assertions.assertEquals(0, acquirer.acquiredCount(ACME_BUNDLE));
      Assertions.assertEquals(1, acquirer.acquiredCount(GLOBEX_BUNDLE));
      final Set<String> downloaded = opened.mapper().getDownloadedFiles();
      Assertions.assertTrue(downloaded.contains(GLOBEX_BUNDLE + "/region"), "got: " + downloaded);
      Assertions.assertTrue(downloaded.stream().noneMatch(f -> f.startsWith(ACME_BUNDLE + "/")), "got: " + downloaded);

      // Each view reads its own group.
      final List<List<String>> rows = new ArrayList<>();
      for (AcquiredView view : views) {
        try (AsyncCursorHolder asyncHolder = view.view().get().as(CursorFactory.class).makeCursorHolderAsync(spec);
             CursorHolder holder = asyncHolder.release()) {
          rows.addAll(scanTenantRegion(holder));
        }
      }
      Assertions.assertEquals(List.of(List.of("globex", "eu-west-1")), rows);

      // The cursor holder released its own hold; closing the views releases theirs.
      Assertions.assertEquals(acquirer.acquiredCount(GLOBEX_BUNDLE) - 1, acquirer.releasedCount(GLOBEX_BUNDLE));
      views.forEach(view -> view.view().close());
      Assertions.assertEquals(acquirer.acquiredCount(GLOBEX_BUNDLE), acquirer.releasedCount(GLOBEX_BUNDLE));
    }
  }

  @Test
  void testViewsOfFullyDownloadedSegmentAreResidentAndHoldTheirBundles() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    final CountingBundleAcquirer acquirer = new CountingBundleAcquirer(directExec());
    try (IndexAndMapper opened = openIndex(rangeReader, "views_resident")) {
      final PartialSegmentFileMapperV10 mapper = opened.mapper();
      mapper.fetchFiles(mapper.getSegmentFileMetadata().getFiles().keySet());
      rangeReader.resetCount();
      final Segment segment =
          new PartialQueryableIndexSegment(opened.index(), SegmentId.dummy("test"), () -> {}, acquirer);

      // The segment is fully downloaded, so the views are resident right away, with nothing to download. Each holds
      // its group's bundle, so the bundle stays on disk while the view is in use.
      final List<AcquiredView> views = segment.as(SegmentViewFetcher.class).acquireViews(CursorBuildSpec.FULL_SCAN);
      Assertions.assertEquals(2, views.size());
      for (AcquiredView view : views) {
        Assertions.assertTrue(view.view().isReady());
        Assertions.assertEquals(0, view.fetchedBytes());
      }
      Assertions.assertEquals(1, acquirer.acquiredCount(ACME_BUNDLE));
      Assertions.assertEquals(1, acquirer.acquiredCount(GLOBEX_BUNDLE));

      final List<List<String>> rows = new ArrayList<>();
      for (AcquiredView view : views) {
        final CursorFactory viewFactory = view.view().get().as(CursorFactory.class);
        try (CursorHolder holder = viewFactory.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
          rows.addAll(scanTenantRegion(holder));
        }
      }
      Assertions.assertEquals(
          List.of(
              List.of("acme", "us-east-1"),
              List.of("acme", "us-west-2"),
              List.of("globex", "eu-west-1")
          ),
          rows
      );
      Assertions.assertEquals(0, rangeReader.getReadCount());

      Assertions.assertEquals(0, acquirer.releasedCount(ACME_BUNDLE));
      views.forEach(view -> view.view().close());
      Assertions.assertEquals(1, acquirer.releasedCount(ACME_BUNDLE));
      Assertions.assertEquals(1, acquirer.releasedCount(GLOBEX_BUNDLE));
    }
  }

  @Test
  void testClosingViewBeforeReadyReleasesItsBundleHold() throws Exception
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    final CountDownLatch gate = new CountDownLatch(1);
    final ExecutorService rawExec = Execs.singleThreaded("partial-clustered-view-cancel-%d");
    final ListeningExecutorService gatedExec = MoreExecutors.listeningDecorator(rawExec);
    final CountingBundleAcquirer acquirer = new CountingBundleAcquirer(gatedExec);
    try (IndexAndMapper opened = openIndex(rangeReader, "views_cancel")) {
      final Segment segment =
          new PartialQueryableIndexSegment(opened.index(), SegmentId.dummy("test"), () -> {}, acquirer);

      // Block the download executor so the views never become resident.
      @SuppressWarnings("unused")
      ListenableFuture<?> unused = gatedExec.submit(() -> {
        try {
          gate.await();
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      final List<AcquiredView> views = segment.as(SegmentViewFetcher.class).acquireViews(CursorBuildSpec.FULL_SCAN);
      Assertions.assertEquals(2, views.size());
      Assertions.assertEquals(1, acquirer.acquiredCount(ACME_BUNDLE), "acme group bundle acquired");
      Assertions.assertEquals(1, acquirer.acquiredCount(GLOBEX_BUNDLE), "globex group bundle acquired");
      for (AcquiredView view : views) {
        Assertions.assertFalse(view.view().isReady(), "must wait while the download executor is blocked");
      }

      // No download has run, so closing a view releases its hold immediately.
      views.get(0).view().close();
      Assertions.assertEquals(1, acquirer.releasedCount(ACME_BUNDLE), "acme hold released on close");
      Assertions.assertEquals(0, acquirer.releasedCount(GLOBEX_BUNDLE), "globex hold still held");
      views.get(1).view().close();
      Assertions.assertEquals(1, acquirer.releasedCount(GLOBEX_BUNDLE), "globex hold released on close");
    }
    finally {
      gate.countDown();
      gatedExec.shutdownNow();
      rawExec.shutdownNow();
    }
  }

  @Test
  void testSplitRefusesSegmentThatIsNotFullyDownloaded() throws Exception
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    final CountDownLatch gate = new CountDownLatch(1);
    final ExecutorService rawExec = Execs.singleThreaded("partial-clustered-split-%d");
    final ListeningExecutorService gatedExec = MoreExecutors.listeningDecorator(rawExec);
    final CountingBundleAcquirer acquirer = new CountingBundleAcquirer(gatedExec);
    try (IndexAndMapper opened = openIndex(rangeReader, "split_not_downloaded")) {
      final Segment segment =
          new PartialQueryableIndexSegment(opened.index(), SegmentId.dummy("test"), () -> {}, acquirer);

      // Block the download executor so the views never become resident.
      @SuppressWarnings("unused")
      ListenableFuture<?> unused = gatedExec.submit(() -> {
        try {
          gate.await();
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      final DruidException e = Assertions.assertThrows(
          DruidException.class,
          () -> SplitSegment.mapFunction(SegmentMapFunction.IDENTITY, null).apply(Optional.of(segment))
      );
      Assertions.assertTrue(e.getMessage().contains("not fully downloaded"), e.getMessage());
      Assertions.assertEquals(1, acquirer.releasedCount(ACME_BUNDLE), "acme hold released");
      Assertions.assertEquals(1, acquirer.releasedCount(GLOBEX_BUNDLE), "globex hold released");
    }
    finally {
      gate.countDown();
      gatedExec.shutdownNow();
      rawExec.shutdownNow();
    }
  }

  @Test
  void testClusteredColumnCapabilitiesAreMetadataOnly() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "capabilities")) {
      final PartialQueryableIndex index = opened.index();
      // clustering column: type comes from the clustered summary's typed signature
      Assertions.assertEquals(ValueType.STRING, index.getColumnCapabilities("tenant").getType());
      // per-group data columns: type comes from the groups' ColumnDescriptors, never a group sub-index download
      Assertions.assertEquals(ValueType.STRING, index.getColumnCapabilities("region").getType());
      Assertions.assertEquals(ValueType.LONG, index.getColumnCapabilities("__time").getType());
      // an unknown column resolves to null without touching the file mapper
      Assertions.assertNull(index.getColumnCapabilities("nonexistent"));

      Assertions.assertTrue(
          opened.mapper().getDownloadedFiles().isEmpty(),
          "column capabilities must be answered from metadata with no download; got: "
          + opened.mapper().getDownloadedFiles()
      );
    }
  }

  @Test
  void testClusteredColumnCapabilitiesCombineAllGroupsWithoutDownloads() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(mixedSegmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "mixed_capabilities");
         QueryableIndex eagerIndex = TestHelper.getTestIndexIO().loadIndex(mixedSegmentDir)) {
      final PartialQueryableIndex index = opened.index();

      final ColumnCapabilities tagsCaps = index.getColumnCapabilities("tags");
      Assertions.assertEquals(ColumnType.STRING, tagsCaps.toColumnType());
      Assertions.assertTrue(tagsCaps.hasMultipleValues().isTrue());
      Assertions.assertEquals(ColumnType.DOUBLE, index.getColumnCapabilities("num").toColumnType());

      // the eager index reads the same descriptors, so it must agree
      for (String column : List.of("tenant", ColumnHolder.TIME_COLUMN_NAME, "tags", "num")) {
        final ColumnCapabilities eagerCaps = eagerIndex.getColumnCapabilities(column);
        final ColumnCapabilities partialCaps = index.getColumnCapabilities(column);
        Assertions.assertEquals(eagerCaps.toColumnType(), partialCaps.toColumnType(), column);
        Assertions.assertEquals(eagerCaps.hasMultipleValues(), partialCaps.hasMultipleValues(), column);
        Assertions.assertEquals(eagerCaps.hasNulls(), partialCaps.hasNulls(), column);
      }

      Assertions.assertTrue(
          opened.mapper().getDownloadedFiles().isEmpty(),
          "column capabilities must be answered from metadata with no download; got: "
          + opened.mapper().getDownloadedFiles()
      );
    }
  }

  @Test
  void testCursorCapabilitiesAreThoseOfEachGroup() throws IOException
  {
    // Each group's cursor answers for its own group: tags is multi-valued only in globex, and num is LONG in acme and
    // initech but DOUBLE in globex.
    final CountingRangeReader rangeReader = new CountingRangeReader(mixedSegmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "mixed_cursor_capabilities")) {
      final List<CursorFactory> groupFactories = groupFactories(opened.index(), noOpAcquirer(directExec()));
      Assertions.assertEquals(3, groupFactories.size());
      for (int i = 0; i < groupFactories.size(); i++) {
        try (AsyncCursorHolder asyncHolder = groupFactories.get(i).makeCursorHolderAsync(CursorBuildSpec.FULL_SCAN);
             CursorHolder holder = asyncHolder.release()) {
          final ColumnSelectorFactory selectorFactory = holder.asCursor().getColumnSelectorFactory();
          Assertions.assertEquals(i == 1, selectorFactory.getColumnCapabilities("tags").hasMultipleValues().isTrue());
          Assertions.assertEquals(
              i == 1 ? ColumnType.DOUBLE : ColumnType.LONG,
              selectorFactory.getColumnCapabilities("num").toColumnType()
          );
          Assertions.assertTrue(
              selectorFactory.getColumnCapabilities(ColumnHolder.TIME_COLUMN_NAME).hasNulls().isFalse()
          );
        }
      }

      // Factory-level capabilities come from metadata, and are each group's own, since the segment has no projections
      // for the first group to serve. Descriptors don't record nulls, so they must not claim there are none.
      final ColumnCapabilities firstGroupNum = groupFactories.get(0).getColumnCapabilities("num");
      Assertions.assertEquals(ColumnType.LONG, firstGroupNum.toColumnType());
      Assertions.assertTrue(firstGroupNum.hasNulls().isMaybeTrue());
      Assertions.assertTrue(groupFactories.get(0).getColumnCapabilities("tags").hasMultipleValues().isFalse());
      Assertions.assertEquals(ColumnType.DOUBLE, groupFactories.get(1).getColumnCapabilities("num").toColumnType());
      Assertions.assertTrue(groupFactories.get(1).getColumnCapabilities("tags").hasMultipleValues().isTrue());
      Assertions.assertEquals(ColumnType.LONG, groupFactories.get(2).getColumnCapabilities("num").toColumnType());
      Assertions.assertTrue(groupFactories.get(2).getColumnCapabilities("tags").hasMultipleValues().isFalse());
    }
  }

  @Test
  void testSegmentAnalysisWithoutDownloads() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(mixedSegmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "mixed_analysis")) {
      final PartialQueryableIndexSegment segment = new PartialQueryableIndexSegment(
          opened.index(),
          SegmentId.dummy("test"),
          () -> {},
          noOpAcquirer(directExec())
      );
      final Map<String, ColumnAnalysis> analysis =
          new SegmentAnalyzer(EnumSet.noneOf(SegmentMetadataQuery.AnalysisType.class)).analyze(segment);

      for (Map.Entry<String, ColumnAnalysis> entry : analysis.entrySet()) {
        Assertions.assertFalse(entry.getValue().isError(), entry.getKey() + ": " + entry.getValue());
      }
      Assertions.assertEquals(ColumnType.STRING, analysis.get("tenant").getTypeSignature());
      Assertions.assertEquals(ColumnType.LONG, analysis.get(ColumnHolder.TIME_COLUMN_NAME).getTypeSignature());
      Assertions.assertEquals(ColumnType.STRING, analysis.get("tags").getTypeSignature());
      Assertions.assertTrue(analysis.get("tags").isHasMultipleValues());
      Assertions.assertEquals(ColumnType.DOUBLE, analysis.get("num").getTypeSignature());

      Assertions.assertTrue(
          opened.mapper().getDownloadedFiles().isEmpty(),
          "segment analysis must be answered from metadata with no download; got: "
          + opened.mapper().getDownloadedFiles()
      );
    }
  }

  /**
   * Returns the cursor factories of each cluster group of {@code index}, in clustering order, as used by the views of
   * {@link PartialQueryableIndexSegment}. Callers read clustered segments one group at a time, since a cursor cannot
   * span groups. Each factory downloads its own group's columns when it builds a cursor holder.
   */
  private static List<CursorFactory> groupFactories(PartialQueryableIndex index, PartialBundleAcquirer acquirer)
  {
    final TimeBoundaryInspector timeBoundaryInspector = QueryableIndexTimeBoundaryInspector.create(index);
    final PartialQueryableIndexCursorFactory factory =
        new PartialQueryableIndexCursorFactory(index, timeBoundaryInspector, acquirer);
    final List<CursorFactory> factories = new ArrayList<>();
    for (ClusterGroupView clusterGroup : ClusterGroupView.forAllGroups(
        index.getClusterGroupSchemas(),
        ClusterGroupView.hasProjections(index.getMetadata())
    )) {
      factories.add(factory.makeClusterGroupFactory(timeBoundaryInspector, clusterGroup));
    }
    return factories;
  }

  private PartialQueryableIndexCursorFactory factory(PartialQueryableIndex index)
  {
    return new PartialQueryableIndexCursorFactory(
        index,
        QueryableIndexTimeBoundaryInspector.create(index),
        noOpAcquirer(directExec())
    );
  }

  /**
   * Walk (tenant, region) pairs out of a clustered cursor holder. {@code tenant} is the clustering column, injected as
   * a constant by the cluster-group cursor path; {@code region} is the per-group physical column.
   */
  private static List<List<String>> scanTenantRegion(CursorHolder holder)
  {
    final Cursor cursor = holder.asCursor();
    final List<List<String>> out = new ArrayList<>();
    if (cursor == null) {
      return out;
    }
    final DimensionSelector tenantSel =
        cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of("tenant"));
    final DimensionSelector regionSel =
        cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of("region"));
    while (!cursor.isDone()) {
      final String tenant = tenantSel.getRow().size() == 0 ? null : tenantSel.lookupName(tenantSel.getRow().get(0));
      final String region = regionSel.getRow().size() == 0 ? null : regionSel.lookupName(regionSel.getRow().get(0));
      out.add(Arrays.asList(tenant, region));
      cursor.advance();
    }
    return out;
  }

  private static InputRow row(long ts, String tenant, String region)
  {
    final Map<String, Object> event = new HashMap<>();
    event.put("ts", ts);
    event.put("tenant", tenant);
    event.put("region", region);
    return new MapBasedInputRow(ts, List.of("tenant", "region"), event);
  }

  private static InputRow mixedRow(long ts, String tenant, Object tags, Object num)
  {
    return new MapBasedInputRow(
        ts,
        List.of("tenant", "tags", "num"),
        Map.of("ts", ts, "tenant", tenant, "tags", tags, "num", num)
    );
  }

  /**
   * A {@link PartialBundleAcquirer} that counts how many times each bundle is acquired and released, so a test can
   * assert the cursor factory holds (and ultimately releases) one hold per surviving cluster group. Downloads are
   * submitted to the supplied executor (direct for the success path, gated for the cancel path).
   */
  private static final class CountingBundleAcquirer implements PartialBundleAcquirer
  {
    private final ListeningExecutorService downloadExec;
    private final Map<String, Integer> acquired = new ConcurrentHashMap<>();
    private final Map<String, Integer> released = new ConcurrentHashMap<>();

    private CountingBundleAcquirer(ListeningExecutorService downloadExec)
    {
      this.downloadExec = downloadExec;
    }

    @Override
    public Closeable acquire(String bundleName)
    {
      acquired.merge(bundleName, 1, Integer::sum);
      return () -> released.merge(bundleName, 1, Integer::sum);
    }

    @Override
    public <T> AsyncResource<T> submitDownload(Callable<T> task)
    {
      return AsyncResources.fromFutureUnmanaged(downloadExec.submit(task));
    }

    private int acquiredCount(String bundleName)
    {
      return acquired.getOrDefault(bundleName, 0);
    }

    private int releasedCount(String bundleName)
    {
      return released.getOrDefault(bundleName, 0);
    }
  }
}
