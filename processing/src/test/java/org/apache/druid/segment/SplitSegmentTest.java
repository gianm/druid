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

import com.google.common.collect.Iterables;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.RestrictedDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.FilterSegmentPruner;
import org.apache.druid.query.filter.SegmentPruner;
import org.apache.druid.query.planning.ExecutionVertex;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.query.policy.RestrictAllTablesPolicyEnforcer;
import org.apache.druid.query.policy.RowFilterPolicy;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.testing.TemporaryFolderExtension;
import org.apache.druid.timeline.SegmentId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

class SplitSegmentTest extends InitializedNullHandlingTest
{
  @RegisterExtension
  public final TemporaryFolderExtension temporaryFolder = TemporaryFolderExtension.testCaseScoped();

  private QueryableIndex index;

  @AfterEach
  void tearDown()
  {
    if (index != null) {
      index.close();
    }
  }

  @Test
  void testNonClusteredSegmentIsNotSplit()
  {
    final Segment segment = new QueryableIndexSegment(TestIndex.getMMappedTestIndex(), SegmentId.dummy("test"));
    final AtomicInteger mapCount = new AtomicInteger();
    final SegmentMapFunction mapFn = SplitSegment.mapFunction(
        s -> {
          mapCount.incrementAndGet();
          return s;
        },
        null
    );

    Assertions.assertSame(segment, mapFn.apply(Optional.of(segment)).orElseThrow());
    Assertions.assertEquals(1, mapCount.get());
  }

  @Test
  void testClusteredSegmentIsSplitIntoMappedClusterGroups()
  {
    final Segment segment = new QueryableIndexSegment(buildThreeGroupIndex(), SegmentId.dummy("test"));
    final Segment mapped = SplitSegment.mapFunction(
        s -> s.map(base -> new FilteredSegment(base, null)),
        null
    ).apply(Optional.of(segment)).orElseThrow();

    final SplitSegment splitSegment = Assertions.assertInstanceOf(SplitSegment.class, mapped);
    Assertions.assertEquals(segment.getId(), splitSegment.getId());

    final List<Integer> groupNumbers = new ArrayList<>();
    final List<List<String>> tenantsByGroup = new ArrayList<>();
    for (SplitSegment.MappedClusterGroup clusterGroup : splitSegment.getClusterGroups()) {
      groupNumbers.add(clusterGroup.groupNumber());
      // Each group is mapped the same way as the segment.
      Assertions.assertInstanceOf(FilteredSegment.class, clusterGroup.segment());
      Assertions.assertEquals(segment.getId(), clusterGroup.segment().getId());
      tenantsByGroup.add(readTenants(clusterGroup.segment()));
    }
    Assertions.assertEquals(List.of(0, 1, 2), groupNumbers);
    Assertions.assertEquals(
        List.of(List.of("acme", "acme"), List.of("globex"), List.of("initech")),
        tenantsByGroup
    );
  }

  @Test
  void testClusteredRealtimeSegmentWithoutGroupsIsSplitIntoNoGroups()
  {
    // Realtime segments may gain cluster groups after they are split. So a clustered segment with no groups yet is
    // split into no groups, rather than processed as a whole, whose cursor would fail once the segment had several
    // groups.
    try (IncrementalIndex realtimeIndex = SegmentViewTestUtil.buildEmptyClusteredIncrementalIndex()) {
      final Segment segment = new IncrementalIndexSegment(realtimeIndex, SegmentId.dummy("test"));
      final Segment mapped = SplitSegment.mapFunction(SegmentMapFunction.IDENTITY, null)
                                         .apply(Optional.of(segment))
                                         .orElseThrow();
      realtimeIndex.add(SegmentViewTestUtil.row("acme", "2025-01-01T00:00:00", "us-east-1"));
      realtimeIndex.add(SegmentViewTestUtil.row("globex", "2025-01-01T00:30:00", "eu-west-1"));

      final SplitSegment splitSegment = Assertions.assertInstanceOf(SplitSegment.class, mapped);
      Assertions.assertEquals(List.of(), splitSegment.getClusterGroups());
    }
  }

  @Test
  void testPrunerSkipsClusterGroups()
  {
    final Segment segment = new QueryableIndexSegment(buildThreeGroupIndex(), SegmentId.dummy("test"));

    // The segment has no aggregate projections, so the first group is pruned like any other.
    Assertions.assertEquals(List.of(1), splitGroupNumbers(segment, tenantPruner("globex")));
    Assertions.assertEquals(List.of(0), splitGroupNumbers(segment, tenantPruner("acme")));
    Assertions.assertEquals(List.of(), splitGroupNumbers(segment, tenantPruner("nobody")));
    Assertions.assertEquals(List.of(0, 1, 2), splitGroupNumbers(segment, null));
  }

  @Test
  void testClusterGroupsOfSegmentWithoutProjectionsAreCacheable()
  {
    final Segment segment = new QueryableIndexSegment(buildThreeGroupIndex(), SegmentId.dummy("test"));
    // Deliberately not closed: closing the SplitSegment would close the caller's segment.
    final SplitSegment splitSegment = (SplitSegment) SplitSegment.mapFunction(SegmentMapFunction.IDENTITY, null)
                                                                 .apply(Optional.of(segment))
                                                                 .orElseThrow();
    Assertions.assertTrue(splitSegment.isClusterGroupCacheable());
  }

  @Test
  void testCloseClosesClusterGroupsThenSegment() throws IOException
  {
    final List<String> closed = new ArrayList<>();
    final Segment segment = closeTrackingSegment(() -> closed.add("segment"));

    final Segment mapped = SplitSegment.mapFunction(
        s -> s.map(
            base -> base instanceof ClusterGroupSegment group
                    ? closeTracking(group, () -> closed.add("group" + groupNumber(group)))
                    : base
        ),
        null
    ).apply(Optional.of(segment)).orElseThrow();

    Assertions.assertTrue(closed.isEmpty());
    mapped.close();
    Assertions.assertEquals(List.of("group2", "group1", "group0", "segment"), closed);
  }

  @Test
  void testMappingFailureClosesMappedClusterGroupsThenSegment()
  {
    final List<String> closed = new ArrayList<>();
    final Segment segment = closeTrackingSegment(() -> closed.add("segment"));
    final SegmentMapFunction mapFn = SplitSegment.mapFunction(
        s -> s.map(
            base -> {
              if (base instanceof ClusterGroupSegment group) {
                if (groupNumber(group) == 1) {
                  throw DruidException.defensive("oops");
                }
                return closeTracking(group, () -> closed.add("group" + groupNumber(group)));
              }
              return base;
            }
        ),
        null
    );

    Assertions.assertThrows(DruidException.class, () -> mapFn.apply(Optional.of(segment)));
    Assertions.assertEquals(List.of("group0", "segment"), closed);
  }

  @Test
  void testEmptyClusterGroupMappingClosesMappedClusterGroupsThenSegment()
  {
    final List<String> closed = new ArrayList<>();
    final Segment segment = closeTrackingSegment(() -> closed.add("segment"));
    final SegmentMapFunction mapFn = SplitSegment.mapFunction(
        s -> s.flatMap(
            base -> {
              if (base instanceof ClusterGroupSegment group) {
                // Like a join that cannot acquire its right-hand side.
                return groupNumber(group) == 1
                       ? Optional.empty()
                       : Optional.of(closeTracking(group, () -> closed.add("group" + groupNumber(group))));
              }
              return Optional.of(base);
            }
        ),
        null
    );

    Assertions.assertEquals(Optional.empty(), mapFn.apply(Optional.of(segment)));
    Assertions.assertEquals(List.of("group0", "segment"), closed);
  }

  @Test
  void testRowFilterPolicyAppliesToEachClusterGroup()
  {
    // Mapped as segment walkers do. The policy filters on a non-clustering column, so it rules out no groups.
    final Segment segment = closeTrackingSegment(() -> {});
    final SplitSegment splitSegment = (SplitSegment) walkerMapFunction(
        restrictedTable(new EqualityFilter("region", ColumnType.STRING, "us-west-2", null)),
        new RestrictAllTablesPolicyEnforcer(null)
    ).apply(Optional.of(segment)).orElseThrow();

    final List<List<String>> tenantsByGroup = new ArrayList<>();
    for (SplitSegment.MappedClusterGroup clusterGroup : splitSegment.getClusterGroups()) {
      Assertions.assertInstanceOf(RestrictedSegment.class, clusterGroup.segment());
      // Reading columns directly would skip the policy.
      Assertions.assertNull(clusterGroup.segment().as(QueryableIndex.class));
      tenantsByGroup.add(readTenants(clusterGroup.segment()));
    }
    Assertions.assertEquals(List.of(List.of("acme"), List.of(), List.of()), tenantsByGroup);
  }

  @Test
  void testRowFilterPolicyOnClusteringColumnRulesOutClusterGroups()
  {
    final Segment segment = closeTrackingSegment(() -> {});
    final SplitSegment splitSegment = (SplitSegment) walkerMapFunction(
        restrictedTable(new EqualityFilter("tenant", ColumnType.STRING, "globex", null)),
        new RestrictAllTablesPolicyEnforcer(null)
    ).apply(Optional.of(segment)).orElseThrow();

    final SplitSegment.MappedClusterGroup clusterGroup = Iterables.getOnlyElement(splitSegment.getClusterGroups());
    Assertions.assertEquals(1, clusterGroup.groupNumber());
    Assertions.assertEquals(List.of("globex"), readTenants(clusterGroup.segment()));
  }

  @Test
  void testUnrestrictedTableFailsPolicyValidationAndClosesSegment()
  {
    final List<String> closed = new ArrayList<>();
    final Segment segment = closeTrackingSegment(() -> closed.add("segment"));
    final SegmentMapFunction mapFn =
        walkerMapFunction(TableDataSource.create("test"), new RestrictAllTablesPolicyEnforcer(null));

    final DruidException e = Assertions.assertThrows(DruidException.class, () -> mapFn.apply(Optional.of(segment)));
    Assertions.assertEquals(DruidException.Category.FORBIDDEN, e.getCategory());
    Assertions.assertEquals(List.of("segment"), closed);
  }

  /**
   * Returns the function that segment walkers map segments with for a scan of {@code dataSource}.
   */
  private static SegmentMapFunction walkerMapFunction(DataSource dataSource, PolicyEnforcer policyEnforcer)
  {
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(dataSource)
                                  .intervals(new MultipleIntervalSegmentSpec(List.of(Intervals.ETERNITY)))
                                  .build();
    final ExecutionVertex ev = ExecutionVertex.of(query);
    return SplitSegment.mapFunction(ev.createSegmentMapFunction(policyEnforcer), ev.getSegmentPruner());
  }

  private static DataSource restrictedTable(DimFilter rowFilter)
  {
    return RestrictedDataSource.create(TableDataSource.create("test"), RowFilterPolicy.from(rowFilter));
  }

  private static List<Integer> splitGroupNumbers(Segment segment, SegmentPruner pruner)
  {
    // Deliberately not closed: closing the SplitSegment would close the caller's segment.
    final SplitSegment splitSegment = (SplitSegment) SplitSegment.mapFunction(SegmentMapFunction.IDENTITY, pruner)
                                                                 .apply(Optional.of(segment))
                                                                 .orElseThrow();
    final List<Integer> groupNumbers = new ArrayList<>();
    for (SplitSegment.MappedClusterGroup group : splitSegment.getClusterGroups()) {
      groupNumbers.add(group.groupNumber());
    }
    return groupNumbers;
  }

  private static int groupNumber(Segment group)
  {
    return group.as(ClusterGroupView.class).groupNumber();
  }

  private static SegmentPruner tenantPruner(String tenant)
  {
    return new FilterSegmentPruner(
        new EqualityFilter("tenant", ColumnType.STRING, tenant, null),
        null,
        null
    );
  }

  /**
   * Returns a three-group segment that calls {@code onClose} when closed, rather than closing its index.
   */
  private Segment closeTrackingSegment(Closeable onClose)
  {
    return ReferenceCountedSegmentProvider.wrapCloseable(
        new ReferenceCountedSegmentProvider.UnmanagedReference(
            new QueryableIndexSegment(buildThreeGroupIndex(), SegmentId.dummy("test"))
        ),
        onClose
    ).orElseThrow();
  }

  private static Segment closeTracking(ClusterGroupSegment group, Closeable onClose)
  {
    return ReferenceCountedSegmentProvider.wrapCloseable(group, onClose).orElseThrow();
  }

  private static List<String> readTenants(Segment segment)
  {
    final List<String> tenants = new ArrayList<>();
    try (CursorHolder holder = segment.as(CursorFactory.class).makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
      final Cursor cursor = holder.asCursor();
      final DimensionSelector selector =
          cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of("tenant"));
      while (!cursor.isDone()) {
        tenants.add(selector.lookupName(selector.getRow().get(0)));
        cursor.advance();
      }
    }
    return tenants;
  }

  private QueryableIndex buildThreeGroupIndex()
  {
    index = SegmentViewTestUtil.buildThreeGroupIndex(temporaryFolder.getRoot());
    return index;
  }
}
