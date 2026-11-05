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

package org.apache.druid.segment.incremental;

import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.ClusteredValueGroupsBaseTableProjectionSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec.Direction;
import org.apache.druid.segment.ClusterGroupView;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentViewTestUtil;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class IncrementalIndexClusteredProjectionTest extends InitializedNullHandlingTest
{
  private static final long T0 = DateTimes.of("2026-01-01T00:00:00").getMillis();
  private static final TimestampSpec TIMESTAMP_SPEC = new TimestampSpec("ts", "millis", null);

  private static MapBasedInputRow row(long ts, String tenant, String region, long x)
  {
    final Map<String, Object> event = new HashMap<>();
    event.put("ts", ts);
    event.put("tenant", tenant);
    event.put("region", region);
    event.put("x", x);
    return new MapBasedInputRow(ts, List.of("tenant", "region", "x"), event);
  }

  private static OnheapIncrementalIndex clusteredWithProjection()
  {
    return clusteredWithProjection(
        AggregateProjectionSpec.builder("proj")
                               .groupingColumns(new StringDimensionSchema("tenant"))
                               .aggregators(
                                   new CountAggregatorFactory("cnt"),
                                   new LongSumAggregatorFactory("sum_x", "x")
                               )
                               .build()
    );
  }

  private static OnheapIncrementalIndex clusteredWithProjection(AggregateProjectionSpec projectionSpec)
  {
    final ClusteredValueGroupsBaseTableProjectionSpec spec = ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .columns(
            new StringDimensionSchema("tenant"),
            new StringDimensionSchema("region"),
            new LongDimensionSchema("x"),
            new LongDimensionSchema("__time")
        )
        .clusteringColumns("tenant")
        .build();
    final IncrementalIndexSchema schema = IncrementalIndexSchema.builder()
        .withMinTimestamp(T0)
        .withTimestampSpec(TIMESTAMP_SPEC)
        .withQueryGranularity(Granularities.NONE)
        .withDimensionsSpec(spec.getDimensionsSpec())
        .withRollup(false)
        .withClusterSpec(spec)
        .withProjections(List.of(projectionSpec))
        .build();
    final OnheapIncrementalIndex index = (OnheapIncrementalIndex) new OnheapIncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(10_000)
        .build();
    // Add tenants out of clustering order to prove the plain scan still walks groups in clustering-sorted order.
    index.add(row(T0 + 2, "globex", "eu-west-1", 7));
    index.add(row(T0, "acme", "us-east-1", 10));
    index.add(row(T0 + 1, "acme", "us-west-2", 5));
    return index;
  }

  /**
   * A group-by on the projection's grouping column + aggregators, granularity ALL over ETERNITY. Built through the
   * real {@link GroupingEngine#makeCursorBuildSpec} path so the resulting spec is what production planning produces.
   */
  private static CursorBuildSpec projectionMatchingBuildSpec()
  {
    return projectionMatchingBuildSpec(Intervals.ETERNITY);
  }

  private static CursorBuildSpec projectionMatchingBuildSpec(Interval interval)
  {
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(interval)
                    .addDimension("tenant")
                    .addAggregator(new CountAggregatorFactory("cnt"))
                    .addAggregator(new LongSumAggregatorFactory("sum_x", "x"))
                    .addOrderByColumn("tenant", Direction.ASCENDING)
                    .build();
    return GroupingEngine.makeCursorBuildSpec(query, null);
  }

  @Test
  void testProjectionMaterializedOnClusteredIndex()
  {
    try (OnheapIncrementalIndex index = clusteredWithProjection()) {
      // Construction did not throw (the OnheapIncrementalIndex guard against clusterSpec + projections was removed)
      // and the projection accumulated rows during ingest.
      Assertions.assertNotNull(index.getProjection("proj"));
    }
  }

  @Test
  void testProjectionSelectedAndAggregatesCorrectlyOnClusteredIndex()
  {
    try (OnheapIncrementalIndex index = clusteredWithProjection()) {
      final CursorBuildSpec buildSpec = projectionMatchingBuildSpec();

      // The projection must actually be selected for this spec on a clustered index -- not merely present. If this is
      // null, guard removal alone is insufficient and the query path needs more work.
      Assertions.assertNotNull(
          index.getProjection(buildSpec),
          "projection 'proj' was not selected for the group-by build spec on the clustered index"
      );

      final IncrementalIndexCursorFactory factory = new IncrementalIndexCursorFactory(index);
      try (CursorHolder holder = factory.makeCursorHolder(buildSpec)) {
        // The chosen holder is the aggregate-projection cursor: it serves pre-aggregated rows.
        Assertions.assertTrue(holder.isPreAggregated(), "expected the projection (pre-aggregated) cursor holder");

        final Cursor cursor = holder.asCursor();
        final DimensionSelector tenantSel =
            cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of("tenant"));
        final ColumnValueSelector<?> cntSel = cursor.getColumnSelectorFactory().makeColumnValueSelector("cnt");
        final ColumnValueSelector<?> sumSel = cursor.getColumnSelectorFactory().makeColumnValueSelector("sum_x");

        final Map<String, long[]> byTenant = new LinkedHashMap<>();
        while (!cursor.isDone()) {
          final String tenant = tenantSel.lookupName(tenantSel.getRow().get(0));
          byTenant.put(tenant, new long[]{cntSel.getLong(), sumSel.getLong()});
          cursor.advance();
        }

        // acme: 2 rows (x=10, x=5) -> cnt=2, sum_x=15 ; globex: 1 row (x=7) -> cnt=1, sum_x=7
        Assertions.assertEquals(2, byTenant.size(), "expected one pre-aggregated row per tenant");
        Assertions.assertArrayEquals(new long[]{2L, 15L}, byTenant.get("acme"));
        Assertions.assertArrayEquals(new long[]{1L, 7L}, byTenant.get("globex"));
      }
    }
  }

  @Test
  void testProjectionServedByFirstClusterGroup()
  {
    // Views acquired without knowing the query are the cluster groups, so the projection, which spans every group, is
    // served by the first group alone; the other groups' cursors are empty.
    try (OnheapIncrementalIndex index = clusteredWithProjection()) {
      final List<Segment> groups = SegmentViewTestUtil.residentViews(
          new IncrementalIndexSegment(index, SegmentId.dummy("test")),
          null
      );
      Assertions.assertEquals(2, groups.size());

      final CursorBuildSpec buildSpec = projectionMatchingBuildSpec();
      try (CursorHolder holder = groups.get(0).as(CursorFactory.class).makeCursorHolder(buildSpec)) {
        Assertions.assertTrue(holder.isPreAggregated(), "expected the projection (pre-aggregated) cursor holder");
        final Cursor cursor = holder.asCursor();
        final DimensionSelector tenantSel =
            cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of("tenant"));
        final List<String> tenants = new ArrayList<>();
        while (!cursor.isDone()) {
          tenants.add(tenantSel.lookupName(tenantSel.getRow().get(0)));
          cursor.advance();
        }
        Assertions.assertEquals(List.of("acme", "globex"), tenants);
      }
      try (CursorHolder holder = groups.get(1).as(CursorFactory.class).makeCursorHolder(buildSpec)) {
        Assertions.assertTrue(holder.asCursor().isDone());
      }
    }
  }

  @Test
  void testViewsAcquiredWithSpecIgnoreProjections()
  {
    // Views acquired with a spec that matches no projection are only the groups that the spec does not rule out. They
    // read their own rows, even for specs that match a projection, since no group serves it.
    try (OnheapIncrementalIndex index = clusteredWithProjection()) {
      final CursorBuildSpec globexSpec =
          CursorBuildSpec.builder()
                         .setFilter(new EqualityFilter("tenant", ColumnType.STRING, "globex", null))
                         .build();
      final List<Segment> views = SegmentViewTestUtil.residentViews(
          new IncrementalIndexSegment(index, SegmentId.dummy("test")),
          globexSpec
      );
      Assertions.assertEquals(1, views.size());
      Assertions.assertEquals(
          ClusterGroupView.ProjectionRole.IGNORES,
          views.get(0).as(ClusterGroupView.class).projectionRole()
      );
      try (CursorHolder holder = views.get(0).as(CursorFactory.class).makeCursorHolder(projectionMatchingBuildSpec())) {
        Assertions.assertFalse(holder.isPreAggregated());
      }
      Assertions.assertEquals(1, countInputRows(views.get(0), projectionMatchingBuildSpec()));
    }
  }

  @Test
  void testClusterGroupsAgreeOnProjectionWhenRowsArriveBetweenCursors()
  {
    // The projection groups rows by hour, so it serves a query interval that is not aligned to hours only while every
    // row is within the interval. Views acquired together decide whether specs match a projection only once, so a
    // later row past the end of the interval cannot make one group read its own rows after another group left them to
    // the projection (losing them), or the reverse (counting them twice).
    final AggregateProjectionSpec hourlyProjectionSpec =
        AggregateProjectionSpec.builder("proj_hourly")
                               .virtualColumns(Granularities.toVirtualColumn(Granularities.HOUR, "__gran"))
                               .groupingColumns(new LongDimensionSchema("__gran"), new StringDimensionSchema("tenant"))
                               .aggregators(
                                   new CountAggregatorFactory("cnt"),
                                   new LongSumAggregatorFactory("sum_x", "x")
                               )
                               .build();
    final CursorBuildSpec buildSpec = projectionMatchingBuildSpec(Intervals.utc(T0, T0 + 10));

    for (boolean servingGroupFirst : new boolean[]{true, false}) {
      try (OnheapIncrementalIndex index = clusteredWithProjection(hourlyProjectionSpec)) {
        Assertions.assertNotNull(index.getProjection(buildSpec));
        final List<Segment> groups = SegmentViewTestUtil.residentViews(
            new IncrementalIndexSegment(index, SegmentId.dummy("test")),
            null
        );
        Assertions.assertEquals(2, groups.size());

        long numRows = countInputRows(groups.get(servingGroupFirst ? 0 : 1), buildSpec);
        index.add(row(T0 + 7_200_000, "acme", "us-east-1", 100));
        Assertions.assertNull(index.getProjection(buildSpec));
        numRows += countInputRows(groups.get(servingGroupFirst ? 1 : 0), buildSpec);
        Assertions.assertEquals(3, numRows, "servingGroupFirst[" + servingGroupFirst + "]");
      }
    }
  }

  @Test
  void testBaseClusteredScanStillWalksGroupsInClusteringOrder()
  {
    try (OnheapIncrementalIndex index = clusteredWithProjection()) {
      // FULL_SCAN does not match the projection, so each cluster group reads its own rows, and the groups come in
      // clustering-ascending order (acme rows before globex), exactly as for the non-projection clustered index.
      final List<List<String>> out = new ArrayList<>();
      for (Segment group : SegmentViewTestUtil.residentViews(
          new IncrementalIndexSegment(index, SegmentId.dummy("test")),
          CursorBuildSpec.FULL_SCAN
      )) {
        try (CursorHolder holder = group.as(CursorFactory.class).makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
          Assertions.assertFalse(
              holder.isPreAggregated(),
              "FULL_SCAN must not pick the projection; it should use the clustered base-table cursor"
          );
          final Cursor cursor = holder.asCursor();
          final DimensionSelector tenantSel =
              cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of("tenant"));
          final DimensionSelector regionSel =
              cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of("region"));
          while (!cursor.isDone()) {
            final String tenant =
                tenantSel.getRow().size() == 0 ? null : tenantSel.lookupName(tenantSel.getRow().get(0));
            final String region =
                regionSel.getRow().size() == 0 ? null : regionSel.lookupName(regionSel.getRow().get(0));
            out.add(Arrays.asList(tenant, region));
            cursor.advance();
          }
        }
      }
      Assertions.assertEquals(
          List.of(
              List.of("acme", "us-east-1"),
              List.of("acme", "us-west-2"),
              List.of("globex", "eu-west-1")
          ),
          out
      );
    }
  }

  /**
   * Returns the number of input rows that a cursor of {@code segment} for {@code buildSpec} covers: the sum of
   * {@code cnt} if it reads a projection, or else the number of rows.
   */
  private static long countInputRows(Segment segment, CursorBuildSpec buildSpec)
  {
    try (CursorHolder holder = segment.as(CursorFactory.class).makeCursorHolder(buildSpec)) {
      final Cursor cursor = holder.asCursor();
      final ColumnValueSelector<?> cntSel =
          holder.isPreAggregated() ? cursor.getColumnSelectorFactory().makeColumnValueSelector("cnt") : null;
      long numRows = 0;
      while (!cursor.isDone()) {
        numRows += cntSel != null ? cntSel.getLong() : 1;
        cursor.advance();
      }
      return numRows;
    }
  }
}
