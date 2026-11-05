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

package org.apache.druid.query.metadata;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.ClusteredValueGroupsBaseTableProjectionSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.testing.TemporaryFolderExtension;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class SegmentMetadataQueryClusteredTest extends InitializedNullHandlingTest
{
  private static final Interval INTERVAL = Intervals.of("2025-01-01/2025-01-02");
  private static final long T0 = INTERVAL.getStartMillis();

  private static final SegmentMetadataQueryRunnerFactory FACTORY = new SegmentMetadataQueryRunnerFactory(
      new SegmentMetadataQueryQueryToolChest(new SegmentMetadataQueryConfig()),
      QueryRunnerTestHelper.NOOP_QUERYWATCHER
  );

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
  void testAnalysesOfClusterGroupsMergeIntoAnalysisOfSegment() throws IOException
  {
    // Segment walkers analyze a clustered segment one cluster group at a time, and merge the analyses as parts of one
    // segment: types that differ between groups combine into the least restrictive type, cardinality is unknown since
    // each group has its own dictionary, and segment-level metadata, such as projections, is reported once.
    index = buildIndex(
        List.of(
            row(T0, "acme", "us-east-1", 1L),
            row(T0 + 1, "acme", "us-west-2", 2L),
            row(T0 + 2, "globex", "eu-west-1", 1.5)
        )
    );

    final SegmentMetadataQuery query =
        Druids.newSegmentMetadataQueryBuilder()
              .dataSource("test")
              .intervals(List.of(INTERVAL))
              .analysisTypes(
                  SegmentMetadataQuery.AnalysisType.CARDINALITY,
                  SegmentMetadataQuery.AnalysisType.SIZE,
                  SegmentMetadataQuery.AnalysisType.INTERVAL,
                  SegmentMetadataQuery.AnalysisType.PROJECTIONS
              )
              .merge(false)
              .build();
    final List<SegmentAnalysis> analyses = QueryRunnerTestHelper.makeSplittingQueryRunner(
        FACTORY,
        new QueryableIndexSegment(index, SegmentId.dummy("test"))
    ).run(QueryPlus.wrap(query)).toList();

    Assertions.assertEquals(1, analyses.size());
    final SegmentAnalysis analysis = analyses.get(0);
    Assertions.assertEquals(3, analysis.getNumRows());

    // "num" is LONG in the acme group, and DOUBLE in the globex group.
    final ColumnAnalysis num = analysis.getColumns().get("num");
    Assertions.assertFalse(num.isError(), () -> "num: " + num);
    Assertions.assertEquals(ColumnType.DOUBLE, num.getTypeSignature());

    final ColumnAnalysis tenant = analysis.getColumns().get("tenant");
    Assertions.assertFalse(tenant.isError(), () -> "tenant: " + tenant);
    Assertions.assertEquals(ColumnType.STRING, tenant.getTypeSignature());
    Assertions.assertEquals(DimensionDictionarySelector.CARDINALITY_UNKNOWN, tenant.getCardinality());
    Assertions.assertEquals(
        DimensionDictionarySelector.CARDINALITY_UNKNOWN,
        analysis.getColumns().get("region").getCardinality()
    );

    // The projection has a row for each tenant. Each group reports it, but it counts once.
    Assertions.assertEquals(2, analysis.getProjections().get("proj").getNumRows());
  }

  private QueryableIndex buildIndex(List<InputRow> rows) throws IOException
  {
    final ClusteredValueGroupsBaseTableProjectionSpec clusterSpec =
        ClusteredValueGroupsBaseTableProjectionSpec.builder()
            .columns(
                new StringDimensionSchema("tenant"),
                new StringDimensionSchema("region"),
                AutoTypeColumnSchema.of("num"),
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
                              .withProjections(
                                  List.of(
                                      AggregateProjectionSpec.builder("proj")
                                                             .groupingColumns(new StringDimensionSchema("tenant"))
                                                             .aggregators(new CountAggregatorFactory("cnt"))
                                                             .build()
                                  )
                              )
                              .build();
    return IndexBuilder.create()
                       .useV10()
                       .tmpDir(temporaryFolder.newFolder())
                       .schema(schema)
                       .rows(rows)
                       .buildMMappedIndex(INTERVAL);
  }

  private static InputRow row(long ts, String tenant, String region, Object num)
  {
    final Map<String, Object> event = new HashMap<>();
    event.put("ts", ts);
    event.put("tenant", tenant);
    event.put("region", region);
    event.put("num", num);
    return new MapBasedInputRow(ts, List.of("tenant", "region", "num"), event);
  }
}
