/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.bitmap;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.Pair;
import com.metamx.common.guava.Sequences;
import io.druid.collections.StupidPool;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Druids;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.TestQueryRunners;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryEngine;
import io.druid.query.groupby.GroupByQueryQueryToolChest;
import io.druid.query.groupby.GroupByQueryRunnerFactory;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNQueryBuilder;
import io.druid.query.topn.TopNQueryConfig;
import io.druid.query.topn.TopNQueryQueryToolChest;
import io.druid.query.topn.TopNQueryRunnerFactory;
import io.druid.query.topn.TopNResultValue;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.IndexBuilder;
import io.druid.segment.IndexMerger;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.TestHelper;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class BitmapAggregatorQueryTest
{
  private static final String VISITOR_ID = "visitor_id";
  private static final String VISITOR_BITMAP = "visitor_bitmap";
  private static final String CLIENT_ID = "client_type";
  private static final String CLIENT_BITMAP = "client_bitmap";
  private static final DateTime TIME = new DateTime("2016-03-04T00:00:00.000Z");

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final IndexBuilder indexBuilder;
  private final Function<IndexBuilder, Pair<Segment, Closeable>> finisher;

  private Segment segment;
  private Closeable closeable;

  public BitmapAggregatorQueryTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<Segment, Closeable>> finisher
  )
  {
    this.indexBuilder = indexBuilder;
    this.finisher = finisher;
  }

  @Before
  public void setUp() throws Exception
  {
    BitmapAggregationModule module = new BitmapAggregationModule();
    module.configure(null);

    long timestamp = TIME.getMillis();

    indexBuilder
        .tmpDir(temporaryFolder.newFolder())
        .schema(
            new IncrementalIndexSchema.Builder()
                .withDimensionsSpec(
                    new DimensionsSpec(
                        ImmutableList.of(VISITOR_ID),
                        null,
                        null
                    )
                )
                .withMetrics(
                    new AggregatorFactory[]{
                        new CountAggregatorFactory("count"),
                        new BitmapAggregatorFactory(VISITOR_BITMAP, VISITOR_ID, false),
                        new BitmapAggregatorFactory(CLIENT_BITMAP, CLIENT_ID, false)
                    }
                )
                .build()
        )
        .rows(
            new MapBasedInputRow(
                timestamp,
                Lists.newArrayList(VISITOR_ID, CLIENT_ID),
                ImmutableMap.<String, Object>of(VISITOR_ID, "00000000", CLIENT_ID, "00000001")
            ),
            new MapBasedInputRow(
                timestamp,
                Lists.newArrayList(VISITOR_ID, CLIENT_ID),
                ImmutableMap.<String, Object>of(VISITOR_ID, "00000002", CLIENT_ID, "00000002")
            ),
            new MapBasedInputRow(
                timestamp,
                Lists.newArrayList(VISITOR_ID, CLIENT_ID),
                ImmutableMap.<String, Object>of(VISITOR_ID, "00000002", CLIENT_ID, "00000003")
            )
        );

    final Pair<Segment, Closeable> pair = finisher.apply(indexBuilder);
    segment = pair.lhs;
    closeable = pair.rhs;
  }

  @After
  public void tearDown() throws Exception
  {
    closeable.close();
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> constructorFeeder() throws IOException
  {
    return makeConstructors();
  }

  public static Collection<Object[]> makeConstructors()
  {
    // TODO(gianm): Some kind of helper to reduce code duplication with BaseFilterTest

    final List<Object[]> constructors = Lists.newArrayList();

    final Map<String, IndexMerger> indexMergers = ImmutableMap.<String, IndexMerger>of(
        "IndexMerger", TestHelper.getTestIndexMerger(),
        "IndexMergerV9", TestHelper.getTestIndexMergerV9()
    );

    final Map<String, Function<IndexBuilder, Pair<Segment, Closeable>>> finishers = ImmutableMap.of(
        "incremental", new Function<IndexBuilder, Pair<Segment, Closeable>>()
        {
          @Override
          public Pair<Segment, Closeable> apply(IndexBuilder input)
          {
            final IncrementalIndex index = input.buildIncrementalIndex();
            return Pair.<Segment, Closeable>of(
                new IncrementalIndexSegment(index, "dummy"),
                new Closeable()
                {
                  @Override
                  public void close() throws IOException
                  {
                    index.close();
                  }
                }
            );
          }
        },
        "mmapped", new Function<IndexBuilder, Pair<Segment, Closeable>>()
        {
          @Override
          public Pair<Segment, Closeable> apply(IndexBuilder input)
          {
            final QueryableIndex index = input.buildMMappedIndex();
            return Pair.<Segment, Closeable>of(
                new QueryableIndexSegment("dummy", index),
                new Closeable()
                {
                  @Override
                  public void close() throws IOException
                  {
                    index.close();
                  }
                }
            );
          }
        },
        "mmappedMerged", new Function<IndexBuilder, Pair<Segment, Closeable>>()
        {
          @Override
          public Pair<Segment, Closeable> apply(IndexBuilder input)
          {
            final QueryableIndex index = input.buildMMappedMergedIndex();
            return Pair.<Segment, Closeable>of(
                new QueryableIndexSegment("dummy", index),
                new Closeable()
                {
                  @Override
                  public void close() throws IOException
                  {
                    index.close();
                  }
                }
            );
          }
        }
    );

    for (Map.Entry<String, IndexMerger> indexMergerEntry : indexMergers.entrySet()) {
      for (Map.Entry<String, Function<IndexBuilder, Pair<Segment, Closeable>>> finisherEntry : finishers.entrySet()) {
        final String testName = String.format(
            "indexMerger[%s], finisher[%s]",
            indexMergerEntry.getKey(),
            finisherEntry.getKey()
        );
        final IndexBuilder indexBuilder = IndexBuilder.create()
                                                      .indexMerger(indexMergerEntry.getValue());

        constructors.add(new Object[]{testName, indexBuilder, finisherEntry.getValue()});
      }
    }

    return constructors;
  }

  @Test
  public void testTimeseries() throws Exception
  {
    QueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
        new TimeseriesQueryQueryToolChest(QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()),
        new TimeseriesQueryEngine(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.allGran)
                                  .intervals(QueryRunnerTestHelper.fullOnInterval)
                                  .aggregators(
                                      Lists.newArrayList(
                                          QueryRunnerTestHelper.rowsCount,
                                          new LongSumAggregatorFactory("count", "count"),
                                          new BitmapAggregatorFactory("UV", VISITOR_ID, false),
                                          new BitmapAggregatorFactory("UV2", VISITOR_BITMAP, true),
                                          new BitmapAggregatorFactory("UC", CLIENT_BITMAP, true)
                                      )
                                  )
                                  .postAggregators(
                                      ImmutableList.<PostAggregator>of(
                                          new BitmapSizePostAggregator(
                                              "UV-intersect",
                                              new BitmapSetPostAggregator(
                                                  "dummy",
                                                  BitmapSetPostAggregator.Func.INTERSECT.toString(),
                                                  ImmutableList.<PostAggregator>of(
                                                      new FieldAccessPostAggregator("dummy2", "UV"),
                                                      new FieldAccessPostAggregator("dummy3", "UV2")
                                                  )
                                              )
                                          ),
                                          new BitmapRawPostAggregator(
                                              "UV-list",
                                              BitmapRawPostAggregator.Format.LIST.toString(),
                                              new FieldAccessPostAggregator("dummy4", "UV")
                                          ),
                                          new BitmapRawPostAggregator(
                                              "UV-roaringBase64",
                                              BitmapRawPostAggregator.Format.ROARINGBASE64.toString(),
                                              new FieldAccessPostAggregator("dummy5", "UV")
                                          )
                                      )
                                  )
                                  .build();

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        new FinalizeResultsQueryRunner(
            factory.createRunner(segment),
            factory.getToolchest()
        ).run(query, Maps.newHashMap()),
        Lists.<Result<TimeseriesResultValue>>newLinkedList()
    );

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            TIME,
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                    .put("UV", 2)
                    .put("UV2", 2)
                    .put("UC", 3)
                    .put("rows", 3L)
                    .put("count", 3L)
                    .put("UV-intersect", 2)
                    .put("UV-list", ImmutableList.of(0, 2))
                    .put("UV-roaringBase64", "OjAAAAEAAAAAAAEAEAAAAAAAAgA=")
                    .build()
            )
        )
    );
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTopN() throws Exception
  {
    QueryRunnerFactory factory = new TopNQueryRunnerFactory(
        TestQueryRunners.getPool(),
        new TopNQueryQueryToolChest(
            new TopNQueryConfig(),
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
        ),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );

    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .dimension(VISITOR_ID)
        .metric("UC")
        .threshold(5)
        .aggregators(
            Lists.newArrayList(
                new LongSumAggregatorFactory("count", "count"),
                new BitmapAggregatorFactory("UV", VISITOR_ID, false),
                new BitmapAggregatorFactory("UV2", VISITOR_BITMAP, true),
                new BitmapAggregatorFactory("UC", CLIENT_BITMAP, true)
            )
        )
        .build();

    Iterable<Result<TopNResultValue>> results = Sequences.toList(
        new FinalizeResultsQueryRunner(
            factory.createRunner(segment),
            factory.getToolchest()
        ).run(query, Maps.newHashMap()),
        Lists.newArrayList()
    );

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            TIME,
            new TopNResultValue(
                ImmutableList.of(
                    ImmutableMap.<String, Object>of(
                        VISITOR_ID, "00000002",
                        "UC", 2,
                        "UV", 1,
                        "UV2", 1,
                        "count", 2L
                    ),
                    ImmutableMap.<String, Object>of(
                        VISITOR_ID, "00000000",
                        "UC", 1,
                        "UV", 1,
                        "UV2", 1,
                        "count", 1L
                    )
                )
            )
        )
    );
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testGroupBy() throws Exception
  {
    final StupidPool<ByteBuffer> pool = new StupidPool<>(
        new Supplier<ByteBuffer>()
        {
          @Override
          public ByteBuffer get()
          {
            return ByteBuffer.allocate(1024 * 1024);
          }
        }
    );

    final GroupByQueryConfig config = new GroupByQueryConfig();
    config.setMaxIntermediateRows(10000);

    final Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(config);
    final GroupByQueryEngine engine = new GroupByQueryEngine(configSupplier, pool);

    final GroupByQueryRunnerFactory factory = new GroupByQueryRunnerFactory(
        engine,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        configSupplier,
        new GroupByQueryQueryToolChest(
            configSupplier, new DefaultObjectMapper(), engine, TestQueryRunners.pool,
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
        ),
        TestQueryRunners.pool
    );

    GroupByQuery query = GroupByQuery.builder()
                                     .setDataSource(QueryRunnerTestHelper.dataSource)
                                     .setGranularity(QueryRunnerTestHelper.allGran)
                                     .setInterval(QueryRunnerTestHelper.fullOnInterval)
                                     .setDimensions(
                                         ImmutableList.<DimensionSpec>of(
                                             new DefaultDimensionSpec(
                                                 VISITOR_ID,
                                                 VISITOR_ID
                                             )
                                         )
                                     )
                                     .setLimitSpec(
                                         new DefaultLimitSpec(
                                             ImmutableList.of(
                                                 new OrderByColumnSpec(
                                                     VISITOR_ID,
                                                     OrderByColumnSpec.Direction.DESCENDING,
                                                     null
                                                 )
                                             ),
                                             Integer.MAX_VALUE
                                         )
                                     )
                                     .setAggregatorSpecs(
                                         Lists.newArrayList(
                                             new LongSumAggregatorFactory("count", "count"),
                                             new BitmapAggregatorFactory("UV", VISITOR_ID, false),
                                             new BitmapAggregatorFactory("UV2", VISITOR_BITMAP, true),
                                             new BitmapAggregatorFactory("UC", CLIENT_BITMAP, true)
                                         )
                                     )
                                     .build();

    List<Row> results = Sequences.toList(
        new FinalizeResultsQueryRunner(
            factory.getToolchest().mergeResults(
                factory.mergeRunners(
                    MoreExecutors.sameThreadExecutor(),
                    ImmutableList.of(factory.createRunner(segment))
                )
            ),
            factory.getToolchest()
        ).run(query, Maps.newHashMap()),
        Lists.newArrayList()
    );

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime(0),
            ImmutableMap.<String, Object>of(
                VISITOR_ID, "00000002",
                "UC", 2,
                "UV", 1,
                "UV2", 1,
                "count", 2L
            )
        ),
        new MapBasedRow(
            new DateTime(0),
            ImmutableMap.<String, Object>of(
                VISITOR_ID, "00000000",
                "UC", 1,
                "UV", 1,
                "UV2", 1,
                "count", 1L
            )
        )
    );
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }
}
