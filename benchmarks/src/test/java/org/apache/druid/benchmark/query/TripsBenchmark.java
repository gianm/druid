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

package org.apache.druid.benchmark.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.NoopEscalator;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Benchmark that tests various SQL queries.
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsPrepend = "-Djava.library.path=processing/target")
@Warmup(iterations = 15)
@Measurement(iterations = 25)
public class TripsBenchmark
{
  static {
    NullHandling.initializeForTests();
    Calcites.setSystemProperties();
  }

  private static final List<String> QUERIES = ImmutableList.of(
      "SELECT cab_type, count(*) FROM trips GROUP BY cab_type",
      "SELECT passenger_count, avg(total_amount) FROM trips GROUP BY passenger_count",
      "SELECT passenger_count,\n"
      + "       floor(__time to year),\n"
      + "       count(*)\n"
      + "FROM trips\n"
      + "GROUP BY 1, 2",
      "SELECT passenger_count,\n"
      + "       floor(__time to year),\n"
      + "       round(trip_distance),\n"
      + "       count(*)\n"
      + "FROM trips\n"
      + "GROUP BY 1, 2, 3\n"
      + "ORDER BY 2, 4 DESC"
  );

  @Param({"force"})
  private String vectorize;

  @Param({"1"})
  private String query;

  @MonotonicNonNull
  private PlannerFactory plannerFactory;

  private final Closer closer = Closer.create();

  @Setup(Level.Trial)
  public void setup() throws IOException
  {
    final DataSegment dataSegment = DataSegment.builder()
                                               .dataSource("trips")
                                               .interval(Intervals.of("2014-07-01/P1M"))
                                               .version("1")
                                               .shardSpec(new LinearShardSpec(0))
                                               .size(0)
                                               .build();

    final PlannerConfig plannerConfig = new PlannerConfig();
    final QueryableIndex index = new IndexIO(
        TestHelper.makeJsonMapper(),
        new ColumnConfig()
        {
          @Override
          public int columnCacheSizeBytes()
          {
            return 0;
          }
        }
    ).loadIndex(new File("/Users/gian/Documents/implytesting/trips"));
    closer.register(index);

    final QueryRunnerFactoryConglomerate conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(closer);

    final SpecificSegmentsQuerySegmentWalker walker = new SpecificSegmentsQuerySegmentWalker(conglomerate).add(
        dataSegment,
        index
    );
    closer.register(walker);

    final SchemaPlus rootSchema =
        CalciteTests.createMockRootSchema(conglomerate, walker, plannerConfig, AuthTestUtils.TEST_AUTHORIZER_MAPPER);
    plannerFactory = new PlannerFactory(
        rootSchema,
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        CalciteTests.createOperatorTable(),
        CalciteTests.createExprMacroTable(),
        plannerConfig,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        CalciteTests.getJsonMapper(),
        CalciteTests.DRUID_SCHEMA_NAME
    );
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception
  {
    closer.close();
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OperationsPerInvocation(5_000_000)
  public void querySql(Blackhole blackhole) throws Exception
  {
    final Map<String, Object> context = ImmutableMap.of("vectorize", vectorize);
    final AuthenticationResult authenticationResult = NoopEscalator.getInstance()
                                                                   .createEscalatedAuthenticationResult();

    try (final DruidPlanner planner = plannerFactory.createPlanner(context, ImmutableList.of(), authenticationResult)) {
      final PlannerResult plannerResult = planner.plan(QUERIES.get(Integer.parseInt(query)));
      final Sequence<Object[]> resultSequence = plannerResult.run();
      final Object[] lastRow = resultSequence.accumulate(null, (accumulated, in) -> in);
      blackhole.consume(lastRow);
    }
  }
}
