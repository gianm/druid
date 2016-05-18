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

package io.druid.benchmark;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.granularity.QueryGranularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.groupby.epinephelinae.BufferGrouper;
import io.druid.query.groupby.epinephelinae.EpiGroupByMergingQueryRunner;
import io.druid.query.groupby.epinephelinae.Grouper;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
public class IncrementalIndexAddRowsBenchmark
{
  private IncrementalIndex incIndex;
  private IncrementalIndex incFloatIndex;
  private IncrementalIndex incStrIndex;
  private EpiGroupByMergingQueryRunner.GroupByMergingColumnSelectorFactory columnSelectorFactory;
  private Grouper<EpiGroupByMergingQueryRunner.GroupByMergingKey> grouper;
  private static AggregatorFactory[] aggs;
  static final int dimensionCount = 8;
  private Random rng;
  static final int maxRows = 250000;

  private ArrayList<InputRow> longRows = new ArrayList<InputRow>();
  private ArrayList<InputRow> floatRows = new ArrayList<InputRow>();
  private ArrayList<InputRow> stringRows = new ArrayList<InputRow>();


  static {
    final ArrayList<AggregatorFactory> ingestAggregatorFactories = new ArrayList<>(dimensionCount + 1);
    ingestAggregatorFactories.add(new CountAggregatorFactory("rows"));
    for (int i = 0; i < dimensionCount; ++i) {
      ingestAggregatorFactories.add(
          new LongSumAggregatorFactory(
              String.format("sumResult%s", i),
              String.format("Dim_%s", i)
          )
      );
      ingestAggregatorFactories.add(
          new DoubleSumAggregatorFactory(
              String.format("doubleSumResult%s", i),
              String.format("Dim_%s", i)
          )
      );
    }
    aggs = ingestAggregatorFactories.toArray(new AggregatorFactory[0]);
  }

  private MapBasedInputRow getLongRow(long timestamp, int rowID, int dimensionCount)
  {
    List<String> dimensionList = new ArrayList<String>(dimensionCount);
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    for (int i = 0; i < dimensionCount; i++) {
      String dimName = String.format("Dim_%d", i);
      dimensionList.add(dimName);
      builder.put(dimName, rng.nextLong());
    }
    return new MapBasedInputRow(timestamp, dimensionList, builder.build());
  }

  private MapBasedInputRow getFloatRow(long timestamp, int rowID, int dimensionCount)
  {
    List<String> dimensionList = new ArrayList<String>(dimensionCount);
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    for (int i = 0; i < dimensionCount; i++) {
      String dimName = String.format("Dim_%d", i);
      dimensionList.add(dimName);
      builder.put(dimName, rng.nextFloat());
    }
    return new MapBasedInputRow(timestamp, dimensionList, builder.build());
  }

  private MapBasedInputRow getStringRow(long timestamp, int rowID, int dimensionCount)
  {
    List<String> dimensionList = new ArrayList<String>(dimensionCount);
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    for (int i = 0; i < dimensionCount; i++) {
      String dimName = String.format("Dim_%d", i);
      dimensionList.add(dimName);
      builder.put(dimName, String.valueOf(rng.nextLong()));
    }
    return new MapBasedInputRow(timestamp, dimensionList, builder.build());
  }

  private IncrementalIndex makeIncIndex()
  {
    return new OnheapIncrementalIndex(
        0,
        QueryGranularities.NONE,
        aggs,
        false,
        false,
        true,
        maxRows
    );
  }

  @Setup
  public void setup() throws IOException
  {
    rng = new Random(9999);

    for (int i = 0; i < maxRows; i++) {
      longRows.add(getLongRow(0, i, dimensionCount));
    }

    for (int i = 0; i < maxRows; i++) {
      floatRows.add(getFloatRow(0, i, dimensionCount));
    }

    for (int i = 0; i < maxRows; i++) {
      stringRows.add(getStringRow(0, i, dimensionCount));
    }
  }

  @Setup(Level.Iteration)
  public void setup2() throws IOException
  {
    ;
    incIndex = makeIncIndex();
    incFloatIndex = makeIncIndex();
    incStrIndex = makeIncIndex();
    columnSelectorFactory = new EpiGroupByMergingQueryRunner.GroupByMergingColumnSelectorFactory();
    grouper = new BufferGrouper<>(
        ByteBuffer.allocateDirect(1024 * 1024 * 1024),
        new EpiGroupByMergingQueryRunner.GroupByMergingKeySerde(dimensionCount, Long.MAX_VALUE),
        columnSelectorFactory,
        aggs,
        Integer.MAX_VALUE
    );
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @OperationsPerInvocation(maxRows)
  public void normalLongs(Blackhole blackhole) throws Exception
  {
    for (int i = 0; i < maxRows; i++) {
      InputRow row = longRows.get(i);
      int rv = incIndex.add(row);
      blackhole.consume(rv);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @OperationsPerInvocation(maxRows)
  public void normalFloats(Blackhole blackhole) throws Exception
  {
    for (int i = 0; i < maxRows; i++) {
      InputRow row = floatRows.get(i);
      int rv = incFloatIndex.add(row);
      blackhole.consume(rv);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @OperationsPerInvocation(maxRows)
  public void normalStrings() throws Exception
  {
    for (int i = 0; i < maxRows; i++) {
      InputRow row = stringRows.get(i);
      incStrIndex.add(row);
    }
    System.out.println(Iterators.size(incStrIndex.iterator()));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @OperationsPerInvocation(maxRows)
  public void normalStringsBufferGrouper() throws Exception
  {
    for (int i = 0; i < maxRows; i++) {
      InputRow row = stringRows.get(i);
      columnSelectorFactory.setRow(row);
      final String[] dimensions = new String[dimensionCount];
      for (int j = 0; j < dimensions.length; j++) {
        final Object dimValue = row.getRaw(row.getDimensions().get(j));
        dimensions[j] = Strings.nullToEmpty((String) dimValue);
      }
      if (!grouper.aggregate(new EpiGroupByMergingQueryRunner.GroupByMergingKey(row.getTimestampFromEpoch(), dimensions))) {
        throw new ISE("wtf");
      }
    }
    System.out.println(Iterators.size(grouper.iterator(true)));
  }
}
