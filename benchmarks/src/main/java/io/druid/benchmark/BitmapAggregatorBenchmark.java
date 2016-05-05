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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Ints;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.bitmap.BitmapAggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.data.ArrayBasedIndexedInts;
import io.druid.segment.data.IndexedInts;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class BitmapAggregatorBenchmark
{
  final int rows = 10000000;
  final int maxValue = Integer.MAX_VALUE;
  final int[] theInts = new int[rows];
  final Random random = new Random();

  // cardinality, the rows will contain this many different integers
  @Param({"1000", "100000", "1000000", "5000000", "10000000"})
  int cardinality;

  Map<Integer, String> dict;
  Map<String, Integer> reverseDict;

  Aggregator agg;

  int currRow = 0;

  @Setup
  public void setup()
  {
    dict = Maps.newHashMap();
    reverseDict = Maps.newHashMap();

    for (int i = 0; i < theInts.length; i++) {
      int theInt = (i % cardinality) * (maxValue / cardinality);
      theInts[i] = theInt;
      if (!dict.containsKey(theInt)) {
        final String s = BaseEncoding.base16().encode(Ints.toByteArray(theInt));
        dict.put(theInt, s);
        reverseDict.put(s, theInt);
      }
    }

    Collections.shuffle(Arrays.asList(theInts));

    agg = new BitmapAggregatorFactory("x", "foo", false).factorize(
        new ColumnSelectorFactory()
        {
          @Override
          public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
          {
            return new DimensionSelector()
            {
              @Override
              public IndexedInts getRow()
              {
                return new ArrayBasedIndexedInts(new int[]{theInts[currRow]});
              }

              @Override
              public int getValueCardinality()
              {
                return cardinality;
              }

              @Override
              public String lookupName(int id)
              {
                return dict.get(id);
              }

              @Override
              public int lookupId(String name)
              {
                return reverseDict.get(name);
              }
            };
          }

          @Override
          public FloatColumnSelector makeFloatColumnSelector(String columnName)
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public LongColumnSelector makeLongColumnSelector(String columnName)
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public ObjectColumnSelector makeObjectColumnSelector(String columnName)
          {
            throw new UnsupportedOperationException();
          }
        }
    );
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void aggregateHexStringsAndGetCardinality()
  {
    agg.reset();
    for (currRow = 0; currRow < rows; currRow++) {
      agg.aggregate();
    }

    Preconditions.checkState(((ImmutableBitmap) agg.get()).size() == cardinality);
  }
}
