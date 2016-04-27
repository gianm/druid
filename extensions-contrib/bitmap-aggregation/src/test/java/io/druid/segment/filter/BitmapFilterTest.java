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

package io.druid.segment.filter;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.common.Pair;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.bitmap.BitmapAggregationModule;
import io.druid.query.aggregation.bitmap.BitmapAggregatorFactory;
import io.druid.query.filter.BitmapDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.segment.IndexBuilder;
import io.druid.segment.StorageAdapter;
import io.druid.segment.incremental.IncrementalIndexSchema;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class BitmapFilterTest extends BaseFilterTest
{
  private static final String TIMESTAMP_COLUMN = "timestamp";

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIMESTAMP_COLUMN, "iso", new DateTime("2000")),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim0", "dim1")),
              null,
              null
          )
      )
  );

  private static final List<InputRow> ROWS = ImmutableList.of(
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "0", "dim1", "00000000")),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "1", "dim1", "00000001")),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "2", "dim1", "00000002")),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "3", "dim1", "00000003")),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "4", "dim1", "00000004")),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "5", "dim1", "00000005"))
  );

  public BitmapFilterTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
      boolean optimize
  )
  {
    super(
        ROWS,
        indexBuilder.schema(
            new IncrementalIndexSchema.Builder()
                .withMetrics(
                    new AggregatorFactory[]{new BitmapAggregatorFactory("bitmap", "dim1", false)}
                )
                .build()
        ),
        finisher,
        optimize
    );

    BitmapAggregationModule module = new BitmapAggregationModule();
    module.configure(null);
  }

  @Test
  public void testSingleValueStringColumn()
  {
    assertFilterMatches(new BitmapDimFilter("dim1", makeBitmap(), false), ImmutableList.<String>of());
    assertFilterMatches(new BitmapDimFilter("dim1", makeBitmap(0, 2), false), ImmutableList.of("0", "2"));
    assertFilterMatches(new BitmapDimFilter("dim1", makeBitmap(3), false), ImmutableList.of("3"));
  }

  private String makeBitmap(Integer... ints)
  {
    final MutableBitmap bitmap = BitmapAggregatorFactory.BITMAP_FACTORY.makeEmptyMutableBitmap();
    for (Integer n : ints) {
      bitmap.add(n);
    }
    final ImmutableBitmap immutableBitmap = BitmapAggregatorFactory.BITMAP_FACTORY.makeImmutableBitmap(bitmap);
    return BaseEncoding.base64().encode(
        BitmapAggregatorFactory.BITMAP_SERDE_FACTORY.getObjectStrategy().toBytes(immutableBitmap)
    );
  }

  private void assertFilterMatches(
      final DimFilter filter,
      final List<String> expectedRows
  )
  {
    Assert.assertEquals(filter.toString(), expectedRows, selectColumnValuesMatchingFilter(filter, "dim0"));
    Assert.assertEquals(filter.toString(), expectedRows.size(), selectCountUsingFilteredAggregator(filter));
  }
}
