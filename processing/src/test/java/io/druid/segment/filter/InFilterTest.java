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
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.InDimFilter;
import io.druid.segment.IndexBuilder;
import io.druid.segment.StorageAdapter;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class InFilterTest extends BaseFilterTest
{
  private static final String TIMESTAMP_COLUMN = "timestamp";

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIMESTAMP_COLUMN, "iso", new DateTime("2000")),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim0", "dim1", "dim2", "dim3")),
              null,
              null
          )
      )
  );

  private static final List<InputRow> ROWS = ImmutableList.of(
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "0", "dim1", "", "dim2", ImmutableList.of("a", "b"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "1", "dim1", "10", "dim2", ImmutableList.of())),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "2", "dim1", "2", "dim2", ImmutableList.of(""))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "3", "dim1", "1", "dim2", ImmutableList.of("a"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "4", "dim1", "def", "dim2", ImmutableList.of("c"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "5", "dim1", "abc"))
  );

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  public InFilterTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher
  )
  {
    super(ROWS, indexBuilder, finisher);
  }

  @Test
  public void testEmptyValueList()
  {
    final InDimFilter dimFilter = new InDimFilter("dim0", ImmutableList.<String>of());

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("the universe does not exist");
    dimFilter.toFilter();
  }

  @Test
  public void testSingleValueStringColumnWithoutNulls()
  {
    assertFilterMatches(new InDimFilter("dim0", L(null)), ImmutableList.<String>of());
    assertFilterMatches(new InDimFilter("dim0", L("")), ImmutableList.<String>of());
    assertFilterMatches(new InDimFilter("dim0", L("0", "1")), ImmutableList.of("0", "1"));
  }

  @Test
  public void testSingleValueStringColumnWithNulls()
  {
    assertFilterMatches(new InDimFilter("dim1", L(null)), ImmutableList.of("0"));
    assertFilterMatches(new InDimFilter("dim1", L("")), ImmutableList.of("0"));
    assertFilterMatches(new InDimFilter("dim1", L(null, "")), ImmutableList.of("0"));
    assertFilterMatches(new InDimFilter("dim1", L("10", "2")), ImmutableList.of("1", "2"));
    assertFilterMatches(new InDimFilter("dim1", L("10", "2", null)), ImmutableList.of("0", "1", "2"));
    assertFilterMatches(new InDimFilter("dim1", L("10", "2", "xxx")), ImmutableList.of("1", "2"));
  }

  @Test
  public void testMultiValueStringColumn()
  {
    assertFilterMatches(new InDimFilter("dim2", L(null)), ImmutableList.of("1", "2", "5"));
    assertFilterMatches(new InDimFilter("dim2", L("")), ImmutableList.of("1", "2", "5"));
    assertFilterMatches(new InDimFilter("dim2", L("a", "b")), ImmutableList.of("0", "3"));
    assertFilterMatches(new InDimFilter("dim2", L("b", "c")), ImmutableList.of("0", "4"));
    assertFilterMatches(new InDimFilter("dim2", L("b", null, "c")), ImmutableList.of("0", "1", "2", "4", "5"));
  }

  @Test
  public void testMissingColumnSpecifiedInDimensionList()
  {
    assertFilterMatches(new InDimFilter("dim3", L(null)), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    assertFilterMatches(new InDimFilter("dim3", L("")), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    assertFilterMatches(new InDimFilter("dim3", L("a", "")), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    assertFilterMatches(new InDimFilter("dim3", L("a", "b")), ImmutableList.<String>of());
  }

  @Test
  public void testMissingColumnNotSpecifiedInDimensionList()
  {
    assertFilterMatches(new InDimFilter("dim4", L(null)), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    assertFilterMatches(new InDimFilter("dim4", L("")), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    assertFilterMatches(new InDimFilter("dim4", L("a", "")), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    assertFilterMatches(new InDimFilter("dim4", L("a", "b")), ImmutableList.<String>of());
  }

  private void assertFilterMatches(
      final DimFilter filter,
      final List<String> expectedRows
  )
  {
    Assert.assertEquals(filter.toString(), expectedRows, selectColumnValuesMatchingFilter(filter, "dim0"));
    Assert.assertEquals(filter.toString(), expectedRows.size(), selectCountUsingFilteredAggregator(filter));
  }

  private List<String> L(String string, String... strings)
  {
    final ArrayList<String> list = Lists.newArrayListWithCapacity(1 + strings.length);
    list.add(string);
    Collections.addAll(list, strings);
    return list;
  }
}
