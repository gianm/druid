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

package io.druid.query.groupby.epinephelinae;

import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.nary.BinaryFn;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.AllGranularity;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import org.joda.time.DateTime;

import java.util.Map;

/**
 * Expects to run over a stream of MapBasedRows that are ordered according to the GroupByQuery's result
 * ordering (time, then dimensions lexicographically one by one).
 */
public class EpiGroupByStreamMergingQueryRunner extends ResultMergeQueryRunner<Row>
{
  public EpiGroupByStreamMergingQueryRunner(
      final QueryRunner<Row> baseRunner
  )
  {
    super(baseRunner);
  }

  @Override
  protected Ordering<Row> makeOrdering(Query<Row> query)
  {
    return ((GroupByQuery) query).getRowOrdering(true);
  }

  @Override
  protected BinaryFn<Row, Row, Row> createMergeFn(final Query<Row> query0)
  {
    final GroupByQuery query = (GroupByQuery) query0;

    return new BinaryFn<Row, Row, Row>()
    {
      @Override
      public Row apply(final Row arg1, final Row arg2)
      {
        if (arg1 == null) {
          return arg2;
        } else if (arg2 == null) {
          return arg1;
        }

        final Map<String, Object> newMap = Maps.newHashMapWithExpectedSize(
            query.getDimensions().size()
            + query.getAggregatorSpecs().size()
        );

        // Add dimensions
        for (DimensionSpec dimension : query.getDimensions()) {
          newMap.put(dimension.getOutputName(), arg1.getRaw(dimension.getOutputName()));
        }

        // Add aggregations
        for (AggregatorFactory aggregatorFactory : query.getAggregatorSpecs()) {
          newMap.put(
              aggregatorFactory.getName(),
              aggregatorFactory.combine(
                  arg1.getRaw(aggregatorFactory.getName()),
                  arg2.getRaw(aggregatorFactory.getName())
              )
          );
        }

        return new MapBasedRow(adjustTimestamp(query, arg1), newMap);
      }
    };
  }

  private static DateTime adjustTimestamp(final GroupByQuery query, final Row row)
  {
    if (query.getGranularity() instanceof AllGranularity) {
      return row.getTimestamp();
    } else {
      return query.getGranularity().toDateTime(query.getGranularity().truncate(row.getTimestamp().getMillis()));
    }
  }
}
