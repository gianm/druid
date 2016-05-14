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

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.nary.BinaryFn;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularity;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

/**
 * Expects to run over a stream of MapBasedRows that are ordered according to the GroupByQuery's result
 * ordering (time, then dimensions lexicographically one by one).
 *
 * Will return a sequence of merged MapBasedRows, possibly with timestamps fudged (in the manner expected
 * of groupBy queries) and postAggregators applied.
 */
public class EpiGroupByStreamMergingQueryRunner extends ResultMergeQueryRunner<Row>
{
  private final List<PostAggregator> postAggregators;

  public EpiGroupByStreamMergingQueryRunner(
      final QueryRunner<Row> baseRunner,
      final List<PostAggregator> postAggregators
  )
  {
    super(baseRunner);
    this.postAggregators = postAggregators;
  }

  @Override
  public Sequence<Row> doRun(
      final QueryRunner<Row> baseRunner,
      final Query<Row> query0,
      final Map<String, Object> context
  )
  {
    final GroupByQuery query = (GroupByQuery) query0;
    final Sequence<Row> result = super.doRun(baseRunner, query, context);

    // use gran.iterable instead of gran.truncate for fudging timestamps, so that
    // AllGranularity returns timeStart instead of Long.MIN_VALUE
    final QueryGranularity queryGranularity = query.getGranularity();
    final long timeStart = query.getIntervals().get(0).getStartMillis();
    final DateTime granularityTimeStart = new DateTime(
        queryGranularity.iterable(timeStart, timeStart + 1).iterator().next()
    );

    final boolean hasPostAggregations = !postAggregators.isEmpty();

    return Sequences.map(
        result,
        new Function<Row, Row>()
        {
          @Override
          public Row apply(final Row row)
          {
            final Map<String, Object> newMap;

            // Apply postAggregators, maybe.
            if (hasPostAggregations) {
              newMap = Maps.newHashMap(((MapBasedRow) row).getEvent());

              for (PostAggregator postAggregator : postAggregators) {
                newMap.put(postAggregator.getName(), postAggregator.compute(newMap));
              }
            } else {
              newMap = ((MapBasedRow) row).getEvent();
            }

            // Fudge timestamps and return row.
            if (granularityTimeStart.getMillis() > queryGranularity.truncate(row.getTimestampFromEpoch())) {
              return new MapBasedRow(granularityTimeStart, newMap);
            } else {
              return hasPostAggregations ? new MapBasedRow(row.getTimestamp(), newMap) : row;
            }
          }
        }
    );
  }

  @Override
  protected Ordering<Row> makeOrdering(Query<Row> query)
  {
    return query.getResultOrdering();
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

        return new MapBasedRow(arg1.getTimestamp(), newMap);
      }
    };
  }
}
