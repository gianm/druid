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

package io.druid.segment.realtime.skunkworks;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.nary.BinaryFn;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.common.utils.JodaUtils;
import io.druid.query.DruidMetrics;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.aggregation.MetricManipulationFn;

import javax.annotation.Nullable;

public class SkunkworksQueryToolChest
    extends QueryToolChest<Result<SkunkworksQueryResultValue>, SkunkworksQuery>
{
  @Override
  public QueryRunner<Result<SkunkworksQueryResultValue>> mergeResults(
      final QueryRunner<Result<SkunkworksQueryResultValue>> baseRunner
  )
  {
    return new ResultMergeQueryRunner<Result<SkunkworksQueryResultValue>>(baseRunner)
    {
      @Override
      protected Ordering<Result<SkunkworksQueryResultValue>> makeOrdering(
          final Query<Result<SkunkworksQueryResultValue>> query
      )
      {
        return (Ordering) Ordering.allEqual().nullsFirst();
      }

      @Override
      protected BinaryFn<Result<SkunkworksQueryResultValue>, Result<SkunkworksQueryResultValue>, Result<SkunkworksQueryResultValue>> createMergeFn(
          final Query<Result<SkunkworksQueryResultValue>> query
      )
      {
        return new BinaryFn<Result<SkunkworksQueryResultValue>, Result<SkunkworksQueryResultValue>, Result<SkunkworksQueryResultValue>>()
        {
          @Override
          public Result<SkunkworksQueryResultValue> apply(
              @Nullable Result<SkunkworksQueryResultValue> result1,
              @Nullable Result<SkunkworksQueryResultValue> result2
          )
          {
            if (result1 == null) {
              return result2;
            } else if (result2 == null) {
              return result1;
            } else {
              return new Result<>(
                  JodaUtils.minDateTime(result1.getTimestamp(), result2.getTimestamp()),
                  new SkunkworksQueryResultValue(result1.getValue().getCount() + result2.getValue().getCount())
              );
            }
          }
        };
      }
    };
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(
      final SkunkworksQuery query
  )
  {
    return DruidMetrics.makePartialQueryTimeMetric(query);
  }

  @Override
  public Function<Result<SkunkworksQueryResultValue>, Result<SkunkworksQueryResultValue>> makePreComputeManipulatorFn(
      final SkunkworksQuery query,
      final MetricManipulationFn fn
  )
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<Result<SkunkworksQueryResultValue>> getResultTypeReference()
  {
    return new TypeReference<Result<SkunkworksQueryResultValue>>()
    {
    };
  }
}
