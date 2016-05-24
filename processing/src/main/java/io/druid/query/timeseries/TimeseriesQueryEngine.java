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

package io.druid.query.timeseries;

import com.google.common.base.Function;
import com.google.inject.Inject;
import com.metamx.common.guava.ResourceClosingSequence;
import com.metamx.common.guava.Sequence;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidPool;
import io.druid.guice.annotations.Global;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.VectorAggregator;
import io.druid.segment.Cursor;
import io.druid.segment.SegmentMissingException;
import io.druid.segment.StorageAdapter;
import io.druid.segment.filter.Filters;

import java.nio.ByteBuffer;
import java.util.List;

/**
 */
public class TimeseriesQueryEngine
{
  private static final int VECTOR_SIZE = 64;

  private final StupidPool<ByteBuffer> bufferPool;

  public TimeseriesQueryEngine()
  {
    // TODO(gianm): This is here because I'm too lazy to fix tests
    this.bufferPool = null;
  }

  @Inject
  public TimeseriesQueryEngine(
      @Global StupidPool<ByteBuffer> bufferPool
  )
  {
    this.bufferPool = bufferPool;
  }

  public Sequence<Result<TimeseriesResultValue>> process(final TimeseriesQuery query, final StorageAdapter adapter)
  {
    if (adapter == null) {
      throw new SegmentMissingException(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final boolean skipEmptyBuckets = query.isSkipEmptyBuckets();
    final List<AggregatorFactory> aggregatorSpecs = query.getAggregatorSpecs();

    int runningOffset = 0;
    final int[] aggregatorOffsets = new int[aggregatorSpecs.size()];
    for (int i = 0; i < aggregatorOffsets.length; i++) {
      aggregatorOffsets[i] = runningOffset;
      runningOffset += aggregatorSpecs.get(i).getMaxIntermediateSize();
    }

    final ResourceHolder<ByteBuffer> bufferHolder = bufferPool.take();

    return new ResourceClosingSequence<>(
        QueryRunnerHelper.makeCursorBasedQuery(
            adapter,
            query.getQuerySegmentSpec().getIntervals(),
            Filters.toFilter(query.getDimensionsFilter()),
            query.isDescending(),
            VECTOR_SIZE,
            query.getGranularity(),
            new Function<Cursor, Result<TimeseriesResultValue>>()
            {
              @Override
              public Result<TimeseriesResultValue> apply(Cursor cursor)
              {
                if (skipEmptyBuckets && cursor.isDone()) {
                  return null;
                }

                final VectorAggregator[] aggregators = new VectorAggregator[aggregatorSpecs.size()];
                for (int i = 0; i < aggregatorSpecs.size(); i++) {
                  aggregators[i] = aggregatorSpecs.get(i).factorizeVectored(cursor);
                }

                final ByteBuffer buffer = bufferHolder.get();

                try {
                  while (!cursor.isDone()) {
                    for (int i = 0; i < aggregatorSpecs.size(); i++) {
                      aggregators[i].aggregate(buffer, aggregatorOffsets[i], null, 0);
                    }
                    cursor.advance();
                  }

                  TimeseriesResultBuilder bob = new TimeseriesResultBuilder(cursor.getTime());

                  for (int i = 0; i < aggregatorSpecs.size(); i++) {
                    bob.addMetric(aggregatorSpecs.get(i).getName(), aggregators[i].get(buffer, aggregatorOffsets[i]));
                  }

                  return bob.build();
                }
                finally {
                  // cleanup
                  for (VectorAggregator agg : aggregators) {
                    agg.close();
                  }
                }
              }
            }
        ),
        bufferHolder
    );
  }
}
