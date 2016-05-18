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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.nary.BinaryFn;
import io.druid.collections.BlockingPool;
import io.druid.collections.StupidPool;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.AllGranularity;
import io.druid.granularity.QueryGranularity;
import io.druid.guice.annotations.Global;
import io.druid.guice.annotations.Merging;
import io.druid.guice.annotations.Smile;
import io.druid.query.DruidProcessingConfig;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryWatcher;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.strategy.GroupByStrategy;
import io.druid.query.groupby.strategy.GroupByStrategySelector;
import io.druid.segment.StorageAdapter;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;
import java.util.Map;

public class EpiGroupByStrategy implements GroupByStrategy
{
  static final String CTX_KEY_FUDGE_TIMESTAMP = "fudgeTimestamp";

  private final DruidProcessingConfig processingConfig;
  private final Supplier<GroupByQueryConfig> configSupplier;
  private final StupidPool<ByteBuffer> bufferPool;
  private final BlockingPool<ByteBuffer> mergeBufferPool;
  private final ObjectMapper spillMapper;
  private final QueryWatcher queryWatcher;

  @Inject
  public EpiGroupByStrategy(
      DruidProcessingConfig processingConfig,
      Supplier<GroupByQueryConfig> configSupplier,
      @Global StupidPool<ByteBuffer> bufferPool,
      @Merging BlockingPool<ByteBuffer> mergeBufferPool,
      @Smile ObjectMapper spillMapper,
      QueryWatcher queryWatcher
  )
  {
    this.processingConfig = processingConfig;
    this.configSupplier = configSupplier;
    this.bufferPool = bufferPool;
    this.mergeBufferPool = mergeBufferPool;
    this.spillMapper = spillMapper;
    this.queryWatcher = queryWatcher;
  }

  @Override
  public Sequence<Row> mergeResults(
      final QueryRunner<Row> baseRunner,
      final GroupByQuery query,
      final Map<String, Object> responseContext
  )
  {
    final ResultMergeQueryRunner<Row> mergingQueryRunner = new ResultMergeQueryRunner<Row>(baseRunner)
    {
      @Override
      protected Ordering<Row> makeOrdering(Query<Row> queryParam)
      {
        return ((GroupByQuery) queryParam).getRowOrdering(true);
      }

      @Override
      protected BinaryFn<Row, Row, Row> createMergeFn(Query<Row> queryParam)
      {
        return new EpiGroupByBinaryFn((GroupByQuery) queryParam);
      }
    };

    // Fudge timestamp, maybe. Necessary to keep timestamps in sync across partial queries.
    final QueryGranularity gran = query.getGranularity();
    final String fudgeTimestamp;
    if (query.getContextValue(CTX_KEY_FUDGE_TIMESTAMP, "").isEmpty() && gran instanceof AllGranularity) {
      final long timeStart = query.getIntervals().get(0).getStartMillis();
      fudgeTimestamp = String.valueOf(
          new DateTime(gran.iterable(timeStart, timeStart + 1).iterator().next()).getMillis()
      );
    } else {
      fudgeTimestamp = query.getContextValue(CTX_KEY_FUDGE_TIMESTAMP, "");
    }

    return query.applyLimit(
        Sequences.map(
            mergingQueryRunner.run(
                new GroupByQuery(
                    query.getDataSource(),
                    query.getQuerySegmentSpec(),
                    query.getDimFilter(),
                    gran,
                    query.getDimensions(),
                    query.getAggregatorSpecs(),
                    // Don't do post aggs until the end of this method.
                    ImmutableList.<PostAggregator>of(),
                    // Don't do "having" clause until the end of this method.
                    null,
                    null,
                    query.getContext()
                ).withOverriddenContext(
                    ImmutableMap.<String, Object>of(
                        "finalize", false,
                        GroupByStrategySelector.CTX_KEY_STRATEGY, GroupByStrategySelector.STRATEGY_EPINEPHELINAE,
                        CTX_KEY_FUDGE_TIMESTAMP, fudgeTimestamp
                    )
                ),
                responseContext
            ),
            new Function<Row, Row>()
            {
              @Override
              public Row apply(final Row row)
              {
                // Maybe apply postAggregators.

                if (query.getPostAggregatorSpecs().isEmpty()) {
                  return row;
                }

                final Map<String, Object> newMap;

                if (query.getPostAggregatorSpecs().isEmpty()) {
                  newMap = ((MapBasedRow) row).getEvent();
                } else {
                  newMap = Maps.newLinkedHashMap(((MapBasedRow) row).getEvent());

                  for (PostAggregator postAggregator : query.getPostAggregatorSpecs()) {
                    newMap.put(postAggregator.getName(), postAggregator.compute(newMap));
                  }
                }

                return new MapBasedRow(row.getTimestamp(), newMap);
              }
            }
        )
    );
  }

  @Override
  public QueryRunner<Row> mergeRunners(
      ListeningExecutorService exec,
      Iterable<QueryRunner<Row>> queryRunners
  )
  {
    // TODO(gianm): get mapper from somewhere real
    return new EpiGroupByMergingQueryRunner(
        configSupplier.get(),
        exec,
        queryWatcher,
        queryRunners,
        processingConfig.getNumThreads(),
        mergeBufferPool,
        spillMapper
    );
  }

  @Override
  public Sequence<Row> process(
      GroupByQuery query,
      StorageAdapter storageAdapter
  )
  {
    return EpiGroupByQueryEngine.process(query, storageAdapter, bufferPool, configSupplier.get());
  }
}
