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

package io.druid.query.groupby.strategy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import com.metamx.common.guava.Sequence;
import io.druid.collections.BlockingPool;
import io.druid.collections.StupidPool;
import io.druid.data.input.Row;
import io.druid.guice.annotations.Global;
import io.druid.guice.annotations.Merging;
import io.druid.query.QueryRunner;
import io.druid.query.QueryWatcher;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.epinephelinae.EpiGroupByMergingQueryRunner;
import io.druid.query.groupby.epinephelinae.EpiGroupByQueryEngine;
import io.druid.query.groupby.epinephelinae.EpiGroupByStreamMergingQueryRunner;
import io.druid.segment.StorageAdapter;

import java.nio.ByteBuffer;
import java.util.Map;

public class EpiGroupByStrategy implements GroupByStrategy
{
  private final StupidPool<ByteBuffer> bufferPool;
  private final BlockingPool<ByteBuffer> mergeBufferPool;
  private final QueryWatcher queryWatcher;

  @Inject
  public EpiGroupByStrategy(
      @Global StupidPool<ByteBuffer> bufferPool,
      @Merging BlockingPool<ByteBuffer> mergeBufferPool,
      QueryWatcher queryWatcher
  )
  {
    this.bufferPool = bufferPool;
    this.mergeBufferPool = mergeBufferPool;
    this.queryWatcher = queryWatcher;
  }

  @Override
  public Sequence<Row> mergeResults(
      QueryRunner<Row> baseRunner,
      GroupByQuery query,
      Map<String, Object> responseContext
  )
  {
    final ResultMergeQueryRunner<Row> mergingQueryRunner = new EpiGroupByStreamMergingQueryRunner(
        baseRunner,
        query.getPostAggregatorSpecs()
    );

    return query.applyLimit(
        mergingQueryRunner.run(
            new GroupByQuery(
                query.getDataSource(),
                query.getQuerySegmentSpec(),
                query.getDimFilter(),
                query.getGranularity(),
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
                    "finalize", false
                )
            ),
            responseContext
        )
    );
  }

  @Override
  public QueryRunner<Row> mergeRunners(
      ListeningExecutorService exec,
      Iterable<QueryRunner<Row>> queryRunners
  )
  {
    return new EpiGroupByMergingQueryRunner(exec, queryWatcher, queryRunners, mergeBufferPool);
  }

  @Override
  public Sequence<Row> process(
      GroupByQuery query,
      StorageAdapter storageAdapter
  )
  {
    return EpiGroupByQueryEngine.process(query, storageAdapter, bufferPool);
  }
}
