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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.query.ChainedExecutionQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.segment.Segment;

import java.util.Map;
import java.util.concurrent.ExecutorService;

public class SkunkworksQueryRunnerFactory
    implements QueryRunnerFactory<Result<SkunkworksQueryResultValue>, SkunkworksQuery>
{
  private final QueryWatcher watcher;

  @Inject
  public SkunkworksQueryRunnerFactory(
      QueryWatcher watcher
  )
  {
    this.watcher = watcher;
  }

  @Override
  public QueryRunner<Result<SkunkworksQueryResultValue>> createRunner(final Segment segment)
  {
    return new SkunkworksQueryRunner((SkunkworksSegment) segment);
  }

  @Override
  public QueryRunner<Result<SkunkworksQueryResultValue>> mergeRunners(
      final ExecutorService queryExecutor,
      final Iterable<QueryRunner<Result<SkunkworksQueryResultValue>>> runners
  )
  {
    return new ChainedExecutionQueryRunner<Result<SkunkworksQueryResultValue>>(
        queryExecutor,
        watcher,
        runners
    );
  }

  @Override
  public SkunkworksQueryToolChest getToolchest()
  {
    return new SkunkworksQueryToolChest();
  }

  private static class SkunkworksQueryRunner implements QueryRunner<Result<SkunkworksQueryResultValue>>
  {
    private final SkunkworksSegment segment;

    public SkunkworksQueryRunner(SkunkworksSegment segment)
    {
      this.segment = segment;
    }

    @Override
    public Sequence<Result<SkunkworksQueryResultValue>> run(
        final Query<Result<SkunkworksQueryResultValue>> query,
        final Map<String, Object> responseContext
    )
    {
      // Do something cool. In this case we'll just count the number of rows and return it as a single result.
      return Sequences.simple(
          ImmutableList.of(
              new Result<>(
                  segment.getDataInterval().getStart(),
                  new SkunkworksQueryResultValue(segment.getRows().size())
              )
          )
      );
    }
  }
}
