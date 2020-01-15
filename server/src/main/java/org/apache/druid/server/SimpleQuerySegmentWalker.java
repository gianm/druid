/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server;

import com.google.inject.Inject;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentWrangler;
import org.apache.druid.segment.join.JoinableFactory;
import org.joda.time.Interval;

import java.util.function.Function;
import java.util.stream.StreamSupport;

/**
 * Processor that computes "standard" Druid scan-based queries, single-threaded.
 */
public class SimpleQuerySegmentWalker implements QuerySegmentWalker
{
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final SegmentWrangler segmentWrangler;
  private final JoinableFactory joinableFactory;

  @Inject
  public SimpleQuerySegmentWalker(
      QueryRunnerFactoryConglomerate conglomerate,
      SegmentWrangler segmentWrangler,
      JoinableFactory joinableFactory
  )
  {
    this.conglomerate = conglomerate;
    this.segmentWrangler = segmentWrangler;
    this.joinableFactory = joinableFactory;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(final Query<T> query, final Iterable<Interval> intervals)
  {
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(query.getDataSource());

    if (!analysis.isScanBased()) {
      throw new IAE("Not a scan-based dataSource: %s", analysis.getDataSource());
    }

    // TODO(gianm): Less crappy code.
    // TODO(gianm): Need to measure CPU use for joinable factorying.
    final QueryRunnerFactory<T, Query<T>> queryRunnerFactory = conglomerate.findFactory(query);
    final Iterable<Segment> segments = segmentWrangler.getSegmentsForIntervals(analysis.getBaseDataSource(), intervals);
    final Function<Segment, Segment> segmentMapFn = analysis.getBaseSegmentMapFn(joinableFactory);

    return queryRunnerFactory.mergeRunners(
        Execs.directExecutor(),
        () -> StreamSupport.stream(segments.spliterator(), false)
                           .map(segmentMapFn)
                           .map(queryRunnerFactory::createRunner).iterator()
    );
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(final Query<T> query, final Iterable<SegmentDescriptor> specs)
  {
    throw new ISE("Cannot run with specific segments");
  }
}
