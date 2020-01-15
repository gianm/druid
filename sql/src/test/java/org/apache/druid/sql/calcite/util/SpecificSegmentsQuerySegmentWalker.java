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

package org.apache.druid.sql.calcite.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.io.Closeables;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.FunctionalIterable;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.NoopQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.RetryQueryRunnerConfig;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.expression.LookupEnabledTestExprMacroTable;
import org.apache.druid.query.lookup.LookupReferencesManager;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.query.spec.SpecificSegmentQueryRunner;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.DefaultSegmentWrangler;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.join.DefaultJoinableFactory;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.server.ClientQuerySegmentWalker;
import org.apache.druid.server.SimpleQuerySegmentWalker;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class SpecificSegmentsQuerySegmentWalker implements QuerySegmentWalker, Closeable
{
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final QuerySegmentWalker baseWalker;
  private final JoinableFactory joinableFactory;
  private final Map<String, VersionedIntervalTimeline<String, ReferenceCountingSegment>> timelines = new HashMap<>();
  private final List<Closeable> closeables = new ArrayList<>();
  private final List<DataSegment> segments = new ArrayList<>();

  public SpecificSegmentsQuerySegmentWalker(
      final QueryRunnerFactoryConglomerate conglomerate,
      final LookupReferencesManager lookupManager
  )
  {
    final DefaultSegmentWrangler segmentWrangler = new DefaultSegmentWrangler(lookupManager);

    this.conglomerate = conglomerate;
    this.joinableFactory = new DefaultJoinableFactory(conglomerate, segmentWrangler, lookupManager);
    this.baseWalker = new ClientQuerySegmentWalker(
        new NoopServiceEmitter(),
        new TableScanWalker(),
        new SimpleQuerySegmentWalker(
            conglomerate,
            segmentWrangler,
            joinableFactory
        ),
        new QueryToolChestWarehouse()
        {
          @Override
          public <T, QueryType extends Query<T>> QueryToolChest<T, QueryType> getToolChest(final QueryType query)
          {
            return conglomerate.findFactory(query).getToolchest();
          }
        },
        new RetryQueryRunnerConfig(),
        TestHelper.makeJsonMapper(),
        new ServerConfig(),
        null,
        new CacheConfig()
        {
          @Override
          public boolean isPopulateCache()
          {
            return false;
          }

          @Override
          public boolean isUseCache()
          {
            return false;
          }

          @Override
          public boolean isPopulateResultLevelCache()
          {
            return false;
          }

          @Override
          public boolean isUseResultLevelCache()
          {
            return false;
          }
        }
    );
  }

  // TODO(gianm): Remove constructor?
  public SpecificSegmentsQuerySegmentWalker(final QueryRunnerFactoryConglomerate conglomerate)
  {
    this(
        conglomerate,
        LookupEnabledTestExprMacroTable.createTestLookupReferencesManager(ImmutableMap.of())
    );
  }

  private class TableScanWalker implements QuerySegmentWalker
  {
    @Override
    public <T> QueryRunner<T> getQueryRunnerForIntervals(final Query<T> query, final Iterable<Interval> intervals)
    {
      final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(query.getDataSource());

      if (!analysis.isTableScanBased()) {
        throw new ISE("Cannot handle datasource: %s", query.getDataSource());
      }

      final String dataSourceName = ((TableDataSource) analysis.getBaseDataSource()).getName();

      FunctionalIterable<SegmentDescriptor> segmentDescriptors = FunctionalIterable
          .create(intervals)
          .transformCat(interval -> getSegmentsForTable(dataSourceName, interval))
          .transform(WindowedSegment::getDescriptor);

      return getQueryRunnerForSegments(query, segmentDescriptors);
    }

    @Override
    public <T> QueryRunner<T> getQueryRunnerForSegments(final Query<T> query, final Iterable<SegmentDescriptor> specs)
    {
      final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);

      if (factory == null) {
        throw new ISE("Unknown query type[%s].", query.getClass());
      }

      final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(query.getDataSource());

      if (!analysis.isTableScanBased()) {
        throw new ISE("Cannot handle datasource: %s", query.getDataSource());
      }

      final String dataSourceName = ((TableDataSource) analysis.getBaseDataSource()).getName();

      final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();
      final Function<Segment, Segment> segmentMapFn = analysis.getBaseSegmentMapFn(joinableFactory);

      final QueryRunner<T> baseRunner = new FinalizeResultsQueryRunner<>(
          toolChest.postMergeQueryDecoration(
              toolChest.mergeResults(
                  toolChest.preMergeQueryDecoration(
                      makeBaseTableRunner(toolChest, factory, getSegmentsForTable(dataSourceName, specs), segmentMapFn)
                  )
              )
          ),
          toolChest
      );

      // Wrap baseRunner in a runner that rewrites the QuerySegmentSpec to mention the specific segments.
      // This mimics what CachingClusteredClient on the Broker does, and is required for certain queries (like Scan)
      // to function properly.
      final MultipleSpecificSegmentSpec querySegmentSpec = new MultipleSpecificSegmentSpec(Lists.newArrayList(specs));
      return (theQuery, responseContext) -> baseRunner.run(
          theQuery.withQuerySegmentSpec(querySegmentSpec),
          responseContext
      );
    }
  }

  public SpecificSegmentsQuerySegmentWalker add(
      final DataSegment descriptor,
      final QueryableIndex index
  )
  {
    final Segment segment = new QueryableIndexSegment(index, descriptor.getId());
    final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline = timelines
        .computeIfAbsent(descriptor.getDataSource(), datasource -> new VersionedIntervalTimeline<>(Ordering.natural()));
    timeline.add(
        descriptor.getInterval(),
        descriptor.getVersion(),
        descriptor.getShardSpec().createChunk(ReferenceCountingSegment.wrapSegment(segment, descriptor.getShardSpec()))
    );
    segments.add(descriptor);
    closeables.add(index);
    return this;
  }

  public List<DataSegment> getSegments()
  {
    return segments;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(
      final Query<T> query,
      final Iterable<Interval> intervals
  )
  {
    return baseWalker.getQueryRunnerForIntervals(query, intervals);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(
      final Query<T> query,
      final Iterable<SegmentDescriptor> specs
  )
  {
    return baseWalker.getQueryRunnerForSegments(query, specs);
  }

  @Override
  public void close() throws IOException
  {
    for (Closeable closeable : closeables) {
      Closeables.close(closeable, true);
    }
  }

  private List<WindowedSegment> getSegmentsForTable(final String dataSource, final Interval interval)
  {
    final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline = timelines.get(dataSource);

    if (timeline == null) {
      return Collections.emptyList();
    } else {
      final List<WindowedSegment> retVal = new ArrayList<>();

      for (TimelineObjectHolder<String, ReferenceCountingSegment> holder : timeline.lookup(interval)) {
        for (PartitionChunk<ReferenceCountingSegment> chunk : holder.getObject()) {
          retVal.add(new WindowedSegment(chunk.getObject(), holder.getInterval()));
        }
      }

      return retVal;
    }
  }

  private List<WindowedSegment> getSegmentsForTable(final String dataSource, final Iterable<SegmentDescriptor> specs)
  {
    final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline = timelines.get(dataSource);

    if (timeline == null) {
      return Collections.emptyList();
    } else {
      final List<WindowedSegment> retVal = new ArrayList<>();

      for (SegmentDescriptor spec : specs) {
        final PartitionHolder<ReferenceCountingSegment> entry = timeline.findEntry(
            spec.getInterval(),
            spec.getVersion()
        );
        retVal.add(new WindowedSegment(entry.getChunk(spec.getPartitionNumber()).getObject(), spec.getInterval()));
      }

      return retVal;
    }
  }

  private <T> QueryRunner<T> makeBaseTableRunner(
      final QueryToolChest<T, Query<T>> toolChest,
      final QueryRunnerFactory<T, Query<T>> factory,
      final Iterable<WindowedSegment> segments,
      final Function<Segment, Segment> segmentMapFn
  )
  {
    final List<WindowedSegment> segmentsList = Lists.newArrayList(segments);

    if (segmentsList.isEmpty()) {
      // TODO(gianm): Not correct for right join
      return new NoopQueryRunner<>();
    }

    return new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            factory.mergeRunners(
                Execs.directExecutor(),
                FunctionalIterable
                    .create(segmentsList)
                    .transform(
                        segment ->
                            new SpecificSegmentQueryRunner<>(
                                factory.createRunner(segmentMapFn.apply(segment.getSegment())),
                                new SpecificSegmentSpec(segment.getDescriptor())
                            )
                    )
            )
        ),
        toolChest
    );
  }

  public static class WindowedSegment
  {
    private final Segment segment;
    private final Interval interval;

    public WindowedSegment(Segment segment, Interval interval)
    {
      this.segment = segment;
      this.interval = interval;
      Preconditions.checkArgument(segment.getId().getInterval().contains(interval));
    }

    public Segment getSegment()
    {
      return segment;
    }

    public Interval getInterval()
    {
      return interval;
    }

    public SegmentDescriptor getDescriptor()
    {
      return new SegmentDescriptor(interval, segment.getId().getVersion(), segment.getId().getPartitionNum());
    }
  }
}
