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

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.common.ISE;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.emitter.EmittingLogger;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.query.BySegmentQueryRunner;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QueryToolChest;
import io.druid.query.ReportTimelineMissingSegmentQueryRunner;
import io.druid.query.SegmentDescriptor;
import io.druid.query.spec.SpecificSegmentQueryRunner;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.realtime.appenderator.SegmentNotWritableException;
import io.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;

public class SkunkworksAppenderator implements Appenderator
{
  private static final EmittingLogger log = new EmittingLogger(SkunkworksAppenderator.class);

  private final DataSchema schema;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final ExecutorService queryExecutorService;
  private final Map<SegmentIdentifier, SkunkworksSegment> segments = Maps.newHashMap();
  private final VersionedIntervalTimeline<String, SkunkworksSegment> timeline = new VersionedIntervalTimeline<>(
      Ordering.natural()
  );

  public SkunkworksAppenderator(
      DataSchema schema,
      QueryRunnerFactoryConglomerate conglomerate,
      ExecutorService queryExecutorService
  )
  {
    this.schema = schema;
    this.conglomerate = conglomerate;
    this.queryExecutorService = queryExecutorService;
  }

  @Override
  public String getDataSource()
  {
    return schema.getDataSource();
  }

  @Override
  public Object startJob()
  {
    // TODO - should reload data from disk
    return null;
  }

  @Override
  public int add(
      final SegmentIdentifier identifier,
      final InputRow row,
      final Supplier<Committer> committerSupplier
  ) throws IndexSizeExceededException, SegmentNotWritableException
  {
    synchronized (segments) {
      SkunkworksSegment segment = segments.get(identifier);
      if (segment == null) {
        segment = new SkunkworksSegment(identifier, new LinkedBlockingDeque<InputRow>());
        segments.put(identifier, segment);
        timeline.add(
            identifier.getInterval(),
            identifier.getVersion(),
            identifier.getShardSpec().createChunk(segment)
        );
      }

      segment.getRows().add(row);
      return segment.getRows().size();
    }
  }

  @Override
  public List<SegmentIdentifier> getSegments()
  {
    synchronized (segments) {
      return ImmutableList.copyOf(segments.keySet());
    }
  }

  @Override
  public int getRowCount(SegmentIdentifier identifier)
  {
    synchronized (segments) {
      final SkunkworksSegment segment = segments.get(identifier);
      if (segment == null) {
        throw new ISE("no such segment");
      }
      return segment.getRows().size();
    }
  }

  @Override
  public void clear() throws InterruptedException
  {
    synchronized (segments) {
      for (Map.Entry<SegmentIdentifier, SkunkworksSegment> entry : segments.entrySet()) {
        timeline.remove(
            entry.getKey().getInterval(),
            entry.getKey().getVersion(),
            entry.getKey().getShardSpec().createChunk(entry.getValue())
        );
      }
      segments.clear();
    }
  }

  @Override
  public ListenableFuture<?> drop(SegmentIdentifier identifier)
  {
    synchronized (segments) {
      final SkunkworksSegment segment = segments.get(identifier);
      if (segment != null) {
        timeline.remove(
            identifier.getInterval(),
            identifier.getVersion(),
            identifier.getShardSpec().createChunk(segment)
        );
        segments.remove(identifier);
      }
      return Futures.immediateFuture(null);
    }
  }

  @Override
  public ListenableFuture<Object> persistAll(Committer committer)
  {
    // TODO - should persist any un-persisted data to disk in a background thread
    return Futures.immediateFuture(null);
  }

  @Override
  public ListenableFuture<SegmentsAndMetadata> push(
      final List<SegmentIdentifier> identifiers,
      final Committer committer
  )
  {
    // TODO - should persist to disk and push data to deep storage in a background thread
    return Futures.immediateFuture(
        new SegmentsAndMetadata(
            ImmutableList.<DataSegment>of(),
            committer.getMetadata()
        )
    );
  }

  @Override
  public void close()
  {
    // Nothing to do
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(
      final Query<T> query,
      final Iterable<Interval> intervals
  )
  {
    final List<SegmentDescriptor> specs = Lists.newArrayList();

    synchronized (segments) {
      Iterables.addAll(
          specs,
          FunctionalIterable
              .create(intervals)
              .transformCat(
                  new Function<Interval, Iterable<TimelineObjectHolder<String, SkunkworksSegment>>>()
                  {
                    @Override
                    public Iterable<TimelineObjectHolder<String, SkunkworksSegment>> apply(final Interval interval)
                    {
                      return timeline.lookup(interval);
                    }
                  }
              )
              .transformCat(
                  new Function<TimelineObjectHolder<String, SkunkworksSegment>, Iterable<SegmentDescriptor>>()
                  {
                    @Override
                    public Iterable<SegmentDescriptor> apply(final TimelineObjectHolder<String, SkunkworksSegment> holder)
                    {
                      return FunctionalIterable
                          .create(holder.getObject())
                          .transform(
                              new Function<PartitionChunk<SkunkworksSegment>, SegmentDescriptor>()
                              {
                                @Override
                                public SegmentDescriptor apply(final PartitionChunk<SkunkworksSegment> chunk)
                                {
                                  return new SegmentDescriptor(
                                      holder.getInterval(),
                                      holder.getVersion(),
                                      chunk.getChunkNumber()
                                  );
                                }
                              }
                          );
                    }
                  }
              )
      );
    }

    return getQueryRunnerForSegments(query, specs);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(
      final Query<T> query,
      final Iterable<SegmentDescriptor> specs
  )
  {
    // We only handle one dataSource. Make sure it's in the list of names, then ignore from here on out.
    if (!query.getDataSource().getNames().contains(getDataSource())) {
      log.makeAlert("Received query for unknown dataSource")
         .addData("dataSource", query.getDataSource())
         .emit();
      return new NoopQueryRunner<>();
    }

    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (factory == null) {
      log.makeAlert("Unknown query type, [%s]", query.getClass())
         .addData("dataSource", query.getDataSource())
         .emit();
      return new NoopQueryRunner<>();
    }

    final QueryToolChest<T, Query<T>> toolchest = factory.getToolchest();

    return toolchest.mergeResults(
        factory.mergeRunners(
            queryExecutorService,
            FunctionalIterable
                .create(specs)
                .transform(
                    new Function<SegmentDescriptor, QueryRunner<T>>()
                    {
                      @Override
                      public QueryRunner<T> apply(final SegmentDescriptor descriptor)
                      {
                        final PartitionHolder<SkunkworksSegment> holder = timeline.findEntry(
                            descriptor.getInterval(),
                            descriptor.getVersion()
                        );
                        if (holder == null) {
                          return new ReportTimelineMissingSegmentQueryRunner<>(descriptor);
                        }

                        final PartitionChunk<SkunkworksSegment> chunk = holder.getChunk(descriptor.getPartitionNumber());
                        if (chunk == null) {
                          return new ReportTimelineMissingSegmentQueryRunner<>(descriptor);
                        }

                        final SkunkworksSegment segment = chunk.getObject();

                        return new SpecificSegmentQueryRunner<>(
                            new BySegmentQueryRunner<>(
                                segment.getIdentifier(),
                                descriptor.getInterval().getStart(),
                                factory.createRunner(segment)
                            ),
                            new SpecificSegmentSpec(descriptor)
                        );
                      }
                    }
                )
        )
    );
  }
}
