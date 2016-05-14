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
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.ISE;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.collections.BlockingPool;
import io.druid.collections.ResourceHolder;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.query.AbstractPrioritizedCallable;
import io.druid.query.BaseQuery;
import io.druid.query.ChainedExecutionQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryContextKeys;
import io.druid.query.QueryInterruptedException;
import io.druid.query.QueryRunner;
import io.druid.query.QueryWatcher;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.OnheapIncrementalIndex;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class EpiGroupByMergingQueryRunner implements QueryRunner
{
  private static final Logger log = new Logger(EpiGroupByMergingQueryRunner.class);

  private final Iterable<QueryRunner<Row>> queryables;
  private final ListeningExecutorService exec;
  private final QueryWatcher queryWatcher;
  private final BlockingPool<ByteBuffer> mergeBufferPool;

  public EpiGroupByMergingQueryRunner(
      ExecutorService exec,
      QueryWatcher queryWatcher,
      Iterable<QueryRunner<Row>> queryables,
      BlockingPool<ByteBuffer> mergeBufferPool
  )
  {
    this.exec = MoreExecutors.listeningDecorator(exec);
    this.queryWatcher = queryWatcher;
    this.queryables = Iterables.unmodifiableIterable(Iterables.filter(queryables, Predicates.notNull()));
    this.mergeBufferPool = mergeBufferPool;
  }

  @Override
  public Sequence<Row> run(final Query queryParam, final Map responseContext)
  {
    final GroupByQuery query = (GroupByQuery) queryParam;

    if (BaseQuery.getContextBySegment(query, false)) {
      return new ChainedExecutionQueryRunner(exec, queryWatcher, queryables).run(query, responseContext);
    }

    final AggregatorFactory[] combiningAggregatorFactories = new AggregatorFactory[query.getAggregatorSpecs().size()];
    for (int i = 0; i < query.getAggregatorSpecs().size(); i++) {
      combiningAggregatorFactories[i] = query.getAggregatorSpecs().get(i).getCombiningFactory();
    }

    final GroupByMergingKeySerde keySerde = new GroupByMergingKeySerde(query.getDimensions().size());
    final GroupByMergingColumnSelectorFactory columnSelectorFactory = new GroupByMergingColumnSelectorFactory();

    // TODO(gianm): Close grouper when done
    final int totalBufferSize = 1 * 1024 * 1024;

    // TODO(gianm): get concurrency level from processing thread pool size
    final int concurrencyHint = 16;

    // TODO(gianm): Configurable spill dir
    final File spillDirectory = Files.createTempDir();

    final ResourceHolder<ByteBuffer> mergeBuffer;

    try {
      mergeBuffer = mergeBufferPool.take();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }

    return Sequences.withBaggage(
        new BaseSequence<>(
            new BaseSequence.IteratorMaker<Row, CloseableGrouperIterator<GroupByMergingKey, Row>>()
            {
              @Override
              public CloseableGrouperIterator<GroupByMergingKey, Row> make()
              {
                final Grouper<GroupByMergingKey> grouper = new ConcurrentGrouper<>(
                    mergeBuffer.get(),
                    concurrencyHint,
                    spillDirectory,
                    keySerde,
                    columnSelectorFactory,
                    combiningAggregatorFactories
                );

                final Accumulator<Grouper<GroupByMergingKey>, Row> accumulator = new Accumulator<Grouper<GroupByMergingKey>, Row>()
                {
                  @Override
                  public Grouper<GroupByMergingKey> accumulate(
                      final Grouper<GroupByMergingKey> theGrouper,
                      final Row row
                  )
                  {
                    final long timestamp = row.getTimestampFromEpoch();

                    final String[] dimensions = new String[query.getDimensions().size()];
                    for (int i = 0; i < dimensions.length; i++) {
                      final Object dimValue = row.getRaw(query.getDimensions().get(i).getOutputName());
                      dimensions[i] = Strings.nullToEmpty((String) dimValue);
                    }

                    columnSelectorFactory.setRow(row);
                    final boolean didAggregate = theGrouper.aggregate(new GroupByMergingKey(timestamp, dimensions));
                    if (!didAggregate) {
                      // TODO(gianm): Better error message
                      throw new ISE("Grouping resources exhausted");
                    }
                    columnSelectorFactory.setRow(null);

                    return theGrouper;
                  }
                };

                final int priority = BaseQuery.getContextPriority(query, 0);

                ListenableFuture<List<Void>> futures = Futures.allAsList(
                    Lists.newArrayList(
                        Iterables.transform(
                            queryables,
                            new Function<QueryRunner<Row>, ListenableFuture<Void>>()
                            {
                              @Override
                              public ListenableFuture<Void> apply(final QueryRunner<Row> input)
                              {
                                if (input == null) {
                                  throw new ISE("Null queryRunner! Looks to be some segment unmapping action happening");
                                }

                                return exec.submit(
                                    new AbstractPrioritizedCallable<Void>(priority)
                                    {
                                      @Override
                                      public Void call() throws Exception
                                      {
                                        try {
                                          input.run(queryParam, responseContext).accumulate(grouper, accumulator);
                                          return null;
                                        }
                                        catch (QueryInterruptedException e) {
                                          throw Throwables.propagate(e);
                                        }
                                        catch (Exception e) {
                                          log.error(e, "Exception with one of the sequences!");
                                          throw Throwables.propagate(e);
                                        }
                                      }
                                    }
                                );
                              }
                            }
                        )
                    )
                );

                waitForFutureCompletion(query, futures);

                return new CloseableGrouperIterator<>(
                    grouper,
                    new Function<Grouper.Entry<GroupByMergingKey>, Row>()
                    {
                      @Override
                      public Row apply(Grouper.Entry<GroupByMergingKey> entry)
                      {
                        Map<String, Object> theMap = Maps.newHashMap();

                        // Add dimensions.
                        for (int i = 0; i < entry.getKey().getDimensions().length; i++) {
                          theMap.put(
                              query.getDimensions().get(i).getOutputName(),
                              Strings.emptyToNull(entry.getKey().getDimensions()[i])
                          );
                        }

                        // Add aggregations.
                        for (int i = 0; i < entry.getValues().length; i++) {
                          theMap.put(query.getAggregatorSpecs().get(i).getName(), entry.getValues()[i]);
                        }

                        return new MapBasedRow(
                            query.getGranularity().toDateTime(entry.getKey().getTimestamp()),
                            theMap
                        );
                      }
                    },
                    true
                );
              }

              @Override
              public void cleanup(CloseableGrouperIterator<GroupByMergingKey, Row> iterFromMake)
              {
                iterFromMake.close();
              }
            }
        ),
        new Closeable()
        {
          @Override
          public void close() throws IOException
          {
            mergeBuffer.close();
            if (!spillDirectory.delete()) {
              log.warn("Could not delete spillDirectory[%s].");
            }
          }
        }
    );
  }

  private void waitForFutureCompletion(
      GroupByQuery query,
      ListenableFuture<?> future
  )
  {
    try {
      if (queryWatcher != null) {
        queryWatcher.registerQuery(query, future);
      }
      final Number timeout = query.getContextValue(QueryContextKeys.TIMEOUT, (Number) null);
      if (timeout == null) {
        future.get();
      } else {
        future.get(timeout.longValue(), TimeUnit.MILLISECONDS);
      }
    }
    catch (InterruptedException e) {
      log.warn(e, "Query interrupted, cancelling pending results, query id [%s]", query.getId());
      future.cancel(true);
      throw new QueryInterruptedException(e);
    }
    catch (CancellationException e) {
      throw new QueryInterruptedException(e);
    }
    catch (TimeoutException e) {
      log.info("Query timeout, cancelling pending results for query id [%s]", query.getId());
      future.cancel(true);
      throw new QueryInterruptedException(e);
    }
    catch (ExecutionException e) {
      throw Throwables.propagate(e.getCause());
    }
  }

  private static class GroupByMergingKey
  {
    private final long timestamp;
    private final String[] dimensions;

    public GroupByMergingKey(long timestamp, String[] dimensions)
    {
      this.timestamp = timestamp;
      this.dimensions = dimensions;
    }

    public long getTimestamp()
    {
      return timestamp;
    }

    public String[] getDimensions()
    {
      return dimensions;
    }
  }

  private static class GroupByMergingKeySerde implements Grouper.KeySerde<GroupByMergingKey>
  {
    private final OnheapIncrementalIndex.OnHeapDimDim<String> dimDim;
    private final int dimCount;
    private final int keySize;
    private final ThreadLocal<ByteBuffer> threadLocalKeyBuffer;

    public GroupByMergingKeySerde(final int dimCount)
    {
      this.dimDim = new OnheapIncrementalIndex.OnHeapDimDim<>(new Object());
      this.dimCount = dimCount;
      this.keySize = Longs.BYTES + dimCount * Ints.BYTES;
      this.threadLocalKeyBuffer = new ThreadLocal<ByteBuffer>()
      {
        @Override
        protected ByteBuffer initialValue()
        {
          return ByteBuffer.allocate(keySize);
        }
      };
    }

    @Override
    public int keySize()
    {
      return keySize;
    }

    @Override
    public ByteBuffer toByteBuffer(GroupByMergingKey key)
    {
      final ByteBuffer keyBuffer = threadLocalKeyBuffer.get();
      keyBuffer.rewind();
      keyBuffer.putLong(key.getTimestamp());
      for (int i = 0; i < key.getDimensions().length; i++) {
        keyBuffer.putInt(dimDim.add(key.getDimensions()[i]));
      }
      keyBuffer.flip();
      return keyBuffer;
    }

    @Override
    public GroupByMergingKey fromByteBuffer(ByteBuffer buffer, int position)
    {
      final long timestamp = buffer.getLong(position);
      final String[] dimensions = new String[dimCount];
      for (int i = 0; i < dimensions.length; i++) {
        dimensions[i] = dimDim.getValue(buffer.getInt(position + Longs.BYTES + (Ints.BYTES * i)));
      }
      return new GroupByMergingKey(timestamp, dimensions);
    }

    @Override
    public Grouper.KeyComparator comparator()
    {
      // TODO(gianm): Document why this is okay (comparator not being used while writes are occurring)

      final IncrementalIndex.SortedDimLookup<String> sortedDimDim = dimDim.sort();

      return new Grouper.KeyComparator()
      {
        @Override
        public int compare(ByteBuffer lhsBuffer, ByteBuffer rhsBuffer, int lhsPosition, int rhsPosition)
        {
          final int timeCompare = Longs.compare(lhsBuffer.getLong(lhsPosition), rhsBuffer.getLong(rhsPosition));
          if (timeCompare != 0) {
            return timeCompare;
          }

          for (int i = 0; i < dimCount; i++) {
            final int cmp = Ints.compare(
                sortedDimDim.getSortedIdFromUnsortedId(
                    lhsBuffer.getInt(lhsPosition + Longs.BYTES + (Ints.BYTES * i))
                ),
                sortedDimDim.getSortedIdFromUnsortedId(
                    rhsBuffer.getInt(rhsPosition + Longs.BYTES + (Ints.BYTES * i))
                )
            );

            if (cmp != 0) {
              return cmp;
            }
          }

          return 0;
        }
      };
    }
  }

  private static class GroupByMergingColumnSelectorFactory implements ColumnSelectorFactory
  {
    private ThreadLocal<Row> row = new ThreadLocal<>();

    public void setRow(Row row)
    {
      this.row.set(row);
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      // Combining factories shouldn't need dimension selectors, that'd be weird.
      throw new UnsupportedOperationException();
    }

    @Override
    public FloatColumnSelector makeFloatColumnSelector(final String columnName)
    {
      return new FloatColumnSelector()
      {
        @Override
        public float get()
        {
          return row.get().getFloatMetric(columnName);
        }
      };
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(final String columnName)
    {
      return new LongColumnSelector()
      {
        @Override
        public long get()
        {
          return row.get().getLongMetric(columnName);
        }
      };
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(final String columnName)
    {
      return new ObjectColumnSelector()
      {
        @Override
        public Class classOfObject()
        {
          return Object.class;
        }

        @Override
        public Object get()
        {
          return row.get().getRaw(columnName);
        }
      };
    }
  }
}
