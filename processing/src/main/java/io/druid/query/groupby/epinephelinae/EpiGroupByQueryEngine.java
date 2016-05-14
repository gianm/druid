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
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.parsers.CloseableIterator;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidPool;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.groupby.GroupByQuery;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.StorageAdapter;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class EpiGroupByQueryEngine
{
  private EpiGroupByQueryEngine()
  {
    // No instantiation
  }

  public static Sequence<Row> process(
      final GroupByQuery query,
      final StorageAdapter storageAdapter,
      final StupidPool<ByteBuffer> intermediateResultsBufferPool
  )
  {
    if (storageAdapter == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    if (intervals.size() != 1) {
      throw new IAE("Should only have one interval, got[%s]", intervals);
    }

    final Sequence<Cursor> cursors = storageAdapter.makeCursors(
        Filters.toFilter(query.getDimFilter()),
        intervals.get(0),
        query.getGranularity(),
        false
    );

    final Grouper.KeySerde<ByteBuffer> keySerde = new GroupByEngineKeySerde(query.getDimensions().size());
    final ResourceHolder<ByteBuffer> bufferHolder = intermediateResultsBufferPool.take();

    final String fudgeTimestampString = Strings.emptyToNull(
        query.getContextValue(EpiGroupByStrategy.CTX_KEY_FUDGE_TIMESTAMP, "")
    );

    final Long fudgeTimestamp = fudgeTimestampString == null ? null : Long.parseLong(fudgeTimestampString);

    return Sequences.concat(
        Sequences.withBaggage(
            Sequences.map(
                cursors,
                new Function<Cursor, Sequence<Row>>()
                {
                  @Override
                  public Sequence<Row> apply(final Cursor cursor)
                  {
                    return new BaseSequence<>(
                        new BaseSequence.IteratorMaker<Row, CloseableIterator<Row>>()
                        {
                          @Override
                          public CloseableIterator<Row> make()
                          {
                            final Grouper<ByteBuffer> grouper = new BufferGrouper<>(
                                bufferHolder.get(),
                                keySerde,
                                cursor,
                                query.getAggregatorSpecs()
                                     .toArray(new AggregatorFactory[query.getAggregatorSpecs().size()])
                            );

                            final ByteBuffer keyBuffer = ByteBuffer.allocate(keySerde.keySize());
                            final int dimCount = query.getDimensions().size();
                            final DimensionSelector[] selectors = new DimensionSelector[dimCount];
                            for (int i = 0; i < selectors.length; i++) {
                              selectors[i] = cursor.makeDimensionSelector(query.getDimensions().get(i));
                            }
                            final int[] stack = new int[selectors.length];
                            final IndexedInts[] valuess = new IndexedInts[selectors.length];

                            // Time is the same for every row in the cursor
                            keyBuffer.putLong(
                                0,
                                fudgeTimestamp != null ? fudgeTimestamp : cursor.getTime().getMillis()
                            );

                            while (!cursor.isDone()) {
                              int stackp = stack.length - 1;

                              for (int i = 0; i < selectors.length; i++) {
                                valuess[i] = selectors[i].getRow();

                                // Set up first grouping
                                final int position = Longs.BYTES + Ints.BYTES * i;
                                if (valuess[i].size() == 0) {
                                  stack[i] = 0;
                                  keyBuffer.putInt(position, selectors[i].getValueCardinality());
                                } else {
                                  stack[i] = 1;
                                  keyBuffer.putInt(position, valuess[i].get(0));
                                }
                              }

                              // Aggregate first grouping for this row
                              keyBuffer.rewind();
                              if (!grouper.aggregate(keyBuffer)) {
                                // TODO(gianm): Handle overflow by emitting partially grouped results
                                throw new ISE("Oops, buffer filled up");
                              }

                              // Aggregate additional groupings for this row
                              while (stackp >= 0) {
                                if (stack[stackp] < valuess[stackp].size()) {
                                  // Load next value for current slot
                                  keyBuffer.putInt(
                                      Longs.BYTES + Ints.BYTES * stackp,
                                      valuess[stackp].get(stack[stackp])
                                  );
                                  stack[stackp]++;

                                  // Reset later slots
                                  for (int i = stackp + 1; i < stack.length; i++) {
                                    final int position = Longs.BYTES + Ints.BYTES * i;
                                    if (valuess[i].size() == 0) {
                                      stack[i] = 0;
                                      keyBuffer.putInt(position, -1);
                                    } else {
                                      stack[i] = 1;
                                      keyBuffer.putInt(position, valuess[i].get(0));
                                    }
                                  }

                                  // Aggregate additional grouping for this row
                                  keyBuffer.rewind();
                                  if (!grouper.aggregate(keyBuffer)) {
                                    // TODO(gianm): Handle overflow by emitting partially grouped results
                                    throw new ISE("Oops, buffer filled up");
                                  }

                                  stackp = stack.length - 1;
                                } else {
                                  stackp--;
                                }
                              }

                              // Advance to next row
                              cursor.advance();
                            }

                            final Iterator<Row> baseIterator = Iterators.transform(
                                grouper.iterator(true),
                                new Function<Grouper.Entry<ByteBuffer>, Row>()
                                {
                                  @Override
                                  public Row apply(Grouper.Entry<ByteBuffer> entry)
                                  {
                                    Map<String, Object> theMap = Maps.newHashMap();

                                    // Add dimensions.
                                    for (int i = 0; i < dimCount; i++) {
                                      final int id = entry.getKey().getInt(Longs.BYTES + Ints.BYTES * i);

                                      if (id >= 0) {
                                        theMap.put(
                                            query.getDimensions().get(i).getOutputName(),
                                            selectors[i].lookupName(id)
                                        );
                                      }
                                    }

                                    // Add aggregations.
                                    for (int i = 0; i < entry.getValues().length; i++) {
                                      theMap.put(query.getAggregatorSpecs().get(i).getName(), entry.getValues()[i]);
                                    }

                                    return new MapBasedRow(
                                        query.getGranularity().toDateTime(entry.getKey().getLong(0)),
                                        theMap
                                    );
                                  }
                                }
                            );

                            return new CloseableIterator<Row>()
                            {
                              @Override
                              public boolean hasNext()
                              {
                                return baseIterator.hasNext();
                              }

                              @Override
                              public Row next()
                              {
                                return baseIterator.next();
                              }

                              @Override
                              public void remove()
                              {
                                baseIterator.remove();
                              }

                              @Override
                              public void close() throws IOException
                              {
                                grouper.close();
                              }
                            };
                          }

                          @Override
                          public void cleanup(CloseableIterator<Row> iterFromMake)
                          {
                            try {
                              iterFromMake.close();
                            }
                            catch (IOException e) {
                              throw Throwables.propagate(e);
                            }
                          }
                        }
                    );
                  }
                }
            ),
            new Closeable()
            {
              @Override
              public void close() throws IOException
              {
                CloseQuietly.close(bufferHolder);
              }
            }
        )
    );
  }

  private static class GroupByEngineKeySerde implements Grouper.KeySerde<ByteBuffer>
  {
    private final int keySize;

    public GroupByEngineKeySerde(final int dimCount)
    {
      this.keySize = Longs.BYTES + dimCount * Ints.BYTES;
    }

    @Override
    public int keySize()
    {
      return keySize;
    }

    @Override
    public ByteBuffer toByteBuffer(ByteBuffer key)
    {
      return key;
    }

    @Override
    public ByteBuffer fromByteBuffer(ByteBuffer buffer, int position)
    {
      final ByteBuffer dup = buffer.duplicate();
      dup.position(position).limit(position + keySize);
      return dup.slice();
    }

    @Override
    public Grouper.KeyComparator comparator()
    {
      // No sorting, let mergeRunners handle that
      return null;
    }
  }
}
