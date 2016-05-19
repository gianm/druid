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
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Grouper based around a set of underlying {@link SpillingGrouper} instances. Thread-safe.
 *
 * The passed-in buffer is cut up into "concurrencyHint" slices, and each slice is passed to a different underlying
 * grouper. Access to each slice is separately synchronized.
 */
public class ConcurrentGrouper<KeyType extends Comparable<KeyType>> implements Grouper<KeyType>
{
  private final List<Grouper<KeyType>> groupers;

  public ConcurrentGrouper(
      final ByteBuffer buffer,
      final int concurrencyHint,
      final LimitedTemporaryStorage temporaryStorage,
      final ObjectMapper spillMapper,
      final int maxBufferGrouperSize,
      final KeySerdeFactory<KeyType> keySerdeFactory,
      final ColumnSelectorFactory columnSelectorFactory,
      final AggregatorFactory[] aggregatorFactories
  )
  {
    this.groupers = new ArrayList<>(concurrencyHint);

    final int sliceSize = (buffer.capacity() / concurrencyHint);
    for (int i = 0; i < concurrencyHint; i++) {
      final ByteBuffer slice = buffer.duplicate();
      slice.position(sliceSize * i);
      slice.limit(slice.position() + sliceSize);
      groupers.add(
          new SpillingGrouper<>(
              slice.slice(),
              keySerdeFactory,
              columnSelectorFactory,
              aggregatorFactories,
              temporaryStorage,
              spillMapper,
              maxBufferGrouperSize
          )
      );
    }
  }

  @Override
  public boolean aggregate(KeyType key, int keyHash)
  {
    final Grouper<KeyType> grouper = groupers.get(grouperNumberForKeyHash(keyHash));
    synchronized (grouper) {
      return grouper.aggregate(key, keyHash);
    }
  }

  @Override
  public boolean aggregate(KeyType key)
  {
    final int keyHash = Groupers.hash(key);
    final int grouperNumber = grouperNumberForKeyHash(keyHash);
    final Grouper<KeyType> grouper = groupers.get(grouperNumber);
    synchronized (grouper) {
      return grouper.aggregate(key, keyHash);
    }
  }

  @Override
  public void reset()
  {
    for (Grouper<KeyType> grouper : groupers) {
      synchronized (grouper) {
        grouper.reset();
      }
    }
  }

  @Override
  public Iterator<Entry<KeyType>> iterator(final boolean sorted)
  {
    final List<Iterator<Entry<KeyType>>> iterators = new ArrayList<>(groupers.size());

    for (Grouper<KeyType> grouper : groupers) {
      synchronized (grouper) {
        iterators.add(grouper.iterator(sorted));
      }
    }

    return Groupers.mergeIterators(iterators, sorted);
  }

  @Override
  public void close()
  {
    for (Grouper<KeyType> grouper : groupers) {
      synchronized (grouper) {
        grouper.close();
      }
    }
  }

  private int grouperNumberForKeyHash(int keyHash)
  {
    return keyHash % groupers.size();
  }
}
