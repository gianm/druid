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

import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.ColumnSelectorFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ConcurrentGrouper<KeyType> implements Grouper<KeyType>
{
  private final List<Grouper<KeyType>> groupers;
  private final KeySerde<KeyType> keySerde;

  public ConcurrentGrouper(
      final ByteBuffer buffer,
      final int concurrencyHint,
      final File spillDirectory,
      final KeySerde<KeyType> keySerde,
      final ColumnSelectorFactory columnSelectorFactory,
      final AggregatorFactory[] aggregatorFactories
  )
  {
    this.keySerde = keySerde;
    this.groupers = new ArrayList<>(concurrencyHint);

    final int sliceSize = (buffer.capacity() / concurrencyHint);
    for (int i = 0; i < concurrencyHint; i++) {
      final ByteBuffer slice = buffer.duplicate();
      slice.position(sliceSize * i);
      slice.limit(slice.position() + sliceSize);
      groupers.add(
          new SpillingGrouper<>(
              slice.slice(),
              keySerde,
              columnSelectorFactory,
              aggregatorFactories,
              spillDirectory
          )
      );
    }
  }

  @Override
  public boolean aggregate(ByteBuffer keyBuffer, int keyHash)
  {
    int idx = (keyHash >>> 8) % groupers.size();
    final Grouper<KeyType> grouper = groupers.get(idx);
    synchronized (grouper) {
      return grouper.aggregate(keyBuffer, keyHash);
    }
  }

  @Override
  public boolean aggregate(KeyType key)
  {
    final ByteBuffer keyBuffer = keySerde.toByteBuffer(key);
    return aggregate(keyBuffer, Groupers.hash(keyBuffer));
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
  public Iterator<Entry<KeyType>> iterator(final boolean deserialize)
  {
    final List<Iterator<Entry<KeyType>>> iterators = new ArrayList<>(groupers.size());

    for (Grouper<KeyType> grouper : groupers) {
      synchronized (grouper) {
        iterators.add(grouper.iterator(deserialize));
      }
    }

    return Groupers.mergeIterators(iterators, keySerde.comparator());
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
}
