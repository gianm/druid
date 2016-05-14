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

import com.google.common.base.Preconditions;
import com.metamx.common.IAE;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

public class BufferGrouper<KeyType> implements Grouper<KeyType>
{
  private static final float MAX_LOAD_FACTOR = 0.75f;

  private ByteBuffer buffer;
  private final KeySerde<KeyType> keySerde;
  private final int keySize;
  private final BufferAggregator[] aggregators;
  private final int[] aggregatorOffsets;
  private final int bucketSize;
  private final int buckets;
  private final int maxSize;

  private int size;

  public BufferGrouper(
      final ByteBuffer buffer,
      final KeySerde<KeyType> keySerde,
      final ColumnSelectorFactory columnSelectorFactory,
      final AggregatorFactory[] aggregatorFactories
  )
  {
    this.buffer = buffer;
    this.keySerde = keySerde;
    this.keySize = keySerde.keySize();
    this.aggregators = new BufferAggregator[aggregatorFactories.length];
    this.aggregatorOffsets = new int[aggregatorFactories.length];

    int offset = 1 + keySize;
    for (int i = 0; i < aggregatorFactories.length; i++) {
      aggregators[i] = aggregatorFactories[i].factorizeBuffered(columnSelectorFactory);
      aggregatorOffsets[i] = offset;
      offset += aggregatorFactories[i].getMaxIntermediateSize();
    }

    this.bucketSize = offset;
    this.buckets = buffer.capacity() / bucketSize;

    if (buffer.capacity() < bucketSize) {
      throw new IAE("Not enough capacity for even one row! Need[%,d] but have[%,d].", bucketSize, buffer.capacity());
    } else {
      this.maxSize = Math.max(1, (int) (buckets * MAX_LOAD_FACTOR));
    }

    // The passed-in buffer might have some junk in it; clear that out.
    reset();
  }

  @Override
  public boolean aggregate(ByteBuffer keyBuffer, int keyHash)
  {
    Preconditions.checkArgument(
        keyBuffer.remaining() == keySize,
        "key size[%s] != keySize[%s]",
        keyBuffer.remaining(),
        keySize
    );

    final int bucket = findBucket(keyBuffer, keyHash, true);

    if (bucket < 0) {
      return false;
    }

    final int offset = bucket * bucketSize;

    // Set up key if this is a new bucket.
    if (!isUsed(bucket)) {
      buffer.position(offset);
      buffer.put((byte) 1);
      buffer.put(keyBuffer);

      for (int i = 0; i < aggregators.length; i++) {
        aggregators[i].init(buffer, offset + aggregatorOffsets[i]);
      }

      size++;
    }

    // Aggregate the current row.
    for (int i = 0; i < aggregators.length; i++) {
      aggregators[i].aggregate(buffer, offset + aggregatorOffsets[i]);
    }

    return true;
  }

  @Override
  public boolean aggregate(final KeyType key)
  {
    final ByteBuffer keyBuffer = keySerde.toByteBuffer(key);
    return aggregate(keyBuffer, Groupers.hash(keyBuffer));
  }

  @Override
  public void reset()
  {
    for (int i = 0; i < buckets; i++) {
      setUsed(i, false);
    }

    size = 0;
  }

  @Override
  public Iterator<Entry<KeyType>> iterator(final boolean deserialize)
  {
    final KeyComparator comparator = keySerde.comparator();

    if (comparator != null) {
      // Sorted iterator

      // TODO(gianm): Avoid on-heap "sorted" array by storing it in the buffer
      final Integer[] sorted = new Integer[size];
      int entry = 0;
      int offset = 0;
      for (int bucket = 0; bucket < this.buckets; bucket++) {
        if (buffer.get(offset) == 1) {
          sorted[entry++] = bucket;
        }
        offset += bucketSize;
      }

      Arrays.sort(
          sorted,
          new Comparator<Integer>()
          {
            @Override
            public int compare(Integer lhs, Integer rhs)
            {
              return comparator.compare(
                  buffer,
                  buffer,
                  lhs * bucketSize + 1,
                  rhs * bucketSize + 1
              );
            }
          }
      );

      return new Iterator<Entry<KeyType>>()
      {
        int curr = 0;

        @Override
        public boolean hasNext()
        {
          return curr < sorted.length;
        }

        @Override
        public Entry<KeyType> next()
        {
          return bucketEntryForOffset(sorted[curr++] * bucketSize, deserialize);
        }

        @Override
        public void remove()
        {
          throw new UnsupportedOperationException();
        }
      };
    } else {
      // Unsorted iterator
      return new Iterator<Entry<KeyType>>()
      {
        int returned = 0;
        int offset = 0;

        @Override
        public boolean hasNext()
        {
          return returned < size;
        }

        @Override
        public Entry<KeyType> next()
        {
          while (buffer.get(offset) == 0) {
            offset += bucketSize;
          }

          final Entry<KeyType> entry = bucketEntryForOffset(offset, deserialize);
          offset += bucketSize;
          returned++;

          return entry;
        }

        @Override
        public void remove()
        {
          throw new UnsupportedOperationException();
        }
      };
    }
  }

  @Override
  public void close()
  {
    for (BufferAggregator aggregator : aggregators) {
      aggregator.close();
    }

    buffer = null;
  }

  private boolean isUsed(final int bucket)
  {
    return buffer.get(bucket * bucketSize) == 1;
  }

  private void setUsed(final int bucket, final boolean used)
  {
    buffer.put(bucket * bucketSize, used ? (byte) 1 : (byte) 0);
  }

  /**
   * Finds the bucket for a key.
   *
   * @param keyBuffer key, must have exactly keySize bytes remaining
   * @param unusedOk  should we consider unused buckets too?
   *
   * @return bucket index for this key, or -1 if no bucket is available due to being full or unusedOk being false
   */
  private int findBucket(final ByteBuffer keyBuffer, final int keyHash, final boolean unusedOk)
  {
    final int startBucket = Math.abs(keyHash % buckets);
    int bucket = startBucket;

outer:
    while (true) {
      final int bucketOffset = bucket * bucketSize;

      if (buffer.get(bucketOffset) == 0) {
        // Found unused bucket before finding our key
        return unusedOk && size < maxSize ? bucket : -1;
      }

      for (int i = bucketOffset + 1, j = keyBuffer.position(); j < keyBuffer.position() + keySize; i++, j++) {
        if (buffer.get(i) != keyBuffer.get(j)) {
          bucket += 1;
          if (bucket == buckets) {
            bucket = 0;
          }

          if (bucket == startBucket) {
            // Came back around to the start without finding a free slot, that was a long trip!
            // Should never happen unless buckets == maxSize.
            return -1;
          }

          continue outer;
        }
      }

      // Found our key in a used bucket
      return bucket;
    }
  }

  private Entry<KeyType> bucketEntryForOffset(final int bucketOffset, final boolean deserialize)
  {
    final int keyPosition = bucketOffset + 1;

    final ByteBuffer entryBuffer = buffer.duplicate();
    entryBuffer.position(keyPosition);
    entryBuffer.limit(keyPosition + bucketSize - 1);

    return bucketEntryForBuffer(entryBuffer.slice(), deserialize);
  }

  // Package-visible for SpillingGrouper
  int getEntryBufferSize()
  {
    return bucketSize - 1;
  }

  // Package-visible for SpillingGrouper
  Entry<KeyType> bucketEntryForBuffer(final ByteBuffer entryBuffer, final boolean deserialize)
  {
    final KeyType key;
    final Object[] values;

    if (deserialize) {
      key = keySerde.fromByteBuffer(entryBuffer, 0);
      values = new Object[aggregators.length];
      for (int i = 0; i < aggregators.length; i++) {
        values[i] = aggregators[i].get(entryBuffer, aggregatorOffsets[i] - 1);
      }
    } else {
      key = null;
      values = null;
    }

    return new Entry<>(entryBuffer, key, values);
  }
}
