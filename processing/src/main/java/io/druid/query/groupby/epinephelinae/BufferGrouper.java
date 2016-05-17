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
import com.google.common.primitives.Ints;
import com.metamx.common.IAE;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

public class BufferGrouper<KeyType extends Comparable<KeyType>> implements Grouper<KeyType>
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
  private final int tableEnd;

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

    if (buffer.capacity() < bucketSize + Ints.BYTES) {
      throw new IAE(
          "Not enough capacity for even one row! Need[%,d] but have[%,d].",
          bucketSize + Ints.BYTES,
          buffer.capacity()
      );
    }

    this.buckets = buffer.capacity() / (bucketSize + Ints.BYTES);
    this.maxSize = Math.max(1, (int) (buckets * MAX_LOAD_FACTOR));
    this.tableEnd = buckets * bucketSize;

    // Clear out any junk in the buffer
    reset();
  }

  @Override
  public boolean aggregate(KeyType key, int keyHash)
  {
    final int bucket;
    final int offset;
    final ByteBuffer keyBuffer = keySerde.toByteBuffer(key);

    if (keySize == 0) {
      // Optimization for nil key
      bucket = 0;
      offset = 0;
    } else {
      Preconditions.checkArgument(
          keyBuffer.remaining() == keySize,
          "key size[%s] != keySize[%s]",
          keyBuffer.remaining(),
          keySize
      );

      bucket = findBucket(keyBuffer, keyHash, true);

      if (bucket < 0) {
        return false;
      }

      offset = bucket * bucketSize;
    }

    // Set up key if this is a new bucket.
    if (!isUsed(bucket)) {
      buffer.position(offset);
      buffer.put((byte) 1);
      buffer.put(keyBuffer);

      for (int i = 0; i < aggregators.length; i++) {
        aggregators[i].init(buffer, offset + aggregatorOffsets[i]);
      }

      buffer.putInt(tableEnd + size * Ints.BYTES, offset);
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
    return aggregate(key, Groupers.hash(key));
  }

  @Override
  public void reset()
  {
    for (int i = 0; i < buckets; i++) {
      buffer.put(i * bucketSize, (byte) 0);
    }

    size = 0;
    keySerde.reset();
  }

  @Override
  public Iterator<Entry<KeyType>> iterator(final boolean sorted)
  {
    final KeyComparator comparator = keySerde.comparator();

    if (sorted) {
      // TODO(gianm): Sort offsets array in-place?
      final Integer[] offsets = new Integer[size];
      for (int i = 0; i < size; i++) {
        offsets[i] = buffer.getInt(tableEnd + i * Ints.BYTES);
      }

      Arrays.sort(
          offsets,
          new Comparator<Integer>()
          {
            @Override
            public int compare(Integer lhs, Integer rhs)
            {
              return comparator.compare(
                  buffer,
                  buffer,
                  lhs + 1,
                  rhs + 1
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
          return curr < offsets.length;
        }

        @Override
        public Entry<KeyType> next()
        {
          return bucketEntryForOffset(offsets[curr++]);
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
        int curr = 0;

        @Override
        public boolean hasNext()
        {
          return curr < size;
        }

        @Override
        public Entry<KeyType> next()
        {
          final int offset = buffer.getInt(tableEnd + curr * Ints.BYTES);
          final Entry<KeyType> entry = bucketEntryForOffset(offset);
          curr++;

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

  private Entry<KeyType> bucketEntryForOffset(final int bucketOffset)
  {
    final int keyPosition = bucketOffset + 1;

    final ByteBuffer entryBuffer = buffer.duplicate();
    entryBuffer.position(keyPosition);
    entryBuffer.limit(keyPosition + bucketSize - 1);

    return bucketEntryForBuffer(entryBuffer.slice());
  }

  private Entry<KeyType> bucketEntryForBuffer(final ByteBuffer entryBuffer)
  {
    final KeyType key = keySerde.fromByteBuffer(entryBuffer, 0);
    final Object[] values = new Object[aggregators.length];
    for (int i = 0; i < aggregators.length; i++) {
      values[i] = aggregators[i].get(entryBuffer, aggregatorOffsets[i] - 1);
    }

    return new Entry<>(key, values);
  }
}
