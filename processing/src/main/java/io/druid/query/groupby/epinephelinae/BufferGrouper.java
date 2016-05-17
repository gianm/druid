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
import com.metamx.common.ISE;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

public class BufferGrouper<KeyType extends Comparable<KeyType>> implements Grouper<KeyType>
{
  private static final int INITIAL_BUCKETS = 32;
  private static final float MAX_LOAD_FACTOR = 0.75f;
  private static final int HASH_SIZE = Ints.BYTES;

  private final ByteBuffer buffer;
  private final KeySerde<KeyType> keySerde;
  private final int keySize;
  private final BufferAggregator[] aggregators;
  private final int[] aggregatorOffsets;
  private final int bucketSize;
  private final int tableArenaSize;

  // Buffer pointing to the current table (it moves around as the table grows)
  private ByteBuffer tableBuffer;

  // Offset of tableBuffer within the larger buffer
  private int tableStart;

  // Current number of buckets in the table
  private int buckets;

  // Number of elements in the table right now
  private int size;

  // Maximum number of elements in the table before it must be resized
  private int maxSize;

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

    int offset = HASH_SIZE + keySize;
    for (int i = 0; i < aggregatorFactories.length; i++) {
      aggregators[i] = aggregatorFactories[i].factorizeBuffered(columnSelectorFactory);
      aggregatorOffsets[i] = offset;
      offset += aggregatorFactories[i].getMaxIntermediateSize();
    }

    this.bucketSize = offset;
    this.tableArenaSize = buffer.capacity() / (bucketSize + Ints.BYTES) * bucketSize;

    reset();
  }

  @Override
  public boolean aggregate(KeyType key, int keyHash)
  {
    final ByteBuffer keyBuffer = keySerde.toByteBuffer(key);

    Preconditions.checkArgument(
        keyBuffer.remaining() == keySize,
        "key size[%s] != keySize[%s]",
        keyBuffer.remaining(),
        keySize
    );

    int bucket = findBucket(tableBuffer, buckets, bucketSize, size < maxSize, keyBuffer, keySize, keyHash);

    if (bucket < 0) {
      growIfPossible();
      bucket = findBucket(tableBuffer, buckets, bucketSize, size < maxSize, keyBuffer, keySize, keyHash);

      if (bucket < 0) {
        return false;
      }
    }

    final int offset = bucket * bucketSize;

    // Set up key if this is a new bucket.
    if (!isUsed(bucket)) {
      tableBuffer.position(offset);
      tableBuffer.putInt(keyHash | 0x80000000);
      tableBuffer.put(keyBuffer);

      for (int i = 0; i < aggregators.length; i++) {
        aggregators[i].init(tableBuffer, offset + aggregatorOffsets[i]);
      }

      buffer.putInt(tableArenaSize + size * Ints.BYTES, offset);
      size++;
    }

    // Aggregate the current row.
    for (int i = 0; i < aggregators.length; i++) {
      aggregators[i].aggregate(tableBuffer, offset + aggregatorOffsets[i]);
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
    size = 0;
    buckets = Math.min(tableArenaSize / bucketSize, INITIAL_BUCKETS);

    if (buckets < 1) {
      throw new IAE(
          "Not enough capacity for even one row! Need[%,d] but have[%,d].",
          bucketSize + Ints.BYTES,
          buffer.capacity()
      );
    }

    maxSize = Math.max(1, (int) (buckets * MAX_LOAD_FACTOR));

    // Clear used bits of new table
    for (int i = 0; i < buckets; i++) {
      buffer.put(i * bucketSize, (byte) 0);
    }

    final ByteBuffer bufferDup = buffer.duplicate();
    bufferDup.position(0);
    bufferDup.limit(buckets * bucketSize);
    tableBuffer = bufferDup.slice();
    tableStart = 0;

    keySerde.reset();
  }

  @Override
  public Iterator<Entry<KeyType>> iterator(final boolean sorted)
  {
    if (sorted) {
      final KeyComparator comparator = keySerde.comparator();

      // TODO(gianm): Sort offsets array in-place?
      final Integer[] sortedOffsets = new Integer[size];
      for (int i = 0; i < size; i++) {
        sortedOffsets[i] = buffer.getInt(tableArenaSize + i * Ints.BYTES);
      }

      Arrays.sort(
          sortedOffsets,
          new Comparator<Integer>()
          {
            @Override
            public int compare(Integer lhs, Integer rhs)
            {
              return comparator.compare(
                  tableBuffer,
                  tableBuffer,
                  lhs + HASH_SIZE,
                  rhs + HASH_SIZE
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
          return curr < size;
        }

        @Override
        public Entry<KeyType> next()
        {
          return bucketEntryForOffset(sortedOffsets[curr++]);
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
          final int offset = buffer.getInt(tableArenaSize + curr * Ints.BYTES);
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
  }

  private boolean isUsed(final int bucket)
  {
    return (tableBuffer.get(bucket * bucketSize) & 0x80) == 0x80;
  }

  private Entry<KeyType> bucketEntryForOffset(final int bucketOffset)
  {
    final KeyType key = keySerde.fromByteBuffer(tableBuffer, bucketOffset + HASH_SIZE);
    final Object[] values = new Object[aggregators.length];
    for (int i = 0; i < aggregators.length; i++) {
      values[i] = aggregators[i].get(tableBuffer, bucketOffset + aggregatorOffsets[i]);
    }

    return new Entry<>(key, values);
  }

  private void growIfPossible()
  {
    final int newBuckets = buckets * 2;
    final int newMaxSize = maxSizeForBuckets(newBuckets);

    if (newBuckets > (tableArenaSize - tableStart) / bucketSize) {
      // Not enough space left to grow
      return;
    }

    final int newTableStart = tableStart + tableBuffer.limit();

    ByteBuffer newTableBuffer = buffer.duplicate();
    newTableBuffer.position(newTableStart);
    newTableBuffer.limit(newTableBuffer.position() + newBuckets * bucketSize);
    newTableBuffer = newTableBuffer.slice();

    int newSize = 0;

    // Clear used bits of new table
    for (int i = 0; i < newBuckets; i++) {
      newTableBuffer.put(i * bucketSize, (byte) 0);
    }

    // Loop over old buckets and copy to new table
    for (int oldBucket = 0; oldBucket < buckets; oldBucket++) {
      if (isUsed(oldBucket)) {
        final ByteBuffer entryBuffer = tableBuffer.duplicate();
        entryBuffer.position(oldBucket * bucketSize);
        entryBuffer.limit((oldBucket + 1) * bucketSize);

        final ByteBuffer keyBuffer = entryBuffer.duplicate();
        keyBuffer.position(entryBuffer.position() + HASH_SIZE);
        keyBuffer.limit(keyBuffer.position() + keySize);

        final int keyHash = entryBuffer.getInt(entryBuffer.position()) & 0x7fffffff;

        final int newBucket = findBucket(newTableBuffer, newBuckets, bucketSize, true, keyBuffer, keySize, keyHash);
        if (newBucket < 0) {
          throw new ISE("WTF?! Couldn't find a bucket while resizing?!");
        }
        newTableBuffer.position(newBucket * bucketSize);
        newTableBuffer.put(entryBuffer);

        buffer.putInt(tableArenaSize + newSize * Ints.BYTES, newBucket * bucketSize);
        newSize++;
      }
    }

    buckets = newBuckets;
    maxSize = newMaxSize;
    tableBuffer = newTableBuffer;
    tableStart = newTableStart;

    if (size != newSize) {
      throw new ISE("WTF?! size[%,d] != newSize[%,d] after resizing?!", size, maxSize);
    }
  }

  private static int maxSizeForBuckets(int buckets)
  {
    return Math.max(1, (int) (buckets * MAX_LOAD_FACTOR));
  }

  /**
   * Finds the bucket into which we should insert a key.
   *
   * @param keyBuffer key, must have exactly keySize bytes remaining
   *
   * @return bucket index for this key, or -1 if no bucket is available due to being full
   */
  private static int findBucket(
      final ByteBuffer tableBuffer,
      final int buckets,
      final int bucketSize,
      final boolean allowNewBucket,
      final ByteBuffer keyBuffer,
      final int keySize,
      final int keyHash
  )
  {
    Preconditions.checkArgument(
        keySize == keyBuffer.remaining(),
        "keyBuffer.remaining[%s] != keySize[%s]",
        keyBuffer.remaining(),
        keySize
    );

    final int startBucket = Math.abs(keyHash % buckets);
    int bucket = startBucket;

outer:
    while (true) {
      final int bucketOffset = bucket * bucketSize;

      if ((tableBuffer.get(bucketOffset) & 0x80) == 0) {
        // Found unused bucket before finding our key
        return allowNewBucket ? bucket : -1;
      }

      for (int i = bucketOffset + HASH_SIZE, j = keyBuffer.position(); j < keyBuffer.position() + keySize; i++, j++) {
        if (tableBuffer.get(i) != keyBuffer.get(j)) {
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
}
