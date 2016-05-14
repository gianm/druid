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

import java.nio.ByteBuffer;
import java.util.Iterator;

public interface Grouper<KeyType>
{
  /**
   * Aggregate the current row with the provided key. May or may not be thread-safe.
   *
   * @param key     key bytebuffer
   * @param keyHash hash of the key, identical to the hash that {@link #aggregate(Object)} would have used
   *
   * @return true if the row was aggregated, false if not due to hitting maxSize
   */
  boolean aggregate(ByteBuffer key, int keyHash);

  /**
   * Aggregate the current row with the provided key. May or may not be thread-safe.
   *
   * @param key key
   *
   * @return true if the row was aggregated, false if not due to hitting maxSize
   */
  boolean aggregate(KeyType key);

  /**
   * Reset the grouper to its initial state.
   */
  void reset();

  void close();

  /**
   * Iterate through entries. If a comparator is provided, do a sorted iteration.
   *
   * It is safe to call this method multiple times to get multiple iterators.
   *
   * Once this method is called, writes are no longer safe. After you have are done with the iterators returned by this
   * method, you should either call {@link #close()} (if you are done with the Grouper) or {@link #reset()} (if you
   * want to reuse it).
   *
   * @param deserialize if true, populate the "key" and "values" of the returned entries.
   *
   * @return entry iterator
   */
  Iterator<Entry<KeyType>> iterator(boolean deserialize);

  class Entry<T>
  {
    private final ByteBuffer buffer;
    private final T key;
    private final Object[] values;

    public Entry(ByteBuffer buffer, T key, Object[] values)
    {
      this.buffer = Preconditions.checkNotNull(buffer, "buffer");
      this.key = key;
      this.values = values;
    }

    public ByteBuffer getBuffer()
    {
      return buffer;
    }

    public T getKey()
    {
      return key;
    }

    public Object[] getValues()
    {
      return values;
    }
  }

  interface KeySerde<T>
  {
    int keySize();

    ByteBuffer toByteBuffer(T key);

    T fromByteBuffer(ByteBuffer buffer, int position);

    KeyComparator comparator();
  }

  interface KeyComparator
  {
    int compare(ByteBuffer lhsBuffer, ByteBuffer rhsBuffer, int lhsPosition, int rhsPosition);
  }
}
