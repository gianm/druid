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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.ByteBuffer;
import java.util.Iterator;

public interface Grouper<KeyType extends Comparable<KeyType>>
{
  /**
   * Aggregate the current row with the provided key. May or may not be thread-safe.
   *
   * @param key     key object
   * @param keyHash result of {@link Groupers#hash(Object)} on the key
   *
   * @return true if the row was aggregated, false if not due to hitting maxSize
   */
  boolean aggregate(KeyType key, int keyHash);

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
   * If "sorted" is true then the iterator will return sorted results. It will use KeyType's natural ordering on
   * deserialized objects, and will use the {@link KeySerde#comparator()} on serialized objects. Woe be unto you
   * if these comparators are not equivalent.
   *
   * @param sorted return sorted results
   *
   * @return entry iterator
   */
  Iterator<Entry<KeyType>> iterator(final boolean sorted);

  class Entry<T>
  {
    final T key;
    final Object[] values;

    @JsonCreator
    public Entry(
        @JsonProperty("k") T key,
        @JsonProperty("v") Object[] values
    )
    {
      this.key = key;
      this.values = values;
    }

    @JsonProperty("k")
    public T getKey()
    {
      return key;
    }

    @JsonProperty("v")
    public Object[] getValues()
    {
      return values;
    }
  }

  interface KeySerdeFactory<T>
  {
    KeySerde<T> factorize();
  }

  interface KeySerde<T>
  {
    /**
     * Size of the keys returned by {@link #toByteBuffer(Object)} (which must be a fixed size)
     */
    int keySize();

    /**
     * Class of the keys.
     */
    Class<T> keyClazz();

    /**
     * Serialize a key. This will be called by the {@link #aggregate(Comparable)} method. The buffer will not
     * be retained after the aggregate method returns, so reusing buffers is OK.
     *
     * @param key key object
     *
     * @return serialized key
     */
    ByteBuffer toByteBuffer(T key);

    /**
     * Deserialize a key from a buffer. Will be called by the {@link #iterator(boolean)} method.
     *
     * @param buffer   buffer containing the key
     * @param position key start position in the buffer
     *
     * @return key object
     */
    T fromByteBuffer(ByteBuffer buffer, int position);

    /**
     * Return an object that knows how to compare two serialized keys. Will be called by the
     * {@link #iterator(boolean)} method if sorting is enabled.
     *
     * @return comparator for keys
     */
    KeyComparator comparator();

    /**
     * Reset the keySerde to its initial state. After this method is called, it's okay for
     * {@link #fromByteBuffer(ByteBuffer, int)} and {@link #comparator()} to no longer work properly on
     * previously-serialized keys.
     */
    void reset();
  }

  interface KeyComparator
  {
    int compare(ByteBuffer lhsBuffer, ByteBuffer rhsBuffer, int lhsPosition, int rhsPosition);
  }
}
