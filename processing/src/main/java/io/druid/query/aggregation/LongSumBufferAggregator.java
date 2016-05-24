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

package io.druid.query.aggregation;

import io.druid.segment.VectorizedColumnSelector;

import java.nio.ByteBuffer;

/**
 */
public class LongSumBufferAggregator implements BufferAggregator, VectorAggregator
{
  private static final int UNROLL_COUNT = 8;
  private final VectorizedColumnSelector selector;

  public LongSumBufferAggregator(
      VectorizedColumnSelector selector
  )
  {
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putLong(position, 0L);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    buf.putLong(position, buf.getLong(position) + selector.getLongs()[0]);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int[] select, int selectLimit)
  {
    final long[] values = selector.getLongs();
    long sum0 = 0;
    long sum1 = 0;
    long sum2 = 0;
    long sum3 = 0;
    long sum4 = 0;
    long sum5 = 0;
    long sum6 = 0;
    long sum7 = 0;

    if (select == null) {
      int unrollable = selector.numUsableElements() / UNROLL_COUNT * UNROLL_COUNT;
      int i = 0;
      for (; i < unrollable; i += UNROLL_COUNT) {
        sum0 += values[i];
        sum1 += values[i + 1];
        sum2 += values[i + 2];
        sum3 += values[i + 3];
        sum4 += values[i + 4];
        sum5 += values[i + 5];
        sum6 += values[i + 6];
        sum7 += values[i + 7];
      }
      for (; i < selector.numUsableElements(); i++) {
        sum0 += values[i];
      }
    } else {
      for (int i = 0; i < selectLimit; i++) {
        sum0 += values[select[i]];
      }
    }

    buf.putLong(position, buf.getLong(position) + sum0 + sum1 + sum2 + sum3 + sum4 + sum5 + sum6 + sum7);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return buf.getLong(position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return (float) buf.getLong(position);
  }


  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return buf.getLong(position);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
