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
  public void aggregate(ByteBuffer buf, int position, int[] select, int numSelected)
  {
    long register = buf.getLong(position);
    final long[] values = selector.getLongs();
    if (select == null) {
      for (int i = 0; i < selector.numUsableElements(); i++) {
        register += values[i];
      }
    } else {
      for (int i = 0; i < numSelected; i++) {
        register += values[select[i]];
      }
    }
    buf.putLong(position, register);
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
