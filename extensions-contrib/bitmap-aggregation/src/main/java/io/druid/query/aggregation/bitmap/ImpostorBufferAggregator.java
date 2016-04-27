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

package io.druid.query.aggregation.bitmap;

import com.google.common.collect.Maps;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * A regular Aggregator posing as a BufferAggregator. The offheap buffer will not be modified and aggregations
 * will actually occur on-heap.
 *
 * This is an awful idea but we are doing it because,
 *
 * 1) Pre-allocating space for maximally sized bitmaps off-heap is really wasteful when most bitmaps will not
 * actually grow to that size.
 *
 * 2) I have not yet benchmarked serde of bitmaps into/from ByteBuffers but I would guess that's slower than
 * adding things to mutable bitmaps.
 *
 * #1 is a more serious issue than #2.
 */
public class ImpostorBufferAggregator implements BufferAggregator
{
  private final Map<Integer, Aggregator> aggregators = Maps.newHashMap();
  private final AggregatorFactory aggregatorFactory;
  private final ColumnSelectorFactory columnSelectorFactory;

  public ImpostorBufferAggregator(
      AggregatorFactory aggregatorFactory,
      ColumnSelectorFactory columnSelectorFactory
  )
  {
    this.aggregatorFactory = aggregatorFactory;
    this.columnSelectorFactory = columnSelectorFactory;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    aggregators.put(position, aggregatorFactory.factorize(columnSelectorFactory));
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    aggregators.get(position).aggregate();
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return aggregators.get(position).get();
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return aggregators.get(position).getFloat();
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return aggregators.get(position).getLong();
  }

  @Override
  public void close()
  {
    for (Aggregator aggregator : aggregators.values()) {
      aggregator.close();
    }
    aggregators.clear();
  }

  @Override
  public String toString()
  {
    return "ImpostorBufferAggregator{" +
           "aggregatorFactory=" + aggregatorFactory +
           '}';
  }
}
