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

import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;

public class BitmapMergeAggregator implements Aggregator
{
  private final String name;
  private final ObjectColumnSelector objectColumnSelector;
  private final BitmapFactory bitmapFactory;

  private ImmutableBitmap bitmap;

  public BitmapMergeAggregator(
      String name,
      ObjectColumnSelector objectColumnSelector,
      BitmapFactory bitmapFactory
  )
  {
    this.name = name;
    this.objectColumnSelector = objectColumnSelector;
    this.bitmapFactory = bitmapFactory;
    reset();
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public void aggregate()
  {
    final ImmutableBitmap nextBitmap = (ImmutableBitmap) objectColumnSelector.get();
    bitmap = bitmap.union(nextBitmap);
  }

  @Override
  public void reset()
  {
    bitmap = bitmapFactory.makeEmptyImmutableBitmap();
  }

  @Override
  public Object get()
  {
    return bitmap;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException(String.format("%s does not support getFloat()", getClass().getName()));
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException(String.format("%s does not support getLong()", getClass().getName()));
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
