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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.metamx.collections.bitmap.WrappedImmutableRoaringBitmap;
import io.druid.initialization.DruidModule;
import io.druid.query.filter.BitmapDimFilter;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import io.druid.segment.serde.ComplexMetrics;

import java.util.List;

public class BitmapAggregationModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule(getClass().getSimpleName()).registerSubtypes(
            new NamedType(BitmapAggregatorFactory.class, BitmapAggregatorFactory.TYPE_NAME),
            new NamedType(BitmapSetPostAggregator.class, "bitmapSet"),
            new NamedType(BitmapSizePostAggregator.class, "bitmapSize"),
            new NamedType(BitmapRawPostAggregator.class, "bitmapRaw"),
            new NamedType(BitmapDimFilter.class, "bitmap")
        ).addSerializer(
            WrappedImmutableRoaringBitmap.class,
            new BitmapSerializer<WrappedImmutableRoaringBitmap>(new RoaringBitmapSerdeFactory())
        )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    if (ComplexMetrics.getSerdeForType(BitmapAggregatorFactory.TYPE_NAME) == null) {
      ComplexMetrics.registerSerde(BitmapAggregatorFactory.TYPE_NAME, new BitmapAggregatorSerde());
    }
  }
}
