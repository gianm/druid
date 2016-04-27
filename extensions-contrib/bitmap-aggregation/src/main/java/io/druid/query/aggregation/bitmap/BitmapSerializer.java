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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.io.BaseEncoding;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.segment.data.BitmapSerdeFactory;

import java.io.IOException;

/**
 * Knows how to serialize ImmutableBitmaps with Jackson, because they don't know how to serialize themselves.
 *
 * The provided BitmapSerdeFactory must match the type of bitmaps intended to be used with this serializer.
 */
public class BitmapSerializer<T extends ImmutableBitmap> extends JsonSerializer<T>
{
  private final BitmapSerdeFactory bitmapSerdeFactory;

  public BitmapSerializer(BitmapSerdeFactory bitmapSerdeFactory)
  {
    this.bitmapSerdeFactory = bitmapSerdeFactory;
  }

  @Override
  public void serialize(
      final T bitmap,
      final JsonGenerator jg,
      final SerializerProvider provider
  ) throws IOException
  {
    jg.writeString(serialize(bitmap, bitmapSerdeFactory));
  }

  static String serialize(ImmutableBitmap bitmap, BitmapSerdeFactory bitmapSerdeFactory)
  {
    return BaseEncoding.base64().encode(bitmapSerdeFactory.getObjectStrategy().toBytes(bitmap));
  }
}
