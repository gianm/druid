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

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.io.BaseEncoding;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.query.aggregation.bitmap.BitmapAggregatorFactory;
import io.druid.segment.filter.BitmapFilter;

import java.nio.ByteBuffer;

public class BitmapDimFilter implements DimFilter
{
  // TODO(gianm): Support extractionFns

  private final String dimension;
  private final String bitmapBase64;
  private final ImmutableBitmap bitmap;
  private final boolean isInputBitmap;

  @JsonCreator
  public BitmapDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("bitmapBase64") String bitmapBase64,
      @JsonProperty("isInputBitmap") boolean isInputBitmap
  )
  {
    // TODO(gianm): Serde tests
    this.dimension = dimension;
    this.bitmapBase64 = bitmapBase64;
    this.isInputBitmap = isInputBitmap;

    if (isInputBitmap) {
      // TODO(gianm): Two-stage filtering PR would make life easier here. Would want to do this during the scan.
      throw new UnsupportedOperationException("Filtering on bitmap metrics not supported");
    }

    // Decode input bitmap
    if (bitmapBase64 != null && !bitmapBase64.isEmpty()) {
      final ByteBuffer decodedBitmap = ByteBuffer.wrap(BaseEncoding.base64().decode(bitmapBase64));
      this.bitmap = BitmapAggregatorFactory.BITMAP_SERDE_FACTORY.getObjectStrategy().fromByteBuffer(
          decodedBitmap,
          decodedBitmap.remaining()
      );
    } else {
      this.bitmap = BitmapAggregatorFactory.BITMAP_FACTORY.makeEmptyImmutableBitmap();
    }
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public String getBitmapBase64()
  {
    return bitmapBase64;
  }

  @JsonProperty("isInputBitmap")
  public boolean isInputBitmap()
  {
    return isInputBitmap;
  }

  @Override
  public byte[] getCacheKey()
  {
    // TODO(gianm): Cache key
    return new byte[0];
  }

  @Override
  public DimFilter optimize()
  {
    return this;
  }

  @Override
  public Filter toFilter()
  {
    return new BitmapFilter(dimension, bitmap);
  }
}
