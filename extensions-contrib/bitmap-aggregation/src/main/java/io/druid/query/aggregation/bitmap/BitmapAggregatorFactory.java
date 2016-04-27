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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Ints;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.common.IAE;
import com.metamx.common.StringUtils;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.commons.codec.binary.Base64;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

public class BitmapAggregatorFactory extends AggregatorFactory
{
  static final String TYPE_NAME = "bitmap";
  static final byte CACHE_TYPE_ID = 21;
  static final Comparator<ImmutableBitmap> COMPARATOR = Ordering.from(
      new Comparator<ImmutableBitmap>()
      {
        @Override
        public int compare(ImmutableBitmap lhs, ImmutableBitmap rhs)
        {
          return Ints.compare(lhs.size(), rhs.size());
        }
      }
  ).nullsFirst();

  public static final BitmapSerdeFactory BITMAP_SERDE_FACTORY = new RoaringBitmapSerdeFactory();
  public static final BitmapFactory BITMAP_FACTORY = BITMAP_SERDE_FACTORY.getBitmapFactory();

  private final String name;
  private final String fieldName;
  private final boolean isInputBitmap;

  @JsonCreator
  public BitmapAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("isInputBitmap") final boolean isInputBitmap
  )
  {
    // TODO(gianm): Serde tests
    this.name = name;
    this.fieldName = fieldName;
    this.isInputBitmap = isInputBitmap;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty("isInputBitmap")
  public boolean isInputBitmap()
  {
    return isInputBitmap;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory columnSelectorFactory)
  {
    if (isInputBitmap) {
      return new BitmapMergeAggregator(
          name,
          columnSelectorFactory.makeObjectColumnSelector(fieldName),
          BITMAP_FACTORY
      );
    } else {
      return new BitmapBuildAggregator(
          name,
          columnSelectorFactory.makeDimensionSelector(new DefaultDimensionSpec(fieldName, fieldName)),
          BITMAP_FACTORY
      );
    }
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnSelectorFactory)
  {
    return new ImpostorBufferAggregator(this, columnSelectorFactory);
  }

  @Override
  public Comparator<ImmutableBitmap> getComparator()
  {
    return COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return ((ImmutableBitmap) lhs).union((ImmutableBitmap) rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new BitmapAggregatorFactory(name, name, true);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return ImmutableList.<AggregatorFactory>of(new BitmapAggregatorFactory(fieldName, fieldName, isInputBitmap));
  }

  @Override
  public ImmutableBitmap deserialize(Object object)
  {
    final ByteBuffer bb;

    if (object instanceof byte[]) {
      bb = ByteBuffer.wrap((byte[]) object);
    } else if (object instanceof String) {
      bb = ByteBuffer.wrap(Base64.decodeBase64((String) object));
    } else {
      throw new IAE("Cannot deserialize class[%s]", object.getClass().getName());
    }

    return BITMAP_SERDE_FACTORY.getObjectStrategy().fromByteBuffer(bb, bb.remaining());
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return ((ImmutableBitmap) object).size();
  }

  @Override
  public List<String> requiredFields()
  {
    return ImmutableList.of(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);
    return ByteBuffer.allocate(2 + fieldNameBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(isInputBitmap ? (byte) 1 : (byte) 0)
                     .put(fieldNameBytes)
                     .array();
  }

  @Override
  public String getTypeName()
  {
    return TYPE_NAME;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    // Not really off-heap! (See ImpostorBufferAggregator)
    return 1;
  }

  @Override
  public Object getAggregatorStartValue()
  {
    throw new UnsupportedOperationException();
  }

  public static int toInt(final String value)
  {
    // TODO(gianm): Assumes hex encoded int, should be configurable
    return Ints.fromByteArray(BaseEncoding.base16().decode(value));
  }
}
