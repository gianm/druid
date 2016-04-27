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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.common.ISE;
import io.druid.query.aggregation.PostAggregator;
import org.roaringbitmap.IntIterator;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BitmapRawPostAggregator implements PostAggregator
{
  private final String name;
  private final Format format;
  private final PostAggregator field;

  enum Format
  {
    LIST,
    ROARINGBASE64
  }

  @JsonCreator
  public BitmapRawPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("format") String format,
      @JsonProperty("field") PostAggregator field
  )
  {
    this.name = Preconditions.checkNotNull(name, "name");
    this.format = format == null ? Format.LIST : Format.valueOf(format.toUpperCase());
    this.field = Preconditions.checkNotNull(field, "field");
  }

  @Override
  public Set<String> getDependentFields()
  {
    return ImmutableSet.copyOf(field.getDependentFields());
  }

  @Override
  public Comparator<ImmutableBitmap> getComparator()
  {
    return BitmapAggregatorFactory.COMPARATOR;
  }

  @Override
  public Object compute(final Map<String, Object> combinedAggregators)
  {
    final ImmutableBitmap bitmap = (ImmutableBitmap) field.compute(combinedAggregators);

    if (format == Format.LIST) {
      final IntIterator iterator = bitmap.iterator();
      final List<Integer> retVal = Lists.newArrayList();

      while (iterator.hasNext()) {
        retVal.add(iterator.next());
      }

      return retVal;
    } else if (format == Format.ROARINGBASE64) {
      return BitmapSerializer.serialize(bitmap, BitmapAggregatorFactory.BITMAP_SERDE_FACTORY);
    } else {
      throw new ISE("WTF?! No implementation for format[%s]", format);
    }
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public Format getFormat()
  {
    return format;
  }

  @JsonProperty
  public PostAggregator getField()
  {
    return field;
  }
}
