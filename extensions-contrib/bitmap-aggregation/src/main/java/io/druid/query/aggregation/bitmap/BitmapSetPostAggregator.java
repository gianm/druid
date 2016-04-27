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
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import io.druid.query.aggregation.PostAggregator;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BitmapSetPostAggregator implements PostAggregator
{
  private final String name;
  private final List<PostAggregator> fields;
  private final Func func;

  enum Func
  {
    UNION,
    INTERSECT,
    NOT
  }

  @JsonCreator
  public BitmapSetPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("func") String func,
      @JsonProperty("fields") List<PostAggregator> fields
  )
  {
    this.name = name;
    this.fields = fields;
    this.func = Func.valueOf(func.toUpperCase());

    if (fields.size() <= 1) {
      throw new IAE("Illegal number of fields[%s], must be > 1", fields.size());
    }
  }

  @Override
  public Set<String> getDependentFields()
  {
    Set<String> dependentFields = Sets.newLinkedHashSet();
    for (PostAggregator field : fields) {
      dependentFields.addAll(field.getDependentFields());
    }
    return dependentFields;
  }

  @Override
  public Comparator<ImmutableBitmap> getComparator()
  {
    return BitmapAggregatorFactory.COMPARATOR;
  }

  @Override
  public Object compute(final Map<String, Object> combinedAggregators)
  {
    ImmutableBitmap[] bitmaps = new ImmutableBitmap[fields.size()];
    for (int i = 0; i < bitmaps.length; i++) {
      bitmaps[i] = (ImmutableBitmap) fields.get(i).compute(combinedAggregators);
    }

    if (func == Func.UNION) {
      return BitmapAggregatorFactory.BITMAP_FACTORY.union(Arrays.asList(bitmaps));
    } else if (func == Func.INTERSECT) {
      return BitmapAggregatorFactory.BITMAP_FACTORY.intersection(Arrays.asList(bitmaps));
    } else if (func == Func.NOT) {
      return bitmaps[0].intersection(
          BitmapAggregatorFactory.BITMAP_FACTORY.union(
              Lists.transform(
                  Arrays.asList(bitmaps),
                  new Function<ImmutableBitmap, ImmutableBitmap>()
                  {
                    @Override
                    public ImmutableBitmap apply(ImmutableBitmap bitmap)
                    {
                      return BitmapAggregatorFactory.BITMAP_FACTORY.complement(bitmap);
                    }
                  }
              )
          )
      );
    } else {
      throw new ISE("WTF?! No implementation for function[%s]", func);
    }
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFunc()
  {
    return func.toString();
  }

  @JsonProperty
  public List<PostAggregator> getFields()
  {
    return fields;
  }
}
