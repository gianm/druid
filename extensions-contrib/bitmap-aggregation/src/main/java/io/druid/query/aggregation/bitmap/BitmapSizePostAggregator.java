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
import com.google.common.collect.ImmutableSet;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.query.aggregation.PostAggregator;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

public class BitmapSizePostAggregator implements PostAggregator
{
  private final String name;
  private final PostAggregator field;

  @JsonCreator
  public BitmapSizePostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("field") PostAggregator field
  )
  {
    this.name = name;
    this.field = field;
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
    return ((ImmutableBitmap) field.compute(combinedAggregators)).size();
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public PostAggregator getField()
  {
    return field;
  }
}
