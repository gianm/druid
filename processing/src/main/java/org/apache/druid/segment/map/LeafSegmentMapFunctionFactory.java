/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.map;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.apache.druid.query.Query;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.SegmentReference;

import javax.annotation.Nullable;
import java.util.function.Function;

public class LeafSegmentMapFunctionFactory implements SegmentMapFunctionFactory
{
  private final boolean cache;

  public LeafSegmentMapFunctionFactory(final boolean cache)
  {
    this.cache = cache;
  }

  @Override
  public Function<SegmentReference, SegmentReference> makeFunction(@Nullable final Query<?> query)
  {
    return Function.identity();
  }

  @Nullable
  @Override
  public DimFilter getPruningFilter(@Nullable DimFilter nextFilter)
  {
    return nextFilter;
  }

  @Nullable
  @Override
  public byte[] getCacheKey()
  {
    return cache ? ByteArrays.EMPTY_ARRAY : null;
  }
}