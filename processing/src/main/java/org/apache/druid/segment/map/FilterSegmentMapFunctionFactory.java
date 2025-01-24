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

import com.google.common.base.Preconditions;
import org.apache.druid.query.Query;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.DimFilters;
import org.apache.druid.segment.FilteredSegment;
import org.apache.druid.segment.SegmentReference;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;

public class FilterSegmentMapFunctionFactory implements SegmentMapFunctionFactory
{
  private final SegmentMapFunctionFactory baseFunction;
  private final DimFilter filter;

  public FilterSegmentMapFunctionFactory(SegmentMapFunctionFactory baseFunction, DimFilter filter)
  {
    this.baseFunction = Preconditions.checkNotNull(baseFunction, "baseFunction");
    this.filter = Preconditions.checkNotNull(filter, "filter");
  }

  @Override
  public Function<SegmentReference, SegmentReference> makeFunction(@Nullable final Query<?> query)
  {
    final Function<SegmentReference, SegmentReference> baseMapper = baseFunction.makeFunction(null);
    return baseSegment -> new FilteredSegment(baseMapper.apply(baseSegment), filter);
  }

  @Nullable
  @Override
  public DimFilter getPruningFilter(@Nullable DimFilter nextFilter)
  {
    return baseFunction.getPruningFilter(DimFilters.maybeAnd(Arrays.asList(filter, nextFilter)).orElse(null));
  }

  @Nullable
  @Override
  public byte[] getCacheKey()
  {
    // No caching for FilteredDataSource.
    return null;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FilterSegmentMapFunctionFactory that = (FilterSegmentMapFunctionFactory) o;
    return Objects.equals(baseFunction, that.baseFunction) && Objects.equals(filter, that.filter);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(baseFunction, filter);
  }

  @Override
  public String toString()
  {
    return "FilterSegmentMapFunction{" +
           "baseFunction=" + baseFunction +
           ", filter=" + filter +
           '}';
  }
}
