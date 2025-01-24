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

import org.apache.druid.query.Query;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.DimFilters;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.UnnestSegment;
import org.apache.druid.segment.VirtualColumn;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;

public class UnnestSegmentMapFunctionFactory implements SegmentMapFunctionFactory
{
  private final SegmentMapFunctionFactory baseFunction;
  private final VirtualColumn virtualColumn;

  @Nullable
  private final DimFilter unnestFilter;

  public UnnestSegmentMapFunctionFactory(
      final SegmentMapFunctionFactory baseFunction,
      final VirtualColumn virtualColumn,
      @Nullable final DimFilter unnestFilter
  )
  {
    this.baseFunction = baseFunction;
    this.virtualColumn = virtualColumn;
    this.unnestFilter = unnestFilter;
  }

  @Override
  public Function<SegmentReference, SegmentReference> makeFunction(@Nullable final Query<?> query)
  {
    final Function<SegmentReference, SegmentReference> baseMapper = baseFunction.makeFunction(null);
    return baseSegment -> new UnnestSegment(baseMapper.apply(baseSegment), virtualColumn, unnestFilter);
  }

  @Nullable
  @Override
  public DimFilter getPruningFilter(@Nullable final DimFilter nextFilter)
  {
    return baseFunction.getPruningFilter(
        SegmentMapFunctionUtils.makePushDownFilter(
            DimFilters.maybeAnd(Arrays.asList(unnestFilter, nextFilter)).orElse(null),
            this::isPushDownColumn
        )
    );
  }

  @Nullable
  @Override
  public byte[] getCacheKey()
  {
    // No caching for UnnestDataSource.
    return null;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UnnestSegmentMapFunctionFactory that = (UnnestSegmentMapFunctionFactory) o;
    return Objects.equals(baseFunction, that.baseFunction)
           && Objects.equals(virtualColumn, that.virtualColumn)
           && Objects.equals(unnestFilter, that.unnestFilter);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(baseFunction, virtualColumn, unnestFilter);
  }

  @Override
  public String toString()
  {
    return "UnnestSegmentMapFunction{" +
           "baseFunction=" + baseFunction +
           ", virtualColumn=" + virtualColumn +
           ", unnestFilter=" + unnestFilter +
           '}';
  }

  private boolean isPushDownColumn(final String column)
  {
    return !column.equals(virtualColumn.getOutputName());
  }
}
