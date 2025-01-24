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
import org.apache.druid.query.policy.Policy;
import org.apache.druid.query.policy.RowFilterPolicy;
import org.apache.druid.segment.RestrictedSegment;
import org.apache.druid.segment.SegmentReference;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.function.Function;

public class RestrictedSegmentMapFunctionFactory implements SegmentMapFunctionFactory
{
  private final SegmentMapFunctionFactory baseFunction;
  private final Policy policy;

  @Nullable
  private final DimFilter pruningFilter;

  public RestrictedSegmentMapFunctionFactory(SegmentMapFunctionFactory baseFunction, Policy policy)
  {
    this.baseFunction = Preconditions.checkNotNull(baseFunction, "baseFunction");
    this.policy = Preconditions.checkNotNull(policy, "policy");
    this.pruningFilter = computePruningFilter(policy);
  }

  @Override
  public Function<SegmentReference, SegmentReference> makeFunction(@Nullable Query<?> query)
  {
    final Function<SegmentReference, SegmentReference> baseMapper = baseFunction.makeFunction(null);
    return baseSegment -> new RestrictedSegment(baseMapper.apply(baseSegment), policy);
  }

  @Nullable
  @Override
  public DimFilter getPruningFilter(@Nullable DimFilter nextFilter)
  {
    return baseFunction.getPruningFilter(DimFilters.maybeAnd(Arrays.asList(pruningFilter, nextFilter)).orElse(null));
  }

  @Nullable
  @Override
  public byte[] getCacheKey()
  {
    // TODO(gianm): caching for restricted datasource
    return null;
  }

  @Nullable
  static DimFilter computePruningFilter(Policy policy)
  {
    // TODO(gianm): handle other types of policies. New method on Policy, perhaps.
    if (policy instanceof RowFilterPolicy) {
      return ((RowFilterPolicy) policy).getRowFilter();
    } else {
      return null;
    }
  }
}
