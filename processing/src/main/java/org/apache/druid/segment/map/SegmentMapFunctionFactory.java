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
import org.apache.druid.query.planning.PreJoinableClause;
import org.apache.druid.segment.SegmentReference;

import javax.annotation.Nullable;
import java.util.function.Function;

/**
 * TODO(gianm): javadoc, note that fields can be modified but __time cannot
 */
public interface SegmentMapFunctionFactory
{
  /**
   * TODO(gianm): Move CPU time accumulation into the callers
   * TODO(gianm): doc when query is nonnull (when this mapper is top level)
   */
  Function<SegmentReference, SegmentReference> makeFunction(@Nullable Query<?> query);

  /**
   * Returns a filter that may be used for excluding certain segments from processing. Applying this filter is not
   * necessary for correctness; it is purely an optimization.
   *
   * @param nextFilter higher-level filter that is applied after this segment mapping function. The pruning filter
   *                   returned by this function takes the higher-level filter into account.
   *
   * @return pruning filter for the base segment
   */
  @Nullable
  DimFilter getPruningFilter(@Nullable DimFilter nextFilter);

  /**
   * TODO(gianm): update text to describe what an empty array means (should only be used for identity)
   * TODO(gianm): wrap nonempty ones in some sigils when used?
   *
   * Compute a cache key prefix for a data source. This includes the data sources that participate in the RHS of a
   * join as well as any query specific constructs associated with join data source such as base table filter. This key prefix
   * can be used in segment level cache or result level cache. The function can return following
   * - Non-empty byte array - If there is join datasource involved and caching is possible. The result includes
   * join condition expression, join type and cache key returned by joinable factory for each {@link PreJoinableClause}
   * - NULL - There is a join but caching is not possible. It may happen if one of the participating datasource
   * in the JOIN is not cacheable.
   *
   * @return the cache key to be used as part of query cache key
   */
  @Nullable
  byte[] getCacheKey(/* TODO(gianm): add isBroker? */);

  /**
   * TODO(gianm): javadocs. override for join
   */
  default boolean isMakeMapperCostly()
  {
    return false;
  }
}
