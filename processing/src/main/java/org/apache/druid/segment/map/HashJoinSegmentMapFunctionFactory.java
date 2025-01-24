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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.Query;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.DimFilters;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.planning.PreJoinableClause;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.join.HashJoinSegment;
import org.apache.druid.segment.join.JoinPrefixUtils;
import org.apache.druid.segment.join.JoinableClause;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.join.filter.JoinFilterAnalyzer;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysis;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysisKey;
import org.apache.druid.segment.join.filter.JoinableClauses;
import org.apache.druid.segment.join.filter.rewrite.JoinFilterRewriteConfig;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public class HashJoinSegmentMapFunctionFactory implements SegmentMapFunctionFactory
{
  private final SegmentMapFunctionFactory baseFunction;
  private final List<PreJoinableClause> clauses;
  @Nullable
  private final DimFilter baseFilter;
  private final JoinableFactory joinableFactory;

  /**
   * Create a join segment mapping function.
   *
   * @param baseFunction Function to apply before the join takes place
   * @param baseFilter   Filter to apply before the join takes place
   * @param clauses      Pre-joinable clauses
   */
  public HashJoinSegmentMapFunctionFactory(
      final SegmentMapFunctionFactory baseFunction,
      @Nullable final DimFilter baseFilter,
      final List<PreJoinableClause> clauses,
      final JoinableFactory joinableFactory
  )
  {
    this.baseFunction = Preconditions.checkNotNull(baseFunction, "baseFunction");
    this.clauses = Preconditions.checkNotNull(clauses, "clauses");
    this.baseFilter = baseFilter;
    this.joinableFactory = Preconditions.checkNotNull(joinableFactory, "joinableFactory");
  }

  @Override
  public Function<SegmentReference, SegmentReference> makeFunction(@Nullable final Query<?> query)
  {
    final Function<SegmentReference, SegmentReference> baseMapFn = baseFunction.makeFunction(null);

    if (clauses.isEmpty()) {
      return baseMapFn;
    }

    final JoinFilterPreAnalysis joinFilterPreAnalysis;
    final Filter baseFilterToUse;
    final List<JoinableClause> clausesToUse;

    if (query != null) {
      final JoinableClauses joinableClauses = JoinableClauses.createClauses(clauses, joinableFactory);
      final JoinFilterRewriteConfig filterRewriteConfig = JoinFilterRewriteConfig.forQueryContext(query.context());

      // Pick off any join clauses that can be converted into filters.
      final Set<String> requiredColumns = query.getRequiredColumns();

      if (requiredColumns != null && filterRewriteConfig.isEnableRewriteJoinToFilter()) {
        final Pair<List<Filter>, List<JoinableClause>> conversionResult = JoinableFactoryWrapper.convertJoinsToFilters(
            joinableClauses.getJoinableClauses(),
            requiredColumns,
            Ints.checkedCast(Math.min(filterRewriteConfig.getFilterRewriteMaxSize(), Integer.MAX_VALUE))
        );

        baseFilterToUse =
            Filters.maybeAnd(
                Lists.newArrayList(
                    Iterables.concat(
                        Collections.singleton(Filters.toFilter(baseFilter)),
                        conversionResult.lhs
                    )
                )
            ).orElse(null);
        clausesToUse = conversionResult.rhs;
      } else {
        baseFilterToUse = Filters.toFilter(baseFilter);
        clausesToUse = joinableClauses.getJoinableClauses();
      }

      // Analyze remaining join clauses to see if filters on them can be pushed down.
      joinFilterPreAnalysis = JoinFilterAnalyzer.computeJoinFilterPreAnalysis(
          new JoinFilterPreAnalysisKey(
              filterRewriteConfig,
              clausesToUse,
              query.getVirtualColumns(),
              Filters.maybeAnd(Arrays.asList(baseFilterToUse, Filters.toFilter(query.getFilter())))
                     .orElse(null)
          )
      );
    } else {
      // This join isn't at the top of a datasource tree. Don't precompute the pre-analysis.
      // It will be computed at the time of cursor creation.
      baseFilterToUse = Filters.toFilter(baseFilter);
      joinFilterPreAnalysis = null;
      clausesToUse = JoinableClauses.createClauses(clauses, joinableFactory).getJoinableClauses();
    }

    return baseSegment ->
        new HashJoinSegment(
            baseMapFn.apply(baseSegment),
            baseFilterToUse,
            GuavaUtils.firstNonNull(clausesToUse, ImmutableList.of()),
            joinFilterPreAnalysis
        );
  }

  @Nullable
  @Override
  public DimFilter getPruningFilter(@Nullable DimFilter nextFilter)
  {
    return baseFunction.getPruningFilter(
        SegmentMapFunctionUtils.makePushDownFilter(
            DimFilters.maybeAnd(Arrays.asList(baseFilter, nextFilter)).orElse(null),
            this::isPushDownColumn
        )
    );
  }

  @Nullable
  @Override
  public byte[] getCacheKey()
  {
    final byte[] baseCacheKey = baseFunction.getCacheKey();

    if (baseCacheKey == null || baseCacheKey.length != 0) {
      // Skip caching when the baseFunction is uncacheable (baseCacheKey == null) or when it has its
      // own cache key (we do not currently have a way to combine its cache key with our own).
      return null;
    }

    final CacheKeyBuilder keyBuilder = new CacheKeyBuilder(JoinableFactoryWrapper.JOIN_OPERATION);

    // Append a key for baseFilter.
    keyBuilder.appendBoolean(baseFilter != null);
    if (baseFilter != null) {
      keyBuilder.appendCacheable(baseFilter);
    }

    // Append a key for each clause.
    keyBuilder.appendInt(clauses.size());
    for (PreJoinableClause clause : clauses) {
      final Optional<byte[]> dataSourceCacheKey =
          joinableFactory.computeJoinCacheKey(clause.getDataSource(), clause.getCondition());
      if (dataSourceCacheKey.isEmpty()) {
        // The joinable data behind this clause does not support caching, so neither does the overall join.
        return null;
      }
      keyBuilder.appendByteArray(dataSourceCacheKey.get());
      keyBuilder.appendString(clause.getCondition().getOriginalExpression());
      keyBuilder.appendString(clause.getPrefix());
      keyBuilder.appendString(clause.getJoinType().name());
    }

    return keyBuilder.build();
  }

  @Override
  public boolean isMakeMapperCostly()
  {
    return true;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HashJoinSegmentMapFunctionFactory that = (HashJoinSegmentMapFunctionFactory) o;
    return Objects.equals(baseFunction, that.baseFunction)
           && Objects.equals(clauses, that.clauses)
           && Objects.equals(baseFilter, that.baseFilter);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(baseFunction, clauses, baseFilter);
  }

  @Override
  public String toString()
  {
    return "HashJoinSegmentMapFunction{" +
           "baseFunction=" + baseFunction +
           ", clauses=" + clauses +
           ", leftFilter=" + baseFilter +
           '}';
  }

  private boolean isPushDownColumn(String column)
  {
    for (final PreJoinableClause clause : clauses) {
      if (JoinPrefixUtils.isPrefixedBy(column, clause.getPrefix())) {
        return false;
      }
    }

    return true;
  }
}
