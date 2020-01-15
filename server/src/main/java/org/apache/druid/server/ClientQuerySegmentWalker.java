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

package org.apache.druid.server;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import org.apache.druid.client.CachingClusteredClient;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.FluentQueryRunnerBuilder;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.PostProcessingOperator;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.ResultLevelCachingQueryRunner;
import org.apache.druid.query.RetryQueryRunner;
import org.apache.druid.query.RetryQueryRunnerConfig;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.server.initialization.ServerConfig;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ClientQuerySegmentWalker implements QuerySegmentWalker
{
  private final ServiceEmitter emitter;
  private final QuerySegmentWalker tableScanWalker;
  private final QuerySegmentWalker globalWalker;
  private final QueryToolChestWarehouse warehouse;
  private final RetryQueryRunnerConfig retryConfig;
  private final ObjectMapper objectMapper;
  private final ServerConfig serverConfig;
  private final Cache cache;
  private final CacheConfig cacheConfig;

  public ClientQuerySegmentWalker(
      ServiceEmitter emitter,
      QuerySegmentWalker tableScanWalker,
      QuerySegmentWalker globalWalker,
      QueryToolChestWarehouse warehouse,
      RetryQueryRunnerConfig retryConfig,
      ObjectMapper objectMapper,
      ServerConfig serverConfig,
      Cache cache,
      CacheConfig cacheConfig
  )
  {
    this.emitter = emitter;
    this.tableScanWalker = tableScanWalker;
    this.globalWalker = globalWalker;
    this.warehouse = warehouse;
    this.retryConfig = retryConfig;
    this.objectMapper = objectMapper;
    this.serverConfig = serverConfig;
    this.cache = cache;
    this.cacheConfig = cacheConfig;
  }

  @Inject
  public ClientQuerySegmentWalker(
      ServiceEmitter emitter,
      CachingClusteredClient baseClient,
      SimpleQuerySegmentWalker simpleQuerySegmentWalker,
      QueryToolChestWarehouse warehouse,
      RetryQueryRunnerConfig retryConfig,
      ObjectMapper objectMapper,
      ServerConfig serverConfig,
      Cache cache,
      CacheConfig cacheConfig
  )
  {
    this(
        emitter,
        (QuerySegmentWalker) baseClient,
        (QuerySegmentWalker) simpleQuerySegmentWalker,
        warehouse,
        retryConfig,
        objectMapper,
        serverConfig,
        cache,
        cacheConfig
    );
  }

  public static <T> InlineDataSource inline(
      final QueryToolChest<T, Query<T>> toolChest,
      final QueryRunner<T> queryRunner,
      final Query<T> query
  )
  {
    final List<String> columnNames = toolChest.resultArrayFields(query);

    // TODO(gianm): Need to know types. (Right now we're setting all to STRING)
    final List<ValueType> columnTypes = columnNames.stream()
                                                   .map(ignored -> ValueType.STRING)
                                                   .collect(Collectors.toList());

    // TODO(gianm): Need config here (overridable in context) for max rows to accumulate.
    final Sequence<T> resultSequence = queryRunner.run(QueryPlus.wrap(query));
    final List<Object[]> resultList = toolChest.resultsAsArrays(query, resultSequence).toList();

    return InlineDataSource.fromIterable(columnNames, columnTypes, resultList);
  }

  private DataSource inlineSubqueriesForQueryRunner(final DataSource dataSource)
  {
    if (dataSource instanceof QueryDataSource) {
      return dataSource.withChildren(
          Collections.singletonList(
              inlineSubqueriesForQueryRunner(Iterables.getOnlyElement(dataSource.getChildren()))
          )
      );
    } else {
      // Inline any subqueries on 'table' datasources.
      final DataSource inlined = inlineSubqueries(dataSource, DataSourceAnalysis::isTableScanBased);
      final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(inlined);

      if (analysis.isTableScanBased() || (analysis.isScanBased() && analysis.isGlobal())) {
        return inlined;
      } else {
        // Inline all subqueries.
        return inlineSubqueries(dataSource, ignored -> true);
      }
    }
  }

  private DataSource inlineSubqueries(final DataSource dataSource, final Predicate<DataSourceAnalysis> inlinePredicate)
  {
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(dataSource);

    if (analysis.isQuery() && inlinePredicate.test(analysis)) {
      // Inlinable subquery.
      final Query subquery = ((QueryDataSource) analysis.getDataSource()).getQuery();
      final QueryRunner subqueryRunner = subquery.getRunner(this);

      //noinspection unchecked
      return inline(warehouse.getToolChest(subquery), subqueryRunner, subquery);
    } else {
      // Check if any children can be inlined, then inline them and examine overall datasource again.
      DataSource currentDataSource;
      DataSource newDataSource = dataSource;

      do {
        currentDataSource = newDataSource;
        newDataSource = currentDataSource.withChildren(
            currentDataSource.getChildren()
                             .stream()
                             .map(ds -> inlineSubqueries(ds, inlinePredicate))
                             .collect(Collectors.toList())
        );
      } while (!newDataSource.equals(currentDataSource));

      return newDataSource;
    }
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    // TODO(gianm): Verify the query _can_ be transformed before starting to do it.
    final DataSource inlined = inlineSubqueriesForQueryRunner(query.getDataSource());
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(inlined);

    if (!analysis.isTableScanBased() && !(analysis.isScanBased() && analysis.isGlobal())) {
      throw new ISE("Cannot handle subquery structure for dataSource: %s", query.getDataSource());
    }

    final Query<T> newQuery = query.withDataSource(inlined);

    if (analysis.isTableScanBased()) {
      return makeRunner(newQuery, tableScanWalker.getQueryRunnerForIntervals(newQuery, intervals));
    } else {
      assert analysis.isScanBased() && analysis.isGlobal();
      return makeRunner(newQuery, globalWalker.getQueryRunnerForIntervals(newQuery, intervals));
    }
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(query.getDataSource());

    if (analysis.isTableScanBased()) {
      return makeRunner(query, tableScanWalker.getQueryRunnerForSegments(query, specs));
    } else {
      throw new ISE("Query is not table-scan-based, cannot run with specific segments");
    }
  }

  private <T> QueryRunner<T> makeRunner(Query<T> query, QueryRunner<T> baseClientRunner)
  {
    QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);

    // This does not adhere to the fluent workflow. See https://github.com/apache/druid/issues/5517
    return new ResultLevelCachingQueryRunner<>(
        makeRunner(query, baseClientRunner, toolChest),
        toolChest,
        query,
        objectMapper,
        cache,
        cacheConfig
    );
  }

  private <T> QueryRunner<T> makeRunner(
      Query<T> query,
      QueryRunner<T> baseClientRunner,
      QueryToolChest<T, Query<T>> toolChest
  )
  {
    PostProcessingOperator<T> postProcessing = objectMapper.convertValue(
        query.<String>getContextValue("postProcessing"),
        new TypeReference<PostProcessingOperator<T>>() {}
    );

    return new FluentQueryRunnerBuilder<>(toolChest)
        .create(
            new SetAndVerifyContextQueryRunner<>(
                serverConfig,
                new RetryQueryRunner<>(
                    baseClientRunner,
                    retryConfig,
                    objectMapper
                )
            )
        )
        .applyPreMergeDecoration()
        .mergeResults()
        .applyPostMergeDecoration()
        .emitCPUTimeMetric(emitter)
        .postProcess(postProcessing);
  }
}
