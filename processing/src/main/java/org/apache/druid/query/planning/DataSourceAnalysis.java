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

package org.apache.druid.query.planning;

import com.google.common.base.Preconditions;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.map.SegmentMapFunctionFactory;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;

/**
 * TODO(gianm): update javadoc
 * Analysis of a datasource for purposes of deciding how to execute a particular query.
 *
 * The analysis breaks a datasource down in the following way:
 *
 * <pre>
 *
 *                             Q  <-- Possible query datasource(s) [may be none, or multiple stacked]
 *                             |
 *                             Q  <-- Base query datasource, returned by {@link #getInnerQuery()} if it exists
 *                             |
 *                             J  <-- Possible join tree, expected to be left-leaning
 *                            / \
 *                           J  Dj <--  Other leaf datasources
 *   Base datasource        / \         which will be joined
 *  (bottom-leftmost) -->  Db Dj  <---- into the base datasource
 *
 * </pre>
 *
 * The base datasource (Db) is returned by {@link #getBaseDataSource()}. The other leaf datasources are returned by
 * {@link #getPreJoinableClauses()}.
 *
 * The base datasource (Db) will never be a join, but it can be any other type of datasource (table, query, etc).
 * Note that join trees are only flattened if they occur at the top of the overall tree (or underneath an outer query),
 * and that join trees are only flattened to the degree that they are left-leaning. Due to these facts, it is possible
 * for the base or leaf datasources to include additional joins.
 *
 * The base datasource is the one that will be considered by the core Druid query stack for scanning via
 * {@link org.apache.druid.segment.Segment} and {@link org.apache.druid.segment.CursorFactory}. The other leaf
 * datasources must be joinable onto the base data.
 *
 * The idea here is to keep things simple and dumb. So we focus only on identifying left-leaning join trees, which map
 * neatly onto a series of hash table lookups at query time. The user/system generating the queries, e.g. the druid-sql
 * layer (or the end user in the case of native queries), is responsible for containing the smarts to structure the
 * tree in a way that will lead to optimal execution.
 */
public class DataSourceAnalysis
{
  private final Query<?> innerQuery;
  private final DataSource baseDataSource;
  @Nullable
  private final SegmentMapFunctionFactory segmentMapFunctionFactory;

  DataSourceAnalysis(
      final Query<?> innerQuery,
      final DataSource baseDataSource
  )
  {
    if (baseDataSource.isConcrete() && !baseDataSource.getChildren().isEmpty()) {
      // The base cannot have children if it is concrete (this is a class invariant).
      // If it happens, it's a bug in the datasource analyzer.
      throw DruidException.defensive(
          "Base dataSource[%s] is invalid, cannot be segment-mappable unless it is also concrete",
          baseDataSource
      );
    }

    this.innerQuery = Preconditions.checkNotNull(innerQuery, "innerQuery");
    this.baseDataSource = Preconditions.checkNotNull(baseDataSource, "baseDataSource");

    if (innerQuery.getDataSource().isConcrete()) {
      segmentMapFunctionFactory = innerQuery.getDataSource().getSegmentMapFunctionFactory();
    } else {
      segmentMapFunctionFactory = null;
    }
  }

  /**
   * Returns the base (bottom-leftmost) datasource.
   */
  public DataSource getBaseDataSource()
  {
    return baseDataSource;
  }

  /**
   * If {@link #getBaseDataSource()} is a {@link TableDataSource}, returns it. Otherwise, throws an error of type
   * {@link DruidException.Category#DEFENSIVE}. This method should only be used if the caller believes the base
   * datasource really should be a table.
   *
   * Note that this can throw an error even if {@link #isConcreteAndTableBased()} is true. This happens if the base
   * datasource is a {@link UnionDataSource} of {@link TableDataSource}.
   */
  public TableDataSource getBaseTableDataSource()
  {
    if (baseDataSource instanceof TableDataSource) {
      return (TableDataSource) baseDataSource;
    } else {
      throw DruidException.defensive("Base dataSource was not a table for query[%s]", innerQuery);
    }
  }

  /**
   * If {@link #getBaseDataSource()} is a {@link UnionDataSource}, returns it. Otherwise, returns an empty Optional.
   */
  public Optional<UnionDataSource> getBaseUnionDataSource()
  {
    if (baseDataSource instanceof UnionDataSource) {
      return Optional.of((UnionDataSource) baseDataSource);
    } else {
      return Optional.empty();
    }
  }

  /**
   * Returns the bottom-most (i.e. innermost) {@link Query} from a possible stack of outer queries at the root of
   * the datasource tree. This is the query that will be applied to the base datasource plus any joinables that might
   * be present.
   *
   * @return the query associated with the base datasource if  is true, else empty
   */
  public Query<?> getInnerQuery()
  {
    return innerQuery;
  }

  /**
   * TODO(gianm): javadoc
   */
  @Nullable
  public DimFilter getPruningFilter()
  {
    if (segmentMapFunctionFactory == null) {
      throw DruidException.defensive("No segmentMapFunction for query[%s]", innerQuery);
    }

    return segmentMapFunctionFactory.getPruningFilter(innerQuery.getFilter());
  }

  /**
   * TODO(gianm): javadoc
   */
  public SegmentMapFunctionFactory getSegmentMapFunction()
  {
    if (segmentMapFunctionFactory == null) {
      throw DruidException.defensive("No segmentMapFunction for query[%s]", innerQuery);
    }

    return segmentMapFunctionFactory;
  }

  /**
   * Returns the {@link QuerySegmentSpec} that is associated with the base datasource. This {@link QuerySegmentSpec} is
   * taken from the query returned by {@link #getInnerQuery()}.
   *
   * @return the query segment spec associated with the base datasource
   */
  public QuerySegmentSpec getInnerQuerySegmentSpec()
  {
    return ((BaseQuery<?>) getInnerQuery()).getQuerySegmentSpec();
  }

  /**
   * Returns true if the base datasource is {@link DataSource#isConcrete()}.
   */
  public boolean isConcreteBased()
  {
    return baseDataSource.isConcrete();
  }

  /**
   * Returns whether this datasource is one of:
   *
   * <ul>
   *   <li>{@link TableDataSource}</li>
   *   <li>{@link UnionDataSource} composed entirely of {@link TableDataSource}</li>
   * </ul>
   *
   * When this is true, and all joinables are global, the query can be handled by Druid's distributed
   * query stack. See {@link #isConcreteAndTableBased()} for this check.
   */
  public boolean isTableBased()
  {
    if (baseDataSource instanceof TableDataSource) {
      return true;
    } else if (baseDataSource instanceof UnionDataSource) {
      // getBaseTableDataSource() returns empty if this is a union, but we may still need to return true here.
      return ((UnionDataSource) baseDataSource).isTables();
    } else {
      return false;
    }
  }

  /**
   * Returns true if this datasource is both (see {@link #isConcreteBased()} and {@link #isTableBased()}.
   * This is an important property, because it corresponds to datasources that can be handled by Druid's distributed
   * query stack.
   */
  public boolean isConcreteAndTableBased()
  {
    return isConcreteBased() && isTableBased();
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DataSourceAnalysis that = (DataSourceAnalysis) o;
    return Objects.equals(innerQuery, that.innerQuery)
           && Objects.equals(baseDataSource, that.baseDataSource)
           && Objects.equals(segmentMapFunctionFactory, that.segmentMapFunctionFactory);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(innerQuery, baseDataSource, segmentMapFunctionFactory);
  }

  @Override
  public String toString()
  {
    return "DataSourceAnalysis{" +
           "innerQuery=" + innerQuery +
           ", baseDataSource=" + baseDataSource +
           ", segmentMapFunction=" + segmentMapFunctionFactory +
           '}';
  }
}
