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

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.join.HashJoinSegment;
import org.apache.druid.segment.join.Joinable;
import org.apache.druid.segment.join.JoinableClause;
import org.apache.druid.segment.join.JoinableFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Analysis of a datasource for purposes of deciding how to execute a particular query.
 *
 * The analysis breaks a datasource down in the following way:
 *
 * <pre>
 *
 *                             Q  <-- Possible outer query datasource(s) [may be multiple stacked]
 *                             |
 *                             J  <-- Possible join tree, expected to be left-leaning
 *                            / \
 *                           J  Dj <--  Other leaf datasources
 *   Base datasource        / \         which will be joined
 *  (bottom-leftmost) -->  Db Dj  <---- into the base datasource
 *
 * </pre>
 *
 * The base datasource (Db) will never be a join, but it can be any other type of datasource (table, query, etc).
 * Note that join trees are only flattened if they occur at the top of the overall tree (or underneath an outer query),
 * and that join trees are only flattened to the degree that they are left-leaning. Due to these facts, it is possible
 * for the base or leaf datasources to include additional joins.
 *
 * The idea here is to keep things simple and dumb. So we focus only on identifying left-leaning join trees, which map
 * neatly onto a series of hash table lookups at query time. The user/system generating the queries, e.g. the druid-sql
 * layer (or the end user in the case of native queries), is responsible for containing the smarts to structure the
 * tree in a way that will lead to optimal execution.
 */
public class DataSourceAnalysis
{
  private final DataSource dataSource;
  private final DataSource baseDataSource;
  private final List<JoinClause> joinClauses;

  private DataSourceAnalysis(
      DataSource dataSource,
      DataSource baseDataSource,
      List<JoinClause> joinClauses
  )
  {
    if (baseDataSource instanceof JoinDataSource) {
      // The base cannot be a join (this is a class invariant).
      // If it happens, it's a bug in the datasource analyzer.
      throw new IAE("Base dataSource cannot be a join! Original dataSource was: %s", dataSource);
    }

    this.dataSource = dataSource;
    this.baseDataSource = baseDataSource;
    this.joinClauses = joinClauses;
  }

  public static DataSourceAnalysis forDataSource(final DataSource dataSource)
  {
    final DataSource stripped = stripOuterQueries(dataSource);

    if (stripped instanceof JoinDataSource) {
      final Pair<DataSource, List<JoinClause>> flattened = flattenJoin(stripped);
      return new DataSourceAnalysis(dataSource, flattened.lhs, flattened.rhs);
    } else {
      return new DataSourceAnalysis(dataSource, stripped, Collections.emptyList());
    }
  }

  /**
   * Strips outer queries from a dataSource, getting down to a dataSource that is (hopefully) mappable onto
   * specific segments.
   */
  private static DataSource stripOuterQueries(final DataSource dataSource)
  {
    if (dataSource instanceof QueryDataSource) {
      return stripOuterQueries(((QueryDataSource) dataSource).getQuery().getDataSource());
    } else {
      return dataSource;
    }
  }

  /**
   * Flatten a datasource into two parts: the left-hand side datasource (the 'base' datasource), and a list of join
   * clauses, if any.
   *
   * @throws IllegalArgumentException if dataSource cannot be fully flattened.
   */
  private static Pair<DataSource, List<JoinClause>> flattenJoin(final DataSource dataSource)
  {
    DataSource current = dataSource;
    final List<JoinClause> joinClauses = new ArrayList<>();

    while (current instanceof JoinDataSource) {
      final JoinDataSource joinDataSource = (JoinDataSource) current;
      current = joinDataSource.getLeft();
      joinClauses.add(
          new JoinClause(
              joinDataSource.getRightPrefix(),
              joinDataSource.getRight(),
              joinDataSource.getJoinType(),
              joinDataSource.getConditionAnalysis()
          )
      );
    }

    // Join clauses were added in the order we saw them while traversing down, but we need to apply them in the
    // going-up order. So reverse them.
    Collections.reverse(joinClauses);

    return Pair.of(current, joinClauses);
  }

  /**
   * Returns the topmost datasource: the original one passed to {@link #forDataSource(DataSource)}.
   */
  public DataSource getDataSource()
  {
    return dataSource;
  }

  /**
   * Returns the base (bottom-leftmost) datasource.
   */
  public DataSource getBaseDataSource()
  {
    return baseDataSource;
  }

  /**
   * Returns join clauses corresponding to joinable leaf datasources (every leaf except the bottom-leftmost).
   *
   * @see #getBaseSegmentMapFn which returns a function that actually applies these join clauses
   */
  public List<JoinClause> getJoinClauses()
  {
    return joinClauses;
  }

  /**
   * Returns true if all servers have the ability to compute this datasource. These datasources depend only on
   * globally broadcast data, like lookups or inline data.
   */
  public boolean isGlobal()
  {
    return dataSource.isGlobal();
  }

  /**
   * Returns true if this datasource can be computed by the core Druid query stack via a scan of the base datasource.
   * The base datasource must be concrete and all other datasources involved, if any, must be global.
   */
  public boolean isScanBased()
  {
    return baseDataSource.isConcrete() && joinClauses.stream().allMatch(clause -> clause.getDataSource().isGlobal());
  }

  /**
   * Returns true if this datasource is scan-based (see {@link #isScanBased()}, and the base datasource is a 'table'
   * or union of them. This is an important property because it corresponds to datasources that can be handled by Druid
   * data servers, like Historicals.
   */
  public boolean isTableScanBased()
  {
    // At the time of writing this comment, UnionDataSource children are required to be tables, so the instanceof
    // check is redundant. But in the future, we will likely want to support unions of things other than tables,
    // so check anyway for future-proofing.
    return isScanBased() && (baseDataSource instanceof TableDataSource
                             || (baseDataSource instanceof UnionDataSource &&
                                 baseDataSource.getChildren().stream().allMatch(ds -> ds instanceof TableDataSource)));
  }

  /**
   * Returns true if this datasource represents a subquery.
   */
  public boolean isQuery()
  {
    return dataSource instanceof QueryDataSource;
  }

  /**
   * @param joinableFactory object that maps dataSources to joinables. This function will be called on all dataSources
   *                        except the primary one.
   *
   * @throws IllegalStateException if "joinableFactory" returns absent on anything other than the primary dataSource.
   */
  public Function<Segment, Segment> getBaseSegmentMapFn(final JoinableFactory joinableFactory)
  {
    final List<JoinableClause> joinableClauses =
        joinClauses.stream().map(joinClause -> {
          final Optional<Joinable> joinable = joinableFactory.build(
              joinClause.getDataSource(),
              joinClause.getCondition()
          );
          return new JoinableClause(
              joinClause.getPrefix(),
              joinable.orElseThrow(() -> new ISE("dataSource is not joinable: %s", joinClause.getDataSource())),
              joinClause.getJoinType(),
              joinClause.getCondition()
          );
        }).collect(Collectors.toList());

    if (joinableClauses.isEmpty()) {
      return Function.identity();
    } else {
      return baseSegment -> new HashJoinSegment(baseSegment, joinableClauses);
    }
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DataSourceAnalysis that = (DataSourceAnalysis) o;
    return Objects.equals(dataSource, that.dataSource) &&
           Objects.equals(baseDataSource, that.baseDataSource) &&
           Objects.equals(joinClauses, that.joinClauses);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, baseDataSource, joinClauses);
  }

  @Override
  public String toString()
  {
    return "DataSourceAnalysis{" +
           "dataSource=" + dataSource +
           ", baseDataSource=" + baseDataSource +
           ", joinClauses=" + joinClauses +
           '}';
  }
}
