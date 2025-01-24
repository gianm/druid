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

package org.apache.druid.query;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.planning.PreJoinableClause;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinPrefixUtils;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.map.HashJoinSegmentMapFunctionFactory;
import org.apache.druid.segment.map.SegmentMapFunctionFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents a join of two datasources.
 * <p>
 * Logically, this datasource contains the result of:
 * <p>
 * (1) prefixing all right-side columns with "rightPrefix"
 * (2) then, joining the left and (prefixed) right sides using the provided type and condition
 * <p>
 * Any columns from the left-hand side that start with "rightPrefix", and are at least one character longer than
 * the prefix, will be shadowed. It is up to the caller to ensure that no important columns are shadowed by the
 * chosen prefix.
 * <p>
 * When analyzed by {@link DataSourceAnalysis}, the right-hand side of this datasource
 * will become a {@link PreJoinableClause} object.
 */
public class JoinDataSource implements DataSource
{
  private final DataSource left;
  private final DataSource right;
  private final String rightPrefix;
  private final JoinConditionAnalysis conditionAnalysis;
  private final JoinType joinType;
  // An optional filter on the left side if left is direct table access
  @Nullable
  private final DimFilter leftFilter;
  @Nullable
  private final JoinableFactoryWrapper joinableFactoryWrapper;
  private final JoinAlgorithm joinAlgorithm;

  private JoinDataSource(
      DataSource left,
      DataSource right,
      String rightPrefix,
      JoinConditionAnalysis conditionAnalysis,
      JoinType joinType,
      @Nullable DimFilter leftFilter,
      @Nullable JoinableFactoryWrapper joinableFactoryWrapper,
      JoinAlgorithm joinAlgorithm
  )
  {
    this.left = Preconditions.checkNotNull(left, "left");
    this.right = Preconditions.checkNotNull(right, "right");
    this.rightPrefix = JoinPrefixUtils.validatePrefix(rightPrefix);
    this.conditionAnalysis = Preconditions.checkNotNull(conditionAnalysis, "conditionAnalysis");
    this.joinType = Preconditions.checkNotNull(joinType, "joinType");
    this.leftFilter = validateLeftFilter(leftFilter, left, Collections.singletonList(rightPrefix));
    this.joinableFactoryWrapper = joinableFactoryWrapper;
    this.joinAlgorithm = JoinAlgorithm.BROADCAST.equals(joinAlgorithm) ? null : joinAlgorithm;
  }

  /**
   * Create a join dataSource from a string condition.
   */
  @JsonCreator
  public static JoinDataSource create(
      @JsonProperty("left") DataSource left,
      @JsonProperty("right") DataSource right,
      @JsonProperty("rightPrefix") String rightPrefix,
      @JsonProperty("condition") String condition,
      @JsonProperty("joinType") JoinType joinType,
      @Nullable @JsonProperty("leftFilter") DimFilter leftFilter,
      @JacksonInject ExprMacroTable macroTable,
      @Nullable @JacksonInject JoinableFactoryWrapper joinableFactoryWrapper,
      @Nullable @JsonProperty("joinAlgorithm") JoinAlgorithm joinAlgorithm
  )
  {
    return new JoinDataSource(
        left,
        right,
        StringUtils.nullToEmptyNonDruidDataString(rightPrefix),
        JoinConditionAnalysis.forExpression(
            Preconditions.checkNotNull(condition, "condition"),
            StringUtils.nullToEmptyNonDruidDataString(rightPrefix),
            macroTable
        ),
        joinType,
        leftFilter,
        joinableFactoryWrapper,
        joinAlgorithm
    );
  }

  /**
   * Create a join dataSource from an existing {@link JoinConditionAnalysis}.
   */
  public static JoinDataSource create(
      final DataSource left,
      final DataSource right,
      final String rightPrefix,
      final JoinConditionAnalysis conditionAnalysis,
      final JoinType joinType,
      final DimFilter leftFilter,
      @Nullable final JoinableFactoryWrapper joinableFactoryWrapper,
      @Nullable final JoinAlgorithm joinAlgorithm
  )
  {
    return new JoinDataSource(
        left,
        right,
        rightPrefix,
        conditionAnalysis,
        joinType,
        leftFilter,
        joinableFactoryWrapper,
        joinAlgorithm
    );
  }

  @Override
  public Set<String> getTableNames()
  {
    final Set<String> names = new HashSet<>();
    names.addAll(left.getTableNames());
    names.addAll(right.getTableNames());
    return names;
  }

  @JsonProperty
  public DataSource getLeft()
  {
    return left;
  }

  @JsonProperty
  public DataSource getRight()
  {
    return right;
  }

  @JsonProperty
  public String getRightPrefix()
  {
    return rightPrefix;
  }

  @JsonProperty
  public String getCondition()
  {
    return conditionAnalysis.getOriginalExpression();
  }

  public JoinConditionAnalysis getConditionAnalysis()
  {
    return conditionAnalysis;
  }

  @JsonProperty
  public JoinType getJoinType()
  {
    return joinType;
  }

  @JsonProperty
  @Nullable
  @JsonInclude(Include.NON_NULL)
  public DimFilter getLeftFilter()
  {
    return leftFilter;
  }

  @Nullable
  public JoinableFactoryWrapper getJoinableFactoryWrapper()
  {
    return joinableFactoryWrapper;
  }

  @Override
  public List<DataSource> getChildren()
  {
    return ImmutableList.of(left, right);
  }

  @Override
  public DataSource withChildren(List<DataSource> children)
  {
    if (children.size() != 2) {
      throw new IAE("Expected [2] children, got [%d]", children.size());
    }

    return new JoinDataSource(
        children.get(0),
        children.get(1),
        rightPrefix,
        conditionAnalysis,
        joinType,
        leftFilter,
        joinableFactoryWrapper,
        joinAlgorithm
    );
  }

  @Override
  public boolean isCacheable(boolean isBroker)
  {
    return left.isCacheable(isBroker) && right.isCacheable(isBroker);
  }

  @Override
  public boolean isGlobal()
  {
    return left.isGlobal() && right.isGlobal();
  }

  @Override
  public boolean isConcrete()
  {
    if (joinableFactoryWrapper == null) {
      return false;
    } else {
      return left.isConcrete() && right.isConcrete();
    }
  }

  @Nullable
  @Override
  public DataSource getConcreteBase()
  {
    if (isConcrete()) {
      return left.getConcreteBase();
    } else {
      return null;
    }
  }

  @Nullable
  @Override
  public SegmentMapFunctionFactory getSegmentMapFunctionFactory()
  {
    return createSegmentMapFunctionFactory(this);
  }

  /**
   * TODO(gianm): javadoc
   */
  public JoinDataSource withJoinableFactory(final JoinableFactory joinableFactory)
  {
    return new JoinDataSource(
        left,
        right,
        rightPrefix,
        conditionAnalysis,
        joinType,
        leftFilter,
        new JoinableFactoryWrapper(joinableFactory),
        joinAlgorithm
    );
  }

  /**
   * Computes a set of column names for left table expressions in join condition which may already have been defined as
   * a virtual column in the virtual column registry. It helps to remove any extraenous virtual columns created and only
   * use the relevant ones.
   *
   * @return a set of column names which might be virtual columns on left table in join condition
   */
  public Set<String> getVirtualColumnCandidates()
  {
    return getConditionAnalysis().getEquiConditions()
                                 .stream()
                                 .filter(equality -> equality.getLeftExpr() != null)
                                 .map(equality -> equality.getLeftExpr().analyzeInputs().getRequiredBindings())
                                 .flatMap(Set::stream)
                                 .collect(Collectors.toSet());
  }

  @JsonProperty("joinAlgorithm")
  @JsonInclude(Include.NON_NULL)
  private JoinAlgorithm getJoinAlgorithmForSerialization()
  {
    return joinAlgorithm;
  }

  public JoinAlgorithm getJoinAlgorithm()
  {
    return joinAlgorithm == null ? JoinAlgorithm.BROADCAST : joinAlgorithm;
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
    JoinDataSource that = (JoinDataSource) o;
    return Objects.equals(left, that.left) &&
           Objects.equals(right, that.right) &&
           Objects.equals(rightPrefix, that.rightPrefix) &&
           Objects.equals(conditionAnalysis, that.conditionAnalysis) &&
           Objects.equals(leftFilter, that.leftFilter) &&
           joinAlgorithm == that.joinAlgorithm &&
           joinType == that.joinType;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(left, right, rightPrefix, conditionAnalysis, joinType, leftFilter, joinAlgorithm);
  }

  @Override
  public String toString()
  {
    return "JoinDataSource{" +
           "left=" + left +
           ", right=" + right +
           ", rightPrefix='" + rightPrefix + '\'' +
           ", condition=" + conditionAnalysis +
           ", joinType=" + joinType +
           ", leftFilter=" + leftFilter +
           ", joinAlgorithm=" + joinAlgorithm +
           '}';
  }

  /**
   * Creates the {@link HashJoinSegmentMapFunctionFactory} that computes a join datasource. Flattens to include
   * any joins that are immediately below it (following left-hand pointers).
   */
  @Nullable
  private static HashJoinSegmentMapFunctionFactory createSegmentMapFunctionFactory(JoinDataSource dataSource)
  {
    if (!dataSource.isConcrete()) {
      return null;
    }

    // Flatten in any joins that are immediately below this one (following left-hand pointers).
    final List<PreJoinableClause> clauses = new ArrayList<>();
    DataSource current = dataSource;
    DimFilter leftFilter = null;

    while (current instanceof JoinDataSource) {
      final JoinDataSource joinDataSource = (JoinDataSource) current;

      clauses.add(
          new PreJoinableClause(
              joinDataSource.getRightPrefix(),
              joinDataSource.getRight(),
              joinDataSource.getJoinType(),
              joinDataSource.getConditionAnalysis(),
              joinDataSource.getJoinAlgorithm()
          )
      );

      current = joinDataSource.getLeft();

      if (joinDataSource.getLeftFilter() != null) {
        // Stop following the chain once we see a leftFilter. There can only be one of these
        // per segmentMapFn.
        leftFilter = validateLeftFilter(
            joinDataSource.getLeftFilter(),
            current,
            clauses.stream().map(PreJoinableClause::getPrefix).collect(Collectors.toList())
        );
        break;
      }
    }

    // Join clauses were added in the order we saw them while traversing down, but we need to apply them in the
    // going-up order. So reverse them.
    Collections.reverse(clauses);

    return new HashJoinSegmentMapFunctionFactory(
        // "current" is the left-hand side of the deepest join clause.
        current.getSegmentMapFunctionFactory(),
        leftFilter,
        clauses,
        dataSource.joinableFactoryWrapper.getJoinableFactory()
    );
  }

  /**
   * Validates whether the provided leftFilter is permitted to apply to the provided left-hand datasource. Throws an
   * exception if the combination is invalid. Returns the filter if the combination is valid.
   */
  @Nullable
  private static DimFilter validateLeftFilter(
      @Nullable final DimFilter leftFilter,
      final DataSource leftDataSource,
      final List<String> rightPrefixes
  )
  {
    if (leftFilter == null) {
      return null;
    }

    // Currently we only support leftFilter when applied to concrete leaf datasources (ones with no children).
    // Note that this mean we don't support unions of table, even though this would be reasonable to add in the future.
    if (!leftDataSource.isConcrete() || !leftDataSource.getChildren().isEmpty()) {
      throw DruidException.defensive(
          "leftFilter[%s] is only supported if left data source is direct table access, but it was[%s]",
          leftFilter,
          leftDataSource
      );
    }

    for (final String column : leftFilter.getRequiredColumns()) {
      for (final String rightPrefix : rightPrefixes) {
        if (JoinPrefixUtils.isPrefixedBy(column, rightPrefix)) {
          throw DruidException.defensive("leftFilter[%s] requires right-hand column[%s]", leftFilter, column);
        }
      }
    }

    return leftFilter;
  }
}
