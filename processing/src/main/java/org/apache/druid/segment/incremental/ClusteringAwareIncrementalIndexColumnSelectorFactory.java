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

package org.apache.druid.segment.incremental;

import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.Order;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.ConstantExprEvalSelector;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.projections.ClusteredColumnInspector;

import javax.annotation.Nullable;

/**
 * An {@link IncrementalIndexColumnSelectorFactory} that also serves a cluster group's clustering columns as per-group
 * constants. A clustered base table stores each cluster group's rows WITHOUT the (constant) clustering columns, so the
 * group's row selector cannot resolve them directly.
 * <p>
 * The clustering-constant interception only applies when the requested name is NOT owned by a query virtual column. The
 * superclass resolves virtual columns before physical columns, so a query VC whose output name shadows a clustering
 * column must win: intercepting it here would make that VC (and any other retained VC that reads it) silently read
 * the per-group constant instead of the computed value.
 */
final class ClusteringAwareIncrementalIndexColumnSelectorFactory extends IncrementalIndexColumnSelectorFactory
{
  private final RowSignature clusteringColumns;
  private final Object[] clusteringValues;
  private final VirtualColumns queryVirtualColumns;

  ClusteringAwareIncrementalIndexColumnSelectorFactory(
      IncrementalIndexRowSelector rowSelector,
      IncrementalIndexRowHolder rowHolder,
      CursorBuildSpec cursorBuildSpec,
      Order timeOrder,
      RowSignature clusteringColumns,
      Object[] clusteringValues
  )
  {
    super(rowSelector, rowHolder, cursorBuildSpec, timeOrder);
    this.clusteringColumns = clusteringColumns;
    this.clusteringValues = clusteringValues;
    this.queryVirtualColumns = cursorBuildSpec.getVirtualColumns();
  }

  /**
   * Returns the position of {@code name} in {@link #clusteringColumns} if it should be served as this group's
   * clustering constant, or -1 if not. A query virtual column that shares a clustering column's name shadows it (the
   * superclass resolves virtual columns before physical columns), so deferring to the superclass lets that VC, and
   * anything that reads it through this factory, observe the computed value rather than the constant.
   */
  private int clusteringConstantIndex(String name)
  {
    return queryVirtualColumns.exists(name) ? -1 : clusteringColumns.indexOf(name);
  }

  @Override
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
  {
    final int index = clusteringConstantIndex(dimensionSpec.getDimension());
    if (index >= 0) {
      final Object value = clusteringValues[index];
      return dimensionSpec.decorate(
          DimensionSelector.constant(value == null ? null : String.valueOf(value), dimensionSpec.getExtractionFn())
      );
    }
    return super.makeDimensionSelector(dimensionSpec);
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
  {
    final int index = clusteringConstantIndex(columnName);
    if (index >= 0) {
      return new ConstantValueSelector(
          ExprEval.ofType(
              ExpressionType.fromColumnTypeStrict(clusteringColumns.getColumnType(index).orElseThrow()),
              clusteringValues[index]
          )
      );
    }
    return super.makeColumnValueSelector(columnName);
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String columnName)
  {
    final int index = clusteringConstantIndex(columnName);
    if (index >= 0) {
      final ColumnType type = clusteringColumns.getColumnType(index).orElseThrow();
      return ClusteredColumnInspector.clusteringColumnCapabilities(type);
    }
    return super.getColumnCapabilities(columnName);
  }

  /**
   * Value selector for a clustering column, returning the group's typed constant from {@link #getObject()}.
   */
  private static final class ConstantValueSelector implements ColumnValueSelector<Object>
  {
    private final ConstantExprEvalSelector delegate;

    private ConstantValueSelector(ExprEval<?> eval)
    {
      this.delegate = new ConstantExprEvalSelector(eval);
    }

    @Override
    public double getDouble()
    {
      return delegate.getDouble();
    }

    @Override
    public float getFloat()
    {
      return delegate.getFloat();
    }

    @Override
    public long getLong()
    {
      return delegate.getLong();
    }

    @Override
    public boolean isNull()
    {
      return delegate.isNull();
    }

    @Nullable
    @Override
    public Object getObject()
    {
      return delegate.getObject().value();
    }

    @Override
    public Class<Object> classOfObject()
    {
      return Object.class;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("delegate", delegate);
    }
  }
}
