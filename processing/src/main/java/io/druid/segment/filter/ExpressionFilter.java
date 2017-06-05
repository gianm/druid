/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.filter;

import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.ColumnSelector;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.virtual.ExpressionSelectors;

public class ExpressionFilter implements Filter
{
  private final Expr expr;

  public ExpressionFilter(final Expr expr)
  {
    this.expr = expr;
  }

  @Override
  public ValueMatcher makeMatcher(final ColumnSelectorFactory factory)
  {
    final LongColumnSelector selector = ExpressionSelectors.makeLongColumnSelector(factory, expr, 0L);
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        return Evals.asBoolean(selector.get());
      }

      @Override
      public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", selector);
      }
    };
  }

  @Override
  public boolean supportsBitmapIndex(final BitmapIndexSelector selector)
  {
    return false;
  }

  @Override
  public ImmutableBitmap getBitmapIndex(final BitmapIndexSelector selector)
  {
    // Bitmap indexing not supported.
    // TODO(gianm): Could support bitmap indexing on expressions of a single column.
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean supportsSelectivityEstimation(
      final ColumnSelector columnSelector,
      final BitmapIndexSelector indexSelector
  )
  {
    return false;
  }

  @Override
  public double estimateSelectivity(final BitmapIndexSelector indexSelector)
  {
    // Selectivity estimation not supported.
    throw new UnsupportedOperationException();
  }
}
