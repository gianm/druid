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

package org.apache.druid.sql.calcite.expression.builtin;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;

public class SubstringOperatorConversion implements SqlOperatorConversion
{
  @Override
  public SqlOperator calciteOperator()
  {
    return SqlStdOperatorTable.SUBSTRING;
  }

  @Override
  public DruidExpression toDruidExpression(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    // Can't simply pass-through operands, since SQL standard args don't match what Druid's expression language wants.
    // SQL is 1-indexed, Druid is 0-indexed.

    final RexCall call = (RexCall) rexNode;
    final DruidExpression input = Expressions.toDruidExpression(
        plannerContext,
        rowSignature,
        call.getOperands().get(0)
    );
    if (input == null) {
      return null;
    }
    final int index = RexLiteral.intValue(call.getOperands().get(1)) - 1;
    final int length;
    if (call.getOperands().size() > 2) {
      length = RexLiteral.intValue(call.getOperands().get(2));
    } else {
      length = -1;
    }

    return input.map(
        simpleExtraction -> simpleExtraction.cascade(new SubstringDimExtractionFn(index, length < 0 ? null : length)),
        expression -> StringUtils.format(
            "substring(%s, %s, %s)",
            expression,
            DruidExpression.longLiteral(index),
            DruidExpression.longLiteral(length)
        )
    );
  }
}
