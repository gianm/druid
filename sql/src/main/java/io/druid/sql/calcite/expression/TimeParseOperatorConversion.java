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

package io.druid.sql.calcite.expression;

import com.google.common.collect.ImmutableList;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

public class TimeParseOperatorConversion implements SqlOperatorConversion
{
  private static final TimeParseSqlFunction CALCITE_OPERATOR = new TimeParseSqlFunction();

  @Override
  public SqlOperator calciteOperator()
  {
    return CALCITE_OPERATOR;
  }

  @Override
  public DruidExpression toDruidExpression(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    final RexCall call = (RexCall) rexNode;
    final List<RexNode> operands = call.getOperands();
    final List<DruidExpression> druidExpressions = Expressions.toDruidExpressions(
        plannerContext,
        rowSignature,
        operands
    );

    if (druidExpressions == null) {
      return null;
    }

    return DruidExpression.fromFunctionCall("timestamp_parse", druidExpressions);
  }

  private static class TimeParseSqlFunction extends SqlFunction
  {
    TimeParseSqlFunction()
    {
      super(
          "TIME_PARSE",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(
              factory ->
                  factory.createTypeWithNullability(
                      factory.createSqlType(SqlTypeName.TIMESTAMP),
                      true
                  )
          ),
          null,
          OperandTypes.family(
              ImmutableList.of(
                  SqlTypeFamily.CHARACTER,
                  SqlTypeFamily.CHARACTER,
                  SqlTypeFamily.CHARACTER
              ),
              i -> i > 0
          ),
          SqlFunctionCategory.TIMEDATE
      );
    }
  }
}
