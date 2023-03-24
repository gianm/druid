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

package org.apache.druid.sql.calcite.expression;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.ExpressionPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

/**
 * Utilities for assisting in writing {@link SqlOperatorConversion} implementations.
 */
public class OperatorConversions
{

  @Nullable
  public static DruidExpression convertDirectCall(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode,
      final String functionName
  )
  {
    return convertCall(
        plannerContext,
        rowSignature,
        rexNode,
        druidExpressions -> DruidExpression.ofFunctionCall(
            Calcites.getColumnTypeForRelDataType(rexNode.getType()),
            functionName,
            druidExpressions
        )
    );
  }

  @Nullable
  public static DruidExpression convertDirectCallWithExtraction(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode,
      final String functionName,
      final Function<List<DruidExpression>, SimpleExtraction> simpleExtractionFunction
  )
  {
    return convertCall(
        plannerContext,
        rowSignature,
        rexNode,
        druidExpressions -> DruidExpression.ofExpression(
            Calcites.getColumnTypeForRelDataType(rexNode.getType()),
            simpleExtractionFunction == null ? null : simpleExtractionFunction.apply(druidExpressions),
            DruidExpression.functionCall(functionName),
            druidExpressions
        )
    );
  }

  @Nullable
  public static DruidExpression convertCallBuilder(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode,
      final DruidExpression.ExpressionGenerator expressionGenerator
  )
  {
    return convertCall(
        plannerContext,
        rowSignature,
        rexNode,
        (operands) -> DruidExpression.ofExpression(
            Calcites.getColumnTypeForRelDataType(rexNode.getType()),
            expressionGenerator,
            operands
        )
    );
  }

  @Nullable
  public static DruidExpression convertCall(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode,
      final DruidExpression.DruidExpressionCreator expressionFunction
  )
  {
    final RexCall call = (RexCall) rexNode;

    final List<DruidExpression> druidExpressions = Expressions.toDruidExpressions(
        plannerContext,
        rowSignature,
        call.getOperands()
    );

    if (druidExpressions == null) {
      return null;
    }

    return expressionFunction.create(druidExpressions);
  }

  @Deprecated
  @Nullable
  public static DruidExpression convertCall(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode,
      final String functionName
  )
  {
    return convertDirectCall(plannerContext, rowSignature, rexNode, functionName);
  }

  @Deprecated
  @Nullable
  public static DruidExpression convertCall(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode,
      final String functionName,
      final Function<List<DruidExpression>, SimpleExtraction> simpleExtractionFunction
  )
  {
    return convertDirectCallWithExtraction(
        plannerContext,
        rowSignature,
        rexNode,
        functionName,
        simpleExtractionFunction
    );
  }

  /**
   * Gets operand "i" from "operands", or returns a default value if it doesn't exist (operands is too short)
   * or is null.
   */
  public static <T> T getOperandWithDefault(
      final List<RexNode> operands,
      final int i,
      final Function<RexNode, T> f,
      final T defaultReturnValue
  )
  {
    if (operands.size() > i && !RexLiteral.isNullLiteral(operands.get(i))) {
      return f.apply(operands.get(i));
    } else {
      return defaultReturnValue;
    }
  }

  @Nullable
  public static DruidExpression convertCallWithPostAggOperands(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode,
      final DruidExpression.DruidExpressionCreator expressionFunction,
      final PostAggregatorVisitor postAggregatorVisitor
  )
  {
    final RexCall call = (RexCall) rexNode;

    final List<DruidExpression> druidExpressions = Expressions.toDruidExpressionsWithPostAggOperands(
        plannerContext,
        rowSignature,
        call.getOperands(),
        postAggregatorVisitor
    );

    if (druidExpressions == null) {
      return null;
    }

    return expressionFunction.create(druidExpressions);
  }

  /**
   * Translate a Calcite {@code RexNode} to a Druid PostAggregator
   *
   * @param plannerContext        SQL planner context
   * @param rowSignature          signature of the rows to be extracted from
   * @param rexNode               expression meant to be applied on top of the rows
   * @param postAggregatorVisitor visitor that manages postagg names and tracks postaggs that were created
   *                              by the translation
   * @param useExpressions        whether we should consider {@link ExpressionPostAggregator} as a target
   *
   * @return rexNode referring to fields in rowOrder, or null if not possible
   */
  @Nullable
  public static PostAggregator toPostAggregator(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode,
      final PostAggregatorVisitor postAggregatorVisitor,
      final boolean useExpressions
  )
  {
    final SqlKind kind = rexNode.getKind();
    if (kind == SqlKind.INPUT_REF) {
      // Translate field references.
      final RexInputRef ref = (RexInputRef) rexNode;
      final String columnName = rowSignature.getColumnName(ref.getIndex());
      if (columnName == null) {
        throw new ISE("PostAggregator referred to nonexistent index[%d]", ref.getIndex());
      }

      return new FieldAccessPostAggregator(
          postAggregatorVisitor.getOutputNamePrefix() + postAggregatorVisitor.getAndIncrementCounter(),
          columnName
      );
    } else if (rexNode instanceof RexCall) {
      final SqlOperator operator = ((RexCall) rexNode).getOperator();
      final SqlOperatorConversion conversion = plannerContext.getPlannerToolbox().operatorTable()
                                                             .lookupOperatorConversion(operator);

      if (conversion != null) {
        // Try call-specific translation.
        final PostAggregator postAggregator = conversion.toPostAggregator(
            plannerContext,
            rowSignature,
            rexNode,
            postAggregatorVisitor
        );

        if (postAggregator != null) {
          return postAggregator;
        }
      }
    }

    if (useExpressions) {
      // Try to translate to expression postaggregator.
      final DruidExpression druidExpression = Expressions.toDruidExpression(
          plannerContext,
          rowSignature,
          rexNode
      );

      if (druidExpression != null) {
        return new ExpressionPostAggregator(
            postAggregatorVisitor.getOutputNamePrefix() + postAggregatorVisitor.getAndIncrementCounter(),
            druidExpression.getExpression(),
            null,
            plannerContext.getExprMacroTable()
        );
      }
    }

    // Could not translate.
    if (rexNode instanceof RexCall || kind == SqlKind.LITERAL) {
      return null;
    } else {
      throw new IAE("Unknown rexnode kind: " + kind);
    }
  }

  /**
   * Returns a builder that helps {@link SqlOperatorConversion} implementations create the {@link SqlFunction}
   * objects they need to return from {@link SqlOperatorConversion#calciteOperator()}.
   */
  public static OperatorBuilder operatorBuilder(final String name)
  {
    return new OperatorBuilder(name);
  }

  public static class OperatorBuilder
  {
    private final String name;
    private SqlKind kind = SqlKind.OTHER_FUNCTION;
    private SqlReturnTypeInference returnTypeInference;
    private SqlFunctionCategory functionCategory = SqlFunctionCategory.USER_DEFINED_FUNCTION;

    // For operand type checking
    private SqlOperandTypeChecker operandTypeChecker;
    private List<SqlTypeFamily> operandTypes;
    private Integer requiredOperandCount = null;
    private int[] literalOperands = null;
    private SqlOperandTypeInference operandTypeInference;

    private OperatorBuilder(final String name)
    {
      this.name = Preconditions.checkNotNull(name, "name");
    }

    /**
     * Sets the return type of the operator to "typeName", marked as non-nullable. If this method is used it implies the
     * operator should never, ever, return null.
     *
     * One of {@link #returnTypeNonNull}, {@link #returnTypeNullable}, {@link #returnTypeCascadeNullable(SqlTypeName)}
     * {@link #returnTypeNullableArrayWithNullableElements}, or {@link #returnTypeInference(SqlReturnTypeInference)}
     * must be used before calling {@link #build()}. These methods cannot be mixed; you must call exactly one.
     */
    public OperatorBuilder returnTypeNonNull(final SqlTypeName typeName)
    {
      Preconditions.checkState(this.returnTypeInference == null, "Cannot set return type multiple times");

      this.returnTypeInference = ReturnTypes.explicit(
          factory -> Calcites.createSqlType(factory, typeName)
      );
      return this;
    }

    /**
     * Sets the return type of the operator to "typeName", marked as nullable.
     *
     * One of {@link #returnTypeNonNull}, {@link #returnTypeNullable}, {@link #returnTypeCascadeNullable(SqlTypeName)}
     * {@link #returnTypeNullableArrayWithNullableElements}, or {@link #returnTypeInference(SqlReturnTypeInference)}
     * must be used before calling {@link #build()}. These methods cannot be mixed; you must call exactly one.
     */
    public OperatorBuilder returnTypeNullable(final SqlTypeName typeName)
    {
      Preconditions.checkState(this.returnTypeInference == null, "Cannot set return type multiple times");

      this.returnTypeInference = ReturnTypes.explicit(
          factory -> Calcites.createSqlTypeWithNullability(factory, typeName, true)
      );
      return this;
    }

    /**
     * Sets the return type of the operator to "typeName", marked as nullable if any of its operands are nullable.
     *
     * One of {@link #returnTypeNonNull}, {@link #returnTypeNullable}, {@link #returnTypeCascadeNullable(SqlTypeName)}
     * {@link #returnTypeNullableArrayWithNullableElements}, or {@link #returnTypeInference(SqlReturnTypeInference)}
     * must be used before calling {@link #build()}. These methods cannot be mixed; you must call exactly one.
     */
    public OperatorBuilder returnTypeCascadeNullable(final SqlTypeName typeName)
    {
      Preconditions.checkState(this.returnTypeInference == null, "Cannot set return type multiple times");
      this.returnTypeInference = ReturnTypes.cascade(ReturnTypes.explicit(typeName), SqlTypeTransforms.TO_NULLABLE);
      return this;
    }

    /**
     * Sets the return type of the operator to an array type with elements of "typeName", marked as nullable.
     *
     * One of {@link #returnTypeNonNull}, {@link #returnTypeNullable}, {@link #returnTypeCascadeNullable(SqlTypeName)}
     * {@link #returnTypeArrayWithNullableElements}, or {@link #returnTypeInference(SqlReturnTypeInference)} must be
     * used before calling {@link #build()}. These methods cannot be mixed; you must call exactly one.
     */
    public OperatorBuilder returnTypeArrayWithNullableElements(final SqlTypeName elementTypeName)
    {
      Preconditions.checkState(this.returnTypeInference == null, "Cannot set return type multiple times");

      this.returnTypeInference = ReturnTypes.explicit(
          factory -> Calcites.createSqlArrayTypeWithNullability(factory, elementTypeName, true)
      );
      return this;
    }

    /**
     * Sets the return type of the operator to an array type with elements of "typeName", marked as nullable.
     *
     * One of {@link #returnTypeNonNull}, {@link #returnTypeNullable}, {@link #returnTypeCascadeNullable(SqlTypeName)}
     * {@link #returnTypeArrayWithNullableElements}, or {@link #returnTypeInference(SqlReturnTypeInference)} must be
     * used before calling {@link #build()}. These methods cannot be mixed; you must call exactly one.
     */
    public OperatorBuilder returnTypeNullableArrayWithNullableElements(final SqlTypeName elementTypeName)
    {
      this.returnTypeInference = ReturnTypes.cascade(
          opBinding -> Calcites.createSqlArrayTypeWithNullability(
              opBinding.getTypeFactory(),
              elementTypeName,
              true
          ),
          SqlTypeTransforms.FORCE_NULLABLE
      );
      return this;
    }


    /**
     * Provides customized return type inference logic.
     *
     * One of {@link #returnTypeNonNull}, {@link #returnTypeNullable}, {@link #returnTypeCascadeNullable(SqlTypeName)}
     * {@link #returnTypeNullableArrayWithNullableElements}, or {@link #returnTypeInference(SqlReturnTypeInference)}
     * must be used before calling {@link #build()}. These methods cannot be mixed; you must call exactly one.
     */
    public OperatorBuilder returnTypeInference(final SqlReturnTypeInference returnTypeInference)
    {
      Preconditions.checkState(this.returnTypeInference == null, "Cannot set return type multiple times");

      this.returnTypeInference = returnTypeInference;
      return this;
    }

    /**
     * Sets the {@link SqlKind} of the operator.
     *
     * The default, if not provided, is {@link SqlFunctionCategory#USER_DEFINED_FUNCTION}.
     */
    public OperatorBuilder functionCategory(final SqlFunctionCategory functionCategory)
    {
      this.functionCategory = functionCategory;
      return this;
    }

    /**
     * Provide customized operand type checking logic.
     *
     * Either {@link #operandTypes(SqlTypeFamily...)} or this method must be used before calling {@link #build()}.
     * These methods cannot be mixed; you must call exactly one.
     */
    public OperatorBuilder operandTypeChecker(final SqlOperandTypeChecker operandTypeChecker)
    {
      this.operandTypeChecker = operandTypeChecker;
      return this;
    }

    /**
     * Equivalent to calling {@link BasicOperandTypeChecker.Builder#operandTypes(SqlTypeFamily...)}; leads to using a
     * {@link BasicOperandTypeChecker} as our operand type checker.
     *
     * Not compatible with {@link #operandTypeChecker(SqlOperandTypeChecker)}.
     */
    public OperatorBuilder operandTypes(final SqlTypeFamily... operandTypes)
    {
      this.operandTypes = Arrays.asList(operandTypes);
      return this;
    }

    /**
     * Equivalent to calling {@link BasicOperandTypeChecker.Builder#requiredOperandCount(int)}; leads to using a
     * {@link BasicOperandTypeChecker} as our operand type checker.
     *
     * Not compatible with {@link #operandTypeChecker(SqlOperandTypeChecker)}.
     *
     * @deprecated use requiredOperandCount instead
     */
    @Deprecated
    public OperatorBuilder requiredOperands(final int requiredOperands)
    {
      this.requiredOperandCount = requiredOperands;
      return this;
    }

    /**
     * Equivalent to calling {@link BasicOperandTypeChecker.Builder#requiredOperandCount(int)}; leads to using a
     * {@link BasicOperandTypeChecker} as our operand type checker.
     *
     * Not compatible with {@link #operandTypeChecker(SqlOperandTypeChecker)}.
     */
    @Deprecated
    public OperatorBuilder requiredOperandCount(final int requiredOperandCount)
    {
      this.requiredOperandCount = requiredOperandCount;
      return this;
    }

    /**
     * Equivalent to calling {@link BasicOperandTypeChecker.Builder#literalOperands(int...)}; leads to using a
     * {@link BasicOperandTypeChecker} as our operand type checker.
     *
     * Not compatible with {@link #operandTypeChecker(SqlOperandTypeChecker)}.
     */
    public OperatorBuilder literalOperands(final int... literalOperands)
    {
      this.literalOperands = literalOperands;
      return this;
    }

    public OperatorBuilder operandTypeInference(SqlOperandTypeInference operandTypeInference)
    {
      this.operandTypeInference = operandTypeInference;
      return this;
    }

    public OperatorBuilder sqlKind(SqlKind kind)
    {
      this.kind = kind;
      return this;
    }

    /**
     * Creates a {@link SqlFunction} from this builder.
     */
    public SqlFunction build()
    {
      // Create "nullableOperands" set including all optional arguments.
      final IntSet nullableOperands = new IntArraySet();
      if (requiredOperandCount != null) {
        IntStream.range(requiredOperandCount, operandTypes.size()).forEach(nullableOperands::add);
      }

      final SqlOperandTypeChecker theOperandTypeChecker;

      if (operandTypeChecker == null) {
        final BasicOperandTypeChecker.Builder checkerBuilder = BasicOperandTypeChecker.builder();
        checkerBuilder.operandTypes(operandTypes);
        if (requiredOperandCount != null) {
          checkerBuilder.requiredOperandCount(requiredOperandCount);
        }
        checkerBuilder.literalOperands(literalOperands);
        theOperandTypeChecker = checkerBuilder.build();
      } else if (operandTypes == null && requiredOperandCount == null && literalOperands == null) {
        theOperandTypeChecker = operandTypeChecker;
      } else {
        throw new ISE(
            "Cannot have both 'operandTypeChecker' and 'operandTypes' / 'requiredOperandCount' / 'literalOperands'"
        );
      }

      if (operandTypeInference == null) {
        SqlOperandTypeInference defaultInference = new DefaultOperandTypeInference(operandTypes, nullableOperands);
        operandTypeInference = (callBinding, returnType, types) -> {
          for (int i = 0; i < types.length; i++) {
            // calcite sql validate tries to do bad things to dynamic parameters if the type is inferred to be a string
            if (callBinding.operand(i).isA(ImmutableSet.of(SqlKind.DYNAMIC_PARAM))) {
              types[i] = new BasicSqlType(
                  DruidTypeSystem.INSTANCE,
                  SqlTypeName.ANY
              );
            } else {
              defaultInference.inferOperandTypes(callBinding, returnType, types);
            }
          }
        };
      }
      return new SqlFunction(
          name,
          kind,
          Preconditions.checkNotNull(returnTypeInference, "returnTypeInference"),
          operandTypeInference,
          theOperandTypeChecker,
          functionCategory
      );
    }
  }

  /**
   * Return the default, inferred specific type for a parameter that has a particular type family. Used to infer
   * the type of NULL literals.
   */
  private static SqlTypeName defaultTypeForFamily(final SqlTypeFamily family)
  {
    switch (family) {
      case NUMERIC:
      case APPROXIMATE_NUMERIC:
        return SqlTypeName.DOUBLE;
      case INTEGER:
      case EXACT_NUMERIC:
        return SqlTypeName.BIGINT;
      case CHARACTER:
        return SqlTypeName.VARCHAR;
      case TIMESTAMP:
        return SqlTypeName.TIMESTAMP;
      default:
        // No good default type for this family; just return the first one (or NULL, if empty).
        return Iterables.getFirst(family.getTypeNames(), SqlTypeName.NULL);
    }
  }

  /**
   * Operand type inference that simply reports the types derived by the validator.
   *
   * We do this so that Calcite will allow NULL literals for type-checked operands. Otherwise, it will not be able to
   * infer their types, and it will report them as NULL types, which will make operand type checking fail.
   */
  private static class DefaultOperandTypeInference implements SqlOperandTypeInference
  {
    private final List<SqlTypeFamily> operandTypes;
    private final IntSet nullableOperands;

    DefaultOperandTypeInference(final List<SqlTypeFamily> operandTypes, final IntSet nullableOperands)
    {
      this.operandTypes = operandTypes;
      this.nullableOperands = nullableOperands;
    }

    @Override
    public void inferOperandTypes(
        final SqlCallBinding callBinding,
        final RelDataType returnType,
        final RelDataType[] operandTypesOut
    )
    {
      for (int i = 0; i < operandTypesOut.length; i++) {
        final RelDataType derivedType = callBinding.getValidator()
                                                   .deriveType(callBinding.getScope(), callBinding.operand(i));

        final RelDataType inferredType;

        if (derivedType.getSqlTypeName() != SqlTypeName.NULL) {
          // We could derive a non-NULL type; retain it.
          inferredType = derivedType;
        } else {
          // We couldn't derive a non-NULL type; infer the default for the operand type family.
          if (nullableOperands.contains(i)) {
            inferredType = Calcites.createSqlTypeWithNullability(
                callBinding.getTypeFactory(),
                defaultTypeForFamily(operandTypes.get(i)),
                true
            );
          } else {
            inferredType = callBinding.getValidator().getUnknownType();
          }
        }

        operandTypesOut[i] = inferredType;
      }
    }
  }

  public static DirectOperatorConversion druidUnaryLongFn(String sqlOperator, String druidFunctionName)
  {
    return new DirectOperatorConversion(
        operatorBuilder(sqlOperator)
            .requiredOperands(1)
            .operandTypes(SqlTypeFamily.NUMERIC)
            .returnTypeNullable(SqlTypeName.BIGINT)
            .functionCategory(SqlFunctionCategory.NUMERIC)
            .build(),
        druidFunctionName
    );
  }

  public static DirectOperatorConversion druidBinaryLongFn(String sqlOperator, String druidFunctionName)
  {
    return new DirectOperatorConversion(
        operatorBuilder(sqlOperator)
            .requiredOperands(2)
            .operandTypes(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)
            .returnTypeNullable(SqlTypeName.BIGINT)
            .functionCategory(SqlFunctionCategory.NUMERIC)
            .build(),
        druidFunctionName
    );
  }

  public static DirectOperatorConversion druidUnaryDoubleFn(String sqlOperator, String druidFunctionName)
  {
    return new DirectOperatorConversion(
        operatorBuilder(StringUtils.toUpperCase(sqlOperator))
            .requiredOperands(1)
            .operandTypes(SqlTypeFamily.NUMERIC)
            .returnTypeNullable(SqlTypeName.DOUBLE)
            .functionCategory(SqlFunctionCategory.NUMERIC)
            .build(),
        druidFunctionName
    );
  }
}
