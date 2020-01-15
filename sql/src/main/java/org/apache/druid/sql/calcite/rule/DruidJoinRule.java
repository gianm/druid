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

package org.apache.druid.sql.calcite.rule;

import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.druid.sql.calcite.rel.DruidJoinQueryRel;
import org.apache.druid.sql.calcite.rel.DruidQueryRel;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class DruidJoinRule extends RelOptRule
{
  private static final DruidJoinRule INSTANCE = new DruidJoinRule();

  private DruidJoinRule()
  {
    super(
        operand(
            Join.class,
            operand(DruidRel.class, none()),
            operand(DruidRel.class, none())
        )
    );
  }

  public static DruidJoinRule instance()
  {
    return INSTANCE;
  }

  /**
   * Check if a druidRel is a simple table scan, or a projection that merely remaps columns without transforming them.
   * Like {@link #isScanOrProject} but more restrictive: only remappings are allowed.
   *
   * @param druidRel  the rel to check
   * @param canBeJoin consider a 'join' that doesn't do anything fancy to be a scan-or-mapping too.
   */
  public static boolean isScanOrMapping(final DruidRel<?> druidRel, final boolean canBeJoin)
  {
    if (isScanOrProject(druidRel, canBeJoin)) {
      // Like isScanOrProject, but don't allow transforming projections.
      final PartialDruidQuery partialQuery = druidRel.getPartialDruidQuery();
      return partialQuery.getSelectProject() == null || partialQuery.getSelectProject().isMapping();
    } else {
      return false;
    }
  }

  /**
   * Check if a druidRel is a simple table scan or a scan + projection.
   *
   * @param druidRel  the rel to check
   * @param canBeJoin consider a 'join' that doesn't do anything fancy to be a scan-or-mapping too.
   */
  private static boolean isScanOrProject(final DruidRel<?> druidRel, final boolean canBeJoin)
  {
    if (druidRel instanceof DruidQueryRel || (canBeJoin && druidRel instanceof DruidJoinQueryRel)) {
      final PartialDruidQuery partialQuery = druidRel.getPartialDruidQuery();
      final PartialDruidQuery.Stage stage = partialQuery.stage();
      return stage.compareTo(PartialDruidQuery.Stage.SELECT_PROJECT) <= 0 && partialQuery.getWhereFilter() == null;
    } else {
      return false;
    }
  }

  @Override
  public boolean matches(RelOptRuleCall call)
  {
    final DruidRel right = call.rel(2);

    // Right must not be a join (we want to generate left-heavy join operation trees).
    return !(right instanceof DruidJoinQueryRel);
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Join join = call.rel(0);
    final DruidRel left = call.rel(1);
    final DruidRel right = call.rel(2);

    if (canHandleCondition(join)) {
      call.transformTo(
          DruidJoinQueryRel.create(
              join,
              isScanOrMapping(left, false),
              isScanOrMapping(right, false),
              left.getQueryMaker()
          )
      );
    }
  }

  /**
   * Returns true if this condition is an AND of equality conditions of the form: f(LeftRel) = RightColumn.
   */
  private static boolean canHandleCondition(final Join join)
  {
    final RexNode condition = join.getCondition();
    final List<RexNode> subConditions = decomposeAnd(condition);

    for (RexNode subCondition : subConditions) {
      if (subCondition.isA(SqlKind.LITERAL)) {
        // Literals are always OK.
        continue;
      }

      if (!subCondition.isA(SqlKind.EQUALS)) {
        // If it's not EQUALS, it's not supported.
        return false;
      }

      final List<RexNode> operands = ((RexCall) subCondition).getOperands();
      Preconditions.checkState(operands.size() == 2, "WTF?! Expected 2 operands, got[%,d]", operands.size());

      final int numLeftFields = join.getLeft().getRowType().getFieldList().size();

      final boolean rhsIsFieldOfRightRel =
          operands.get(1).isA(SqlKind.INPUT_REF)
          && ((RexInputRef) operands.get(1)).getIndex() >= numLeftFields;

      final boolean lhsIsExpressionOfLeftRel =
          RelOptUtil.InputFinder.bits(operands.get(0)).intersects(ImmutableBitSet.range(numLeftFields));

      if (!(lhsIsExpressionOfLeftRel && rhsIsFieldOfRightRel)) {
        // Cannot handle this condition.
        return false;
      }
    }

    return true;
  }

  private static List<RexNode> decomposeAnd(final RexNode condition)
  {
    final List<RexNode> retVal = new ArrayList<>();
    final Stack<RexNode> stack = new Stack<>();

    stack.push(condition);

    while (!stack.empty()) {
      final RexNode current = stack.pop();

      if (current.isA(SqlKind.AND)) {
        ((RexCall) current).getOperands().forEach(stack::push);
      } else {
        retVal.add(current);
      }
    }

    return retVal;
  }
}
