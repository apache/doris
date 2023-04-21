// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.BinaryOperator;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.functions.window.DenseRank;
import org.apache.doris.nereids.trees.expressions.functions.window.Rank;
import org.apache.doris.nereids.trees.expressions.functions.window.RowNumber;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Push down the 'filter' into the 'window'.
 * It will convert the filter condition to the 'limit value' and embed it to the 'window'.
 * But there are some restrictions, the details are explained below.
 * For example:
 * 'SELECT * FROM (
 *     SELECT *, ROW_NUMBER() OVER (ORDER BY b) AS row_number
 *     FROM t
 * ) AS tt WHERE row_number <= 100;'
 * The filter 'row_number <= 100' can be pushed down into the window operator.
 * The following will demonstrate how the plan changes:
 * Logical plan tree:
 *                 any_node
 *                   |
 *                filter (row_number <= 100)
 *                   |
 *                window (PARTITION BY a ORDER BY b)
 *                   |
 *                 any_node
 * transformed to:
 *                 any_node
 *                   |
 *                filter (row_number <= 100)
 *                   |
 *                window (PARTITION BY a ORDER BY b) [Limit: 100]
 *                   |
 *                 any_node
 */

public class PushdownFilterThroughWindow extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalFilter(logicalWindow()).then(filter -> {
            LogicalWindow<Plan> window = filter.child();

            if (window.getPartitionLimit() > 0) {
                // The window already has the partition limit, so return directly.
                return filter;
            }

            // Check the filter conditions. Now, we currently only support
            // simple conditions of the form 'column </<= constant'.
            // TODO: Support more complex situations in filter conditions.
            Set<Expression> conjuncts = filter.getConjuncts();
            if (conjuncts.size() != 1) {
                return filter;
            }

            Expression conjunct = conjuncts.iterator().next();
            if (!(conjunct instanceof LessThan || conjunct instanceof LessThanEqual)) {
                return filter;
            }

            BinaryOperator op = (BinaryOperator) conjunct;
            Expression leftChild = op.children().get(0);
            Expression rightChild = op.children().get(1);
            if (!(leftChild instanceof SlotReference) || !(rightChild instanceof IntegerLikeLiteral)) {
                return filter;
            }

            // Adjust the value for 'limitVal' based on the comparison operators.
            long limitVal = ((IntegerLikeLiteral) rightChild).getLongValue();
            if (conjunct instanceof LessThan) {
                limitVal--;
            }
            if (limitVal < 0) {
                return new LogicalEmptyRelation(filter.getOutput());
            }

            // Check the window function. There are some restrictions for window function:
            // 1. The number of window function should be 1.
            // 2. The window function should be one of the 'row_number()', 'rank()', 'dense_rank()'.
            // 3. The window type should be 'ROW'.
            // 4. The window frame should be 'UNBOUNDED' to 'CURRENT'.
            List<NamedExpression> windowExprs = window.getWindowExpressions();
            if (windowExprs.size() != 1) {
                return filter;
            }
            NamedExpression windowExpr = windowExprs.get(0);
            if (windowExpr.children().size() != 1 || !(windowExpr.child(0) instanceof WindowExpression)) {
                return filter;
            }

            // Check the column in filter conditions.
            // The column used in the filter condition must match the slot
            // reference for the window function result used as the alias.
            if (!checkSlotReferenceMatch(leftChild, windowExpr)) {
                return filter;
            }

            WindowExpression windowFunc = (WindowExpression) windowExpr.child(0);
            // Check the window function name.
            if (!checkWindowFuncName(windowFunc)) {
                return filter;
            }

            // Check the window type and window frame.
            if (!checkWindowFrame(windowFunc)) {
                return filter;
            }

            // Embedded the limit value to the window.
            window = window.withPartitionLimit(limitVal);
            Plan newFilter = filter.withChildren(window);

            return newFilter;
        }).toRule(RuleType.PUSHDOWN_FILTER_THROUGH_WINDOW);
    }

    private boolean checkSlotReferenceMatch(Expression slotRefInFilterExpr, NamedExpression windowExpr) {
        ExprId filterExprID = ((SlotReference) slotRefInFilterExpr).getExprId();
        ExprId windowExprID = windowExpr.getExprId();
        return filterExprID == windowExprID;
    }

    private boolean checkWindowFuncName(WindowExpression windowFunc) {
        return windowFunc.getFunction() instanceof RowNumber
            || windowFunc.getFunction() instanceof Rank
            || windowFunc.getFunction() instanceof DenseRank;
    }

    private boolean checkWindowFrame(WindowExpression windowFunc) {
        Optional<WindowFrame> windowFrame = windowFunc.getWindowFrame();
        if (windowFrame.isPresent()) {
            WindowFrame frame = windowFrame.get();
            return frame.getFrameUnits() == WindowFrame.FrameUnitsType.ROWS
                && frame.getLeftBoundary().getFrameBoundType() == WindowFrame.FrameBoundType.UNBOUNDED_PRECEDING
                && frame.getRightBoundary().getFrameBoundType() == WindowFrame.FrameBoundType.CURRENT_ROW;
        } else {
            return false;
        }
    }
}
