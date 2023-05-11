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
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

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

            // We have already done such optimization rule, so just ignore it.
            if (window.child(0) instanceof LogicalPartitionTopN) {
                return filter;
            }

            // Check the window function. There are some restrictions for window function:
            // * The number of window function should be 1.
            // * The window function should be one of the 'row_number()', 'rank()', 'dense_rank()'.
            // * The window type should be 'ROW'.
            // * The window frame should be 'UNBOUNDED' to 'CURRENT'.
            // * The 'PARTITION' key and 'ORDER' key can not be empty at the same time.
            List<NamedExpression> windowExprs = window.getWindowExpressions();
            if (windowExprs.size() != 1) {
                return filter;
            }
            NamedExpression windowExpr = windowExprs.get(0);
            if (windowExpr.children().size() != 1 || !(windowExpr.child(0) instanceof WindowExpression)) {
                return filter;
            }

            WindowExpression windowFunc = (WindowExpression) windowExpr.child(0);
            // Check the window function name.
            if (!LogicalWindow.checkWindowFuncName4PartitionLimit(windowFunc)) {
                return filter;
            }

            // Check the partition key and order key.
            if (!LogicalWindow.checkWindowPartitionAndOrderKey4PartitionLimit(windowFunc)) {
                return filter;
            }

            // Check the window type and window frame.
            if (!LogicalWindow.checkWindowFrame4PartitionLimit(windowFunc)) {
                return filter;
            }

            // Check the filter conditions. Now, we currently only support simple conditions of the form
            // 'column </ <=/ = constant'. We will extract some related conjuncts and do some check.
            // Only all the related conjuncts are met the condition we will handle it.
            Set<Expression> conjuncts = filter.getConjuncts();
            boolean existsOrInConjuncts = conjuncts.stream().anyMatch(this::existOR);
            if (existsOrInConjuncts) {
                return filter;
            }

            Set<Expression> relatedConjuncts = extractRelatedConjuncts(conjuncts, windowExpr.getExprId());

            boolean hasPartitionLimit = false;
            long partitionLimit = Long.MAX_VALUE;

            for (Expression conjunct : relatedConjuncts) {
                Preconditions.checkArgument(conjunct instanceof BinaryOperator);
                BinaryOperator op = (BinaryOperator) conjunct;
                Expression leftChild = op.children().get(0);
                Expression rightChild = op.children().get(1);

                Preconditions.checkArgument(leftChild instanceof SlotReference
                        && rightChild instanceof IntegerLikeLiteral);

                long limitVal = ((IntegerLikeLiteral) rightChild).getLongValue();
                // Adjust the value for 'limitVal' based on the comparison operators.
                if (conjunct instanceof LessThan) {
                    limitVal--;
                }
                if (limitVal < 0) {
                    return new LogicalEmptyRelation(filter.getOutput());
                }
                if (hasPartitionLimit) {
                    partitionLimit = Math.min(partitionLimit, limitVal);
                }
                hasPartitionLimit = true;
            }

            if (!hasPartitionLimit) {
                return filter;
            }
            // return PartitionTopN -> Window -> Filter
            return filter.withChildren(window.withChildren(
                new LogicalPartitionTopN<>(windowFunc, false, partitionLimit, window.child(0))));
        }).toRule(RuleType.PUSHDOWN_FILTER_THROUGH_WINDOW);
    }

    private boolean existOR(Expression conjunct) {
        if (conjunct instanceof Or) {
            return true;
        }

        for (Expression child : conjunct.children()) {
            if (existOR(child)) {
                return true;
            }
        }
        return false;
    }

    private Set<Expression> extractRelatedConjuncts(Set<Expression> conjuncts, ExprId slotRefID) {
        Predicate<Expression> condition = conjunct -> {
            if (conjunct instanceof BinaryOperator) {
                return false;
            }
            BinaryOperator op = (BinaryOperator) conjunct;
            Expression leftChild = op.children().get(0);
            Expression rightChild = op.children().get(1);

            if (!(conjunct instanceof LessThan || conjunct instanceof LessThanEqual || conjunct instanceof EqualTo)) {
                return false;
            }

            if (!(leftChild instanceof SlotReference) || !(rightChild instanceof IntegerLikeLiteral)) {
                return false;
            }
            return ((SlotReference) leftChild).getExprId() == slotRefID;
        };

        Set<Expression> relatedConjuncts = conjuncts.stream()
                .filter(condition)
                .collect(ImmutableSet.toImmutableSet());
        return relatedConjuncts;

    }
}
