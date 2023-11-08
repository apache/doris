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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.BinaryOperator;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Push down the 'partitionTopN' into the 'window'.
 * It will convert the filter condition to the 'limit value' and push down below the 'window'.
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
 *                window (PARTITION BY a ORDER BY b)
 *                   |
 *                partition_topn(PARTITION BY: a, ORDER BY b, Partition Limit: 100)
 *                   |
 *                 any_node
 */

public class CreatePartitionTopNFromWindow extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalFilter(logicalWindow()).thenApply(ctx -> {
            LogicalFilter<LogicalWindow<Plan>> filter = ctx.root;
            LogicalWindow<Plan> window = filter.child();

            // We have already done such optimization rule, so just ignore it.
            if (window.child(0) instanceof LogicalPartitionTopN
                    || (window.child(0) instanceof LogicalFilter
                    && window.child(0).child(0) != null
                    && window.child(0).child(0) instanceof LogicalPartitionTopN)) {
                return filter;
            }

            List<NamedExpression> windowExprs = window.getWindowExpressions();
            if (windowExprs.size() != 1) {
                return filter;
            }
            NamedExpression windowExpr = windowExprs.get(0);
            if (windowExpr.children().size() != 1 || !(windowExpr.child(0) instanceof WindowExpression)) {
                return filter;
            }

            // Check the filter conditions. Now, we currently only support simple conditions of the form
            // 'column </ <=/ = constant'. We will extract some related conjuncts and do some check.
            Set<Expression> conjuncts = filter.getConjuncts();
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
                    return new LogicalEmptyRelation(ctx.statementContext.getNextRelationId(), filter.getOutput());
                }
                if (hasPartitionLimit) {
                    partitionLimit = Math.min(partitionLimit, limitVal);
                } else {
                    partitionLimit = limitVal;
                    hasPartitionLimit = true;
                }
            }

            if (!hasPartitionLimit) {
                return filter;
            }

            Optional<Plan> newWindow = window.pushPartitionLimitThroughWindow(partitionLimit, false);
            if (!newWindow.isPresent()) {
                return filter;
            }
            return filter.withChildren(newWindow.get());
        }).toRule(RuleType.CREATE_PARTITION_TOPN_FOR_WINDOW);
    }

    private Set<Expression> extractRelatedConjuncts(Set<Expression> conjuncts, ExprId slotRefID) {
        Predicate<Expression> condition = conjunct -> {
            if (!(conjunct instanceof BinaryOperator)) {
                return false;
            }
            BinaryOperator op = (BinaryOperator) conjunct;
            Expression leftChild = op.children().get(0);
            Expression rightChild = op.children().get(1);

            if (!(conjunct instanceof LessThan || conjunct instanceof LessThanEqual || conjunct instanceof EqualTo)) {
                return false;
            }

            // TODO: Now, we only support the column on the left side.
            if (!(leftChild instanceof SlotReference) || !(rightChild instanceof IntegerLikeLiteral)) {
                return false;
            }
            return ((SlotReference) leftChild).getExprId() == slotRefID;
        };

        return conjuncts.stream()
                .filter(condition)
                .collect(ImmutableSet.toImmutableSet());
    }
}
