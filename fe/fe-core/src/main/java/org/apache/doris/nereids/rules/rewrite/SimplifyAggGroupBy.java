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
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.BinaryArithmetic;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

/**
 * Simplify Aggregate group by Multiple to One. For example
 * <p>
 * GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3
 * -->
 * GROUP BY ClientIP
 */
public class SimplifyAggGroupBy extends OneRewriteRuleFactory {
    private static final ImmutableSet<Class<? extends Expression>> supportedFunctions
            = ImmutableSet.of(Add.class, Subtract.class, Multiply.class, Divide.class);

    @Override
    public Rule build() {
        return logicalAggregate()
                .when(agg -> agg.getGroupByExpressions().size() > 1
                        && ExpressionUtils.allMatch(agg.getGroupByExpressions(),
                        SimplifyAggGroupBy::isBinaryArithmeticSlot))
                .then(agg -> {
                    List<Expression> groupByExpressions = agg.getGroupByExpressions();
                    ImmutableSet.Builder<Expression> inputSlots
                            = ImmutableSet.builderWithExpectedSize(groupByExpressions.size());
                    for (Expression groupByExpression : groupByExpressions) {
                        inputSlots.addAll(groupByExpression.getInputSlots());
                    }
                    Set<Expression> slots = inputSlots.build();
                    if (slots.size() != 1) {
                        return null;
                    }
                    return agg.withGroupByAndOutput(Utils.fastToImmutableList(slots), agg.getOutputExpressions());
                })
                .toRule(RuleType.SIMPLIFY_AGG_GROUP_BY);
    }

    @VisibleForTesting
    protected static boolean isBinaryArithmeticSlot(Expression expr) {
        if (expr instanceof Slot) {
            return true;
        }
        if (!(expr instanceof BinaryArithmetic)) {
            return false;
        }
        if (!supportedFunctions.contains(expr.getClass())) {
            return false;
        }

        // Float/double arithmetic: precision loss for all operations
        if (expr.child(0).getDataType().isFloatLikeType()
                || expr.child(1).getDataType().isFloatLikeType()) {
            return false;
        }

        Expression slotExpr;
        Literal literal;
        if (expr.child(0) instanceof Literal) {
            literal = (Literal) expr.child(0);
            slotExpr = expr.child(1);
        } else if (expr.child(1) instanceof Literal) {
            literal = (Literal) expr.child(1);
            slotExpr = expr.child(0);
        } else {
            return false;
        }

        if (!canExtractSlot(slotExpr)) {
            return false;
        }

        return checkLiteral(expr, literal);
    }

    @VisibleForTesting
    protected static boolean checkLiteral(Expression expr, Literal literal) {
        if (literal.isNullLiteral()) {
            return false;
        }
        if (expr instanceof Multiply || expr instanceof Divide) {
            if (literal.isZero()) {
                return false;
            }
        }
        return true;
    }

    @VisibleForTesting
    protected static boolean canExtractSlot(Expression expr) {
        while (expr instanceof Cast) {
            Cast cast = (Cast) expr;
            Expression inner = cast.child();
            if (!inner.getDataType().isInjectiveCastTo(cast.getDataType())) {
                return false;
            }
            expr = inner;
        }
        return expr instanceof Slot;
    }

}
