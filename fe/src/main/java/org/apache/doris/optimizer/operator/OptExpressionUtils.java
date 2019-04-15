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

package org.apache.doris.optimizer.operator;

import com.google.common.collect.Lists;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.OptUtils;

import java.util.List;

public class OptExpressionUtils {
    // Unnest a given expression's child and append unnested nodes to
    // the given expression array
    private static void unnestChild(
            OptExpression expr, boolean isAnd,
            boolean isOr, boolean hasNegateChild,
            List<OptExpression> exprArray) {

        if ((isAnd && OptUtils.isItemAnd(expr)) || (isOr && OptUtils.isItemOr(expr))) {
            // two cascaded AND nodes or two cascaded OR nodes, recursively
            // pull-up children
            exprArray.addAll(unnestChildren(expr));
        }

        if (hasNegateChild && OptUtils.isItemNot(expr) && OptUtils.isItemNot(expr.getInput(0))) {
            expr = expr.getInput(0).getInput(0);
        }
        exprArray.add(unnestExpr(expr));
    }

    // Return an array of expression's children after unnesting nested
    // AND/OR/NOT subtrees
    private static List<OptExpression> unnestChildren(OptExpression expr) {
        boolean isAnd = OptUtils.isItemAnd(expr);
        boolean isOr = OptUtils.isItemOr(expr);
        boolean hasNegateChild = OptUtils.hasItemNotChild(expr);

        List<OptExpression> newInputs = Lists.newArrayList();
        for (OptExpression input : expr.getInputs()) {
            unnestChild(input, isAnd, isOr, hasNegateChild, newInputs);
        }
        return newInputs;
    }

    // Push not expression one level down the given expression. For example:
    // 1. AND of expressions into an OR a negation of these expression
    // 2. OR of expressions into an AND a negation of these expression
    // 3. EXISTS into NOT EXISTS and vice versa
    // 4. Else, return NOT of given expression
    private static OptExpression pushNotOneLevel(OptExpression expr) {
        boolean isAnd = OptUtils.isItemAnd(expr);
        boolean isOr = OptUtils.isItemOr(expr);
        if (isAnd || isOr) {
            OptOperator op;
            if (isAnd) {
                op = new OptItemCompoundPredicate(CompoundPredicate.Operator.OR);
            } else {
                op = new OptItemCompoundPredicate(CompoundPredicate.Operator.AND);
            }
            List<OptExpression> newInputs = Lists.newArrayList();
            for (OptExpression input : expr.getInputs()) {
                newInputs.add(OptUtils.createNegate(input));
            }
            return OptExpression.create(op, newInputs);
        }
        return OptUtils.createNegate(expr);
    }

    // Unnest AND/OR/NOT predicates
    public static OptExpression unnestExpr(OptExpression expr) {
        if (OptUtils.isItemNot(expr)) {
            OptExpression child = expr.getInput(0);
            OptExpression newExpr = pushNotOneLevel(child);
            List<OptExpression> unnestChildren = unnestChildren(newExpr);
            OptExpression.create(newExpr.getOp(), unnestChildren);
        }
        List<OptExpression> children = unnestChildren(expr);
        return OptExpression.create(expr.getOp(), children);
    }

    // Remove duplicate AND/OR children
    public static OptExpression dedupChildern(OptExpression expr) {
        List<OptExpression> newInputs = Lists.newArrayList();
        for (OptExpression input : expr.getInputs()) {
            newInputs.add(dedupChildern(input));
        }
        OptOperator op = expr.getOp();
        if (OptUtils.isItemAnd(expr) || OptUtils.isItemOr(expr)) {
            newInputs = OptUtils.dedupExpressions(newInputs);
            if (newInputs.size() == 1) {
                return newInputs.get(0);
            }
        }
        return OptExpression.create(expr.getOp(), newInputs);
    }
}
