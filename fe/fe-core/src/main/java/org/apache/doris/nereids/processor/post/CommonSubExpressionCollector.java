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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference.ArrayItemSlot;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SessionVarGuardExpr;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ShortCircuitIf;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * collect common expr
 */
public class CommonSubExpressionCollector extends ExpressionVisitor<Integer, Boolean> {
    public final Map<Integer, Set<Expression>> commonExprByDepth = new HashMap<>();
    private final Map<Integer, Set<Expression>> expressionsByDepth = new HashMap<>();

    public int collect(Expression expr) {
        return expr.accept(this, expr instanceof Lambda);
    }

    @Override
    public Integer visit(Expression expr, Boolean inLambda) {
        return processExpressionWithChildren(expr.children(), expr, inLambda);
    }

    @Override
    public Integer visitShortCircuitIf(ShortCircuitIf expr, Boolean inLambda) {
        // A short-circuit expression is an evaluation boundary. Extracting anything from its
        // branches into an earlier projection layer would evaluate inactive branches eagerly.
        return 0;
    }

    @Override
    public Integer visitSessionVarGuardExpr(SessionVarGuardExpr expr, Boolean inLambda) {
        // Process SessionVarGuardExpr and its children together, don't process SessionVarGuardExpr child separately.
        return processExpressionWithChildren(expr.child(0).children(), expr, inLambda);
    }

    private Integer processExpressionWithChildren(
            List<Expression> children, Expression expr, Boolean inLambda) {
        if (children.isEmpty()) {
            return 0;
        }
        return collectCommonExpressionByDepth(collectChildrenDepth(children, inLambda),
                expr, inLambda == null || inLambda);
    }

    private int collectChildrenDepth(List<Expression> children, Boolean inLambda) {
        return children.stream()
                .map(child -> child.accept(this, inLambda == null || inLambda || child instanceof Lambda))
                .reduce(Math::max)
                .map(depth -> depth + 1)
                .orElse(1);
    }

    private int collectCommonExpressionByDepth(int depth, Expression expr, boolean inLambda) {
        Set<Expression> expressions = getExpressionsFromDepthMap(depth, expressionsByDepth);
        // An expression that mentions a lambda argument bound outside it cannot be evaluated
        // outside that lambda: hoisting it into an earlier projection leaves the argument without
        // its binding. The check must not depend on the inLambda flag -- a lambda nested in another
        // expression can be visited again from a context that does not carry the flag (for example
        // when the same subtree appears in two projections), and the extraction then breaks the
        // lambda scope. An expression that *contains* the whole lambda, such as an array_map with
        // its lambda, binds its own arguments and stays extractable.
        // A lambda and its parameter references are not standalone values: only an expression that
        // contains the whole binding can be hoisted, never the binding pieces themselves.
        if (expressions.contains(expr)
                && !(expr instanceof WhenClause)
                && !(expr instanceof Lambda)
                && !(expr instanceof ArrayItemReference)
                && !hasFreeArrayItem(expr)) {
            Set<Expression> commonExpression = getExpressionsFromDepthMap(depth, commonExprByDepth);
            commonExpression.add(expr);
        }
        expressions.add(expr);
        return depth;
    }

    /**
     * Whether {@code expr} mentions a lambda argument that no lambda inside {@code expr} binds.
     *
     * <p>A lambda binds its parameter references for its body only, so the walk carries the set of
     * bound argument ids: entering a lambda adds its parameters, and an array item slot is free
     * when its id is not in that set.
     */
    private static boolean hasFreeArrayItem(Expression expr) {
        return hasFreeArrayItem(expr, new LinkedHashSet<>());
    }

    private static boolean hasFreeArrayItem(Expression expr, Set<ExprId> bound) {
        if (expr instanceof Lambda) {
            Set<ExprId> inner = new LinkedHashSet<>(bound);
            for (Expression child : expr.children()) {
                if (child instanceof ArrayItemReference) {
                    inner.add(((ArrayItemReference) child).getExprId());
                }
            }
            for (Expression child : expr.children()) {
                if (!(child instanceof ArrayItemReference) && hasFreeArrayItem(child, inner)) {
                    return true;
                }
            }
            return false;
        }
        if (expr instanceof ArrayItemSlot) {
            return !bound.contains(((ArrayItemSlot) expr).getExprId());
        }
        for (Expression child : expr.children()) {
            if (hasFreeArrayItem(child, bound)) {
                return true;
            }
        }
        return false;
    }

    public static Set<Expression> getExpressionsFromDepthMap(
            int depth, Map<Integer, Set<Expression>> depthMap) {
        return depthMap.computeIfAbsent(depth, d -> new LinkedHashSet<>());
    }
}
