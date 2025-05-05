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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import java.util.HashMap;
import java.util.LinkedHashSet;
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
        if (expr.children().isEmpty()) {
            return 0;
        }
        return collectCommonExpressionByDepth(
                expr.children()
                        .stream()
                        .map(child -> child.accept(this, inLambda == null || inLambda || child instanceof Lambda))
                        .reduce(Math::max)
                        .map(m -> m + 1)
                        .orElse(1),
                expr,
                inLambda == null || inLambda
        );
    }

    private int collectCommonExpressionByDepth(int depth, Expression expr, boolean inLambda) {
        Set<Expression> expressions = getExpressionsFromDepthMap(depth, expressionsByDepth);
        // ArrayItemSlot and ArrayItemReference could not be common expressions
        // TODO: could not extract common expression when expression contains same lambda expression
        //   because ArrayItemSlot in Lambda are not same.
        if (expressions.contains(expr)
                && !(inLambda && expr.containsType(ArrayItemSlot.class, ArrayItemReference.class))) {
            Set<Expression> commonExpression = getExpressionsFromDepthMap(depth, commonExprByDepth);
            commonExpression.add(expr);
        }
        expressions.add(expr);
        return depth;
    }

    public static Set<Expression> getExpressionsFromDepthMap(
            int depth, Map<Integer, Set<Expression>> depthMap) {
        return depthMap.computeIfAbsent(depth, d -> new LinkedHashSet<>());
    }
}
