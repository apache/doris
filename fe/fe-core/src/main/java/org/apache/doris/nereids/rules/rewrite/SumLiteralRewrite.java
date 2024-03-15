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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.BinaryArithmetic;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;
import org.apache.thrift.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * sum(expr +/- literal) ==> sum(expr) +/- literal * count(expr)
 */
public class SumLiteralRewrite extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate()
                .whenNot(agg -> agg.getSourceRepeat().isPresent())
                .then(agg -> {
                    Map<NamedExpression, Pair<Expression, Literal>> sumLiteralMap = new HashMap<>();
                    for (NamedExpression namedExpression : agg.getOutputs()) {
                        Pair<NamedExpression, Pair<Expression, Literal>> pel = extractSumLiteral(namedExpression);
                        if (pel == null) {
                            continue;
                        }
                        sumLiteralMap.put(pel.first, pel.second);
                    }
                    if (sumLiteralMap.isEmpty()) {
                        return null;
                    }
                    return rewriteSumLiteral(agg, sumLiteralMap);
                }).toRule(RuleType.SUM_LITERAL_REWRITE);
    }

    private Plan rewriteSumLiteral(
            LogicalAggregate<?> agg, Map<NamedExpression, Pair<Expression, Literal>> sumLiteralMap) {
        Set<NamedExpression> newAggOutput = new HashSet<>();
        for (NamedExpression expr : agg.getOutputExpressions()) {
            if (!sumLiteralMap.containsKey(expr)) {
                newAggOutput.add(expr);
            }
        }

        Map<Expression, Slot> exprToSum = new HashMap<>();
        Map<Expression, Slot> exprToCount = new HashMap<>();

        Map<AggregateFunction, NamedExpression> existedAggFunc = new HashMap<>();
        for (NamedExpression e : agg.getOutputExpressions()) {
            if (e.children().size() == 1 && e.child(0) instanceof AggregateFunction) {
                existedAggFunc.put((AggregateFunction) e.child(0), e);
            }
        }

        Set<Expression> countSumExpr = new HashSet<>();
        for (Pair<Expression, Literal> pair : sumLiteralMap.values()) {
            countSumExpr.add(pair.first);
        }

        for (Expression e : countSumExpr) {
            NamedExpression namedSum = constructSum(e, existedAggFunc);
            NamedExpression namedCount = constructCount(e, existedAggFunc);
            exprToSum.put(e, namedSum.toSlot());
            exprToCount.put(e, namedCount.toSlot());
            newAggOutput.add(namedSum);
            newAggOutput.add(namedCount);
        }

        LogicalAggregate<?> newAgg = agg.withAggOutput(ImmutableList.copyOf(newAggOutput));

        List<NamedExpression> newProjects = constructProjectExpression(agg, sumLiteralMap, exprToSum, exprToCount);

        return new LogicalProject<>(newProjects, newAgg);
    }

    private List<NamedExpression> constructProjectExpression(
            LogicalAggregate<?> agg, Map<NamedExpression, Pair<Expression, Literal>> sumLiteralMap,
            Map<Expression, Slot> exprToSum, Map<Expression, Slot> exprToCount) {
        List<NamedExpression> newProjects = new ArrayList<>();
        for (NamedExpression namedExpr : agg.getOutputExpressions()) {
            if (!sumLiteralMap.containsKey(namedExpr)) {
                newProjects.add(namedExpr.toSlot());
                continue;
            }
            Expression originExpr = sumLiteralMap.get(namedExpr).first;
            Literal literal = sumLiteralMap.get(namedExpr).second;
            Expression newExpr;
            if (namedExpr.child(0).child(0) instanceof Add) {
                newExpr = new Add(exprToSum.get(originExpr),
                        new Multiply(literal, exprToCount.get(originExpr)));
            } else {
                newExpr = new Subtract(exprToSum.get(originExpr),
                        new Multiply(literal, exprToCount.get(originExpr)));
            }
            newProjects.add(new Alias(namedExpr.getExprId(), newExpr, namedExpr.getName()));
        }
        return newProjects;
    }

    private NamedExpression constructSum(Expression child, Map<AggregateFunction, NamedExpression> existedAggFunc) {
        Sum sum = new Sum(child);
        NamedExpression namedSum;
        if (existedAggFunc.containsKey(sum)) {
            namedSum = existedAggFunc.get(sum);
        } else {
            namedSum = new Alias(sum);
        }
        return namedSum;
    }

    private NamedExpression constructCount(Expression child, Map<AggregateFunction, NamedExpression> existedAggFunc) {
        Count count = new Count(child);
        NamedExpression namedCount;
        if (existedAggFunc.containsKey(count)) {
            namedCount = existedAggFunc.get(count);
        } else {
            namedCount = new Alias(count);
        }
        return namedCount;
    }

    private @Nullable Pair<NamedExpression, Pair<Expression, Literal>> extractSumLiteral(
            NamedExpression namedExpression) {
        if (namedExpression.children().size() != 1) {
            return null;
        }
        Expression func = namedExpression.child(0);
        if (!(func instanceof Sum)) {
            return null;
        }
        Expression child = func.child(0);
        if (!(child instanceof Add) && !(child instanceof Subtract)) {
            return null;
        }

        Expression left = ((BinaryArithmetic) child).left();
        Expression right = ((BinaryArithmetic) child).right();
        if (!(right.isLiteral() && left instanceof Slot)) {
            // right now, only support slot +/- literal
            return null;
        }
        if (!(right.getDataType().isIntegerLikeType() || right.getDataType().isFloatLikeType())) {
            // only support integer or float types
            return null;
        }
        return Pair.of(namedExpression, Pair.of(left, (Literal) right));
    }
}
