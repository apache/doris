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
import java.util.Map.Entry;
import java.util.Objects;
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
                    Map<NamedExpression, Pair<SumInfo, Literal>> sumLiteralMap = new HashMap<>();
                    for (NamedExpression namedExpression : agg.getOutputs()) {
                        Pair<NamedExpression, Pair<SumInfo, Literal>> pel = extractSumLiteral(namedExpression);
                        if (pel == null) {
                            continue;
                        }
                        sumLiteralMap.put(pel.first, pel.second);
                    }
                    Map<NamedExpression, Pair<SumInfo, Literal>> validSumLiteralMap =
                            removeOneSumLiteral(sumLiteralMap);
                    if (validSumLiteralMap.isEmpty()) {
                        return null;
                    }
                    return rewriteSumLiteral(agg, validSumLiteralMap);
                }).toRule(RuleType.SUM_LITERAL_REWRITE);
    }

    // when there only one sum literal like select count(id1 + 1), count(id2 + 1) from t, we don't rewrite them.
    private Map<NamedExpression, Pair<SumInfo, Literal>> removeOneSumLiteral(
            Map<NamedExpression, Pair<SumInfo, Literal>> sumLiteralMap) {
        Map<Expression, Integer> countSum = new HashMap<>();
        for (Entry<NamedExpression, Pair<SumInfo, Literal>> e : sumLiteralMap.entrySet()) {
            Expression expr = e.getValue().first.expr;
            countSum.merge(expr, 1, Integer::sum);
        }
        Map<NamedExpression, Pair<SumInfo, Literal>> validSumLiteralMap = new HashMap<>();
        for (Entry<NamedExpression, Pair<SumInfo, Literal>> e : sumLiteralMap.entrySet()) {
            Expression expr = e.getValue().first.expr;
            if (countSum.get(expr) > 1) {
                validSumLiteralMap.put(e.getKey(), e.getValue());
            }
        }
        return validSumLiteralMap;
    }

    private Plan rewriteSumLiteral(
            LogicalAggregate<?> agg, Map<NamedExpression, Pair<SumInfo, Literal>> sumLiteralMap) {
        Set<NamedExpression> newAggOutput = new HashSet<>();
        for (NamedExpression expr : agg.getOutputExpressions()) {
            if (!sumLiteralMap.containsKey(expr)) {
                newAggOutput.add(expr);
            }
        }

        Map<SumInfo, Slot> exprToSum = new HashMap<>();
        Map<SumInfo, Slot> exprToCount = new HashMap<>();

        Map<AggregateFunction, NamedExpression> existedAggFunc = new HashMap<>();
        for (NamedExpression e : agg.getOutputExpressions()) {
            if (e.children().size() == 1 && e.child(0) instanceof AggregateFunction) {
                existedAggFunc.put((AggregateFunction) e.child(0), e);
            }
        }

        Set<SumInfo> countSumExpr = new HashSet<>();
        for (Pair<SumInfo, Literal> pair : sumLiteralMap.values()) {
            countSumExpr.add(pair.first);
        }

        for (SumInfo info : countSumExpr) {
            NamedExpression namedSum = constructSum(info, existedAggFunc);
            NamedExpression namedCount = constructCount(info, existedAggFunc);
            exprToSum.put(info, namedSum.toSlot());
            exprToCount.put(info, namedCount.toSlot());
            newAggOutput.add(namedSum);
            newAggOutput.add(namedCount);
        }

        LogicalAggregate<?> newAgg = agg.withAggOutput(ImmutableList.copyOf(newAggOutput));

        List<NamedExpression> newProjects = constructProjectExpression(agg, sumLiteralMap, exprToSum, exprToCount);

        return new LogicalProject<>(newProjects, newAgg);
    }

    private List<NamedExpression> constructProjectExpression(
            LogicalAggregate<?> agg, Map<NamedExpression, Pair<SumInfo, Literal>> sumLiteralMap,
            Map<SumInfo, Slot> exprToSum, Map<SumInfo, Slot> exprToCount) {
        List<NamedExpression> newProjects = new ArrayList<>();
        for (NamedExpression namedExpr : agg.getOutputExpressions()) {
            if (!sumLiteralMap.containsKey(namedExpr)) {
                newProjects.add(namedExpr.toSlot());
                continue;
            }
            SumInfo originExpr = sumLiteralMap.get(namedExpr).first;
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

    private NamedExpression constructSum(SumInfo info, Map<AggregateFunction, NamedExpression> existedAggFunc) {
        Sum sum = new Sum(info.isDistinct, info.isAlwaysNullable, info.expr);
        NamedExpression namedSum;
        if (existedAggFunc.containsKey(sum)) {
            namedSum = existedAggFunc.get(sum);
        } else {
            namedSum = new Alias(sum);
        }
        return namedSum;
    }

    private NamedExpression constructCount(SumInfo info, Map<AggregateFunction, NamedExpression> existedAggFunc) {
        Count count = new Count(info.isDistinct, info.expr);
        NamedExpression namedCount;
        if (existedAggFunc.containsKey(count)) {
            namedCount = existedAggFunc.get(count);
        } else {
            namedCount = new Alias(count);
        }
        return namedCount;
    }

    private @Nullable Pair<NamedExpression, Pair<SumInfo, Literal>> extractSumLiteral(
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
        SumInfo info = new SumInfo(left, ((Sum) func).isDistinct(), ((Sum) func).isAlwaysNullable());
        return Pair.of(namedExpression, Pair.of(info, (Literal) right));
    }

    static class SumInfo {
        Expression expr;
        boolean isDistinct;
        boolean isAlwaysNullable;

        SumInfo(Expression expr, boolean isDistinct, boolean isAlwaysNullable) {
            this.expr = expr;
            this.isDistinct = isDistinct;
            this.isAlwaysNullable = isAlwaysNullable;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SumInfo sumInfo = (SumInfo) o;

            if (isDistinct != sumInfo.isDistinct) {
                return false;
            }
            if (isAlwaysNullable != sumInfo.isAlwaysNullable) {
                return false;
            }
            return Objects.equals(expr, sumInfo.expr);
        }

        @Override
        public int hashCode() {
            return Objects.hash(expr, isDistinct, isAlwaysNullable);
        }
    }
}
