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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Used to generate the merge agg node for distributed execution.
 * NOTICE: GLOBAL output expressions' ExprId should SAME with ORIGIN output expressions' ExprId.
 * If we have a query: SELECT SUM(v1 * v2) + 1 FROM t GROUP BY k + 1
 * the initial plan is:
 *   Aggregate(phase: [GLOBAL], outputExpr: [Alias(k + 1) #1, Alias(SUM(v1 * v2) + 1) #2], groupByExpr: [k + 1])
 *   +-- childPlan
 * we should rewrite to:
 *   Aggregate(phase: [GLOBAL], outputExpr: [Alias(b) #1, Alias(SUM(a) + 1) #2], groupByExpr: [b])
 *   +-- Aggregate(phase: [LOCAL], outputExpr: [SUM(v1 * v2) as a, (k + 1) as b], groupByExpr: [k + 1])
 *       +-- childPlan
 *
 * Distinct Agg With Group By Processing:
 * If we have a query: SELECT count(distinct v1 * v2) + 1 FROM t GROUP BY k + 1
 * the initial plan is:
 *   Aggregate(phase: [GLOBAL], outputExpr: [Alias(k + 1) #1, Alias(COUNT(distinct v1 * v2) + 1) #2]
 *                            , groupByExpr: [k + 1])
 *   +-- childPlan
 * we should rewrite to:
 *   Aggregate(phase: [DISTINCT_LOCAL], outputExpr: [Alias(b) #1, Alias(COUNT(distinct a) + 1) #2], groupByExpr: [b])
 *   +-- Aggregate(phase: [GLOBAL], outputExpr: [b, a], groupByExpr: [b, a])
 *       +-- Aggregate(phase: [LOCAL], outputExpr: [(k + 1) as b, (v1 * v2) as a], groupByExpr: [k + 1, a])
 *           +-- childPlan
 *
 * TODO:
 *     1. use different class represent different phase aggregate
 *     2. if instance count is 1, shouldn't disassemble the agg plan
 */
public class AggregateDisassemble extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalAggregate()
                .whenNot(LogicalAggregate::isDisassembled)
                .then(aggregate -> {
                    // used in secondDisassemble to transform local expressions into global
                    final Map<Expression, Expression> globalOutputSMap = Maps.newHashMap();
                    // used in secondDisassemble to transform local expressions into global
                    final Map<Expression, Expression> globalGroupBySMap = Maps.newHashMap();
                    Pair<LogicalAggregate, Boolean> ret = firstDisassemble(aggregate, globalOutputSMap,
                            globalGroupBySMap);
                    if (!ret.second) {
                        return ret.first;
                    }
                    return secondDisassemble(ret.first, globalOutputSMap, globalGroupBySMap);
                }).toRule(RuleType.AGGREGATE_DISASSEMBLE);
    }

    // only support distinct function with group by
    // TODO: support distinct function without group by. (add second global phase)
    private LogicalAggregate secondDisassemble(
            LogicalAggregate<LogicalAggregate> aggregate,
            Map<Expression, Expression> globalOutputSMap,
            Map<Expression, Expression> globalGroupBySMap) {
        LogicalAggregate<GroupPlan> local = aggregate.child();
        // replace expression in globalOutputExprs and globalGroupByExprs
        List<NamedExpression> globalOutputExprs = local.getOutputExpressions().stream()
                .map(e -> ExpressionUtils.replace(e, globalOutputSMap))
                .map(NamedExpression.class::cast)
                .collect(Collectors.toList());
        List<Expression> globalGroupByExprs = local.getGroupByExpressions().stream()
                .map(e -> ExpressionUtils.replace(e, globalGroupBySMap))
                .collect(Collectors.toList());

        // generate new plan
        LogicalAggregate globalAggregate = new LogicalAggregate<>(
                globalGroupByExprs,
                globalOutputExprs,
                true,
                aggregate.isNormalized(),
                false,
                AggPhase.GLOBAL,
                local
        );
        return new LogicalAggregate<>(
                aggregate.getGroupByExpressions(),
                aggregate.getOutputExpressions(),
                true,
                aggregate.isNormalized(),
                true,
                AggPhase.DISTINCT_LOCAL,
                globalAggregate
        );
    }

    private Pair<LogicalAggregate, Boolean> firstDisassemble(
            LogicalAggregate<GroupPlan> aggregate,
            Map<Expression, Expression> globalOutputSMap,
            Map<Expression, Expression> globalGroupBySMap) {
        Boolean hasDistinct = Boolean.FALSE;
        List<NamedExpression> originOutputExprs = aggregate.getOutputExpressions();
        List<Expression> originGroupByExprs = aggregate.getGroupByExpressions();
        Map<Expression, Expression> inputSubstitutionMap = Maps.newHashMap();

        // 1. generate a map from local aggregate output to global aggregate expr substitution.
        //    inputSubstitutionMap use for replacing expression in global aggregate
        //    replace rule is:
        //        a: Expression is a group by key and is a slot reference. e.g. group by k1
        //        b. Expression is a group by key and is an expression. e.g. group by k1 + 1
        //        c. Expression is an aggregate function. e.g. sum(v1) in select list
        //    +-----------+---------------------+-------------------------+--------------------------------+
        //    | situation | origin expression   | local output expression | expression in global aggregate |
        //    +-----------+---------------------+-------------------------+--------------------------------+
        //    | a         | Ref(k1)#1           | Ref(k1)#1               | Ref(k1)#1                      |
        //    +-----------+---------------------+-------------------------+--------------------------------+
        //    | b         | Ref(k1)#1 + 1       | A(Ref(k1)#1 + 1, key)#2 | Ref(key)#2                     |
        //    +-----------+---------------------+-------------------------+--------------------------------+
        //    | c         | A(AF(v1#1), 'af')#2 | A(AF(v1#1), 'af')#3     | AF(af#3)                       |
        //    +-----------+---------------------+-------------------------+--------------------------------+
        //    NOTICE: Ref: SlotReference, A: Alias, AF: AggregateFunction, #x: ExprId x
        // 2. collect local aggregate output expressions and local aggregate group by expression list
        List<Expression> localGroupByExprs = aggregate.getGroupByExpressions();
        List<NamedExpression> localOutputExprs = Lists.newArrayList();
        for (Expression originGroupByExpr : originGroupByExprs) {
            if (inputSubstitutionMap.containsKey(originGroupByExpr)) {
                continue;
            }
            if (originGroupByExpr instanceof SlotReference) {
                inputSubstitutionMap.put(originGroupByExpr, originGroupByExpr);
                globalOutputSMap.put(originGroupByExpr, originGroupByExpr);
                globalGroupBySMap.put(originGroupByExpr, originGroupByExpr);
                localOutputExprs.add((SlotReference) originGroupByExpr);
            } else {
                NamedExpression localOutputExpr = new Alias(originGroupByExpr, originGroupByExpr.toSql());
                inputSubstitutionMap.put(originGroupByExpr, localOutputExpr.toSlot());
                globalOutputSMap.put(localOutputExpr, localOutputExpr.toSlot());
                globalGroupBySMap.put(originGroupByExpr, localOutputExpr.toSlot());
                localOutputExprs.add(localOutputExpr);
            }
        }
        List<Expression> distinctExprsForLocalGroupBy = Lists.newArrayList();
        List<NamedExpression> distinctExprsForLocalOutput = Lists.newArrayList();
        for (NamedExpression originOutputExpr : originOutputExprs) {
            Set<AggregateFunction> aggregateFunctions
                    = originOutputExpr.collect(AggregateFunction.class::isInstance);
            for (AggregateFunction aggregateFunction : aggregateFunctions) {
                if (inputSubstitutionMap.containsKey(aggregateFunction)) {
                    continue;
                }
                if (aggregateFunction.isDistinct()) {
                    hasDistinct = Boolean.TRUE;
                    for (Expression expr : aggregateFunction.children()) {
                        if (expr instanceof SlotReference) {
                            distinctExprsForLocalOutput.add((SlotReference) expr);
                            if (!inputSubstitutionMap.containsKey(expr)) {
                                inputSubstitutionMap.put(expr, expr);
                                globalOutputSMap.put(expr, expr);
                                globalGroupBySMap.put(expr, expr);
                            }
                        } else {
                            NamedExpression globalOutputExpr = new Alias(expr, expr.toSql());
                            distinctExprsForLocalOutput.add(globalOutputExpr);
                            if (!inputSubstitutionMap.containsKey(expr)) {
                                inputSubstitutionMap.put(expr, globalOutputExpr.toSlot());
                                globalOutputSMap.put(globalOutputExpr, globalOutputExpr.toSlot());
                                globalGroupBySMap.put(expr, globalOutputExpr.toSlot());
                            }
                        }
                        distinctExprsForLocalGroupBy.add(expr);
                    }
                    continue;
                }
                NamedExpression localOutputExpr = new Alias(aggregateFunction, aggregateFunction.toSql());
                Expression substitutionValue = aggregateFunction.withChildren(
                        Lists.newArrayList(localOutputExpr.toSlot()));
                inputSubstitutionMap.put(aggregateFunction, substitutionValue);
                globalOutputSMap.put(aggregateFunction, substitutionValue);
                localOutputExprs.add(localOutputExpr);
            }
        }

        // 3. replace expression in globalOutputExprs and globalGroupByExprs
        List<NamedExpression> globalOutputExprs = aggregate.getOutputExpressions().stream()
                .map(e -> ExpressionUtils.replace(e, inputSubstitutionMap))
                .map(NamedExpression.class::cast)
                .collect(Collectors.toList());
        List<Expression> globalGroupByExprs = localGroupByExprs.stream()
                .map(e -> ExpressionUtils.replace(e, inputSubstitutionMap)).collect(Collectors.toList());
        // To avoid repeated substitution of distinct expressions,
        // here the expressions are put into the local after the substitution is completed
        localOutputExprs.addAll(distinctExprsForLocalOutput);
        localGroupByExprs.addAll(distinctExprsForLocalGroupBy);
        // 4. generate new plan
        LogicalAggregate localAggregate = new LogicalAggregate<>(
                localGroupByExprs,
                localOutputExprs,
                true,
                aggregate.isNormalized(),
                false,
                AggPhase.LOCAL,
                aggregate.child()
        );
        return Pair.of(new LogicalAggregate<>(
                globalGroupByExprs,
                globalOutputExprs,
                true,
                aggregate.isNormalized(),
                true,
                AggPhase.GLOBAL,
                localAggregate
        ), hasDistinct);
    }
}
