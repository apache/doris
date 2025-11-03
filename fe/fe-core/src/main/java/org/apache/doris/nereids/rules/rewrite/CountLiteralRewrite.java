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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * count(1) ==> count(*)
 * count(null) ==> 0
 */
public class CountLiteralRewrite extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate().then(
                agg -> {
                    List<NamedExpression> newExprs = Lists.newArrayListWithCapacity(agg.getOutputExpressions().size());
                    if (!rewriteCountLiteral(agg.getOutputExpressions(), newExprs)) {
                        // no need to rewrite
                        return agg;
                    }

                    List<NamedExpression> projectFuncs = Lists.newArrayListWithCapacity(newExprs.size());
                    Builder<NamedExpression> aggFuncsBuilder
                            = ImmutableList.builderWithExpectedSize(newExprs.size());
                    for (NamedExpression newExpr : newExprs) {
                        if (newExpr.isConstant()) {
                            projectFuncs.add(newExpr);
                        } else {
                            aggFuncsBuilder.add(newExpr);
                        }
                    }

                    List<NamedExpression> aggFuncs = aggFuncsBuilder.build();
                    if (aggFuncs.isEmpty()) {
                        // if there is no group by keys and other agg func, don't rewrite
                        return null;
                    } else {
                        // if there is group by keys, put count(null) in projects, such as
                        // project(0 as count(null))
                        // --Aggregate(k1, group by k1)
                        Plan plan = agg.withAggOutput(aggFuncs);
                        if (!projectFuncs.isEmpty()) {
                            for (NamedExpression aggFunc : aggFuncs) {
                                projectFuncs.add(aggFunc.toSlot());
                            }
                            plan = new LogicalProject<>(projectFuncs, plan);
                        }
                        return plan;
                    }
                }
        ).toRule(RuleType.COUNT_LITERAL_REWRITE);
    }

    private boolean rewriteCountLiteral(List<NamedExpression> oldExprs, List<NamedExpression> newExprs) {
        boolean changed = false;
        for (Expression expr : oldExprs) {
            Map<Expression, Expression> replaced = new HashMap<>();
            Set<AggregateFunction> oldAggFuncSet = expr.collect(AggregateFunction.class::isInstance);
            for (AggregateFunction aggFun : oldAggFuncSet) {
                if (isCountLiteral(aggFun)) {
                    replaced.put(aggFun, rewrite((Count) aggFun));
                }
            }
            expr = expr.rewriteUp(s -> replaced.getOrDefault(s, s));
            changed |= !replaced.isEmpty();
            newExprs.add((NamedExpression) expr);
        }
        return changed;
    }

    private boolean isCountLiteral(AggregateFunction aggFunc) {
        return !aggFunc.isDistinct()
                && aggFunc instanceof Count
                && aggFunc.children().size() == 1
                && aggFunc.child(0).isLiteral();
    }

    private Expression rewrite(Count count) {
        if (count.child(0).isNullLiteral()) {
            return new BigIntLiteral(0);
        }
        return new Count();
    }
}
