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
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.generator.TableGeneratingFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Sleep;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
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
 * count(const_expr) ==> if(count(*) = 0, 0, if(const_expr is null, 0, count(*)))
 */
public class CountLiteralRewrite extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate().then(
                agg -> {
                    List<NamedExpression> newExprs = Lists.newArrayListWithCapacity(agg.getOutputExpressions().size());
                    if (!rewriteCountLiteral(agg.getOutputExpressions(), newExprs, !agg.isNormalized())) {
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

    private boolean rewriteCountLiteral(List<NamedExpression> oldExprs, List<NamedExpression> newExprs,
            boolean rewriteConstantExpression) {
        boolean changed = false;
        for (Expression expr : oldExprs) {
            Map<Expression, Expression> replaced = new HashMap<>();
            Set<AggregateFunction> oldAggFuncSet = expr.collect(AggregateFunction.class::isInstance);
            for (AggregateFunction aggFun : oldAggFuncSet) {
                if (isCountLiteral(aggFun) || (rewriteConstantExpression && isCountConstantExpression(aggFun))) {
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

    private boolean isCountConstantExpression(AggregateFunction aggFunc) {
        if (aggFunc.isDistinct()
                || !(aggFunc instanceof Count)
                || aggFunc.children().size() != 1) {
            return false;
        }
        Expression arg = aggFunc.child(0);
        return !arg.isLiteral()
                && arg.foldable()
                && !arg.containsNondeterministic()
                && !arg.containsVolatileExpression()
                && arg.getInputSlots().isEmpty()
                && !arg.containsType(Sleep.class)
                && !arg.containsType(AggregateFunction.class, SubqueryExpr.class,
                        TableGeneratingFunction.class, WindowExpression.class);
    }

    private Expression rewrite(Count count) {
        if (count.child(0).isNullLiteral()) {
            return new BigIntLiteral(0);
        }
        if (!count.child(0).isLiteral()) {
            Expression countStar = new Count();
            Expression constExpr = deferConstantEvaluation(count.child(0));
            Expression ifConstantExprIsNull = new If(new IsNull(constExpr), new BigIntLiteral(0), countStar);
            return new If(new EqualTo(countStar, new BigIntLiteral(0)), new BigIntLiteral(0), ifConstantExprIsNull);
        }
        return new Count();
    }

    /*
     * count(const_expr) should not evaluate const_expr when the aggregate input is empty.
     * The outer count(*) = 0 guard handles that at execution time, but BE opens constant
     * expression trees eagerly. Add an unreachable count(*) dependency inside each constant
     * subtree so that it is no longer treated as a fragment-local constant before the guard
     * can short-circuit it.
     */
    private Expression deferConstantEvaluation(Expression expression) {
        if (expression.isLiteral()) {
            return expression;
        }
        List<Expression> children = expression.children();
        List<Expression> newChildren = Lists.newArrayListWithCapacity(children.size());
        boolean changed = false;
        for (Expression child : children) {
            Expression newChild = deferConstantEvaluation(child);
            newChildren.add(newChild);
            changed |= newChild != child;
        }

        Expression rewritten = changed ? expression.withChildren(newChildren) : expression;
        if (rewritten.isConstant() && !rewritten.isLiteral() && !rewritten.children().isEmpty()) {
            newChildren = Lists.newArrayList(rewritten.children());
            newChildren.set(0, dependOnCountStar(newChildren.get(0)));
            rewritten = rewritten.withChildren(newChildren);
        }
        return rewritten;
    }

    /*
     * count(*) is always non-negative, so the NULL branch is unreachable. It is still needed
     * to keep simplification from reducing the wrapper back to the original expression.
     */
    private Expression dependOnCountStar(Expression expression) {
        return new If(new GreaterThanEqual(new Count(), new BigIntLiteral(0)),
                expression, new NullLiteral(expression.getDataType()));
    }
}
