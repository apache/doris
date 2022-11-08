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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.analysis.ArithmeticExpr.Operator;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * BindFunction.
 */
public class BindFunction implements AnalysisRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            RuleType.BINDING_ONE_ROW_RELATION_FUNCTION.build(
                logicalOneRowRelation().thenApply(ctx -> {
                    LogicalOneRowRelation oneRowRelation = ctx.root;
                    List<NamedExpression> projects = oneRowRelation.getProjects();
                    List<NamedExpression> boundProjects = bind(projects, ctx.connectContext.getEnv());
                    // TODO:
                    // trick logic: currently XxxRelation in GroupExpression always difference to each other,
                    // so this rule must check the expression whether is changed to prevent dead loop because
                    // new LogicalOneRowRelation can hit this rule too. we would remove code until the pr
                    // (@wangshuo128) mark the id in XxxRelation, then we can compare XxxRelation in
                    // GroupExpression by id
                    if (projects.equals(boundProjects)) {
                        return oneRowRelation;
                    }
                    return new LogicalOneRowRelation(boundProjects);
                })
            ),
            RuleType.BINDING_PROJECT_FUNCTION.build(
                logicalProject().thenApply(ctx -> {
                    LogicalProject<GroupPlan> project = ctx.root;
                    List<NamedExpression> boundExpr = bind(project.getProjects(), ctx.connectContext.getEnv());
                    return new LogicalProject<>(boundExpr, project.child());
                })
            ),
            RuleType.BINDING_AGGREGATE_FUNCTION.build(
                logicalAggregate().thenApply(ctx -> {
                    LogicalAggregate<GroupPlan> agg = ctx.root;
                    List<Expression> groupBy = bind(agg.getGroupByExpressions(), ctx.connectContext.getEnv());
                    List<NamedExpression> output = bind(agg.getOutputExpressions(), ctx.connectContext.getEnv());
                    return agg.withGroupByAndOutput(groupBy, output);
                })
            ),
            RuleType.BINDING_FILTER_FUNCTION.build(
               logicalFilter().thenApply(ctx -> {
                   LogicalFilter<GroupPlan> filter = ctx.root;
                   List<Expression> predicates = bind(filter.getExpressions(), ctx.connectContext.getEnv());
                   return new LogicalFilter<>(predicates.get(0), filter.child());
               })
            ),
            RuleType.BINDING_HAVING_FUNCTION.build(
                logicalHaving().thenApply(ctx -> {
                    LogicalHaving<GroupPlan> having = ctx.root;
                    List<Expression> predicates = bind(having.getExpressions(), ctx.connectContext.getEnv());
                    return new LogicalHaving<>(predicates.get(0), having.child());
                })
            ),
            RuleType.BINDING_SORT_FUNCTION.build(
                    logicalSort().thenApply(ctx -> {
                        LogicalSort<GroupPlan> sort = ctx.root;
                        List<OrderKey> orderKeys = sort.getOrderKeys().stream()
                                .map(orderKey -> new OrderKey(
                                        FunctionBinder.INSTANCE.bind(orderKey.getExpr(), ctx.connectContext.getEnv()),
                                        orderKey.isAsc(),
                                        orderKey.isNullFirst()
                                ))
                                .collect(ImmutableList.toImmutableList());
                        return new LogicalSort<>(orderKeys, sort.child());
                    })
            )
        );
    }

    private <E extends Expression> List<E> bind(List<? extends E> exprList, Env env) {
        return exprList.stream()
            .map(expr -> FunctionBinder.INSTANCE.bind(expr, env))
            .collect(Collectors.toList());
    }

    private static class FunctionBinder extends DefaultExpressionRewriter<Env> {
        public static final FunctionBinder INSTANCE = new FunctionBinder();

        public <E extends Expression> E bind(E expression, Env env) {
            return (E) expression.accept(this, env);
        }

        @Override
        public BoundFunction visitUnboundFunction(UnboundFunction unboundFunction, Env env) {
            unboundFunction = (UnboundFunction) super.visitUnboundFunction(unboundFunction, env);

            // FunctionRegistry can't support boolean arg now, tricky here.
            if (unboundFunction.getName().equalsIgnoreCase("count")) {
                List<Expression> arguments = unboundFunction.getArguments();
                if ((arguments.size() == 0 && unboundFunction.isStar()) || arguments.stream()
                        .allMatch(Expression::isConstant)) {
                    return new Count();
                }
                if (arguments.size() == 1) {
                    boolean isGlobalAgg = true;
                    AggregateParam aggregateParam = new AggregateParam(unboundFunction.isDistinct(), isGlobalAgg);
                    return new Count(aggregateParam, unboundFunction.getArguments().get(0));
                }
            }
            FunctionRegistry functionRegistry = env.getFunctionRegistry();
            String functionName = unboundFunction.getName();
            FunctionBuilder builder = functionRegistry.findFunctionBuilder(
                    functionName, unboundFunction.getArguments());
            return builder.build(functionName, unboundFunction.getArguments());
        }

        /**
         * gets the method for calculating the time.
         * e.g. YEARS_ADD、YEARS_SUB、DAYS_ADD 、DAYS_SUB
         */
        @Override
        public Expression visitTimestampArithmetic(TimestampArithmetic arithmetic, Env context) {
            arithmetic = (TimestampArithmetic) super.visitTimestampArithmetic(arithmetic, context);

            String funcOpName;
            if (arithmetic.getFuncName() == null) {
                // e.g. YEARS_ADD, MONTHS_SUB
                funcOpName = String.format("%sS_%s", arithmetic.getTimeUnit(),
                        (arithmetic.getOp() == Operator.ADD) ? "ADD" : "SUB");
            } else {
                funcOpName = arithmetic.getFuncName();
            }
            return arithmetic.withFuncName(funcOpName.toLowerCase(Locale.ROOT));
        }
    }
}
