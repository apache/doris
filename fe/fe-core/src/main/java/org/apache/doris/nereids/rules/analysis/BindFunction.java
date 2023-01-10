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
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rewrite.rules.CharacterLiteralTypeCoercion;
import org.apache.doris.nereids.rules.expression.rewrite.rules.TypeCoercion;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.TVFProperties;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.generator.TableGeneratingFunction;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Locale;
import java.util.Set;
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
                    List<NamedExpression> boundProjects = bindAndTypeCoercion(projects, ctx.connectContext);
                    if (projects.equals(boundProjects)) {
                        return oneRowRelation;
                    }
                    return new LogicalOneRowRelation(boundProjects);
                })
            ),
            RuleType.BINDING_PROJECT_FUNCTION.build(
                logicalProject().thenApply(ctx -> {
                    LogicalProject<GroupPlan> project = ctx.root;
                    List<NamedExpression> boundExpr = bindAndTypeCoercion(project.getProjects(),
                            ctx.connectContext);
                    return new LogicalProject<>(boundExpr, project.child(), project.isDistinct());
                })
            ),
            RuleType.BINDING_AGGREGATE_FUNCTION.build(
                logicalAggregate().thenApply(ctx -> {
                    LogicalAggregate<GroupPlan> agg = ctx.root;
                    List<Expression> groupBy = bindAndTypeCoercion(agg.getGroupByExpressions(),
                            ctx.connectContext);
                    List<NamedExpression> output = bindAndTypeCoercion(agg.getOutputExpressions(),
                            ctx.connectContext);
                    return agg.withGroupByAndOutput(groupBy, output);
                })
            ),
            RuleType.BINDING_REPEAT_FUNCTION.build(
                logicalRepeat().thenApply(ctx -> {
                    LogicalRepeat<GroupPlan> repeat = ctx.root;
                    List<List<Expression>> groupingSets = repeat.getGroupingSets()
                            .stream()
                            .map(groupingSet -> bindAndTypeCoercion(groupingSet, ctx.connectContext))
                            .collect(ImmutableList.toImmutableList());
                    List<NamedExpression> output = bindAndTypeCoercion(repeat.getOutputExpressions(),
                            ctx.connectContext);
                    return repeat.withGroupSetsAndOutput(groupingSets, output);
                })
            ),
            RuleType.BINDING_FILTER_FUNCTION.build(
               logicalFilter().thenApply(ctx -> {
                   LogicalFilter<GroupPlan> filter = ctx.root;
                   Set<Expression> conjuncts = bindAndTypeCoercion(filter.getConjuncts(), ctx.connectContext);
                   return new LogicalFilter<>(conjuncts, filter.child());
               })
            ),
            RuleType.BINDING_HAVING_FUNCTION.build(
                logicalHaving().thenApply(ctx -> {
                    LogicalHaving<GroupPlan> having = ctx.root;
                    Set<Expression> conjuncts = bindAndTypeCoercion(having.getConjuncts(), ctx.connectContext);
                    return new LogicalHaving<>(conjuncts, having.child());
                })
            ),
            RuleType.BINDING_SORT_FUNCTION.build(
                logicalSort().thenApply(ctx -> {
                    LogicalSort<GroupPlan> sort = ctx.root;
                    List<OrderKey> orderKeys = sort.getOrderKeys().stream()
                            .map(orderKey -> new OrderKey(
                                        bindAndTypeCoercion(orderKey.getExpr(),
                                                ctx.connectContext.getEnv(),
                                                new ExpressionRewriteContext(ctx.connectContext)
                                                ),
                                        orderKey.isAsc(),
                                        orderKey.isNullFirst())

                            )
                            .collect(ImmutableList.toImmutableList());
                    return new LogicalSort<>(orderKeys, sort.child());
                })
            ),
            RuleType.BINDING_JOIN_FUNCTION.build(
                logicalJoin().thenApply(ctx -> {
                    LogicalJoin<GroupPlan, GroupPlan> join = ctx.root;
                    List<Expression> hashConjuncts = bindAndTypeCoercion(join.getHashJoinConjuncts(),
                            ctx.connectContext);
                    List<Expression> otherConjuncts = bindAndTypeCoercion(join.getOtherJoinConjuncts(),
                            ctx.connectContext);
                    return new LogicalJoin<>(join.getJoinType(), hashConjuncts, otherConjuncts,
                            join.getHint(),
                            join.left(), join.right());
                })
            ),
            RuleType.BINDING_UNBOUND_TVF_RELATION_FUNCTION.build(
                unboundTVFRelation().thenApply(ctx -> {
                    UnboundTVFRelation relation = ctx.root;
                    return FunctionBinder.INSTANCE.bindTableValuedFunction(relation, ctx.statementContext);
                })
            )
        );
    }

    private <E extends Expression> List<E> bindAndTypeCoercion(List<? extends E> exprList, ConnectContext ctx) {
        ExpressionRewriteContext rewriteContext = new ExpressionRewriteContext(ctx);
        return exprList.stream()
            .map(expr -> bindAndTypeCoercion(expr, ctx.getEnv(), rewriteContext))
            .collect(Collectors.toList());
    }

    private <E extends Expression> E bindAndTypeCoercion(E expr, Env env, ExpressionRewriteContext ctx) {
        expr = FunctionBinder.INSTANCE.bind(expr, env);
        expr = (E) CharacterLiteralTypeCoercion.INSTANCE.rewrite(expr, ctx);
        return (E) TypeCoercion.INSTANCE.rewrite(expr, null);
    }

    private <E extends Expression> Set<E> bindAndTypeCoercion(Set<? extends E> exprSet, ConnectContext ctx) {
        ExpressionRewriteContext rewriteContext = new ExpressionRewriteContext(ctx);
        return exprSet.stream()
                .map(expr -> bindAndTypeCoercion(expr, ctx.getEnv(), rewriteContext))
                .collect(Collectors.toSet());
    }

    /**
     * function binder
     */
    public static class FunctionBinder extends DefaultExpressionRewriter<Env> {
        public static final FunctionBinder INSTANCE = new FunctionBinder();

        public <E extends Expression> E bind(E expression, Env env) {
            return (E) expression.accept(this, env);
        }

        /**
         * bindTableValuedFunction
         */
        public LogicalTVFRelation bindTableValuedFunction(UnboundTVFRelation unboundTVFRelation,
                StatementContext statementContext) {
            Env env = statementContext.getConnectContext().getEnv();
            FunctionRegistry functionRegistry = env.getFunctionRegistry();

            String functionName = unboundTVFRelation.getFunctionName();
            TVFProperties arguments = unboundTVFRelation.getProperties();
            FunctionBuilder functionBuilder = functionRegistry.findFunctionBuilder(functionName, arguments);
            BoundFunction function = functionBuilder.build(functionName, arguments);
            if (!(function instanceof TableValuedFunction)) {
                throw new AnalysisException(function.toSql() + " is not a TableValuedFunction");
            }

            RelationId relationId = statementContext.getNextRelationId();
            return new LogicalTVFRelation(relationId, (TableValuedFunction) function);
        }

        /**
         * bindTableGeneratingFunction
         */
        public BoundFunction bindTableGeneratingFunction(UnboundFunction unboundFunction,
                StatementContext statementContext) {
            Env env = statementContext.getConnectContext().getEnv();
            FunctionRegistry functionRegistry = env.getFunctionRegistry();

            String functionName = unboundFunction.getName();
            FunctionBuilder functionBuilder = functionRegistry.findFunctionBuilder(
                    functionName, unboundFunction.getArguments());
            BoundFunction function = functionBuilder.build(functionName, unboundFunction.getArguments());
            if (!(function instanceof TableGeneratingFunction)) {
                throw new AnalysisException(function.toSql() + " is not a TableGeneratingFunction");
            }

            return function;
        }

        @Override
        public Expression visitUnboundFunction(UnboundFunction unboundFunction, Env env) {
            unboundFunction = (UnboundFunction) super.visitUnboundFunction(unboundFunction, env);

            FunctionRegistry functionRegistry = env.getFunctionRegistry();
            String functionName = unboundFunction.getName();
            List<Object> arguments = unboundFunction.isDistinct()
                    ? ImmutableList.builder()
                        .add(unboundFunction.isDistinct())
                        .addAll(unboundFunction.getArguments())
                        .build()
                    : (List) unboundFunction.getArguments();

            FunctionBuilder builder = functionRegistry.findFunctionBuilder(functionName, arguments);
            BoundFunction boundFunction = builder.build(functionName, arguments);
            return boundFunction;
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
