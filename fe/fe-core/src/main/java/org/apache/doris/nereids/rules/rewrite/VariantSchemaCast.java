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

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.VariantField;
import org.apache.doris.nereids.types.VariantType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Automatically cast variant element access expressions based on schema template.
 *
 * This rule only targets non-select clauses (e.g. WHERE, GROUP BY, HAVING, ORDER BY, JOIN ON).
 * It should run before VariantSubPathPruning so ElementAt is still present.
 */
public class VariantSchemaCast implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        return plan.accept(PlanRewriter.INSTANCE, null);
    }

    private static class PlanRewriter extends DefaultPlanRewriter<Void> {
        public static final PlanRewriter INSTANCE = new PlanRewriter();

        private static final Function<Expression, Expression> EXPRESSION_REWRITER = expr -> {
            if (!(expr instanceof ElementAt)) {
                return expr;
            }
            ElementAt elementAt = (ElementAt) expr;
            Expression left = elementAt.left();
            Expression right = elementAt.right();

            if (!(left.getDataType() instanceof VariantType)) {
                return expr;
            }
            if (!(right instanceof StringLikeLiteral)) {
                return expr;
            }

            VariantType variantType = (VariantType) left.getDataType();
            String fieldName = ((StringLikeLiteral) right).getStringValue();
            Optional<VariantField> matchingField = variantType.findMatchingField(fieldName);
            if (!matchingField.isPresent()) {
                return expr;
            }

            DataType targetType = matchingField.get().getDataType();
            return new Cast(elementAt, targetType);
        };

        private Expression rewriteExpression(Expression expr) {
            return expr.rewriteDownShortCircuit(EXPRESSION_REWRITER);
        }

        private Expression rewriteExpression(Expression expr, Map<ExprId, Expression> aliasMap) {
            if (aliasMap.isEmpty()) {
                return rewriteExpression(expr);
            }
            return expr.rewriteDownShortCircuit(node -> {
                if (node instanceof SlotReference) {
                    ExprId exprId = ((SlotReference) node).getExprId();
                    Expression aliasExpr = aliasMap.get(exprId);
                    if (aliasExpr != null) {
                        Expression rewrittenAlias = rewriteExpression(aliasExpr);
                        if (rewrittenAlias instanceof Cast) {
                            return new Cast(node, ((Cast) rewrittenAlias).getDataType());
                        }
                    }
                }
                return EXPRESSION_REWRITER.apply(node);
            });
        }

        private Map<ExprId, Expression> buildAliasMap(Plan plan) {
            Map<ExprId, Expression> aliasMap = new HashMap<>();
            Plan current = plan;
            while (current instanceof LogicalSubQueryAlias) {
                current = ((LogicalSubQueryAlias<?>) current).child();
            }
            if (current instanceof LogicalProject) {
                collectAliasMap(aliasMap, ((LogicalProject<?>) current).getProjects());
            } else if (current instanceof LogicalAggregate) {
                collectAliasMap(aliasMap, ((LogicalAggregate<?>) current).getOutputExpressions());
            }
            return aliasMap;
        }

        private void collectAliasMap(Map<ExprId, Expression> aliasMap, List<? extends NamedExpression> outputs) {
            for (NamedExpression output : outputs) {
                if (output instanceof Alias) {
                    Alias alias = (Alias) output;
                    aliasMap.put(alias.getExprId(), alias.child());
                }
            }
        }

        @Override
        public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, Void context) {
            filter = (LogicalFilter<? extends Plan>) super.visit(filter, context);
            Map<ExprId, Expression> aliasMap = buildAliasMap(filter.child());
            Set<Expression> newConjuncts = filter.getConjuncts().stream()
                    .map(expr -> rewriteExpression(expr, aliasMap))
                    .collect(ImmutableSet.toImmutableSet());
            return filter.withConjuncts(newConjuncts);
        }

        @Override
        public Plan visitLogicalHaving(LogicalHaving<? extends Plan> having, Void context) {
            having = (LogicalHaving<? extends Plan>) super.visit(having, context);
            Map<ExprId, Expression> aliasMap = buildAliasMap(having.child());
            Set<Expression> newConjuncts = having.getConjuncts().stream()
                    .map(expr -> rewriteExpression(expr, aliasMap))
                    .collect(ImmutableSet.toImmutableSet());
            return having.withConjuncts(newConjuncts);
        }

        @Override
        public Plan visitLogicalSort(LogicalSort<? extends Plan> sort, Void context) {
            sort = (LogicalSort<? extends Plan>) super.visit(sort, context);
            Map<ExprId, Expression> aliasMap = buildAliasMap(sort.child());
            List<OrderKey> newOrderKeys = sort.getOrderKeys().stream()
                    .map(orderKey -> orderKey.withExpression(rewriteExpression(orderKey.getExpr(), aliasMap)))
                    .collect(ImmutableList.toImmutableList());
            return sort.withOrderKeys(newOrderKeys);
        }

        @Override
        public Plan visitLogicalTopN(LogicalTopN<? extends Plan> topN, Void context) {
            topN = (LogicalTopN<? extends Plan>) super.visit(topN, context);
            Map<ExprId, Expression> aliasMap = buildAliasMap(topN.child());
            List<OrderKey> newOrderKeys = topN.getOrderKeys().stream()
                    .map(orderKey -> orderKey.withExpression(rewriteExpression(orderKey.getExpr(), aliasMap)))
                    .collect(ImmutableList.toImmutableList());
            return topN.withOrderKeys(newOrderKeys);
        }

        @Override
        public Plan visitLogicalPartitionTopN(LogicalPartitionTopN<? extends Plan> topN, Void context) {
            topN = (LogicalPartitionTopN<? extends Plan>) super.visit(topN, context);
            Map<ExprId, Expression> aliasMap = buildAliasMap(topN.child());
            List<Expression> newPartitionKeys = topN.getPartitionKeys().stream()
                    .map(expr -> rewriteExpression(expr, aliasMap))
                    .collect(ImmutableList.toImmutableList());
            List<OrderExpression> newOrderKeys = topN.getOrderKeys().stream()
                    .map(orderExpr -> (OrderExpression) orderExpr.withChildren(ImmutableList.of(
                            rewriteExpression(orderExpr.child(), aliasMap))))
                    .collect(ImmutableList.toImmutableList());
            return topN.withPartitionKeysAndOrderKeys(newPartitionKeys, newOrderKeys);
        }

        @Override
        public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, Void context) {
            join = (LogicalJoin<? extends Plan, ? extends Plan>) super.visit(join, context);
            Map<ExprId, Expression> aliasMap = buildAliasMap(join.left());
            aliasMap.putAll(buildAliasMap(join.right()));
            List<Expression> newHashConditions = join.getHashJoinConjuncts().stream()
                    .map(expr -> rewriteExpression(expr, aliasMap))
                    .collect(ImmutableList.toImmutableList());
            List<Expression> newOtherConditions = join.getOtherJoinConjuncts().stream()
                    .map(expr -> rewriteExpression(expr, aliasMap))
                    .collect(ImmutableList.toImmutableList());
            List<Expression> newMarkConditions = join.getMarkJoinConjuncts().stream()
                    .map(expr -> rewriteExpression(expr, aliasMap))
                    .collect(ImmutableList.toImmutableList());
            return join.withJoinConjuncts(newHashConditions, newOtherConditions,
                    newMarkConditions, join.getJoinReorderContext());
        }

        @Override
        public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, Void context) {
            aggregate = (LogicalAggregate<? extends Plan>) super.visit(aggregate, context);
            Map<ExprId, Expression> aliasMap = buildAliasMap(aggregate.child());
            List<Expression> newGroupByKeys = aggregate.getGroupByExpressions().stream()
                    .map(expr -> rewriteExpression(expr, aliasMap))
                    .collect(ImmutableList.toImmutableList());
            List<NamedExpression> outputs = aggregate.getOutputExpressions();
            return aggregate.withGroupByAndOutput(newGroupByKeys, outputs);
        }

        @Override
        public Plan visitLogicalWindow(LogicalWindow<? extends Plan> window, Void context) {
            window = (LogicalWindow<? extends Plan>) super.visit(window, context);
            Map<ExprId, Expression> aliasMap = buildAliasMap(window.child());
            List<NamedExpression> newExprs = window.getWindowExpressions().stream()
                    .map(expr -> rewriteWindowExpression(expr, aliasMap))
                    .collect(ImmutableList.toImmutableList());
            return window.withExpressionsAndChild(newExprs, window.child());
        }

        private NamedExpression rewriteWindowExpression(NamedExpression expr, Map<ExprId, Expression> aliasMap) {
            if (expr instanceof Alias) {
                Alias alias = (Alias) expr;
                if (alias.child() instanceof WindowExpression) {
                    WindowExpression windowExpr = (WindowExpression) alias.child();
                    List<Expression> newPartitionKeys = windowExpr.getPartitionKeys().stream()
                            .map(partitionKey -> rewriteExpression(partitionKey, aliasMap))
                            .collect(ImmutableList.toImmutableList());
                    List<OrderExpression> newOrderKeys = windowExpr.getOrderKeys().stream()
                            .map(orderExpr -> (OrderExpression) orderExpr.withChildren(ImmutableList.of(
                                    rewriteExpression(orderExpr.child(), aliasMap))))
                            .collect(ImmutableList.toImmutableList());
                    return alias.withChildren(ImmutableList.of(
                            windowExpr.withPartitionKeysOrderKeys(newPartitionKeys, newOrderKeys)));
                }
            }
            return expr;
        }
    }
}
