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
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.VariantField;
import org.apache.doris.nereids.types.VariantType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Automatically cast variant element access expressions based on schema template.
 *
 * For example, if a variant column is defined as:
 * payload VARIANT&lt;'number_*': BIGINT, 'string_*': STRING&gt;
 *
 * Then payload['number_latency'] will be automatically cast to BIGINT,
 * and payload['string_message'] will be automatically cast to STRING.
 *
 * This allows users to use variant sub-fields directly in WHERE, ORDER BY,
 * and other clauses without explicit CAST.
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

            // Only process if left is VariantType and right is a string literal
            if (!(left.getDataType() instanceof VariantType)) {
                return expr;
            }
            if (!(right instanceof StringLikeLiteral)) {
                return expr;
            }

            VariantType variantType = (VariantType) left.getDataType();
            String fieldName = ((StringLikeLiteral) right).getStringValue();

            // Find matching field in schema template
            Optional<VariantField> matchingField = variantType.findMatchingField(fieldName);
            if (!matchingField.isPresent()) {
                return expr;
            }

            DataType targetType = matchingField.get().getDataType();

            // Wrap with Cast
            return new Cast(elementAt, targetType);
        };

        private Expression rewriteExpression(Expression expr) {
            return expr.rewriteDownShortCircuit(EXPRESSION_REWRITER);
        }

        @Override
        public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, Void context) {
            filter = (LogicalFilter<? extends Plan>) super.visit(filter, context);
            Set<Expression> newConjuncts = filter.getConjuncts().stream()
                    .map(this::rewriteExpression)
                    .collect(ImmutableSet.toImmutableSet());
            return filter.withConjuncts(newConjuncts);
        }

        @Override
        public Plan visitLogicalProject(LogicalProject<? extends Plan> project, Void context) {
            project = (LogicalProject<? extends Plan>) super.visit(project, context);
            List<NamedExpression> newProjects = project.getProjects().stream()
                    .map(expr -> (NamedExpression) rewriteExpression(expr))
                    .collect(ImmutableList.toImmutableList());
            return project.withProjects(newProjects);
        }

        @Override
        public Plan visitLogicalSort(LogicalSort<? extends Plan> sort, Void context) {
            sort = (LogicalSort<? extends Plan>) super.visit(sort, context);
            List<OrderKey> newOrderKeys = sort.getOrderKeys().stream()
                    .map(orderKey -> orderKey.withExpression(rewriteExpression(orderKey.getExpr())))
                    .collect(ImmutableList.toImmutableList());
            return sort.withOrderKeys(newOrderKeys);
        }

        @Override
        public Plan visitLogicalTopN(LogicalTopN<? extends Plan> topN, Void context) {
            topN = (LogicalTopN<? extends Plan>) super.visit(topN, context);
            List<OrderKey> newOrderKeys = topN.getOrderKeys().stream()
                    .map(orderKey -> orderKey.withExpression(rewriteExpression(orderKey.getExpr())))
                    .collect(ImmutableList.toImmutableList());
            return topN.withOrderKeys(newOrderKeys);
        }

        @Override
        public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, Void context) {
            join = (LogicalJoin<? extends Plan, ? extends Plan>) super.visit(join, context);
            List<Expression> newHashConditions = join.getHashJoinConjuncts().stream()
                    .map(this::rewriteExpression)
                    .collect(ImmutableList.toImmutableList());
            List<Expression> newOtherConditions = join.getOtherJoinConjuncts().stream()
                    .map(this::rewriteExpression)
                    .collect(ImmutableList.toImmutableList());
            List<Expression> newMarkConditions = join.getMarkJoinConjuncts().stream()
                    .map(this::rewriteExpression)
                    .collect(ImmutableList.toImmutableList());
            return join.withJoinConjuncts(newHashConditions, newOtherConditions,
                    newMarkConditions, join.getJoinReorderContext());
        }

        @Override
        public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, Void context) {
            aggregate = (LogicalAggregate<? extends Plan>) super.visit(aggregate, context);
            List<Expression> newGroupByKeys = aggregate.getGroupByExpressions().stream()
                    .map(this::rewriteExpression)
                    .collect(ImmutableList.toImmutableList());
            List<NamedExpression> newOutputs = aggregate.getOutputExpressions().stream()
                    .map(expr -> (NamedExpression) rewriteExpression(expr))
                    .collect(ImmutableList.toImmutableList());
            return aggregate.withGroupByAndOutput(newGroupByKeys, newOutputs);
        }
    }
}
