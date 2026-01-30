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
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;
import java.util.Set;

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

    private static final Logger LOG = LogManager.getLogger(VariantSchemaCast.class);

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        return plan.accept(new PlanRewriter(), null);
    }

    private static class PlanRewriter extends DefaultPlanRewriter<Void> {

        private final java.util.function.Function<Expression, Expression> expressionRewriter = expr -> {
            // Skip Match expressions - they require SlotRef as left operand
            if (expr instanceof Match) {
                return expr;
            }
            // Handle ElementAt expressions
            if (expr instanceof ElementAt) {
                return rewriteElementAt((ElementAt) expr);
            }
            // Handle SlotReference that represents variant element access (e.g., data['field'])
            if (expr instanceof SlotReference) {
                return rewriteSlotReference((SlotReference) expr);
            }
            return expr;
        };

        private static Expression rewriteElementAt(ElementAt elementAt) {
            Expression left = elementAt.left();
            Expression right = elementAt.right();

            // Debug logging
            LOG.info("Processing ElementAt: {}", elementAt);
            LOG.info("Left type: {}, class: {}", left.getDataType(),
                    left.getDataType().getClass().getName());

            // Only process if left is VariantType and right is a string literal
            if (!(left.getDataType() instanceof VariantType)) {
                LOG.info("Left is not VariantType, skipping");
                return elementAt;
            }
            if (!(right instanceof StringLikeLiteral)) {
                LOG.info("Right is not StringLikeLiteral, skipping");
                return elementAt;
            }

            VariantType variantType = (VariantType) left.getDataType();
            String fieldName = ((StringLikeLiteral) right).getStringValue();

            LOG.info("predefinedFields: {}, fieldName: {}",
                    variantType.getPredefinedFields(), fieldName);

            // Find matching field in schema template
            Optional<VariantField> matchingField = variantType.findMatchingField(fieldName);
            if (!matchingField.isPresent()) {
                LOG.info("No matching field found for: {}", fieldName);
                return elementAt;
            }

            DataType targetType = matchingField.get().getDataType();
            LOG.info("Found matching field, target type: {}", targetType);
            return new Cast(elementAt, targetType);
        }

        private static Expression rewriteSlotReference(SlotReference slotRef) {
            // Check if the SlotReference's DataType is VariantType with predefinedFields
            if (!(slotRef.getDataType() instanceof VariantType)) {
                return slotRef;
            }

            VariantType variantType = (VariantType) slotRef.getDataType();
            if (variantType.getPredefinedFields().isEmpty()) {
                return slotRef;
            }

            // Extract field name from SlotReference name pattern like "data['field_name']"
            String slotName = slotRef.getName();

            // Parse field name from pattern like "column['field']" or "column[\"field\"]"
            int bracketStart = slotName.indexOf('[');
            if (bracketStart < 0) {
                return slotRef;
            }

            int bracketEnd = slotName.lastIndexOf(']');
            if (bracketEnd <= bracketStart) {
                return slotRef;
            }

            // Extract the content between brackets and remove quotes
            String bracketContent = slotName.substring(bracketStart + 1, bracketEnd);
            String fieldName = bracketContent;
            if ((bracketContent.startsWith("'") && bracketContent.endsWith("'"))
                    || (bracketContent.startsWith("\"") && bracketContent.endsWith("\""))) {
                fieldName = bracketContent.substring(1, bracketContent.length() - 1);
            }

            // Find matching field in schema template
            Optional<VariantField> matchingField = variantType.findMatchingField(fieldName);
            if (!matchingField.isPresent()) {
                return slotRef;
            }

            DataType targetType = matchingField.get().getDataType();
            return new Cast(slotRef, targetType);
        }

        private Expression rewriteExpression(Expression expr) {
            return expr.rewriteDownShortCircuit(expressionRewriter);
        }

        private NamedExpression rewriteNamedExpression(NamedExpression expr) {
            Expression rewritten = rewriteExpression(expr);
            if (rewritten instanceof NamedExpression) {
                return (NamedExpression) rewritten;
            }
            // If the result is not a NamedExpression (e.g., Cast), wrap it in an Alias
            // Preserve the original ExprId to maintain consistency
            return new Alias(expr.getExprId(), rewritten, expr.getName());
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
                    .map(this::rewriteNamedExpression)
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
        public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, Void context) {
            aggregate = (LogicalAggregate<? extends Plan>) super.visit(aggregate, context);
            List<Expression> newGroupByExprs = aggregate.getGroupByExpressions().stream()
                    .map(this::rewriteExpression)
                    .collect(ImmutableList.toImmutableList());
            List<NamedExpression> newOutputExprs = aggregate.getOutputExpressions().stream()
                    .map(this::rewriteNamedExpression)
                    .collect(ImmutableList.toImmutableList());
            return aggregate.withGroupByAndOutput(newGroupByExprs, newOutputExprs);
        }

        @Override
        public Plan visitLogicalHaving(LogicalHaving<? extends Plan> having, Void context) {
            having = (LogicalHaving<? extends Plan>) super.visit(having, context);
            Set<Expression> newConjuncts = having.getConjuncts().stream()
                    .map(this::rewriteExpression)
                    .collect(ImmutableSet.toImmutableSet());
            return having.withConjuncts(newConjuncts);
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
    }
}
