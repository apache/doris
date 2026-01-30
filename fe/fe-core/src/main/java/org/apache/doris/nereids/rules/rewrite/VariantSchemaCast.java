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
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
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
        return plan.accept(new PlanRewriter(), null);
    }

    private static class PlanRewriter extends DefaultPlanRewriter<Void> {

        private Expression rewriteElementAt(ElementAt elementAt) {
            Expression left = elementAt.left();
            Expression right = elementAt.right();

            // Only process if left is VariantType and right is a string literal
            if (!(left.getDataType() instanceof VariantType)) {
                return elementAt;
            }
            if (!(right instanceof StringLikeLiteral)) {
                return elementAt;
            }

            VariantType variantType = (VariantType) left.getDataType();
            String fieldName = ((StringLikeLiteral) right).getStringValue();

            // Find matching field in schema template
            Optional<VariantField> matchingField = variantType.findMatchingField(fieldName);
            if (!matchingField.isPresent()) {
                return elementAt;
            }

            DataType targetType = matchingField.get().getDataType();
            return new Cast(elementAt, targetType);
        }

        private Expression rewriteSlotReference(SlotReference slotRef) {
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
            // Skip Match expressions - they require SlotRef as left operand
            if (expr instanceof Match) {
                return expr;
            }

            // Recursively rewrite children first
            boolean childrenChanged = false;
            ImmutableList.Builder<Expression> newChildren = ImmutableList.builder();
            for (Expression child : expr.children()) {
                Expression newChild = rewriteExpression(child);
                newChildren.add(newChild);
                if (newChild != child) {
                    childrenChanged = true;
                }
            }

            Expression newExpr = childrenChanged ? expr.withChildren(newChildren.build()) : expr;

            // Then apply the rewriter to the current expression
            if (newExpr instanceof ElementAt) {
                return rewriteElementAt((ElementAt) newExpr);
            }
            // Handle SlotReference that represents variant element access (e.g., data['field'])
            if (newExpr instanceof SlotReference) {
                return rewriteSlotReference((SlotReference) newExpr);
            }
            return newExpr;
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
            // Don't rewrite SELECT projections - BE expects Variant type for ElementAt in SELECT
            return super.visit(project, context);
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
    }
}
