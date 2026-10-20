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

import org.apache.doris.analysis.SearchDslParser;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SearchExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Search;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Rewrite search function to add proper slot reference children.
 * This is crucial for BE's "action on slot" detection in normalize conjuncts.
 */
public class RewriteSearchToSlots extends OneRewriteRuleFactory {
    private static final Logger LOG = LogManager.getLogger(RewriteSearchToSlots.class);

    @Override
    public Rule build() {
        return logicalFilter()
                .when(filter -> ExpressionUtils.containsTypes(filter.getExpressions(), Search.class))
                .then(this::rewriteSearchExpressions)
                .toRule(RuleType.REWRITE_SEARCH_TO_SLOTS);
    }

    private Plan rewriteSearchExpressions(LogicalFilter<? extends Plan> filter) {
        List<Expression> newExpressions = new ArrayList<>();

        for (Expression expr : filter.getExpressions()) {
            Expression rewritten = rewriteExpression(expr, filter.child());
            newExpressions.add(rewritten);
        }

        if (!newExpressions.equals(filter.getExpressions())) {
            return filter.withConjuncts(Utils.fastToImmutableSet(newExpressions));
        }

        return filter;
    }

    private Expression rewriteExpression(Expression expr, Plan input) {
        if (expr instanceof Search) {
            return rewriteSearch((Search) expr, input);
        }

        // Recursively process children
        List<Expression> newChildren = expr.children().stream()
                .map(child -> rewriteExpression(child, input))
                .collect(Collectors.toList());

        if (!newChildren.equals(expr.children())) {
            return expr.withChildren(newChildren);
        }

        return expr;
    }

    private Expression rewriteSearch(Search search, Plan input) {
        try {
            // Parse DSL to get field bindings
            SearchDslParser.QsPlan qsPlan = search.getQsPlan();
            if (qsPlan == null || qsPlan.getFieldBindings() == null || qsPlan.getFieldBindings().isEmpty()) {
                LOG.warn("Search function has no field bindings: {}", search.getDslString());
                return search;
            }

            Map<String, String> normalizedFields = new HashMap<>();

            // Create slot reference children from field bindings
            List<Expression> slotChildren = new ArrayList<>();
            for (SearchDslParser.QsFieldBinding binding : qsPlan.getFieldBindings()) {
                String originalFieldName = binding.getFieldName();
                Expression childExpr;
                String normalizedFieldName;

                if (originalFieldName.contains(".")) {
                    int firstDotPos = originalFieldName.indexOf('.');
                    String parentFieldName = originalFieldName.substring(0, firstDotPos);
                    String subcolumnPath = originalFieldName.substring(firstDotPos + 1);

                    // Find parent slot
                    Slot parentSlot = findSlotByName(parentFieldName, input);

                    // Verify it's a variant type
                    if (!parentSlot.getDataType().isVariantType()) {
                        throw new AnalysisException(String.format(
                                "Field '%s' is not VARIANT type for subcolumn access: %s",
                                parentFieldName, search.getDslString()));
                    }
                    String normalizedParentFieldName = parentSlot.getName();

                    // Create ElementAt expression for variant subcolumn
                    // This will be converted to an extracted column slot by VariantSubPathPruning rule
                    // If the subcolumn doesn't exist, ElementAt will remain and BE will handle it gracefully
                    childExpr = new ElementAt(parentSlot, new StringLiteral(subcolumnPath));
                    normalizedFieldName = normalizedParentFieldName + "." + subcolumnPath;

                    LOG.info(
                            "Created ElementAt expression for variant subcolumn: parent='{}', "
                                    + "subcolumn='{}', field_name='{}'",
                            normalizedParentFieldName, subcolumnPath, normalizedFieldName);
                } else {
                    // Normal field - find slot directly
                    Slot slot = findSlotByName(originalFieldName, input);
                    childExpr = slot;
                    normalizedFieldName = slot.getName();
                }

                normalizedFields.put(originalFieldName, normalizedFieldName);
                binding.setFieldName(normalizedFieldName);
                slotChildren.add(childExpr);
            }

            LOG.info("Rewriting search function: dsl='{}' with {} slot children",
                    search.getDslString(), slotChildren.size());

            normalizePlanFields(qsPlan.getRoot(), normalizedFields);

            // Create SearchExpression with slot children
            return new SearchExpression(search.getDslString(), qsPlan, slotChildren);

        } catch (Exception e) {
            throw new AnalysisException("Failed to rewrite search expression: " + e.getMessage(), e);
        }
    }

    /** Resolve a field only against the visible input, rejecting missing or ambiguous names. */
    private Slot findSlotByName(String fieldName, Plan input) {
        List<Slot> matches = input.getOutput().stream()
                .filter(slot -> slot.getName().equalsIgnoreCase(fieldName))
                .collect(Collectors.toList());
        if (matches.size() != 1) {
            throw new AnalysisException("search() field '" + fieldName
                    + "' must resolve to exactly one input column");
        }
        return matches.get(0);
    }

    private void normalizePlanFields(SearchDslParser.QsNode node, Map<String, String> normalized) {
        if (node == null) {
            return;
        }
        if (node.getField() != null) {
            for (Map.Entry<String, String> entry : normalized.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(node.getField())) {
                    node.setField(entry.getValue());
                    break;
                }
            }
        }
        if (node.getChildren() != null) {
            for (SearchDslParser.QsNode child : node.getChildren()) {
                normalizePlanFields(child, normalized);
            }
        }
    }

}
