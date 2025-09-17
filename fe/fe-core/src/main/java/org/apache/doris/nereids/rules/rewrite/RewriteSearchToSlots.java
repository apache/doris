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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SearchExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Search;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SearchDslParser;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Rewrite search function to add proper slot reference children.
 * This is crucial for BE's "action on slot" detection in normalize conjuncts.
 */
public class RewriteSearchToSlots extends OneRewriteRuleFactory {
    private static final Logger LOG = LogManager.getLogger(RewriteSearchToSlots.class);

    @Override
    public Rule build() {
        return logicalFilter(logicalOlapScan())
                .when(filter -> ExpressionUtils.containsTypes(filter.getExpressions(), Search.class))
                .then(this::rewriteSearchExpressions)
                .toRule(RuleType.REWRITE_SEARCH_TO_SLOTS);
    }

    private Plan rewriteSearchExpressions(LogicalFilter<LogicalOlapScan> filter) {
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

    private Expression rewriteExpression(Expression expr, LogicalOlapScan scan) {
        if (expr instanceof Search) {
            return rewriteSearch((Search) expr, scan);
        }

        // Recursively process children
        List<Expression> newChildren = expr.children().stream()
                .map(child -> rewriteExpression(child, scan))
                .collect(Collectors.toList());

        if (!newChildren.equals(expr.children())) {
            return expr.withChildren(newChildren);
        }

        return expr;
    }

    private Expression rewriteSearch(Search search, LogicalOlapScan scan) {
        try {
            // Parse DSL to get field bindings
            SearchDslParser.QsPlan qsPlan = search.getQsPlan();
            if (qsPlan == null || qsPlan.fieldBindings == null || qsPlan.fieldBindings.isEmpty()) {
                LOG.warn("Search function has no field bindings: {}", search.getDslString());
                return search;
            }

            // Create slot reference children from field bindings
            List<Expression> slotChildren = new ArrayList<>();
            for (SearchDslParser.QsFieldBinding binding : qsPlan.fieldBindings) {
                Slot slot = findSlotByName(binding.fieldName, scan);
                if (slot == null) {
                    throw new AnalysisException(String.format(
                            "Field '%s' not found in table for search: %s",
                            binding.fieldName, search.getDslString()));
                }
                slotChildren.add(slot);
            }

            LOG.info("Rewriting search function: dsl='{}' with {} slot children",
                    search.getDslString(), slotChildren.size());

            // Create SearchExpression with slot children
            return new SearchExpression(search.getDslString(), qsPlan, slotChildren);

        } catch (Exception e) {
            throw new AnalysisException("Failed to rewrite search expression: " + e.getMessage(), e);
        }
    }

    private Slot findSlotByName(String fieldName, LogicalOlapScan scan) {
        for (Slot slot : scan.getOutput()) {
            if (slot.getName().equalsIgnoreCase(fieldName)) {
                return slot;
            }
        }
        return null;
    }

}
