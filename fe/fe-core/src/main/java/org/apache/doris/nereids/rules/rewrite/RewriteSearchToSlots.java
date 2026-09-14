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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SearchExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Search;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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

    private Expression rewriteExpression(Expression expr, Plan scan) {
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

    private Expression rewriteSearch(Search search, Plan scan) {
        try {
            // Parse DSL to get field bindings
            SearchDslParser.QsPlan qsPlan = search.getQsPlan();
            if (qsPlan == null || qsPlan.getFieldBindings() == null || qsPlan.getFieldBindings().isEmpty()) {
                LOG.warn("Search function has no field bindings: {}", search.getDslString());
                return search;
            }

            Map<String, String> normalizedFields = new HashMap<>();
            Map<String, String> fieldAnalyzers = new HashMap<>();
            Set<List<String>> qualifiers = new HashSet<>();

            // Create slot reference children from field bindings
            List<Expression> slotChildren = new ArrayList<>();
            for (SearchDslParser.QsFieldBinding binding : qsPlan.getFieldBindings()) {
                String bindingName = binding.getFieldName();
                String originalFieldName = bindingName;
                int analyzerSeparator = bindingName.lastIndexOf('@');
                while (analyzerSeparator > 0 && bindingName.charAt(analyzerSeparator - 1) == '\\') {
                    analyzerSeparator = bindingName.lastIndexOf('@', analyzerSeparator - 1);
                }
                if (analyzerSeparator >= 0 && findSlotByName(bindingName, scan) == null) {
                    originalFieldName = bindingName.substring(0, analyzerSeparator);
                    String analyzer = bindingName.substring(analyzerSeparator + 1);
                    if (originalFieldName.isEmpty() || analyzer.isEmpty()) {
                        throw new AnalysisException("SEARCH analyzer selector must be field@analyzer: " + bindingName);
                    }
                    binding.setAnalyzerName(analyzer);
                }
                originalFieldName = originalFieldName.replace("\\@", "@");
                Expression childExpr;
                String normalizedFieldName;

                if (originalFieldName.contains(".")) {
                    int firstDotPos = originalFieldName.indexOf('.');
                    String parentFieldName = originalFieldName.substring(0, firstDotPos);
                    String subcolumnPath = originalFieldName.substring(firstDotPos + 1);

                    // Find parent slot
                    Slot parentSlot = findSlotByName(parentFieldName, scan);
                    if (parentSlot == null) {
                        throw new AnalysisException(String.format(
                                "Parent field '%s' not found in table for search: %s",
                                parentFieldName, search.getDslString()));
                    }

                    // Verify it's a variant type
                    if (!parentSlot.getDataType().isVariantType()) {
                        throw new AnalysisException(String.format(
                                "Field '%s' is not VARIANT type for subcolumn access: %s",
                                parentFieldName, search.getDslString()));
                    }
                    String normalizedParentFieldName = parentSlot.getName();

                    // Check the parent variant column has at least one INVERTED index. The concrete
                    // subcolumn binding is resolved per-segment in BE, so we only enforce the parent
                    // level here. See function_search.cpp is_variant_sub branch.
                    checkInvertedIndexExists(tableForSlot(parentSlot, scan), normalizedParentFieldName,
                            search.getDslString(), true);

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
                    Slot slot = findSlotByName(originalFieldName, scan);
                    if (slot == null) {
                        throw new AnalysisException(String.format(
                                "Field '%s' not found in table for search: %s",
                                originalFieldName, search.getDslString()));
                    }
                    checkInvertedIndexExists(tableForSlot(slot, scan), slot.getName(), search.getDslString(), false);
                    childExpr = slot;
                    normalizedFieldName = slot.getName();
                }

                for (Slot input : childExpr.getInputSlots()) {
                    qualifiers.add(input.getQualifier());
                }
                if (qualifiers.size() > 1) {
                    throw new AnalysisException("Each SEARCH expression must reference fields from one table; "
                            + "combine separate SEARCH expressions with SQL AND/OR");
                }
                String fieldKey = normalizedFieldName.toLowerCase(Locale.ROOT);
                if (fieldAnalyzers.containsKey(fieldKey)
                        && !Objects.equals(fieldAnalyzers.get(fieldKey), binding.getAnalyzerName())) {
                    throw new AnalysisException("SEARCH supports one analyzer per field; use separate SEARCH "
                            + "expressions for different analyzers on " + normalizedFieldName);
                }
                fieldAnalyzers.put(fieldKey, binding.getAnalyzerName());
                normalizedFields.put(bindingName, normalizedFieldName);
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

    /**
     * Ensure the column referenced by a Lucene-syntax SEARCH predicate has an inverted index.
     * Without this check the BE path would silently fall back to an empty bitmap (i.e. all FALSE),
     * which is indistinguishable from "no rows matched" to the user. Throw at planning time so the
     * behavior is consistent with referencing a non-existent column.
     *
     * @param table         table backing the LogicalOlapScan
     * @param columnName    column name (parent column name when isVariantParent)
     * @param dsl           original DSL, used in the error message
     * @param isVariantParent true when {@code columnName} is the parent of a variant subcolumn
     *                        access (e.g. {@code msg.body}); for that case any INVERTED index on
     *                        the parent column is accepted because the concrete subcolumn binding
     *                        is resolved per-segment in BE.
     */
    private void checkInvertedIndexExists(OlapTable table, String columnName, String dsl,
            boolean isVariantParent) {
        Column column = table.getColumn(columnName);
        if (column == null) {
            // Field existence is already validated by findSlotByName; if we reach here the schema
            // changed concurrently. Surface a clear error rather than fall through.
            throw new AnalysisException(String.format(
                    "Column '%s' not found in table '%s' for search: %s",
                    columnName, table.getName(), dsl));
        }

        if (isVariantParent) {
            for (Index index : table.getIndexes()) {
                if (index.getIndexType() != IndexType.INVERTED) {
                    continue;
                }
                List<String> columns = index.getColumns();
                if (columns != null && !columns.isEmpty()
                        && columnName.equalsIgnoreCase(columns.get(0))) {
                    return;
                }
            }
        } else if (table.getInvertedIndex(column, null) != null) {
            return;
        }

        throw new AnalysisException(String.format(
                "Field '%s' has no inverted index, cannot be used in search: %s. "
                        + "Create an inverted index on the column first "
                        + "(ALTER TABLE ... ADD INDEX ... USING INVERTED).",
                columnName, dsl));
    }

    private Slot findSlotByName(String fieldName, Plan scan) {
        Slot result = null;
        for (Slot slot : scan.getOutput()) {
            if (slot.getName().equalsIgnoreCase(fieldName)) {
                if (result != null) {
                    throw new AnalysisException("Ambiguous field '" + fieldName + "' in search()");
                }
                result = slot;
            }
        }
        return result;
    }

    private OlapTable tableForSlot(Slot slot, Plan plan) {
        if (plan instanceof LogicalOlapScan) {
            return ((LogicalOlapScan) plan).getTable();
        }
        if (slot instanceof SlotReference
                && ((SlotReference) slot).getOriginalTable().orElse(null) instanceof OlapTable) {
            return (OlapTable) ((SlotReference) slot).getOriginalTable().get();
        }
        throw new AnalysisException("search() requires a field from an OLAP table: " + slot.toSql());
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
