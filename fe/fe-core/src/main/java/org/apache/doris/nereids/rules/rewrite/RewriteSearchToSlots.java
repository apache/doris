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
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SearchExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Search;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Bind the fields of a search() function: resolve each DSL field reference to a slot of the filter's child and
 * produce a SearchExpression whose children are those slots (or variant subcolumns of them).
 * This is crucial for BE's "action on slot" detection in normalize conjuncts.
 *
 * <p>The steps are kept apart on purpose. SearchDslParser owns the DSL syntax (field path, {@code @analyzer}
 * selector, escapes). Field names resolve exactly like SQL column references, so {@code alias.field} selects a
 * relation of a join before {@code column.subcolumn} is tried. Index validation and the field names sent to BE
 * use the physical column behind the slot, never its output alias. Where a SEARCH may be evaluated is not
 * decided here: PushDownIndexSearchAsVirtualColumn places it and CheckAfterRewrite verifies the final plan.
 */
public class RewriteSearchToSlots extends OneRewriteRuleFactory {
    private static final Logger LOG = LogManager.getLogger(RewriteSearchToSlots.class);

    @Override
    public Rule build() {
        return logicalFilter()
                .when(filter -> ExpressionUtils.containsTypes(filter.getExpressions(), Search.class))
                .thenApply(ctx -> rewriteSearchExpressions(ctx.root, ctx.cascadesContext))
                .toRule(RuleType.REWRITE_SEARCH_TO_SLOTS);
    }

    private Plan rewriteSearchExpressions(LogicalFilter<? extends Plan> filter, CascadesContext cascadesContext) {
        List<Expression> newExpressions = new ArrayList<>();

        for (Expression expr : filter.getExpressions()) {
            Expression rewritten = rewriteExpression(expr, filter.child(), cascadesContext);
            newExpressions.add(rewritten);
        }

        if (!newExpressions.equals(filter.getExpressions())) {
            return filter.withConjuncts(Utils.fastToImmutableSet(newExpressions));
        }

        return filter;
    }

    private Expression rewriteExpression(Expression expr, Plan child, CascadesContext cascadesContext) {
        if (expr instanceof Search) {
            return rewriteSearch((Search) expr, child, cascadesContext);
        }

        // Recursively process children
        List<Expression> newChildren = expr.children().stream()
                .map(c -> rewriteExpression(c, child, cascadesContext))
                .collect(Collectors.toList());

        if (!newChildren.equals(expr.children())) {
            return expr.withChildren(newChildren);
        }

        return expr;
    }

    private Expression rewriteSearch(Search search, Plan child, CascadesContext cascadesContext) {
        try {
            // Parse DSL to get field bindings
            SearchDslParser.QsPlan qsPlan = search.getQsPlan();
            if (qsPlan == null || qsPlan.getFieldBindings() == null || qsPlan.getFieldBindings().isEmpty()) {
                LOG.warn("Search function has no field bindings: {}", search.getDslString());
                return search;
            }

            Map<String, String> normalizedFields = new HashMap<>();
            // physical field -> the inverted index its analyzer selects
            Map<String, Index> fieldIndexes = new HashMap<>();
            Scope scope = new Scope(child.getOutput());
            ExpressionAnalyzer analyzer = new ExpressionAnalyzer(child, scope, cascadesContext, false, false);

            // Create slot reference children from field bindings
            List<Expression> slotChildren = new ArrayList<>();
            for (SearchDslParser.QsFieldBinding binding : qsPlan.getFieldBindings()) {
                String fieldReference = binding.getFieldName();
                Pair<String, String> pathAndAnalyzer = SearchDslParser.splitAnalyzerSelector(fieldReference);
                binding.setAnalyzerName(pathAndAnalyzer.second);

                Pair<SlotReference, List<String>> field = resolveField(pathAndAnalyzer.first, child, analyzer,
                        scope, search.getDslString());
                SlotReference slot = field.first;
                List<String> subPath = field.second;
                Index index = checkInvertedIndex(slot, subPath, binding.getAnalyzerName(), search.getDslString());

                String normalizedFieldName = physicalFieldName(slot, subPath);
                Expression childExpr = slot;
                if (!subPath.isEmpty()) {
                    // This will be converted to an extracted column slot by VariantSubPathPruning rule
                    // If the subcolumn doesn't exist, ElementAt will remain and BE will handle it gracefully
                    childExpr = new ElementAt(slot, new StringLiteral(String.join(".", subPath)));
                }

                // BE keeps one index per field, so two references to a field must select the same index.
                // Comparing the selected indexes reuses the analyzer identity of the index lookup itself.
                if (fieldIndexes.containsKey(normalizedFieldName)
                        && fieldIndexes.get(normalizedFieldName) != index) {
                    throw new AnalysisException("SEARCH supports one analyzer per field; use separate SEARCH "
                            + "expressions for different analyzers on " + normalizedFieldName);
                }
                fieldIndexes.put(normalizedFieldName, index);
                normalizedFields.put(fieldReference, normalizedFieldName);
                binding.setFieldName(normalizedFieldName);
                slotChildren.add(childExpr);
            }

            LOG.info("Rewriting search function: dsl='{}' with {} slot children",
                    search.getDslString(), slotChildren.size());

            normalizePlanFields(qsPlan.getRoot(), normalizedFields,
                    nestedPath -> {
                        Pair<SlotReference, List<String>> nested = resolveField(nestedPath, child, analyzer, scope,
                                search.getDslString());
                        return physicalFieldName(nested.first, nested.second);
                    });

            // Create SearchExpression with slot children
            return new SearchExpression(search.getDslString(), qsPlan, slotChildren);

        } catch (Exception e) {
            throw new AnalysisException("Failed to rewrite search expression: " + e.getMessage(), e);
        }
    }

    /**
     * Resolve a DSL field path against the child's output with the SQL name resolution rules
     * (ExpressionAnalyzer#bindSlotByScope): {@code field}, {@code alias.field}, {@code db.tbl.field}, each
     * optionally followed by a variant subcolumn path. Returns the slot and the subcolumn path (empty for a
     * plain column).
     */
    private Pair<SlotReference, List<String>> resolveField(String fieldPath, Plan child,
            ExpressionAnalyzer analyzer, Scope scope, String dsl) {
        List<String> nameParts = Arrays.asList(fieldPath.split("\\.", -1));
        if (nameParts.stream().anyMatch(String::isEmpty)) {
            throw new AnalysisException(String.format("Invalid field '%s' for search: %s", fieldPath, dsl));
        }
        List<Expression> candidates = analyzer.bindSlotByScope(new UnboundSlot(nameParts), scope)
                .stream().distinct().collect(Collectors.toList());
        if (candidates.isEmpty()) {
            throw new AnalysisException(String.format("Field '%s' not found in table for search: %s",
                    fieldPath, dsl));
        }
        if (candidates.size() > 1) {
            throw new AnalysisException(String.format("Ambiguous field '%s' in search(); qualify it with its "
                    + "table alias, as in a SQL column reference: %s", fieldPath, dsl));
        }

        // A nested reference is bound as Alias(element_at(element_at(slot, 'a'), 'b')).
        Expression bound = candidates.get(0) instanceof Alias ? candidates.get(0).child(0) : candidates.get(0);
        List<String> subPath = new ArrayList<>();
        while (bound instanceof ElementAt) {
            subPath.add(0, ((StringLiteral) bound.child(1)).getStringValue());
            bound = bound.child(0);
        }
        SlotReference boundSlot = (SlotReference) bound;
        if (!subPath.isEmpty() && !boundSlot.getDataType().isVariantType()) {
            throw new AnalysisException(String.format(
                    "Field '%s' is not VARIANT type for subcolumn access: %s", boundSlot.getName(), dsl));
        }
        // bindSlotByScope renames the slot to the spelling of the reference; keep the child's own slot.
        for (Slot output : child.getOutput()) {
            if (output.getExprId().equals(boundSlot.getExprId())) {
                return Pair.of((SlotReference) output, subPath);
            }
        }
        throw new AnalysisException(String.format("Field '%s' not found in table for search: %s", fieldPath, dsl));
    }

    // BE looks fields up in the tablet schema, so it gets the physical column name, never an output alias.
    private String physicalFieldName(SlotReference slot, List<String> subPath) {
        String columnName = slot.getOriginalColumn().map(Column::getName).orElse(slot.getName());
        return subPath.isEmpty() ? columnName : columnName + "." + String.join(".", subPath);
    }

    /**
     * Ensure the physical column referenced by a Lucene-syntax SEARCH predicate has an inverted index, and return
     * the index the analyzer selects (OlapTable#getInvertedIndex, the lookup the translator sends to BE; null
     * when a variant subcolumn gets its index per segment in BE).
     * Without this check the BE path would silently fall back to an empty bitmap (i.e. all FALSE),
     * which is indistinguishable from "no rows matched" to the user. Throw at planning time so the
     * behavior is consistent with referencing a non-existent column.
     *
     * @param slot     resolved field; its original table and column identify the physical column, whatever
     *                 alias the slot carries
     * @param subPath  variant subcolumn path, empty for a plain column. For a subcolumn any INVERTED index on
     *                 the parent column is accepted because the concrete subcolumn binding is resolved
     *                 per-segment in BE. See function_search.cpp is_variant_sub branch.
     * @param analyzer analyzer selected by {@code field@analyzer}, or null
     * @param dsl      original DSL, used in the error message
     */
    private Index checkInvertedIndex(SlotReference slot, List<String> subPath, String analyzer, String dsl) {
        if (!(slot.getOriginalTable().orElse(null) instanceof OlapTable) || !slot.getOriginalColumn().isPresent()) {
            throw new AnalysisException("search() requires a field from an OLAP table: " + slot.toSql());
        }
        OlapTable table = (OlapTable) slot.getOriginalTable().get();
        Column column = slot.getOriginalColumn().get();
        Index index = table.getInvertedIndex(column, subPath, analyzer);
        if (index == null && analyzer != null) {
            throw new AnalysisException(String.format(
                    "No inverted index found for SEARCH analyzer '%s' on field '%s': %s",
                    analyzer, column.getName(), dsl));
        }
        boolean hasIndex = index != null;
        if (!subPath.isEmpty()) {
            hasIndex = table.getIndexes().stream().anyMatch(i -> i.getIndexType() == IndexType.INVERTED
                    && i.getColumns() != null && !i.getColumns().isEmpty()
                    && column.getName().equalsIgnoreCase(i.getColumns().get(0)));
        }
        if (!hasIndex) {
            throw new AnalysisException(String.format(
                    "Field '%s' has no inverted index, cannot be used in search: %s. "
                            + "Create an inverted index on the column first "
                            + "(ALTER TABLE ... ADD INDEX ... USING INVERTED).",
                    column.getName(), dsl));
        }
        return index;
    }

    private void normalizePlanFields(SearchDslParser.QsNode node, Map<String, String> normalized,
            Function<String, String> nestedPathNormalizer) {
        if (node == null) {
            return;
        }
        // Variant subcolumn paths are case sensitive, so match the reference exactly as the binding was named.
        if (node.getField() != null && !node.getField().isEmpty()) {
            // The parser names every binding after a node's field.
            Preconditions.checkState(normalized.containsKey(node.getField()),
                    "SEARCH field %s has no binding", node.getField());
            node.setField(normalized.get(node.getField()));
        }
        if (node.getNestedPath() != null) {
            node.setNestedPath(nestedPathNormalizer.apply(node.getNestedPath()));
        }
        if (node.getChildren() != null) {
            for (SearchDslParser.QsNode child : node.getChildren()) {
                normalizePlanFields(child, normalized, nestedPathNormalizer);
            }
        }
    }

}
