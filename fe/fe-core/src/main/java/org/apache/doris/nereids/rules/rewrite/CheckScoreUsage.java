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

import org.apache.doris.analysis.InvertedIndexProperties;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.DdlException;
import org.apache.doris.indexpolicy.IndexPolicyMgr;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.SearchExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Score;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Check score function usage in project and aggregate without proper optimization context.
 *
 * This rule ensures that score() function is used only in contexts where it can be optimized,
 * requiring WHERE clause with MATCH function, ORDER BY and LIMIT.
 */
public class CheckScoreUsage implements RewriteRuleFactory {
    private static final Logger LOG = LogManager.getLogger(CheckScoreUsage.class);

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            logicalProject(any())
                .when(project -> {
                    boolean hasScore = hasScoreFunction(project);
                    boolean isOptimized = isScoreAlreadyOptimized(project);
                    LOG.debug("Project check - hasScore: " + hasScore + ", isOptimized: " + isOptimized);
                    return hasScore && !isOptimized;
                })
                .then(project -> {
                    throw new AnalysisException(
                            "score() function requires WHERE clause with MATCH function, "
                            + "ORDER BY and LIMIT for optimization");
                }).toRule(RuleType.CHECK_SCORE_USAGE),

            logicalAggregate(any())
                .when(this::hasScoreInAggregate)
                .then(agg -> {
                    throw new AnalysisException(
                            "score() function cannot be used in aggregate functions. "
                            + "score() requires WHERE clause with MATCH function, "
                            + "ORDER BY and LIMIT for optimization");
                }).toRule(RuleType.CHECK_SCORE_USAGE)
        );
    }

    static void checkScoringPolicyAdmission(LogicalFilter<LogicalOlapScan> filter,
            LogicalOlapScan scan, IndexPolicyMgr indexPolicyMgr) {
        for (Expression conjunct : filter.getConjuncts()) {
            for (Expression expression : conjunct.<Expression>collect(e -> e instanceof Match)) {
                Match match = (Match) expression;
                if (!(match.left() instanceof SlotReference)) {
                    // Wrapped MATCH inputs are resolved later; do not infer a selected index from nested slots.
                    continue;
                }
                SlotReference slot = (SlotReference) match.left();
                Index selectedIndex = resolveSelectedInvertedIndex(
                        slot, match.getAnalyzer().orElse(null));
                checkSelectedIndexPolicyAdmission(selectedIndex, scan, indexPolicyMgr);
            }

            for (Expression expression : conjunct.<Expression>collect(e -> e instanceof SearchExpression)) {
                SearchExpression search = (SearchExpression) expression;
                for (Expression slotExpression : search.getSlotChildren()) {
                    if (!(slotExpression instanceof SlotReference)) {
                        // Variant ElementAt bindings are resolved after this rule or per segment in the BE.
                        continue;
                    }
                    SlotReference slot = (SlotReference) slotExpression;
                    // SEARCH has no explicit analyzer option. Mirror the implicit index selection in
                    // ExpressionTranslator.visitSearchExpression so admission checks the same index.
                    Index selectedIndex = resolveSelectedInvertedIndex(slot, null);
                    checkSelectedIndexPolicyAdmission(selectedIndex, scan, indexPolicyMgr);
                }
            }
        }
    }

    private static Index resolveSelectedInvertedIndex(SlotReference slot, String analyzer) {
        Optional<OlapTable> originalTable = slot.getOriginalTable()
                .filter(OlapTable.class::isInstance)
                .map(OlapTable.class::cast);
        Optional<Column> originalColumn = slot.getOriginalColumn();
        if (!originalTable.isPresent() || !originalColumn.isPresent()) {
            // The BE remains the authoritative admission gate when rewrite metadata is lost.
            return null;
        }
        return originalTable.get().getInvertedIndex(
                originalColumn.get(), slot.getSubPath(), analyzer);
    }

    private static void checkSelectedIndexPolicyAdmission(Index selectedIndex,
            LogicalOlapScan scan, IndexPolicyMgr indexPolicyMgr) {
        if (selectedIndex == null) {
            return;
        }
        Map<String, String> properties = selectedIndex.getProperties();
        if (properties == null
                || !properties.containsKey(
                        InvertedIndexProperties.INVERTED_INDEX_ANALYZER_NAME_KEY)) {
            return;
        }

        String analyzerName = properties.get(
                InvertedIndexProperties.INVERTED_INDEX_ANALYZER_NAME_KEY);
        boolean usesCommonGrams;
        try {
            usesCommonGrams = indexPolicyMgr.validateAnalyzerUsesCommonGrams(analyzerName);
        } catch (DdlException e) {
            throw new AnalysisException("score() cannot use inverted index '"
                    + selectedIndex.getIndexName() + "': " + e.getMessage(), e);
        }
        if (usesCommonGrams
                && scan.getTable().getInvertedIndexFileStorageFormat()
                        != TInvertedIndexFileStorageFormat.SNII) {
            throw new AnalysisException("score() cannot use CommonGrams analyzer '"
                    + analyzerName + "' on inverted index '" + selectedIndex.getIndexName()
                    + "': CommonGrams scoring is supported only by SNII");
        }
    }

    private boolean hasScoreFunction(LogicalProject<?> project) {
        return project.getProjects().stream()
                .anyMatch(projection -> {
                    if (projection instanceof Alias) {
                        return containsScoreFunction(((Alias) projection).child());
                    }
                    return false;
                });
    }

    private boolean hasScoreInAggregate(LogicalAggregate<?> agg) {
        boolean hasScoreInOutput = agg.getOutputExpressions().stream()
                .anyMatch(this::containsScoreFunction);

        boolean hasScoreInGroupBy = agg.getGroupByExpressions().stream()
                .anyMatch(this::containsScoreFunction);

        return hasScoreInOutput || hasScoreInGroupBy;
    }

    private boolean containsScoreFunction(Expression expr) {
        if (expr instanceof Score) {
            return true;
        }
        return expr.children().stream().anyMatch(this::containsScoreFunction);
    }

    private boolean isScoreAlreadyOptimized(LogicalProject<?> project) {
        Plan child = project.child();
        if (child instanceof LogicalOlapScan) {
            LogicalOlapScan scan = (LogicalOlapScan) child;
            return scan.getVirtualColumns().stream()
                    .anyMatch(virtualCol -> {
                        if (virtualCol instanceof Alias) {
                            Expression childExpr = ((Alias) virtualCol).child();
                            return containsScoreFunction(childExpr);
                        }
                        return false;
                    });
        }
        return false;
    }
}
