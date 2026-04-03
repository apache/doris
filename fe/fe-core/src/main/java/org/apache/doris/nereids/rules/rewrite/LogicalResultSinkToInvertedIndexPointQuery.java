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

import org.apache.doris.analysis.InvertedIndexUtil;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Inverted index point query optimization.
 * Triggers when WHERE has exactly one equality condition on a non-fulltext inverted index column.
 * Works for both select * and select specific columns.
 */
public class LogicalResultSinkToInvertedIndexPointQuery implements RewriteRuleFactory {

    private Expression removeCast(Expression expression) {
        if (expression instanceof Cast) {
            return expression.child(0);
        }
        return expression;
    }

    private boolean scanMatchCondition(LogicalOlapScan olapScan) {
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null && ctx.getSessionVariable().isEnableInvertedIndexPointQuery()) {
            return true;
        }
        // Even if session variable is off, check if any inverted index has point_query=true
        OlapTable olapTable = olapScan.getTable();
        for (Index index : olapTable.getIndexes()) {
            if (index.getIndexType() == org.apache.doris.analysis.IndexDef.IndexType.INVERTED
                    && InvertedIndexUtil.isPointQueryEnabled(index.getProperties())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if filter conjuncts contain exactly one inverted-index equality driver condition.
     * Other conjuncts (non-inverted-index) are allowed as normal post-filters.
     */
    private boolean filterMatchCondition(LogicalFilter<LogicalOlapScan> filter) {
        OlapTable olapTable = filter.child().getTable();
        int invertedIndexEqCount = 0;
        for (Expression expr : filter.getConjuncts()) {
            if (isInvertedIndexEqualityOnColumn(expr, olapTable)) {
                invertedIndexEqCount++;
            }
        }
        return invertedIndexEqCount == 1;
    }

    private boolean isInvertedIndexEqualityOnColumn(Expression expr, OlapTable table) {
        if (!(expr instanceof EqualTo)) {
            return false;
        }
        Expression left = removeCast(expr.child(0));
        Expression right = expr.child(1);
        if (!(left instanceof SlotReference) || !right.isLiteral()) {
            return false;
        }
        SlotReference slot = (SlotReference) left;
        if (!slot.getOriginalColumn().isPresent()) {
            return false;
        }
        Column column = slot.getOriginalColumn().get();
        Index invertedIndex = table.getInvertedIndex(column, Collections.emptyList());
        if (invertedIndex == null) {
            return false;
        }
        // If index has point_query=true, it always qualifies
        if (InvertedIndexUtil.isPointQueryEnabled(invertedIndex.getProperties())) {
            return true;
        }
        // Otherwise, session variable must be on, and only non-fulltext indexes qualify
        ConnectContext ctx = ConnectContext.get();
        if (ctx == null || !ctx.getSessionVariable().isEnableInvertedIndexPointQuery()) {
            return false;
        }
        if (invertedIndex.isAnalyzedInvertedIndex()) {
            return false;
        }
        return true;
    }

    private Plan trySetInvertedIndexPointQuery(Plan root, OlapTable olapTable,
            Set<Expression> conjuncts, StatementContext statementContext) {
        if (statementContext.isShortCircuitQuery()) {
            return root;
        }
        for (Expression expr : conjuncts) {
            if (isInvertedIndexEqualityOnColumn(expr, olapTable)) {
                SlotReference slot = (SlotReference) removeCast(expr.child(0));
                Column column = slot.getOriginalColumn().get();
                String literalValue = expr.child(1).toSql();
                statementContext.setInvertedIndexPointQuery(true);
                statementContext.setInvertedIndexPointQueryColumnName(column.getName());
                statementContext.setInvertedIndexPointQueryColumnUniqueId(column.getUniqueId());
                statementContext.setInvertedIndexPointQueryLiteralValue(literalValue);
                return root;
            }
        }
        return root;
    }

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // With Project: ResultSink -> Project -> Filter -> OlapScan
                RuleType.INVERTED_INDEX_POINT_QUERY.build(
                        logicalResultSink(logicalProject(logicalFilter(logicalOlapScan()
                                .when(this::scanMatchCondition)
                        ).when(this::filterMatchCondition)))
                                .thenApply(ctx -> {
                                    return trySetInvertedIndexPointQuery(ctx.root,
                                            ctx.root.child().child().child().getTable(),
                                            ctx.root.child().child().getConjuncts(),
                                            ctx.statementContext);
                                })),
                // Without Project: ResultSink -> Filter -> OlapScan
                RuleType.INVERTED_INDEX_POINT_QUERY.build(
                        logicalResultSink(logicalFilter(logicalOlapScan()
                                .when(this::scanMatchCondition)
                        ).when(this::filterMatchCondition))
                                .thenApply(ctx -> {
                                    return trySetInvertedIndexPointQuery(ctx.root,
                                            ctx.root.child().child().getTable(),
                                            ctx.root.child().getConjuncts(),
                                            ctx.statementContext);
                                }))
        );
    }
}
