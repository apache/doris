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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/**
 * short circuit query optimization
 * pattern : select xxx from tbl where key = ? or key IN (?, ?, ...)
 */
public class LogicalResultSinkToShortCircuitPointQuery implements RewriteRuleFactory {

    private Expression removeCast(Expression expression) {
        if (expression instanceof Cast) {
            return expression.child(0);
        }
        return expression;
    }

    // Check if an expression in the filter is a valid short-circuit condition.
    // Supports: key = literal/placeholder, or key IN (literal/placeholder, ...)
    private boolean isValidShortCircuitExpression(Expression expression) {
        if (expression instanceof EqualTo) {
            // key = literal or key = placeholder
            return (removeCast(expression.child(0)).isKeyColumnFromTable()
                        || (expression.child(0) instanceof SlotReference
                        && ((SlotReference) expression.child(0)).getName().equals(Column.DELETE_SIGN)))
                    && (expression.child(1).isLiteral() || expression.child(1) instanceof Placeholder);
        } else if (expression instanceof InPredicate) {
            // key IN (literal/placeholder, ...)
            InPredicate inPredicate = (InPredicate) expression;
            Expression compareExpr = removeCast(inPredicate.getCompareExpr());
            if (!compareExpr.isKeyColumnFromTable()) {
                return false;
            }
            // All options must be literals or placeholders
            for (Expression option : inPredicate.getOptions()) {
                if (!(option instanceof Literal) && !(option instanceof Placeholder)) {
                    return false;
                }
            }
            return !inPredicate.getOptions().isEmpty();
        }
        return false;
    }

    private boolean filterMatchShortCircuitCondition(LogicalFilter<LogicalOlapScan> filter) {
        return filter.getConjuncts().stream().allMatch(this::isValidShortCircuitExpression);
    }

    private boolean scanMatchShortCircuitCondition(LogicalOlapScan olapScan) {
        if (!ConnectContext.get().getSessionVariable().isEnableShortCircuitQuery()) {
            return false;
        }
        OlapTable olapTable = olapScan.getTable();
        if (olapTable.hasVariantColumns()) {
            return false;
        }
        return olapTable.getEnableLightSchemaChange() && olapTable.getEnableUniqueKeyMergeOnWrite()
                        && olapTable.storeRowColumn();
    }

    // set short circuit flag and return the original plan
    private Plan shortCircuit(Plan root, OlapTable olapTable,
                Set<Expression> conjuncts, StatementContext statementContext) {
        // All key columns covered by conjuncts (EqualTo or InPredicate)
        Set<String> colNames = Sets.newHashSet();
        for (Expression expr : conjuncts) {
            if (expr instanceof EqualTo) {
                SlotReference slot = (SlotReference) removeCast(expr.child(0));
                if (slot.isKeyColumnFromTable()) {
                    colNames.add(slot.getName());
                }
            } else if (expr instanceof InPredicate) {
                InPredicate inPredicate = (InPredicate) expr;
                Expression compareExpr = removeCast(inPredicate.getCompareExpr());
                if (compareExpr instanceof SlotReference
                        && ((SlotReference) compareExpr).isKeyColumnFromTable()) {
                    colNames.add(((SlotReference) compareExpr).getName());
                }
            }
        }
        // set short circuit flag and modify nothing to the plan
        if (olapTable.getBaseSchemaKeyColumns().size() <= colNames.size()) {
            statementContext.setShortCircuitQuery(true);
        }
        return root;
    }

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.SHOR_CIRCUIT_POINT_QUERY.build(
                        logicalResultSink(logicalProject(logicalFilter(logicalOlapScan()
                            .when(this::scanMatchShortCircuitCondition)
                    ).when(this::filterMatchShortCircuitCondition)))
                        .thenApply(ctx -> {
                            return shortCircuit(ctx.root, ctx.root.child().child().child().getTable(),
                                        ctx.root.child().child().getConjuncts(), ctx.statementContext);
                        })),
                RuleType.SHOR_CIRCUIT_POINT_QUERY.build(
                        logicalResultSink(logicalFilter(logicalOlapScan()
                                .when(this::scanMatchShortCircuitCondition)
                        ).when(this::filterMatchShortCircuitCondition))
                                .thenApply(ctx -> {
                                    return shortCircuit(ctx.root, ctx.root.child().child().getTable(),
                                            ctx.root.child().getConjuncts(), ctx.statementContext);
                                }))
        );
    }
}
