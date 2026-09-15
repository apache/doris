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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.doris.RemoteOlapTable;
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
import org.apache.doris.policy.PolicyMgr;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContext.ConnectType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/**
 * short circuit query optimization
 * pattern : select xxx from tbl where key = ?
 */
public class LogicalResultSinkToShortCircuitPointQuery implements RewriteRuleFactory {

    private Expression removeInjectiveCast(Expression expression) {
        if (expression instanceof Cast
                && expression.child(0).getDataType().isInjectiveCastTo(expression.getDataType())) {
            return expression.child(0);
        }
        return expression;
    }

    private boolean filterMatchShortCircuitCondition(LogicalFilter<LogicalOlapScan> filter) {
        return filter.getConjuncts().stream().allMatch(
                // all conjuncts match with pattern `key = literal`
                expression -> (expression instanceof EqualTo)
                        && (removeInjectiveCast(expression.child(0)).isKeyColumnFromTable()
                        || (expression.child(0) instanceof SlotReference
                        && ((SlotReference) expression.child(0)).getName().equals(Column.DELETE_SIGN)))
                        && expression.child(1).isLiteral());
    }

    /** Any row policy on the table makes point-query planning ineligible, regardless of its target. */
    private boolean hasRowPolicy(OlapTable table, StatementContext statementContext) {
        try {
            DatabaseIf<?> database = table.getDatabase();
            CatalogIf<?> catalog = database == null ? null : database.getCatalog();
            ConnectContext connectContext = statementContext.getConnectContext();
            PolicyMgr policyMgr = connectContext == null || connectContext.getEnv() == null
                    ? null : connectContext.getEnv().getPolicyMgr();
            return database == null || catalog == null || policyMgr == null
                    || policyMgr.hasRowPolicy(catalog.getName(), database.getFullName(), table.getName());
        } catch (RuntimeException e) {
            return true;
        }
    }

    @VisibleForTesting
    boolean scanMatchShortCircuitCondition(LogicalOlapScan olapScan) {
        ConnectContext connectContext = ConnectContext.get();
        if (!connectContext.getSessionVariable().isEnableShortCircuitQuery()) {
            return false;
        }
        // The short circuit produces no Arrow result at either end. PointQueryExecutor is not a
        // Coordinator, and Coordinator/NereidsCoordinator are the only places that register a
        // FlightSqlEndpointsLocation, so GetFlightInfo found none and failed the query with
        // "no FlightSqlEndpointsLocations"; the BE side cannot be pointed at either, since the lookup rpc
        // serializes with VMysqlResultWriter into PTabletKeyLookupResponse.row_batch and never creates the
        // ArrowFlightResultBlockBuffer that fetch_arrow_flight_schema looks up. Keep Arrow Flight SQL on
        // the normal execution path. This has to be decided here at plan time rather than when picking the
        // executor: OlapScanNode.computeTabletInfo and several rewrite and property rules read
        // StatementContext.isShortCircuitQuery() while building the plan. See #67368.
        if (connectContext.getConnectType() == ConnectType.ARROW_FLIGHT_SQL) {
            return false;
        }
        // Lazy point-query pruning does not preserve explicit PARTITION/TABLET restrictions.
        // Keep these queries on the normal execution path so the physical scan enforces them.
        if (!olapScan.getManuallySpecifiedPartitions().isEmpty()
                || !olapScan.getManuallySpecifiedTabletIds().isEmpty()) {
            return false;
        }
        OlapTable olapTable = olapScan.getTable();
        // Remote Doris metadata refresh replaces the RemoteOlapTable instance. A prepared context retains the old
        // instance, so its table-local topology version cannot observe remote partition changes.
        if (olapTable instanceof RemoteOlapTable) {
            return false;
        }
        if (olapTable.hasVariantColumns()) {
            return false;
        }
        return olapTable.getEnableLightSchemaChange() && olapTable.getEnableUniqueKeyMergeOnWrite()
                        && olapTable.storeRowColumn();
    }

    // set short circuit flag and return the original plan
    private Plan shortCircuit(Plan root, OlapTable olapTable,
                Set<Expression> conjuncts, StatementContext statementContext) {
        // Keep policy-bearing tables, inlined views, and placeholders outside the final filter on
        // the normal path. A cached no-policy plan repeats the table-level lookup before reuse.
        if (hasRowPolicy(olapTable, statementContext)
                || statementContext.getSecurityDependencyContext().hasEffectiveRowPolicy()
                || statementContext.getSecurityDependencyContext().hasDataMask()
                || statementContext.hasNonFilterPlaceholder()
                || !statementContext.getViewDdlSqls().isEmpty()) {
            return root;
        }
        // All key columns in conjuncts
        Set<String> colNames = Sets.newHashSet();
        for (Expression expr : conjuncts) {
            SlotReference slot = (SlotReference) removeInjectiveCast(expr.child(0));
            if (slot.isKeyColumnFromTable()) {
                // The executor updates cached conjuncts by column name. More than one predicate on
                // the same key would make a fixed literal indistinguishable from a placeholder.
                if (!colNames.add(slot.getName())) {
                    return root;
                }
            }
        }
        // set short circuit flag and modify nothing to the plan
        if (olapTable.getBaseSchemaKeyColumns().size() == colNames.size()) {
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
