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
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.datasource.doris.RemoteOlapTable;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.qe.BatchPointQueryExecutor;
import org.apache.doris.qe.ConnectContext;

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

    private Expression removeCast(Expression expression) {
        if (expression instanceof Cast) {
            return expression.child(0);
        }
        return expression;
    }

    private boolean filterMatchShortCircuitCondition(LogicalFilter<LogicalOlapScan> filter) {
        return batchFilterMatchShortCircuitCondition(filter) || filter.getConjuncts().stream().allMatch(
                // all conjuncts match with pattern `key = ?`
                expression -> (expression instanceof EqualTo)
                        && (removeCast(expression.child(0)).isKeyColumnFromTable()
                        || (expression.child(0) instanceof SlotReference
                        && ((SlotReference) expression.child(0)).getName().equals(Column.DELETE_SIGN)))
                        && expression.child(1).isLiteral());
    }

    private boolean batchFilterMatchShortCircuitCondition(LogicalFilter<LogicalOlapScan> filter) {
        // Limit the first batch implementation to literal lookups on one VARCHAR hash key.
        // Prepared statements must not cache an IN plan in the equality-only parameter updater.
        if (!ConnectContext.get().getSessionVariable().isEnableBatchPointQuery()
                || ConnectContext.get().getCommand() != MysqlCommand.COM_QUERY
                || ConnectContext.get().getSessionVariable().isInDebugMode()
                || filter.child().getTableSample().isPresent()) {
            // Debug scans can skip versions/deletes, and sampling must keep the normal scan semantics.
            return false;
        }
        OlapTable table = filter.child().getTable();
        List<Column> keys = table.getBaseSchemaKeyColumns();
        if (keys.size() != 1 || !keys.get(0).getType().isVarchar() || keys.get(0).isAllowNull()
                || table.getPartitionInfo().getType() != PartitionType.UNPARTITIONED
                || !(table.getDefaultDistributionInfo() instanceof HashDistributionInfo)
                || table.getTableProperty().getCopiedRowStoreColumns() != null) {
            return false;
        }
        List<Column> distributionColumns = ((HashDistributionInfo) table.getDefaultDistributionInfo())
                .getDistributionColumns();
        if (distributionColumns.size() != 1 || !distributionColumns.get(0).equals(keys.get(0))) {
            return false;
        }
        int inCount = 0;
        for (Expression expression : filter.getConjuncts()) {
            if (expression instanceof InPredicate) {
                InPredicate in = (InPredicate) expression;
                if (!(in.getCompareExpr() instanceof SlotReference)
                        || !in.getCompareExpr().isKeyColumnFromTable()
                        || in.getOptions().isEmpty()
                        || in.getOptions().size() > BatchPointQueryExecutor.MAX_KEYS
                        || !in.getOptions().stream().allMatch(option -> option instanceof StringLikeLiteral)) {
                    return false;
                }
                ++inCount;
            } else if (!(expression instanceof EqualTo)
                    || !(expression.child(0) instanceof SlotReference)
                    || !((SlotReference) expression.child(0)).getName().equals(Column.DELETE_SIGN)
                    || !expression.child(1).isLiteral()
                    || !expression.child(1).toSql().equals("0")) {
                // Additional predicates need the normal scan's filter evaluation.
                return false;
            }
        }
        return inCount == 1;
    }

    @VisibleForTesting
    boolean scanMatchShortCircuitCondition(LogicalOlapScan olapScan) {
        ConnectContext connectContext = ConnectContext.get();
        if (!connectContext.getSessionVariable().isEnableShortCircuitQuery()) {
            return false;
        }
        // A protocol whose client pulls the result from the backend has no result to pull for a
        // short circuit (see FlightProtocolAdapter.supportsShortCircuitPointQuery). This has to be
        // decided here at plan time rather than when picking the executor: OlapScanNode.computeTabletInfo
        // and several rewrite and property rules read StatementContext.isShortCircuitQuery() while
        // building the plan. See #67368.
        if (!connectContext.getProtocolAdapter().supportsShortCircuitPointQuery()) {
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
        if (conjuncts.stream().anyMatch(expression -> expression instanceof InPredicate)
                && root.child(0) instanceof LogicalProject) {
            LogicalProject<?> project = (LogicalProject<?>) root.child(0);
            // Scalar functions can have different evaluation boundaries in the point-query executor.
            if (!project.getProjects().stream().allMatch(expression -> expression instanceof SlotReference
                    || (expression instanceof Alias && expression.child(0) instanceof SlotReference))) {
                return root;
            }
        }
        // All key columns in conjuncts
        Set<String> colNames = Sets.newHashSet();
        for (Expression expr : conjuncts) {
            SlotReference slot = ((SlotReference) removeCast((expr.child(0))));
            if (slot.isKeyColumnFromTable()) {
                colNames.add(slot.getName());
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
