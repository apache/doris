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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Column;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.iceberg.IcebergMergeOperation;
import org.apache.doris.datasource.iceberg.IcebergNereidsUtils;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.commands.delete.DeleteCommandContext;
import org.apache.doris.nereids.trees.plans.logical.LogicalIcebergMergeSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Merge-plan synthesizer for UPDATE on Iceberg tables, invoked via
 * IcebergRowLevelDmlTransform.synthesize. The legacy Command execution half
 * was removed as dead post-cutover code.
 *
 * UPDATE operations are implemented as a single scan + merge sink:
 *   1. Scan rows matching WHERE condition with row_id injected
 *   2. Project operation + row_id + updated columns
 *   3. Merge sink writes position deletes and new data files
 *   4. RowDelta commits delete + insert atomically
 */
public class IcebergUpdateCommand {

    private final List<EqualTo> assignments;
    private final List<String> nameParts;
    private final String tableAlias;
    private final LogicalPlan logicalQuery;
    private final DeleteCommandContext deleteCtx;

    /**
     * constructor
     */
    public IcebergUpdateCommand(
            List<String> nameParts,
            String tableAlias,
            List<EqualTo> assignments,
            LogicalPlan logicalQuery,
            DeleteCommandContext deleteCtx) {
        this.nameParts = Utils.copyRequiredList(nameParts);
        this.assignments = Utils.copyRequiredList(assignments);
        this.tableAlias = tableAlias;
        this.logicalQuery = logicalQuery;
        this.deleteCtx = deleteCtx != null ? deleteCtx : new DeleteCommandContext();
    }

    @VisibleForTesting
    LogicalPlan buildMergeProjectPlan(ConnectContext ctx, LogicalPlan logicalQuery,
                                      List<EqualTo> assignments, List<Column> columns, String tableName) {
        Map<String, Expression> colNameToExpression = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (EqualTo equalTo : assignments) {
            List<String> colNameParts = ((UnboundSlot) equalTo.left()).getNameParts();
            UpdateCommand.checkAssignmentColumn(ctx, colNameParts, this.nameParts, this.tableAlias);
            colNameToExpression.put(colNameParts.get(colNameParts.size() - 1), equalTo.right());
        }
        List<NamedExpression> updateColumns = buildUpdateSelectItems(colNameToExpression, columns, tableName);
        LogicalPlan planWithRowId = IcebergNereidsUtils.injectRowIdColumn(logicalQuery);
        NamedExpression rowIdColumn = getRowIdColumnExpr(planWithRowId);
        NamedExpression operationColumn = new UnboundAlias(
                new TinyIntLiteral(IcebergMergeOperation.UPDATE_OPERATION_NUMBER),
                IcebergMergeOperation.OPERATION_COLUMN);
        List<NamedExpression> projectItems = new ArrayList<>(2 + updateColumns.size());
        projectItems.add(operationColumn);
        projectItems.add(rowIdColumn);
        projectItems.addAll(updateColumns);
        for (Column col : columns) {
            if (IcebergUtils.isIcebergRowLineageColumn(col)) {
                projectItems.add(new UnboundSlot(tableName, col.getName()));
            }
        }
        return new LogicalProject<>(projectItems, planWithRowId);
    }

    // package-visible: the generic RowLevelDmlCommand shell delegates synthesis here (T07c).
    LogicalPlan buildMergePlan(ConnectContext ctx, LogicalPlan logicalQuery,
                                       List<EqualTo> assignments, ExternalTable icebergTable) {
        String tableName = tableAlias != null
                ? tableAlias
                : Util.getTempTableDisplayName(icebergTable.getName());
        LogicalPlan queryPlan = buildMergeProjectPlan(ctx, logicalQuery, assignments,
                icebergTable.getBaseSchema(true), tableName);

        List<NamedExpression> outputExprs;
        if (!IcebergNereidsUtils.hasUnboundPlan(queryPlan)) {
            outputExprs = queryPlan.getOutput().stream()
                    .map(NamedExpression.class::cast)
                    .collect(Collectors.toList());
        } else if (queryPlan instanceof LogicalProject) {
            outputExprs = ((LogicalProject<?>) queryPlan).getProjects();
        } else {
            outputExprs = ImmutableList.of();
        }

        return new LogicalIcebergMergeSink<>(
                (ExternalDatabase) icebergTable.getDatabase(),
                icebergTable,
                icebergTable.getBaseSchema(true),
                outputExprs,
                deleteCtx,
                Optional.empty(),
                Optional.empty(),
                queryPlan);
    }

    private NamedExpression getRowIdColumnExpr(LogicalPlan planWithRowId) {
        if (!IcebergNereidsUtils.hasUnboundPlan(planWithRowId)) {
            Optional<Slot> rowIdSlot = IcebergNereidsUtils.findRowIdSlot(planWithRowId.getOutput());
            if (rowIdSlot.isPresent()) {
                return (NamedExpression) rowIdSlot.get();
            }
        }
        return new UnboundSlot(Column.ICEBERG_ROWID_COL);
    }

    @VisibleForTesting
    List<NamedExpression> buildUpdateSelectItems(Map<String, Expression> colNameToExpression,
                                                 List<Column> columns, String tableName) {
        List<NamedExpression> selectItems = new ArrayList<>();
        for (Column column : columns) {
            if (!column.isVisible()) {
                continue;
            }
            if (colNameToExpression.containsKey(column.getName())) {
                Expression expr = colNameToExpression.get(column.getName());
                selectItems.add(expr instanceof UnboundSlot
                        ? ((NamedExpression) expr)
                        : new UnboundAlias(expr));
                colNameToExpression.remove(column.getName());
            } else {
                selectItems.add(new UnboundSlot(tableName, column.getName()));
            }
        }
        if (!colNameToExpression.isEmpty()) {
            throw new AnalysisException("unknown column in assignment list: "
                    + String.join(", ", colNameToExpression.keySet()));
        }
        return selectItems;
    }

    public DeleteCommandContext getDeleteCtx() {
        return deleteCtx;
    }
}
