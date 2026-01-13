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

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.iceberg.IcebergConflictDetectionFilterUtils;
import org.apache.doris.datasource.iceberg.IcebergExternalDatabase;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergMergeOperation;
import org.apache.doris.datasource.iceberg.IcebergRowId;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.Unbound;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.delete.DeleteCommandContext;
import org.apache.doris.nereids.trees.plans.commands.insert.IcebergMergeExecutor;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalIcebergMergeSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIcebergMergeSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * UPDATE command for Iceberg tables.
 *
 * UPDATE operations are implemented as a single scan + merge sink:
 *   1. Scan rows matching WHERE condition with row_id injected
 *   2. Project operation + row_id + updated columns
 *   3. Merge sink writes position deletes and new data files
 *   4. RowDelta commits delete + insert atomically
 */
public class IcebergUpdateCommand extends Command implements ForwardWithSync, Explainable {

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
        super(PlanType.UPDATE_COMMAND);
        this.nameParts = Utils.copyRequiredList(nameParts);
        this.assignments = Utils.copyRequiredList(assignments);
        this.tableAlias = tableAlias;
        this.logicalQuery = logicalQuery;
        this.deleteCtx = deleteCtx != null ? deleteCtx : new DeleteCommandContext();
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // Check if target table is Iceberg table
        List<String> qualifiedTableName = RelationUtil.getQualifierName(ctx, nameParts);
        TableIf table = RelationUtil.getTable(qualifiedTableName, ctx.getEnv(), Optional.empty());

        if (!(table instanceof IcebergExternalTable)) {
            throw new AnalysisException("UPDATE command can only be used on Iceberg tables. "
                    + "Table " + Util.getTempTableDisplayName(table.getName()) + " is not an Iceberg table.");
        }

        IcebergExternalTable icebergTable = (IcebergExternalTable) table;

        // Verify table format version (must be v2+ for update support)
        // org.apache.iceberg.Table icebergTableObj = icebergTable.getIcebergTable();
        // String formatVersionStr = icebergTableObj.properties().get("format-version");
        // int formatVersion = formatVersionStr != null ? Integer.parseInt(formatVersionStr) : 1;
        // if (formatVersion < 2) {
        //     throw new AnalysisException("Iceberg table UPDATE requires format version >= 2. "
        //             + "Current format version: " + formatVersion);
        // }

        boolean previousNeedIcebergRowId = ctx.needIcebergRowId();
        ctx.setNeedIcebergRowId(true);
        try {
            // UPDATE is implemented as a single merge plan (delete + insert in one scan)
            LogicalPlan mergePlan = buildMergePlan(ctx, logicalQuery, assignments, icebergTable);
            executeMergePlan(ctx, executor, icebergTable, mergePlan);
        } finally {
            ctx.setNeedIcebergRowId(previousNeedIcebergRowId);
        }
    }

    private boolean executeMergePlan(ConnectContext ctx, StmtExecutor executor,
                                     IcebergExternalTable icebergTable,
                                     LogicalPlan logicalPlan) throws Exception {
        LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(logicalPlan, ctx.getStatementContext());
        NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
        planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());
        executor.setPlanner(planner);
        executor.checkBlockRules();
        Optional<org.apache.iceberg.expressions.Expression> conflictFilter =
                IcebergConflictDetectionFilterUtils.buildConflictDetectionFilter(
                        planner.getAnalyzedPlan(), icebergTable);

        PhysicalSink<?> physicalSink = getPhysicalMergeSink(planner);
        PlanFragment fragment = planner.getFragments().get(0);
        DataSink dataSink = fragment.getSink();
        boolean emptyInsert = childIsEmptyRelation(physicalSink);
        String label = String.format("iceberg_update_merge_%x_%x", ctx.queryId().hi, ctx.queryId().lo);

        IcebergMergeExecutor insertExecutor =
                new IcebergMergeExecutor(ctx, icebergTable, label, planner, emptyInsert, -1L);
        insertExecutor.setConflictDetectionFilter(conflictFilter);

        if (insertExecutor.isEmptyInsert()) {
            return true;
        }

        insertExecutor.beginTransaction();
        insertExecutor.finalizeSinkForMerge(fragment, dataSink, physicalSink);
        insertExecutor.getCoordinator().setTxnId(insertExecutor.getTxnId());
        executor.setCoord(insertExecutor.getCoordinator());
        insertExecutor.executeSingleInsert(executor);
        return ctx.getState().getStateType() != QueryState.MysqlStateType.ERR;
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
        LogicalPlan planWithRowId = injectRowIdColumn(logicalQuery);
        NamedExpression rowIdColumn = getRowIdColumnExpr(planWithRowId);
        NamedExpression operationColumn = new UnboundAlias(
                new TinyIntLiteral(IcebergMergeOperation.UPDATE_OPERATION_NUMBER),
                IcebergMergeOperation.OPERATION_COLUMN);

        List<NamedExpression> projectItems = new ArrayList<>(2 + updateColumns.size());
        projectItems.add(operationColumn);
        projectItems.add(rowIdColumn);
        projectItems.addAll(updateColumns);
        return new LogicalProject<>(projectItems, planWithRowId);
    }

    private LogicalPlan buildMergePlan(ConnectContext ctx, LogicalPlan logicalQuery,
                                       List<EqualTo> assignments, IcebergExternalTable icebergTable) {
        String tableName = tableAlias != null
                ? tableAlias
                : Util.getTempTableDisplayName(icebergTable.getName());
        LogicalPlan queryPlan = buildMergeProjectPlan(ctx, logicalQuery, assignments,
                icebergTable.getBaseSchema(true), tableName);

        List<NamedExpression> outputExprs;
        if (!hasUnboundPlan(queryPlan)) {
            outputExprs = queryPlan.getOutput().stream()
                    .map(NamedExpression.class::cast)
                    .collect(Collectors.toList());
        } else if (queryPlan instanceof LogicalProject) {
            outputExprs = ((LogicalProject<?>) queryPlan).getProjects();
        } else {
            outputExprs = ImmutableList.of();
        }

        return new LogicalIcebergMergeSink<>(
                (IcebergExternalDatabase) icebergTable.getDatabase(),
                icebergTable,
                icebergTable.getBaseSchema(true),
                outputExprs,
                deleteCtx,
                Optional.empty(),
                Optional.empty(),
                queryPlan);
    }

    private LogicalPlan injectRowIdColumn(LogicalPlan plan) {
        if (hasUnboundPlan(plan)) {
            return plan;
        }
        return (LogicalPlan) plan.accept(new IcebergRowIdInjector(), null);
    }

    private static boolean hasUnboundPlan(Plan plan) {
        return plan.anyMatch(node -> node instanceof Unbound || ((Plan) node).hasUnboundExpression());
    }

    private static class IcebergRowIdInjector extends DefaultPlanRewriter<Void> {
        @Override
        public Plan visitLogicalFileScan(LogicalFileScan scan, Void context) {
            if (!(scan.getTable() instanceof IcebergExternalTable)) {
                return scan;
            }
            if (hasRowIdSlot(scan.getOutput())) {
                return scan;
            }
            IcebergExternalTable table = (IcebergExternalTable) scan.getTable();
            Column rowIdColumn = getRowIdColumn(table);
            SlotReference rowIdSlot = SlotReference.fromColumn(
                    StatementScopeIdGenerator.newExprId(), table, rowIdColumn, scan.getQualifier());
            List<Slot> outputs = new ArrayList<>(scan.getOutput());
            outputs.add(rowIdSlot);
            return scan.withCachedOutput(outputs);
        }

        @Override
        public Plan visitLogicalProject(LogicalProject<? extends Plan> project, Void context) {
            project = (LogicalProject<? extends Plan>) visitChildren(this, project, context);
            Optional<Slot> rowIdSlot = findRowIdSlot(project.child().getOutput());
            if (!rowIdSlot.isPresent() || hasRowIdProject(project.getProjects())) {
                return project;
            }
            List<NamedExpression> newProjects = new ArrayList<>(project.getProjects());
            newProjects.add((NamedExpression) rowIdSlot.get());
            return project.withProjects(newProjects);
        }
    }

    private static boolean hasRowIdSlot(List<Slot> slots) {
        return findRowIdSlot(slots).isPresent();
    }

    private static Optional<Slot> findRowIdSlot(List<Slot> slots) {
        for (Slot slot : slots) {
            if (Column.ICEBERG_ROWID_COL.equalsIgnoreCase(slot.getName())) {
                return Optional.of(slot);
            }
        }
        return Optional.empty();
    }

    private static boolean hasRowIdProject(List<NamedExpression> projects) {
        for (NamedExpression project : projects) {
            if (project instanceof Slot
                    && Column.ICEBERG_ROWID_COL.equalsIgnoreCase(((Slot) project).getName())) {
                return true;
            }
        }
        return false;
    }

    private static Column getRowIdColumn(IcebergExternalTable table) {
        List<Column> fullSchema = table.getFullSchema();
        if (fullSchema != null) {
            for (Column column : fullSchema) {
                if (Column.ICEBERG_ROWID_COL.equalsIgnoreCase(column.getName())) {
                    return column;
                }
            }
        }
        return IcebergRowId.createHiddenColumn();
    }

    private NamedExpression getRowIdColumnExpr(LogicalPlan planWithRowId) {
        if (!hasUnboundPlan(planWithRowId)) {
            Optional<Slot> rowIdSlot = findRowIdSlot(planWithRowId.getOutput());
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

    private PhysicalSink<?> getPhysicalMergeSink(NereidsPlanner planner) {
        Optional<PhysicalSink<?>> plan = planner.getPhysicalPlan()
                .<PhysicalSink<?>>collect(PhysicalSink.class::isInstance).stream().findAny();
        if (!plan.isPresent()) {
            throw new AnalysisException("UPDATE command must contain target table");
        }
        PhysicalSink<?> sink = plan.get();
        if (!(sink instanceof PhysicalIcebergMergeSink)) {
            throw new AnalysisException("UPDATE merge plan must use Iceberg merge sink");
        }
        return sink;
    }

    private boolean childIsEmptyRelation(PhysicalSink<?> sink) {
        return sink.children() != null && sink.children().size() == 1
                && sink.child(0) instanceof PhysicalEmptyRelation;
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        List<String> qualifiedTableName = RelationUtil.getQualifierName(ctx, nameParts);
        TableIf table = RelationUtil.getTable(qualifiedTableName, ctx.getEnv(), Optional.empty());
        if (!(table instanceof IcebergExternalTable)) {
            throw new AnalysisException("Table must be IcebergExternalTable in UPDATE command");
        }
        boolean previousNeedIcebergRowId = ctx.needIcebergRowId();
        ctx.setNeedIcebergRowId(true);
        try {
            return buildMergePlan(ctx, logicalQuery, assignments, (IcebergExternalTable) table);
        } finally {
            ctx.setNeedIcebergRowId(previousNeedIcebergRowId);
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.UPDATE;
    }

    public DeleteCommandContext getDeleteCtx() {
        return deleteCtx;
    }
}
