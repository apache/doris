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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.delete.DeleteCommandContext;
import org.apache.doris.nereids.trees.plans.commands.insert.IcebergDeleteExecutor;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalIcebergDeleteSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIcebergDeleteSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * DELETE command for Iceberg tables.
 *
 * This command converts DELETE operations to INSERT operations that generate
 * position DeleteFile entries instead of data files.
 *
 * Example:
 *   DELETE FROM iceberg_table WHERE id = 1
 *
 * This will:
 *   1. Scan rows matching the WHERE condition
 *   2. Generate DeleteFile containing the matching rows
 *   3. Commit the DeleteFile to Iceberg table using RowDelta API
 */
public class IcebergDeleteCommand extends Command implements ForwardWithSync, Explainable {

    protected final List<String> nameParts;
    protected final String tableAlias;
    protected final boolean isTempPart;
    protected final List<String> partitions;
    protected final LogicalPlan logicalQuery;
    protected final DeleteCommandContext deleteCtx;

    /**
     * constructor
     */
    public IcebergDeleteCommand(
            List<String> nameParts,
            String tableAlias,
            boolean isTempPart,
            List<String> partitions,
            LogicalPlan logicalQuery,
            DeleteCommandContext deleteCtx) {
        super(PlanType.DELETE_COMMAND);
        this.nameParts = Utils.copyRequiredList(nameParts);
        this.tableAlias = tableAlias;
        this.isTempPart = isTempPart;
        this.partitions = Utils.copyRequiredList(partitions);
        this.logicalQuery = logicalQuery;
        this.deleteCtx = deleteCtx != null ? deleteCtx : new DeleteCommandContext();
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // Check if target table is Iceberg table
        List<String> qualifiedTableName = RelationUtil.getQualifierName(ctx, nameParts);
        TableIf table = RelationUtil.getTable(qualifiedTableName, ctx.getEnv(), Optional.empty());

        if (!(table instanceof IcebergExternalTable)) {
            throw new AnalysisException("DELETE command can only be used on Iceberg tables. "
                    + "Table " + Util.getTempTableDisplayName(table.getName()) + " is not an Iceberg table.");
        }

        IcebergExternalTable icebergTable = (IcebergExternalTable) table;

        // Verify table format version (must be v2+ for delete support)
        // org.apache.iceberg.Table icebergTableObj = icebergTable.getIcebergTable();
        // String formatVersionStr = icebergTableObj.properties().get("format-version");
        // int formatVersion = formatVersionStr != null ? Integer.parseInt(formatVersionStr) : 1;
        // if (formatVersion < 2) {
        //     throw new AnalysisException("Iceberg table DELETE requires format version >= 2. "
        //             + "Current format version: " + formatVersion);
        // }

        boolean previousNeedIcebergRowId = ctx.needIcebergRowId();
        ctx.setNeedIcebergRowId(true);
        try {
            // Build query plan with DELETE sink
            LogicalPlan deleteQueryPlan = completeQueryPlan(ctx, logicalQuery, icebergTable);

            // Create planner and plan the delete operation
            NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
            LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(deleteQueryPlan, ctx.getStatementContext());

            // Plan the delete query to generate physical plan and distributed plan
            planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());

            // Set planner in executor for later use
            executor.setPlanner(planner);
            executor.checkBlockRules();
            Optional<org.apache.iceberg.expressions.Expression> conflictFilter =
                    IcebergConflictDetectionFilterUtils.buildConflictDetectionFilter(
                            planner.getAnalyzedPlan(), icebergTable);

            PhysicalSink<?> physicalSink = getPhysicalSink(planner);
            PlanFragment fragment = planner.getFragments().get(0);
            DataSink dataSink = fragment.getSink();
            boolean emptyInsert = childIsEmptyRelation(physicalSink);
            String label = String.format("iceberg_delete_%x_%x", ctx.queryId().hi, ctx.queryId().lo);

            // Create IcebergDeleteExecutor and execute
            IcebergDeleteExecutor deleteExecutor = new IcebergDeleteExecutor(
                    ctx,
                    icebergTable,
                    label,
                    planner,
                    emptyInsert,
                    -1L);
            deleteExecutor.setConflictDetectionFilter(conflictFilter);

            if (deleteExecutor.isEmptyInsert()) {
                return;
            }

            deleteExecutor.beginTransaction();
            deleteExecutor.finalizeSinkForDelete(fragment, dataSink, physicalSink);
            deleteExecutor.getCoordinator().setTxnId(deleteExecutor.getTxnId());
            executor.setCoord(deleteExecutor.getCoordinator());
            deleteExecutor.executeSingleInsert(executor);
        } finally {
            ctx.setNeedIcebergRowId(previousNeedIcebergRowId);
        }
    }

    /**
     * Complete the query plan by adding necessary columns for position delete operation.
     * Select $row_id (file_path, row_position, partition info).
     */
    private LogicalPlan completeQueryPlan(ConnectContext ctx, LogicalPlan logicalQuery,
                                         IcebergExternalTable icebergTable) {
        LogicalPlan queryPlan = buildPositionDeletePlan(ctx, logicalQuery, icebergTable);

        // Convert output to NamedExpression list
        List<NamedExpression> outputExprs;
        if (!hasUnboundPlan(queryPlan)) {
            outputExprs = queryPlan.getOutput().stream()
                    .map(slot -> (NamedExpression) slot)
                    .collect(java.util.stream.Collectors.toList());
        } else if (queryPlan instanceof LogicalProject) {
            outputExprs = ((LogicalProject<?>) queryPlan).getProjects();
        } else {
            outputExprs = ImmutableList.of();
        }

        // Wrap query plan with LogicalIcebergDeleteSink
        LogicalIcebergDeleteSink<LogicalPlan> deleteSink = new LogicalIcebergDeleteSink<>(
                (IcebergExternalDatabase) icebergTable.getDatabase(),
                icebergTable,
                icebergTable.getBaseSchema(true),  // cols
                outputExprs,  // outputExprs
                deleteCtx,
                Optional.empty(),  // groupExpression
                Optional.empty(),  // logicalProperties
                queryPlan  // child
        );

        return deleteSink;
    }

    /**
     * Build query plan for position delete.
     * Add $row_id column to select list.
     *
     * This follows Trino's approach:
     * 1. Original query filters rows based on WHERE clause
     * 2. We project $row_id metadata column from matching rows
     * 3. The $row_id contains (file_path, row_position, partition_spec_id, partition_data)
     * 4. These will be written to Position Delete file
     */
    private LogicalPlan buildPositionDeletePlan(ConnectContext ctx, LogicalPlan logicalQuery,
                                                IcebergExternalTable icebergTable) {
        // Step 1: Inject $row_id metadata column into the scan
        // 这会触发 getFullSchema() 返回包含隐藏列的完整 Schema
        LogicalPlan planWithRowId = injectRowIdColumn(logicalQuery);

        // Step 2: Project operation + __DORIS_ICEBERG_ROWID_COL__
        // These are sent to the delete file writer and used for shuffle requirements
        Optional<Slot> rowIdSlot = Optional.empty();
        if (!hasUnboundPlan(planWithRowId)) {
            rowIdSlot = findRowIdSlot(planWithRowId.getOutput());
        }
        NamedExpression operationColumn = new UnboundAlias(
                new TinyIntLiteral(IcebergMergeOperation.DELETE_OPERATION_NUMBER),
                IcebergMergeOperation.OPERATION_COLUMN);
        NamedExpression rowIdColumn = rowIdSlot.isPresent()
                ? (NamedExpression) rowIdSlot.get()
                : new UnboundSlot(Column.ICEBERG_ROWID_COL);
        List<NamedExpression> projectItems = ImmutableList.of(operationColumn, rowIdColumn);

        return new LogicalProject<>(projectItems, planWithRowId);
    }

    private PhysicalSink<?> getPhysicalSink(NereidsPlanner planner) {
        Optional<PhysicalSink<?>> plan = planner.getPhysicalPlan()
                .<PhysicalSink<?>>collect(PhysicalSink.class::isInstance).stream().findAny();
        if (!plan.isPresent()) {
            throw new AnalysisException("DELETE command must contain target table");
        }
        PhysicalSink<?> sink = plan.get();
        if (!(sink instanceof PhysicalIcebergDeleteSink)) {
            throw new AnalysisException("DELETE plan must use Iceberg delete sink");
        }
        return sink;
    }

    private boolean childIsEmptyRelation(PhysicalSink<?> sink) {
        return sink.children() != null && sink.children().size() == 1
                && sink.child(0) instanceof PhysicalEmptyRelation;
    }

    /**
     * Inject $row_id metadata column into the logical plan.
     *
     * This method traverses the plan tree and marks scans to include the $row_id column.
     * The actual column generation happens in BE during execution.
     *
     * Similar to Trino's IcebergPageSourceProvider which injects row position
     * during page source creation.
     */
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

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        List<String> qualifiedTableName = RelationUtil.getQualifierName(ctx, nameParts);
        TableIf table = RelationUtil.getTable(qualifiedTableName, ctx.getEnv(), Optional.empty());
        if (!(table instanceof IcebergExternalTable)) {
            throw new AnalysisException("Table must be IcebergExternalTable in DELETE command");
        }
        boolean previousNeedIcebergRowId = ctx.needIcebergRowId();
        ctx.setNeedIcebergRowId(true);
        try {
            return completeQueryPlan(ctx, logicalQuery, (IcebergExternalTable) table);
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
        return StmtType.DELETE;
    }

    public DeleteCommandContext getDeleteCtx() {
        return deleteCtx;
    }
}
