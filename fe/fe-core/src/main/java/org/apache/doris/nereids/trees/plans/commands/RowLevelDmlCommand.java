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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.insert.BaseExternalTableInsertExecutor;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.annotations.VisibleForTesting;

import java.util.concurrent.Callable;

/**
 * Generic shell for row-level DML ({@code DELETE}/{@code UPDATE}/{@code MERGE INTO}) against external tables.
 *
 * <p>Owns the single live planner-drive loop that was triplicated across {@code IcebergDeleteCommand},
 * {@code IcebergUpdateCommand} and {@code IcebergMergeCommand}: the per-operation points (mode check, plan
 * synthesis, required sink, executor factory, label prefix, conflict-detection wiring, finalize) are routed
 * through a {@link RowLevelDmlTransform} resolved from {@link RowLevelDmlRegistry}. The dispatching commands
 * ({@code UpdateCommand}/{@code DeleteFromCommand}/{@code MergeIntoCommand}) delegate here once a transform is
 * found, so the reverse {@code instanceof} dispatch is consolidated into the registry.</p>
 *
 * <p>This is intentionally a plain class, not a Nereids {@code Command}: it is invoked from within the
 * dispatching commands' {@code run}/{@code getExplainPlan}, so it needs no visitor/plan-type/stmt-type of its
 * own (those stay on the dispatching commands, preserving their per-op differences).</p>
 */
public class RowLevelDmlCommand {

    private final RowLevelDmlTransform transform;
    private final RowLevelDmlArgs args;
    private final RowLevelDmlOp op;

    public RowLevelDmlCommand(RowLevelDmlTransform transform, RowLevelDmlArgs args, RowLevelDmlOp op) {
        this.transform = transform;
        this.args = args;
        this.op = op;
    }

    /**
     * Execute the row-level DML. Mirrors legacy {@code IcebergDeleteCommand.run} /
     * {@code IcebergUpdateCommand.executeMergePlan} / {@code IcebergMergeCommand.executeMergePlan} step-for-step;
     * the four divergences (required sink, label prefix, executor + finalize, result) are parameterized by op.
     */
    public void run(ConnectContext ctx, StmtExecutor stmtExecutor) throws Exception {
        TableIf table = args.getTable();
        transform.checkMode(table, op);
        long previousTargetTableId = ctx.getIcebergRowIdTargetTableId();
        ctx.setIcebergRowIdTargetTableId(table.getId());
        try {
            LogicalPlan plan = transform.synthesize(ctx, args, op);
            executeWithExternalTableBatchModeDisabled(ctx, () -> {
                LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(plan, ctx.getStatementContext());
                NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
                planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());
                stmtExecutor.setPlanner(planner);
                stmtExecutor.checkBlockRules();

                PhysicalSink<?> physicalSink = transform.requirePhysicalSink(planner, op);
                PlanFragment fragment = planner.getFragments().get(0);
                DataSink dataSink = fragment.getSink();
                boolean emptyInsert = childIsEmptyRelation(physicalSink);
                String label = String.format(transform.labelPrefix(op) + "_%x_%x",
                        ctx.queryId().hi, ctx.queryId().lo);

                BaseExternalTableInsertExecutor insertExecutor =
                        transform.newExecutor(ctx, table, label, planner, emptyInsert, op);
                transform.setupConflictDetection(insertExecutor, planner.getAnalyzedPlan(), table, op);

                if (insertExecutor.isEmptyInsert()) {
                    return null;
                }

                insertExecutor.beginTransaction();
                applyWriteConstraintIfPresent(transform, insertExecutor, planner.getAnalyzedPlan(), table);
                transform.finalizeSink(insertExecutor, op, fragment, dataSink, physicalSink);
                insertExecutor.getCoordinator().setTxnId(insertExecutor.getTxnId());
                stmtExecutor.setCoord(insertExecutor.getCoordinator());
                insertExecutor.executeSingleInsert(stmtExecutor);
                return null;
            });
        } finally {
            ctx.setIcebergRowIdTargetTableId(previousTargetTableId);
        }
    }

    /** EXPLAIN path: synthesis only (no planner-drive loop, no transaction), mirroring legacy getExplainPlan. */
    public Plan getExplainPlan(ConnectContext ctx) {
        TableIf table = args.getTable();
        transform.checkMode(table, op);
        long previousTargetTableId = ctx.getIcebergRowIdTargetTableId();
        ctx.setIcebergRowIdTargetTableId(table.getId());
        try {
            return transform.synthesize(ctx, args, op);
        } finally {
            ctx.setIcebergRowIdTargetTableId(previousTargetTableId);
        }
    }

    /**
     * O5-2 new write-constraint path. Dormant until P6.6: only fires when the executor exposes an SPI
     * {@link ConnectorTransaction}. Today iceberg DELETE/MERGE run on the legacy {@code IcebergTransaction}
     * (the base {@code getConnectorTransactionOrNull()} returns {@code null}), so this is a no-op; the legacy
     * 3-hop conflict-detection path ({@link RowLevelDmlTransform#setupConflictDetection}) remains the live one.
     */
    @VisibleForTesting
    static void applyWriteConstraintIfPresent(RowLevelDmlTransform transform,
            BaseExternalTableInsertExecutor executor, Plan analyzedPlan, TableIf table) {
        ConnectorTransaction connectorTx = executor.getConnectorTransactionOrNull();
        if (connectorTx == null) {
            return;
        }
        transform.extractWriteConstraint(analyzedPlan, table).ifPresent(connectorTx::applyWriteConstraint);
    }

    /**
     * Run {@code action} with external-table batch mode disabled so the iceberg scan node yields all splits
     * (needed by {@code IcebergRewritableDeletePlanner.collect}). Byte-identical to the per-command copies
     * retained on the legacy {@code Iceberg*Command} classes until P6.7.
     */
    static <T> T executeWithExternalTableBatchModeDisabled(ConnectContext ctx, Callable<T> action) throws Exception {
        boolean previousEnableExternalTableBatchMode = ctx.getSessionVariable().enableExternalTableBatchMode;
        ctx.getSessionVariable().enableExternalTableBatchMode = false;
        try {
            return action.call();
        } finally {
            ctx.getSessionVariable().enableExternalTableBatchMode = previousEnableExternalTableBatchMode;
        }
    }

    private static boolean childIsEmptyRelation(PhysicalSink<?> sink) {
        return sink.children() != null && sink.children().size() == 1
                && sink.child(0) instanceof PhysicalEmptyRelation;
    }
}
