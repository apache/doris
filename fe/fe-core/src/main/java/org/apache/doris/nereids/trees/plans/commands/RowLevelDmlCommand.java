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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.connector.spi.handle.ConnectorTransaction;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.PlannerHook;
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
import com.google.common.base.Throwables;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.OptionalLong;
import java.util.concurrent.Callable;

/**
 * Generic shell for row-level DML ({@code DELETE}/{@code UPDATE}/{@code MERGE INTO}) against external tables.
 *
 * <p>Owns the single live planner-drive loop that was triplicated across {@code ExternalRowLevelDeletePlanBuilder},
 * {@code ExternalRowLevelUpdatePlanBuilder} and {@code ExternalRowLevelMergePlanBuilder}: the per-operation
 * points (mode check, plan synthesis, required sink, executor factory, label prefix and finalize) are routed
 * through a {@link RowLevelDmlTransform} resolved from {@link RowLevelDmlRegistry}. The dispatching commands
 * ({@code UpdateCommand}/{@code DeleteFromCommand}/{@code MergeIntoCommand}) delegate here once a transform is
 * found, so the reverse {@code instanceof} dispatch is consolidated into the registry.</p>
 *
 * <p>This is intentionally a plain class, not a Nereids {@code Command}: it is invoked from within the
 * dispatching commands' {@code run}/{@code getExplainPlan}, so it needs no visitor/plan-type/stmt-type of its
 * own (those stay on the dispatching commands, preserving their per-op differences).</p>
 */
public class RowLevelDmlCommand {

    private static final Logger LOG = LogManager.getLogger(RowLevelDmlCommand.class);

    private final RowLevelDmlTransform transform;
    private final RowLevelDmlArgs args;
    private final RowLevelDmlOp op;

    public RowLevelDmlCommand(RowLevelDmlTransform transform, RowLevelDmlArgs args, RowLevelDmlOp op) {
        this.transform = transform;
        this.args = args;
        this.op = op;
    }

    /**
     * Execute the row-level DML. Mirrors the per-op plan builders
     * ({@code ExternalRowLevelDeletePlanBuilder} / {@code ExternalRowLevelUpdatePlanBuilder} /
     * {@code ExternalRowLevelMergePlanBuilder}) step-for-step;
     * the four divergences (required sink, label prefix, executor + finalize, result) are parameterized by op.
     */
    public void run(ConnectContext ctx, StmtExecutor stmtExecutor) throws Exception {
        TableIf table = args.getTable();
        transform.checkMode(table, op);
        ConnectorWriteSchemaUtils.pinAndGet(ctx, (ExternalTable) table);
        long previousTargetTableId = ctx.getSyntheticWriteColTargetTableId();
        ctx.setSyntheticWriteColTargetTableId(table.getId());
        try {
            LogicalPlan plan = transform.synthesize(ctx, args, op);
            Callable<Void> execute = () -> {
                LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(plan, ctx.getStatementContext());
                NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
                if (op == RowLevelDmlOp.DELETE) {
                    captureAnalyzedPlan(ctx);
                }
                planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());
                stmtExecutor.setPlanner(planner);
                stmtExecutor.checkBlockRules();

                // Whole-file metadata delete (Spark/StarRocks-style): when the connector can satisfy the DELETE
                // by dropping whole data files, it commits that remote metadata operation here and the
                // row-level scan/sink never runs. Skipped for a PARTITION(...) scope, whose row restriction is
                // carried by the scan rather than by the statement predicate the connector receives.
                if (op == RowLevelDmlOp.DELETE && args.getPartitions().isEmpty() && !args.isTempPart()) {
                    // The ordinary row-level path replays this retained cancellation when the executor publishes
                    // its coordinator; this branch has no coordinator, so do it explicitly before touching the
                    // remote table.
                    throwIfCancelled(stmtExecutor);
                    OptionalLong metadataDeletedRows =
                            transform.deleteUsingMetadata(ctx, args, planner.getAnalyzedPlan(),
                                    () -> stmtExecutor.getPendingCancelReason() != null);
                    if (metadataDeletedRows.isPresent()) {
                        reportMetadataDelete(ctx, table, metadataDeletedRows.getAsLong());
                        return null;
                    }
                    // The connector aborts before committing when the query was cancelled; surface the original
                    // cancellation instead of falling back to a row-level write.
                    throwIfCancelled(stmtExecutor);
                }

                PhysicalSink<?> physicalSink = transform.requirePhysicalSink(planner, op);
                PlanFragment fragment = planner.getFragments().get(0);
                DataSink dataSink = fragment.getSink();
                boolean emptyInsert = childIsEmptyRelation(physicalSink);
                String label = String.format(transform.labelPrefix(args.getTable(), op) + "_%x_%x",
                        ctx.queryId().hi, ctx.queryId().lo);

                BaseExternalTableInsertExecutor insertExecutor =
                        transform.newExecutor(ctx, table, label, planner, emptyInsert, op);

                if (insertExecutor.isEmptyInsert()) {
                    return null;
                }

                beginTransactionAndFinalizeSink(transform, op, insertExecutor, stmtExecutor,
                        planner.getAnalyzedPlan(), table, fragment, dataSink, physicalSink);
                insertExecutor.executeSingleInsert(stmtExecutor);
                return null;
            };
            if (transform.requiresExternalTableBatchModeDisabled()) {
                executeWithExternalTableBatchModeDisabled(ctx, execute);
            } else {
                execute.call();
            }
        } finally {
            ctx.setSyntheticWriteColTargetTableId(previousTargetTableId);
        }
    }

    /** EXPLAIN path: synthesis only (no planner-drive loop, no transaction), mirroring legacy getExplainPlan. */
    public Plan getExplainPlan(ConnectContext ctx) {
        TableIf table = args.getTable();
        transform.checkMode(table, op);
        ConnectorWriteSchemaUtils.pinAndGet(ctx, (ExternalTable) table);
        long previousTargetTableId = ctx.getSyntheticWriteColTargetTableId();
        ctx.setSyntheticWriteColTargetTableId(table.getId());
        try {
            return transform.synthesize(ctx, args, op);
        } finally {
            ctx.setSyntheticWriteColTargetTableId(previousTargetTableId);
        }
    }

    /**
     * The begin→finalize window, guarded like {@code InsertIntoTableCommand}'s prepare step:
     * {@code beginTransaction} registers the transaction with the connector transaction manager and the
     * global external-transaction registry, but the executor's own failure handling only takes over at
     * {@code executeSingleInsert} — a throw from the constraint/finalize/coordinator steps in between
     * would otherwise leak both registrations until FE restart.
     */
    @VisibleForTesting
    static void beginTransactionAndFinalizeSink(RowLevelDmlTransform transform, RowLevelDmlOp op,
            BaseExternalTableInsertExecutor insertExecutor, StmtExecutor stmtExecutor, Plan analyzedPlan,
            TableIf table, PlanFragment fragment, DataSink dataSink, PhysicalSink<?> physicalSink) {
        try {
            insertExecutor.beginTransaction();
            applyWriteConstraintIfPresent(transform, insertExecutor, analyzedPlan, table);
            transform.finalizeSink(insertExecutor, op, fragment, dataSink, physicalSink);
            insertExecutor.getCoordinator().setTxnId(insertExecutor.getTxnId());
        } catch (Throwable e) {
            // the abortTxn in onFail need to acquire table write lock
            insertExecutor.onFail(e);
            Throwables.throwIfInstanceOf(e, RuntimeException.class);
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /** Applies the connector-neutral optimistic write constraint when the transaction supports it. */
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
     * Runs {@code action} with external-table batch mode disabled for row-change representations that require
     * every source split to be available while the write is planned.
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

    /** Replays a cancellation retained by the executor before the statement may mutate the remote table. */
    @VisibleForTesting
    static void throwIfCancelled(StmtExecutor stmtExecutor) throws UserException {
        Status cancelReason = stmtExecutor.getPendingCancelReason();
        if (cancelReason != null) {
            throw new UserException(cancelReason.getErrorMsg());
        }
    }

    /**
     * Registers the analyzed-plan capture the whole-file DELETE fast path consumes.
     *
     * <p>{@link NereidsPlanner} only retains {@code analyzedPlan} for {@code EXPLAIN ANALYZED}/{@code ALL} or when
     * a {@link PlannerHook} records it; a plain DELETE leaves {@code getAnalyzedPlan()} null, which would make
     * {@code extractCompletePredicate} return empty and silently disable the optimization. The hook runs right
     * after analysis and before rewrite, so the captured plan still carries the bound statement predicate rather
     * than a rewrite-pruned one — the same capture the lineage extractor installs.</p>
     */
    @VisibleForTesting
    static void captureAnalyzedPlan(ConnectContext ctx) {
        ctx.getStatementContext().addPlannerHook(new PlannerHook() {
            @Override
            public void afterAnalyze(NereidsPlanner planner) {
                if (planner.getCascadesContext() != null
                        && planner.getCascadesContext().getRewritePlan() != null) {
                    planner.setAnalyzedPlan(planner.getCascadesContext().getRewritePlan());
                }
            }
        });
    }

    /**
     * Publishes the result of a whole-file metadata delete. The connector has already committed the remote
     * delete as one atomic metadata operation, so no coordinator or connector transaction is involved here:
     * mirror the surfaces the row-level path's {@code afterExec} feeds — {@link org.apache.doris.qe.QueryState}
     * for the client's "Query OK, N rows affected" and the fe.audit.log return-rows counter — plus its
     * post-commit refresh, which a remote commit needs so later queries do not read a stale cached snapshot.
     */
    private static void reportMetadataDelete(ConnectContext ctx, TableIf table, long deletedRows) {
        ctx.getState().setOk(deletedRows, 0, "{'status':'COMMITTED'}");
        ctx.updateReturnRows((int) deletedRows);
        try {
            Env.getCurrentEnv().getRefreshManager().refreshTableAfterExternalMutation((ExternalTable) table);
        } catch (Exception e) {
            // The remote delete is already durable: a refresh failure must not make the client retry it.
            LOG.warn("Post-commit refresh failed for table {}. Data was committed successfully.",
                    table.getName(), e);
        }
    }
}
