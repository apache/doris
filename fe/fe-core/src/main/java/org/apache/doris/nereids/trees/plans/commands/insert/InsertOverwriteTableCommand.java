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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.InternalDatabaseUtil;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.datasource.doris.RemoteDorisExternalTable;
import org.apache.doris.datasource.doris.RemoteOlapTable;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.insertoverwrite.AbstractInsertOverwriteManager;
import org.apache.doris.insertoverwrite.InsertOverwriteUtil;
import org.apache.doris.insertoverwrite.RemoteInsertOverwriteManager;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundConnectorTableSink;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.analyzer.UnboundTableSinkCreator;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.lineage.LineageInfoExtractor;
import org.apache.doris.nereids.lineage.LineageUtils;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.TVFRelation;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.ForwardWithSync;
import org.apache.doris.nereids.trees.plans.commands.NeedAuditEncryption;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.UnboundLogicalSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTableSink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * insert into select command implementation
 * insert into select command support the grammer: explain? insert into table columns? partitions? hints? query
 * InsertIntoTableCommand is a command to represent insert the answer of a query into a table.
 * class structure's:
 * InsertIntoTableCommand(Query())
 * ExplainCommand(Query())
 */
public class InsertOverwriteTableCommand extends Command
        implements NeedAuditEncryption, ForwardWithSync, Explainable, CancelableCommand {

    /**
     * Fails an overwrite in the one window a refresh cannot recover from by itself: after the rows have been
     * committed into the temporary partitions and before the swap publishes them. Everything the write read
     * is committed by then -- the base table streams it consumed, among them -- and the partitions it was
     * going to replace still hold what they had, so a refresh that dies here leaves rows missing and nothing
     * durable saying so unless it raised a rebuild requirement before it read. See
     * test_ivm_overwrite_failure_between_the_halves, which pins the recovery.
     *
     * <p>Scoped by the MV name the point carries as its {@code mv_name} parameter: the read below answers
     * with the default when the point is not enabled or carries no such parameter, and no MV is named by an
     * empty string, so enabling it cannot disturb an overwrite that is not the one under test.
     */
    public static final String DEBUG_POINT_FAIL_BETWEEN_THE_HALVES_OF_AN_OVERWRITE =
            "InsertOverwriteTableCommand.failBetweenTheTwoHalvesOfAnOverwrite";

    /**
     * Cancels the overwrite before it has committed anything, so that the half of a cancellation's meaning
     * that takes the statement back can be pinned by a test: nothing durable happened, so the statement has
     * to fail rather than report the success of an overwrite that did not run. See test_insert_overwrite_cancel.
     *
     * <p>Its {@code table_name} parameter names the one table the point may disturb: the read below answers
     * with the default when the point is not enabled or carries no such parameter, and no table is named by an
     * empty string, so an enabled point cannot disturb an overwrite that is not the one under test.
     */
    public static final String DEBUG_POINT_CANCEL_BEFORE_THE_INSERT_OF_AN_OVERWRITE =
            "InsertOverwriteTableCommand.cancelBeforeTheInsertOfAnOverwrite";

    /**
     * Cancels the overwrite in the window between its two halves -- after the insert, before the swap -- so
     * that the other half of a cancellation's meaning can be pinned: where the rows are durable, the swap runs
     * and the statement reports the success it is; where the insert committed nothing, the cancellation still
     * has everything to take back. See test_insert_overwrite_cancel.
     *
     * <p>Separate from the point above rather than one point with a stage parameter: a point is consumed by
     * the first lookup that reads it (see {@code DebugPointUtil#getDebugPoint}), so a shared name would let
     * the check at one site spend the other site's allowance and make an armed point silently not fire.
     */
    public static final String DEBUG_POINT_CANCEL_BETWEEN_THE_HALVES_OF_AN_OVERWRITE =
            "InsertOverwriteTableCommand.cancelBetweenTheTwoHalvesOfAnOverwrite";

    /**
     * Cancels the overwrite while the swap holds the target table's write lock, which is the window a
     * cancellation can reach only after the check that reads the flag before the swap was issued: the swap
     * waits for that lock, and the wait can be as long as whoever holds it. A cancellation with nothing
     * committed is still honoured there, because there is nothing durable to publish and refusing costs the
     * statement and nothing else. See test_insert_overwrite_cancel.
     */
    public static final String DEBUG_POINT_CANCEL_WHILE_THE_SWAP_WAITS_FOR_THE_TABLE_LOCK =
            "InsertOverwriteTableCommand.cancelWhileTheSwapWaitsForTheTableLock";

    /**
     * The swap that publishes an overwrite: replacing the temp partitions for an explicit-partition
     * overwrite, or making a task group's replacements visible for an auto-detect one. Both run through
     * {@link #publishTheOverwrite}, which holds the target table's write lock and takes the last look at the
     * cancellation flag before letting one run.
     */
    @FunctionalInterface
    private interface OverwritePublication {
        void publish() throws UserException;
    }

    private static final Logger LOG = LogManager.getLogger(InsertOverwriteTableCommand.class);

    private LogicalPlan originLogicalQuery;
    private Optional<LogicalPlan> logicalQuery;
    private Optional<String> labelName;
    private final Optional<LogicalPlan> cte;
    private AtomicBoolean isCancelled = new AtomicBoolean(false);
    private AtomicBoolean isRunning = new AtomicBoolean(false);
    private Optional<String> branchName;
    private Optional<Plan> lineagePlan = Optional.empty();

    /**
     * constructor
     */
    public InsertOverwriteTableCommand(LogicalPlan logicalQuery, Optional<String> labelName,
            Optional<LogicalPlan> cte, Optional<String> branchName) {
        super(PlanType.INSERT_INTO_TABLE_COMMAND);
        this.originLogicalQuery = Objects.requireNonNull(logicalQuery, "logicalQuery should not be null");
        this.logicalQuery = Optional.empty();
        this.labelName = Objects.requireNonNull(labelName, "labelName should not be null");
        this.cte = cte;
        this.branchName = branchName;
    }

    public void setLabelName(Optional<String> labelName) {
        this.labelName = labelName;
    }

    public boolean isAutoDetectOverwrite(LogicalPlan logicalQuery) {
        return (logicalQuery instanceof UnboundTableSink)
                && ((UnboundTableSink<?>) logicalQuery).isAutoDetectPartition();
    }

    public LogicalPlan getLogicalQuery() {
        return logicalQuery.orElse(originLogicalQuery);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        TableIf targetTableIf = InsertUtils.getTargetTable(originLogicalQuery, ctx);
        // check allow insert overwrite
        if (!allowInsertOverwrite(targetTableIf)) {
            String errMsg = "insert into overwrite only support OLAP/Remote OLAP table and external"
                    + " tables (HMS/Iceberg, or a plugin-driven connector that supports overwrite)."
                    + " But current table type is " + targetTableIf.getType();
            LOG.error(errMsg);
            throw new AnalysisException(errMsg);
        }
        //check allow modify MTMVData
        if (targetTableIf instanceof MTMV && !MTMVUtil.allowModifyMTMVData(ctx)) {
            throw new AnalysisException("Not allowed to perform current operation on async materialized view");
        }
        // Check the branch capability before resolving the branch-specific writer schema. Otherwise,
        // an unsupported connector can fail while resolving a branch instead of reporting the
        // INSERT OVERWRITE capability error.
        if (branchName.isPresent() && !pluginConnectorSupportsWriteBranch(targetTableIf)) {
            throw new AnalysisException(
                    "Only support insert overwrite into iceberg table's branch");
        }
        ctx.getStatementContext().setIsInsert(true);
        Optional<CascadesContext> analyzeContext = Optional.of(
                CascadesContext.initContext(ctx.getStatementContext(), originLogicalQuery, PhysicalProperties.ANY)
        );
        InsertUtils.pinConnectorWriteSchema(
                ctx.getStatementContext(), targetTableIf, originLogicalQuery, branchName);
        this.logicalQuery = Optional.of((LogicalPlan) InsertUtils.normalizePlan(
            originLogicalQuery, (targetTableIf instanceof RemoteDorisExternalTable)
                        ? ((RemoteDorisExternalTable) targetTableIf).getOlapTable() : targetTableIf,
                analyzeContext, Optional.empty()));
        if (cte.isPresent()) {
            LogicalPlan logicalQuery = this.logicalQuery.get();
            this.logicalQuery = Optional.of(
                    (LogicalPlan) logicalQuery.withChildren(
                            cte.get().withChildren(logicalQuery.child(0))
                    )
            );
        }
        LogicalPlan logicalQuery = this.logicalQuery.get();
        LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(logicalQuery, ctx.getStatementContext());
        NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
        LineageInfoExtractor.registerAnalyzePlanHook(ctx.getStatementContext(), planner);
        planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());
        // This plan only locates the sink and the partitions; the insert below plans again and runs
        // that plan. No coordinator ever takes this one, so what its scan nodes opened for the
        // backend while planning (a remote Doris scan's Flight SQL session on the other frontend, a
        // batch split source) is released here, before the real insert opens its own.
        for (ScanNode scanNode : planner.getScanNodes()) {
            scanNode.stop();
        }
        Plan analyzedPlan = planner.getAnalyzedPlan();
        lineagePlan = Optional.ofNullable(analyzedPlan);
        executor.checkBlockRules();

        Optional<TreeNode<?>> plan = (planner.getPhysicalPlan()
                .<TreeNode<?>>collect(node -> node instanceof PhysicalTableSink)).stream().findAny();
        Preconditions.checkArgument(plan.isPresent(), "insert into command must contain OlapTableSinkNode");
        PhysicalTableSink<?> physicalTableSink = ((PhysicalTableSink<?>) plan.get());
        TableIf targetTable = physicalTableSink.getTargetTable();
        List<String> partitionNames;
        boolean wholeTable = false;
        if (physicalTableSink instanceof PhysicalOlapTableSink) {
            if (targetTable instanceof OlapTable) {
                InternalDatabaseUtil
                        .checkDatabase(((OlapTable) targetTable).getQualifiedDbName(), ConnectContext.get());
                // check auth
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkTblPriv(ConnectContext.get(), targetTable.getDatabase().getCatalog().getName(),
                                ((OlapTable) targetTable).getQualifiedDbName(),
                                targetTable.getName(), PrivPredicate.LOAD)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                            ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                            ((OlapTable) targetTable).getQualifiedDbName() + ": " + targetTable.getName());
                }
            }
            partitionNames = ((UnboundTableSink<?>) logicalQuery).getPartitions();
            // If not specific partition to overwrite, means it's a command to overwrite the table.
            // not we execute as overwrite every partitions.
            if (CollectionUtils.isEmpty(partitionNames)) {
                wholeTable = true;
                try { // avoid concurrent modification exception when get partition names
                    targetTable.readLock();
                    partitionNames = Lists.newArrayList(targetTable.getPartitionNames());
                } finally {
                    targetTable.readUnlock();
                }
            }
        } else {
            // Do not create temp partition on FE
            partitionNames = new ArrayList<>();
        }

        AbstractInsertOverwriteManager insertOverwriteManager = (targetTable instanceof RemoteOlapTable)
                ? new RemoteInsertOverwriteManager(((RemoteOlapTable) targetTable).getCatalog())
                : Env.getCurrentEnv().getInsertOverwriteManager();
        insertOverwriteManager.recordRunningTableOrException(targetTable.getDatabase(), targetTable);
        isRunning.set(true);
        long taskId = 0;
        try {
            // OLAP overwrite runs its internal partition replacement with the auth check skipped.
            // Set the flag here, inside the try, so the finally below always pairs the reset even if
            // an earlier step (e.g. the @branch guard) throws before we get here.
            if (physicalTableSink instanceof PhysicalOlapTableSink && targetTable instanceof OlapTable) {
                ctx.setSkipAuth(true);
            }
            if (isAutoDetectOverwrite(getLogicalQuery())) {
                // taskId here is a group id. it contains all replace tasks made and registered in rpc process.
                final long groupId = insertOverwriteManager.registerTaskGroup(targetTable);
                taskId = groupId;
                // When inserting, BE will call to replace partition by FrontendService. FE will register new temp
                // partitions and return. for transactional, the replacement will really occur when insert successed,
                // i.e. `insertInto` finished. then we call taskGroupSuccess to make replacement.
                InsertCommandContext insertCtx = insertIntoAutoDetect(ctx, executor, groupId);
                if (isCancelled.get() && !insertCtx.hasCommitted()) {
                    // The load committed nothing -- an empty plan takes the path that begins no transaction --
                    // so the cancellation still has everything to take back: the catch drops the group's temp
                    // partitions (an empty plan registers none), and the statement fails rather than reporting
                    // a replacement the client cancelled. A cancellation landing after this check, while the
                    // swap waits for the table lock, is taken up again by publishTheOverwrite below.
                    throw cancelledBeforeTheRowsWereCommitted("after a load that committed nothing", ctx);
                }
                publishTheOverwrite(targetTable, insertCtx, ctx,
                        () -> insertOverwriteManager.taskGroupSuccess(groupId, (OlapTable) targetTable,
                                isForceDropPartition()));
            } else {
                // it's overwrite table(as all partitions) or specific partition(s)
                List<String> tempPartitionNames = InsertOverwriteUtil.generateTempPartitionNames(partitionNames);
                cancelTheOverwriteAt(DEBUG_POINT_CANCEL_BEFORE_THE_INSERT_OF_AN_OVERWRITE, targetTable);
                if (isCancelled.get()) {
                    // Nothing durable happened: no task is registered, no temp partition exists, no row was
                    // written and nothing was committed. The statement is a plain failure, like the one the
                    // inner insert reports when it is cancelled, rather than the success of an overwrite that
                    // did not run.
                    throw cancelledBeforeTheRowsWereCommitted("before registerTask", ctx);
                }
                taskId = insertOverwriteManager.registerTask(targetTable, tempPartitionNames);
                if (isCancelled.get()) {
                    // The catch below takes the registration back; no temp partition exists yet, so there is
                    // nothing else to drop.
                    throw cancelledBeforeTheRowsWereCommitted("before addTempPartitions", ctx);
                }
                InsertOverwriteUtil.addTempPartitions(targetTable, partitionNames, tempPartitionNames);
                if (isCancelled.get()) {
                    // The catch below drops the temp partitions this cancelled statement created.
                    throw cancelledBeforeTheRowsWereCommitted("before insertInto", ctx);
                }
                // todo: need to refresh remote target table after add temp partitions
                InsertCommandContext insertCtx = insertIntoPartitions(ctx, executor, tempPartitionNames, wholeTable);
                cancelTheOverwriteAt(DEBUG_POINT_CANCEL_BETWEEN_THE_HALVES_OF_AN_OVERWRITE, targetTable);
                if (isCancelled.get()) {
                    if (!insertCtx.hasCommitted()) {
                        // The insert committed nothing: its plan folded to an empty relation, so it took the
                        // path that begins no transaction, and this window holds no durable work at all.
                        // Completing the swap would publish an empty table for a statement the client
                        // cancelled; the catch drops the empty temp partitions instead and the statement
                        // fails, which is the same boundary the cancellations above sit on.
                        throw cancelledBeforeTheRowsWereCommitted("after an insert that committed nothing", ctx);
                    }
                    // Too late to cancel: insertIntoPartitions returns only once its transaction has committed
                    // the rows into the temp partitions -- visible, or still waiting for a publication that
                    // timed out -- and everything the read consumed, the base table stream offsets among it,
                    // was committed with that same transaction. Dropping the temp partitions here is exactly
                    // what would lose those rows against an advanced offset, while the swap below is what
                    // publishes them. The overwrite completes, and it is the outcome the statement reports.
                    LOG.info("insert overwrite is cancelled after its rows were committed, completing it,"
                            + " queryId: {}", ctx.getQueryIdentifier());
                }
                failBetweenTheTwoHalvesOfAnOverwrite(targetTable);
                // The publication below is a lambda, so the partitions it replaces need a name that is final:
                // partitionNames is assigned on more than one path above.
                final List<String> replacedPartitionNames = partitionNames;
                publishTheOverwrite(targetTable, insertCtx, ctx,
                        () -> InsertOverwriteUtil.replacePartition(targetTable, replacedPartitionNames,
                                tempPartitionNames, isForceDropPartition()));
                if (isCancelled.get()) {
                    LOG.info("insert overwrite is cancelled before taskSuccess, do nothing, queryId: {}",
                            ctx.getQueryIdentifier());
                }
                insertOverwriteManager.taskSuccess(taskId);
            }
        } catch (Exception e) {
            LOG.warn("insert into overwrite failed with task(or group) id {}", taskId, e);
            // A cancel that landed before registerTask leaves nothing registered to fail, and no id was taken.
            if (isAutoDetectOverwrite(getLogicalQuery()) && taskId != 0) {
                insertOverwriteManager.taskGroupFail(taskId);
            } else if (taskId != 0) {
                insertOverwriteManager.taskFail(taskId);
            }
            throw e;
        } finally {
            ConnectContext.get().setSkipAuth(false);
            insertOverwriteManager.dropRunningRecord(targetTable.getDatabase(), targetTable);
            isRunning.set(false);
        }
        LineageUtils.submitLineageEventIfNeeded(executor, lineagePlan, getLogicalQuery(), getClass());
    }

    /**
     * cancel insert overwrite
     */
    public void cancel() {
        this.isCancelled.set(true);
    }

    /**
     * wait insert overwrite not running
     */
    public void waitNotRunning() {
        long waitMaxTimeSecond = 10L;
        try {
            Awaitility.await().atMost(waitMaxTimeSecond, TimeUnit.SECONDS).untilFalse(isRunning);
        } catch (Exception e) {
            LOG.warn("waiting time exceeds {} second, stop wait, labelName: {}", waitMaxTimeSecond,
                    labelName.isPresent() ? labelName.get() : "", e);
        }
    }

    private boolean allowInsertOverwrite(TableIf targetTable) {
        if (targetTable instanceof OlapTable || targetTable instanceof RemoteDorisExternalTable) {
            return true;
        } else {
            return targetTable instanceof PluginDrivenExternalTable
                    && pluginConnectorSupportsInsertOverwrite((PluginDrivenExternalTable) targetTable);
        }
    }

    /**
     * A plugin-driven (SPI connector) table supports INSERT OVERWRITE only if its connector
     * declares the capability. Connectors that support plain INSERT but not overwrite (e.g. jdbc)
     * must be rejected here so the command fails loud, rather than reaching the sink and silently
     * degrading OVERWRITE to a plain append. Mirrors the connector-access pattern in
     * {@code PhysicalPlanTranslator}.
     */
    private static boolean pluginConnectorSupportsInsertOverwrite(PluginDrivenExternalTable table) {
        // Per-handle write-op probe (a heterogeneous gateway answers per-table; OVERWRITE happens to be admitted
        // by both hive and iceberg, but the probe is resolved uniformly with the other write-op admission gates).
        return table.connectorSupportedWriteOperations().contains(WriteOperation.OVERWRITE);
    }

    /**
     * A plugin-driven (SPI connector) table accepts an {@code INSERT OVERWRITE t@branch(name)} only if
     * its connector declares {@code supportsWriteBranch()}. Connectors with no branch concept must be
     * rejected here (fail loud) instead of reaching the generic sink, which would silently drop the
     * branch and overwrite the table's default ref. Mirrors {@code pluginConnectorSupportsInsertOverwrite}.
     */
    private static boolean pluginConnectorSupportsWriteBranch(TableIf targetTable) {
        if (!(targetTable instanceof PluginDrivenExternalTable)) {
            return false;
        }
        // Per-handle: a heterogeneous gateway supports write-to-branch for its iceberg tables but not its hive.
        return ((PluginDrivenExternalTable) targetTable).connectorSupportsWriteBranch();
    }

    /**
     * Throws when the debug point names the MV this overwrite targets; see the constant above.
     */
    private static void failBetweenTheTwoHalvesOfAnOverwrite(TableIf targetTable) throws UserException {
        if (!(targetTable instanceof MTMV)
                || !targetTable.getName().equals(DebugPointUtil.getDebugParamOrDefault(
                        DEBUG_POINT_FAIL_BETWEEN_THE_HALVES_OF_AN_OVERWRITE, "mv_name", ""))) {
            return;
        }
        throw new UserException("debug point: " + DEBUG_POINT_FAIL_BETWEEN_THE_HALVES_OF_AN_OVERWRITE);
    }

    /**
     * Cancels this overwrite when the debug point names the table it targets; see the constants above.
     * Nothing here decides what a cancelled overwrite means -- the call sites do, and they differ: the ones
     * before the rows are durable take the statement back, the one after them does not.
     *
     * <p>One lookup, because a point is consumed by the lookup that reads it: reading it twice with
     * {@code execute=1} armed would have the first read spend the allowance and the point be gone before the
     * second, which would silently leave the overwrite uncancelled.
     */
    private void cancelTheOverwriteAt(String debugPointName, TableIf targetTable) {
        if (!targetTable.getName().equals(DebugPointUtil.getDebugParamOrDefault(
                debugPointName, "table_name", ""))) {
            return;
        }
        LOG.info("debug point {} cancels the overwrite of {}", debugPointName, targetTable.getName());
        cancel();
    }

    /**
     * Publishes this overwrite by running its swap, with a last look at the cancellation flag taken under the
     * lock the swap contends for.
     *
     * <p>{@link #run} reads the flag before the swap is issued, and the swap then waits for the table's write
     * lock, so a cancellation that arrives during that wait is the one place a check before the swap cannot
     * see. Reading it again here costs nothing and is where the wait happens: for a cancellation with nothing
     * committed there is nothing durable to publish, so refusing to swap costs the statement and leaves the
     * rows the client asked to keep -- while a swap that went ahead would replace them with an empty result.
     *
     * <p>Only a local table is wrapped: a remote table swaps on the frontend that owns it, where this lock
     * says nothing.
     */
    private void publishTheOverwrite(TableIf targetTable, InsertCommandContext insertCtx, ConnectContext ctx,
            OverwritePublication publication) throws UserException {
        if (!(targetTable instanceof OlapTable) || targetTable instanceof RemoteOlapTable) {
            publication.publish();
            return;
        }
        OlapTable olapTable = (OlapTable) targetTable;
        if (!olapTable.writeLockIfExist()) {
            // The target was dropped while this overwrite ran, so there is nothing to publish into and no swap
            // to issue. Failing is also what the utility's own early return did for a dropped table -- its
            // finally unlocks a lock that return never took, which raises -- and it is what a client whose
            // swap never happened is owed: acknowledging the overwrite would claim rows the table cannot hold.
            // The catch drops the temp partitions of the dropped table and takes the task back.
            throw new UserException("insert overwrite could not publish its temporary partitions: table "
                    + olapTable.getName() + " was dropped, queryId: " + ctx.getQueryIdentifier());
        }
        try {
            cancelTheOverwriteAt(DEBUG_POINT_CANCEL_WHILE_THE_SWAP_WAITS_FOR_THE_TABLE_LOCK, targetTable);
            if (isCancelled.get() && !insertCtx.hasCommitted()) {
                throw cancelledBeforeTheRowsWereCommitted("while the swap waited for the table lock", ctx);
            }
            publication.publish();
        } finally {
            olapTable.writeUnlock();
        }
    }

    /**
     * The failure a cancellation that found nothing durable is reported as. No row and no stream offset was
     * committed, so a re-run reads the same rows -- which is why this is a failure rather than the success of
     * an overwrite that never ran. What a cancellation means on the other side of that boundary, where the
     * rows are durable, is decided where the swap runs.
     */
    private static UserException cancelledBeforeTheRowsWereCommitted(String stage, ConnectContext ctx) {
        return new UserException("insert overwrite is cancelled " + stage + ", queryId: "
                + ctx.getQueryIdentifier());
    }

    private void runInsertCommand(LogicalPlan logicalQuery, InsertCommandContext insertCtx,
            ConnectContext ctx, StmtExecutor executor) throws Exception {
        InsertIntoTableCommand insertCommand = new InsertIntoTableCommand(logicalQuery, labelName,
                Optional.of(insertCtx), Optional.empty(), false, branchName);
        insertCommand.run(ctx, executor);
        if (ctx.getState().getStateType() == MysqlStateType.ERR) {
            if (insertCtx.hasCommitted()) {
                // The rows are durable and only their publication timed out, which the session's
                // visibility-timeout mode turns into this error (`insert_visible_timeout_return_mode=error`).
                // Dropping the temp partitions for it would lose exactly what the error says was committed,
                // so the overwrite keeps going: the swap below publishes the rows, and the error the client
                // gets stays what it is -- a statement about visibility, not about whether the overwrite ran.
                LOG.info("insert overwrite continues over an error state whose rows are committed, queryId: {}",
                        ctx.getQueryIdentifier());
                return;
            }
            String errMsg = Strings.emptyToNull(ctx.getState().getErrorMessage());
            LOG.warn("InsertInto state error:{}", errMsg);
            throw new UserException(errMsg);
        }
    }

    /**
     * insert into select. for sepecified temp partitions or all partitions(table).
     *
     * @param ctx                ctx
     * @param executor           executor
     * @param tempPartitionNames tempPartitionNames
     * @param wholeTable         overwrite target is the whole table. not one by one by partitions(...)
     * @return the context the inner insert ran under, which says whether it committed anything; see
     *         {@link InsertCommandContext#hasCommittedNothing()}
     */
    private InsertCommandContext insertIntoPartitions(ConnectContext ctx, StmtExecutor executor,
            List<String> tempPartitionNames, boolean wholeTable)
            throws Exception {
        // copy sink tot replace by tempPartitions
        UnboundLogicalSink<?> copySink;
        InsertCommandContext insertCtx;
        LogicalPlan logicalQuery = getLogicalQuery();
        if (logicalQuery instanceof UnboundTableSink) {
            UnboundTableSink<?> sink = (UnboundTableSink<?>) logicalQuery;
            copySink = (UnboundLogicalSink<?>) UnboundTableSinkCreator.createUnboundTableSink(
                    sink.getNameParts(),
                    sink.getColNames(),
                    sink.getHints(),
                    true,
                    tempPartitionNames,
                    sink.isPartialUpdate(),
                    sink.getPartialUpdateNewRowPolicy(),
                    sink.getDMLCommandType(),
                    (LogicalPlan) (sink.child(0)));
            // 1. when overwrite table, allow auto partition or not is controlled by session variable.
            // 2. we save and pass overwrite auto detect by insertCtx
            boolean allowAutoPartition = wholeTable && ctx.getSessionVariable().isEnableAutoCreateWhenOverwrite();
            insertCtx = new OlapInsertCommandContext(allowAutoPartition, true);
        } else if (logicalQuery instanceof UnboundConnectorTableSink) {
            UnboundConnectorTableSink<?> sink = (UnboundConnectorTableSink<?>) logicalQuery;
            copySink = (UnboundLogicalSink<?>) UnboundTableSinkCreator.createUnboundTableSink(
                    sink.getNameParts(), sink.getColNames(), sink.getHints(),
                    false, sink.getPartitions(), false,
                    TPartialUpdateNewRowPolicy.APPEND,
                    sink.getDMLCommandType(),
                    (LogicalPlan) (sink.child(0)),
                    sink.getStaticPartitionKeyValues());
            PluginDrivenInsertCommandContext pluginCtx = new PluginDrivenInsertCommandContext();
            pluginCtx.setOverwrite(true);
            // Thread the @branch target onto the generic write context (the inner InsertIntoTableCommand
            // reuses this ctx) so the connector points the overwrite commit at the branch. The guard above
            // already rejected @branch for connectors without supportsWriteBranch().
            branchName.ifPresent(notUsed -> pluginCtx.setBranchName(branchName));
            if (sink.hasStaticPartition()) {
                Map<String, String> staticSpec = Maps.newHashMap();
                for (Map.Entry<String, Expression> e : sink.getStaticPartitionKeyValues().entrySet()) {
                    if (e.getValue() instanceof Literal) {
                        staticSpec.put(e.getKey(), ((Literal) e.getValue()).getStringValue());
                    }
                }
                pluginCtx.setStaticPartitionSpec(staticSpec);
            }
            insertCtx = pluginCtx;
        } else {
            throw new UserException("Current catalog does not support insert overwrite yet.");
        }
        runInsertCommand(copySink, insertCtx, ctx, executor);
        return insertCtx;
    }

    /**
     * insert into auto detect partition.
     *
     * @param ctx ctx
     * @param executor executor
     * @return the context the inner insert ran under, which says whether it committed anything; see
     *         {@link InsertCommandContext#hasCommittedNothing()}
     */
    private InsertCommandContext insertIntoAutoDetect(ConnectContext ctx, StmtExecutor executor, long groupId)
            throws Exception {
        InsertCommandContext insertCtx;
        LogicalPlan logicalQuery = getLogicalQuery();
        if (logicalQuery instanceof UnboundTableSink) {
            // 1. when overwrite auto-detect, allow auto partition or not is controlled by session variable.
            // 2. we save and pass overwrite auto detect by insertCtx
            boolean allowAutoPartition = ctx.getSessionVariable().isEnableAutoCreateWhenOverwrite();
            insertCtx = new OlapInsertCommandContext(allowAutoPartition,
                    ((UnboundTableSink<?>) logicalQuery).isAutoDetectPartition(), groupId, true);
        } else {
            throw new UserException("Current catalog does not support insert overwrite with auto-detect partition.");
        }
        runInsertCommand(logicalQuery, insertCtx, ctx, executor);
        return insertCtx;
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        Optional<CascadesContext> analyzeContext = Optional.of(
                CascadesContext.initContext(ctx.getStatementContext(), originLogicalQuery, PhysicalProperties.ANY)
        );
        return InsertUtils.getPlanForExplain(ctx, analyzeContext, getLogicalQuery(), branchName);
    }

    @Override
    public Optional<NereidsPlanner> getExplainPlanner(LogicalPlan logicalPlan, StatementContext ctx) {
        LogicalPlan logicalQuery = getLogicalQuery();
        if (logicalQuery instanceof UnboundTableSink) {
            boolean allowAutoPartition = ctx.getConnectContext().getSessionVariable().isEnableAutoCreateWhenOverwrite();
            OlapInsertCommandContext insertCtx = new OlapInsertCommandContext(allowAutoPartition, true);
            InsertIntoTableCommand insertIntoTableCommand = new InsertIntoTableCommand(
                    logicalQuery, labelName, Optional.of(insertCtx), Optional.empty(), true, Optional.empty());
            return insertIntoTableCommand.getExplainPlanner(logicalPlan, ctx);
        }
        return Optional.empty();
    }

    public boolean isForceDropPartition() {
        return true;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitInsertOverwriteTableCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.INSERT;
    }

    @Override
    public boolean needAuditEncryption() {
        return originLogicalQuery.anyMatch(node -> node instanceof TVFRelation);
    }

    @Override
    public String toDigest() {
        // if with cte, query will be print twice
        StringBuilder sb = new StringBuilder();
        sb.append("OVERWRITE TABLE "); // there is no way add overwrite flag in sink(logic query), so add it here
        sb.append(originLogicalQuery.toDigest());
        if (cte.isPresent()) {
            sb.append(" (").append(cte.get().toDigest()).append(")");
        }
        return sb.toString();
    }
}
