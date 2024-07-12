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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.ProfileManager.ProfileType;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.load.loadv2.LoadStatistic;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.ForwardWithSync;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHiveTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIcebergTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.planner.DataSink;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.Optional;

/**
 * insert into select command implementation
 * insert into select command support the grammar: explain? insert into table columns? partitions? hints? query
 * InsertIntoTableCommand is a command to represent insert the answer of a query into a table.
 * class structure's:
 * InsertIntoTableCommand(Query())
 * ExplainCommand(Query())
 */
public class InsertIntoTableCommand extends Command implements ForwardWithSync, Explainable {

    public static final Logger LOG = LogManager.getLogger(InsertIntoTableCommand.class);

    private LogicalPlan logicalQuery;
    private Optional<String> labelName;
    /**
     * When source it's from job scheduler,it will be set.
     */
    private long jobId;
    private Optional<InsertCommandContext> insertCtx;
    private final Optional<LogicalPlan> cte;

    /**
     * constructor
     */
    public InsertIntoTableCommand(LogicalPlan logicalQuery, Optional<String> labelName,
                                  Optional<InsertCommandContext> insertCtx, Optional<LogicalPlan> cte) {
        super(PlanType.INSERT_INTO_TABLE_COMMAND);
        this.logicalQuery = Objects.requireNonNull(logicalQuery, "logicalQuery should not be null");
        this.labelName = Objects.requireNonNull(labelName, "labelName should not be null");
        this.insertCtx = insertCtx;
        this.cte = cte;
    }

    public Optional<String> getLabelName() {
        return labelName;
    }

    public void setLabelName(Optional<String> labelName) {
        this.labelName = labelName;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        runInternal(ctx, executor);
    }

    public void runWithUpdateInfo(ConnectContext ctx, StmtExecutor executor,
                                  LoadStatistic loadStatistic) throws Exception {
        // TODO: add coordinator statistic
        runInternal(ctx, executor);
    }

    /**
     * This function is used to generate the plan for Nereids.
     * There are some load functions that only need to the plan, such as stream_load.
     * Therefore, this section will be presented separately.
     */
    public AbstractInsertExecutor initPlan(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!ctx.getSessionVariable().isEnableNereidsDML()) {
            try {
                ctx.getSessionVariable().enableFallbackToOriginalPlannerOnce();
            } catch (Exception e) {
                throw new AnalysisException("failed to set fallback to original planner to true", e);
            }
            throw new AnalysisException("Nereids DML is disabled, will try to fall back to the original planner");
        }

        TableIf targetTableIf = InsertUtils.getTargetTable(logicalQuery, ctx);
        // check auth
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), targetTableIf.getDatabase().getCatalog().getName(),
                        targetTableIf.getDatabase().getFullName(), targetTableIf.getName(),
                        PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    targetTableIf.getDatabase().getFullName() + "." + targetTableIf.getName());
        }

        AbstractInsertExecutor insertExecutor = null;
        // should lock target table until we begin transaction.
        targetTableIf.readLock();
        try {
            // 1. process inline table (default values, empty values)
            this.logicalQuery = (LogicalPlan) InsertUtils.normalizePlan(logicalQuery, targetTableIf, insertCtx);
            if (cte.isPresent()) {
                this.logicalQuery = ((LogicalPlan) cte.get().withChildren(logicalQuery));
            }
            boolean isOverwrite = insertCtx.isPresent() && insertCtx.get() instanceof OlapInsertCommandContext
                    && ((OlapInsertCommandContext) insertCtx.get()).isOverwrite();
            if (this.logicalQuery instanceof UnboundTableSink && !isOverwrite) {
                OlapGroupCommitInsertExecutor.analyzeGroupCommit(ctx, targetTableIf,
                        (UnboundTableSink<?>) this.logicalQuery);
            }
            LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(logicalQuery, ctx.getStatementContext());
            NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
            planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());
            executor.setPlanner(planner);
            executor.checkBlockRules();
            if (ctx.getConnectType() == ConnectType.MYSQL && ctx.getMysqlChannel() != null) {
                ctx.getMysqlChannel().reset();
            }
            Optional<PhysicalSink<?>> plan = (planner.getPhysicalPlan()
                    .<PhysicalSink<?>>collect(PhysicalSink.class::isInstance)).stream()
                    .findAny();
            Preconditions.checkArgument(plan.isPresent(), "insert into command must contain target table");
            PhysicalSink physicalSink = plan.get();
            DataSink sink = planner.getFragments().get(0).getSink();
            // Transaction insert should reuse the label in the transaction.
            String label = this.labelName.orElse(
                    ctx.isTxnModel() ? null : String.format("label_%x_%x", ctx.queryId().hi, ctx.queryId().lo));

            if (physicalSink instanceof PhysicalOlapTableSink) {
                boolean emptyInsert = childIsEmptyRelation(physicalSink);
                OlapTable olapTable = (OlapTable) targetTableIf;
                // the insertCtx contains some variables to adjust SinkNode
                if (ctx.isTxnModel()) {
                    insertExecutor = new OlapTxnInsertExecutor(ctx, olapTable, label, planner, insertCtx, emptyInsert);
                } else if (ctx.isGroupCommit()) {
                    insertExecutor = new OlapGroupCommitInsertExecutor(ctx, olapTable, label, planner, insertCtx,
                            emptyInsert);
                } else {
                    insertExecutor = new OlapInsertExecutor(ctx, olapTable, label, planner, insertCtx, emptyInsert);
                }

                boolean isEnableMemtableOnSinkNode =
                        olapTable.getTableProperty().getUseSchemaLightChange()
                                ? insertExecutor.getCoordinator().getQueryOptions().isEnableMemtableOnSinkNode()
                                : false;
                insertExecutor.getCoordinator().getQueryOptions()
                        .setEnableMemtableOnSinkNode(isEnableMemtableOnSinkNode);
            } else if (physicalSink instanceof PhysicalHiveTableSink) {
                boolean emptyInsert = childIsEmptyRelation(physicalSink);
                HMSExternalTable hiveExternalTable = (HMSExternalTable) targetTableIf;
                insertExecutor = new HiveInsertExecutor(ctx, hiveExternalTable, label, planner,
                        Optional.of(insertCtx.orElse((new HiveInsertCommandContext()))), emptyInsert);
                // set hive query options
            } else if (physicalSink instanceof PhysicalIcebergTableSink) {
                boolean emptyInsert = childIsEmptyRelation(physicalSink);
                IcebergExternalTable icebergExternalTable = (IcebergExternalTable) targetTableIf;
                insertExecutor = new IcebergInsertExecutor(ctx, icebergExternalTable, label, planner,
                        Optional.of(insertCtx.orElse((new BaseExternalTableInsertCommandContext()))), emptyInsert);
            } else {
                // TODO: support other table types
                throw new AnalysisException("insert into command only support [olap, hive, iceberg] table");
            }

            insertExecutor.beginTransaction();
            insertExecutor.finalizeSink(planner.getFragments().get(0), sink, physicalSink);
            targetTableIf.readUnlock();
        } catch (Throwable e) {
            targetTableIf.readUnlock();
            // the abortTxn in onFail need to acquire table write lock
            if (insertExecutor != null) {
                insertExecutor.onFail(e);
            }
            throw e;
        }

        executor.setProfileType(ProfileType.LOAD);
        // We exposed @StmtExecutor#cancel as a unified entry point for statement interruption,
        // so we need to set this here
        executor.setCoord(insertExecutor.getCoordinator());
        return insertExecutor;
    }

    private void runInternal(ConnectContext ctx, StmtExecutor executor) throws Exception {
        AbstractInsertExecutor insertExecutor = initPlan(ctx, executor);
        // if the insert stmt data source is empty, directly return, no need to be executed.
        if (insertExecutor.isEmptyInsert()) {
            return;
        }
        insertExecutor.executeSingleInsert(executor, jobId);
    }

    public boolean isExternalTableSink() {
        return !(logicalQuery instanceof UnboundTableSink);
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        return InsertUtils.getPlanForExplain(ctx, this.logicalQuery);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitInsertIntoTableCommand(this, context);
    }

    private boolean childIsEmptyRelation(PhysicalSink sink) {
        if (sink.children() != null && sink.children().size() == 1
                && sink.child(0) instanceof PhysicalEmptyRelation) {
            return true;
        }
        return false;
    }
}
