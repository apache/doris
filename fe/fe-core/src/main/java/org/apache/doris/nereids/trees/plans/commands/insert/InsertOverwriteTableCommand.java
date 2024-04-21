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
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.InternalDatabaseUtil;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.insertoverwrite.InsertOverwriteUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.UnboundHiveTableSink;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.analyzer.UnboundTableSinkCreator;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.ForwardWithSync;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.UnboundLogicalSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTableSink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * insert into select command implementation
 * insert into select command support the grammer: explain? insert into table columns? partitions? hints? query
 * InsertIntoTableCommand is a command to represent insert the answer of a query into a table.
 * class structure's:
 * InsertIntoTableCommand(Query())
 * ExplainCommand(Query())
 */
public class InsertOverwriteTableCommand extends Command implements ForwardWithSync, Explainable {

    private static final Logger LOG = LogManager.getLogger(InsertOverwriteTableCommand.class);

    private LogicalPlan logicalQuery;
    private Optional<String> labelName;

    /**
     * constructor
     */
    public InsertOverwriteTableCommand(LogicalPlan logicalQuery, Optional<String> labelName) {
        super(PlanType.INSERT_INTO_TABLE_COMMAND);
        this.logicalQuery = Objects.requireNonNull(logicalQuery, "logicalQuery should not be null");
        this.labelName = Objects.requireNonNull(labelName, "labelName should not be null");
    }

    public void setLabelName(Optional<String> labelName) {
        this.labelName = labelName;
    }

    public boolean isAutoDetectOverwrite() {
        return (logicalQuery instanceof UnboundTableSink)
                && ((UnboundTableSink<?>) this.logicalQuery).isAutoDetectPartition();
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!ctx.getSessionVariable().isEnableNereidsDML()) {
            try {
                ctx.getSessionVariable().enableFallbackToOriginalPlannerOnce();
            } catch (Exception e) {
                throw new AnalysisException("failed to set fallback to original planner to true", e);
            }
            throw new AnalysisException("Nereids DML is disabled, will try to fall back to the original planner");
        }

        TableIf targetTableIf = InsertUtils.getTargetTable(logicalQuery, ctx);
        if (!(targetTableIf instanceof OlapTable || targetTableIf instanceof HMSExternalTable)) {
            throw new AnalysisException("insert into overwrite only support OLAP and HMS table."
                    + " But current table type is " + targetTableIf.getType());
        }
        this.logicalQuery = (LogicalPlan) InsertUtils.normalizePlan(logicalQuery, targetTableIf);

        LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(logicalQuery, ctx.getStatementContext());
        NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
        planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());
        executor.checkBlockRules();
        if (ctx.getMysqlChannel() != null) {
            ctx.getMysqlChannel().reset();
        }

        Optional<TreeNode<?>> plan = (planner.getPhysicalPlan()
                .<Set<TreeNode<?>>>collect(node -> node instanceof PhysicalTableSink)).stream().findAny();
        Preconditions.checkArgument(plan.isPresent(), "insert into command must contain OlapTableSinkNode");
        PhysicalTableSink<?> physicalTableSink = ((PhysicalTableSink<?>) plan.get());
        TableIf targetTable = physicalTableSink.getTargetTable();
        List<String> partitionNames;
        if (physicalTableSink instanceof PhysicalOlapTableSink) {
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
            ConnectContext.get().setSkipAuth(true);
            partitionNames = ((UnboundTableSink<?>) logicalQuery).getPartitions();
            if (CollectionUtils.isEmpty(partitionNames)) {
                partitionNames = Lists.newArrayList(targetTable.getPartitionNames());
            }
        } else {
            // Do not create temp partition on FE
            partitionNames = new ArrayList<>();
        }
        long taskId = 0;
        try {
            if (isAutoDetectOverwrite()) {
                // taskId here is a group id. it contains all replace tasks made and registered in rpc process.
                taskId = Env.getCurrentEnv().getInsertOverwriteManager().preRegisterTask();
                // When inserting, BE will call to replace partition by FrontendService. FE do the real
                // add&replacement and return replace result. So there's no need to do anything else.
                insertInto(ctx, executor, taskId);
                Env.getCurrentEnv().getInsertOverwriteManager().taskGroupSuccess(taskId);
            } else {
                List<String> tempPartitionNames = InsertOverwriteUtil.generateTempPartitionNames(partitionNames);
                taskId = Env.getCurrentEnv().getInsertOverwriteManager()
                        .registerTask(targetTable.getDatabase().getId(), targetTable.getId(), tempPartitionNames);
                InsertOverwriteUtil.addTempPartitions(targetTable, partitionNames, tempPartitionNames);
                insertInto(ctx, executor, tempPartitionNames);
                InsertOverwriteUtil.replacePartition(targetTable, partitionNames, tempPartitionNames);
                Env.getCurrentEnv().getInsertOverwriteManager().taskSuccess(taskId);
            }
        } catch (Exception e) {
            LOG.warn("insert into overwrite failed");
            if (isAutoDetectOverwrite()) {
                Env.getCurrentEnv().getInsertOverwriteManager().taskGroupFail(taskId);
            } else {
                Env.getCurrentEnv().getInsertOverwriteManager().taskFail(taskId);
            }
            throw e;
        } finally {
            ConnectContext.get().setSkipAuth(false);
        }
    }

    private void runInsertCommand(LogicalPlan logicalQuery, InsertCommandContext insertCtx,
                                  ConnectContext ctx, StmtExecutor executor) throws Exception {
        InsertIntoTableCommand insertCommand = new InsertIntoTableCommand(logicalQuery, labelName,
                Optional.of(insertCtx));
        insertCommand.run(ctx, executor);
        if (ctx.getState().getStateType() == MysqlStateType.ERR) {
            String errMsg = Strings.emptyToNull(ctx.getState().getErrorMessage());
            LOG.warn("InsertInto state error:{}", errMsg);
            throw new UserException(errMsg);
        }
    }

    /**
     * insert into select. for sepecified temp partitions
     *
     * @param ctx                ctx
     * @param executor           executor
     * @param tempPartitionNames tempPartitionNames
     */
    private void insertInto(ConnectContext ctx, StmtExecutor executor, List<String> tempPartitionNames)
            throws Exception {
        // copy sink tot replace by tempPartitions
        UnboundLogicalSink<?> copySink;
        InsertCommandContext insertCtx;
        if (logicalQuery instanceof UnboundTableSink) {
            UnboundTableSink<?> sink = (UnboundTableSink<?>) logicalQuery;
            copySink = (UnboundLogicalSink<?>) UnboundTableSinkCreator.createUnboundTableSink(
                    sink.getNameParts(),
                    sink.getColNames(),
                    sink.getHints(),
                    true,
                    tempPartitionNames,
                    sink.isPartialUpdate(),
                    sink.getDMLCommandType(),
                    (LogicalPlan) (sink.child(0)));
            // 1. for overwrite situation, we disable auto create partition.
            // 2. we save and pass overwrite auto detect by insertCtx
            insertCtx = new OlapInsertCommandContext(false);
        } else if (logicalQuery instanceof UnboundHiveTableSink) {
            UnboundHiveTableSink<?> sink = (UnboundHiveTableSink<?>) logicalQuery;
            copySink = (UnboundLogicalSink<?>) UnboundTableSinkCreator.createUnboundTableSink(
                    sink.getNameParts(),
                    sink.getColNames(),
                    sink.getHints(),
                    false,
                    sink.getPartitions(),
                    false,
                    sink.getDMLCommandType(),
                    (LogicalPlan) (sink.child(0)));
            insertCtx = new HiveInsertCommandContext();
            ((HiveInsertCommandContext) insertCtx).setOverwrite(true);
        } else {
            throw new UserException("Current catalog does not support insert overwrite yet.");
        }
        runInsertCommand(copySink, insertCtx, ctx, executor);
    }

    /**
     * insert into auto detect partition.
     *
     * @param ctx      ctx
     * @param executor executor
     */
    private void insertInto(ConnectContext ctx, StmtExecutor executor, long groupId) throws Exception {
        // 1. for overwrite situation, we disable auto create partition.
        // 2. we save and pass overwrite auto-detected by insertCtx
        InsertCommandContext insertCtx;
        if (logicalQuery instanceof UnboundTableSink) {
            insertCtx = new OlapInsertCommandContext(false,
                    ((UnboundTableSink<?>) logicalQuery).isAutoDetectPartition(), groupId);
        } else if (logicalQuery instanceof UnboundHiveTableSink) {
            insertCtx = new HiveInsertCommandContext();
            ((HiveInsertCommandContext) insertCtx).setOverwrite(true);
        } else {
            throw new UserException("Current catalog does not support insert overwrite yet.");
        }
        runInsertCommand(logicalQuery, insertCtx, ctx, executor);
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        return InsertUtils.getPlanForExplain(ctx, this.logicalQuery);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitInsertOverwriteTableCommand(this, context);
    }
}
