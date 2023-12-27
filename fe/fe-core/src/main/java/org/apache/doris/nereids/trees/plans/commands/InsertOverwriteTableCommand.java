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

import org.apache.doris.analysis.AddPartitionLikeClause;
import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.DropPartitionClause;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.ReplacePartitionClause;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.DdlExecutor;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

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
        // insert overwrite only support
        TableIf targetTableIf = InsertExecutor.getTargetTable(logicalQuery, ctx);
        if (!(targetTableIf instanceof OlapTable)) {
            throw new AnalysisException("insert into overwrite only support OLAP table."
                    + " But current table type is " + targetTableIf.getType());
        }
        this.logicalQuery = (LogicalPlan) InsertExecutor.normalizePlan(logicalQuery, targetTableIf);

        LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(logicalQuery, ctx.getStatementContext());
        NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
        planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());
        executor.checkBlockRules();
        if (ctx.getMysqlChannel() != null) {
            ctx.getMysqlChannel().reset();
        }

        Optional<TreeNode<?>> plan = (planner.getPhysicalPlan()
                .<Set<TreeNode<?>>>collect(node -> node instanceof PhysicalOlapTableSink)).stream().findAny();
        Preconditions.checkArgument(plan.isPresent(), "insert into command must contain OlapTableSinkNode");
        PhysicalOlapTableSink<?> physicalOlapTableSink = ((PhysicalOlapTableSink<?>) plan.get());
        OlapTable targetTable = physicalOlapTableSink.getTargetTable();
        // check auth
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), targetTable.getQualifiedDbName(), targetTable.getName(),
                        PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    targetTable.getQualifiedDbName() + ": " + targetTable.getName());
        }

        TableName tableName = new TableName(InternalCatalog.INTERNAL_CATALOG_NAME,
                targetTable.getQualifiedDbName(), targetTable.getName());
        ConnectContext.get().setSkipAuth(true);
        try {
            List<String> partitionNames = ((UnboundTableSink<?>) logicalQuery).getPartitions();
            if (CollectionUtils.isEmpty(partitionNames)) {
                partitionNames = Lists.newArrayList(targetTable.getPartitionNames());
            }
            List<String> tempPartitionNames = addTempPartitions(ctx, tableName, partitionNames);
            boolean insertRes = insertInto(ctx, executor, tempPartitionNames, tableName);
            if (!insertRes) {
                return;
            }
            replacePartition(ctx, tableName, partitionNames, tempPartitionNames);
        } finally {
            ConnectContext.get().setSkipAuth(false);
        }
    }

    /**
     * replacing partitionNames with tempPartitionNames
     *
     * @param ctx ctx
     * @param tableName tableName
     * @param partitionNames partitionNames
     * @param tempPartitionNames tempPartitionNames
     * @throws UserException UserException
     */
    private void replacePartition(ConnectContext ctx, TableName tableName, List<String> partitionNames,
            List<String> tempPartitionNames)
            throws UserException {
        // overwrite old partition with tmp partition
        try {
            List<AlterClause> ops = new ArrayList<>();
            Map<String, String> properties = new HashMap<>();
            properties.put("use_temp_partition_name", "false");
            ops.add(new ReplacePartitionClause(new PartitionNames(false, partitionNames),
                    new PartitionNames(true, tempPartitionNames), properties));
            AlterTableStmt alterTableStmt = new AlterTableStmt(tableName, ops);
            Env.getCurrentEnv().alterTable(alterTableStmt);
        } catch (Exception e) {
            LOG.warn("IOT overwrite table partitions error", e);
            rollback(ctx, tableName, tempPartitionNames);
            throw e;
        }
    }

    /**
     * insert into select
     *
     * @param ctx ctx
     * @param executor executor
     * @param tempPartitionNames tempPartitionNames
     * @param tableName tableName
     */
    private boolean insertInto(ConnectContext ctx, StmtExecutor executor, List<String> tempPartitionNames,
            TableName tableName) {
        try {
            UnboundTableSink<?> sink = (UnboundTableSink<?>) logicalQuery;
            UnboundTableSink<?> copySink = new UnboundTableSink<>(
                    sink.getNameParts(),
                    sink.getColNames(),
                    sink.getHints(),
                    true,
                    tempPartitionNames,
                    sink.isPartialUpdate(),
                    sink.getDMLCommandType(),
                    (LogicalPlan) (sink.child(0)));
            // for overwrite situation, we disable auto create partition.
            InsertIntoTableCommand insertCommand = new InsertIntoTableCommand(copySink, labelName);
            insertCommand.setAllowAutoPartition(false);
            insertCommand.run(ctx, executor);
            if (ctx.getState().getStateType() == MysqlStateType.ERR) {
                String errMsg = Strings.emptyToNull(ctx.getState().getErrorMessage());
                LOG.warn("InsertInto state error:{}", errMsg);
                rollback(ctx, tableName, tempPartitionNames);
                return false;
            }
            return true;
        } catch (Exception e) {
            LOG.warn("InsertInto error", e);
            rollback(ctx, tableName, tempPartitionNames);
            return false;
        }
    }

    /**
     * add some tempPartitions
     *
     * @param ctx ctx
     * @param tableName tableName
     * @param partitionNames partitionNames
     * @return tempPartitionNames
     * @throws Exception Exception
     */
    private List<String> addTempPartitions(ConnectContext ctx, TableName tableName, List<String> partitionNames)
            throws Exception {
        List<String> tempPartitionNames = new ArrayList<>();
        try {
            // create tmp partitions with uuid
            for (String partitionName : partitionNames) {
                UUID uuid = UUID.randomUUID();
                // to comply with naming rules
                String tempPartName = "tmp_partition_" + uuid.toString().replace('-', '_');
                List<AlterClause> ops = new ArrayList<>();
                ops.add(new AddPartitionLikeClause(tempPartName, partitionName, true));

                AlterTableStmt alterTableStmt = new AlterTableStmt(tableName, ops);
                Analyzer tempAnalyzer = new Analyzer(Env.getCurrentEnv(), ctx);
                alterTableStmt.analyze(tempAnalyzer);
                DdlExecutor.execute(ctx.getEnv(), alterTableStmt);
                // only when execution succeeded, put the temp partition name into list
                tempPartitionNames.add(tempPartName);
            }
            return tempPartitionNames;
        } catch (Exception e) {
            LOG.warn("IOT create tmp table partitions error", e);
            rollback(ctx, tableName, tempPartitionNames);
            throw e;
        }
    }

    /**
     * delete temp partitions
     *
     * @param ctx ctx
     * @param targetTableName targetTableName
     * @param tempPartitionNames tempPartitionNames
     */
    private void rollback(ConnectContext ctx, TableName targetTableName,
            List<String> tempPartitionNames) {
        try {
            for (String partitionName : tempPartitionNames) {
                List<AlterClause> ops = new ArrayList<>();
                ops.add(new DropPartitionClause(true, partitionName, true, true));
                AlterTableStmt dropTablePartitionStmt = new AlterTableStmt(targetTableName, ops);
                Analyzer tempAnalyzer = new Analyzer(Env.getCurrentEnv(), ctx);
                dropTablePartitionStmt.analyze(tempAnalyzer);
                DdlExecutor.execute(ctx.getEnv(), dropTablePartitionStmt);
            }
        } catch (Exception ex) {
            LOG.warn("IOT drop partitions error", ex);
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Unexpected exception: " + ex.getMessage());
        }
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        if (!ctx.getSessionVariable().isEnableNereidsDML()) {
            try {
                ctx.getSessionVariable().enableFallbackToOriginalPlannerOnce();
            } catch (Exception e) {
                throw new AnalysisException("failed to set fallback to original planner to true", e);
            }
            throw new AnalysisException("Nereids DML is disabled, will try to fall back to the original planner");
        }
        return InsertExecutor.normalizePlan(this.logicalQuery, InsertExecutor.getTargetTable(this.logicalQuery, ctx));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitInsertOverwriteTableCommand(this, context);
    }
}
