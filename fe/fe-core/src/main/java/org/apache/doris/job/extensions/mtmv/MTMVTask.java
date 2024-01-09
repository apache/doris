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

package org.apache.doris.job.extensions.mtmv;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshMethod;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.commands.UpdateMvByPartitionCommand;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public class MTMVTask extends AbstractTask {
    private static final Logger LOG = LogManager.getLogger(MTMVTask.class);
    public static final int DEFAULT_REFRESH_PARTITION_NUM = 1;

    public static final ImmutableList<Column> SCHEMA = ImmutableList.of(
            new Column("TaskId", ScalarType.createStringType()),
            new Column("JobId", ScalarType.createStringType()),
            new Column("JobName", ScalarType.createStringType()),
            new Column("MvId", ScalarType.createStringType()),
            new Column("MvName", ScalarType.createStringType()),
            new Column("MvDatabaseId", ScalarType.createStringType()),
            new Column("MvDatabaseName", ScalarType.createStringType()),
            new Column("Status", ScalarType.createStringType()),
            new Column("ErrorMsg", ScalarType.createStringType()),
            new Column("CreateTime", ScalarType.createStringType()),
            new Column("StartTime", ScalarType.createStringType()),
            new Column("FinishTime", ScalarType.createStringType()),
            new Column("DurationMs", ScalarType.createStringType()),
            new Column("TaskContext", ScalarType.createStringType()),
            new Column("RefreshMode", ScalarType.createStringType()),
            new Column("NeedRefreshPartitions", ScalarType.createStringType()),
            new Column("CompletedPartitions", ScalarType.createStringType()),
            new Column("Progress", ScalarType.createStringType()));

    public static final ImmutableMap<String, Integer> COLUMN_TO_INDEX;

    static {
        ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder();
        for (int i = 0; i < SCHEMA.size(); i++) {
            builder.put(SCHEMA.get(i).getName().toLowerCase(), i);
        }
        COLUMN_TO_INDEX = builder.build();
    }

    public enum MTMVTaskTriggerMode {
        MANUAL,
        SYSTEM
    }

    public enum MTMVTaskRefreshMode {
        COMPLETE,
        PARTIAL,
        NOT_REFRESH
    }

    @SerializedName(value = "di")
    private long dbId;
    @SerializedName(value = "mi")
    private long mtmvId;
    @SerializedName("taskContext")
    private MTMVTaskContext taskContext;
    @SerializedName("needRefreshPartitions")
    List<String> needRefreshPartitions;
    @SerializedName("completedPartitions")
    List<String> completedPartitions;
    @SerializedName("refreshMode")
    MTMVTaskRefreshMode refreshMode;

    private MTMV mtmv;
    private MTMVRelation relation;
    private StmtExecutor executor;

    public MTMVTask() {
    }

    public MTMVTask(long dbId, long mtmvId, MTMVTaskContext taskContext) {
        this.dbId = Objects.requireNonNull(dbId);
        this.mtmvId = Objects.requireNonNull(mtmvId);
        this.taskContext = Objects.requireNonNull(taskContext);
    }

    @Override
    public void run() throws JobException {
        LOG.info("mtmv task run, taskId: {}", super.getTaskId());
        try {
            ConnectContext ctx = MTMVPlanUtil.createMTMVContext(mtmv);
            // Every time a task is run, the relation is regenerated because baseTables and baseViews may change,
            // such as deleting a table and creating a view with the same name
            this.relation = MTMVPlanUtil.generateMTMVRelation(mtmv, ctx);
            List<Long> needRefreshPartitionIds = calculateNeedRefreshPartitions();
            this.needRefreshPartitions = MTMVUtil.getPartitionNamesByIds(mtmv, needRefreshPartitionIds);
            this.refreshMode = generateRefreshMode(needRefreshPartitionIds);
            if (refreshMode == MTMVTaskRefreshMode.NOT_REFRESH) {
                return;
            }
            Map<OlapTable, String> tableWithPartKey = getIncrementalTableMap();
            this.completedPartitions = Lists.newArrayList();
            int refreshPartitionNum = mtmv.getRefreshPartitionNum();
            long execNum = (needRefreshPartitionIds.size() / refreshPartitionNum) + ((needRefreshPartitionIds.size()
                    % refreshPartitionNum) > 0 ? 1 : 0);
            for (int i = 0; i < execNum; i++) {
                int start = i * refreshPartitionNum;
                int end = start + refreshPartitionNum;
                Set<Long> execPartitionIds = Sets.newHashSet(needRefreshPartitionIds
                        .subList(start, end > needRefreshPartitionIds.size() ? needRefreshPartitionIds.size() : end));
                // need get names before exec
                List<String> execPartitionNames = MTMVUtil.getPartitionNamesByIds(mtmv, execPartitionIds);
                exec(ctx, execPartitionIds, tableWithPartKey);
                completedPartitions.addAll(execPartitionNames);
            }
        } catch (Throwable e) {
            LOG.warn("run task failed: ", e);
            throw new JobException(e);
        }
    }

    private void exec(ConnectContext ctx, Set<Long> refreshPartitionIds, Map<OlapTable, String> tableWithPartKey)
            throws Exception {
        TUniqueId queryId = generateQueryId();
        // if SELF_MANAGE mv, only have default partition,  will not have partitionItem, so we give empty set
        UpdateMvByPartitionCommand command = UpdateMvByPartitionCommand
                .from(mtmv, mtmv.getMvPartitionInfo().getPartitionType() == MTMVPartitionType.FOLLOW_BASE_TABLE
                        ? refreshPartitionIds : Sets.newHashSet(), tableWithPartKey);
        executor = new StmtExecutor(ctx, new LogicalPlanAdapter(command, ctx.getStatementContext()));
        ctx.setExecutor(executor);
        ctx.setQueryId(queryId);
        command.run(ctx, executor);
        if (ctx.getState().getStateType() != MysqlStateType.OK) {
            throw new JobException(ctx.getState().getErrorMessage());
        }
    }

    @Override
    public synchronized void onFail() throws JobException {
        LOG.info("mtmv task onFail, taskId: {}", super.getTaskId());
        super.onFail();
        after();
    }

    @Override
    public synchronized void onSuccess() throws JobException {
        LOG.info("mtmv task onSuccess, taskId: {}", super.getTaskId());
        super.onSuccess();
        after();
    }

    @Override
    public synchronized void cancel() throws JobException {
        LOG.info("mtmv task cancel, taskId: {}", super.getTaskId());
        super.cancel();
        if (executor != null) {
            executor.cancel();
        }
        after();
    }

    @Override
    public void before() throws JobException {
        LOG.info("mtmv task before, taskId: {}", super.getTaskId());
        super.before();
        try {
            mtmv = getMTMV();
            if (mtmv.getMvPartitionInfo().getPartitionType() == MTMVPartitionType.FOLLOW_BASE_TABLE) {
                OlapTable relatedTable = (OlapTable) MTMVUtil.getTable(mtmv.getMvPartitionInfo().getRelatedTable());
                MTMVUtil.alignMvPartition(mtmv, relatedTable);
            }
        } catch (UserException e) {
            LOG.warn("before task failed:", e);
            throw new JobException(e);
        }
    }

    private MTMV getMTMV() throws DdlException, MetaNotFoundException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbId);
        return (MTMV) db.getTableOrMetaException(mtmvId, TableType.MATERIALIZED_VIEW);
    }

    @Override
    public void runTask() throws JobException {
        LOG.info("mtmv task runTask, taskId: {}", super.getTaskId());
        MTMVJob job = (MTMVJob) getJobOrJobException();
        try {
            LOG.info("mtmv task get writeLock start, taskId: {}", super.getTaskId());
            job.writeLock();
            LOG.info("mtmv task get writeLock end, taskId: {}", super.getTaskId());
            super.runTask();
        } finally {
            job.writeUnlock();
            LOG.info("mtmv task release writeLock, taskId: {}", super.getTaskId());
        }
    }

    @Override
    public TRow getTvfInfo() {
        TRow trow = new TRow();
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(super.getTaskId())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(super.getJobId())));
        trow.addToColumnValue(new TCell().setStringVal(super.getJobName()));
        String dbName = "";
        String mvName = "";
        try {
            MTMV mtmv = getMTMV();
            dbName = mtmv.getQualifiedDbName();
            mvName = mtmv.getName();
        } catch (UserException e) {
            LOG.warn("can not find mv", e);
        }
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(mtmvId)));
        trow.addToColumnValue(new TCell().setStringVal(mvName));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(dbId)));
        trow.addToColumnValue(new TCell().setStringVal(dbName));
        trow.addToColumnValue(new TCell()
                .setStringVal(super.getStatus() == null ? FeConstants.null_string : super.getStatus().toString()));
        trow.addToColumnValue(new TCell().setStringVal(super.getErrMsg()));
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(super.getCreateTimeMs())));
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(super.getStartTimeMs())));
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(super.getFinishTimeMs())));
        trow.addToColumnValue(new TCell().setStringVal(
                (super.getFinishTimeMs() == null || super.getFinishTimeMs() == 0) ? FeConstants.null_string
                        : String.valueOf(super.getFinishTimeMs() - super.getStartTimeMs())));
        trow.addToColumnValue(new TCell()
                .setStringVal(taskContext == null ? FeConstants.null_string : new Gson().toJson(taskContext)));
        trow.addToColumnValue(
                new TCell().setStringVal(refreshMode == null ? FeConstants.null_string : refreshMode.toString()));
        trow.addToColumnValue(
                new TCell().setStringVal(
                        needRefreshPartitions == null ? FeConstants.null_string : new Gson().toJson(
                                needRefreshPartitions)));
        trow.addToColumnValue(
                new TCell().setStringVal(
                        completedPartitions == null ? FeConstants.null_string : new Gson().toJson(
                                completedPartitions)));
        trow.addToColumnValue(
                new TCell().setStringVal(getProgress()));
        return trow;
    }

    private String getProgress() {
        if (CollectionUtils.isEmpty(needRefreshPartitions)) {
            return FeConstants.null_string;
        }
        int completedSize = CollectionUtils.isEmpty(completedPartitions) ? 0 : completedPartitions.size();
        BigDecimal result = new BigDecimal(completedSize * 100)
                .divide(new BigDecimal(needRefreshPartitions.size()), 2, RoundingMode.HALF_UP);
        StringBuilder builder = new StringBuilder(result.toString());
        builder.append("% (");
        builder.append(completedSize);
        builder.append("/");
        builder.append(needRefreshPartitions.size());
        builder.append(")");
        return builder.toString();
    }

    private TUniqueId generateQueryId() {
        UUID taskId = UUID.randomUUID();
        return new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
    }

    private void after() {
        if (mtmv != null) {
            Env.getCurrentEnv()
                    .addMTMVTaskResult(new TableNameInfo(mtmv.getQualifiedDbName(), mtmv.getName()), this, relation);
        }
        mtmv = null;
        relation = null;
        executor = null;
    }

    private Map<OlapTable, String> getIncrementalTableMap() throws AnalysisException {
        Map<OlapTable, String> tableWithPartKey = Maps.newHashMap();
        if (mtmv.getMvPartitionInfo().getPartitionType() == MTMVPartitionType.FOLLOW_BASE_TABLE) {
            OlapTable relatedTable = (OlapTable) MTMVUtil.getTable(mtmv.getMvPartitionInfo().getRelatedTable());
            tableWithPartKey.put(relatedTable, mtmv.getMvPartitionInfo().getRelatedCol());
        }
        return tableWithPartKey;
    }


    private MTMVTaskRefreshMode generateRefreshMode(List<Long> needRefreshPartitionIds) {
        if (CollectionUtils.isEmpty(needRefreshPartitionIds)) {
            return MTMVTaskRefreshMode.NOT_REFRESH;
        } else if (needRefreshPartitionIds.size() == mtmv.getPartitionIds().size()) {
            return MTMVTaskRefreshMode.COMPLETE;
        } else {
            return MTMVTaskRefreshMode.PARTIAL;
        }
    }

    private List<Long> calculateNeedRefreshPartitions() throws AnalysisException {
        // check whether the user manually triggers it
        if (taskContext.getTriggerMode() == MTMVTaskTriggerMode.MANUAL) {
            if (taskContext.isComplete()) {
                return mtmv.getPartitionIds();
            } else if (!CollectionUtils
                    .isEmpty(taskContext.getPartitions())) {
                return MTMVUtil.getPartitionsIdsByNames(mtmv, taskContext.getPartitions());
            }
        }
        // check if data is fresh
        Set<String> excludedTriggerTables = mtmv.getExcludedTriggerTables();
        boolean fresh = MTMVUtil.isMTMVSync(mtmv, relation.getBaseTables(), excludedTriggerTables, 0L);
        if (fresh) {
            return Lists.newArrayList();
        }
        // current, if partitionType is SELF_MANAGE, we can only FULL refresh
        if (mtmv.getMvPartitionInfo().getPartitionType() == MTMVPartitionType.SELF_MANAGE) {
            return mtmv.getPartitionIds();
        }
        // if refreshMethod is COMPLETE, we only FULL refresh
        if (mtmv.getRefreshInfo().getRefreshMethod() == RefreshMethod.COMPLETE) {
            return mtmv.getPartitionIds();
        }
        return MTMVUtil.getMTMVNeedRefreshPartitions(mtmv);
    }

    public MTMVTaskContext getTaskContext() {
        return taskContext;
    }

    @Override
    public String toString() {
        return "MTMVTask{"
                + "dbId=" + dbId
                + ", mtmvId=" + mtmvId
                + ", taskContext=" + taskContext
                + ", needRefreshPartitions=" + needRefreshPartitions
                + ", completedPartitions=" + completedPartitions
                + ", refreshMode=" + refreshMode
                + ", mtmv=" + mtmv
                + ", relation=" + relation
                + ", executor=" + executor
                + "} " + super.toString();
    }
}
