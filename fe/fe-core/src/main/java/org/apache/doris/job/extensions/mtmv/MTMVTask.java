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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVPartitionUtil;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshMethod;
import org.apache.doris.mtmv.MTMVRefreshPartitionSnapshot;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.nereids.StatementContext;
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
            new Column("Progress", ScalarType.createStringType()),
            new Column("LastQueryId", ScalarType.createStringType()));

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
        COMMIT,
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
    @SerializedName("lastQueryId")
    String lastQueryId;

    private MTMV mtmv;
    private MTMVRelation relation;
    private StmtExecutor executor;
    private Map<String, MTMVRefreshPartitionSnapshot> partitionSnapshots;

    public MTMVTask() {
    }

    public MTMVTask(long dbId, long mtmvId, MTMVTaskContext taskContext) {
        this.dbId = Objects.requireNonNull(dbId);
        this.mtmvId = Objects.requireNonNull(mtmvId);
        this.taskContext = Objects.requireNonNull(taskContext);
    }

    // only for test
    public MTMVTask(MTMV mtmv, MTMVRelation relation, MTMVTaskContext taskContext) {
        this.mtmv = mtmv;
        this.relation = relation;
        this.taskContext = taskContext;
    }

    @Override
    public void run() throws JobException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("mtmv task run, taskId: {}", super.getTaskId());
        }
        ConnectContext ctx = MTMVPlanUtil.createMTMVContext(mtmv);
        try {
            if (LOG.isDebugEnabled()) {
                String taskSessionContext = ctx.getSessionVariable().toJson().toJSONString();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("mtmv task session variable, taskId: {}, session: {}",
                            super.getTaskId(), taskSessionContext);
                }
            }
            // Every time a task is run, the relation is regenerated because baseTables and baseViews may change,
            // such as deleting a table and creating a view with the same name
            this.relation = MTMVPlanUtil.generateMTMVRelation(mtmv, ctx);
            // Now, the MTMV first ensures consistency with the data in the cache.
            // To be completely consistent with hive, you need to manually refresh the cache
            // refreshHmsTable();
            if (mtmv.getMvPartitionInfo().getPartitionType() != MTMVPartitionType.SELF_MANAGE) {
                MTMVPartitionUtil.alignMvPartition(mtmv);
            }
            Map<String, Set<String>> partitionMappings = mtmv.calculatePartitionMappings();
            this.needRefreshPartitions = calculateNeedRefreshPartitions(partitionMappings);
            this.refreshMode = generateRefreshMode(needRefreshPartitions);
            if (refreshMode == MTMVTaskRefreshMode.NOT_REFRESH) {
                return;
            }
            Map<TableIf, String> tableWithPartKey = getIncrementalTableMap();
            this.completedPartitions = Lists.newCopyOnWriteArrayList();
            int refreshPartitionNum = mtmv.getRefreshPartitionNum();
            long execNum = (needRefreshPartitions.size() / refreshPartitionNum) + ((needRefreshPartitions.size()
                    % refreshPartitionNum) > 0 ? 1 : 0);
            this.partitionSnapshots = Maps.newConcurrentMap();
            for (int i = 0; i < execNum; i++) {
                int start = i * refreshPartitionNum;
                int end = start + refreshPartitionNum;
                Set<String> execPartitionNames = Sets.newHashSet(needRefreshPartitions
                        .subList(start, end > needRefreshPartitions.size() ? needRefreshPartitions.size() : end));
                // need get names before exec
                Map<String, MTMVRefreshPartitionSnapshot> execPartitionSnapshots = MTMVPartitionUtil
                        .generatePartitionSnapshots(mtmv, relation.getBaseTables(), execPartitionNames,
                                partitionMappings);
                exec(ctx, execPartitionNames, tableWithPartKey);
                completedPartitions.addAll(execPartitionNames);
                partitionSnapshots.putAll(execPartitionSnapshots);
            }
        } catch (Throwable e) {
            if (getStatus() == TaskStatus.RUNNING) {
                StringBuilder errMsg = new StringBuilder();
                // when env ctl/db not exist, need give client tips
                Pair<Boolean, String> pair = MTMVPlanUtil.checkEnvInfo(mtmv.getEnvInfo(), ctx);
                if (!pair.first) {
                    errMsg.append(pair.second);
                }
                errMsg.append(e.getMessage());
                LOG.warn("run task failed: ", errMsg.toString());
                throw new JobException(errMsg.toString(), e);
            } else {
                // if status is not `RUNNING`,maybe the task was canceled, therefore, it is a normal situation
                LOG.info("task [{}] interruption running, because status is [{}]", getTaskId(), getStatus());
            }
        }
    }

    private void exec(ConnectContext ctx, Set<String> refreshPartitionNames,
            Map<TableIf, String> tableWithPartKey)
            throws Exception {
        Objects.requireNonNull(ctx, "ctx should not be null");
        StatementContext statementContext = new StatementContext();
        ctx.setStatementContext(statementContext);
        TUniqueId queryId = generateQueryId();
        lastQueryId = DebugUtil.printId(queryId);
        // if SELF_MANAGE mv, only have default partition,  will not have partitionItem, so we give empty set
        UpdateMvByPartitionCommand command = UpdateMvByPartitionCommand
                .from(mtmv, mtmv.getMvPartitionInfo().getPartitionType() != MTMVPartitionType.SELF_MANAGE
                        ? refreshPartitionNames : Sets.newHashSet(), tableWithPartKey);
        executor = new StmtExecutor(ctx, new LogicalPlanAdapter(command, ctx.getStatementContext()));
        ctx.setExecutor(executor);
        ctx.setQueryId(queryId);
        ctx.getState().setNereids(true);
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("mtmv task onSuccess, taskId: {}", super.getTaskId());
        }
        super.onSuccess();
        after();
    }

    @Override
    protected synchronized void executeCancelLogic() {
        LOG.info("mtmv task cancel, taskId: {}", super.getTaskId());
        if (executor != null) {
            executor.cancel();
        }
        after();
    }

    @Override
    public void before() throws JobException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("mtmv task before, taskId: {}", super.getTaskId());
        }
        super.before();
        try {
            mtmv = MTMVUtil.getMTMV(dbId, mtmvId);
        } catch (UserException e) {
            LOG.warn("before task failed:", e);
            throw new JobException(e);
        }
    }

    /**
     * // Before obtaining information from hmsTable, refresh to ensure that the data is up-to-date
     *
     * @throws AnalysisException
     * @throws DdlException
     */
    private void refreshHmsTable() throws AnalysisException, DdlException {
        for (BaseTableInfo tableInfo : relation.getBaseTables()) {
            TableIf tableIf = MTMVUtil.getTable(tableInfo);
            if (tableIf instanceof HMSExternalTable) {
                HMSExternalTable hmsTable = (HMSExternalTable) tableIf;
                Env.getCurrentEnv().getRefreshManager()
                        .refreshTable(hmsTable.getCatalog().getName(), hmsTable.getDbName(), hmsTable.getName(), true);
            }

        }
    }

    @Override
    public void runTask() throws JobException {
        LOG.info("mtmv task runTask, taskId: {}", super.getTaskId());
        MTMVJob job = (MTMVJob) getJobOrJobException();
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("mtmv task get writeLock start, taskId: {}", super.getTaskId());
            }
            job.writeLock();
            if (LOG.isDebugEnabled()) {
                LOG.debug("mtmv task get writeLock end, taskId: {}", super.getTaskId());
            }
            super.runTask();
        } finally {
            job.writeUnlock();
            if (LOG.isDebugEnabled()) {
                LOG.debug("mtmv task release writeLock, taskId: {}", super.getTaskId());
            }
        }
    }

    @Override
    public TRow getTvfInfo(String jobName) {
        TRow trow = new TRow();
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(super.getTaskId())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(super.getJobId())));
        trow.addToColumnValue(new TCell().setStringVal(jobName));
        String dbName = "";
        String mvName = "";
        try {
            MTMV mtmv = MTMVUtil.getMTMV(dbId, mtmvId);
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
        trow.addToColumnValue(
                new TCell().setStringVal(lastQueryId));
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
                    .addMTMVTaskResult(new TableNameInfo(mtmv.getQualifiedDbName(), mtmv.getName()), this, relation,
                            partitionSnapshots);
        }

    }

    @Override
    protected void closeOrReleaseResources() {
        if (null != mtmv) {
            mtmv = null;
        }
        if (null != executor) {
            executor = null;
        }
        if (null != relation) {
            relation = null;
        }
        if (null != partitionSnapshots) {
            partitionSnapshots = null;
        }
    }

    private Map<TableIf, String> getIncrementalTableMap() throws AnalysisException {
        Map<TableIf, String> tableWithPartKey = Maps.newHashMap();
        if (mtmv.getMvPartitionInfo().getPartitionType() != MTMVPartitionType.SELF_MANAGE) {
            tableWithPartKey
                    .put(mtmv.getMvPartitionInfo().getRelatedTable(), mtmv.getMvPartitionInfo().getRelatedCol());
        }
        return tableWithPartKey;
    }

    private MTMVTaskRefreshMode generateRefreshMode(List<String> needRefreshPartitionIds) {
        if (CollectionUtils.isEmpty(needRefreshPartitionIds)) {
            return MTMVTaskRefreshMode.NOT_REFRESH;
        } else if (needRefreshPartitionIds.size() == mtmv.getPartitionNames().size()) {
            return MTMVTaskRefreshMode.COMPLETE;
        } else {
            return MTMVTaskRefreshMode.PARTIAL;
        }
    }

    public List<String> calculateNeedRefreshPartitions(Map<String, Set<String>> partitionMappings)
            throws AnalysisException {
        // check whether the user manually triggers it
        if (taskContext.getTriggerMode() == MTMVTaskTriggerMode.MANUAL) {
            if (taskContext.isComplete()) {
                return Lists.newArrayList(mtmv.getPartitionNames());
            } else if (!CollectionUtils
                    .isEmpty(taskContext.getPartitions())) {
                return taskContext.getPartitions();
            }
        }
        // if refreshMethod is COMPLETE, we must FULL refresh, avoid external table MTMV always not refresh
        if (mtmv.getRefreshInfo().getRefreshMethod() == RefreshMethod.COMPLETE) {
            return Lists.newArrayList(mtmv.getPartitionNames());
        }
        // check if data is fresh
        // We need to use a newly generated relationship and cannot retrieve it using mtmv.getRelation()
        // to avoid rebuilding the baseTable and causing a change in the tableId
        boolean fresh = MTMVPartitionUtil.isMTMVSync(mtmv, relation.getBaseTables(), mtmv.getExcludedTriggerTables(),
                partitionMappings);
        if (fresh) {
            return Lists.newArrayList();
        }
        // current, if partitionType is SELF_MANAGE, we can only FULL refresh
        if (mtmv.getMvPartitionInfo().getPartitionType() == MTMVPartitionType.SELF_MANAGE) {
            return Lists.newArrayList(mtmv.getPartitionNames());
        }
        // We need to use a newly generated relationship and cannot retrieve it using mtmv.getRelation()
        // to avoid rebuilding the baseTable and causing a change in the tableId
        return MTMVPartitionUtil.getMTMVNeedRefreshPartitions(mtmv, relation.getBaseTables(), partitionMappings);
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
                + "} " + super.toString();
    }
}
