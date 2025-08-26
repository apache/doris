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

import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.MetaLockUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.datasource.mvcc.MvccTableInfo;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVBaseTableIf;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVPartitionUtil;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshMethod;
import org.apache.doris.mtmv.MTMVRefreshPartitionSnapshot;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVSnapshotIf;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.UpdateMvUtils;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.qe.AuditLogHelper;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TStatusCode;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
        ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder<String, Integer>();
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

    private Map<MvccTableInfo, MvccSnapshot> snapshots = Maps.newHashMap();

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
            Set<TableIf> tablesInPlan = MTMVPlanUtil.getBaseTableFromQuery(mtmv.getQuerySql(), ctx);
            this.relation = MTMVPlanUtil.generateMTMVRelation(tablesInPlan, ctx);
            beforeMTMVRefresh();
            List<TableIf> tableIfs = Lists.newArrayList(tablesInPlan);
            tableIfs.sort(Comparator.comparing(TableIf::getId));

            MTMVRefreshContext context;
            Pair<List<String>, List<PartitionKeyDesc>> syncPartitions = null;
            // lock table order by id to avoid deadlock
            MetaLockUtils.readLockTables(tableIfs);
            try {
                // if mtmv is schema_change, check if column type has changed
                // If it's not in the schema_change state, the column type definitely won't change.
                if (MTMVState.SCHEMA_CHANGE.equals(mtmv.getStatus().getState())) {
                    checkColumnTypeIfChange(mtmv, ctx);
                }
                if (mtmv.getMvPartitionInfo().getPartitionType() != MTMVPartitionType.SELF_MANAGE) {
                    MTMVRelatedTableIf relatedTable = mtmv.getMvPartitionInfo().getRelatedTable();
                    if (!relatedTable.isValidRelatedTable()) {
                        throw new JobException("MTMV " + mtmv.getName() + "'s related table " + relatedTable.getName()
                                + " is not a valid related table anymore, stop refreshing."
                                + " e.g. Table has multiple partition columns"
                                + " or including not supported transform functions.");
                    }
                    syncPartitions = MTMVPartitionUtil.alignMvPartition(mtmv);
                }
            } finally {
                MetaLockUtils.readUnlockTables(tableIfs);
            }
            if (syncPartitions != null) {
                for (String pName : syncPartitions.first) {
                    MTMVPartitionUtil.dropPartition(mtmv, pName);
                }
                for (PartitionKeyDesc partitionKeyDesc : syncPartitions.second) {
                    MTMVPartitionUtil.addPartition(mtmv, partitionKeyDesc);
                }
            }
            this.partitionSnapshots = Maps.newConcurrentMap();
            MetaLockUtils.readLockTables(tableIfs);
            Map<String, MTMVRefreshPartitionSnapshot> execTableSnapshots = new HashMap<>();
            MTMVDataRefreshExec mtmvDataRefreshExec;
            try {
                context = MTMVRefreshContext.buildContext(mtmv);
                mtmvDataRefreshExec = getMTMVDataRefreshExec();
                mtmvDataRefreshExec.prepare(context, execTableSnapshots);
                Map<String, Set<String>> partitionMappings = context.getPartitionMappings();
                if (partitionMappings.isEmpty()) {
                    partitionSnapshots.putAll(execTableSnapshots);
                    return;
                }
                this.needRefreshPartitions = mtmvDataRefreshExec.calculateNeedRefreshPartitions(context);
            } finally {
                MetaLockUtils.readUnlockTables(tableIfs);
            }
            this.refreshMode = generateRefreshMode(needRefreshPartitions);
            if (refreshMode == MTMVTaskRefreshMode.NOT_REFRESH) {
                return;
            }
            Map<TableIf, String> tableWithPartKey = getIncrementalTableMap();
            this.completedPartitions = Lists.newCopyOnWriteArrayList();
            int refreshPartitionNum = mtmv.getRefreshPartitionNum();
            long execNum = (needRefreshPartitions.size() / refreshPartitionNum) + ((needRefreshPartitions.size()
                    % refreshPartitionNum) > 0 ? 1 : 0);
            for (int i = 0; i < execNum; i++) {
                int start = i * refreshPartitionNum;
                int end = start + refreshPartitionNum;
                Set<String> execPartitionNames = Sets.newHashSet(needRefreshPartitions
                        .subList(start, Math.min(end, needRefreshPartitions.size())));
                // need get names before exec
                if (!mtmv.getIncrementalRefresh()) {
                    execTableSnapshots = MTMVPartitionUtil
                            .generatePartitionSnapshots(context,
                                    relation.getBaseTablesOneLevel(), execPartitionNames);
                }
                try {
                    mtmvDataRefreshExec.executeWithRetry(execPartitionNames, tableWithPartKey);
                } catch (Exception e) {
                    LOG.error("Execution failed after retries: {}", e.getMessage());
                    throw new JobException(e.getMessage(), e);
                }
                completedPartitions.addAll(execPartitionNames);
                partitionSnapshots.putAll(execTableSnapshots);
            }
        } catch (Throwable e) {
            if (getStatus() == TaskStatus.RUNNING) {
                LOG.warn("run task failed: {}", e.getMessage());
                throw new JobException(e.getMessage(), e);
            } else {
                // if status is not `RUNNING`,maybe the task was canceled, therefore, it is a normal situation
                LOG.info("task [{}] interruption running, because status is [{}]", getTaskId(), getStatus());
            }
        }
    }

    public MTMVDataRefreshExec getMTMVDataRefreshExec() {
        if (mtmv.getIncrementalRefresh()) {
            return new MTMVSnapshotRefreshExec();
        } else {
            return new MTMVPartitionRefreshExec();
        }
    }

    public abstract class MTMVDataRefreshExec {

        protected void prepare(MTMVRefreshContext context,
                Map<String, MTMVRefreshPartitionSnapshot> execTableSnapshots) throws Exception {}

        private void executeWithRetry(Set<String> execPartitionNames, Map<TableIf, String> tableWithPartKey)
                throws Exception {
            int retryCount = 0;
            int retryTime = Config.max_query_retry_time;
            retryTime = retryTime <= 0 ? 1 : retryTime + 1;
            Exception lastException = null;
            while (retryCount < retryTime) {
                try {
                    exec(execPartitionNames, tableWithPartKey);
                    break; // Exit loop if execution is successful
                } catch (Exception e) {
                    if (!(Config.isCloudMode() && e.getMessage().contains(FeConstants.CLOUD_RETRY_E230))) {
                        throw e; // Re-throw if it's not a retryable exception
                    }
                    lastException = e;

                    int randomMillis = 10 + (int) (Math.random() * 10);
                    if (retryCount > retryTime / 2) {
                        randomMillis = 20 + (int) (Math.random() * 10);
                    }
                    if (DebugPointUtil.isEnable("MTMVTask.retry.longtime")) {
                        randomMillis = 1000;
                    }

                    retryCount++;
                    LOG.warn("Retrying execution due to exception: {}. Attempt {}/{}, "
                            + "taskId {} execPartitionNames {} lastQueryId {}, randomMillis {}",
                            e.getMessage(), retryCount, retryTime, getTaskId(),
                            execPartitionNames, lastQueryId, randomMillis);
                    if (retryCount >= retryTime) {
                        throw new Exception("Max retry attempts reached, original: " + lastException);
                    }
                    Thread.sleep(randomMillis);
                }
            }
        }

        private void exec(Set<String> refreshPartitionNames,
                Map<TableIf, String> tableWithPartKey)
                throws Exception {
            ConnectContext ctx = MTMVPlanUtil.createMTMVContext(mtmv);
            StatementContext statementContext = new StatementContext();
            for (Entry<MvccTableInfo, MvccSnapshot> entry : snapshots.entrySet()) {
                statementContext.setSnapshot(entry.getKey(), entry.getValue());
            }
            ctx.setStatementContext(statementContext);
            TUniqueId queryId = generateQueryId();
            lastQueryId = DebugUtil.printId(queryId);
            // if SELF_MANAGE mv, only have default partition,  will not have partitionItem, so we give empty set
            Command command = getDataRefreshCommand(refreshPartitionNames, tableWithPartKey);
            try {
                executor = new StmtExecutor(ctx, new LogicalPlanAdapter(command, ctx.getStatementContext()));
                ctx.setExecutor(executor);
                ctx.setQueryId(queryId);
                ctx.getState().setNereids(true);
                command.run(ctx, executor);
                if (getStatus() == TaskStatus.CANCELED) {
                    // Throwing an exception to interrupt subsequent partition update tasks
                    throw new JobException("task is CANCELED");
                }
                if (ctx.getState().getStateType() != MysqlStateType.OK) {
                    throw new JobException(ctx.getState().getErrorMessage());
                }
            } finally {
                if (executor != null) {
                    AuditLogHelper.logAuditLog(ctx, getDummyStmt(refreshPartitionNames),
                            executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog(),
                            true);
                }
            }
        }

        protected abstract Command getDataRefreshCommand(
                Set<String> refreshPartitionNames, Map<TableIf, String> tableWithPartKey) throws UserException;

        public List<String> calculateNeedRefreshPartitions(MTMVRefreshContext context)
                throws AnalysisException {
            List<String> needRefreshPartitionsByManual = calculateNeedRefreshPartitionsByManual(context);
            if (!needRefreshPartitionsByManual.isEmpty()) {
                return needRefreshPartitionsByManual;
            }
            return calculateNeedRefreshPartitionsInternal(context);
        }

        protected List<String> calculateNeedRefreshPartitionsInternal(MTMVRefreshContext context)
                throws AnalysisException {
            // if refreshMethod is COMPLETE, we must FULL refresh, avoid external table MTMV always not refresh
            if (mtmv.getRefreshInfo().getRefreshMethod() == RefreshMethod.COMPLETE) {
                return Lists.newArrayList(mtmv.getPartitionNames());
            }
            // check if data is fresh
            // We need to use a newly generated relationship and cannot retrieve it using mtmv.getRelation()
            // to avoid rebuilding the baseTable and causing a change in the tableId
            boolean fresh = MTMVPartitionUtil.isMTMVSync(context, relation.getBaseTablesOneLevel(),
                    mtmv.getExcludedTriggerTables());
            if (fresh) {
                return Lists.newArrayList();
            }
            // current, if partitionType is SELF_MANAGE, we can only FULL refresh
            if (mtmv.getMvPartitionInfo().getPartitionType() == MTMVPartitionType.SELF_MANAGE) {
                return Lists.newArrayList(mtmv.getPartitionNames());
            }
            // We need to use a newly generated relationship and cannot retrieve it using mtmv.getRelation()
            // to avoid rebuilding the baseTable and causing a change in the tableId
            return MTMVPartitionUtil.getMTMVNeedRefreshPartitions(context, relation.getBaseTablesOneLevel());
        }

        public List<String> calculateNeedRefreshPartitionsByManual(MTMVRefreshContext context) {
            // check whether the user manually triggers it
            if (taskContext.getTriggerMode() == MTMVTaskTriggerMode.MANUAL) {
                if (taskContext.isComplete()) {
                    return Lists.newArrayList(mtmv.getPartitionNames());
                } else if (!CollectionUtils.isEmpty(taskContext.getPartitions())) {
                    return taskContext.getPartitions();
                }
            }
            return Lists.newArrayList();
        }
    }

    private class MTMVPartitionRefreshExec extends MTMVDataRefreshExec {

        @Override
        protected Command getDataRefreshCommand(
                Set<String> refreshPartitionNames, Map<TableIf, String> tableWithPartKey) throws UserException {
            return UpdateMvUtils
                    .from(mtmv, mtmv.getMvPartitionInfo().getPartitionType() != MTMVPartitionType.SELF_MANAGE
                            ? refreshPartitionNames : Sets.newHashSet(), tableWithPartKey, false, new HashMap<>());
        }
    }

    private class MTMVSnapshotRefreshExec extends MTMVDataRefreshExec {
        private boolean incremental;
        private long tableSnapshotId;
        private long mvSnapshotId;
        private Map<String, String> params;
        private List<String> needRefreshPartitions;

        public void prepare(MTMVRefreshContext context,
                Map<String, MTMVRefreshPartitionSnapshot> execTableSnapshots) throws Exception {
            params = new HashMap<>();
            needRefreshPartitions = new ArrayList<>();
            MTMVSnapshotIf mvSnapshot = MaterializedViewUtils.getIncrementalMVSnapshotInfo(mtmv);
            mvSnapshotId = Optional.ofNullable(mvSnapshot).map(MTMVSnapshotIf::getSnapshotVersion).orElse(0L);
            MTMVExternalTableParamsAssembler paramsAssembler =
                    MTMVExternalTableParamsAssembler.getMTMVExternalTableParamsAssembler(mtmv);
            if (taskContext.getTriggerMode() == MTMVTaskTriggerMode.MANUAL
                    && (taskContext.isComplete() || !CollectionUtils.isEmpty(taskContext.getPartitions()))) {
                paramsAssembler.markReadBySnapshot(params, this.mvSnapshotId);
            } else {
                BaseTableInfo baseTableInfo = MaterializedViewUtils.getIncrementalMVBaseTable(mtmv);
                TableIf table = MTMVUtil.getTable(baseTableInfo);
                MvccTable mvccTable = (MvccTable) table;

                // The first data synchronization, based on a full update from the latest snapshot
                if (this.mvSnapshotId == 0L) {
                    MTMVSnapshotIf latestSnapshot =
                            MTMVPartitionUtil.getTableSnapshotFromContext((MTMVRelatedTableIf) table, context);
                    this.tableSnapshotId = latestSnapshot.getSnapshotVersion();
                    paramsAssembler.markReadBySnapshot(this.params, this.tableSnapshotId);
                } else {
                    Optional<MTMVExternalTableParamsAssembler.RefreshSnapshotInfo> refreshSnapshotInfoOpt =
                            paramsAssembler.calculateNextSnapshot(mvccTable, mvSnapshotId + 1, 0);
                    this.tableSnapshotId = this.mvSnapshotId;
                    if (refreshSnapshotInfoOpt.isPresent()) {
                        MTMVExternalTableParamsAssembler.RefreshSnapshotInfo refreshSnapshotInfo =
                                refreshSnapshotInfoOpt.get();
                        this.tableSnapshotId = refreshSnapshotInfo.getSnapshotId();
                        Map<String, Set<String>> partitionMappings = context.getPartitionMappings();
                        this.needRefreshPartitions = paramsAssembler.calculateNeedRefreshPartitions(
                                refreshSnapshotInfo, partitionMappings, mvccTable);
                    }
                    if (this.needRefreshPartitions.isEmpty()) {
                        paramsAssembler.markReadBySnapshotIncremental(
                                this.params, this.mvSnapshotId, this.tableSnapshotId);
                    } else {
                        paramsAssembler.markReadBySnapshot(this.params, this.tableSnapshotId);
                    }
                }
                MvccSnapshot mvccSnapshot = mvccTable.loadSnapshot(
                        Optional.of(TableSnapshot.versionOf(String.valueOf(this.tableSnapshotId))), Optional.empty());
                MTMVRefreshPartitionSnapshot refreshPartitionSnapshot =
                        MTMVPartitionUtil.generateIncrementalPartitionSnapshotsBySnapshotId(
                                mvccSnapshot, baseTableInfo, (MTMVRelatedTableIf) table);
                execTableSnapshots.put(mtmv.getName(), refreshPartitionSnapshot);
            }
            this.incremental = paramsAssembler.isIncremental();
        }

        @Override
        protected Command getDataRefreshCommand(
                Set<String> refreshPartitionNames, Map<TableIf, String> tableWithPartKey) throws UserException {
            if (incremental) {
                // do not add partition filter on insert SQL
                refreshPartitionNames = Sets.newHashSet();
            }
            return UpdateMvUtils
                    .from(mtmv, mtmv.getMvPartitionInfo().getPartitionType() != MTMVPartitionType.SELF_MANAGE
                            ? refreshPartitionNames : Sets.newHashSet(), tableWithPartKey, incremental, params);
        }

        public List<String> calculateNeedRefreshPartitionsInternal(MTMVRefreshContext context)
                throws AnalysisException {
            if (mvSnapshotId > 0 && mvSnapshotId >= tableSnapshotId) {
                return Collections.emptyList();
            }

            if (incremental) {
                return Collections.singletonList(mtmv.getName());
            } else if (!this.needRefreshPartitions.isEmpty()) {
                return this.needRefreshPartitions;
            }

            return super.calculateNeedRefreshPartitionsInternal(context);
        }
    }

    private void checkColumnTypeIfChange(MTMV mtmv, ConnectContext ctx) throws JobException {
        List<ColumnDefinition> currentColumnsDefinition = MTMVPlanUtil.generateColumnsBySql(mtmv.getQuerySql(), ctx,
                mtmv.getMvPartitionInfo().getPartitionCol(),
                mtmv.getDistributionColumnNames(), null, mtmv.getTableProperty().getProperties());
        List<Column> currentColumns = currentColumnsDefinition.stream()
                .map(ColumnDefinition::translateToCatalogStyle)
                .collect(Collectors.toList());
        List<Column> originalColumns = mtmv.getBaseSchema(true);
        if (currentColumns.size() != originalColumns.size()) {
            throw new JobException(String.format(
                    "column length not equals, please check whether columns of base table have changed, "
                            + "original length is: %s, current length is: %s",
                    originalColumns.size(), currentColumns.size()));
        }
        for (int i = 0; i < originalColumns.size(); i++) {
            if (!isTypeLike(originalColumns.get(i).getType(), currentColumns.get(i).getType())) {
                throw new JobException(String.format(
                        "column type not same, please check whether columns of base table have changed, "
                                + "column name is: %s, original type is: %s, current type is: %s",
                        originalColumns.get(i).getName(), originalColumns.get(i).getType().toSql(),
                        currentColumns.get(i).getType().toSql()));
            }
        }
    }

    private boolean isTypeLike(Type type, Type typeOther) {
        if (type.isStringType()) {
            return typeOther.isStringType();
        } else {
            return type.equals(typeOther);
        }
    }

    private String getDummyStmt(Set<String> refreshPartitionNames) {
        return String.format(
                "Asynchronous materialized view refresh task, mvName: %s,"
                        + "taskId: %s, partitions refreshed by this insert overwrite: %s",
                mtmv.getName(), super.getTaskId(), refreshPartitionNames);
    }

    @Override
    public synchronized boolean onFail() throws JobException {
        LOG.info("mtmv task onFail, taskId: {}", super.getTaskId());
        boolean res = super.onFail();
        if (!res) {
            return false;
        }
        after();
        return true;
    }

    @Override
    public synchronized boolean onSuccess() throws JobException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("mtmv task onSuccess, taskId: {}", super.getTaskId());
        }
        boolean res = super.onSuccess();
        if (!res) {
            return false;
        }
        after();
        return true;
    }

    /**
     * The reason for overriding the parent class is to add synchronized protection
     */
    @Override
    public synchronized boolean cancel(boolean needWaitCancelComplete) throws JobException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("mtmv task cancel, taskId: {}", super.getTaskId());
        }
        return super.cancel(needWaitCancelComplete);
    }

    @Override
    protected void executeCancelLogic(boolean needWaitCancelComplete) {
        try {
            // Mtmv is initialized in the before method.
            // If the task has not yet run, the before method will not be used, so mtmv will be empty,
            // which prevents the canceled task from being added to the history list
            if (mtmv == null) {
                mtmv = MTMVUtil.getMTMV(dbId, mtmvId);
            }
        } catch (UserException e) {
            LOG.warn("executeCancelLogic failed:", e);
            return;
        }
        if (executor != null) {
            executor.cancel(new Status(TStatusCode.CANCELLED, "mtmv task cancelled"), needWaitCancelComplete);
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
     * Do something before refreshing, such as clearing the cache of the external table
     *
     * @throws AnalysisException
     * @throws DdlException
     */
    private void beforeMTMVRefresh() throws AnalysisException, DdlException {
        for (BaseTableInfo tableInfo : relation.getBaseTablesOneLevel()) {
            TableIf tableIf = MTMVUtil.getTable(tableInfo);
            if (tableIf instanceof MTMVBaseTableIf) {
                MTMVBaseTableIf baseTableIf = (MTMVBaseTableIf) tableIf;
                baseTableIf.beforeMTMVRefresh(mtmv);
            }
            if (tableIf instanceof MvccTable) {
                MvccTable mvccTable = (MvccTable) tableIf;
                MvccSnapshot mvccSnapshot = mvccTable.loadSnapshot(Optional.empty(), null);
                snapshots.put(new MvccTableInfo(mvccTable), mvccSnapshot);
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
        if (null != snapshots) {
            snapshots = null;
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
