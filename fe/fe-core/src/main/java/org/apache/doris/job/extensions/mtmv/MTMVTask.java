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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.catalog.stream.StreamReadMode;
import org.apache.doris.cloud.qe.ComputeGroupException;
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
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.datasource.mvcc.MvccTableInfo;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mtmv.BaseColInfo;
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
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.mtmv.ivm.IvmFailureReason;
import org.apache.doris.mtmv.ivm.IvmPlanSignature;
import org.apache.doris.mtmv.ivm.IvmIncrRefreshContext;
import org.apache.doris.mtmv.ivm.IvmIncrRefreshManager;
import org.apache.doris.mtmv.ivm.IvmIncrRefreshResult;
import org.apache.doris.mtmv.ivm.IvmRewriteContext;
import org.apache.doris.mtmv.ivm.IvmRewriteResult;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.commands.UpdateMvByPartitionCommand;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo.RefreshMode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;

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
            new Column("LastQueryId", ScalarType.createStringType()),
            new Column("ComputeGroup", ScalarType.createStringType()),
            new Column("IvmFallbackReason", ScalarType.createStringType()));

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

    private enum RefreshAttemptType {
        IVM,
        PARTITIONS,
        COMPLETE
    }

    private enum AttemptResultType {
        SUCCESS,
        // The current attempt failed before writing MV data, so the task may
        // continue to the next configured fallback attempt.
        FALLBACK_ALLOWED,
        // A previous IVM delta may have partially written data. PARTITIONS
        // cannot prove it repairs that state, so recovery must be COMPLETE.
        FALLBACK_TO_COMPLETE
    }

    private static class RefreshRequest {
        private final RefreshMode refreshMode;
        private final boolean allowFallback;
        private final List<String> partitions;
        // True only for REFRESH ... PARTITION(S). Explicit partition refresh is
        // a user-selected scope and must not expand to COMPLETE via fallback.
        private final boolean explicitPartitions;

        private RefreshRequest(RefreshMode refreshMode, boolean allowFallback,
                List<String> partitions, boolean explicitPartitions) {
            this.refreshMode = Objects.requireNonNull(refreshMode, "refreshMode can not be null");
            this.allowFallback = allowFallback;
            this.partitions = partitions == null ? Lists.newArrayList() : partitions;
            this.explicitPartitions = explicitPartitions;
        }
    }

    private static class PartitionRefreshPlan {
        private final MTMVRefreshContext context;
        // False means partition planning failed before any refresh write. The
        // caller may convert it to COMPLETE only when the request allows fallback.
        private final boolean canRefreshByPartitions;
        private final List<String> partitions;
        private final String fallbackReason;

        private PartitionRefreshPlan(MTMVRefreshContext context, boolean canRefreshByPartitions,
                List<String> partitions, String fallbackReason) {
            this.context = context;
            this.canRefreshByPartitions = canRefreshByPartitions;
            this.partitions = partitions == null ? Lists.newArrayList() : partitions;
            this.fallbackReason = fallbackReason;
        }

        private static PartitionRefreshPlan success(MTMVRefreshContext context, List<String> partitions) {
            return new PartitionRefreshPlan(context, true, partitions, null);
        }

        private static PartitionRefreshPlan fallback(String fallbackReason) {
            return new PartitionRefreshPlan(null, false, Lists.newArrayList(), fallbackReason);
        }
    }

    private static class PartitionPlanningException extends Exception {
        private PartitionPlanningException(String message) {
            super(message);
        }

        private PartitionPlanningException(String message, Throwable cause) {
            super(message, cause);
        }
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
    // Persisted for SHOW MTMV TASK diagnostics. It records the IVM pre-execution
    // reason that caused fallback, or the hard failure reason from IVM execution.
    @SerializedName("ifr")
    private String ivmFallbackReason;
    @SerializedName("cg")
    private String computeGroup;
    private MTMV mtmv;
    private MTMVRelation relation;
    private StmtExecutor executor;
    private Map<String, MTMVRefreshPartitionSnapshot> partitionSnapshots;
    private long mtmvSchemaChangeVersion;

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
        mtmvSchemaChangeVersion = mtmv.getSchemaChangeVersion();
        ConnectContext ctx = MTMVPlanUtil.createMTMVContext(mtmv, MTMVPlanUtil.DISABLE_RULES_WHEN_RUN_MTMV_TASK);
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
            Pair<Set<TableIf>, Set<TableIf>> tablesInPlan = MTMVPlanUtil.getBaseTableFromQuery(mtmv.getQuerySql(), ctx);
            this.relation = MTMVPlanUtil.generateMTMVRelation(tablesInPlan.first, tablesInPlan.second);
            beforeMTMVRefresh();
            List<TableIf> tableIfs = Lists.newArrayList(tablesInPlan.first);
            tableIfs.sort(Comparator.comparing(TableIf::getId));

            // This checks whether an MV in SCHEMA_CHANGE state still matches
            // its base-table schema and partition definition. It is not part of
            // refresh fallback: incompatible MV definitions must fail directly.
            ensureQueryUsableIfNeeded(ctx, tableIfs);
            RefreshRequest request = resolveRefreshRequest();
            List<RefreshAttemptType> attempts = buildAttempts(request);
            try {
                syncPartitionsIfNeeded(ctx, tableIfs);
            } catch (PartitionPlanningException e) {
                throw new JobException(e.getMessage(), e);
            }
            MTMVRefreshContext refreshContext = buildRefreshContext(tableIfs);
            boolean disablePartitionRefresh = false;
            for (RefreshAttemptType attemptType : attempts) {
                switch (attemptType) {
                    case IVM:
                        AttemptResultType ivmResult = executeIvmAttempt(refreshContext, request);
                        if (ivmResult == AttemptResultType.SUCCESS) {
                            return;
                        }
                        if (ivmResult == AttemptResultType.FALLBACK_TO_COMPLETE) {
                            disablePartitionRefresh = true;
                        }
                        break;
                    case PARTITIONS:
                        if (disablePartitionRefresh) {
                            break;
                        }
                        if (executePartitionBasedRefresh(refreshContext, request)) {
                            return;
                        }
                        break;
                    case COMPLETE:
                        executeCompleteAttempt(refreshContext);
                        return;
                    default:
                        throw new JobException("Unsupported refresh attempt type: " + attemptType);
                }
            }
            throw new JobException("No refresh attempt succeeded for mv=" + mtmv.getName());
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

    private void ensureQueryUsableIfNeeded(ConnectContext ctx, List<TableIf> tableIfs)
            throws JobException, AnalysisException {
        MetaLockUtils.readLockTables(tableIfs);
        try {
            if (MTMVState.SCHEMA_CHANGE.equals(mtmv.getStatus().getState())) {
                MTMVPlanUtil.ensureMTMVQueryUsable(mtmv, ctx);
            }
        } finally {
            MetaLockUtils.readUnlockTables(tableIfs);
        }
    }

    private void syncPartitionsIfNeeded(ConnectContext ctx, List<TableIf> tableIfs)
            throws JobException, AnalysisException, DdlException, PartitionPlanningException {
        Pair<List<String>, List<PartitionKeyDesc>> syncPartitions = null;
        // lock table order by id to avoid deadlock
        MetaLockUtils.readLockTables(tableIfs);
        try {
            if (mtmv.getMvPartitionInfo().getPartitionType() != MTMVPartitionType.SELF_MANAGE) {
                Set<MTMVRelatedTableIf> pctTables = mtmv.getMvPartitionInfo().getPctTables();
                for (MTMVRelatedTableIf pctTable : pctTables) {
                    if (!pctTable.isValidRelatedTable()) {
                        throw new PartitionPlanningException("MTMV " + mtmv.getName()
                                + "'s pct table " + pctTable.getName()
                                + " is not a valid pct table anymore, stop refreshing."
                                + " e.g. Table has multiple partition columns"
                                + " or including not supported transform functions.");
                    }
                }
                try {
                    syncPartitions = MTMVPartitionUtil.alignMvPartition(mtmv);
                } catch (Exception e) {
                    throw new PartitionPlanningException(e.getMessage(), e);
                }
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
    }

    private RefreshRequest resolveRefreshRequest() throws JobException {
        if (taskContext.useMvDefaultRefreshPolicy()) {
            // Scheduled/on-commit/system tasks use the policy persisted on the
            // MV, not the default AUTO value of a newly created task context.
            RefreshMethod refreshMethod = mtmv.getRefreshInfo().getRefreshMethod();
            if (refreshMethod == null) {
                throw new JobException("MTMV " + mtmv.getName()
                        + " has unknown refresh method, please refresh or recreate it.");
            }
            return new RefreshRequest(RefreshMode.valueOf(refreshMethod.name()),
                    mtmv.getRefreshInfo().allowFallback(), Lists.newArrayList(), false);
        }
        if (!CollectionUtils.isEmpty(taskContext.getPartitions())) {
            // A partitionSpec is an exact manual request. It never falls back to
            // COMPLETE because that would refresh more data than the user asked.
            return new RefreshRequest(RefreshMode.PARTITIONS, false, taskContext.getPartitions(), true);
        }
        return new RefreshRequest(taskContext.getRefreshMode(), taskContext.allowFallback(),
                Lists.newArrayList(), false);
    }

    private List<RefreshAttemptType> buildAttempts(RefreshRequest request) {
        if (taskContext.getTriggerMode() != MTMVTaskTriggerMode.MANUAL
                && mtmv.isIvm() && !mtmv.hasRefreshSnapshot()) {
            return Lists.newArrayList(RefreshAttemptType.COMPLETE);
        }
        List<RefreshAttemptType> attempts = Lists.newArrayList();
        switch (request.refreshMode) {
            case AUTO:
                if (mtmv.isIvm()) {
                    attempts.add(RefreshAttemptType.IVM);
                }
                // AUTO always has the full fallback chain. If the MV was created
                // as non-IVM, it starts from PARTITIONS and may end at COMPLETE.
                attempts.add(RefreshAttemptType.PARTITIONS);
                attempts.add(RefreshAttemptType.COMPLETE);
                break;
            case INCREMENTAL:
                attempts.add(RefreshAttemptType.IVM);
                if (request.allowFallback) {
                    attempts.add(RefreshAttemptType.PARTITIONS);
                    attempts.add(RefreshAttemptType.COMPLETE);
                }
                break;
            case PARTITIONS:
                attempts.add(RefreshAttemptType.PARTITIONS);
                if (!request.explicitPartitions && request.allowFallback) {
                    attempts.add(RefreshAttemptType.COMPLETE);
                }
                break;
            case COMPLETE:
                attempts.add(RefreshAttemptType.COMPLETE);
                break;
            default:
                throw new IllegalStateException("Unsupported refresh mode: " + request.refreshMode);
        }
        return attempts;
    }

    private PartitionRefreshPlan planPartitionRefresh(MTMVRefreshContext context,
            RefreshRequest request) throws AnalysisException {
        if (request.explicitPartitions) {
            return PartitionRefreshPlan.success(context, request.partitions);
        }
        if (mtmv.getMvPartitionInfo().getPartitionType() == MTMVPartitionType.SELF_MANAGE) {
            // Keep this inside the PARTITIONS attempt so PARTITIONS FALLBACK and
            // AUTO can still continue to COMPLETE for non-partitioned MVs.
            return PartitionRefreshPlan.fallback(
                    "The partition method of this asynchronous materialized view "
                            + "does not support refreshing by partition");
        }
        boolean fresh;
        try {
            fresh = MTMVPartitionUtil.isMTMVSync(context, relation.getBaseTablesOneLevelAndFromView(),
                    mtmv.getExcludedTriggerTables());
        } catch (Exception e) {
            return PartitionRefreshPlan.fallback(e.getMessage());
        }
        if (fresh) {
            return PartitionRefreshPlan.success(context, Lists.newArrayList());
        }
        try {
            return PartitionRefreshPlan.success(context,
                    MTMVPartitionUtil.getMTMVNeedRefreshPartitions(context,
                            relation.getBaseTablesOneLevelAndFromView()));
        } catch (Exception e) {
            return PartitionRefreshPlan.fallback(e.getMessage());
        }
    }

    private MTMVRefreshContext buildRefreshContext(List<TableIf> tableIfs) throws AnalysisException {
        MetaLockUtils.readLockTables(tableIfs);
        try {
            return MTMVRefreshContext.buildContext(mtmv);
        } finally {
            MetaLockUtils.readUnlockTables(tableIfs);
        }
    }

    private void executeCompleteAttempt(MTMVRefreshContext context)
            throws JobException, AnalysisException {
        this.needRefreshPartitions = Lists.newArrayList(mtmv.getPartitionNames());
        this.refreshMode = generateRefreshMode(needRefreshPartitions);
        if (refreshMode == MTMVTaskRefreshMode.NOT_REFRESH) {
            return;
        }
        executePartitionBasedRefresh(context, RefreshMode.COMPLETE);
    }

    private AttemptResultType executeIvmAttempt(MTMVRefreshContext refreshContext,
            RefreshRequest request) throws JobException {
        if (!mtmv.isIvm()) {
            throw new JobException("Cannot use " + request.refreshMode
                    + " refresh on a materialized view without INCREMENTAL capability.");
        }
        // A strict INCREMENTAL request must reach IVM so the stream can establish its initial baseline.
        // Only fallback-enabled requests may rebuild a missing or invalidated snapshot with COMPLETE.
        if (!mtmv.hasRefreshSnapshot() && request.allowFallback) {
            ivmFallbackReason = "INCOMPLETE_REFRESH_SNAPSHOT";
            LOG.warn("IVM refresh fell back for mv={}, reason=INCOMPLETE_REFRESH_SNAPSHOT, taskId={}. "
                    + "Continuing with COMPLETE refresh.", mtmv.getName(), getTaskId());
            return AttemptResultType.FALLBACK_TO_COMPLETE;
        }
        this.completedPartitions = Lists.newCopyOnWriteArrayList();
        this.partitionSnapshots = Maps.newConcurrentMap();
        // Determine which partitions need refresh, same as partition-based flow.
        this.needRefreshPartitions = MTMVPartitionUtil.getMTMVNeedRefreshPartitions(refreshContext,
                relation.getBaseTablesOneLevelAndFromView());
        if (mtmv.getIvmInfo().isBinlogBroken()) {
            return handleIvmFallbackResult(IvmIncrRefreshResult.fallback(
                    IvmFailureReason.BINLOG_BROKEN, "Stream binlog is marked as broken"), request);
        }
        if (CollectionUtils.isEmpty(needRefreshPartitions)) {
            LOG.info("IVM incremental refresh skipped for mv={}: all partitions are synced, taskId={}",
                    mtmv.getName(), getTaskId());
            return AttemptResultType.SUCCESS;
        }
        IvmIncrRefreshManager ivmIncrRefreshManager = new IvmIncrRefreshManager();
        // Capture base table snapshots under read lock before execution, same as
        // partition-based refresh. This ensures snapshot versions are consistent
        // with the data the INSERT will read.
        Map<String, MTMVRefreshPartitionSnapshot> capturedSnapshots;
        try {
            capturedSnapshots = MTMVPartitionUtil.generatePartitionSnapshots(
                    refreshContext, relation.getBaseTablesOneLevelAndFromView(),
                    Sets.newHashSet(needRefreshPartitions));
        } catch (Exception e) {
            throw new JobException("IVM snapshot generation failed for mv=" + mtmv.getName(), e);
        }
        IvmIncrRefreshResult ivmResult;
        try {
            ivmResult = executeWithRetry(() -> {
                ConnectContext ivmConnectContext = MTMVPlanUtil.createMTMVContext(mtmv,
                        MTMVPlanUtil.DISABLE_RULES_WHEN_RUN_MTMV_TASK);
                setupComputeGroup(ivmConnectContext);
                IvmIncrRefreshContext ivmIncrRefreshContext = new IvmIncrRefreshContext(mtmv,
                        ivmConnectContext,
                        getRefreshAuditStmt(RefreshMode.INCREMENTAL, Sets.newHashSet(needRefreshPartitions)),
                        this::recordQueryId);
                return ivmIncrRefreshManager.doRefresh(ivmIncrRefreshContext);
            }, "IVM refresh");
        } catch (Exception e) {
            throw new JobException("IVM incremental refresh failed for mv=" + mtmv.getName()
                    + ", detail=" + Util.getRootCauseMessage(e), e);
        }
        if (ivmResult.isSuccess()) {
            this.partitionSnapshots.putAll(capturedSnapshots);
            this.completedPartitions.addAll(needRefreshPartitions);
            LOG.info("IVM incremental refresh succeeded for mv={}, taskId={}",
                    mtmv.getName(), getTaskId());
            return AttemptResultType.SUCCESS;
        }
        return handleIvmFallbackResult(ivmResult, request);
    }

    private AttemptResultType handleIvmFallbackResult(IvmIncrRefreshResult ivmResult, RefreshRequest request)
            throws JobException {
        ivmFallbackReason = ivmResult.getFailureReason().name();
        if (!request.allowFallback) {
            throw new JobException(
                    "IVM incremental refresh failed for mv=" + mtmv.getName()
                    + ", reason=" + ivmResult.getFailureReason()
                    + ", detail=" + ivmResult.getDetailMessage());
        }
        if (ivmResult.getFailureReason() == IvmFailureReason.BINLOG_BROKEN) {
            // The previous task already entered the IVM execution phase. If
            // fallback is allowed, jump directly to COMPLETE recovery instead of
            // trying PARTITIONS first.
            LOG.warn("IVM previous run incomplete for mv={}, taskId={}. Continuing with COMPLETE recovery.",
                    mtmv.getName(), getTaskId());
            return AttemptResultType.FALLBACK_TO_COMPLETE;
        }
        if (ivmResult.getFailureReason().requiresCompleteRefresh()) {
            LOG.warn("IVM refresh fell back for mv={}, reason={}, detail={}, taskId={}. "
                    + "Continuing with COMPLETE refresh.",
                    mtmv.getName(), ivmResult.getFailureReason(),
                    ivmResult.getDetailMessage(), getTaskId());
            return AttemptResultType.FALLBACK_TO_COMPLETE;
        }
        LOG.warn("IVM refresh fell back for mv={}, reason={}, detail={}, taskId={}. "
                + "Continuing with partition-based refresh.",
                mtmv.getName(), ivmResult.getFailureReason(),
                ivmResult.getDetailMessage(), getTaskId());
        return AttemptResultType.FALLBACK_ALLOWED;
    }

    private boolean executePartitionBasedRefresh(MTMVRefreshContext refreshContext,
            RefreshRequest request) throws JobException, AnalysisException {
        PartitionRefreshPlan partitionPlan = planPartitionRefresh(refreshContext, request);
        if (!partitionPlan.canRefreshByPartitions) {
            if (request.allowFallback) {
                LOG.warn("MTMV partition refresh fell back for mv={}, reason={}, taskId={}",
                        mtmv.getName(), partitionPlan.fallbackReason, getTaskId());
                return false;
            }
            throw new JobException(partitionPlan.fallbackReason);
        }
        this.needRefreshPartitions = partitionPlan.partitions;
        this.refreshMode = generateRefreshMode(needRefreshPartitions);
        if (refreshMode == MTMVTaskRefreshMode.NOT_REFRESH) {
            return true;
        }
        executePartitionBasedRefresh(partitionPlan.context, RefreshMode.PARTITIONS);
        return true;
    }

    private void executePartitionBasedRefresh(MTMVRefreshContext context, RefreshMode refreshMode)
            throws JobException, AnalysisException {
        boolean useIvmFallbackStreams = mtmv.isIvm();
        Map<TableIf, String> tableWithPartKey = getIncrementalTableMap();
        long baselineGeneration = useIvmFallbackStreams
                ? IvmIncrRefreshManager.markIvmBaselineBroken(mtmv) : -1;
        this.completedPartitions = Lists.newCopyOnWriteArrayList();
        int refreshPartitionNum = mtmv.getRefreshPartitionNum();
        long execNum = (needRefreshPartitions.size() / refreshPartitionNum) + ((needRefreshPartitions.size()
                % refreshPartitionNum) > 0 ? 1 : 0);
        boolean refreshAllPartitions = Sets.newHashSet(needRefreshPartitions).equals(mtmv.getPartitionNames());
        this.partitionSnapshots = Maps.newConcurrentMap();
        IvmPlanSignature fullRefreshPlanSignature = null;
        for (int i = 0; i < execNum; i++) {
            int start = i * refreshPartitionNum;
            int end = start + refreshPartitionNum;
            Set<String> execPartitionNames = Sets.newHashSet(needRefreshPartitions
                    .subList(start, Math.min(end, needRefreshPartitions.size())));
            Map<BaseTableInfo, Set<Long>> batchResetPartitionIds = useIvmFallbackStreams
                    ? collectPctResetPartitionIds(context, execPartitionNames) : Maps.newHashMap();
            Optional<IvmRewriteContext> rewriteContext = Optional.empty();
            if (useIvmFallbackStreams) {
                StreamReadMode nonPctReadMode = i == 0 && refreshAllPartitions
                        ? StreamReadMode.RESET : StreamReadMode.SNAPSHOT;
                rewriteContext = Optional.of(
                        IvmRewriteContext.full(mtmv, batchResetPartitionIds, nonPctReadMode));
            }
            // need get names before exec
            Map<String, MTMVRefreshPartitionSnapshot> execPartitionSnapshots = MTMVPartitionUtil
                    .generatePartitionSnapshots(context, relation.getBaseTablesOneLevelAndFromView(),
                            execPartitionNames);
            try {
                IvmPlanSignature planSignature = refreshPartitionsWithRetry(execPartitionNames, tableWithPartKey,
                        rewriteContext, refreshMode);
                if (useIvmFallbackStreams) {
                    if (fullRefreshPlanSignature == null) {
                        fullRefreshPlanSignature = planSignature;
                    } else if (!fullRefreshPlanSignature.getSha256().equals(planSignature.getSha256())) {
                        throw new JobException("IVM COMPLETE refresh generated inconsistent plan signatures, mv="
                                + mtmv.getName());
                    }
                }
            } catch (Exception e) {
                LOG.error("Execution failed after retries: {}", e.getMessage());
                throw new JobException(e.getMessage(), e);
            }
            completedPartitions.addAll(execPartitionNames);
            partitionSnapshots.putAll(execPartitionSnapshots);
        }
        if (useIvmFallbackStreams) {
            IvmIncrRefreshManager.finishIvmFullRefresh(mtmv, baselineGeneration,
                    Objects.requireNonNull(fullRefreshPlanSignature,
                            "IVM COMPLETE refresh plan signature can not be null"));
        }
        LOG.info("MTMVTask refresh used snapshot: {}, mvDbName: {}, mvName: {}, taskId: {}", partitionSnapshots,
                mtmv.getDatabase().getFullName(), mtmv.getName(), getTaskId());
    }

    private Map<BaseTableInfo, Set<Long>> collectPctResetPartitionIds(MTMVRefreshContext context,
            Set<String> execPartitionNames) throws AnalysisException {
        Map<BaseTableInfo, Set<Long>> resetPartitionIds = Maps.newHashMap();
        for (String mvPartitionName : execPartitionNames) {
            for (Entry<MTMVRelatedTableIf, Set<String>> entry
                    : context.getByPartitionName(mvPartitionName).entrySet()) {
                if (!(entry.getKey() instanceof OlapTable)) {
                    continue;
                }
                OlapTable pctTable = (OlapTable) entry.getKey();
                Set<Long> partitionIds = resetPartitionIds.computeIfAbsent(
                        new BaseTableInfo(pctTable), key -> Sets.newHashSet());
                for (String pctPartitionName : entry.getValue()) {
                    partitionIds.add(pctTable.getPartitionOrAnalysisException(pctPartitionName).getId());
                }
            }
        }
        return resetPartitionIds;
    }

    private IvmPlanSignature refreshPartitionsWithRetry(Set<String> execPartitionNames,
            Map<TableIf, String> tableWithPartKey,
            Optional<IvmRewriteContext> rewriteContext, RefreshMode refreshMode)
            throws Exception {
        return executeWithRetry(() -> refreshPartitions(execPartitionNames, tableWithPartKey,
                        rewriteContext, refreshMode),
                "partition refresh, execPartitionNames=" + execPartitionNames);
    }

    private <T> T executeWithRetry(Callable<T> callable, String description) throws Exception {
        int retryCount = 0;
        int retryTime = Config.max_query_retry_time;
        retryTime = retryTime <= 0 ? 1 : retryTime + 1;
        Exception lastException = null;
        while (retryCount < retryTime) {
            if (TaskStatus.CANCELED.equals(getStatus())) {
                throw new JobException("MTMV task is CANCELED");
            }
            try {
                return callable.call();
            } catch (Exception e) {
                if (TaskStatus.CANCELED.equals(getStatus()) || !needRetry(e)) {
                    throw e;
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
                                + "taskId {} description {} lastQueryId {}, randomMillis {}",
                        e.getMessage(), retryCount, retryTime, getTaskId(),
                        description, lastQueryId, randomMillis);
                if (retryCount >= retryTime) {
                    throw new Exception("Max retry attempts reached, original: " + lastException);
                }
                Thread.sleep(randomMillis);
            }
        }
        throw new IllegalStateException("MTMV refresh retry loop exited unexpectedly");
    }

    private boolean needRetry(Exception exception) {
        Throwable cause = exception;
        while (cause != null) {
            if (cause instanceof RpcException) {
                return true;
            }
            cause = cause.getCause();
        }
        return Config.isCloudMode() && SystemInfoService.needRetryWithReplan(
                Util.getRootCauseMessage(exception));
    }

    private IvmPlanSignature refreshPartitions(Set<String> refreshPartitionNames,
            Map<TableIf, String> tableWithPartKey,
            Optional<IvmRewriteContext> rewriteContext, RefreshMode refreshMode)
            throws Exception {
        // Create MTMV context first so that new StatementContext() captures the
        // correct thread-local ConnectContext (with MTMV disabled rules, etc.).
        ConnectContext mtmvCtx = MTMVPlanUtil.createMTMVContext(mtmv, MTMVPlanUtil.DISABLE_RULES_WHEN_RUN_MTMV_TASK);
        StatementContext statementContext = new StatementContext();
        // Install the StatementContext on the ConnectContext before parsing
        // the MV definition SQL.  UpdateMvByPartitionCommand.from() calls
        // NereidsParser.parseSingle() which, for SQL containing SET_VAR hints,
        // accesses ConnectContext.get().getStatementContext() inside
        // LogicalPlanBuilder.withHints().  Without this assignment the
        // StatementContext is null and a NullPointerException is thrown.
        mtmvCtx.setStatementContext(statementContext);
        statementContext.setConnectContext(mtmvCtx);
        statementContext.setExcludedTriggerTables(mtmv.getExcludedTriggerTables());
        statementContext.setIvmRewriteContext(rewriteContext);
        for (Entry<MvccTableInfo, MvccSnapshot> entry : snapshots.entrySet()) {
            statementContext.setSnapshot(entry.getKey(), entry.getValue());
        }
        // if SELF_MANAGE mv, only have default partition,  will not have partitionItem, so we give empty set
        UpdateMvByPartitionCommand command = UpdateMvByPartitionCommand
                .from(mtmv, mtmv.getMvPartitionInfo().getPartitionType() != MTMVPartitionType.SELF_MANAGE
                        ? refreshPartitionNames : Sets.newHashSet(), tableWithPartKey, statementContext);
        setupComputeGroup(mtmvCtx);
        try {
            executor = MTMVPlanUtil.executeCommand(mtmvCtx, command, statementContext,
                    getRefreshAuditStmt(refreshMode, refreshPartitionNames));
        } finally {
            recordQueryId(DebugUtil.printId(mtmvCtx.queryId()));
        }
        if (getStatus() == TaskStatus.CANCELED) {
            throw new JobException("task is CANCELED");
        }
        if (!rewriteContext.isPresent()) {
            return null;
        }
        IvmRewriteResult rewriteResult = ((NereidsPlanner) executor.planner()).getCascadesContext()
                .getIvmRewriteResult().orElseThrow(() -> new IllegalStateException(
                        "IVM COMPLETE refresh did not produce an IVM rewrite result"));
        return Objects.requireNonNull(rewriteResult.getPlanSignature(),
                "IVM COMPLETE refresh did not produce a plan signature");
    }

    private void setupComputeGroup(ConnectContext ctx) {
        if (!Config.isCloudMode()) {
            computeGroup = FeConstants.null_string;
            return;
        }
        String taskComputeGroup = taskContext.getComputeGroup();
        if (!Strings.isNullOrEmpty(taskComputeGroup)) {
            ctx.setCloudCluster(taskComputeGroup);
        }
        try {
            computeGroup = ctx.getCloudCluster(false);
        } catch (ComputeGroupException e) {
            computeGroup = FeConstants.null_string;
            LOG.warn("failed to resolve compute group for mtmv task, taskId: {}", getTaskId(), e);
        }
    }

    private void recordQueryId(String queryId) {
        if (!Strings.isNullOrEmpty(queryId)) {
            lastQueryId = queryId;
        }
    }

    private String getRefreshAuditStmt(RefreshMode refreshMode, Set<String> refreshPartitionNames) {
        String mvName = mtmv.getName();
        DatabaseIf database = mtmv.getDatabase();
        if (database != null) {
            mvName = database.getFullName() + "." + mvName;
            CatalogIf catalog = database.getCatalog();
            if (catalog != null) {
                mvName = catalog.getName() + "." + mvName;
            }
        }
        return String.format(
                "Asynchronous materialized view refresh task, mvName: %s,"
                        + "taskId: %s, refreshMode: %s, partitions: %s",
                mvName, super.getTaskId(), refreshMode, refreshPartitionNames);
    }

    @Override
    public synchronized boolean onFail() throws JobException {
        LOG.info("mtmv task onFail, taskId: {}", super.getTaskId());
        boolean res = super.onFail();
        if (!res) {
            return false;
        }
        after();
        if (MetricRepo.isInit) {
            MetricRepo.COUNTER_ASYNC_MATERIALIZED_VIEW_TASK_FAILED_NUM.increase(1L);
        }
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
        if (MetricRepo.isInit) {
            MetricRepo.HISTO_ASYNC_MATERIALIZED_VIEW_TASK_DURATION.update(
                    super.getFinishTimeMs() - super.getStartTimeMs());
            MetricRepo.COUNTER_ASYNC_MATERIALIZED_VIEW_TASK_SUCCESS_NUM.increase(1L);
        }
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
        for (BaseTableInfo tableInfo : relation.getBaseTablesOneLevelAndFromView()) {
            TableIf tableIf = MTMVUtil.getTable(tableInfo);
            if (tableIf instanceof MTMVBaseTableIf) {
                MTMVBaseTableIf baseTableIf = (MTMVBaseTableIf) tableIf;
                baseTableIf.beforeMTMVRefresh(mtmv);
            }
            if (tableIf instanceof MvccTable) {
                MvccTable mvccTable = (MvccTable) tableIf;
                MvccSnapshot mvccSnapshot = mvccTable.loadSnapshot(Optional.empty(), Optional.empty());
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
        trow.addToColumnValue(new TCell().setStringVal(
                computeGroup == null || computeGroup.isEmpty() ? FeConstants.null_string : computeGroup));
        trow.addToColumnValue(new TCell().setStringVal(
                ivmFallbackReason == null ? FeConstants.null_string : ivmFallbackReason));
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
            List<BaseColInfo> pctInfos = mtmv.getMvPartitionInfo().getPctInfos();
            for (BaseColInfo pctInfo : pctInfos) {
                tableWithPartKey
                        .put(MTMVUtil.getTable(pctInfo.getTableInfo()), pctInfo.getColName());
            }
        }
        return tableWithPartKey;
    }

    private MTMVTaskRefreshMode generateRefreshMode(List<String> needRefreshPartitionIds) {
        if (CollectionUtils.isEmpty(needRefreshPartitionIds)) {
            return MTMVTaskRefreshMode.NOT_REFRESH;
        } else if (Sets.newHashSet(needRefreshPartitionIds).equals(mtmv.getPartitionNames())) {
            return MTMVTaskRefreshMode.COMPLETE;
        } else {
            return MTMVTaskRefreshMode.PARTIAL;
        }
    }

    public List<String> calculateNeedRefreshPartitions(MTMVRefreshContext context)
            throws AnalysisException, JobException {
        RefreshRequest request = resolveRefreshRequest();
        if (request.refreshMode == RefreshMode.COMPLETE) {
            return Lists.newArrayList(mtmv.getPartitionNames());
        }
        // check whether the user manually triggers it
        if (taskContext.getTriggerMode() == MTMVTaskTriggerMode.MANUAL) {
            if (!CollectionUtils.isEmpty(taskContext.getPartitions())) {
                return taskContext.getPartitions();
            }
        }
        // if refreshMethod is COMPLETE, we must FULL refresh, avoid external table MTMV always not refresh
        if (mtmv.getRefreshInfo().getRefreshMethod() == RefreshMethod.COMPLETE) {
            return Lists.newArrayList(mtmv.getPartitionNames());
        }
        // We need to use a newly generated relationship and cannot retrieve it using mtmv.getRelation()
        // to avoid rebuilding the baseTable and causing a change in the tableId
        boolean fresh = MTMVPartitionUtil.isMTMVSync(context, relation.getBaseTablesOneLevelAndFromView(),
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
        return MTMVPartitionUtil.getMTMVNeedRefreshPartitions(context, relation.getBaseTablesOneLevelAndFromView());
    }

    public MTMVTaskContext getTaskContext() {
        return taskContext;
    }

    public long getMtmvSchemaChangeVersion() {
        return mtmvSchemaChangeVersion;
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
