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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.catalog.stream.BaseTableStream;
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
import org.apache.doris.mtmv.MTMVPartitionState;
import org.apache.doris.mtmv.MTMVPartitionUtil;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVRefreshState;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshMethod;
import org.apache.doris.mtmv.MTMVRefreshPartitionSnapshot;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.mtmv.ivm.IvmFailureReason;
import org.apache.doris.mtmv.ivm.IvmIncrRefreshContext;
import org.apache.doris.mtmv.ivm.IvmIncrRefreshManager;
import org.apache.doris.mtmv.ivm.IvmIncrRefreshResult;
import org.apache.doris.mtmv.ivm.IvmPlanSignature;
import org.apache.doris.mtmv.ivm.IvmRewriteContext;
import org.apache.doris.mtmv.ivm.IvmUtil;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.commands.CreateMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.UpdateMvByPartitionCommand;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo.RefreshMode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QeProcessorImpl;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class MTMVTask extends AbstractTask {
    private static final Logger LOG = LogManager.getLogger(MTMVTask.class);
    public static final int DEFAULT_REFRESH_PARTITION_NUM = 1;
    public static final String DEBUG_POINT_SKIP_PARTITION_SYNC =
            "MTMVTask.syncPartitionsIfNeeded.skip";
    public static final String DEBUG_POINT_SKIP_PARTITION_SYNC_FILTER =
            "MTMVTask.syncPartitionsIfNeeded.skip.filter";

    private static final Gson GSON = new Gson();

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
            new Column("IvmFallbackReason", ScalarType.createStringType()),
            new Column("IvmRebuiltPartitions", ScalarType.createStringType()));

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
    // Written by the executing (Disruptor worker) thread via the executeCommand consumer
    // callback and read by the cancel (command) thread, so it must be volatile.
    private volatile StmtExecutor executor;
    private Map<String, MTMVRefreshPartitionSnapshot> partitionSnapshots;
    // The requirement each refreshed partition was read under, captured before the base tables were read
    // and recorded only once that batch's data committed (see commitCapturedEpochs). In memory only: the
    // journal carries the resulting states, and a replay applies those instead of recomputing anything.
    private transient Map<String, Long> ivmCapturedEpochs = Maps.newHashMap();
    // The requirement every partition had when this refresh planned its work. What a batch records is
    // clamped to it (see commitCapturedEpochs): a mark that lands after the plan must leave its partition
    // dirty rather than be written back as satisfied. Empty on a path that does not plan partition work,
    // which is the plain COMPLETE path -- a whole-MV rebuild replaces every partition, so whatever it read
    // is what it repaired.
    private transient Map<String, Long> ivmPlannedEpochs = Maps.newHashMap();
    // How many partitions this refresh rebuilt because the criterion demanded it, which a strict
    // INCREMENTAL request reports so that "the request was incremental but the work was not" is visible.
    @SerializedName("irp")
    private int ivmRebuiltPartitions;
    private long mtmvSchemaChangeVersion;
    // Published only after a signature-mismatch fallback succeeds and its task result is accepted.
    private transient String refreshedIvmPlanSignature;

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
            MTMVPlanUtil.QueryAnalysisResult queryAnalysis = MTMVPlanUtil.getBaseTableFromQuery(
                    mtmv.getQuerySql(), ctx);
            this.relation = MTMVPlanUtil.generateMTMVRelation(queryAnalysis.getAllLevelTables(),
                    queryAnalysis.getOneLevelTables());
            beforeMTMVRefresh();
            List<TableIf> tableIfs = Lists.newArrayList(queryAnalysis.getAllLevelTables());
            tableIfs.sort(Comparator.comparing(TableIf::getId));

            // This checks whether an MV in SCHEMA_CHANGE state still matches
            // its base-table schema and partition definition. It is not part of
            // refresh fallback: incompatible MV definitions must fail directly.
            ensureQueryUsableIfNeeded(ctx, tableIfs);
            RefreshRequest request = resolveRefreshRequest();
            try {
                syncPartitionsIfNeeded(ctx, tableIfs);
            } catch (PartitionPlanningException e) {
                throw new JobException(e.getMessage(), e);
            }
            // Partition sync has decided which partitions exist, and nothing has read a base table yet:
            // this is the point where an entry and the partition it describes become the same thing.
            // Doing it any later would let a partition that sync has just added be refreshed without an
            // entry, and an invalidation arriving in between would have nothing to land on.
            mtmv.alignPartitionStates(mtmv.getPartitionNames());
            // Decided after the sync and the alignment, because the escalation it can take reads the
            // partition states and only then is the partition set they describe final.
            List<RefreshAttemptType> attempts = buildAttempts(request, queryAnalysis.containsOneRowRelation());
            MTMVRefreshContext refreshContext = buildRefreshContext(tableIfs);
            boolean disablePartitionRefresh = false;
            for (RefreshAttemptType attemptType : attempts) {
                switch (attemptType) {
                    case IVM:
                        AttemptResultType ivmResult = executeIvmAttempt(refreshContext, request, ctx, tableIfs);
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
                        if (executePartitionBasedRefresh(refreshContext, request, ctx)) {
                            return;
                        }
                        break;
                    case COMPLETE:
                        executeCompleteAttempt(refreshContext, ctx);
                        // Recorded here rather than where the escalation was decided: the count is what the
                        // rebuild actually replaced, and a refresh that failed before its first commit must
                        // not report the whole MV as rebuilt. The rebuild records its own count for the
                        // partitions it replaced; this one is only reached when it succeeded.
                        recordRebuiltPartitions(request, mtmv.getPartitionNames().size());
                        return;
                    default:
                        throw new JobException("Unsupported refresh attempt type: " + attemptType);
                }
            }
            throw new JobException("No refresh attempt succeeded for mv=" + mtmv.getName());
        } catch (Throwable e) {
            if (getStatus() == TaskStatus.RUNNING) {
                LOG.warn("run task failed, mvName: {}, taskId: {}",
                        mtmv.getName(), getTaskId(), e);
                throw new JobException(e.getMessage(), e);
            } else {
                // if status is not `RUNNING`,maybe the task was canceled, therefore, it is a normal situation
                LOG.info("task [{}] interruption running, because status is [{}]", getTaskId(), getStatus());
            }
        } finally {
            closeExecutionContext(ctx);
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
        if (isSkipPartitionSyncDebugPointEnabled()) {
            LOG.info("Skip MTMV partition synchronization for debug point, mv={}, taskId={}",
                    mtmv.getName(), getTaskId());
            return;
        }
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
                    syncPartitions = MTMVPartitionUtil.alignMvPartition(mtmv, snapshots);
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

    private boolean isSkipPartitionSyncDebugPointEnabled() {
        String targetMvName = DebugPointUtil.getDebugParamOrDefault(
                DEBUG_POINT_SKIP_PARTITION_SYNC_FILTER, "mv_name", "");
        return mtmv.getName().equals(targetMvName)
                && DebugPointUtil.isEnable(DEBUG_POINT_SKIP_PARTITION_SYNC);
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

    private List<RefreshAttemptType> buildAttempts(RefreshRequest request, boolean containsOneRowRelation) {
        if (shouldUseCompleteForInitialIvmRefresh(containsOneRowRelation)) {
            return Lists.newArrayList(RefreshAttemptType.COMPLETE);
        }
        // A schema-level invalidation is not a set of dirty partitions: it means every partition, including
        // the ones partition sync has not created yet, and no per-partition requirement can express that.
        // IVM only -- a non-IVM MV reaches the same effect through its cleared snapshot, which its own
        // refresh already depends on.
        if (mtmv.isIvm() && !request.explicitPartitions
                && mtmv.getStatus().getState() == MTMVState.SCHEMA_CHANGE) {
            LOG.info("IVM MV is in SCHEMA_CHANGE, rebuilding the whole MV, mv={}, taskId={}",
                    mtmv.getName(), getTaskId());
            return Lists.newArrayList(RefreshAttemptType.COMPLETE);
        }
        List<RefreshAttemptType> attempts = Lists.newArrayList();
        switch (request.refreshMode) {
            case AUTO:
                // ALTER excluded_trigger_tables clears a successful baseline without changing the MV state.
                if (!mtmv.isIvm() && !mtmv.hasRefreshSnapshot()
                        && mtmv.getStatus().getState() == MTMVState.NORMAL
                        && mtmv.getStatus().getRefreshState() == MTMVRefreshState.SUCCESS) {
                    attempts.add(RefreshAttemptType.COMPLETE);
                    break;
                }
                if (mtmv.isIvm()) {
                    attempts.add(RefreshAttemptType.IVM);
                }
                // AUTO always ends with COMPLETE. For an MV defined REFRESH COMPLETE,
                // skip the PARTITIONS attempt: its sync check treats base tables that are
                // not MTMVRelatedTableIf (external tables, views) as always synchronous,
                // so the refresh would be skipped forever after the first build.
                if (mtmv.getRefreshInfo().getRefreshMethod() != RefreshMethod.COMPLETE) {
                    attempts.add(RefreshAttemptType.PARTITIONS);
                } else {
                    LOG.info("AUTO refresh of COMPLETE-method mv={} skips partition-sync check "
                            + "and refreshes all directly, taskId={}", mtmv.getName(), super.getTaskId());
                }
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
        // A base table the plan scans without a usable stream makes the incremental attempt fail: its
        // rewrite reads the stream of every table it scans. Only COMPLETE reconciles streams, so a request
        // that would try the incremental path first goes there directly, and the attempt that cannot
        // succeed -- with the baseline barrier it writes before it starts -- stays out of the way.
        //
        // Only that attempt is judged here. A partition refresh reads a narrower set, and how narrow
        // depends on the partitions it plans: a PCT table that no refreshed partition's mapping names
        // keeps its place in the plan as an ordinary scan, and an excluded trigger table is never read
        // through a stream. It checks its own scope once it has planned (see
        // hasUnusableIvmStreamForPartitions), so judging it here by the whole plan would send a partition
        // refresh that would have worked to COMPLETE.
        if (request.allowFallback && mtmv.isIvm() && attempts.contains(RefreshAttemptType.IVM)
                && hasUnusableIvmStream()) {
            ivmFallbackReason = IvmFailureReason.STREAM_UNSUPPORTED.name();
            LOG.warn("IVM stream is unusable, mv={}, taskId={}. Continuing with COMPLETE refresh.",
                    mtmv.getName(), getTaskId());
            return Lists.newArrayList(RefreshAttemptType.COMPLETE);
        }
        // Every partition either needs a rebuild or was never filled, and at least one needs a rebuild:
        // COMPLETE then does nothing the per-partition routing would not, in one read of the MV.
        if (!request.explicitPartitions && attempts.contains(RefreshAttemptType.IVM)
                && shouldEscalateToComplete()) {
            LOG.info("Every MV partition needs a rebuild or has no data yet, mv={}, taskId={}. "
                    + "Continuing with COMPLETE refresh.", mtmv.getName(), getTaskId());
            return Lists.newArrayList(RefreshAttemptType.COMPLETE);
        }
        return attempts;
    }

    /**
     * Notes that this refresh rebuilds partitions the request did not ask to rebuild, which is what a
     * strict INCREMENTAL request cannot tell from its result otherwise: it reports the count, and a request
     * that asked for a complete refresh reports nothing because rebuilding everything is what it asked for.
     */
    private void recordRebuiltPartitions(RefreshRequest request, int rebuiltPartitions) {
        if (request.refreshMode == RefreshMode.COMPLETE) {
            return;
        }
        ivmRebuiltPartitions = Math.max(ivmRebuiltPartitions, rebuiltPartitions);
    }

    /**
     * Whether every MV partition is dirty or was never refreshed, and at least one is dirty.
     *
     * <p>A partition that holds data and does not need a rebuild is what makes this false: COMPLETE would
     * recompute it for nothing, which is the waste the per-partition routing exists to avoid. A partition
     * that was never refreshed does not count against it -- COMPLETE fills it, which its routing branch
     * would do as well.
     */
    private boolean shouldEscalateToComplete() {
        boolean anyDirty = false;
        for (MTMVPartitionState state : mtmv.getPartitionStates().values()) {
            if (state.isDirty()) {
                anyDirty = true;
            } else if (!state.isNeverRefreshed()) {
                return false;
            }
        }
        return anyDirty;
    }

    private boolean shouldUseCompleteForInitialIvmRefresh(boolean containsOneRowRelation) {
        if (!mtmv.isIvm() || mtmv.hasRefreshSnapshot()) {
            return false;
        }
        // Excluded trigger tables produce no delta stream in the incremental rewrite, so an
        // INCREMENTAL first refresh would never read their pre-existing rows. Whenever the
        // MV has excluded sources, the first refresh must build the full COMPLETE baseline
        // first (covering manual INCREMENTAL as well); afterwards the snapshot exists and
        // incremental refreshes only track the remaining streamed tables.
        return taskContext.getTriggerMode() != MTMVTaskTriggerMode.MANUAL
                || !CollectionUtils.isEmpty(mtmv.getExcludedTriggerTables())
                || containsOneRowRelation;
    }

    private PartitionRefreshPlan planPartitionRefresh(MTMVRefreshContext context,
            RefreshRequest request) throws AnalysisException {
        if (request.explicitPartitions) {
            return PartitionRefreshPlan.success(context, request.partitions);
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
        if (mtmv.getMvPartitionInfo().getPartitionType() == MTMVPartitionType.SELF_MANAGE) {
            // Keep this inside the PARTITIONS attempt so PARTITIONS FALLBACK and
            // AUTO can still continue to COMPLETE for non-partitioned MVs.
            return PartitionRefreshPlan.fallback(
                    "The partition method of this asynchronous materialized view "
                            + "does not support refreshing by partition");
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
            return MTMVRefreshContext.buildContext(mtmv, Maps.newHashMap(), snapshots);
        } finally {
            MetaLockUtils.readUnlockTables(tableIfs);
        }
    }

    private void executeCompleteAttempt(MTMVRefreshContext context, ConnectContext ctx)
            throws JobException, AnalysisException {
        this.needRefreshPartitions = Lists.newArrayList(mtmv.getPartitionNames());
        // A whole-MV rebuild replaces every partition, so there is nothing for a captured epoch to be
        // clamped against: whatever this refresh read is what it repaired. Dropped rather than kept so a
        // refresh that planned partition work and then fell back to COMPLETE does not leave the partitions
        // it did rebuild looking like they still owe one.
        this.ivmPlannedEpochs = Maps.newHashMap();
        this.refreshMode = generateRefreshMode(needRefreshPartitions);
        if (refreshMode == MTMVTaskRefreshMode.NOT_REFRESH) {
            return;
        }
        // Marked before the streams are reconciled, not merely before the rebuild: reconciling durably
        // creates a replacement stream whose historical rows are read as an append, so a crash after that
        // create and before this rebuild publishes its epochs would leave a populated MV with clean epochs
        // and a usable stream -- and the next incremental refresh would add those rows to the old baseline
        // again. The requirement is raised as a real mark on the partitions, which is what "these must be
        // rebuilt" means to the refresh, and it is journaled as the whole map before the reconcile below.
        mtmv.markPartitionsForRebuild(Sets.newHashSet(needRefreshPartitions));
        // A complete rebuild resets the stream baselines, so reconcile missing or unusable streams
        // before reading anything. Only COMPLETE may do this: a stream baseline is global, resetting
        // it during a partial refresh would corrupt the partitions that refresh does not touch.
        if (mtmv.isIvm()) {
            reconcileIvmStreams(ctx);
        }
        executePartitionBasedRefresh(context, RefreshMode.COMPLETE, ctx);
    }

    private AttemptResultType executeIvmAttempt(MTMVRefreshContext refreshContext,
            RefreshRequest request, ConnectContext ctx, List<TableIf> tableIfs)
            throws JobException, AnalysisException {
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
        // The partitions the criterion says must be rebuilt rather than caught up: the delta path can only
        // append, so a partition it treated as current would record that in its epoch while its rows still
        // come from before the change. Rebuilt first, with the partition executor, because that is the
        // full recomputation they need -- and only in this task's batches, so a change that arrives while
        // it runs leaves them dirty for the next round instead of being swallowed.
        // One read of the states decides both what has to be rebuilt and the requirement each batch may
        // write back. Reading them separately would leave a window between the two in which a mark lands,
        // the routing decision does not see it, and the batch that follows captures the raised requirement
        // and records it as met by a delta that cannot remove the rows that mark made unusable.
        Map<String, MTMVPartitionState> plannedStates = mtmv.getPartitionStates();
        Set<String> livePartitionNames = mtmv.getPartitionNames();
        Set<String> dirtyPartitions = Sets.newLinkedHashSet();
        Map<String, Long> plannedEpochs = Maps.newHashMap();
        for (Entry<String, MTMVPartitionState> plannedState : plannedStates.entrySet()) {
            if (!livePartitionNames.contains(plannedState.getKey())) {
                continue;
            }
            plannedEpochs.put(plannedState.getKey(), plannedState.getValue().getLatestEpoch());
            if (plannedState.getValue().isDirty()) {
                dirtyPartitions.add(plannedState.getKey());
            }
        }
        this.ivmPlannedEpochs = plannedEpochs;
        Map<String, MTMVRefreshPartitionSnapshot> rebuiltSnapshots = Maps.newHashMap();
        List<String> rebuildScope = Lists.newArrayList();
        Set<String> rebuildCompleted = Sets.newLinkedHashSet();
        if (!dirtyPartitions.isEmpty()) {
            LOG.info("Rebuilding {} invalidated MV partitions before the incremental refresh, mv={}, taskId={}",
                    dirtyPartitions.size(), mtmv.getName(), getTaskId());
            List<String> toRebuild = Lists.newArrayList(dirtyPartitions);
            toRebuild.sort(Comparator.naturalOrder());
            this.needRefreshPartitions = toRebuild;
            this.refreshMode = generateRefreshMode(toRebuild);
            try {
                executePartitionBasedRefresh(refreshContext, RefreshMode.PARTITIONS, ctx);
            } finally {
                // Counted from the groups that committed, not from the ones that were planned: a refresh
                // that failed part-way through the rebuild must not report partitions it never replaced.
                recordRebuiltPartitions(request, partitionSnapshots.size());
            }
            rebuiltSnapshots.putAll(partitionSnapshots);
            // Kept before the incremental attempt resets the accumulators to its own scope: both phases
            // belong to this refresh, so the progress it reports is the union of the two.
            rebuildScope.addAll(needRefreshPartitions);
            rebuildCompleted.addAll(completedPartitions);
        }
        MTMVRefreshContext currentRefreshContext = refreshContext;
        int ivmAttemptLimit = Math.max(Config.max_query_retry_time, 0) + 1;
        IvmIncrRefreshResult ivmResult = null;
        for (int partitionSyncRetryCount = 0;
                partitionSyncRetryCount < ivmAttemptLimit; partitionSyncRetryCount++) {
            ivmResult = executeSingleIvmAttempt(currentRefreshContext, dirtyPartitions);
            if (ivmResult.isSuccess()) {
                // The incremental attempt reset the accumulators it owns, so the rebuild's are merged back
                // here: its batches committed, and without them the partitions it rebuilt would look
                // unsynced and be refreshed again on every following round.
                this.partitionSnapshots.putAll(rebuiltSnapshots);
                // The incremental attempt reset the accumulators to its own scope. Both phases are part of
                // the refresh that is being reported, so the denominator is the union of the two and the
                // completed side keeps what each phase committed: a refresh that rebuilt one partition and
                // caught up another would otherwise record two of one.
                Set<String> mergedScope = Sets.newLinkedHashSet(rebuildScope);
                mergedScope.addAll(needRefreshPartitions);
                this.needRefreshPartitions = Lists.newArrayList(mergedScope);
                this.completedPartitions.addAll(rebuildCompleted);
                return AttemptResultType.SUCCESS;
            }
            if (ivmResult.getFailureReason() != IvmFailureReason.MV_PARTITION_NOT_FOUND) {
                return handleIvmFallbackResult(ivmResult, request);
            }
            if (partitionSyncRetryCount + 1 >= ivmAttemptLimit) {
                break;
            }
            try {
                syncPartitionsIfNeeded(ctx, tableIfs);
                // The retry can add a partition that did not exist at the first alignment. It has to get
                // its entry before the retried refresh reads a base table, or an invalidation arriving
                // in between would have nothing to land on for rows this task is about to write.
                mtmv.alignPartitionStates(mtmv.getPartitionNames());
                currentRefreshContext = buildRefreshContext(tableIfs);
            } catch (Exception e) {
                throw new JobException("Failed to synchronize MV partitions before IVM retry for mv="
                        + mtmv.getName(), e);
            }
            LOG.warn("Retrying IVM refresh after synchronizing MV partitions, mv={}, attempt={}/{}, taskId={}",
                    mtmv.getName(), partitionSyncRetryCount + 1, ivmAttemptLimit, getTaskId());
        }
        throw new JobException("IVM refresh could not recover missing MV partition for mv="
                + mtmv.getName() + ", detail=" + ivmResult.getDetailMessage());
    }

    private IvmIncrRefreshResult executeSingleIvmAttempt(MTMVRefreshContext refreshContext,
            Set<String> dirtyPartitions)
            throws JobException {
        this.completedPartitions = Lists.newCopyOnWriteArrayList();
        this.partitionSnapshots = Maps.newConcurrentMap();
        // Determine which partitions need refresh, same as partition-based flow. The partitions the
        // rebuild above handled are taken out: an incremental refresh of one of them would record it as
        // caught up while its rows are exactly what the rebuild had to replace.
        Set<String> incrementalScope = Sets.newLinkedHashSet(MTMVPartitionUtil.getMTMVNeedRefreshPartitions(
                refreshContext, relation.getBaseTablesOneLevelAndFromView()));
        incrementalScope.removeAll(dirtyPartitions);
        this.needRefreshPartitions = Lists.newArrayList(incrementalScope);
        if (CollectionUtils.isEmpty(needRefreshPartitions)) {
            LOG.info("IVM incremental refresh skipped for mv={}: all partitions are synced, taskId={}",
                    mtmv.getName(), getTaskId());
            return IvmIncrRefreshResult.success();
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
        // The requirement these partitions are read under, captured before the read inside doRefresh and
        // recorded only if the refresh commits; see captureLatestEpochs.
        Map<String, Long> capturedEpochs = captureLatestEpochs(Sets.newHashSet(needRefreshPartitions));
        IvmIncrRefreshResult ivmResult;
        try {
            ivmResult = executeWithRetry(() -> {
                ConnectContext ivmConnectContext = MTMVPlanUtil.createMTMVContext(mtmv,
                        MTMVPlanUtil.DISABLE_RULES_WHEN_RUN_MTMV_TASK);
                try {
                    setupComputeGroup(ivmConnectContext);
                    IvmIncrRefreshContext ivmIncrRefreshContext = new IvmIncrRefreshContext(mtmv,
                            ivmConnectContext,
                            getRefreshAuditStmt(RefreshMode.INCREMENTAL, Sets.newHashSet(needRefreshPartitions)),
                            this::recordQueryId,
                            this::registerExecutor);
                    mtmv.validateIvmRefreshStart(mtmvSchemaChangeVersion);
                    return ivmIncrRefreshManager.doRefresh(ivmIncrRefreshContext);
                } finally {
                    closeExecutionContext(ivmConnectContext);
                }
            }, "IVM refresh");
        } catch (Exception e) {
            throw new JobException("IVM incremental refresh failed for mv=" + mtmv.getName()
                    + ", detail=" + Util.getRootCauseMessage(e), e);
        }
        if (ivmResult.isSuccess()) {
            this.partitionSnapshots.putAll(capturedSnapshots);
            this.completedPartitions.addAll(needRefreshPartitions);
            commitCapturedEpochs(capturedEpochs);
            LOG.info("IVM incremental refresh succeeded for mv={}, taskId={}",
                    mtmv.getName(), getTaskId());
        }
        return ivmResult;
    }

    /**
     * Captures the requirement these partitions are about to be read under: the epoch in force at the
     * moment the refresh starts reading, which is what the data it writes will be described by.
     *
     * <p>A refresh writes back the requirement it captured, not the one in force when it finishes, which
     * is what keeps an invalidation that arrives while the refresh runs from being swallowed: the
     * requirement it raises stays above the value the task writes, so the partition still counts as
     * needing a rebuild.
     *
     * <p>It has to run before the base tables are read and never after. An epoch captured after the read
     * could claim data newer than what the read saw, and the partition would then look caught up while
     * it holds rows from before the change.
     *
     * <p>The caller keeps the result and hands it to {@link #commitCapturedEpochs} only once that batch's
     * data has committed. Recording it here would credit a batch whose write never happened with data
     * that does not exist, which is the one direction the epoch must never be wrong in.
     *
     * <p>A non-IVM MV carries no states, so this captures nothing for it.
     */
    private Map<String, Long> captureLatestEpochs(Set<String> partitionNames) {
        if (CollectionUtils.isEmpty(partitionNames)) {
            return Maps.newHashMap();
        }
        return mtmv.getLatestEpochs(partitionNames);
    }

    /**
     * Commits the captured epochs of a batch whose data has landed, so its work is not repeated after a
     * restart.
     *
     * <p>A partition read by two phases of one task keeps the higher value: that is the requirement in
     * force when the data that survived was read.
     */
    private void commitCapturedEpochs(Map<String, Long> capturedEpochs) {
        for (Entry<String, Long> entry : capturedEpochs.entrySet()) {
            ivmCapturedEpochs.merge(entry.getKey(), plannedCeiling(entry), Math::max);
        }
    }

    /**
     * The epoch to record for a captured partition: the one it was read at, or the one it was planned at
     * when that is lower.
     *
     * <p>The planned value is the one the routing decision was made on. An invalidation that arrives after
     * that decision but before this batch is read would otherwise be captured here and written back as
     * satisfied, while the delta this refresh applies cannot remove the rows the invalidation made
     * unusable -- the partition holds them still, and only a rebuild replaces them. Recording the planned
     * value leaves the partition dirty, so the next refresh rebuilds it. Rebuilding once more than
     * strictly needed is the safe direction; keeping rows nothing can remove is not.
     */
    private long plannedCeiling(Entry<String, Long> captured) {
        Long planned = ivmPlannedEpochs.get(captured.getKey());
        return planned == null ? captured.getValue() : Math.min(captured.getValue(), planned);
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
            RefreshRequest request, ConnectContext ctx) throws JobException, AnalysisException {
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
        // This attempt now knows which partitions it refreshes, and with them which streams it reads.
        // Judged here rather than in buildAttempts because only the plan knows that scope, and judged
        // before the NOT_REFRESH return below so that a fallback-capable request still reaches the only
        // attempt that reconciles streams. Falling back here continues to COMPLETE, which is what repairs
        // them; a request that may not fall back fails instead of quietly refreshing less than it asked.
        if (mtmv.isIvm()
                && hasUnusableIvmStreamForPartitions(partitionPlan.context, needRefreshPartitions)) {
            if (!request.allowFallback) {
                throw new JobException("IVM stream is unusable for the partitions of this refresh, mv="
                        + mtmv.getName());
            }
            ivmFallbackReason = IvmFailureReason.STREAM_UNSUPPORTED.name();
            LOG.warn("IVM stream is unusable for the partitions this refresh plans, mv={}, taskId={}. "
                    + "Continuing with COMPLETE refresh.", mtmv.getName(), getTaskId());
            return false;
        }
        if (refreshMode == MTMVTaskRefreshMode.NOT_REFRESH) {
            return true;
        }
        executePartitionBasedRefresh(partitionPlan.context, RefreshMode.PARTITIONS, ctx);
        return true;
    }

    private void executePartitionBasedRefresh(MTMVRefreshContext context, RefreshMode refreshMode,
            ConnectContext ctx)
            throws JobException, AnalysisException {
        boolean useIvmFallbackStreams = mtmv.isIvm();
        Map<TableIf, String> tableWithPartKey = getIncrementalTableMap();
        this.completedPartitions = Lists.newCopyOnWriteArrayList();
        try {
            // Snapshot persistence happens after refresh partitions are split into execution groups. Load the
            // complete union here so the default one-partition group size cannot turn a large Hive MTMV into
            // one metadata request per MV partition; generatePartitionSnapshots reuses this context cache.
            context.preparePartitionSnapshots(Sets.newHashSet(needRefreshPartitions));
        } catch (Exception e) {
            // Preloading is only a batching optimization. Retrying through the existing per-group load below
            // preserves completed-group progress when a later chunk of the union fails.
            LOG.warn("Failed to preload partition snapshots for mv={}, taskId={}; "
                    + "falling back to per-group loading", mtmv.getName(), getTaskId(), e);
        }
        int refreshPartitionNum = mtmv.getRefreshPartitionNum();
        long execNum = (needRefreshPartitions.size() / refreshPartitionNum) + ((needRefreshPartitions.size()
                % refreshPartitionNum) > 0 ? 1 : 0);
        boolean refreshAllPartitions = Sets.newHashSet(needRefreshPartitions).equals(mtmv.getPartitionNames());
        // Every COMPLETE refresh of an IVM MV establishes the baseline its signature describes, whichever
        // route asked for it: the mismatch fallback is one, the escalation an invalidated MV takes is
        // another. Publishing only the former leaves the MV on its old signature, so the next refresh runs
        // a second COMPLETE through the fallback and a strict INCREMENTAL rejects a baseline that has just
        // been rebuilt. Non-IVM MVs keep the old condition: their refresh produces no IVM plan signature.
        boolean capturePlanSignature = refreshMode == RefreshMode.COMPLETE
                && IvmFailureReason.PLAN_SIGNATURE_MISMATCH.name().equals(ivmFallbackReason);
        this.partitionSnapshots = Maps.newConcurrentMap();
        IvmPlanSignature refreshedPlanSignature = null;
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
            // The requirement this batch is read under, captured before the read below and recorded once
            // the read's data has committed, next to its snapshots. Capturing it per batch keeps an
            // invalidation that arrives during the refresh from holding back the whole round: only the
            // batches already read keep a requirement above their captured value.
            Map<String, Long> batchCapturedEpochs = captureLatestEpochs(execPartitionNames);
            // need get names before exec
            Map<String, MTMVRefreshPartitionSnapshot> execPartitionSnapshots = MTMVPartitionUtil
                    .generatePartitionSnapshots(context, relation.getBaseTablesOneLevelAndFromView(),
                            execPartitionNames);
            try {
                IvmPlanSignature batchPlanSignature = refreshPartitionsWithRetry(
                        execPartitionNames, tableWithPartKey, rewriteContext, refreshMode);
                if (capturePlanSignature) {
                    batchPlanSignature = Objects.requireNonNull(batchPlanSignature,
                            "IVM COMPLETE refresh did not produce a plan signature");
                    if (refreshedPlanSignature == null) {
                        refreshedPlanSignature = batchPlanSignature;
                    } else if (!refreshedPlanSignature.getSha256().equals(batchPlanSignature.getSha256())) {
                        throw new JobException("IVM COMPLETE refresh generated inconsistent plan signatures, mv="
                                + mtmv.getName());
                    }
                }
            } catch (Exception e) {
                LOG.error("Execution failed after retries, mvName: {}, taskId: {}",
                        mtmv.getName(), getTaskId(), e);
                throw new JobException(e.getMessage(), e);
            }
            completedPartitions.addAll(execPartitionNames);
            partitionSnapshots.putAll(execPartitionSnapshots);
            commitCapturedEpochs(batchCapturedEpochs);
        }
        if (capturePlanSignature) {
            refreshedIvmPlanSignature = refreshedPlanSignature.getSha256();
        }
        LOG.info("MTMVTask refresh used snapshot: {}, mvDbName: {}, mvName: {}, taskId: {}", partitionSnapshots,
                mtmv.getDatabase().getFullName(), mtmv.getName(), getTaskId());
    }

    /**
     * Whether a base table the refresh reads has no stream that can be read, which no attempt other
     * than COMPLETE can work around.
     *
     * <p>A base table that cannot be resolved is skipped rather than judged: it says nothing about the
     * streams, and the refresh fails on it for its own reasons -- the attempt that runs reports that,
     * this one only decides which attempt that should be.
     */
    private boolean hasUnusableIvmStream() {
        Database mvDb = (Database) mtmv.getDatabase();
        if (mvDb == null) {
            // Nothing to look the streams up in, so there is nothing to decide here.
            return false;
        }
        Set<TableNameInfo> excluded = mtmv.getExcludedTriggerTables();
        // The tables in the plan, not the relation's closure: a chained MV is created with a stream for
        // every base table behind the MVs it reads, but no rewrite ever looks those up -- the incremental
        // rewriter and the full refresh take the streams of the plan's scans -- so judging them would
        // rebuild an MV whose refresh had nothing wrong with it.
        for (BaseTableInfo baseTableInfo : relation.getBaseTablesOneLevelAndFromView()) {
            OlapTable baseTable = resolveIvmBaseTable(baseTableInfo);
            if (baseTable == null) {
                continue;
            }
            if (MTMVPartitionUtil.isTableExcluded(excluded,
                    new TableNameInfo(baseTable.getFullQualifiers()))) {
                continue;
            }
            if (usableIvmStream(mvDb, baseTable) == null) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether a stream this partition refresh will read is missing or unusable.
     *
     * <p>The set is narrower than the plan, and which tables are in it depends on the partitions being
     * refreshed: a PCT table that no refreshed partition's mapping names keeps its place in the plan as
     * an ordinary scan, so its stream is never read, and a table outside the plan's own tables is not
     * scanned at all. Judging the whole plan instead sends a partition refresh that would have worked to
     * a full rebuild whenever such an unread stream is missing.
     */
    private boolean hasUnusableIvmStreamForPartitions(MTMVRefreshContext context,
            List<String> mvPartitionNames) {
        Database mvDb = (Database) mtmv.getDatabase();
        if (mvDb == null) {
            // Nothing to look the streams up in, so there is nothing to decide here.
            return false;
        }
        Set<TableNameInfo> excluded = mtmv.getExcludedTriggerTables();
        Set<OlapTable> streamedTables = Sets.newLinkedHashSet();
        Set<BaseTableInfo> pctTableInfos = Sets.newHashSet();
        for (BaseColInfo pctInfo : mtmv.getMvPartitionInfo().getPctInfos()) {
            pctTableInfos.add(pctInfo.getTableInfo());
        }
        // Every table of the plan that is not a PCT table is read through its stream: the partition
        // refresh gives those tables a read mode (IvmRewriteContext.full).
        for (BaseTableInfo baseTableInfo : relation.getBaseTablesOneLevelAndFromView()) {
            if (pctTableInfos.contains(baseTableInfo)) {
                continue;
            }
            OlapTable baseTable = resolveIvmBaseTable(baseTableInfo);
            if (baseTable != null) {
                streamedTables.add(baseTable);
            }
        }
        // A PCT table is read through its stream only while the mapping of a refreshed partition names
        // it, which is exactly the mapping the reset read mode is built from.
        for (String mvPartitionName : mvPartitionNames) {
            for (MTMVRelatedTableIf relatedTable : context.getByPartitionName(mvPartitionName).keySet()) {
                if (relatedTable instanceof OlapTable) {
                    streamedTables.add((OlapTable) relatedTable);
                }
            }
        }
        for (OlapTable baseTable : streamedTables) {
            if (MTMVPartitionUtil.isTableExcluded(excluded,
                    new TableNameInfo(baseTable.getFullQualifiers()))) {
                continue;
            }
            if (usableIvmStream(mvDb, baseTable) == null) {
                LOG.warn("IVM stream is unusable for the partitions this refresh plans, mv={}, baseTable={}",
                        mtmv.getName(), baseTable.getName());
                return true;
            }
        }
        return false;
    }

    /**
     * A base table of the plan, or null when it cannot be resolved -- which says nothing about its stream,
     * so a caller that is only judging streams skips it.
     */
    private OlapTable resolveIvmBaseTable(BaseTableInfo baseTableInfo) {
        try {
            return (OlapTable) MTMVUtil.getTable(baseTableInfo);
        } catch (Exception e) {
            LOG.warn("Cannot resolve base table {} of mv={}", baseTableInfo, mtmv.getName(), e);
            return null;
        }
    }

    /** The MV's stream for the base table, or null when it is missing or cannot be used. */
    private BaseTableStream usableIvmStream(Database mvDb, OlapTable baseTable) {
        TableIf stream = mvDb.getTableNullable(
                IvmUtil.streamName(mtmv.getId(), baseTable.getFullQualifiers()));
        if (stream instanceof BaseTableStream
                && IvmUtil.isIvmStreamUsable((BaseTableStream) stream, baseTable)) {
            return (BaseTableStream) stream;
        }
        return null;
    }

    private void reconcileIvmStreams(ConnectContext ctx) throws JobException {
        try {
            Database mvDb = (Database) mtmv.getDatabase();
            Set<TableNameInfo> excluded = mtmv.getExcludedTriggerTables();
            for (BaseTableInfo baseTableInfo : relation.getBaseTables()) {
                OlapTable baseTable = (OlapTable) MTMVUtil.getTable(baseTableInfo);
                if (MTMVPartitionUtil.isTableExcluded(excluded,
                        new TableNameInfo(baseTable.getFullQualifiers()))) {
                    continue;
                }
                if (usableIvmStream(mvDb, baseTable) != null) {
                    continue;
                }
                CreateMTMVCommand.createTableStream(ctx, mvDb, mtmv, baseTable);
            }
        } catch (UserException e) {
            throw new JobException("Failed to reconcile IVM streams for mv=" + mtmv.getName(), e);
        }
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
        AtomicReference<IvmPlanSignature> signatureRef = new AtomicReference<>();
        try {
            MTMVPlanUtil.executeCommand(mtmvCtx, command, statementContext,
                    getRefreshAuditStmt(refreshMode, refreshPartitionNames),
                    createRefreshConsumer(signatureRef));
        } finally {
            try {
                recordQueryId(DebugUtil.printId(mtmvCtx.queryId()));
            } finally {
                closeExecutionContext(mtmvCtx);
            }
        }
        if (getStatus() == TaskStatus.CANCELED) {
            throw new JobException("task is CANCELED");
        }
        if (!rewriteContext.isPresent()) {
            return null;
        }
        return Objects.requireNonNull(signatureRef.get(),
                "IVM COMPLETE refresh did not produce a plan signature");
    }

    /**
     * Builds the executor consumer for a COMPLETE refresh: registers the executing
     * statement for cancellation, and when the command finishes (consumer invoked with
     * {@code null}), captures the IVM plan signature into {@code signatureRef} from the
     * still-registered executor before it is cleared.
     */
    private Consumer<StmtExecutor> createRefreshConsumer(AtomicReference<IvmPlanSignature> signatureRef) {
        return executor -> {
            // executor == null 且 this.executor 仍注册:executeCommand 的 finally 回调,
            // 命令已执行完成,在 registerExecutor(null) 清空字段前先摘取 IVM 签名。
            if (executor == null && this.executor != null && this.executor.planner() != null) {
                ((NereidsPlanner) this.executor.planner())
                        .getCascadesContext()
                        .getIvmRewriteResult()
                        .ifPresent(r -> signatureRef.set(r.getPlanSignature()));
            }
            registerExecutor(executor);
        };
    }

    private static void closeExecutionContext(ConnectContext executionContext) {
        try {
            if (executionContext.queryId() != null) {
                QeProcessorImpl.INSTANCE.unregisterQuery(executionContext.queryId());
            }
        } finally {
            StatementContext statementContext = executionContext.getStatementContext();
            if (statementContext != null) {
                statementContext.close();
            }
        }
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

    /**
     * Registers the currently executing statement (or clears it with {@code null}).
     * Called by the executeCommand executor consumer: non-null right before the command
     * runs, null after it finishes. The cancel path reads this to interrupt execution.
     * Rejects registration with an exception when the task has already been cancelled,
     * so the command is not started at all. Uses an unchecked exception so the method
     * reference can be used as a {@code Consumer<StmtExecutor>}.
     */
    public void registerExecutor(StmtExecutor executor) {
        if (executor != null && getStatus() == TaskStatus.CANCELED) {
            this.executor = null;
            throw new IllegalStateException("task is CANCELED");
        }
        this.executor = executor;
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
                .setStringVal(taskContext == null ? FeConstants.null_string : GSON.toJson(taskContext)));
        trow.addToColumnValue(
                new TCell().setStringVal(refreshMode == null ? FeConstants.null_string : refreshMode.toString()));
        trow.addToColumnValue(
                new TCell().setStringVal(
                        needRefreshPartitions == null ? FeConstants.null_string : GSON.toJson(
                                needRefreshPartitions)));
        trow.addToColumnValue(
                new TCell().setStringVal(
                        completedPartitions == null ? FeConstants.null_string : GSON.toJson(
                                completedPartitions)));
        trow.addToColumnValue(
                new TCell().setStringVal(getProgress()));
        trow.addToColumnValue(
                new TCell().setStringVal(lastQueryId));
        trow.addToColumnValue(new TCell().setStringVal(
                computeGroup == null || computeGroup.isEmpty() ? FeConstants.null_string : computeGroup));
        trow.addToColumnValue(new TCell().setStringVal(
                ivmFallbackReason == null ? FeConstants.null_string : ivmFallbackReason));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(ivmRebuiltPartitions)));
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

    public MTMVTaskContext getTaskContext() {
        return taskContext;
    }

    public long getMtmvSchemaChangeVersion() {
        return mtmvSchemaChangeVersion;
    }

    /** The requirement each refreshed partition was read under; see captureLatestEpochs. */
    public Map<String, Long> getIvmCapturedEpochs() {
        return ivmCapturedEpochs;
    }

    public String getRefreshedIvmPlanSignature() {
        return refreshedIvmPlanSignature;
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
