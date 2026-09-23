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

package org.apache.doris.catalog;

import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.catalog.OlapTableFactory.MTMVParams;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MetaLockUtils;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTableInfo;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.mtmv.BaseColInfo;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.EnvInfo;
import org.apache.doris.mtmv.MTMVAlterOpType;
import org.apache.doris.mtmv.MTMVCache;
import org.apache.doris.mtmv.MTMVCacheManager;
import org.apache.doris.mtmv.MTMVJobInfo;
import org.apache.doris.mtmv.MTMVJobManager;
import org.apache.doris.mtmv.MTMVPartitionExpander;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVPartitionState;
import org.apache.doris.mtmv.MTMVPartitionUtil;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.mtmv.MTMVPropertyUtil;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVRefreshState;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
import org.apache.doris.mtmv.MTMVRefreshInfo;
import org.apache.doris.mtmv.MTMVRefreshPartitionSnapshot;
import org.apache.doris.mtmv.MTMVRefreshSnapshot;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVSnapshotIf;
import org.apache.doris.mtmv.MTMVStatus;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.mtmv.ivm.IvmInfo;
import org.apache.doris.mtmv.ivm.IvmUtil;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.analysis.SessionVarGuardRewriter;
import org.apache.doris.persist.AlterMTMV;
import org.apache.doris.persist.EditLog.EditLogItem;
import org.apache.doris.persist.OperationType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class MTMV extends OlapTable {
    private static final Logger LOG = LogManager.getLogger(MTMV.class);
    private ReentrantReadWriteLock mvRwLock;

    @SerializedName("ri")
    private MTMVRefreshInfo refreshInfo;
    @SerializedName("qs")
    private String querySql;
    @SerializedName("s")
    private MTMVStatus status;
    @Deprecated
    @SerializedName("ei")
    private EnvInfo envInfo;
    @SerializedName("ji")
    private MTMVJobInfo jobInfo;
    @SerializedName("mp")
    private Map<String, String> mvProperties;
    @SerializedName("r")
    private MTMVRelation relation;
    @SerializedName("mpi")
    private MTMVPartitionInfo mvPartitionInfo;
    @SerializedName("rs")
    private MTMVRefreshSnapshot refreshSnapshot = new MTMVRefreshSnapshot();
    @SerializedName("ii")
    private IvmInfo ivmInfo = new IvmInfo();
    /**
     * The refresh epoch of every MV partition, keyed by MV partition name.
     *
     * <p>Deliberately on MTMV rather than inside {@link IvmInfo}: the field is shared, the behaviour is
     * not. Both kinds of MV carry it, but only an IVM MV ever populates it -- alignment, invalidation,
     * the ADD_TASK payload and ALTER_PARTITION_STATES are all no-ops for a non-IVM MV, so for one an
     * empty map is the complete answer.
     *
     * <p>Never null, so a reader has no null case to answer: an MV is created with an empty map, and
     * {@link #gsonPostProcess()} gives an MV loaded from an image written before the field existed the
     * same one.
     */
    @SerializedName("pst")
    private Map<String, MTMVPartitionState> partitionStates = Maps.newLinkedHashMap();
    // Increased every time rewrite cache is invalidated to prevent publishing stale in-flight cache builds.
    private transient long rewriteCacheGeneration;
    private long schemaChangeVersion;
    @SerializedName(value = "sv")
    private Map<String, String> sessionVariables;

    // For deserialization
    public MTMV() {
        type = TableType.MATERIALIZED_VIEW;
        mvRwLock = new ReentrantReadWriteLock(true);
    }

    MTMV(MTMVParams params) {
        super(
                params.tableId,
                params.tableName,
                params.schema,
                params.keysType,
                params.partitionInfo,
                params.distributionInfo
        );
        this.type = TableType.MATERIALIZED_VIEW;
        this.querySql = params.querySql;
        this.refreshInfo = params.refreshInfo;
        this.status = new MTMVStatus();
        this.jobInfo = new MTMVJobInfo(MTMVJobManager.MTMV_JOB_PREFIX + params.tableId);
        this.mvProperties = params.mvProperties;
        this.mvPartitionInfo = params.mvPartitionInfo;
        this.relation = params.relation;
        this.refreshSnapshot = new MTMVRefreshSnapshot();
        this.ivmInfo = new IvmInfo();
        this.ivmInfo.setEnableIvm(params.enableIvm);
        if (params.enableIvm) {
            this.ivmInfo.setUseFullKeys(MTMVPropertyUtil.isIvmUseFullKeys(params.mvProperties));
            if (params.ivmPlanSignature == null) {
                throw new IllegalArgumentException("IVM materialized view requires a plan signature");
            }
            this.ivmInfo.setPlanSignature(params.ivmPlanSignature);
        }
        this.envInfo = new EnvInfo(-1L, -1L);
        this.sessionVariables = params.sessionVariables;
        mvRwLock = new ReentrantReadWriteLock(true);
    }

    @Override
    public boolean needReadLockWhenPlan() {
        return true;
    }

    public MTMVRefreshInfo getRefreshInfo() {
        readMvLock();
        try {
            return refreshInfo;
        } finally {
            readMvUnlock();
        }
    }

    public String getQuerySql() {
        return querySql;
    }

    public MTMVStatus getStatus() {
        readMvLock();
        try {
            return status;
        } finally {
            readMvUnlock();
        }
    }

    public EnvInfo getEnvInfo() {
        return envInfo;
    }

    public MTMVJobInfo getJobInfo() {
        readMvLock();
        try {
            return jobInfo;
        } finally {
            readMvUnlock();
        }
    }

    public MTMVRelation getRelation() {
        readMvLock();
        try {
            return relation;
        } finally {
            readMvUnlock();
        }
    }

    public MTMVRefreshInfo alterRefreshInfo(MTMVRefreshInfo newRefreshInfo) {
        writeMvLock();
        try {
            return refreshInfo.updateNotNull(newRefreshInfo);
        } finally {
            writeMvUnlock();
        }
    }

    /**
     * Applies a status change, together with the invalidation it stands for: the version bump that
     * discards a task result computed against the state being replaced, and the snapshot drop that stops
     * the transparent rewrite serving rows from it.
     *
     * <p>This is the step that <em>applies</em> a change, not the one that records it -- it takes the MV
     * lock and moves all three, but nothing here is journaled. It is what {@code Alter#processAlterMTMV}
     * calls for an {@code ALTER_STATUS} op, both live and on replay; a live caller reaches it through
     * {@link #invalidateWholeMv}, which goes the journaled way round. A new invalidation belongs there:
     * calling this directly would leave the MV in a state a restart forgets.
     */
    public MTMVStatus alterStatus(MTMVStatus newStatus) {
        writeMvLock();
        try {
            // only can update state, refresh state will be change by add task
            this.schemaChangeVersion++;
            this.refreshSnapshot = new MTMVRefreshSnapshot();
            return this.status.updateStateAndDetail(newStatus);
        } finally {
            writeMvUnlock();
        }
    }

    public void processBaseViewChange(String schemaChangeDetail) {
        writeMvLock();
        try {
            this.schemaChangeVersion++;
            this.status.setState(MTMVState.SCHEMA_CHANGE);
            this.status.setSchemaChangeDetail(schemaChangeDetail);
            this.refreshSnapshot = new MTMVRefreshSnapshot();
        } finally {
            writeMvUnlock();
        }
    }

    public boolean isIvm() {
        return getIvmInfo().isEnableIvm();
    }

    public long getNextSequencePrefix() {
        return Config.isCloudMode() ? getNextVersion() : getIvmInfo().getSequencePrefix() + 1;
    }

    public boolean addTaskResult(AlterMTMV alterMTMV, boolean isReplay) {
        MTMVTask task = alterMTMV.getTask();
        MTMVRelation relation = alterMTMV.getRelation();
        Map<String, MTMVRefreshPartitionSnapshot> partitionSnapshots = alterMTMV.getPartitionSnapshots();
        MTMVCache mtmvCacheWithGuard = null;
        MTMVCache mtmvCacheWithoutGuard = null;
        boolean needUpdateCache = false;
        long cacheGeneration = -1;
        if (task.getStatus() == TaskStatus.SUCCESS && !Env.isCheckpointThread()
                && !Config.enable_check_compatibility_mode) {
            needUpdateCache = true;
            readMvLock();
            try {
                cacheGeneration = rewriteCacheGeneration;
            } finally {
                readMvUnlock();
            }
            try {
                // The replay thread may not have initialized the catalog yet to avoid getting stuck due
                // to connection issues such as S3, so it is directly set to null.
                if (!isReplay && Env.getCurrentEnv().getMtmvCacheManager().isEnabled()) {
                    ConnectContext currentContext = ConnectContext.get();
                    // shouldn't do this while holding mvWriteLock
                    // TODO: these two cache compute share something same, can be simplified in future
                    mtmvCacheWithGuard = createRewriteCache(currentContext, true, true);
                    mtmvCacheWithoutGuard = createRewriteCache(currentContext, true, false);
                }
            } catch (Throwable e) {
                mtmvCacheWithGuard = null;
                mtmvCacheWithoutGuard = null;
                LOG.warn("generate cache failed", e);
            }
        }
        EditLogItem editLogItem;
        writeMvLock();
        try {
            if (!isReplay && task.getMtmvSchemaChangeVersion() != this.schemaChangeVersion) {
                LOG.warn(
                        "addTaskResult failed, schemaChangeVersion has changed. "
                                + "mvName: {}, taskId: {}, taskSchemaChangeVersion: {}, "
                                + "mvSchemaChangeVersion: {}",
                        name, task.getTaskId(), task.getMtmvSchemaChangeVersion(), this.schemaChangeVersion);
                return false;
            }
            if (isReplay) {
                if (alterMTMV.getIvmInfo() != null) {
                    // Replay the final IVM state; ADD_TASK does not change schemaChangeVersion.
                    ivmInfo = new IvmInfo(alterMTMV.getIvmInfo());
                }
                if (alterMTMV.getPartitionStates() != null) {
                    // A journal written before the field existed carries no state at all: leave the
                    // partition states alone rather than clearing them. What a payload does carry is
                    // merged rather than assigned: a task result journals only the partitions it
                    // published, so the entries it does not mention belong to other records -- an
                    // invalidation that ran during the task, or an entry alignment added -- and
                    // assigning would drop them. The state-map channel proper
                    // (ALTER_PARTITION_STATES) still replaces, because that one carries the whole map.
                    for (Entry<String, MTMVPartitionState> entry : alterMTMV.getPartitionStates().entrySet()) {
                        partitionStates.put(entry.getKey(), new MTMVPartitionState(entry.getValue()));
                    }
                }
            } else {
                if (ivmInfo.isEnableIvm()) {
                    // The batches this task committed now hold data read at the epoch they captured, so
                    // the requirement is met for exactly those partitions. Recorded for a failed task
                    // too: its snapshots and epochs only ever cover the batches that succeeded, and
                    // leaving their rebuilt work unrecorded would only make the next refresh redo it.
                    applyRefreshedEpochs(task.getIvmCapturedEpochs());
                }
            }
            if (task.getStatus() == TaskStatus.SUCCESS) {
                this.status.setState(MTMVState.NORMAL);
                this.status.setSchemaChangeDetail(null);
                this.status.setRefreshState(MTMVRefreshState.SUCCESS);
                this.relation = relation;
                if (!isReplay && ivmInfo.isEnableIvm()) {
                    String refreshedIvmPlanSignature = task.getRefreshedIvmPlanSignature();
                    if (refreshedIvmPlanSignature != null) {
                        ivmInfo.setPlanSignature(refreshedIvmPlanSignature);
                    }
                }
                // The refresh publishes a new plan, so every cache built before this commit is stale.
                // Bump before publishing so an in-flight build cannot pass its generation check later.
                boolean publishCache = needUpdateCache && cacheGeneration == rewriteCacheGeneration && !isDropped;
                rewriteCacheGeneration++;
                if (needUpdateCache) {
                    MTMVCacheManager manager = Env.getCurrentEnv().getMtmvCacheManager();
                    if (publishCache && mtmvCacheWithGuard != null) {
                        manager.put(this.id, true, mtmvCacheWithGuard);
                    } else {
                        manager.invalidate(this.id);
                    }
                    if (publishCache && mtmvCacheWithoutGuard != null) {
                        manager.put(this.id, false, mtmvCacheWithoutGuard);
                    }
                }
            } else {
                this.status.setRefreshState(MTMVRefreshState.FAIL);
            }
            this.jobInfo.addHistoryTask(task);
            compatiblePctSnapshot(partitionSnapshots);
            // What this task wrote is described by the epochs just recorded, so a partition the result
            // left dirty is left out: its snapshot would otherwise come back after an invalidation
            // dropped it, and transparent rewrite reads that map to decide what it may serve.
            Map<String, MTMVRefreshPartitionSnapshot> snapshotsToWrite = partitionSnapshots;
            if (!isReplay && ivmInfo.isEnableIvm()) {
                snapshotsToWrite = snapshotsOfCleanPartitions(partitionSnapshots);
            }
            this.refreshSnapshot.updateSnapshots(snapshotsToWrite, getPartitionNames());
            Env.getCurrentEnv().getMtmvService()
                    .refreshComplete(this, relation, task);
            if (isReplay) {
                return true;
            }
            if (ivmInfo.isEnableIvm()) {
                alterMTMV.setIvmInfo(ivmInfo);
                // Only the partitions this result published, not the whole map: the map has one entry per
                // MV partition, so a scheduled refresh of an MV with many partitions would deep-copy and
                // journal all of them on every run to say what almost all of them already said. What the
                // record has to carry is the change; the replay merges it. A result that published nothing
                // carries nothing, which is what a payload without the member already means.
                alterMTMV.setPartitionStates(publishedPartitionStates(task.getIvmCapturedEpochs()));
                // Journal the map that was applied, not the one the task proposed: a partition this result
                // left dirty was dropped from it above, and a replay that restored the raw map would put
                // back the snapshot of a partition an invalidation has just cleared. The replay skips the
                // filter, so what the payload carries is exactly what a restart ends up with.
                alterMTMV.setPartitionSnapshots(snapshotsToWrite);
            }
            editLogItem = submitAlterLog(alterMTMV);
        } finally {
            writeMvUnlock();
        }
        // Preserve MV-lock order in the journal without waiting while holding the lock.
        editLogItem.await();
        return true;
    }

    public void alterMvProperties(AlterMTMV alterMTMV, boolean isReplay) {
        EditLogItem editLogItem;
        writeMvLock();
        try {
            Map<String, String> mvProperties = alterMTMV.getMvProperties();
            // Read the old values before the properties are applied, and unconditionally: a property that is
            // not part of this ALTER has to be compared against the value the MV actually holds. Reading it
            // only when its key is present would compare an empty default against the real value, report a
            // change that is not there, and drop the snapshot of an unrelated ALTER.
            Set<TableNameInfo> oldExcludedTriggerTables = parseExcludedTriggerTables();
            Map<TableNameInfo, Integer> oldWindowLimits =
                    MTMVPropertyUtil.getIvmPartitionWindowLimit(this.mvProperties);
            Map<String, String> oldSyncWindow = MTMVPropertyUtil.partitionSyncWindowOf(this.mvProperties);
            this.mvProperties.putAll(mvProperties);
            // The one thing a property change can owe the refresh baseline: a whole-MV rebuild, when it
            // brings base table partitions back into the set the MV maintains. Their stream backlog was
            // skipped while they were outside that set, so no delta can repair them -- and the rebuild is
            // whole-MV rather than per-partition because it covers the partitions partition sync has not
            // created yet. invalidateWholeMv owns all of it: the state the refresh reads, the version bump
            // that discards a task result computed before the change, and the snapshot drop that stops
            // transparent rewrite serving rows from it.
            //
            // Narrowing that set owes nothing. The MV's rows for a table it no longer maintains are allowed
            // to be stale by design, and the snapshot entry describing them is skipped by the next
            // incremental refresh anyway, so dropping the whole snapshot and discarding a running task
            // result for them buys nothing. A change that leaves the maintained set alone owes nothing
            // either.
            if (isReplay) {
                // The property change itself is applied above. Nothing else on this path has to run for a
                // replay: the state, the version and the snapshot a whole-MV invalidation moves come back
                // from the status record that precedes this one, through MTMV#alterStatus, and this
                // property record never carried a snapshot.
                return;
            }
            if (rebuildsWholeMv(oldExcludedTriggerTables, oldWindowLimits, oldSyncWindow)) {
                // Journaled on its own record, ahead of the property change below; a replay applies both
                // in that order.
                invalidateWholeMv("The MV's refresh baseline changed with its properties");
            }
            editLogItem = submitAlterLog(alterMTMV);
        } finally {
            writeMvUnlock();
        }
        editLogItem.await();
    }

    /**
     * Whether a property change brings base table partitions back into the set the MV maintains, and so
     * owes a whole-MV rebuild.
     *
     * <p>Takes the values the MV held before the change; see the call site for why they are read
     * unconditionally. Widening decides on its own: a change that both takes a partition out of the
     * maintained set and puts one back is the rebuild, because the partition coming back is the one whose
     * backlog was skipped.
     */
    private boolean rebuildsWholeMv(Set<TableNameInfo> oldExcludedTriggerTables,
            Map<TableNameInfo, Integer> oldWindowLimits, Map<String, String> oldSyncWindow) {
        return unexcludesABaseTable(oldExcludedTriggerTables)
                || widensPartitionWindowLimit(oldWindowLimits)
                || widensSyncWindow(oldSyncWindow);
    }

    /**
     * Whether this MV has an IVM baseline to maintain at all, which every widening check needs.
     *
     * <p>No null check on {@code ivmInfo}: it is initialized where an MV is built and
     * {@link #gsonPostProcess()} gives an MV loaded from an image written before the field existed the
     * same one, so it is non-null by the time anything reads it.
     */
    private boolean maintainsIvmBaseline() {
        return ivmInfo.isEnableIvm() && relation != null && relation.getBaseTables() != null;
    }

    /**
     * Whether a base table of this MV stopped being excluded.
     *
     * <p>An excluded table has no stream, so the partitions the MV read from it have no backlog to apply;
     * while it was excluded the MV did not maintain them.
     */
    private boolean unexcludesABaseTable(Set<TableNameInfo> oldExcludedTriggerTables) {
        if (!maintainsIvmBaseline()) {
            return false;
        }
        Set<TableNameInfo> newExcludedTriggerTables = parseExcludedTriggerTables();
        for (BaseTableInfo baseTableInfo : relation.getBaseTables()) {
            TableNameInfo baseTableName = new TableNameInfo(baseTableInfo.getCtlName(),
                    baseTableInfo.getDbName(), baseTableInfo.getTableName());
            if (MTMVPartitionUtil.isTableExcluded(oldExcludedTriggerTables, baseTableName)
                    && !MTMVPartitionUtil.isTableExcluded(newExcludedTriggerTables, baseTableName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether an ivm_partition_window_limit was removed or enlarged for some base table, which brings the
     * partitions the windowed refreshes skipped back into range with their backlog unapplied.
     */
    private boolean widensPartitionWindowLimit(Map<TableNameInfo, Integer> oldWindowLimits) {
        if (!maintainsIvmBaseline()) {
            return false;
        }
        Map<TableNameInfo, Integer> newWindowLimits =
                MTMVPropertyUtil.getIvmPartitionWindowLimit(this.mvProperties);
        for (BaseTableInfo baseTableInfo : relation.getBaseTables()) {
            TableNameInfo baseTableName = new TableNameInfo(baseTableInfo.getCtlName(),
                    baseTableInfo.getDbName(), baseTableInfo.getTableName());
            int oldLimit = MTMVPropertyUtil.getPartitionWindowLimit(oldWindowLimits, baseTableName);
            if (oldLimit == -1) {
                continue;
            }
            int newLimit = MTMVPropertyUtil.getPartitionWindowLimit(newWindowLimits, baseTableName);
            if (newLimit == -1 || newLimit > oldLimit) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether a partition_sync_limit window was widened.
     *
     * <p>A window that starts applying, a narrower one, and one that describes the same set as before all
     * leave the applied deltas intact: the partitions they take out are dropped by partition sync before
     * the refresh plans, and taking one back in is the widening this answers.
     */
    private boolean widensSyncWindow(Map<String, String> oldSyncWindow) {
        return MTMVPropertyUtil.partitionSyncWindowWidens(oldSyncWindow,
                MTMVPropertyUtil.partitionSyncWindowOf(this.mvProperties));
    }

    public long getGracePeriod() {
        readMvLock();
        try {
            if (!StringUtils.isEmpty(mvProperties.get(PropertyAnalyzer.PROPERTIES_GRACE_PERIOD))) {
                return Long.parseLong(mvProperties.get(PropertyAnalyzer.PROPERTIES_GRACE_PERIOD)) * 1000;
            } else {
                return 0L;
            }
        } finally {
            readMvUnlock();
        }
    }

    public Optional<String> getWorkloadGroup() {
        readMvLock();
        try {
            if (mvProperties.containsKey(PropertyAnalyzer.PROPERTIES_WORKLOAD_GROUP) && !StringUtils
                    .isEmpty(mvProperties.get(PropertyAnalyzer.PROPERTIES_WORKLOAD_GROUP))) {
                return Optional.of(mvProperties.get(PropertyAnalyzer.PROPERTIES_WORKLOAD_GROUP));
            }
            return Optional.empty();
        } finally {
            readMvUnlock();
        }
    }

    public boolean isUseForRewrite() {
        readMvLock();
        try {
            if (!StringUtils.isEmpty(mvProperties.get(PropertyAnalyzer.PROPERTIES_USE_FOR_REWRITE))) {
                return Boolean.valueOf(mvProperties.get(PropertyAnalyzer.PROPERTIES_USE_FOR_REWRITE));
            }
            // default is true
            return true;
        } finally {
            readMvUnlock();
        }
    }

    public int getRefreshPartitionNum() {
        readMvLock();
        try {
            if (!StringUtils.isEmpty(mvProperties.get(PropertyAnalyzer.PROPERTIES_REFRESH_PARTITION_NUM))) {
                int value = Integer.parseInt(mvProperties.get(PropertyAnalyzer.PROPERTIES_REFRESH_PARTITION_NUM));
                return value < 1 ? MTMVTask.DEFAULT_REFRESH_PARTITION_NUM : value;
            } else {
                return MTMVTask.DEFAULT_REFRESH_PARTITION_NUM;
            }
        } finally {
            readMvUnlock();
        }
    }

    public Set<TableNameInfo> getExcludedTriggerTables() {
        readMvLock();
        try {
            return parseExcludedTriggerTables();
        } finally {
            readMvUnlock();
        }
    }

    private Set<TableNameInfo> parseExcludedTriggerTables() {
        return MTMVPropertyUtil.parseTableNameInfos(
                mvProperties.get(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES));
    }

    public Set<TableNameInfo> getQueryRewriteConsistencyRelaxedTables() {
        readMvLock();
        try {
            return MTMVPropertyUtil.parseTableNameInfos(
                    mvProperties.get(PropertyAnalyzer.ASYNC_MV_QUERY_REWRITE_CONSISTENCY_RELAXED_TABLES));
        } finally {
            readMvUnlock();
        }
    }

    /**
     * Called when in query; should use one connection context for the query.
     * Returns the rewrite cache matching the current session variables, rebuilding it on demand.
     */
    public MTMVCache getOrGenerateCache(ConnectContext connectionContext) throws
            org.apache.doris.nereids.exceptions.AnalysisException {
        Map<String, String> currentSessionVars =
                connectionContext.getSessionVariable().getAffectQueryResultInPlanVariables();
        boolean sessionVarsMatch = SessionVarGuardRewriter.checkSessionVariablesMatch(
                currentSessionVars, this.sessionVariables);
        boolean guarded = !sessionVarsMatch;
        MTMVCacheManager manager = Env.getCurrentEnv().getMtmvCacheManager();
        StatementContext statementContext = connectionContext.getStatementContext();

        while (true) {
            long cacheGeneration;
            MTMVCache cached;
            readMvLock();
            try {
                cached = manager.isEnabled() ? manager.getIfPresent(this.id, guarded) : null;
                if (cached == null && statementContext != null) {
                    cached = statementContext.getQueryLocalMtmvCache(this.id, guarded);
                }
                cacheGeneration = rewriteCacheGeneration;
            } finally {
                readMvUnlock();
            }
            if (cached != null) {
                return cached;
            }
            MTMVCache generated = createRewriteCache(connectionContext, false, guarded);
            readMvLock();
            try {
                if (cacheGeneration != rewriteCacheGeneration) {
                    // Someone invalidated between our snapshot and now; drop the stale build and retry.
                    continue;
                }
                if (manager.isEnabled()) {
                    MTMVCache existing = manager.getIfPresent(this.id, guarded);
                    if (existing != null) {
                        return existing;
                    }
                    if (!isDropped) {
                        manager.put(this.id, guarded, generated);
                    }
                } else if (statementContext != null && !isDropped) {
                    // Global cache is disabled (maximumSize=0); keep one copy for this statement only.
                    MTMVCache existing = statementContext.getQueryLocalMtmvCache(this.id, guarded);
                    if (existing != null) {
                        return existing;
                    }
                    statementContext.putQueryLocalMtmvCache(this.id, guarded, generated);
                }
                return generated;
            } finally {
                readMvUnlock();
            }
        }
    }

    public Map<String, String> getMvProperties() {
        readMvLock();
        try {
            return mvProperties;
        } finally {
            readMvUnlock();
        }
    }

    public MTMVPartitionInfo getMvPartitionInfo() {
        return mvPartitionInfo;
    }

    public MTMVRefreshSnapshot getRefreshSnapshot() {
        return refreshSnapshot;
    }

    public boolean hasRefreshSnapshot() {
        readMvLock();
        try {
            // IVM only needs to know whether a baseline has ever been built.
            // A newly added MV partition legitimately has no PCT snapshot yet,
            // but that must not block row-level incremental refresh.
            return !MapUtils.isEmpty(refreshSnapshot.getPartitionSnapshots());
        } finally {
            readMvUnlock();
        }
    }

    public IvmInfo getIvmInfo() {
        writeMvLock();
        try {
            if (ivmInfo == null) {
                ivmInfo = new IvmInfo();
            }
            return ivmInfo;
        } finally {
            writeMvUnlock();
        }
    }

    // ALTER_IVM_INFO replay applies a detached snapshot here. Live IVM changes submit their journal
    // from the mutating method so mutation and journal enqueue stay under the same MV write lock.
    public void alterIvmInfo(IvmInfo ivmInfo) {
        writeMvLock();
        try {
            this.ivmInfo = new IvmInfo(ivmInfo);
        } finally {
            writeMvUnlock();
        }
    }

    /**
     * A snapshot of the partition states, taken under the MV read lock.
     *
     * <p>The caller gets its own map and its own state objects, not the ones the MV owns: handing those
     * out would let a caller add or change an entry while {@link #addTaskResult} copies the same map
     * into the journal, and a replay that replaces the field would leave the caller's reference
     * pointing at state that is no longer the MV's. Changing the states is the MV's own job, under its
     * write lock.
     */
    public Map<String, MTMVPartitionState> getPartitionStates() {
        readMvLock();
        try {
            return Collections.unmodifiableMap(MTMVPartitionState.copyOf(partitionStates));
        } finally {
            readMvUnlock();
        }
    }

    // ALTER_PARTITION_STATES replay applies a detached snapshot here, mirroring alterIvmInfo(). Live
    // invalidation changes submit their journal from the mutating method instead.
    //
    // A payload without the member carries no state at all, which is not the same as an empty map that
    // says the states are now empty: leaving them alone is the only answer that cannot lose state.
    public void alterPartitionStates(Map<String, MTMVPartitionState> partitionStates) {
        replayAlterPartitionStates(partitionStates, null);
    }

    /**
     * ALTER_PARTITION_STATES replay: applies the states the payload carries, and drops the snapshots it
     * names. Both in one lock acquisition, because a reader that saw the new requirement while the
     * snapshot was still there could let a transparent rewrite serve rows the rebuild has to replace.
     *
     * <p>A payload without the states carries none, which is not the same as an empty map that says the
     * states are now empty: leaving them alone is the only answer that cannot lose state.
     */
    public void replayAlterPartitionStates(Map<String, MTMVPartitionState> partitionStates,
            Set<String> removedSnapshotPartitions) {
        writeMvLock();
        try {
            if (partitionStates != null) {
                this.partitionStates = MTMVPartitionState.copyOf(partitionStates);
            }
            refreshSnapshot.removeSnapshots(removedSnapshotPartitions);
        } finally {
            writeMvUnlock();
        }
    }

    /**
     * The {@code latestEpoch} of the given MV partitions, taken under the MV read lock.
     *
     * <p>This is the value a refresh has to remember: what it read from the base tables is described by
     * the requirement in force when it started reading, so writing that value back as the new
     * {@code refreshEpoch} is what keeps an invalidation arriving mid-refresh from being swallowed. A
     * partition without an entry is left out -- a caller writes an epoch only for what it captured.
     */
    public Map<String, Long> getLatestEpochs(Set<String> partitionNames) {
        if (CollectionUtils.isEmpty(partitionNames)) {
            return Collections.emptyMap();
        }
        // Sized before the lock: the state map is what needs it, and building the map is not part of that.
        Map<String, Long> res = Maps.newHashMapWithExpectedSize(partitionNames.size());
        readMvLock();
        try {
            for (String partitionName : partitionNames) {
                MTMVPartitionState state = partitionStates.get(partitionName);
                if (state != null) {
                    res.put(partitionName, state.getLatestEpoch());
                }
            }
            return res;
        } finally {
            readMvUnlock();
        }
    }

    /**
     * Brings the partition states in line with the MV's partitions: every partition gets an entry, and
     * every entry whose partition is gone is dropped.
     *
     * <p>Alignment is what makes "the partition exists" and "the entry exists" the same thing, and it is
     * why an invalidation cannot miss: rows are only written by a refresh, and every refresh aligns
     * before it reads a base table, so a partition that holds rows always has an entry for the mark to
     * land on. The other direction is what makes the criterion safe -- an entry created here describes a
     * partition with no rows yet, so requiring one generation of it discards no requirement that was
     * made earlier.
     *
     * <p>What it changes is journaled, because the entry has to be on disk before the rows it describes
     * can be: a crash between this and the task result would otherwise leave a partition that holds rows
     * with no entry at all, and every later invalidation of it would find nothing to land on. That is the
     * one shape in which the criterion cannot be read -- "no entry" is supposed to mean "no rows" -- so
     * the entry is made durable before any base table is read rather than derived again on the next run.
     *
     * <p>It is deliberately not a hook on every path that creates or drops a partition. An entry is
     * derived state, and rebuilding it from the live partition set also repairs whatever a crash left
     * behind: the drop of a partition and the removal of its entry are two journal records, and only
     * their order -- partition first -- is safe, which leaves at most a stale entry that the next
     * alignment drops.
     *
     * <p>Only an IVM MV is aligned. For a non-IVM MV the map stays as it is, and every reader treats
     * "empty" and "no state" the same.
     */
    public void alignPartitionStates(Set<String> livePartitionNames) {
        if (!isIvm()) {
            return;
        }
        // Copied up front: callers pass what OlapTable holds, and that is mutated under the table's own
        // write lock, not this one. Iterating the live collection could see it change.
        Set<String> livePartitions = Sets.newHashSet(livePartitionNames);
        EditLogItem editLogItem = null;
        writeMvLock();
        try {
            boolean changed = partitionStates.keySet().retainAll(livePartitions);
            for (String partitionName : livePartitions) {
                if (!partitionStates.containsKey(partitionName)) {
                    partitionStates.put(partitionName, MTMVPartitionState.initial());
                    changed = true;
                }
            }
            if (changed) {
                editLogItem = submitPartitionStatesChange();
            }
        } finally {
            writeMvUnlock();
        }
        if (editLogItem != null) {
            editLogItem.await();
        }
    }

    /**
     * The MV partitions whose data has to be rebuilt instead of caught up incrementally: their rows were
     * read before a change of a base table that emits no row binlog, so no delta can remove them.
     *
     * <p>Intersected with the partitions the MV has, because one can be dropped while a task decides.
     * The read lock is enough: a requirement only ever grows, so a value read here is at most the one in
     * force when the caller acts, and what is written back is the value the refresh captured, not this.
     */
    public Set<String> getDirtyPartitions() {
        Set<String> res = Sets.newLinkedHashSet();
        // Neither of these needs the lock: the names come from the table, which its own lock protects,
        // and the selection is built from the state map read under the lock below.
        Set<String> livePartitionNames = getPartitionNames();
        readMvLock();
        try {
            for (Entry<String, MTMVPartitionState> entry : partitionStates.entrySet()) {
                if (entry.getValue().isDirty()) {
                    res.add(entry.getKey());
                }
            }
        } finally {
            readMvUnlock();
        }
        res.retainAll(livePartitionNames);
        return res;
    }

    /**
     * The snapshots of the partitions that are clean after this result's epochs were applied.
     *
     * <p>An invalidation that reached a partition while the task ran leaves it dirty, and its snapshot
     * must stay gone: dropping the entry is what keeps transparent rewrite away from rows the rebuild has
     * to replace, and a result written back afterwards would undo exactly that. Removing only the entry
     * keeps the rest of the map, which the removal on the invalidation side cannot express.
     *
     * <p>The caller holds the MV write lock and has already applied the epochs, so {@code isDirty} here
     * reads the state the data is actually described by.
     *
     * <p>Only an IVM MV has partition states, so only its write-back is narrowed here: every entry of a
     * non-IVM MV has no state to be dirty in and is written back as it always was.
     */
    private Map<String, MTMVRefreshPartitionSnapshot> snapshotsOfCleanPartitions(
            Map<String, MTMVRefreshPartitionSnapshot> snapshots) {
        if (MapUtils.isEmpty(snapshots)) {
            return snapshots;
        }
        Map<String, MTMVRefreshPartitionSnapshot> res = Maps.newHashMapWithExpectedSize(snapshots.size());
        for (Entry<String, MTMVRefreshPartitionSnapshot> entry : snapshots.entrySet()) {
            MTMVPartitionState state = partitionStates.get(entry.getKey());
            // No entry means the partition was created after the alignment, so it can only hold rows this
            // task wrote; a dirty one needs its rebuild before anything may read it through the MV.
            if (state == null || !state.isDirty()) {
                res.put(entry.getKey(), entry.getValue());
            }
        }
        return res;
    }

    /**
     * Records the epochs the given partitions were read at, which is how a refresh turns a requirement
     * into the state of the data.
     *
     * <p>Only {@code refreshEpoch} is written: a refresh writes back the requirement it captured, and the
     * requirement may have been raised again since that capture. A payload built from the captured map
     * would overwrite the newer value and lose the rebuild it asks for, so {@code latestEpoch} is left
     * alone here.
     *
     * <p>The caller holds the MV write lock (it is applied together with the rest of a task result).
     */
    private void applyRefreshedEpochs(Map<String, Long> capturedEpochs) {
        if (MapUtils.isEmpty(capturedEpochs)) {
            return;
        }
        for (Entry<String, Long> entry : capturedEpochs.entrySet()) {
            MTMVPartitionState state = partitionStates.get(entry.getKey());
            if (state == null) {
                // The partition was dropped while the task ran, so its state went with it.
                continue;
            }
            state.setRefreshEpoch(entry.getValue());
        }
    }

    /**
     * The states a task result publishes: the partitions whose epochs this result just wrote.
     *
     * <p>Read under the MV write lock, after {@link #applyRefreshedEpochs}, so what it captures is the state
     * as published. A requirement raised during the task is carried along rather than recomputed: the
     * write-back only moves {@code refreshEpoch}, and a payload that omitted the newer {@code latestEpoch}
     * would let a replay restore the older one and lose the rebuild it asks for.
     */
    private Map<String, MTMVPartitionState> publishedPartitionStates(Map<String, Long> capturedEpochs) {
        if (MapUtils.isEmpty(capturedEpochs)) {
            return Collections.emptyMap();
        }
        Map<String, MTMVPartitionState> published = Maps.newLinkedHashMapWithExpectedSize(capturedEpochs.size());
        for (String partitionName : capturedEpochs.keySet()) {
            MTMVPartitionState state = partitionStates.get(partitionName);
            if (state != null) {
                published.put(partitionName, state);
            }
        }
        return published;
    }

    public void invalidateWholeMv(String detail) {
        Env.getCurrentEnv().alterMTMVStatus(new TableNameInfo(getQualifiedDbName(), getName()),
                new MTMVStatus(MTMVState.SCHEMA_CHANGE, detail));
    }

    /**
     * Mark the MV partitions that may hold rows read from the changed base table partitions as needing a
     * rebuild. When those partitions cannot be determined, the whole MV is marked instead.
     */
    /**
     * @return whether a barrier was recorded. The caller reports the two outcomes differently: a change
     *         that no MV partition reads leaves nothing to rebuild and must not be logged as one.
     */
    public boolean invalidateIvmBaseline(BaseTableInfo baseTableInfo, Map<String, Long> changedPartitions,
            String reason) {
        // Computed before the MV lock is taken, not inside it: the mapping reads the partition items of the
        // MV and of every PCT table, so it takes those tables' locks, and the MV lock has to stay a leaf
        // (nothing may be acquired under it) the way the rest of this class assumes. The selection does not
        // need to be atomic with the barrier it produces: the barrier is recorded under the lock below, and
        // the names it carries are intersected with the live partition names when they are consumed
        // (MTMVTask).
        Optional<Set<String>> affectedMvPartitions = selectAffectedMvPartitions(baseTableInfo,
                changedPartitions);
        if (affectedMvPartitions.isPresent() && affectedMvPartitions.get().isEmpty()) {
            // No MV partition reads any of the changed base partitions, so this change cannot leave
            // anything behind here: there is no barrier to persist, and skipping the version bump
            // keeps it from discarding the result of a task that is already running.
            LOG.debug("No MV partition is affected by changed base partitions, mv={}, baseTable={}, "
                    + "changedPartitions={}", name, baseTableInfo, changedPartitions);
            return false;
        }
        if (!affectedMvPartitions.isPresent()) {
            // A narrower rebuild could leave a partition holding rows of the changed base partition
            // untouched, and those rows cannot be repaired later: the change emitted no row binlog. The
            // whole MV is invalidated instead, which says "every partition, including the ones partition
            // sync has not created yet" -- what a per-partition requirement cannot express.
            invalidateWholeMv(reason);
            return true;
        }
        EditLogItem editLogItem;
        writeMvLock();
        try {
            // Placed: the partitions that read the change get the requirement raised, which is what sends
            // them to a rebuild while every other partition keeps catching up incrementally. No version
            // bump here -- a partial invalidation does not invalidate a task result, and the requirement it
            // raises survives the write-back by construction.
            Set<String> marked = markIvmPartitionsInvalidated(affectedMvPartitions.get());
            if (marked.isEmpty()) {
                LOG.debug("No MV partition holds the changed base partitions, mv={}, baseTable={}, "
                        + "changedPartitions={}", name, baseTableInfo, changedPartitions);
                return false;
            }
            editLogItem = submitPartitionStatesChange(marked);
        } finally {
            writeMvUnlock();
        }
        editLogItem.await();
        return true;
    }

    /**
     * Raises the requirement of the given MV partitions and drops their snapshots.
     *
     * <p>Only partitions that have an entry are marked: an entry is created before anything reads a base
     * table, so a partition without one holds no rows and there is nothing of its to rebuild. The two
     * halves belong together -- the requirement is what sends the partition to a rebuild, and the missing
     * snapshot is what keeps a transparent rewrite away from rows that are about to be replaced.
     *
     * <p>The caller holds the MV write lock, which is what keeps this read-modify-write of
     * {@code latestEpoch} from losing a concurrent invalidation, and which makes the journal enqueue
     * follow the mutation order.
     */
    private Set<String> markIvmPartitionsInvalidated(Set<String> mvPartitionNames) {
        Set<String> marked = Sets.newLinkedHashSet();
        for (String partitionName : mvPartitionNames) {
            MTMVPartitionState state = partitionStates.get(partitionName);
            if (state == null) {
                continue;
            }
            state.setLatestEpoch(state.getLatestEpoch() + 1);
            marked.add(partitionName);
        }
        refreshSnapshot.removeSnapshots(marked);
        return marked;
    }

    /**
     * Select the MV partitions that may hold rows read from the changed base table partitions.
     *
     * <p>This asks which MV partitions read the changed base partitions at all, instead of (as the
     * refresh snapshot based selection did) which of them had already seen them. The snapshot is a lower
     * bound that is allowed to lag: a base partition that was added after the snapshot was captured never
     * appears in it, so it can report "this partition never read the changed base partition" about a
     * partition that does hold its rows. Missing a partition here is not repaired by a later refresh --
     * dropping or truncating a base partition emits no row binlog, so the incremental path never learns
     * about those orphan rows and they stay in the MV forever.
     *
     * <p>Three cases have no answer in the mapping, and each of them must rebuild the whole MV instead:
     * a SELF_MANAGE MV (the mapping API answers nothing for it, although its single partition reads every
     * base partition); a base table that is not one of the MV's PCT tables (the mapping is seeded from
     * {@code getPctTables()} and never gains a table later, so a joined partition table that the MV's
     * partition column does not reach is not described at all); and a changed partition that is not in
     * the base table's metadata right now, which is how RECOVER PARTITION arrives here -- it marks before
     * the partition is added back, so at this point the partition is still in the recycle bin.
     *
     * <p>Locking is the fourth way to end up rebuilding everything, but it is contention rather than a
     * property of the MV: the tables whose partition items the mapping reads are locked with a bounded
     * tryLock, and the MV is rebuilt only while one of them is being written. See the comment at that
     * loop.
     *
     * <p>An empty result is meaningful, on the other hand: the mapping lists every base partition read by
     * the MV, so a changed base partition that no MV partition maps to is read by none of them.
     *
     * @param changedBasePartitions base partition name to partition id, never empty
     * @return {@link Optional#empty()} when the affected MV partitions cannot be determined, otherwise the
     *         (possibly empty) set of MV partition names that must be rebuilt
     */
    private Optional<Set<String>> selectAffectedMvPartitions(BaseTableInfo baseTableInfo,
            Map<String, Long> changedBasePartitions) {
        if (mvPartitionInfo.getPartitionType() == MTMVPartitionType.SELF_MANAGE) {
            return Optional.empty();
        }
        MTMVRelatedTableIf pctTable = findPctTable(baseTableInfo);
        if (pctTable == null) {
            return Optional.empty();
        }
        // Computing the mapping reads the partition items of the MV and of every PCT table, which means
        // taking their read locks. The caller already holds the changed table's write lock (a partition DDL
        // marks before it releases it), so these other reads must not block: two partition DDLs on two PCT
        // tables of this MV would otherwise each hold the write lock the other one needs, and acquiring in
        // id order cannot break a cycle whose first lock is already held. They are taken with a bounded
        // tryLock instead, the way the stream cleanup treats a busy table: a busy table means a writer is
        // involved, and then the whole MV is rebuilt. The list is still sorted by id so that the acquisition
        // order matches the rest of the code base.
        List<TableIf> tablesToRead = Lists.newArrayListWithCapacity(mvPartitionInfo.getPctInfos().size() + 1);
        tablesToRead.add(this);
        for (BaseColInfo pctInfo : mvPartitionInfo.getPctInfos()) {
            if (pctInfo.getTableInfo().equals(baseTableInfo)) {
                continue;
            }
            try {
                tablesToRead.add(MTMVUtil.getTable(pctInfo.getTableInfo()));
            } catch (Exception e) {
                LOG.warn("Failed to resolve PCT table {}, rebuild the whole MV. mv={}",
                        pctInfo.getTableInfo(), name, e);
                return Optional.empty();
            }
        }
        tablesToRead.sort(Comparator.comparing(TableIf::getId));
        if (!MetaLockUtils.tryReadLockTables(tablesToRead, Table.TRY_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            LOG.warn("A PCT table is busy, rebuild the whole MV {} instead of selecting part of it", name);
            return Optional.empty();
        }
        try {
            // A partition that is missing from the metadata here is invisible to the mapping as well, so
            // an empty answer below would be indistinguishable from "no MV partition reads it". It has to
            // be the partition the caller described, not merely one carrying the same name: RECOVER
            // PARTITION reports the recycled partition under its old name, and a partition added after
            // the drop may be live under that name again, with a different range. Matching on the name
            // alone would accept that replacement, select the MV partitions of its range, and leave the
            // recovered range -- whose rows no row binlog can repair -- without a barrier. The lookup is
            // by exact name, so a name that only differs in case takes the whole-MV path too. Base tables
            // that do not implement getPartition -- the external ones -- answer null for every name, so a
            // partition change on them always rebuilds the whole MV. That matches what the
            // refresh-snapshot selection answered for them, and the mapping has never been exercised for
            // external tables (IVM does not support them as base tables yet): revisit before taking the
            // narrow path for them.
            for (Entry<String, Long> changedBasePartition : changedBasePartitions.entrySet()) {
                Partition livePartition = pctTable.getPartition(changedBasePartition.getKey());
                if (livePartition == null || livePartition.getId() != changedBasePartition.getValue()) {
                    return Optional.empty();
                }
            }
            // Whether a partition_sync_limit is in effect decides whether the mapping built below may be
            // trusted, and it is read on both sides of that construction. It has to be: the property is
            // mutable (ALTER MATERIALIZED VIEW ... SET is not generation guarded) and the mapping is built
            // from it, so a read taken on one side only can be the stale one. Reading it after the mapping
            // alone misses a limit cleared while the mapping was built -- the mapping is then the windowed
            // one and would be trusted; reading it before alone misses a limit set in that same window, for
            // the opposite reason. The two reads bracket exactly the construction, and a limit in effect on
            // either of them means the mapping that came out of it may carry a window.
            boolean partitionSyncLimitActiveBeforeMapping =
                    MTMVPartitionUtil.isPartitionSyncLimitActive(mvProperties);
            Map<String, Map<MTMVRelatedTableIf, Set<String>>> partitionMappings =
                    calculatePartitionMappings(Maps.newHashMap());
            boolean partitionSyncLimitActiveAfterMapping =
                    MTMVPartitionUtil.isPartitionSyncLimitActive(mvProperties);
            Set<String> res = Sets.newHashSet();
            boolean pctTableMapped = false;
            // Every base partition this table's part of the mapping describes, which is what the selection
            // below is only allowed to trust when it covers the whole change.
            Set<String> mappedBasePartitions = Sets.newHashSet();
            for (Entry<String, Map<MTMVRelatedTableIf, Set<String>>> mapping : partitionMappings.entrySet()) {
                for (Entry<MTMVRelatedTableIf, Set<String>> tableMapping : mapping.getValue().entrySet()) {
                    if (!tableMapping.getKey().equals(pctTable)) {
                        continue;
                    }
                    pctTableMapped = true;
                    mappedBasePartitions.addAll(tableMapping.getValue());
                    if (!Collections.disjoint(tableMapping.getValue(), changedBasePartitions.keySet())) {
                        res.add(mapping.getKey());
                    }
                }
            }
            // The mapping does not describe this base table at all. That contradicts the PCT check above,
            // so it is safer to rebuild everything than to trust a selection that never saw the table --
            // unless the MV has no partition of its own yet, which is the one shape where the missing
            // entries are not a surprise: an MV without partitions holds no rows.
            if (!pctTableMapped) {
                if (getPartitionNames().isEmpty()) {
                    LOG.info("MV has no partition yet, nothing can hold the changed base partitions. "
                            + "baseTable={}, mv={}", baseTableInfo, name);
                    return Optional.of(Sets.newHashSet());
                }
                LOG.warn("Base table is not described by the partition mapping, rebuild the whole MV. "
                        + "baseTable={}, mv={}", baseTableInfo, name);
                return Optional.empty();
            }
            // A selection is only trustworthy while the mapping describes every base partition that
            // changed. With a partition_sync_limit in effect it does not: the window leaves out the
            // partitions it dropped, and one of those can still have its rows in an MV partition --
            // shrinking the window does not touch the MV's own partitions, and widening it again makes
            // partition sync keep them. A name the mapping leaves out cannot be told apart from a
            // partition no MV partition reads, so the whole MV is rebuilt instead. Requiring the whole
            // change to be described, rather than only a non-empty selection, is what covers a change
            // that mixes a partition inside the window with one outside it: the inside half would
            // otherwise fill the selection and hide the missing half. Without a limit the mapping is
            // complete, and a partition it leaves out really is one no MV partition reads. Either of the
            // two reads above counts: a limit that was in effect while the mapping was built leaves it
            // incomplete even if the limit is gone by now.
            if ((partitionSyncLimitActiveBeforeMapping || partitionSyncLimitActiveAfterMapping)
                    && !mappedBasePartitions.containsAll(changedBasePartitions.keySet())) {
                LOG.info("Changed base partitions are outside the partition_sync_limit window and the MV may "
                        + "still hold their rows, rebuild the whole MV. baseTable={}, changedPartitions={}, "
                        + "undescribed={}, mv={}", baseTableInfo, changedBasePartitions.keySet(),
                        Sets.difference(changedBasePartitions.keySet(), mappedBasePartitions), name);
                return Optional.empty();
            }
            return Optional.of(res);
        } catch (Exception e) {
            // The base table change is applied either way, so this must not fail the DDL: warn and take
            // the safe direction instead.
            LOG.warn("Failed to map base table partitions to MV partitions, rebuild the whole MV. "
                    + "baseTable={}, changedPartitions={}, mv={}", baseTableInfo, changedBasePartitions,
                    name, e);
            return Optional.empty();
        } finally {
            MetaLockUtils.readUnlockTables(tablesToRead);
        }
    }

    /**
     * Resolve the PCT table that {@code baseTableInfo} refers to, or null when the MV has no PCT entry
     * for it.
     */
    private MTMVRelatedTableIf findPctTable(BaseTableInfo baseTableInfo) {
        for (BaseColInfo pctInfo : mvPartitionInfo.getPctInfos()) {
            if (!pctInfo.getTableInfo().equals(baseTableInfo)) {
                continue;
            }
            try {
                TableIf pctTable = MTMVUtil.getTable(pctInfo.getTableInfo());
                if (pctTable instanceof MTMVRelatedTableIf) {
                    return (MTMVRelatedTableIf) pctTable;
                }
            } catch (Exception e) {
                LOG.warn("Failed to resolve PCT table {}, mv={}", pctInfo.getTableInfo(), name, e);
            }
            return null;
        }
        return null;
    }

    private EditLogItem submitIvmInfoChange() {
        // The caller has already mutated ivmInfo under the MV write lock. Submit its snapshot directly;
        // replay later applies the payload through alterIvmInfo().
        AlterMTMV alterMTMV = new AlterMTMV(
                new TableNameInfo(getQualifiedDbName(), getName()), MTMVAlterOpType.ALTER_IVM_INFO);
        alterMTMV.setIvmInfo(ivmInfo);
        return submitAlterLog(alterMTMV);
    }

    /**
     * Raises the requirement of the given MV partitions, so the next refresh rebuilds them.
     *
     * <p>This is an invalidation-shaped mutation, journaled as the whole state map before whatever needs
     * it is done. A caller about to make a partition's rows unusable says so with it: the raised
     * requirement survives a crash, so a refresh that never got to publish its rebuild leaves partitions
     * naming a generation they do not hold, and the next refresh rebuilds them.
     */
    public void markPartitionsForRebuild(Set<String> partitionNames) {
        if (CollectionUtils.isEmpty(partitionNames)) {
            return;
        }
        EditLogItem editLogItem;
        writeMvLock();
        try {
            boolean changed = false;
            for (String partitionName : partitionNames) {
                MTMVPartitionState state = partitionStates.get(partitionName);
                if (state == null) {
                    // A partition dropped since the caller planned it has no rows to protect.
                    continue;
                }
                state.setLatestEpoch(state.getLatestEpoch() + 1);
                changed = true;
            }
            if (!changed) {
                return;
            }
            editLogItem = submitPartitionStatesChange();
        } finally {
            writeMvUnlock();
        }
        editLogItem.await();
    }

    private EditLogItem submitPartitionStatesChange() {
        return submitPartitionStatesChange(Collections.emptySet());
    }


    /**
     * Journals the current states, and the MV partitions whose snapshots the same change dropped.
     *
     * <p>Same shape as submitIvmInfoChange: the caller mutated under the MV write lock, and replay applies
     * this payload through replayAlterPartitionStates(). The states ride as the MV's own map -- the setter
     * copies them -- so the payload cannot be written out half-mutated.
     */
    private EditLogItem submitPartitionStatesChange(Set<String> removedSnapshotPartitions) {
        AlterMTMV alterMTMV = new AlterMTMV(
                new TableNameInfo(getQualifiedDbName(), getName()), MTMVAlterOpType.ALTER_PARTITION_STATES);
        alterMTMV.setPartitionStates(partitionStates);
        alterMTMV.setRemovedSnapshotPartitions(removedSnapshotPartitions);
        return submitAlterLog(alterMTMV);
    }

    private EditLogItem submitAlterLog(AlterMTMV alterMTMV) {
        // Callers hold the MV write lock so journal order matches metadata mutation order.
        return Env.getCurrentEnv().getEditLog().submitEdit(OperationType.OP_ALTER_MTMV, alterMTMV);
    }

    public List<String> getInsertedColumnNames()  {
        List<Column> columns = getBaseSchema(true);
        List<String> columnNames = Lists.newArrayListWithExpectedSize(columns.size());
        for (Column column : columns) {
            if (column.isVisible() || IvmUtil.isIvmHiddenColumn(column.getName())) {
                columnNames.add(column.getName());
            }
        }
        return columnNames;
    }

    public long getSchemaChangeVersion() {
        readMvLock();
        try {
            return schemaChangeVersion;
        } finally {
            readMvUnlock();
        }
    }

    public void validateIvmRefreshStart(long expectedSchemaChangeVersion) throws JobException {
        readMvLock();
        try {
            if (schemaChangeVersion != expectedSchemaChangeVersion) {
                throw new JobException("Base table metadata changed before IVM refresh, mv=" + getName());
            }
        } finally {
            readMvUnlock();
        }
    }

    /**
     * Invalidate rewrite cache after metadata changes such as ADD/DROP CONSTRAINT.
     * Bumping the generation prevents any cache built before this call from being published later.
     */
    public void invalidateRewriteCache() {
        writeMvLock();
        try {
            rewriteCacheGeneration++;
            Env.getCurrentEnv().getMtmvCacheManager().invalidate(this.id);
        } finally {
            writeMvUnlock();
        }
    }

    protected MTMVCache createRewriteCache(ConnectContext currentContext, boolean needLock,
            boolean addSessionVarGuard) {
        return MTMVCache.from(this.getQuerySql(),
                MTMVPlanUtil.createMTMVContext(this, MTMVPlanUtil.DISABLE_RULES_WHEN_GENERATE_MTMV_CACHE),
                true, needLock, currentContext, addSessionVarGuard);
    }

    /**
     * generateMvPartitionDescs
     *
     * @return mvPartitionName ==> mvPartitionKeyDesc
     */
    public Map<String, PartitionKeyDesc> generateMvPartitionDescs() {
        Map<String, PartitionItem> mtmvItems = getAndCopyPartitionItems();
        Map<String, PartitionKeyDesc> result = Maps.newHashMap();
        for (Entry<String, PartitionItem> entry : mtmvItems.entrySet()) {
            result.put(entry.getKey(), entry.getValue().toPartitionKeyDesc());
        }
        return result;
    }

    /**
     * Normalize query-used base table partitions to the effective filter consumed by
     * partition-mapping generation.
     */
    public Map<List<String>, Set<String>> getEffectiveQueryUsedBaseTablePartitionMap(
            Map<List<String>, Set<String>> queryUsedBaseTablePartitionMap) throws AnalysisException {
        return getEffectiveQueryUsedBaseTablePartitionMap(queryUsedBaseTablePartitionMap, null);
    }

    private Map<List<String>, Set<String>> getEffectiveQueryUsedBaseTablePartitionMap(
            Map<List<String>, Set<String>> queryUsedBaseTablePartitionMap,
            Map<String, PartitionItem> mvPartitionItems) throws AnalysisException {
        return getEffectiveQueryUsedBaseTablePartitionMap(queryUsedBaseTablePartitionMap, mvPartitionItems,
                null);
    }

    private Map<List<String>, Set<String>> getEffectiveQueryUsedBaseTablePartitionMap(
            Map<List<String>, Set<String>> queryUsedBaseTablePartitionMap,
            Map<String, PartitionItem> mvPartitionItems,
            Map<MvccTableInfo, MvccSnapshot> pinnedSnapshots) throws AnalysisException {
        if (queryUsedBaseTablePartitionMap.isEmpty()
                || mvPartitionInfo.getPartitionType() != MTMVPartitionType.EXPR) {
            return queryUsedBaseTablePartitionMap;
        }
        return MTMVPartitionExpander.expandToMvPartitionGranularity(queryUsedBaseTablePartitionMap,
                mvPartitionItems != null ? mvPartitionItems : getAndCopyPartitionItems(),
                mvPartitionInfo.getPctTables(), pinnedSnapshots);
    }

    /**
     * Calculate the partition and associated partition mapping relationship of the MTMV
     * It is the result of real-time comparison calculation, so there may be some costs,
     * so it should be called with caution
     *
     * @return mvPartitionName ==> pctTable ==> pctPartitionName
     * @throws AnalysisException
     */
    public Map<String, Map<MTMVRelatedTableIf, Set<String>>> calculatePartitionMappings(
            Map<List<String>, Set<String>> queryUsedBaseTablePartitionMap) throws AnalysisException {
        return calculatePartitionMappings(queryUsedBaseTablePartitionMap, null);
    }

    public Map<String, Map<MTMVRelatedTableIf, Set<String>>> calculatePartitionMappings(
            Map<List<String>, Set<String>> queryUsedBaseTablePartitionMap,
            Map<MvccTableInfo, MvccSnapshot> pinnedSnapshots) throws AnalysisException {
        if (mvPartitionInfo.getPartitionType() == MTMVPartitionType.SELF_MANAGE) {
            return Maps.newHashMap();
        }
        long start = System.currentTimeMillis();
        // For EXPR-type partitions with RANGE base tables, expand the query-used partition
        // filter to MV partition granularity. This ensures complete partition mappings per
        // MV partition (needed for isSyncWithPartitions correctness) while skipping
        // irrelevant MV partitions entirely (the performance optimization).
        // For nested MVs where pctTable is not in the filter, the expanded map is empty,
        // so the pipeline runs without filtering (full computation) — correct behavior.
        Map<String, PartitionItem> mvPartitionItems = getAndCopyPartitionItems();
        Map<List<String>, Set<String>> effectiveFilter
                = getEffectiveQueryUsedBaseTablePartitionMap(
                        queryUsedBaseTablePartitionMap, mvPartitionItems, pinnedSnapshots);
        Map<String, Map<MTMVRelatedTableIf, Set<String>>> res = Maps.newHashMap();
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> pctPartitionDescs = MTMVPartitionUtil
                .generateRelatedPartitionDescs(mvPartitionInfo, mvProperties, getPartitionColumns(),
                        effectiveFilter, pinnedSnapshots);
        for (Entry<String, PartitionItem> entry : mvPartitionItems.entrySet()) {
            res.put(entry.getKey(),
                    pctPartitionDescs.getOrDefault(entry.getValue().toPartitionKeyDesc(), Maps.newHashMap()));
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("calculatePartitionMappings use [{}] mills, mvName is [{}]",
                    System.currentTimeMillis() - start, name);
        }
        return res;
    }

    public ConcurrentLinkedQueue<MTMVTask> getHistoryTasks() {
        return jobInfo.getHistoryTasks();
    }

    // for test
    public void setRefreshInfo(MTMVRefreshInfo refreshInfo) {
        this.refreshInfo = refreshInfo;
    }

    // for test
    public void setQuerySql(String querySql) {
        this.querySql = querySql;
    }

    // for test
    public void setStatus(MTMVStatus status) {
        this.status = status;
    }

    // for test
    public void setJobInfo(MTMVJobInfo jobInfo) {
        this.jobInfo = jobInfo;
    }

    // for test
    public void setMvProperties(Map<String, String> mvProperties) {
        this.mvProperties = mvProperties;
    }

    // for test
    public void setRelation(MTMVRelation relation) {
        this.relation = relation;
    }

    // for test
    public void setMvPartitionInfo(MTMVPartitionInfo mvPartitionInfo) {
        this.mvPartitionInfo = mvPartitionInfo;
    }

    // for test
    public void setRefreshSnapshot(MTMVRefreshSnapshot refreshSnapshot) {
        this.refreshSnapshot = refreshSnapshot;
    }

    public boolean canBeCandidate() {
        return getStatus().canBeCandidate();
    }

    public void readMvLock() {
        this.mvRwLock.readLock().lock();
    }

    public void readMvUnlock() {
        this.mvRwLock.readLock().unlock();
    }

    public void writeMvLock() {
        this.mvRwLock.writeLock().lock();
    }

    public void writeMvUnlock() {
        this.mvRwLock.writeLock().unlock();
    }

    // toString() is not easy to find where to call the method
    public String toInfoString() {
        final StringBuilder sb = new StringBuilder("MTMV{");
        sb.append("refreshInfo=").append(refreshInfo);
        sb.append(", querySql='").append(querySql).append('\'');
        sb.append(", status=").append(status);
        if (jobInfo != null) {
            sb.append(", jobInfo=").append(jobInfo.toInfoString());
        }
        sb.append(", mvProperties=").append(mvProperties);
        if (relation != null) {
            sb.append(", relation=").append(relation.toInfoString());
        }
        if (mvPartitionInfo != null) {
            sb.append(", mvPartitionInfo=").append(mvPartitionInfo.toInfoString());
        }
        sb.append(", refreshSnapshot=").append(refreshSnapshot);
        sb.append(", id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", qualifiedDbName='").append(qualifiedDbName).append('\'');
        sb.append(", comment='").append(comment).append('\'');
        sb.append('}');
        return sb.toString();
    }

    /**
     * Previously, ID was used to store the related table of materialized views,
     * but when the catalog is deleted, the ID will change, so name is used instead.
     * The logic here is to be compatible with older versions by converting ID to name
     */
    public void compatible(CatalogMgr catalogMgr) {
        try {
            compatibleInternal(catalogMgr);
            Env.getCurrentEnv().getMtmvService().unregisterMTMV(this);
            Env.getCurrentEnv().getMtmvService().registerMTMV(this, this.getDatabase().getId());
        } catch (Throwable e) {
            LOG.warn("MTMV compatible failed, dbName: {}, mvName: {}, errMsg: {}", getDBName(), name, e.getMessage());
            status.setState(MTMVState.SCHEMA_CHANGE);
            status.setSchemaChangeDetail("compatible failed, please refresh or recreate it, reason: " + e.getMessage());
        }
    }

    private void compatibleInternal(CatalogMgr catalogMgr) throws Exception {
        if (mvPartitionInfo != null) {
            mvPartitionInfo.compatible(catalogMgr);
        }
        if (relation != null) {
            relation.compatible(catalogMgr);
        }
        if (refreshSnapshot != null) {
            refreshSnapshot.compatible(this);
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        super.gsonPostProcess();
        if (sessionVariables == null) {
            sessionVariables = Maps.newHashMap();
        }
        if (ivmInfo == null) {
            // Created with the MV as well; this covers an image that carries the member as null.
            ivmInfo = new IvmInfo();
        }
        if (partitionStates == null) {
            // The field is created with the MV, so an image that leaves it out keeps that empty map. This
            // covers the one image that carries it as null, which reader code has no case for.
            partitionStates = Maps.newLinkedHashMap();
        }
        if (refreshInfo != null && refreshInfo.getRefreshMethod() == null) {
            LOG.warn("MTMV {} has unknown refresh method, marking as schema change", name);
            status.setState(MTMVState.SCHEMA_CHANGE);
            status.setSchemaChangeDetail("Unknown refresh method detected during deserialization");
        }
        Map<String, MTMVRefreshPartitionSnapshot> partitionSnapshots = refreshSnapshot.getPartitionSnapshots();
        compatiblePctSnapshot(partitionSnapshots);
    }

    @Override
    public void markDropped() {
        super.markDropped();
        // A refresh or query building a cache outside the MV lock must not
        // be able to republish it after the drop.
        writeMvLock();
        try {
            rewriteCacheGeneration++;
            Env.getCurrentEnv().getMtmvCacheManager().invalidate(this.id);
        } finally {
            writeMvUnlock();
        }
    }

    private void compatiblePctSnapshot(Map<String, MTMVRefreshPartitionSnapshot> partitionSnapshots) {
        BaseTableInfo relatedTableInfo = mvPartitionInfo.getRelatedTableInfo();
        if (relatedTableInfo == null) {
            return;
        }
        if (MapUtils.isEmpty(partitionSnapshots)) {
            return;
        }
        for (MTMVRefreshPartitionSnapshot partitionSnapshot : partitionSnapshots.values()) {
            Map<String, MTMVSnapshotIf> partitions = partitionSnapshot.getPartitions();
            Map<BaseTableInfo, Map<String, MTMVSnapshotIf>> pcts = partitionSnapshot.getPcts();
            if (!MapUtils.isEmpty(partitions) && MapUtils.isEmpty(pcts)) {
                pcts.put(relatedTableInfo, partitions);
            }
        }
    }

    public Map<String, String> getSessionVariables() {
        return sessionVariables;
    }
}
