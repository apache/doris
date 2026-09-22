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
import org.apache.doris.nereids.rules.analysis.SessionVarGuardRewriter;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo.RefreshMode;
import org.apache.doris.persist.AlterMTMV;
import org.apache.doris.persist.EditLog.EditLogItem;
import org.apache.doris.persist.OperationType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
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
    private MTMVRefreshSnapshot refreshSnapshot;
    @SerializedName("ii")
    private IvmInfo ivmInfo;
    /**
     * The refresh epoch of every MV partition, keyed by MV partition name.
     *
     * <p>Deliberately on MTMV rather than inside {@link IvmInfo}: the field is shared, the behaviour is
     * not. Both kinds of MV carry it, but only an IVM MV ever populates it -- alignment, invalidation,
     * the ADD_TASK payload and ALTER_PARTITION_STATES are all no-ops for a non-IVM MV, so for one an
     * empty map is the complete answer.
     *
     * <p>Null means the same thing -- no state -- and has three causes: an image written before the
     * field existed, a non-IVM MV, and a live MV that has not been aligned yet. Only
     * {@link #gsonPostProcess()} turns it into an empty map, on load; nothing else needs to, because a
     * reader treats the two the same.
     */
    @SerializedName("pst")
    private Map<String, MTMVPartitionState> partitionStates;
    // Should update after every fresh, not persist
    // Cache with SessionVarGuardExpr: used when query session variables differ from MV creation variables
    private MTMVCache cacheWithGuard;
    // Cache without SessionVarGuardExpr: used when query session variables match MV creation variables
    private MTMVCache cacheWithoutGuard;
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

    public long getNextRefreshVersion() {
        return Config.isCloudMode() ? getNextVersion() : getIvmInfo().getRefreshVersion() + 1;
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
                // to connection issues such as S3, so it is directly set to null
                if (!isReplay) {
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
            if (isReplay && alterMTMV.getIvmInfo() != null) {
                // Replay the final IVM state; ADD_TASK does not change schemaChangeVersion.
                ivmInfo = new IvmInfo(alterMTMV.getIvmInfo());
            }
            if (isReplay && alterMTMV.getPartitionStates() != null) {
                // A journal written before the field existed carries no state at all: leave the
                // partition states alone rather than clearing them.
                partitionStates = MTMVPartitionState.copyOf(alterMTMV.getPartitionStates());
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
                    ivmInfo.clearBaselineRebuild();
                }
                if (needUpdateCache) {
                    if (cacheGeneration == rewriteCacheGeneration) {
                        // Initialize cacheWithGuard, cacheWithoutGuard will be lazily generated when needed
                        this.cacheWithGuard = mtmvCacheWithGuard;
                        // Clear the other cache to ensure consistency
                        this.cacheWithoutGuard = mtmvCacheWithoutGuard;
                    }
                }
            } else {
                this.status.setRefreshState(MTMVRefreshState.FAIL);
            }
            this.jobInfo.addHistoryTask(task);
            compatiblePctSnapshot(partitionSnapshots);
            this.refreshSnapshot.updateSnapshots(partitionSnapshots, getPartitionNames());
            Env.getCurrentEnv().getMtmvService()
                    .refreshComplete(this, relation, task);
            if (isReplay) {
                return true;
            }
            if (ivmInfo.isEnableIvm()) {
                alterMTMV.setIvmInfo(ivmInfo);
                // Same condition as ivmInfo, so the journal of a non-IVM MV stays byte-for-byte what it
                // was. The map is null until the states are first aligned, and a payload without the
                // member means the same as one carrying an empty map.
                alterMTMV.setPartitionStates(partitionStates);
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
            boolean containsExcludedTriggerTables = mvProperties.containsKey(
                    PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES);
            Set<TableNameInfo> oldExcludedTriggerTables = containsExcludedTriggerTables
                    ? parseExcludedTriggerTables()
                    : Sets.newHashSet();
            // Enlarging or removing ivm_partition_window_limit brings previously lossy
            // partitions back into the refresh range. Their stream backlog was skipped by
            // the windowed refreshes, so a strict incremental refresh would wrongly judge
            // "all partitions are synced" and return SUCCESS with stale data. Force the
            // next refresh to rebuild a complete baseline instead.
            boolean containsPartitionWindowLimit = mvProperties.containsKey(
                    PropertyAnalyzer.PROPERTIES_IVM_PARTITION_WINDOW_LIMIT);
            Map<TableNameInfo, Integer> oldWindowLimits = containsPartitionWindowLimit
                    ? MTMVPropertyUtil.getIvmPartitionWindowLimit(this.mvProperties)
                    : Maps.newHashMap();
            // A partition_sync_limit window decides which base partitions the MV maintains. Only a change
            // that can bring a partition back into that set needs a complete baseline rebuild -- a removed
            // or wider limit -- because its deltas were skipped while it was outside and nothing
            // incremental can repair them. That is the same trade as the two properties around it. A
            // window that starts applying, a narrower one, and one that describes the same set as before
            // leave the applied deltas intact; the partitions they take out are dropped by partition sync
            // before the refresh plans, and taking one back in is the widening this answers. Doing it here,
            // in the critical section that applies the ALTER, is also what keeps a window set and cleared
            // while an invalidation reads the mapping from making that mapping look unwindowed.
            boolean containsSyncWindow = MTMVPropertyUtil.containsPartitionSyncWindow(mvProperties);
            Map<String, String> oldSyncWindow = containsSyncWindow
                    ? MTMVPropertyUtil.partitionSyncWindowOf(this.mvProperties) : null;
            this.mvProperties.putAll(mvProperties);
            // Both excluded_trigger_tables changes and window limit enlargement/removal
            // change the refresh baseline semantics: partitions previously skipped become
            // refreshable again, and their stream backlog was not applied. Invalidate the
            // snapshots (once) and require a complete baseline rebuild so the next refresh
            // covers the new range instead of wrongly judging "all partitions are synced".
            boolean invalidateRefreshSnapshot = false;
            boolean requireCompleteBaselineRebuild = false;
            if (containsExcludedTriggerTables) {
                Set<TableNameInfo> newExcludedTriggerTables = parseExcludedTriggerTables();
                if (!oldExcludedTriggerTables.equals(newExcludedTriggerTables)) {
                    invalidateRefreshSnapshot = true;
                    if (ivmInfo != null && ivmInfo.isEnableIvm()
                            && relation != null && relation.getBaseTables() != null) {
                        for (BaseTableInfo baseTableInfo : relation.getBaseTables()) {
                            TableNameInfo baseTableName = new TableNameInfo(baseTableInfo.getCtlName(),
                                    baseTableInfo.getDbName(), baseTableInfo.getTableName());
                            if (MTMVPartitionUtil.isTableExcluded(oldExcludedTriggerTables, baseTableName)
                                    && !MTMVPartitionUtil.isTableExcluded(newExcludedTriggerTables, baseTableName)) {
                                requireCompleteBaselineRebuild = true;
                                break;
                            }
                        }
                    }
                }
            }
            if (containsPartitionWindowLimit && ivmInfo != null && ivmInfo.isEnableIvm()
                    && relation != null && relation.getBaseTables() != null) {
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
                        requireCompleteBaselineRebuild = true;
                        break;
                    }
                }
            }
            if (containsSyncWindow && ivmInfo != null && ivmInfo.isEnableIvm()
                    && MTMVPropertyUtil.partitionSyncWindowWidens(oldSyncWindow,
                            MTMVPropertyUtil.partitionSyncWindowOf(this.mvProperties))) {
                requireCompleteBaselineRebuild = true;
            }
            if (invalidateRefreshSnapshot || requireCompleteBaselineRebuild) {
                this.schemaChangeVersion++;
                this.refreshSnapshot = new MTMVRefreshSnapshot();
            }
            if (requireCompleteBaselineRebuild) {
                ivmInfo.requireCompleteBaselineRebuild();
            }
            if (isReplay) {
                return;
            }
            editLogItem = submitAlterLog(alterMTMV);
        } finally {
            writeMvUnlock();
        }
        editLogItem.await();
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
        // store two MTMVCaches: one is a cache where SessionVariables differ from those at creation time,
        // and the MTMV plan includes a guardexpr;
        // the other is a cache where SessionVariables are the same as at creation time, and the MTMV plan
        // does not include a guardexpr;
        // This way, when sessionVariables are the same, rewriting is possible;
        // When sessionVariables are different, there are two cases:
        // 1. If a guardexpr is present, rewriting is not possible;
        // 2. If no guardexpr is present, rewriting is possible.
        // Determine if current session variables match MV creation session variables
        Map<String, String> currentSessionVars =
                connectionContext.getSessionVariable().getAffectQueryResultInPlanVariables();
        boolean sessionVarsMatch = SessionVarGuardRewriter.checkSessionVariablesMatch(
                currentSessionVars, this.sessionVariables);

        while (true) {
            long cacheGeneration;
            // Select appropriate cache based on session variable match
            readMvLock();
            try {
                MTMVCache cache = getCache(sessionVarsMatch);
                if (cache != null) {
                    return cache;
                }
                cacheGeneration = rewriteCacheGeneration;
            } finally {
                readMvUnlock();
            }

            // Generate cache if not exists
            // Concurrent situations may result in duplicate cache generation,
            // but we tolerate this in order to prevent nested use of readLock and write MvLock for the table
            MTMVCache mtmvCache = createRewriteCache(connectionContext, false, !sessionVarsMatch);
            writeMvLock();
            try {
                MTMVCache cache = getCache(sessionVarsMatch);
                if (cache != null) {
                    return cache;
                }
                if (cacheGeneration != rewriteCacheGeneration) {
                    continue;
                }
                setCache(sessionVarsMatch, mtmvCache);
                return mtmvCache;
            } finally {
                writeMvUnlock();
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
            return refreshSnapshot != null && !MapUtils.isEmpty(refreshSnapshot.getPartitionSnapshots());
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
     *
     * <p>A missing map -- an image written before the field existed, or a non-IVM MV -- reads as empty.
     */
    public Map<String, MTMVPartitionState> getPartitionStates() {
        readMvLock();
        try {
            if (partitionStates == null) {
                return Collections.emptyMap();
            }
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
        if (partitionStates == null) {
            return;
        }
        writeMvLock();
        try {
            this.partitionStates = MTMVPartitionState.copyOf(partitionStates);
        } finally {
            writeMvUnlock();
        }
    }

    public void invalidateIvmBaseline() {
        EditLogItem editLogItem;
        writeMvLock();
        try {
            if (ivmInfo == null) {
                ivmInfo = new IvmInfo();
            }
            ivmInfo.requireCompleteBaselineRebuild();
            // Bump the version even when a rebuild is already pending, so a task that started before
            // this visible base-table change cannot clear the barrier with an old result.
            schemaChangeVersion++;
            editLogItem = submitIvmInfoChange();
        } finally {
            writeMvUnlock();
        }
        editLogItem.await();
    }

    /**
     * Mark the MV partitions that may hold rows read from the changed base table partitions as needing a
     * rebuild. When those partitions cannot be determined, the whole MV is marked instead.
     */
    /**
     * @return whether a barrier was recorded. The caller reports the two outcomes differently: a change
     *         that no MV partition reads leaves nothing to rebuild and must not be logged as one.
     */
    public boolean invalidateIvmBaseline(BaseTableInfo baseTableInfo, Map<String, Long> changedPartitions) {
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
        EditLogItem editLogItem;
        writeMvLock();
        try {
            if (ivmInfo == null) {
                ivmInfo = new IvmInfo();
            }
            if (!affectedMvPartitions.isPresent()) {
                // A narrower rebuild could leave a partition holding rows of the changed base partition
                // untouched, and those rows cannot be repaired later: the change emitted no row binlog.
                ivmInfo.requireCompleteBaselineRebuild();
            } else {
                ivmInfo.addPendingBaselineRebuildPartitions(affectedMvPartitions.get());
            }
            schemaChangeVersion++;
            editLogItem = submitIvmInfoChange();
        } finally {
            writeMvUnlock();
        }
        editLogItem.await();
        return true;
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

    /**
     * Release the IVM baseline barrier after the partitions it named have been rebuilt, or after
     * partition sync removed them (a dropped partition resolves its own entry: the partition and its
     * IVM offsets are both gone).
     *
     * <p>Guarded by schemaChangeVersion, like {@link #persistIvmBaselineGuard}: a base-table change
     * landing while the rebuild runs carries its own barrier entry, and a blind clear would swallow
     * it. Failing instead preserves that entry -- the next refresh rebuilds it together with the
     * partitions this task handled.
     *
     * <p>Journals the new state right away, like every other ivmInfo mutation here. A task that dies
     * before {@link #addTaskResult} would otherwise leave the release in memory only, and a restart
     * would resurrect the barrier from disk.
     */
    public void releaseIvmBaselineRebuild(long expectedSchemaChangeVersion) throws JobException {
        EditLogItem editLogItem;
        writeMvLock();
        try {
            if (ivmInfo == null || !ivmInfo.isBaselineRebuildRequired()) {
                // Nothing to release: skip both the mutation and the journal entry. Any base-table
                // change that raced us in is still caught by validateIvmRefreshStart() below.
                return;
            }
            if (schemaChangeVersion != expectedSchemaChangeVersion) {
                throw new JobException("Base table metadata changed before IVM baseline refresh, mv="
                        + getName());
            }
            ivmInfo.clearBaselineRebuild();
            editLogItem = submitIvmInfoChange();
        } finally {
            writeMvUnlock();
        }
        editLogItem.await();
    }

    public void persistIvmBaselineGuard(RefreshMode refreshMode, Set<String> baselinePartitions,
            long expectedSchemaChangeVersion) throws JobException {
        EditLogItem editLogItem;
        writeMvLock();
        try {
            if (schemaChangeVersion != expectedSchemaChangeVersion) {
                throw new JobException("Base table metadata changed before IVM baseline refresh, mv=" + getName());
            }
            if (refreshMode == RefreshMode.COMPLETE) {
                ivmInfo.requireCompleteBaselineRebuild();
            } else {
                ivmInfo.addPendingBaselineRebuildPartitions(baselinePartitions);
            }
            editLogItem = submitIvmInfoChange();
        } finally {
            writeMvUnlock();
        }
        editLogItem.await();
    }

    private EditLogItem submitIvmInfoChange() {
        // The caller has already mutated ivmInfo under the MV write lock. Submit its snapshot directly;
        // replay later applies the payload through alterIvmInfo().
        AlterMTMV alterMTMV = new AlterMTMV(
                new TableNameInfo(getQualifiedDbName(), getName()), MTMVAlterOpType.ALTER_IVM_INFO);
        alterMTMV.setIvmInfo(ivmInfo);
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
            if (ivmInfo != null && ivmInfo.isBaselineRebuildRequired()) {
                throw new JobException("IVM baseline rebuild is pending, mv=" + getName());
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
            cacheWithGuard = null;
            cacheWithoutGuard = null;
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

    private MTMVCache getCache(boolean sessionVarsMatch) {
        return sessionVarsMatch ? cacheWithoutGuard : cacheWithGuard;
    }

    private void setCache(boolean sessionVarsMatch, MTMVCache cache) {
        if (sessionVarsMatch) {
            this.cacheWithoutGuard = cache;
        } else {
            this.cacheWithGuard = cache;
        }
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
            ivmInfo = new IvmInfo();
        }
        if (partitionStates == null) {
            // An image written before the field existed deserializes it as null, and so does a non-IVM MV.
            // Both mean "no state", so an empty map is the whole answer.
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
