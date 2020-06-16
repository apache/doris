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

package org.apache.doris.alter;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.MVColumnItem;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.AlterReplicaTask;
import org.apache.doris.task.CreateReplicaTask;
import org.apache.doris.thrift.TStorageFormat;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTabletType;
import org.apache.doris.thrift.TTaskType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

/**
 * Version 2 of RollupJob.
 * This is for replacing the old RollupJob
 * https://github.com/apache/incubator-doris/issues/1429
 */
public class RollupJobV2 extends AlterJobV2 implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(RollupJobV2.class);

    // partition id -> (rollup tablet id -> base tablet id)
    @SerializedName(value = "partitionIdToBaseRollupTabletIdMap")
    private Map<Long, Map<Long, Long>> partitionIdToBaseRollupTabletIdMap = Maps.newHashMap();
    @SerializedName(value = "partitionIdToRollupIndex")
    private Map<Long, MaterializedIndex> partitionIdToRollupIndex = Maps.newHashMap();

    // rollup and base schema info
    @SerializedName(value = "baseIndexId")
    private long baseIndexId;
    @SerializedName(value = "rollupIndexId")
    private long rollupIndexId;
    @SerializedName(value = "baseIndexName")
    private String baseIndexName;
    @SerializedName(value =  "rollupIndexName")
    private String rollupIndexName;

    @SerializedName(value = "rollupSchema")
    private List<Column> rollupSchema = Lists.newArrayList();
    @SerializedName(value = "baseSchemaHash")
    private int baseSchemaHash;
    @SerializedName(value = "rollupSchemaHash")
    private int rollupSchemaHash;

    @SerializedName(value = "rollupKeysType")
    private KeysType rollupKeysType;
    @SerializedName(value = "rollupShortKeyColumnCount")
    private short rollupShortKeyColumnCount;
    @SerializedName(value = "origStmt")
    private OriginStatement origStmt;

    // optional
    @SerializedName(value = "storageFormat")
    private TStorageFormat storageFormat = TStorageFormat.DEFAULT;

    // The rollup job will wait all transactions before this txn id finished, then send the rollup tasks.
    @SerializedName(value = "watershedTxnId")
    protected long watershedTxnId = -1;

    // save all create rollup tasks
    private AgentBatchTask rollupBatchTask = new AgentBatchTask();

    public RollupJobV2(long jobId, long dbId, long tableId, String tableName, long timeoutMs,
            long baseIndexId, long rollupIndexId, String baseIndexName, String rollupIndexName,
            List<Column> rollupSchema, int baseSchemaHash, int rollupSchemaHash, KeysType rollupKeysType,
            short rollupShortKeyColumnCount, OriginStatement origStmt) {
        super(jobId, JobType.ROLLUP, dbId, tableId, tableName, timeoutMs);

        this.baseIndexId = baseIndexId;
        this.rollupIndexId = rollupIndexId;
        this.baseIndexName = baseIndexName;
        this.rollupIndexName = rollupIndexName;

        this.rollupSchema = rollupSchema;
        this.baseSchemaHash = baseSchemaHash;
        this.rollupSchemaHash = rollupSchemaHash;
        this.rollupKeysType = rollupKeysType;
        this.rollupShortKeyColumnCount = rollupShortKeyColumnCount;

        this.origStmt = origStmt;
    }

    private RollupJobV2() {
        super(JobType.ROLLUP);
    }

    public void addTabletIdMap(long partitionId, long rollupTabletId, long baseTabletId) {
        Map<Long, Long> tabletIdMap = partitionIdToBaseRollupTabletIdMap.computeIfAbsent(partitionId, k -> Maps.newHashMap());
        tabletIdMap.put(rollupTabletId, baseTabletId);
    }

    public void addMVIndex(long partitionId, MaterializedIndex mvIndex) {
        this.partitionIdToRollupIndex.put(partitionId, mvIndex);
    }

    public void setStorageFormat(TStorageFormat storageFormat) {
        this.storageFormat = storageFormat;
    }

    /**
     * runPendingJob():
     * 1. Create all rollup replicas and wait them finished.
     * 2. After creating done, add this shadow rollup index to catalog, user can not see this
     *    rollup, but internal load process will generate data for this rollup index.
     * 3. Get a new transaction id, then set job's state to WAITING_TXN
     */
    @Override
    protected void runPendingJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.PENDING, jobState);

        LOG.info("begin to send create rollup replica tasks. job: {}", jobId);
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            throw new AlterCancelException("Database " + dbId + " does not exist");
        }

        if (!checkTableStable(db)) {
            return;
        }

        // 1. create rollup replicas
        AgentBatchTask batchTask = new AgentBatchTask();
        // count total replica num
        int totalReplicaNum = 0;
        for (MaterializedIndex rollupIdx : partitionIdToRollupIndex.values()) {
            for (Tablet tablet : rollupIdx.getTablets()) {
                totalReplicaNum += tablet.getReplicas().size();
            }
        }
        MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<Long, Long>(totalReplicaNum);
        OlapTable tbl = null;
        try {
            tbl = (OlapTable) db.getTableOrThrowException(tableId, Table.TableType.OLAP);
        } catch (MetaNotFoundException e) {
            throw new AlterCancelException(e.getMessage());
        }

        tbl.readLock();
        try {
            Preconditions.checkState(tbl.getState() == OlapTableState.ROLLUP);
            for (Map.Entry<Long, MaterializedIndex> entry : this.partitionIdToRollupIndex.entrySet()) {
                long partitionId = entry.getKey();
                Partition partition = tbl.getPartition(partitionId);
                if (partition == null) {
                    continue;
                }
                TStorageMedium storageMedium = tbl.getPartitionInfo().getDataProperty(partitionId).getStorageMedium();
                TTabletType tabletType = tbl.getPartitionInfo().getTabletType(partitionId);
                MaterializedIndex rollupIndex = entry.getValue();

                Map<Long, Long> tabletIdMap = this.partitionIdToBaseRollupTabletIdMap.get(partitionId);
                for (Tablet rollupTablet : rollupIndex.getTablets()) {
                    long rollupTabletId = rollupTablet.getId();
                    List<Replica> rollupReplicas = rollupTablet.getReplicas();
                    for (Replica rollupReplica : rollupReplicas) {
                        long backendId = rollupReplica.getBackendId();
                        Preconditions.checkNotNull(tabletIdMap.get(rollupTabletId)); // baseTabletId
                        countDownLatch.addMark(backendId, rollupTabletId);
                        // create replica with version 1.
                        // version will be updated by following load process, or when rollup task finished.
                        CreateReplicaTask createReplicaTask = new CreateReplicaTask(
                                backendId, dbId, tableId, partitionId, rollupIndexId, rollupTabletId,
                                rollupShortKeyColumnCount, rollupSchemaHash,
                                Partition.PARTITION_INIT_VERSION, Partition.PARTITION_INIT_VERSION_HASH,
                                rollupKeysType, TStorageType.COLUMN, storageMedium,
                                rollupSchema, tbl.getCopiedBfColumns(), tbl.getBfFpp(), countDownLatch,
                                tbl.getCopiedIndexes(),
                                tbl.isInMemory(),
                                tabletType);
                        createReplicaTask.setBaseTablet(tabletIdMap.get(rollupTabletId), baseSchemaHash);
                        if (this.storageFormat != null) {
                            createReplicaTask.setStorageFormat(this.storageFormat);
                        }
                        batchTask.addTask(createReplicaTask);
                    } // end for rollupReplicas
                } // end for rollupTablets
            }
        } finally {
            tbl.readUnlock();
        }

        if (!FeConstants.runningUnitTest) {
            // send all tasks and wait them finished
            AgentTaskQueue.addBatchTask(batchTask);
            AgentTaskExecutor.submit(batchTask);
            long timeout = Math.min(Config.tablet_create_timeout_second * 1000L * totalReplicaNum,
                    Config.max_create_table_timeout_second * 1000L);
            boolean ok = false;
            try {
                ok = countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOG.warn("InterruptedException: ", e);
                ok = false;
            }

            if (!ok || !countDownLatch.getStatus().ok()) {
                // create rollup replicas failed. just cancel the job
                // clear tasks and show the failed replicas to user
                AgentTaskQueue.removeBatchTask(batchTask, TTaskType.CREATE);
                String errMsg = null;
                if (!countDownLatch.getStatus().ok()) {
                    errMsg = countDownLatch.getStatus().getErrorMsg();
                } else {
                    List<Entry<Long, Long>> unfinishedMarks = countDownLatch.getLeftMarks();
                    // only show at most 3 results
                    List<Entry<Long, Long>> subList = unfinishedMarks.subList(0, Math.min(unfinishedMarks.size(), 3));
                    errMsg = "Error replicas:" + Joiner.on(", ").join(subList);
                }
                LOG.warn("failed to create rollup replicas for job: {}, {}", jobId, errMsg);
                throw new AlterCancelException("Create rollup replicas failed. Error: " + errMsg);
            }
        }

        // create all rollup replicas success.
        // add rollup index to catalog
        tbl.writeLock();
        try {
            Preconditions.checkState(tbl.getState() == OlapTableState.ROLLUP);
            addRollupIndexToCatalog(tbl);
        } finally {
            tbl.writeUnlock();
        }

        this.watershedTxnId = Catalog.getCurrentGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId();
        this.jobState = JobState.WAITING_TXN;

        // write edit log
        Catalog.getCurrentCatalog().getEditLog().logAlterJob(this);
        LOG.info("transfer rollup job {} state to {}, watershed txn id: {}", jobId, this.jobState, watershedTxnId);
    }

    private void addRollupIndexToCatalog(OlapTable tbl) {
        for (Partition partition : tbl.getPartitions()) {
            long partitionId = partition.getId();
            MaterializedIndex rollupIndex = this.partitionIdToRollupIndex.get(partitionId);
            Preconditions.checkNotNull(rollupIndex);
            Preconditions.checkState(rollupIndex.getState() == IndexState.SHADOW, rollupIndex.getState());
            partition.createRollupIndex(rollupIndex);
        }

        tbl.setIndexMeta(rollupIndexId, rollupIndexName, rollupSchema, 0 /* init schema version */,
                rollupSchemaHash, rollupShortKeyColumnCount,TStorageType.COLUMN, rollupKeysType, origStmt);
        tbl.rebuildFullSchema();
    }

    /**
     * runWaitingTxnJob():
     * 1. Wait the transactions before the watershedTxnId to be finished.
     * 2. If all previous transactions finished, send create rollup tasks to BE.
     * 3. Change job state to RUNNING.
     */
    @Override
    protected void runWaitingTxnJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.WAITING_TXN, jobState);

        try {
            if (!isPreviousLoadFinished()) {
                LOG.info("wait transactions before {} to be finished, rollup job: {}", watershedTxnId, jobId);
                return;
            }
        } catch (AnalysisException e) {
            throw new AlterCancelException(e.getMessage());
        }

        LOG.info("previous transactions are all finished, begin to send rollup tasks. job: {}", jobId);
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            throw new AlterCancelException("Databasee " + dbId + " does not exist");
        }

        OlapTable tbl = null;
        try {
            tbl = (OlapTable) db.getTableOrThrowException(tableId, Table.TableType.OLAP);
        } catch (MetaNotFoundException e) {
            throw new AlterCancelException(e.getMessage());
        }

        tbl.readLock();
        try {
            Preconditions.checkState(tbl.getState() == OlapTableState.ROLLUP);
            for (Map.Entry<Long, MaterializedIndex> entry : this.partitionIdToRollupIndex.entrySet()) {
                long partitionId = entry.getKey();
                Partition partition = tbl.getPartition(partitionId);
                Preconditions.checkNotNull(partition, partitionId);

                // the rollup task will transform the data before visible version(included).
                long visibleVersion = partition.getVisibleVersion();
                long visibleVersionHash = partition.getVisibleVersionHash();

                MaterializedIndex rollupIndex = entry.getValue();
                Map<Long, Long> tabletIdMap = this.partitionIdToBaseRollupTabletIdMap.get(partitionId);
                for (Tablet rollupTablet : rollupIndex.getTablets()) {
                    long rollupTabletId = rollupTablet.getId();
                    long baseTabletId = tabletIdMap.get(rollupTabletId);

                    Map<String, Expr> defineExprs = Maps.newHashMap();
                    for (Column column : rollupSchema) {
                        if (column.getDefineExpr() != null) {
                            defineExprs.put(column.getName(), column.getDefineExpr());
                        }
                    }

                    List<Replica> rollupReplicas = rollupTablet.getReplicas();
                    for (Replica rollupReplica : rollupReplicas) {
                        AlterReplicaTask rollupTask = new AlterReplicaTask(
                                rollupReplica.getBackendId(), dbId, tableId, partitionId,
                                rollupIndexId, baseIndexId,
                                rollupTabletId, baseTabletId, rollupReplica.getId(),
                                rollupSchemaHash, baseSchemaHash,
                                visibleVersion, visibleVersionHash, jobId, JobType.ROLLUP, defineExprs);
                        rollupBatchTask.addTask(rollupTask);
                    }
                }
            }
        } finally {
            tbl.readUnlock();
        }

        AgentTaskQueue.addBatchTask(rollupBatchTask);
        AgentTaskExecutor.submit(rollupBatchTask);
        this.jobState = JobState.RUNNING;

        // DO NOT write edit log here, tasks will be send again if FE restart or master changed.
        LOG.info("transfer rollup job {} state to {}", jobId, this.jobState);
    }

    /**
     * runRunningJob()
     * 1. Wait all create rollup tasks to be finished.
     * 2. Check the integrity of the newly created rollup index.
     * 3. Set rollup index's state to NORMAL to let it visible to query.
     * 4. Set job'state as FINISHED.
     */
    @Override
    protected void runRunningJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.RUNNING, jobState);
        
        // must check if db or table still exist first.
        // or if table is dropped, the tasks will never be finished,
        // and the job will be in RUNNING state forever.
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            throw new AlterCancelException("Databasee " + dbId + " does not exist");
        }

        OlapTable tbl = null;
        try {
            tbl = (OlapTable) db.getTableOrThrowException(tableId, Table.TableType.OLAP);
        } catch (MetaNotFoundException e) {
            throw new AlterCancelException(e.getMessage());
        }

        if (!rollupBatchTask.isFinished()) {
            LOG.info("rollup tasks not finished. job: {}", jobId);
            List<AgentTask> tasks = rollupBatchTask.getUnfinishedTasks(2000);
            for (AgentTask task : tasks) {
                if (task.getFailedTimes() >= 3) {
                    throw new AlterCancelException("rollup task failed after try three times: " + task.getErrorMsg());
                }
            }
            return;
        }

        /*
         * all tasks are finished. check the integrity.
         * we just check whether all rollup replicas are healthy.
         */
        tbl.writeLock();
        try {
            Preconditions.checkState(tbl.getState() == OlapTableState.ROLLUP);
            for (Map.Entry<Long, MaterializedIndex> entry : this.partitionIdToRollupIndex.entrySet()) {
                long partitionId = entry.getKey();
                Partition partition = tbl.getPartition(partitionId);
                if (partition == null) {
                    continue;
                }

                long visiableVersion = partition.getVisibleVersion();
                long visiableVersionHash = partition.getVisibleVersionHash();
                short expectReplicationNum = tbl.getPartitionInfo().getReplicationNum(partition.getId());

                MaterializedIndex rollupIndex = entry.getValue();
                for (Tablet rollupTablet : rollupIndex.getTablets()) {
                    List<Replica> replicas = rollupTablet.getReplicas();
                    int healthyReplicaNum = 0;
                    for (Replica replica : replicas) {
                        if (replica.getLastFailedVersion() < 0
                                && replica.checkVersionCatchUp(visiableVersion, visiableVersionHash, false)) {
                            healthyReplicaNum++;
                        }
                    }

                    if (healthyReplicaNum < expectReplicationNum / 2 + 1) {
                        LOG.warn("rollup tablet {} has few healthy replicas: {}, rollup job: {}",
                                rollupTablet.getId(), replicas, jobId);
                        throw new AlterCancelException("rollup tablet " + rollupTablet.getId() + " has few healthy replicas");
                    }
                } // end for tablets
            } // end for partitions

            onFinished(tbl);
        } finally {
            tbl.writeUnlock();
        }

        this.jobState = JobState.FINISHED;
        this.finishedTimeMs = System.currentTimeMillis();

        Catalog.getCurrentCatalog().getEditLog().logAlterJob(this);
        LOG.info("rollup job finished: {}", jobId);
    }

    private void onFinished(OlapTable tbl) {
        for (Partition partition : tbl.getPartitions()) {
            MaterializedIndex rollupIndex = partition.getIndex(rollupIndexId);
            Preconditions.checkNotNull(rollupIndex, rollupIndexId);
            for (Tablet tablet : rollupIndex.getTablets()) {
                for (Replica replica : tablet.getReplicas()) {
                    replica.setState(ReplicaState.NORMAL);
                }
            }
            partition.visualiseShadowIndex(rollupIndexId, false);
        }
        tbl.rebuildFullSchema();
    }

    /**
     * cancelImpl() can be called any time any place.
     * We need to clean any possible residual of this job.
     */
    @Override
    protected boolean cancelImpl(String errMsg) {
        if (jobState.isFinalState()) {
            return false;
        }

        cancelInternal();

        jobState = JobState.CANCELLED;
        this.errMsg = errMsg;
        this.finishedTimeMs = System.currentTimeMillis();
        LOG.info("cancel {} job {}, err: {}", this.type, jobId, errMsg);
        Catalog.getCurrentCatalog().getEditLog().logAlterJob(this);
        return true;
    }

    private void cancelInternal() {
        // clear tasks if has
        AgentTaskQueue.removeBatchTask(rollupBatchTask, TTaskType.ALTER);
        // remove all rollup indexes, and set state to NORMAL
        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db != null) {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl != null) {
                tbl.writeLock();
                try {
                    for (Long partitionId : partitionIdToRollupIndex.keySet()) {
                        MaterializedIndex rollupIndex = partitionIdToRollupIndex.get(partitionId);
                        for (Tablet rollupTablet : rollupIndex.getTablets()) {
                            invertedIndex.deleteTablet(rollupTablet.getId());
                        }
                        Partition partition = tbl.getPartition(partitionId);
                        partition.deleteRollupIndex(rollupIndexId);
                    }
                    tbl.deleteIndexInfo(rollupIndexName);
                } finally {
                    tbl.writeUnlock();
                }
            }
        }
    }

    // Check whether transactions of the given database which txnId is less than 'watershedTxnId' are finished.
    protected boolean isPreviousLoadFinished() throws AnalysisException {
        return Catalog.getCurrentGlobalTransactionMgr().isPreviousTransactionsFinished(watershedTxnId, dbId, Lists.newArrayList(tableId));
    }

    /**
     * Replay job in PENDING state.
     * Should replay all changes before this job's state transfer to PENDING.
     * These changes should be same as changes in RollupHander.processAddRollup()
     */
    private void replayPending(RollupJobV2 replayedJob) {
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            // database may be dropped before replaying this log. just return
            return;
        }

        OlapTable tbl = (OlapTable) db.getTable(tableId);
        if (tbl == null) {
            // table may be dropped before replaying this log. just return
            return;
        }

        tbl.writeLock();
        try {
            addTabletToInvertedIndex(tbl);
        } finally {
            tbl.writeUnlock();
        }

        // to make sure that this job will run runPendingJob() again to create the rollup replicas
        this.jobState = JobState.PENDING;
        this.watershedTxnId = replayedJob.watershedTxnId;

        LOG.info("replay pending rollup job: {}", jobId);
    }

    private void addTabletToInvertedIndex(OlapTable tbl) {
        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
        // add all rollup replicas to tablet inverted index
        for (Long partitionId : partitionIdToRollupIndex.keySet()) {
            MaterializedIndex rollupIndex = partitionIdToRollupIndex.get(partitionId);
            TStorageMedium medium = tbl.getPartitionInfo().getDataProperty(partitionId).getStorageMedium();
            TabletMeta rollupTabletMeta = new TabletMeta(dbId, tableId, partitionId, rollupIndexId,
                    rollupSchemaHash, medium);

            for (Tablet rollupTablet : rollupIndex.getTablets()) {
                invertedIndex.addTablet(rollupTablet.getId(), rollupTabletMeta);
                for (Replica rollupReplica : rollupTablet.getReplicas()) {
                    invertedIndex.addReplica(rollupTablet.getId(), rollupReplica);
                }
            }
        }
    }

    /**
     * Replay job in WAITING_TXN state.
     * Should replay all changes in runPendingJob()
     */
    private void replayWaitingTxn(RollupJobV2 replayedJob) {
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            // database may be dropped before replaying this log. just return
            return;
        }

        OlapTable tbl = (OlapTable) db.getTable(tableId);
        if (tbl == null) {
            // table may be dropped before replaying this log. just return
            return;
        }

        tbl.writeLock();
        try {
            addRollupIndexToCatalog(tbl);
        } finally {
            tbl.writeUnlock();
        }

        // should still be in WAITING_TXN state, so that the alter tasks will be resend again
        this.jobState = JobState.WAITING_TXN;
        this.watershedTxnId = replayedJob.watershedTxnId;

        LOG.info("replay waiting txn rollup job: {}", jobId);
    }

    /**
     * Replay job in FINISHED state.
     * Should replay all changes in runRuningJob()
     */
    private void replayFinished(RollupJobV2 replayedJob) {
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db != null) {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl != null) {
                tbl.writeLock();
                try {
                    Preconditions.checkState(tbl.getState() == OlapTableState.ROLLUP);
                    onFinished(tbl);
                } finally {
                    tbl.writeUnlock();
                }
            }
        }
        
        this.jobState = JobState.FINISHED;
        this.finishedTimeMs = replayedJob.finishedTimeMs;

        LOG.info("replay finished rollup job: {}", jobId);
    }

    /**
     * Replay job in CANCELLED state.
     */
    private void replayCancelled(RollupJobV2 replayedJob) {
        cancelInternal();
        this.jobState = JobState.CANCELLED;
        this.finishedTimeMs = replayedJob.finishedTimeMs;
        this.errMsg = replayedJob.errMsg;
        LOG.info("replay cancelled rollup job: {}", jobId);
    }

    @Override
    public void replay(AlterJobV2 replayedJob) {
        RollupJobV2 replayedRollupJob = (RollupJobV2) replayedJob;
        switch (replayedJob.jobState) {
            case PENDING:
                replayPending(replayedRollupJob);
                break;
            case WAITING_TXN:
                replayWaitingTxn(replayedRollupJob);
                break;
            case FINISHED:
                replayFinished(replayedRollupJob);
                break;
            case CANCELLED:
                replayCancelled(replayedRollupJob);
                break;
            default:
                break;
        }
    }

    @Override
    protected void getInfo(List<List<Comparable>> infos) {
        List<Comparable> info = Lists.newArrayList();
        info.add(jobId);
        info.add(tableName);
        info.add(TimeUtils.longToTimeString(createTimeMs));
        info.add(TimeUtils.longToTimeString(finishedTimeMs));
        info.add(baseIndexName);
        info.add(rollupIndexName);
        info.add(rollupIndexId);
        info.add(watershedTxnId);
        info.add(jobState.name());
        info.add(errMsg);
        // progress
        if (jobState == JobState.RUNNING && rollupBatchTask.getTaskNum() > 0) {
            info.add(rollupBatchTask.getFinishedTaskNum() + "/" + rollupBatchTask.getTaskNum());
        } else {
            info.add(FeConstants.null_string);
        }
        info.add(timeoutMs / 1000);
        infos.add(info);
    }

    public List<List<String>> getUnfinishedTasks(int limit) {
        List<List<String>> taskInfos = Lists.newArrayList();
        if (jobState == JobState.RUNNING) {
            List<AgentTask> tasks = rollupBatchTask.getUnfinishedTasks(limit);
            for (AgentTask agentTask : tasks) {
                AlterReplicaTask rollupTask = (AlterReplicaTask)agentTask;
                List<String> info = Lists.newArrayList();
                info.add(String.valueOf(rollupTask.getBackendId()));
                info.add(String.valueOf(rollupTask.getBaseTabletId()));
                info.add(String.valueOf(rollupTask.getSignature()));
                taskInfos.add(info);
            }
        }
        return taskInfos;
    }

    public Map<Long, MaterializedIndex> getPartitionIdToRollupIndex() {
        return partitionIdToRollupIndex;
    }

    public void setJobState(JobState jobState) {
        this.jobState = jobState;
    }

    private void setColumnsDefineExpr(List<MVColumnItem> mvColumnItemList) {
        for (MVColumnItem mvColumnItem : mvColumnItemList) {
            for (Column column : rollupSchema) {
                if (column.getName().equals(mvColumnItem.getName())) {
                    column.setDefineExpr(mvColumnItem.getDefineExpr());
                    break;
                }
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this, AlterJobV2.class);
        Text.writeString(out, json);
    }

    /**
     * This method is only used to deserialize the text mate which version is less then 86.
     * If the meta version >=86, it will be deserialized by the `read` of AlterJobV2 rather then here.
     */
    public static RollupJobV2 read(DataInput in) throws IOException {
        Preconditions.checkState(Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_86);
        RollupJobV2 rollupJob = new RollupJobV2();
        rollupJob.readFields(in);
        return rollupJob;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            long partitionId = in.readLong();
            int size2 = in.readInt();
            Map<Long, Long> tabletIdMap = partitionIdToBaseRollupTabletIdMap.computeIfAbsent(partitionId, k -> Maps.newHashMap());
            for (int j = 0; j < size2; j++) {
                long rollupTabletId = in.readLong();
                long baseTabletId = in.readLong();
                tabletIdMap.put(rollupTabletId, baseTabletId);
            }

            partitionIdToRollupIndex.put(partitionId, MaterializedIndex.read(in));
        }

        baseIndexId = in.readLong();
        rollupIndexId = in.readLong();
        baseIndexName = Text.readString(in);
        rollupIndexName = Text.readString(in);

        size = in.readInt();
        for (int i = 0; i < size; i++) {
            Column column = Column.read(in);
            rollupSchema.add(column);
        }
        baseSchemaHash = in.readInt();
        rollupSchemaHash = in.readInt();

        rollupKeysType = KeysType.valueOf(Text.readString(in));
        rollupShortKeyColumnCount = in.readShort();

        watershedTxnId = in.readLong();
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_85) {
            storageFormat = TStorageFormat.valueOf(Text.readString(in));
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        // analyze define stmt
        if (origStmt == null) {
            return;
        }

        if (jobState != JobState.PENDING) {
            return;
        }
        // parse the define stmt to schema
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(origStmt.originStmt),
                                                        SqlModeHelper.MODE_DEFAULT));
        ConnectContext connectContext = new ConnectContext();
        connectContext.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        connectContext.setDatabase(Catalog.getCurrentCatalog().getDb(dbId).getFullName());
        Analyzer analyzer = new Analyzer(Catalog.getCurrentCatalog(), connectContext);
        CreateMaterializedViewStmt stmt = null;
        try {
            stmt = (CreateMaterializedViewStmt) SqlParserUtils.getStmt(parser, origStmt.idx);
            stmt.analyze(analyzer);
        } catch (Exception e) {
            // Under normal circumstances, the stmt will not fail to analyze.
            throw new IOException("error happens when parsing create materialized view stmt: " + stmt, e);
        }
        setColumnsDefineExpr(stmt.getMVColumnItemList());
    }
}
