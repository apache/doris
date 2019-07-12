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
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.Config;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.CreateReplicaTask;
import org.apache.doris.task.CreateRollupTaskV2;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTaskType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

/*
 * Author: Chenmingyu
 * Date: Jul 8, 2019
 */

/*
 * Version 2 of RollupJob.
 * This is for replacing the old RollupJob
 * https://github.com/apache/incubator-doris/issues/1429
 */
public class RollupJobV2 extends AlterJobV2 {
    private static final Logger LOG = LogManager.getLogger(RollupJobV2.class);

    // partition id -> (rollup tablet id -> base tablet id)
    private Map<Long, Map<Long, Long>> partitionIdToBaseRollupTabletIdMap = Maps.newHashMap();
    private Map<Long, MaterializedIndex> partitionIdToRollupIndex = Maps.newHashMap();

    // rollup and base schema info
    private long baseIndexId;
    private long rollupIndexId;
    private String baseIndexName;
    private String rollupIndexName;

    private List<Column> rollupSchema = Lists.newArrayList();
    private int baseSchemaHash;
    private int rollupSchemaHash;

    private KeysType rollupKeysType;
    private short rollupShortKeyColumnCount;

    // The rollup job will wait all transactions before this txn id finished, then send the rollup tasks.
    protected long watershedTxnId = -1;

    // save all create rollup tasks
    private AgentBatchTask rollupBatchTask = new AgentBatchTask();

    public RollupJobV2(long jobId, long dbId, long tableId, String tableName, long timeoutMs,
            long baseIndexId, long rollupIndexId, String baseIndexName, String rollupIndexName,
            List<Column> rollupSchema, int baseSchemaHash, int rollupSchemaHash,
            KeysType rollupKeysType, short rollupShortKeyColumnCount) {
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
    }

    private RollupJobV2() {
        super(JobType.ROLLUP);
    }

    public void addTabletIdMap(long partitionId, long rollupTabletId, long baseTabletId) {
        Map<Long, Long> tabletIdMap = partitionIdToBaseRollupTabletIdMap.get(partitionId);
        if (tabletIdMap == null) {
            tabletIdMap = Maps.newHashMap();
            partitionIdToBaseRollupTabletIdMap.put(partitionId, tabletIdMap);
        }
        tabletIdMap.put(rollupTabletId, baseTabletId);
    }

    public void addRollupIndex(long partitionId, MaterializedIndex rollupIndex) {
        this.partitionIdToRollupIndex.put(partitionId, rollupIndex);
    }

    /*
     * runPendingJob():
     * 1. Create all rollup replicas and wait them finished.
     * 2. After creating done, set the rollup index state as SHADOW, add it to catalog, user can not see this
     *    rollup, but internal load process will generate data for this rollup index.
     * 3. Get a new transaction id, then set job's state to WAITING_TXN
     */
    @Override
    protected void runPendingJob() {
        Preconditions.checkState(jobState == JobState.PENDING, jobState);

        LOG.info("begin to send create rollup replica tasks. job: {}", jobId);
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            cancel("Databasee " + dbId + " does not exist");
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
        db.readLock();
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                cancel("Table " + tableId + " does not exist");
                return;
            }
            Preconditions.checkState(tbl.getState() == OlapTableState.ROLLUP);

            for (Map.Entry<Long, MaterializedIndex> entry : this.partitionIdToRollupIndex.entrySet()) {
                long partitionId = entry.getKey();
                Partition partition = tbl.getPartition(partitionId);
                if (partition == null) {
                    continue;
                }
                TStorageMedium storageMedium = tbl.getPartitionInfo().getDataProperty(partitionId).getStorageMedium();
                MaterializedIndex rollupIndex = entry.getValue();

                Map<Long, Long> tabletIdMap = this.partitionIdToBaseRollupTabletIdMap.get(partitionId);
                for (Tablet rollupTablet : rollupIndex.getTablets()) {
                    long rollupTabletId = rollupTablet.getId();
                    List<Replica> rollupReplicas = rollupTablet.getReplicas();
                    for (Replica rollupReplica : rollupReplicas) {
                        long backendId = rollupReplica.getBackendId();
                        Preconditions.checkNotNull(tabletIdMap.get(rollupTabletId)); // baseTabletId

                        CreateReplicaTask createReplicaTask = new CreateReplicaTask(
                                backendId, dbId, tableId, partitionId, rollupIndexId, rollupTabletId,
                                rollupShortKeyColumnCount, rollupSchemaHash,
                                Partition.PARTITION_INIT_VERSION, Partition.PARTITION_INIT_VERSION_HASH,
                                rollupKeysType, TStorageType.COLUMN, storageMedium,
                                rollupSchema, tbl.getCopiedBfColumns(), tbl.getBfFpp(), countDownLatch);
                        createReplicaTask.setBaseTablet(tabletIdMap.get(rollupTabletId), baseSchemaHash);

                        batchTask.addTask(createReplicaTask);
                    } // end for rollupReplicas
                } // end for rollupTablets
            }
        } finally {
            db.readUnlock();
        }

        // send all tasks and wait them finished
        AgentTaskQueue.addBatchTask(batchTask);
        AgentTaskExecutor.submit(batchTask);
        // max timeout is 30 seconds
        long timeout = Math.min(Config.tablet_create_timeout_second * 1000L * totalReplicaNum, 30000);
        boolean ok = false;
        try {
            ok = countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOG.warn("InterruptedException: ", e);
            ok = false;
        }

        if (!ok) {
            // create rollup replicas failed. just cancel the job
            // clear tasks and show the failed replicas to user
            AgentTaskQueue.removeBatchTask(batchTask, TTaskType.CREATE);
            List<Entry<Long, Long>> unfinishedMarks = countDownLatch.getLeftMarks();
            // only show at most 10 results
            List<Entry<Long, Long>> subList = unfinishedMarks.subList(0, Math.min(unfinishedMarks.size(), 10));
            String idStr = Joiner.on(", ").join(subList);
            LOG.warn("failed to create rollup replicas for job: {}, {}", jobId, idStr);
            cancel("Create rollup replicas failed. Error replicas: " + idStr);
            return;
        }

        // create all rollup replicas success.
        // set rollup index’s state to SHADOW and add it to catalog
        db.writeLock();
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                cancel("Table " + tableId + " does not exist");
                return;
            }
            Preconditions.checkState(tbl.getState() == OlapTableState.ROLLUP);
            addRollupIndexToCatalog(tbl);
        } finally {
            db.writeUnlock();
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
            partition.createRollupIndex(rollupIndex);
        }

        tbl.setIndexSchemaInfo(rollupIndexId, rollupIndexName, rollupSchema, 0 /* init schema version */,
                rollupSchemaHash, rollupShortKeyColumnCount);
        tbl.setStorageTypeToIndex(rollupIndexId, TStorageType.COLUMN);
    }

    /*
     * runWaitingTxnJob():
     * 1. Wait the transactions before the watershedTxnId to be finished.
     * 2. If all previous transactions finished, send create rollup tasks to BE.
     * 3. Change job state to RUNNING.
     */
    @Override
    protected void runWaitingTxnJob() {
        Preconditions.checkState(jobState == JobState.WAITING_TXN, jobState);

        if (!isPreviousLoadFinished()) {
            LOG.info("wait transactions before {} to be finished, rollup job: {}", watershedTxnId, jobId);
            return;
        }

        LOG.info("previous transactions are all finished, begin to send rollup tasks. job: {}", jobId);
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            cancel("Databasee " + dbId + " does not exist");
            return;
        }
        
        db.readLock();
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                cancel("Table " + tableId + " does not exist");
                return;
            }
            Preconditions.checkState(tbl.getState() == OlapTableState.ROLLUP);
            for (Map.Entry<Long, MaterializedIndex> entry : this.partitionIdToRollupIndex.entrySet()) {
                long partitionId = entry.getKey();
                Partition partition = tbl.getPartition(partitionId);
                Preconditions.checkNotNull(partition, partitionId);

                // the rollup task will transform the data before committed version.
                // DO NOT use visible version because we need to handle the committed but not published version on BE.
                long committedVersion = partition.getCommittedVersion();
                long committedVersionHash = partition.getCommittedVersionHash();

                MaterializedIndex rollupIndex = entry.getValue();
                Map<Long, Long> tabletIdMap = this.partitionIdToBaseRollupTabletIdMap.get(partitionId);
                for (Tablet rollupTablet : rollupIndex.getTablets()) {
                    long rollupTabletId = rollupTablet.getId();
                    long baseTabletId = tabletIdMap.get(rollupTabletId);

                    List<Replica> rollupReplicas = rollupTablet.getReplicas();
                    for (Replica rollupReplica : rollupReplicas) {
                        CreateRollupTaskV2 rollupTask = new CreateRollupTaskV2(
                                rollupReplica.getBackendId(), dbId, tableId, partitionId,
                                rollupIndexId, baseIndexId,
                                rollupTabletId, baseTabletId, rollupReplica.getId(),
                                rollupSchemaHash, baseSchemaHash,
                                committedVersion, committedVersionHash, jobId);
                        rollupBatchTask.addTask(rollupTask);
                    }
                }
            }
        } finally {
            db.readUnlock();
        }

        AgentTaskQueue.addBatchTask(rollupBatchTask);
        AgentTaskExecutor.submit(rollupBatchTask);
        this.jobState = JobState.RUNNING;

        // DO NOT write edit log here, tasks will be send again if FE restart or master changed.
        LOG.info("transfer rollup job {} state to {}", jobId, this.jobState);
    }

    @Override
    protected void runRunningJob() {
        Preconditions.checkState(jobState == JobState.RUNNING, jobState);
        if (!rollupBatchTask.isFinished()) {
            LOG.info("rollup tasks not finished. job: {}", jobId);
            return;
        }

        /*
         * all tasks are finished. check the integrity.
         * we just check whether all rollup replicas are healthy.
         */
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            cancel("Databasee " + dbId + " does not exist");
            return;
        }

        db.writeLock();
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                cancel("Table " + tableId + " does not exist");
                return;
            }
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
                                && replica.checkVersionCatchUp(visiableVersion, visiableVersionHash)) {
                            healthyReplicaNum++;
                        }
                    }

                    if (healthyReplicaNum < expectReplicationNum / 2 + 1) {
                        LOG.warn("rollup tablet {} has few healthy replicas: {}, rollup job: {}",
                                rollupTablet.getId(), replicas, jobId);
                        cancel("rollup tablet " + rollupTablet.getId() + " has few healthy replicas");
                        return;
                    }
                } // end for tablets
            } // end for partitions

            // all partitions are good
            // set table and rollup index state to NORMAL, then finish the job
            for (Partition partition : tbl.getPartitions()) {
                // get index from catalog, not from 'partitionIdToRollupIndex'.
                // because if this alter job is recovered from edit log, rollup index in 'partitionIdToRollupIndex'
                // is not the same object in catalog. So modification on that index can not reflect to the index
                // in catalog.
                MaterializedIndex rollupIndex = partition.getIndex(rollupIndexId);
                Preconditions.checkNotNull(rollupIndex, rollupIndex);
                rollupIndex.setState(IndexState.NORMAL);
            }

            tbl.setState(OlapTableState.NORMAL);
        } finally {
            db.writeUnlock();
        }

        this.jobState = JobState.FINISHED;
        this.finishedTimeMs = System.currentTimeMillis();

        Catalog.getCurrentCatalog().getEditLog().logAlterJob(this);
        LOG.info("rollup job finished: {}", jobId);
    }

    @Override
    public synchronized void cancel(String errMsg) {
        if (jobState.isFinalState()) {
            return;
        }

        cancelInternal();

        jobState = JobState.CANCELLED;
        this.errMsg = errMsg;
        this.finishedTimeMs = System.currentTimeMillis();
        LOG.info("cancel {} job {}, err: {}", this.type, jobId, errMsg);
        Catalog.getCurrentCatalog().getEditLog().logAlterJob(this);
    }

    private void cancelInternal() {
        // clear tasks if have
        AgentTaskQueue.removeBatchTask(rollupBatchTask, TTaskType.ROLLUP);
        // remove all rollup indexes, and set state to NORMAL
        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db != null) {
            db.writeLock();
            try {
                OlapTable tbl = (OlapTable) db.getTable(tableId);
                if (tbl != null) {
                    for (Long partitionId : partitionIdToRollupIndex.keySet()) {
                        MaterializedIndex rollupIndex = partitionIdToRollupIndex.get(partitionId);
                        for (Tablet rollupTablet : rollupIndex.getTablets()) {
                            invertedIndex.deleteTablet(rollupTablet.getId());
                        }
                        Partition partition = tbl.getPartition(partitionId);
                        partition.deleteRollupIndex(rollupIndexId);
                    }
                    tbl.setState(OlapTableState.NORMAL);
                }
            } finally {
                db.writeUnlock();
            }
        }
    }

    protected boolean isPreviousLoadFinished() {
        return Catalog.getCurrentGlobalTransactionMgr().isPreviousTransactionsFinished(watershedTxnId, dbId);
    }

    public static RollupJobV2 read(DataInput in) throws IOException {
        RollupJobV2 rollupJob = new RollupJobV2();
        rollupJob.readFields(in);
        return rollupJob;
    }

    @Override
    public synchronized void write(DataOutput out) throws IOException {
        super.write(out);

        out.writeInt(partitionIdToRollupIndex.size());
        for (long partitionId : partitionIdToRollupIndex.keySet()) {
            out.writeLong(partitionId);

            out.writeInt(partitionIdToBaseRollupTabletIdMap.get(partitionId).size());
            for (Map.Entry<Long, Long> entry : partitionIdToBaseRollupTabletIdMap.get(partitionId).entrySet()) {
                out.writeLong(entry.getKey());
                out.writeLong(entry.getValue());
            }

            MaterializedIndex rollupIndex = partitionIdToRollupIndex.get(partitionId);
            rollupIndex.write(out);
        }

        out.writeLong(baseIndexId);
        out.writeLong(rollupIndexId);
        Text.writeString(out, baseIndexName);
        Text.writeString(out, rollupIndexName);

        // rollup schema
        out.writeInt(rollupSchema.size());
        for (Column column : rollupSchema) {
            column.write(out);
        }
        out.writeInt(baseSchemaHash);
        out.writeInt(rollupSchemaHash);

        Text.writeString(out, rollupKeysType.name());
        out.writeShort(rollupShortKeyColumnCount);

        out.writeLong(watershedTxnId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            long partitionId = in.readLong();
            int size2 = in.readInt();
            Map<Long, Long> tabletIdMap = partitionIdToBaseRollupTabletIdMap.get(partitionId);
            if (tabletIdMap == null) {
                tabletIdMap = Maps.newHashMap();
                partitionIdToBaseRollupTabletIdMap.put(partitionId, tabletIdMap);
            }
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
    }

    /*
     * Replay job in PENDING state.
     * Should replay all changes before this job's state transfer to PENDING.
     * These changes should be same as changes in RollupHander.processAddRollup()
     */
    private void replayPending() {
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            // database may be dropped before replaying this log. just return
            return;
        }

        db.writeLock();
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                // table may be dropped before replaying this log. just return
                return;
            }

            // add all rollup replicas to tablet inverted index
            TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
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
            tbl.setState(OlapTableState.ROLLUP);
        } finally {
            db.writeUnlock();
        }
        LOG.info("replay pending rollup job: {}", jobId);
    }

    /*
     * Replay job in WAITING_TXN state.
     * Should replay all changes in runPendingJob()
     */
    private void replayWaitingTxn() {
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            // database may be dropped before replaying this log. just return
            return;
        }

        db.writeLock();
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                // table may be dropped before replaying this log. just return
                return;
            }
            addRollupIndexToCatalog(tbl);
        } finally {
            db.writeUnlock();
        }
        LOG.info("replay waiting txn rollup job: {}", jobId);
    }

    /*
     * Replay job in FINISHED state.
     * Should replay all changes in runRuningJob()
     */
    private void replayFinished() {
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db != null) {
            db.writeLock();
            try {
                OlapTable tbl = (OlapTable) db.getTable(tableId);
                if (tbl != null) {
                    Preconditions.checkState(tbl.getState() == OlapTableState.ROLLUP);
                    for (Partition partition : tbl.getPartitions()) {
                        MaterializedIndex rollupIndex = partition.getIndex(rollupIndexId);
                        Preconditions.checkNotNull(rollupIndex, rollupIndex);
                        rollupIndex.setState(IndexState.NORMAL);
                    }
                    tbl.setState(OlapTableState.NORMAL);
                }
            } finally {
                db.writeUnlock();
            }
        }
        LOG.info("replay finished rollup job: {}", jobId);
    }

    /*
     * Replay job in CANCELLED state.
     */
    private void replayCancelled() {
        cancelInternal();
        LOG.info("replay cancelled rollup job: {}", jobId);
    }

    public void replay() {
        switch (jobState) {
            case PENDING:
                replayPending();
                break;
            case WAITING_TXN:
                replayWaitingTxn();
                break;
            case FINISHED:
                replayFinished();
                break;
            case CANCELLED:
                replayCancelled();
                break;
            default:
                break;
        }
    }

    @Override
    protected void getInfo(List<Comparable> info) {
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
            info.add("N/A");
        }
        info.add(timeoutMs / 1000);
    }

    public List<List<String>> getUnfinishedTasks(int limit) {
        List<List<String>> taskInfos = Lists.newArrayList();
        if (jobState == JobState.RUNNING) {
            List<AgentTask> tasks = rollupBatchTask.getUnfinishedTasks(limit);
            for (AgentTask agentTask : tasks) {
                CreateRollupTaskV2 rollupTask = (CreateRollupTaskV2)agentTask;
                List<String> info = Lists.newArrayList();
                info.add(String.valueOf(rollupTask.getBackendId()));
                info.add(String.valueOf(rollupTask.getBaseTabletId()));
                info.add(String.valueOf(rollupTask.getSignature()));
                taskInfos.add(info);
            }
        }
        return taskInfos;
    }
}
