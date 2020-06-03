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
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.SchemaVersionAndHash;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.AlterReplicaTask;
import org.apache.doris.task.CreateReplicaTask;
import org.apache.doris.thrift.TStorageFormat;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTaskType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import com.google.gson.annotations.SerializedName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/*
 * Version 2 of SchemaChangeJob.
 * This is for replacing the old SchemaChangeJob
 * https://github.com/apache/incubator-doris/issues/1429
 */
public class SchemaChangeJobV2 extends AlterJobV2 {
    private static final Logger LOG = LogManager.getLogger(SchemaChangeJobV2.class);

    // partition id -> (shadow index id -> (shadow tablet id -> origin tablet id))
    @SerializedName(value = "partitionIndexTabletMap")
    private Table<Long, Long, Map<Long, Long>> partitionIndexTabletMap = HashBasedTable.create();
    // partition id -> (shadow index id -> shadow index))
    @SerializedName(value = "partitionIndexMap")
    private Table<Long, Long, MaterializedIndex> partitionIndexMap = HashBasedTable.create();
    // shadow index id -> origin index id
    @SerializedName(value = "indexIdMap")
    private Map<Long, Long> indexIdMap = Maps.newHashMap();
    // shadow index id -> shadow index name(__doris_shadow_xxx)
    @SerializedName(value = "indexIdToName")
    private Map<Long, String> indexIdToName = Maps.newHashMap();
    // shadow index id -> index schema
    @SerializedName(value = "indexSchemaMap")
    private Map<Long, List<Column>> indexSchemaMap = Maps.newHashMap();
    // shadow index id -> (shadow index schema version : schema hash)
    @SerializedName(value = "indexSchemaVersionAndHashMap")
    private Map<Long, SchemaVersionAndHash> indexSchemaVersionAndHashMap = Maps.newHashMap();
    // shadow index id -> shadow index short key count
    @SerializedName(value = "indexShortKeyMap")
    private Map<Long, Short> indexShortKeyMap = Maps.newHashMap();

    // bloom filter info
    @SerializedName(value = "hasBfChange")
    private boolean hasBfChange;
    @SerializedName(value = "bfColumns")
    private Set<String> bfColumns = null;
    @SerializedName(value = "bfFpp")
    private double bfFpp = 0;

    // alter index info
    @SerializedName(value = "indexChange")
    private boolean indexChange = false;
    @SerializedName(value = "indexes")
    private List<Index> indexes = null;

    // The schema change job will wait all transactions before this txn id finished, then send the schema change tasks.
    @SerializedName(value = "watershedTxnId")
    protected long watershedTxnId = -1;
    @SerializedName(value = "storageFormat")
    private TStorageFormat storageFormat = TStorageFormat.DEFAULT;

    // save all schema change tasks
    private AgentBatchTask schemaChangeBatchTask = new AgentBatchTask();

    public SchemaChangeJobV2(long jobId, long dbId, long tableId, String tableName, long timeoutMs) {
        super(jobId, JobType.SCHEMA_CHANGE, dbId, tableId, tableName, timeoutMs);
    }

    private SchemaChangeJobV2() {
        super(JobType.SCHEMA_CHANGE);
    }

    public void addTabletIdMap(long partitionId, long shadowIdxId, long shadowTabletId, long originTabletId) {
        Map<Long, Long> tabletMap = partitionIndexTabletMap.get(partitionId, shadowIdxId);
        if (tabletMap == null) {
            tabletMap = Maps.newHashMap();
            partitionIndexTabletMap.put(partitionId, shadowIdxId, tabletMap);
        }
        tabletMap.put(shadowTabletId, originTabletId);
    }

    public void addPartitionShadowIndex(long partitionId, long shadowIdxId, MaterializedIndex shadowIdx) {
        partitionIndexMap.put(partitionId, shadowIdxId, shadowIdx);
    }

    public void addIndexSchema(long shadowIdxId, long originIdxId,
            String shadowIndexName, int shadowSchemaVersion, int shadowSchemaHash,
            short shadowIdxShortKeyCount, List<Column> shadowIdxSchema) {
        indexIdMap.put(shadowIdxId, originIdxId);
        indexIdToName.put(shadowIdxId, shadowIndexName);
        indexSchemaVersionAndHashMap.put(shadowIdxId, new SchemaVersionAndHash(shadowSchemaVersion, shadowSchemaHash));
        indexShortKeyMap.put(shadowIdxId, shadowIdxShortKeyCount);
        indexSchemaMap.put(shadowIdxId, shadowIdxSchema);
    }

    public void setBloomFilterInfo(boolean hasBfChange, Set<String> bfColumns, double bfFpp) {
        this.hasBfChange = hasBfChange;
        this.bfColumns = bfColumns;
        this.bfFpp = bfFpp;
    }

    public void setAlterIndexInfo(boolean indexChange, List<Index> indexes) {
        this.indexChange = indexChange;
        this.indexes = indexes;
    }

    public void setStorageFormat(TStorageFormat storageFormat) {
        this.storageFormat = storageFormat;
    }

    /**
     * clear some date structure in this job to save memory
     * these data structures must not used in getInfo method
     */
    private void pruneMeta() {
        partitionIndexTabletMap.clear();
        partitionIndexMap.clear();
        indexSchemaMap.clear();
        indexShortKeyMap.clear();
    }

    /**
     * runPendingJob():
     * 1. Create all replicas of all shadow indexes and wait them finished.
     * 2. After creating done, add the shadow indexes to catalog, user can not see this
     *    shadow index, but internal load process will generate data for these indexes.
     * 3. Get a new transaction id, then set job's state to WAITING_TXN
     */
    @Override
    protected void runPendingJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.PENDING, jobState);
        LOG.info("begin to send create replica tasks. job: {}", jobId);
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            throw new AlterCancelException("Databasee " + dbId + " does not exist");
        }

        if (!checkTableStable(db)) {
            return;
        }

        // 1. create replicas
        AgentBatchTask batchTask = new AgentBatchTask();
        // count total replica num
        int totalReplicaNum = 0;
        for (MaterializedIndex shadowIdx : partitionIndexMap.values()) {
            for (Tablet tablet : shadowIdx.getTablets()) {
                totalReplicaNum += tablet.getReplicas().size();
            }
        }
        MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<>(totalReplicaNum);

        OlapTable tbl = (OlapTable) db.getTable(tableId);
        if (tbl == null) {
            throw new AlterCancelException("Table " + tableId + " does not exist");
        }

        tbl.readLock();
        try {

            Preconditions.checkState(tbl.getState() == OlapTableState.SCHEMA_CHANGE);
            for (long partitionId : partitionIndexMap.rowKeySet()) {
                Partition partition = tbl.getPartition(partitionId);
                if (partition == null) {
                    continue;
                }
                TStorageMedium storageMedium = tbl.getPartitionInfo().getDataProperty(partitionId).getStorageMedium();
                
                Map<Long, MaterializedIndex> shadowIndexMap = partitionIndexMap.row(partitionId);
                for (Map.Entry<Long, MaterializedIndex> entry : shadowIndexMap.entrySet()) {
                    long shadowIdxId = entry.getKey();
                    MaterializedIndex shadowIdx = entry.getValue();
                    
                    short shadowShortKeyColumnCount = indexShortKeyMap.get(shadowIdxId);
                    List<Column> shadowSchema = indexSchemaMap.get(shadowIdxId);
                    int shadowSchemaHash = indexSchemaVersionAndHashMap.get(shadowIdxId).schemaHash;
                    long originIndexId = indexIdMap.get(shadowIdxId);
                    int originSchemaHash = tbl.getSchemaHashByIndexId(originIndexId);
                    KeysType originKeysType = tbl.getKeysTypeByIndexId(originIndexId);
                    
                    for (Tablet shadowTablet : shadowIdx.getTablets()) {
                        long shadowTabletId = shadowTablet.getId();
                        List<Replica> shadowReplicas = shadowTablet.getReplicas();
                        for (Replica shadowReplica : shadowReplicas) {
                            long backendId = shadowReplica.getBackendId();
                            countDownLatch.addMark(backendId, shadowTabletId);
                            CreateReplicaTask createReplicaTask = new CreateReplicaTask(
                                    backendId, dbId, tableId, partitionId, shadowIdxId, shadowTabletId,
                                    shadowShortKeyColumnCount, shadowSchemaHash,
                                    Partition.PARTITION_INIT_VERSION, Partition.PARTITION_INIT_VERSION_HASH,
                                    originKeysType, TStorageType.COLUMN, storageMedium,
                                    shadowSchema, bfColumns, bfFpp, countDownLatch, indexes,
                                    tbl.isInMemory(),
                                    tbl.getPartitionInfo().getTabletType(partitionId));
                            createReplicaTask.setBaseTablet(partitionIndexTabletMap.get(partitionId, shadowIdxId).get(shadowTabletId), originSchemaHash);
                            if (this.storageFormat != null) {
                                createReplicaTask.setStorageFormat(this.storageFormat);
                            }

                            batchTask.addTask(createReplicaTask);
                        } // end for rollupReplicas
                    } // end for rollupTablets
                }
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

            if (!ok) {
                // create replicas failed. just cancel the job
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
                LOG.warn("failed to create replicas for job: {}, {}", jobId, errMsg);
                throw new AlterCancelException("Create replicas failed. Error: " + errMsg);
            }
        }

        // create all replicas success.
        // add all shadow indexes to catalog
        tbl.writeLock();
        try {
            Preconditions.checkState(tbl.getState() == OlapTableState.SCHEMA_CHANGE);
            addShadowIndexToCatalog(tbl);
        } finally {
            tbl.writeUnlock();
        }

        this.watershedTxnId = Catalog.getCurrentGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId();
        this.jobState = JobState.WAITING_TXN;

        // write edit log
        Catalog.getCurrentCatalog().getEditLog().logAlterJob(this);
        LOG.info("transfer schema change job {} state to {}, watershed txn id: {}", jobId, this.jobState, watershedTxnId);
    }

    private void addShadowIndexToCatalog(OlapTable tbl) {
        for (long partitionId : partitionIndexMap.rowKeySet()) {
            Partition partition = tbl.getPartition(partitionId);
            if (partition == null) {
                continue;
            }
            Map<Long, MaterializedIndex> shadowIndexMap = partitionIndexMap.row(partitionId);
            for (MaterializedIndex shadowIndex : shadowIndexMap.values()) {
                Preconditions.checkState(shadowIndex.getState() == IndexState.SHADOW, shadowIndex.getState());
                partition.createRollupIndex(shadowIndex);
            }
        }

        for (long shadowIdxId : indexIdMap.keySet()) {
            tbl.setIndexMeta(shadowIdxId, indexIdToName.get(shadowIdxId), indexSchemaMap.get(shadowIdxId),
                    indexSchemaVersionAndHashMap.get(shadowIdxId).schemaVersion,
                    indexSchemaVersionAndHashMap.get(shadowIdxId).schemaHash,
                    indexShortKeyMap.get(shadowIdxId), TStorageType.COLUMN,
                             tbl.getKeysTypeByIndexId(indexIdMap.get(shadowIdxId)));
        }

        tbl.rebuildFullSchema();
    }

    /**
     * runWaitingTxnJob():
     * 1. Wait the transactions before the watershedTxnId to be finished.
     * 2. If all previous transactions finished, send schema change tasks to BE.
     * 3. Change job state to RUNNING.
     */
    @Override
    protected void runWaitingTxnJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.WAITING_TXN, jobState);

        try {
            if (!isPreviousLoadFinished()) {
                LOG.info("wait transactions before {} to be finished, schema change job: {}", watershedTxnId, jobId);
                return;
            }
        } catch (AnalysisException e) {
            throw new AlterCancelException(e.getMessage());
        }

        LOG.info("previous transactions are all finished, begin to send schema change tasks. job: {}", jobId);
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            throw new AlterCancelException("Databasee " + dbId + " does not exist");
        }

        OlapTable tbl = (OlapTable) db.getTable(tableId);
        if (tbl == null) {
            throw new AlterCancelException("Table " + tableId + " does not exist");
        }

        tbl.readLock();
        try {
            Preconditions.checkState(tbl.getState() == OlapTableState.SCHEMA_CHANGE);

            for (long partitionId : partitionIndexMap.rowKeySet()) {
                Partition partition = tbl.getPartition(partitionId);
                Preconditions.checkNotNull(partition, partitionId);

                // the schema change task will transform the data before visible version(included).
                long visibleVersion = partition.getVisibleVersion();
                long visibleVersionHash = partition.getVisibleVersionHash();

                Map<Long, MaterializedIndex> shadowIndexMap = partitionIndexMap.row(partitionId);
                for (Map.Entry<Long, MaterializedIndex> entry : shadowIndexMap.entrySet()) {
                    long shadowIdxId = entry.getKey();
                    MaterializedIndex shadowIdx = entry.getValue();

                    long originIdxId = indexIdMap.get(shadowIdxId);
                    int shadowSchemaHash = indexSchemaVersionAndHashMap.get(shadowIdxId).schemaHash;
                    int originSchemaHash = tbl.getSchemaHashByIndexId(indexIdMap.get(shadowIdxId));

                    for (Tablet shadowTablet : shadowIdx.getTablets()) {
                        long shadowTabletId = shadowTablet.getId();
                        long originTabletId = partitionIndexTabletMap.get(partitionId, shadowIdxId).get(shadowTabletId);
                        List<Replica> shadowReplicas = shadowTablet.getReplicas();
                        for (Replica shadowReplica : shadowReplicas) {
                            AlterReplicaTask rollupTask = new AlterReplicaTask(
                                    shadowReplica.getBackendId(), dbId, tableId, partitionId,
                                    shadowIdxId, originIdxId,
                                    shadowTabletId, originTabletId, shadowReplica.getId(),
                                    shadowSchemaHash, originSchemaHash,
                                    visibleVersion, visibleVersionHash, jobId, JobType.SCHEMA_CHANGE);
                            schemaChangeBatchTask.addTask(rollupTask);
                        }
                    }
                }
            } // end for partitions
        } finally {
            tbl.readUnlock();
        }

        AgentTaskQueue.addBatchTask(schemaChangeBatchTask);
        AgentTaskExecutor.submit(schemaChangeBatchTask);

        this.jobState = JobState.RUNNING;

        // DO NOT write edit log here, tasks will be send again if FE restart or master changed.
        LOG.info("transfer schema change job {} state to {}", jobId, this.jobState);
    }

    /**
     * runRunningJob()
     * 1. Wait all schema change tasks to be finished.
     * 2. Check the integrity of the newly created shadow indexes.
     * 3. Replace the origin index with shadow index, and set shadow index's state as NORMAL to be visible to user.
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
            throw new AlterCancelException("Database " + dbId + " does not exist");
        }

        OlapTable tbl = (OlapTable) db.getTable(tableId);
        if (tbl == null) {
            throw new AlterCancelException("Table " + tableId + " does not exist");
        }


        if (!schemaChangeBatchTask.isFinished()) {
            LOG.info("schema change tasks not finished. job: {}", jobId);
            List<AgentTask> tasks = schemaChangeBatchTask.getUnfinishedTasks(2000);
            for (AgentTask task : tasks) {
                if (task.getFailedTimes() >= 3) {
                    throw new AlterCancelException("schema change task failed after try three times: " + task.getErrorMsg());
                }
            }
            return;
        }


        /*
         * all tasks are finished. check the integrity.
         * we just check whether all new replicas are healthy.
         */
        tbl = (OlapTable) db.getTable(tableId);
        if (tbl == null) {
            throw new AlterCancelException("Table " + tableId + " does not exist");
        }
        tbl.writeLock();
        try {
            Preconditions.checkState(tbl.getState() == OlapTableState.SCHEMA_CHANGE);

            for (long partitionId : partitionIndexMap.rowKeySet()) {
                Partition partition = tbl.getPartition(partitionId);
                Preconditions.checkNotNull(partition, partitionId);

                long visiableVersion = partition.getVisibleVersion();
                long visiableVersionHash = partition.getVisibleVersionHash();
                short expectReplicationNum = tbl.getPartitionInfo().getReplicationNum(partition.getId());

                Map<Long, MaterializedIndex> shadowIndexMap = partitionIndexMap.row(partitionId);
                for (Map.Entry<Long, MaterializedIndex> entry : shadowIndexMap.entrySet()) {
                    MaterializedIndex shadowIdx = entry.getValue();

                    for (Tablet shadowTablet : shadowIdx.getTablets()) {
                        List<Replica> replicas = shadowTablet.getReplicas();
                        int healthyReplicaNum = 0;
                        for (Replica replica : replicas) {
                            if (replica.getLastFailedVersion() < 0
                                    && replica.checkVersionCatchUp(visiableVersion, visiableVersionHash, false)) {
                                healthyReplicaNum++;
                            }
                        }

                        if (healthyReplicaNum < expectReplicationNum / 2 + 1) {
                            LOG.warn("shadow tablet {} has few healthy replicas: {}, schema change job: {}",
                                    shadowTablet.getId(), replicas, jobId);
                            throw new AlterCancelException(
                                    "shadow tablet " + shadowTablet.getId() + " has few healthy replicas");
                        }
                    } // end for tablets
                }
            } // end for partitions

            // all partitions are good
            onFinished(tbl);
        } finally {
            tbl.writeUnlock();
        }

        pruneMeta();
        this.jobState = JobState.FINISHED;
        this.finishedTimeMs = System.currentTimeMillis();

        Catalog.getCurrentCatalog().getEditLog().logAlterJob(this);
        LOG.info("schema change job finished: {}", jobId);
    }

    private void onFinished(OlapTable tbl) {
        // replace the origin index with shadow index, set index state as NORMAL
        for (Partition partition : tbl.getPartitions()) {
            // drop the origin index from partitions
            for (Map.Entry<Long, Long> entry : indexIdMap.entrySet()) {
                long shadowIdxId = entry.getKey();
                long originIdxId = entry.getValue();
                // get index from catalog, not from 'partitionIdToRollupIndex'.
                // because if this alter job is recovered from edit log, index in 'partitionIndexMap'
                // is not the same object in catalog. So modification on that index can not reflect to the index
                // in catalog.
                MaterializedIndex shadowIdx = partition.getIndex(shadowIdxId);
                Preconditions.checkNotNull(shadowIdx, shadowIdxId);
                MaterializedIndex droppedIdx = null;
                if (originIdxId == partition.getBaseIndex().getId()) {
                    droppedIdx = partition.getBaseIndex();
                } else {
                    droppedIdx = partition.deleteRollupIndex(originIdxId);
                }
                Preconditions.checkNotNull(droppedIdx, originIdxId + " vs. " + shadowIdxId);

                // set replica state
                for (Tablet tablet : shadowIdx.getTablets()) {
                    for (Replica replica : tablet.getReplicas()) {
                        replica.setState(ReplicaState.NORMAL);
                    }
                }

                partition.visualiseShadowIndex(shadowIdxId, originIdxId == partition.getBaseIndex().getId());

                // delete origin replicas
                for (Tablet originTablet : droppedIdx.getTablets()) {
                    Catalog.getCurrentInvertedIndex().deleteTablet(originTablet.getId());
                }
            }
        }

        // update index schema info of each index
        for (Map.Entry<Long, Long> entry : indexIdMap.entrySet()) {
            long shadowIdxId = entry.getKey();
            long originIdxId = entry.getValue();
            String shadowIdxName = tbl.getIndexNameById(shadowIdxId);
            String originIdxName = tbl.getIndexNameById(originIdxId);
            tbl.deleteIndexInfo(originIdxName);
            // the shadow index name is '__doris_shadow_xxx', rename it to origin name 'xxx'
            // this will also remove the prefix of columns
            tbl.renameIndexForSchemaChange(shadowIdxName, originIdxName);
            tbl.renameColumnNamePrefix(shadowIdxId);

            if (originIdxId == tbl.getBaseIndexId()) {
                // set base index
                tbl.setBaseIndexId(shadowIdxId);
            }
        }
        // rebuild table's full schema
        tbl.rebuildFullSchema();

        // update bloom filter
        if (hasBfChange) {
            tbl.setBloomFilterInfo(bfColumns, bfFpp);
        }
        // update index
        if (indexChange) {
            tbl.setIndexes(indexes);
        }

        // set storage format of table, only set if format is v2
        if (storageFormat == TStorageFormat.V2) {
            tbl.setStorageFormat(storageFormat);
        }

        tbl.setState(OlapTableState.NORMAL);
    }

    /*
     * cancelImpl() can be called any time any place.
     * We need to clean any possible residual of this job.
     */
    @Override
    protected synchronized boolean cancelImpl(String errMsg) {
        if (jobState.isFinalState()) {
            return false;
        }

        cancelInternal();

        pruneMeta();
        this.errMsg = errMsg;
        this.finishedTimeMs = System.currentTimeMillis();
        LOG.info("cancel {} job {}, err: {}", this.type, jobId, errMsg);
        Catalog.getCurrentCatalog().getEditLog().logAlterJob(this);
        return true;
    }

    private void cancelInternal() {
        // clear tasks if has
        AgentTaskQueue.removeBatchTask(schemaChangeBatchTask, TTaskType.ALTER);
        // remove all shadow indexes, and set state to NORMAL
        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db != null) {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl != null) {
                tbl.writeLock();
                try {
                    for (long partitionId : partitionIndexMap.rowKeySet()) {
                        Partition partition = tbl.getPartition(partitionId);
                        Preconditions.checkNotNull(partition, partitionId);

                        Map<Long, MaterializedIndex> shadowIndexMap = partitionIndexMap.row(partitionId);
                        for (Map.Entry<Long, MaterializedIndex> entry : shadowIndexMap.entrySet()) {
                            MaterializedIndex shadowIdx = entry.getValue();
                            for (Tablet shadowTablet : shadowIdx.getTablets()) {
                                invertedIndex.deleteTablet(shadowTablet.getId());
                            }
                            partition.deleteRollupIndex(shadowIdx.getId());
                        }
                    }
                    for (String shadowIndexName : indexIdToName.values()) {
                        tbl.deleteIndexInfo(shadowIndexName);
                    }
                    tbl.setState(OlapTableState.NORMAL);
                } finally {
                    tbl.writeUnlock();
                }
            }
        }

        jobState = JobState.CANCELLED;
    }

    // Check whether transactions of the given database which txnId is less than 'watershedTxnId' are finished.
    protected boolean isPreviousLoadFinished() throws AnalysisException {
        return Catalog.getCurrentGlobalTransactionMgr().isPreviousTransactionsFinished(watershedTxnId, dbId, Lists.newArrayList(tableId));
    }

    /**
     * Replay job in PENDING state.
     * Should replay all changes before this job's state transfer to PENDING.
     * These changes should be same as changes in SchemaChangeHandler.createJob()
     */
    private void replayPending(SchemaChangeJobV2 replayedJob) {
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
            TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
            for (Cell<Long, Long, MaterializedIndex> cell : partitionIndexMap.cellSet()) {
                long partitionId = cell.getRowKey();
                long shadowIndexId = cell.getColumnKey();
                MaterializedIndex shadowIndex = cell.getValue();

                TStorageMedium medium = tbl.getPartitionInfo().getDataProperty(partitionId).getStorageMedium();
                TabletMeta shadowTabletMeta = new TabletMeta(dbId, tableId, partitionId, shadowIndexId,
                        indexSchemaVersionAndHashMap.get(shadowIndexId).schemaHash, medium);

                for (Tablet shadownTablet : shadowIndex.getTablets()) {
                    invertedIndex.addTablet(shadownTablet.getId(), shadowTabletMeta);
                    for (Replica shadowReplica : shadownTablet.getReplicas()) {
                        invertedIndex.addReplica(shadownTablet.getId(), shadowReplica);
                    }
                }
            }
            
            // set table state
            tbl.setState(OlapTableState.SCHEMA_CHANGE);
        } finally {
            tbl.writeUnlock();
        }
        
        this.watershedTxnId = replayedJob.watershedTxnId;
        jobState = JobState.WAITING_TXN;
        LOG.info("replay pending schema change job: {}", jobId);
    }

    /**
     * Replay job in WAITING_TXN state.
     * Should replay all changes in runPendingJob()
     */
    private void replayWaitingTxn(SchemaChangeJobV2 replayedJob) {
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
            addShadowIndexToCatalog(tbl);
        } finally {
            tbl.writeUnlock();
        }

        // should still be in WAITING_TXN state, so that the alter tasks will be resend again
        this.jobState = JobState.WAITING_TXN;
        this.watershedTxnId = replayedJob.watershedTxnId;
        LOG.info("replay waiting txn schema change job: {}", jobId);
    }

    /**
     * Replay job in FINISHED state.
     * Should replay all changes in runRuningJob()
     */
    private void replayFinished(SchemaChangeJobV2 replayedJob) {
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db != null) {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl != null) {
                tbl.writeLock();
                try {
                    onFinished(tbl);
                } finally {
                    tbl.writeUnlock();
                }

            }
        }
        jobState = JobState.FINISHED;
        this.finishedTimeMs = replayedJob.finishedTimeMs;
        LOG.info("replay finished schema change job: {}", jobId);
    }

    /**
     * Replay job in CANCELLED state.
     */
    private void replayCancelled(SchemaChangeJobV2 replayedJob) {
        cancelInternal();
        this.jobState = JobState.CANCELLED;
        this.finishedTimeMs = replayedJob.finishedTimeMs;
        this.errMsg = replayedJob.errMsg;
        LOG.info("replay cancelled schema change job: {}", jobId);
    }

    @Override
    public void replay(AlterJobV2 replayedJob) {
        SchemaChangeJobV2 replayedSchemaChangeJob = (SchemaChangeJobV2) replayedJob;
        switch (replayedJob.jobState) {
            case PENDING:
                replayPending(replayedSchemaChangeJob);
                break;
            case WAITING_TXN:
                replayWaitingTxn(replayedSchemaChangeJob);
                break;
            case FINISHED:
                replayFinished(replayedSchemaChangeJob);
                break;
            case CANCELLED:
                replayCancelled(replayedSchemaChangeJob);
                break;
            default:
                break;
        }
    }

    @Override
    protected void getInfo(List<List<Comparable>> infos) {
        // calc progress first. all index share the same process
        String progress = FeConstants.null_string;
        if (jobState == JobState.RUNNING && schemaChangeBatchTask.getTaskNum() > 0) {
            progress = schemaChangeBatchTask.getFinishedTaskNum() + "/" + schemaChangeBatchTask.getTaskNum();
        }

        // one line for one shadow index
        for (Map.Entry<Long, Long> entry : indexIdMap.entrySet()) {
            long shadowIndexId = entry.getKey();
            List<Comparable> info = Lists.newArrayList();
            info.add(jobId);
            info.add(tableName);
            info.add(TimeUtils.longToTimeString(createTimeMs));
            info.add(TimeUtils.longToTimeString(finishedTimeMs));
            // only show the origin index name
            info.add(indexIdToName.get(shadowIndexId).substring(SchemaChangeHandler.SHADOW_NAME_PRFIX.length()));
            info.add(shadowIndexId);
            info.add(entry.getValue());
            info.add(indexSchemaVersionAndHashMap.get(shadowIndexId).toString());
            info.add(watershedTxnId);
            info.add(jobState.name());
            info.add(errMsg);
            info.add(progress);
            info.add(timeoutMs / 1000);
            infos.add(info);
        }
    }

    public List<List<String>> getUnfinishedTasks(int limit) {
        List<List<String>> taskInfos = Lists.newArrayList();
        if (jobState == JobState.RUNNING) {
            List<AgentTask> tasks = schemaChangeBatchTask.getUnfinishedTasks(limit);
            for (AgentTask agentTask : tasks) {
                AlterReplicaTask alterTask = (AlterReplicaTask) agentTask;
                List<String> info = Lists.newArrayList();
                info.add(String.valueOf(alterTask.getBackendId()));
                info.add(String.valueOf(alterTask.getBaseTabletId()));
                info.add(String.valueOf(alterTask.getSignature()));
                taskInfos.add(info);
            }
        }
        return taskInfos;
    }

    /**
     * read data need to persist when job not finish
     */
    private void readJobNotFinishData(DataInput in) throws IOException {
        int partitionNum = in.readInt();
        for (int i = 0; i < partitionNum; i++) {
            long partitionId = in.readLong();
            int indexNum = in.readInt();
            for (int j = 0; j < indexNum; j++) {
                long shadowIndexId = in.readLong();
                int tabletNum = in.readInt();
                Map<Long, Long> tabletMap = Maps.newHashMapWithExpectedSize(tabletNum);
                for (int k = 0; k < tabletNum; k++) {
                    long shadowTabletId = in.readLong();
                    long originTabletId = in.readLong();
                    tabletMap.put(shadowTabletId, originTabletId);
                }
                partitionIndexTabletMap.put(partitionId, shadowIndexId, tabletMap);
                // shadow index
                MaterializedIndex shadowIndex = MaterializedIndex.read(in);
                partitionIndexMap.put(partitionId, shadowIndexId, shadowIndex);
            }
        }

        // shadow index info
        int indexNum = in.readInt();
        for (int i = 0; i < indexNum; i++) {
            long shadowIndexId = in.readLong();
            long originIndexId = in.readLong();
            String indexName = Text.readString(in);
            // index schema
            int colNum = in.readInt();
            List<Column> schema = Lists.newArrayListWithCapacity(colNum);
            for (int j = 0; j < colNum; j++) {
                schema.add(Column.read(in));
            }
            int schemaVersion = in.readInt();
            int schemaVersionHash = in.readInt();
            SchemaVersionAndHash schemaVersionAndHash = new SchemaVersionAndHash(schemaVersion, schemaVersionHash);
            short shortKeyCount = in.readShort();

            indexIdMap.put(shadowIndexId, originIndexId);
            indexIdToName.put(shadowIndexId, indexName);
            indexSchemaMap.put(shadowIndexId, schema);
            indexSchemaVersionAndHashMap.put(shadowIndexId, schemaVersionAndHash);
            indexShortKeyMap.put(shadowIndexId, shortKeyCount);
        }

        // bloom filter
        hasBfChange = in.readBoolean();
        if (hasBfChange) {
            int bfNum = in.readInt();
            bfColumns = Sets.newHashSetWithExpectedSize(bfNum);
            for (int i = 0; i < bfNum; i++) {
                bfColumns.add(Text.readString(in));
            }
            bfFpp = in.readDouble();
        }

        watershedTxnId = in.readLong();

        // index
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_70) {
            indexChange = in.readBoolean();
            if (indexChange) {
                if (in.readBoolean()) {
                    int indexCount = in.readInt();
                    this.indexes = new ArrayList<>();
                    for (int i = 0; i < indexCount; ++i) {
                        this.indexes.add(Index.read(in));
                    }
                } else {
                    this.indexes = null;
                }
            }
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_84) {
            storageFormat = TStorageFormat.valueOf(Text.readString(in));
        }
    }

    /**
     * read data need to persist when job finished
     */
    private void readJobFinishedData(DataInput in) throws IOException {
        // shadow index info
        int indexNum = in.readInt();
        for (int i = 0; i < indexNum; i++) {
            long shadowIndexId = in.readLong();
            long originIndexId = in.readLong();
            String indexName = Text.readString(in);
            int schemaVersion = in.readInt();
            int schemaVersionHash = in.readInt();
            SchemaVersionAndHash schemaVersionAndHash = new SchemaVersionAndHash(schemaVersion, schemaVersionHash);

            indexIdMap.put(shadowIndexId, originIndexId);
            indexIdToName.put(shadowIndexId, indexName);
            indexSchemaVersionAndHashMap.put(shadowIndexId, schemaVersionAndHash);
        }

        // bloom filter
        hasBfChange = in.readBoolean();
        if (hasBfChange) {
            int bfNum = in.readInt();
            bfColumns = Sets.newHashSetWithExpectedSize(bfNum);
            for (int i = 0; i < bfNum; i++) {
                bfColumns.add(Text.readString(in));
            }
            bfFpp = in.readDouble();
        }

        watershedTxnId = in.readLong();

        // index
        indexChange = in.readBoolean();
        if (indexChange) {
            if (in.readBoolean()) {
                int indexCount = in.readInt();
                this.indexes = new ArrayList<>();
                for (int i = 0; i < indexCount; ++i) {
                    this.indexes.add(Index.read(in));
                }
            } else {
                this.indexes = null;
            }
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_84) {
            storageFormat = TStorageFormat.valueOf(Text.readString(in));
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
    public static SchemaChangeJobV2 read(DataInput in) throws IOException {
        Preconditions.checkState(Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_86);
        SchemaChangeJobV2 schemaChangeJob = new SchemaChangeJobV2();
        schemaChangeJob.readFields(in);
        return schemaChangeJob;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_80) {
            boolean isMetaPruned = in.readBoolean();
            if (isMetaPruned) {
                readJobFinishedData(in);
            } else {
                readJobNotFinishData(in);
            }
        } else {
            readJobNotFinishData(in);
        }
    }

}
