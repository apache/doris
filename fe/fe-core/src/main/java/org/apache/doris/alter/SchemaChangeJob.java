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
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Partition.PartitionState;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.persist.ReplicaPersistInfo;
import org.apache.doris.persist.ReplicaPersistInfo.ReplicaOperationType;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.ClearAlterTask;
import org.apache.doris.task.SchemaChangeTask;
import org.apache.doris.thrift.TKeysType;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTabletInfo;
import org.apache.doris.thrift.TTaskType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class SchemaChangeJob extends AlterJob {
    private static final Logger LOG = LogManager.getLogger(SchemaChangeJob.class);

    private String tableName;

    // indexId -> changedColumns
    private Map<Long, List<Column>> changedIndexIdToSchema;
    // indexId -> new schema version
    private Map<Long, Integer> changedIndexIdToSchemaVersion;
    // indexId -> new schema hash
    private Map<Long, Integer> changedIndexIdToSchemaHash;
    // indexId -> new short key column count
    private Map<Long, Short> changedIndexIdToShortKeyColumnCount;

    private TResourceInfo resourceInfo;

    // partition id -> list of replica infos
    private Multimap<Long, ReplicaPersistInfo> replicaInfos;

    /*
     * infos bellow do not need to persist
     */
    // total unfinished replicas
    private List<Long> unfinishedReplicaIds;
    // for calculating process of schemaChange of an index
    // indexId -> totalReplicaNum
    private Multimap<Long, Long> indexIdToTotalReplicaNum;
    // use index id to count finished replica num
    private Multiset<Long> indexIdToFinishedReplicaNum;

    private Multimap<Long, Long> partitionIdToFinishedIndexIds;

    // bloom filter columns
    private boolean hasBfChange;
    private Set<String> bfColumns;
    private double bfFpp;

    // Init as null, to be compatible with former schema change job.
    // If this is set to null, storage type will remain what it was.
    // This can only set to COLUMN
    private TStorageType newStorageType = null;

    private SchemaChangeJob() {
        this(-1, -1, null, null, -1);
    }

    public SchemaChangeJob(long dbId, long tableId, TResourceInfo resourceInfo, String tableName, long transactionId) {
        super(JobType.SCHEMA_CHANGE, dbId, tableId, resourceInfo);

        this.tableName = tableName;

        this.changedIndexIdToSchema = new HashMap<Long, List<Column>>();
        this.changedIndexIdToSchemaVersion = new HashMap<Long, Integer>();
        this.changedIndexIdToSchemaHash = new HashMap<Long, Integer>();
        this.changedIndexIdToShortKeyColumnCount = new HashMap<Long, Short>();

        this.replicaInfos = LinkedHashMultimap.create();

        this.unfinishedReplicaIds = new LinkedList<Long>();
        this.indexIdToTotalReplicaNum = HashMultimap.create();
        this.indexIdToFinishedReplicaNum = HashMultiset.create();

        this.partitionIdToFinishedIndexIds = HashMultimap.create();

        this.hasBfChange = false;
        this.bfColumns = null;
        this.bfFpp = 0;
        
        this.transactionId = transactionId;
    }

    public final String getTableName() {
        return tableName;
    }

    public synchronized int getTotalReplicaNumByIndexId(long indexId) {
        return this.indexIdToTotalReplicaNum.get(indexId).size();
    }

    // schema
    public void putToChangedIndexSchemaMap(long indexId, List<Column> alterSchema) {
        this.changedIndexIdToSchema.put(indexId, alterSchema);
    }

    public Map<Long, List<Column>> getChangedIndexToSchema() {
        return changedIndexIdToSchema;
    }

    // schema info
    public void setNewSchemaInfo(long indexId, int newSchemaVersion, int newSchemaHash,
            short newShortKeyColumnCount) {
        this.changedIndexIdToSchemaVersion.put(indexId, newSchemaVersion);
        this.changedIndexIdToSchemaHash.put(indexId, newSchemaHash);
        this.changedIndexIdToShortKeyColumnCount.put(indexId, newShortKeyColumnCount);
    }

    // schema version
    public int getSchemaVersionByIndexId(long indexId) {
        if (changedIndexIdToSchemaVersion.containsKey(indexId)) {
            return changedIndexIdToSchemaVersion.get(indexId);
        }
        return -1;
    }

    // schema hash
    public int getSchemaHashByIndexId(long indexId) {
        if (changedIndexIdToSchemaHash.containsKey(indexId)) {
            return changedIndexIdToSchemaHash.get(indexId);
        }
        return -1;
    }

    // short key column count
    public short getShortKeyColumnCountByIndexId(long indexId) {
        if (changedIndexIdToShortKeyColumnCount.containsKey(indexId)) {
            return changedIndexIdToShortKeyColumnCount.get(indexId);
        }
        return (short) -1;
    }

    // bloom filter info
    public void setTableBloomFilterInfo(boolean hasBfChange, Set<String> bfColumns, double bfFpp) {
        this.hasBfChange = hasBfChange;
        this.bfColumns = bfColumns;
        this.bfFpp = bfFpp;
    }

    public void setNewStorageType(TStorageType newStorageType) {
        Preconditions.checkState(newStorageType == TStorageType.COLUMN);
        this.newStorageType = newStorageType;
    }

    public boolean isSchemaHashRelated(int schemaHash) {
        return changedIndexIdToSchemaHash.values().contains(schemaHash);
    }

    public synchronized int getFinishedReplicaNumByIndexId(long indexId) {
        return this.indexIdToFinishedReplicaNum.count(indexId);
    }

    public void deleteAllTableHistorySchema() {
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            LOG.warn("db[{}] does not exist", dbId);
            return;
        }

        OlapTable olapTable = null;
        try {
            olapTable = (OlapTable) db.getTableOrThrowException(tableId, Table.TableType.OLAP);
        } catch (MetaNotFoundException e) {
            LOG.warn(e.getMessage());
            return;
        }

        olapTable.readLock();
        try {
            // drop all replicas with old schemaHash
            for (Partition partition : olapTable.getPartitions()) {
                long partitionId = partition.getId();
                for (Long indexId : this.changedIndexIdToSchema.keySet()) {
                    MaterializedIndex materializedIndex = partition.getIndex(indexId);
                    if (materializedIndex == null) {
                        LOG.warn("index[{}] does not exist in partition[{}-{}-{}]",
                                 indexId, dbId, tableId, partitionId);
                        continue;
                    }

                    // delete schema hash
                    // the real drop task is handled by report process
                    // we call 'deleteNewSchemaHash' but we delete old one actually.
                    // cause schema hash is switched when job is finished.
                    Catalog.getCurrentInvertedIndex().deleteNewSchemaHash(partitionId, indexId);
                    LOG.info("delete old schema. db[{}], table[{}], partition[{}], index[{}]",
                             dbId, tableId, partitionId, indexId);
                }
            } // end for partitions
        } finally {
            olapTable.readUnlock();
        }
    }

    @Override
    public void addReplicaId(long parentId, long replicaId, long backendId) {
        // here parent id is index id
        this.unfinishedReplicaIds.add(replicaId);
        this.indexIdToTotalReplicaNum.put(parentId, replicaId);
        // this.backendIdToReplicaIds.put(backendId, replicaId);
    }

    @Override
    public synchronized void setReplicaFinished(long parentId, long replicaId) {
        // here parent id is index id
        if (!this.unfinishedReplicaIds.remove(replicaId)) {
            // this replica is already removed
            return;
        }

        if (parentId == -1L) {
            // find out which table is replica belongs to
            for (long oneIndexId : indexIdToTotalReplicaNum.keySet()) {
                Collection<Long> replicaIds = indexIdToTotalReplicaNum.get(oneIndexId);
                if (replicaIds.contains(replicaId)) {
                    this.indexIdToFinishedReplicaNum.add(oneIndexId);
                    break;
                }
            }
        } else {
            this.indexIdToFinishedReplicaNum.add(parentId);
        }
    }
    
    /*
     * return
     * 0:  sending clear tasks
     * 1:  all clear tasks are finished, the job is done normally.
     * -1: job meet some fatal error, like db or table is missing.
     */
    public int checkOrResendClearTasks() {
        Preconditions.checkState(this.state == JobState.FINISHING);
        // 1. check if all task finished
        boolean clearFailed = false;
        if (batchClearAlterTask != null) {
            List<AgentTask> allTasks = batchClearAlterTask.getAllTasks();
            for (AgentTask oneClearAlterTask : allTasks) {
                ClearAlterTask clearAlterTask = (ClearAlterTask) oneClearAlterTask;
                if (!clearAlterTask.isFinished()) {
                    clearFailed = true;
                }
                AgentTaskQueue.removeTask(clearAlterTask.getBackendId(), 
                        TTaskType.CLEAR_ALTER_TASK, clearAlterTask.getSignature());
                // not remove the task from batch task, remove it by gc
            }
        }
        if (!clearFailed && batchClearAlterTask != null) {
            return 1;
        }

        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            cancelMsg = "db[" + dbId + "] does not exist";
            LOG.warn(cancelMsg);
            return -1;
        }

        batchClearAlterTask = new AgentBatchTask();
        OlapTable olapTable = null;
        try {
            olapTable = (OlapTable) db.getTableOrThrowException(tableId, Table.TableType.OLAP);
        } catch (MetaNotFoundException e) {
            LOG.warn(e.getMessage());
            return -1;
        }

        olapTable.readLock();
        try {
            boolean allAddSuccess = true;
            LOG.info("sending clear schema change job tasks for table [{}]", tableId);
            OUTER_LOOP:
            for (Partition partition : olapTable.getPartitions()) {
                long partitionId = partition.getId();
                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                    for (Tablet tablet : index.getTablets()) {
                        List<Replica> replicas = tablet.getReplicas();
                        for (Replica replica : replicas) {
                            long backendId = replica.getBackendId();
                            ClearAlterTask clearAlterTask = new ClearAlterTask(backendId, dbId, tableId,
                                    partitionId, index.getId(), tablet.getId(),
                                    olapTable.getSchemaHashByIndexId(index.getId()));
                            if (AgentTaskQueue.addTask(clearAlterTask)) {
                                batchClearAlterTask.addTask(clearAlterTask);
                            } else {
                                allAddSuccess = false;
                                break OUTER_LOOP;
                            }
                        } // end for rollupReplicas
                    } // end for rollupTablets
                } // end for index
            } // end for partition
            if (!allAddSuccess) {
                for (AgentTask task : batchClearAlterTask.getAllTasks()) {
                    AgentTaskQueue.removeTask(task.getBackendId(), task.getTaskType(), task.getSignature());
                }
                batchClearAlterTask = null;
            }
        } finally {
            olapTable.readUnlock();
        }

        LOG.info("successfully sending clear schema change job [{}]", tableId);
        return 0;
    }

    @Override
    public boolean sendTasks() {
        Preconditions.checkState(this.state == JobState.PENDING);
        // here we just sending tasks to AgentTaskQueue.
        // task report process will later resend this task

        LOG.info("sending schema change job {}, start txn id: {}", tableId, transactionId);

        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            String msg = "db[" + dbId + "] does not exist";
            setMsg(msg);
            LOG.warn(msg);
            return false;
        }

        OlapTable olapTable = null;
        try {
            olapTable = (OlapTable) db.getTableOrThrowException(tableId, Table.TableType.OLAP);
        } catch (MetaNotFoundException e) {
            LOG.warn(e.getMessage());
            return false;
        }

        olapTable.readLock();
        try {
            synchronized (this) {
                Preconditions.checkNotNull(this.unfinishedReplicaIds);

                List<AgentTask> tasks = new LinkedList<AgentTask>();
                for (Partition partition : olapTable.getPartitions()) {
                    long partitionId = partition.getId();
                    short replicationNum = olapTable.getPartitionInfo().getReplicationNum(partitionId);
                    for (Long indexId : this.changedIndexIdToSchema.keySet()) {
                        MaterializedIndex alterIndex = partition.getIndex(indexId);
                        if (alterIndex == null) {
                            cancelMsg = "index[" + indexId + "] does not exist in partition[" + partitionId + "]";
                            LOG.warn(cancelMsg);
                            return false;
                        }

                        List<Column> alterSchema = this.changedIndexIdToSchema.get(indexId);
                        int newSchemaHash = this.changedIndexIdToSchemaHash.get(indexId);
                        Preconditions.checkState(newSchemaHash != -1);
                        int baseSchemaHash = olapTable.getSchemaHashByIndexId(indexId);
                        short newShortKeyColumnCount = this.changedIndexIdToShortKeyColumnCount.get(indexId);
                        Preconditions.checkState(newShortKeyColumnCount != (short) -1);
                        KeysType keysType = olapTable.getKeysType();
                        TKeysType schemaChangeKeysType;
                        if (keysType == KeysType.DUP_KEYS) {
                            schemaChangeKeysType = TKeysType.DUP_KEYS;
                        } else if (keysType == KeysType.UNIQUE_KEYS) {
                            schemaChangeKeysType = TKeysType.UNIQUE_KEYS;
                        } else {
                            schemaChangeKeysType = TKeysType.AGG_KEYS;
                        }

                        TStorageType storageType = newStorageType == null ? olapTable.getStorageTypeByIndexId(indexId)
                                : newStorageType;
                        for (Tablet tablet : alterIndex.getTablets()) {
                            long tabletId = tablet.getId();
                            short replicaSendNum = 0;
                            for (Replica replica : tablet.getReplicas()) {
                                if (replica.getState() != ReplicaState.SCHEMA_CHANGE) {
                                    // for now, all replica should be in SCHEMA_CHANGE,
                                    // because we don't allow tablet repair and balance during schema change.
                                    // but in case some edge cases are not took into consideration, we cancel
                                    // the schema change job here.
                                    cancelMsg = String.format(
                                            "replica %d of tablet %d in backend %d state is invalid: %s [send]",
                                            replica.getId(), tablet.getId(), replica.getBackendId(),
                                            replica.getState().name());
                                    LOG.warn(cancelMsg);
                                    return false;
                                }

                                long backendId = replica.getBackendId();
                                long replicaId = replica.getId();
                                SchemaChangeTask schemaChangeTask =
                                        new SchemaChangeTask(resourceInfo, backendId, dbId, tableId,
                                                             partitionId, indexId, tabletId, replicaId,
                                                             alterSchema, newSchemaHash,
                                                             baseSchemaHash, newShortKeyColumnCount,
                                                             storageType,
                                                             bfColumns, bfFpp, schemaChangeKeysType);
                                addReplicaId(indexId, replicaId, backendId);
                                tasks.add(schemaChangeTask);
                                replicaSendNum++;
                            }
                            
                            if (replicaSendNum < replicationNum / 2 + 1) {
                                // In the case that quorum num of non-NORMAL replica(probably CLONE)
                                // in this tablet, schema change job can not finish.
                                // So cancel it.
                                cancelMsg = String.format("num of normal replica in tablet %d is less than quorum num",
                                                          tabletId);
                                LOG.warn(cancelMsg);
                                return false;
                            }
                        } // end for tablets
                    } // end for alter indices
                } // end for partitions

                AgentBatchTask batchTask = new AgentBatchTask();
                // add all schemaChangeTask to AgentTaskQueue
                for (AgentTask task : tasks) {
                    if (!AgentTaskQueue.addTask(task)) {
                        cancelMsg = "failed add schema change task[" + task.getTabletId()
                                + ":" + task.getBackendId() + "]";
                        LOG.warn(cancelMsg);
                        return false;
                    } else {
                        batchTask.addTask(task);
                    }
                }

                if (batchTask.getTaskNum() > 0) {
                    AgentTaskExecutor.submit(batchTask);
                }
                // change schemaChangeJob's status
                this.state = JobState.RUNNING;
            } // end synchronized block
        } finally {
            olapTable.readUnlock();
        }

        Preconditions.checkState(this.state == JobState.RUNNING);
        LOG.info("successfully sending schema change job[{}]", tableId);
        return true;
    }

    @Override
    public synchronized void cancel(OlapTable olapTable, String msg) {
        // make sure to get table write lock before calling this
        if (olapTable != null) {
            // 1. remove all task and set state
            for (Partition partition : olapTable.getPartitions()) {
                if (partition.getState() == PartitionState.NORMAL) {
                    continue;
                }
                long partitionId = partition.getId();
                for (Long indexId : this.changedIndexIdToSchema.keySet()) {
                    MaterializedIndex index = partition.getIndex(indexId);
                    if (index == null || index.getState() == IndexState.NORMAL) {
                        continue;
                    }
                    for (Tablet tablet : index.getTablets()) {
                        long tabletId = tablet.getId();
                        for (Replica replica : tablet.getReplicas()) {
                            if (replica.getState() == ReplicaState.CLONE
                                    || replica.getState() == ReplicaState.DECOMMISSION
                                    || replica.getState() == ReplicaState.NORMAL) {
                                continue;
                            }
                            Preconditions.checkState(replica.getState() == ReplicaState.SCHEMA_CHANGE);
                            replica.setState(ReplicaState.NORMAL);
                            AgentTaskQueue.removeTask(replica.getBackendId(), TTaskType.SCHEMA_CHANGE, tabletId);
                        } // end for replicas
                    } // end for tablets

                    // delete schema hash in inverted index
                    Catalog.getCurrentInvertedIndex().deleteNewSchemaHash(partitionId, indexId);
                    Preconditions.checkArgument(index.getState() == IndexState.SCHEMA_CHANGE);
                    index.setState(IndexState.NORMAL);
                } // end for indices
                partition.setState(PartitionState.NORMAL);
            } // end for partitions
            olapTable.setState(OlapTableState.NORMAL);
        }

        this.state = JobState.CANCELLED;
        if (Strings.isNullOrEmpty(cancelMsg) && !Strings.isNullOrEmpty(msg)) {
            this.cancelMsg = msg;
        }

        this.finishedTime = System.currentTimeMillis();

        // 2. log
        Catalog.getCurrentCatalog().getEditLog().logCancelSchemaChange(this);
        LOG.info("cancel schema change job[{}] finished, because: {}",
                olapTable == null ? -1 : olapTable.getId(), cancelMsg);
    }

    @Override
    public synchronized void removeReplicaRelatedTask(long parentId, long tabletId, long replicaId, long backendId) {
        // parentId is unused here
        setReplicaFinished(-1, replicaId);
        
        // this.backendIdToReplicaIds.get(backendId).remove(replicaId);
        // remove task
        AgentTaskQueue.removeTask(backendId, TTaskType.SCHEMA_CHANGE, tabletId);
    }

    @Override
    public void handleFinishedReplica(AgentTask task, TTabletInfo finishTabletInfo, long reportVersion)
            throws MetaNotFoundException {
        Preconditions.checkArgument(task instanceof SchemaChangeTask);
        SchemaChangeTask schemaChangeTask = (SchemaChangeTask) task;

        // check schema hash to avoid former schema change task try finishing current task
        int finishTabletInfoSchemaHash = finishTabletInfo.getSchemaHash();
        int taskSchemaHash = schemaChangeTask.getSchemaHash();
        if (finishTabletInfoSchemaHash != taskSchemaHash) {
            throw new MetaNotFoundException("Schema hash is not equal[" + finishTabletInfoSchemaHash + "-"
                    + taskSchemaHash + "], tablet: " + schemaChangeTask.getTabletId());
        }

        long dbId = schemaChangeTask.getDbId();
        long partitionId = schemaChangeTask.getPartitionId();
        long indexId = schemaChangeTask.getIndexId();
        long tabletId = schemaChangeTask.getTabletId();
        long replicaId = schemaChangeTask.getReplicaId();

        // update replica's info
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            throw new MetaNotFoundException("Cannot find db[" + dbId + "]");
        }

        OlapTable olapTable = null;
        try {
            olapTable = (OlapTable) db.getTableOrThrowException(tableId, Table.TableType.OLAP);
        } catch (MetaNotFoundException e) {
            LOG.warn(e.getMessage());
            return;
        }
        olapTable.writeLock();
        try {
            Preconditions.checkState(olapTable.getState() == OlapTableState.SCHEMA_CHANGE);

            Partition partition = olapTable.getPartition(partitionId);
            if (partition == null) {
                throw new MetaNotFoundException("Cannot find partition[" + partitionId + "]");
            }
            Preconditions.checkState(partition.getState() == PartitionState.SCHEMA_CHANGE);

            MaterializedIndex materializedIndex = partition.getIndex(indexId);
            if (materializedIndex == null) {
                throw new MetaNotFoundException("Cannot find index[" + indexId + "]");
            }
            Preconditions.checkState(materializedIndex.getState() == IndexState.SCHEMA_CHANGE);

            Preconditions.checkArgument(finishTabletInfo.getTabletId() == tabletId);
            Tablet tablet = materializedIndex.getTablet(tabletId);
            if (tablet == null) {
                throw new MetaNotFoundException("Cannot find tablet[" + tabletId + "]");
            }

            Replica replica = tablet.getReplicaById(replicaId);
            if (replica == null) {
                throw new MetaNotFoundException("Cannot find replica[" + replicaId + "]");
            }
            // replica's state may be NORMAL(due to clone), so no need to check

            long version = finishTabletInfo.getVersion();
            long versionHash = finishTabletInfo.getVersionHash();
            long dataSize = finishTabletInfo.getDataSize();
            long rowCount = finishTabletInfo.getRowCount();
            // do not need check version > replica.getVersion, because the new replica's version is first set by sc
            replica.updateVersionInfo(version, versionHash, dataSize, rowCount);
            if (finishTabletInfo.isSetPathHash()) {
                replica.setPathHash(finishTabletInfo.getPathHash());
            }
        } finally {
            olapTable.writeUnlock();
        }

        Catalog.getCurrentSystemInfo().updateBackendReportVersion(schemaChangeTask.getBackendId(),
                                                                    reportVersion, dbId);
        setReplicaFinished(indexId, replicaId);

        LOG.info("finish schema change replica[{}]. index[{}]. tablet[{}], backend[{}]",
                 replicaId, indexId, tabletId, task.getBackendId());
    }

    /**
     * should consider following cases:
     * 1. replica is removed from this tablet.
     * 2. backend is dead or is dropped from system
     * 
     * we make new schema visible, but keep table's state as SCHEMA_CHANGE.
     * 1. Make the new schema visible, because we want that the following load jobs will only load
     * data to the new tablet.
     * 2. keep the table's state in SCHEMA_CHANGE, because we don't want another alter job being processed.
     */
    @Override
    public int tryFinishJob() {
        if (this.state != JobState.RUNNING) {
            LOG.info("schema change job[{}] is not running.", tableId);
            return 0;
        }

        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            cancelMsg = String.format("database %d does not exist", dbId);
            LOG.warn(cancelMsg);
            return -1;
        }

        OlapTable olapTable = null;
        try {
            olapTable = (OlapTable) db.getTableOrThrowException(tableId, Table.TableType.OLAP);
        } catch (MetaNotFoundException e) {
            LOG.warn(e.getMessage());
            return -1;
        }

        olapTable.writeLock();
        try {
            synchronized (this) {
                boolean hasUnfinishedPartition = false;
                for (Partition partition : olapTable.getPartitions()) {
                    long partitionId = partition.getId();
                    short expectReplicationNum = olapTable.getPartitionInfo().getReplicationNum(partition.getId());
                    boolean hasUnfinishedIndex = false;
                    for (long indexId : this.changedIndexIdToSchema.keySet()) {
                        MaterializedIndex materializedIndex = partition.getIndex(indexId);
                        if (materializedIndex == null) {
                            // this should not happen, we do not allow dropping rollup during schema change.
                            // cancel the job here.
                            cancelMsg = String.format("index %d is missing", indexId);
                            LOG.warn(cancelMsg);
                            return -1;
                        }

                        for (Tablet tablet : materializedIndex.getTablets()) {
                            List<Replica> replicas = tablet.getReplicas();
                            List<Replica> errorReplicas = Lists.newArrayList();
                            int healthNum = replicas.size();
                            for (Replica replica : replicas) {
                                if (replica.getState() != ReplicaState.SCHEMA_CHANGE) {
                                    // all replicas should be in state SCHEMA_CHANGE
                                    cancelMsg = String.format(
                                            "replica %d of tablet %d in backend %d state is invalid: %s [try finish]",
                                            replica.getId(), tablet.getId(), replica.getBackendId(),
                                            replica.getState().name());
                                    LOG.warn(cancelMsg);
                                    return -1;
                                }

                                if (!checkBackendState(replica)) {
                                    LOG.warn("backend {} state is abnormal, set replica {} of tablet {} as bad",
                                            replica.getBackendId(), tablet.getId(), replica.getId());
                                    errorReplicas.add(replica);
                                    --healthNum;
                                    continue;
                                }

                                if (replica.getLastFailedVersion() > 0 && System.currentTimeMillis()
                                        - replica.getLastFailedTimestamp() > Config.max_backend_down_time_second
                                                * 1000) {
                                    LOG.warn("replica {} of tablet {} last failed version > 0, "
                                            + "and last for an hour, set it as bad", replica, tablet.getId());
                                    --healthNum;
                                    continue;
                                }
                            }

                            if (healthNum < (expectReplicationNum / 2 + 1)) {
                                cancelMsg = String.format("schema change job[%d] cancelled. " 
                                        + "tablet[%d] has few health replica."
                                        + " num: %d", tableId, tablet.getId(), healthNum);
                                LOG.warn(cancelMsg);
                                return -1;
                            }

                            for (Replica errReplica : errorReplicas) {
                                // For now, err replicas are those replicas which the backends they belong to is dead.
                                // We need to set these replicas as finished to let the schema change job
                                // finished.
                                setReplicaFinished(indexId, errReplica.getId());
                                // remove the replica from backend to replica map
                                // backendIdToReplicaIds.get(errReplica.getBackendId()).remove(errReplica.getId());
                                // remove error replica related task
                                AgentTaskQueue.removeTask(errReplica.getBackendId(), TTaskType.SCHEMA_CHANGE,
                                                          tablet.getId());
                            }
                        } // end for tablets

                        // check if index is finished
                        if (!this.partitionIdToFinishedIndexIds.containsKey(partitionId)
                                || !this.partitionIdToFinishedIndexIds.get(partitionId).contains(indexId)) {
                            int finishedReplicaNum = indexIdToFinishedReplicaNum.count(indexId);
                            int totalReplicaNum = indexIdToTotalReplicaNum.get(indexId).size();
                            if (finishedReplicaNum < totalReplicaNum) {
                                LOG.debug("index[{}] has unfinished replica. {}/{}", indexId,
                                          finishedReplicaNum, totalReplicaNum);
                                hasUnfinishedIndex = true;
                                continue;
                            }
                        }

                        // index is finished
                        // remove task for safety
                        // task may be left if some backends are down during schema change
                        if (!this.partitionIdToFinishedIndexIds.containsKey(partitionId)
                                || !this.partitionIdToFinishedIndexIds.get(partitionId).contains(indexId)) {
                            for (Tablet tablet : materializedIndex.getTablets()) {
                                for (Replica replica : tablet.getReplicas()) {
                                    AgentTaskQueue.removeTask(replica.getBackendId(), TTaskType.SCHEMA_CHANGE,
                                                              tablet.getId());
                                }
                            }

                            Preconditions.checkState(materializedIndex.getState() == IndexState.SCHEMA_CHANGE);
                            this.partitionIdToFinishedIndexIds.put(partitionId, indexId);
                            LOG.debug("schema change tasks finished in table[{}]", materializedIndex.getId());
                        }
                    } // end for indices

                    if (hasUnfinishedIndex) {
                        hasUnfinishedPartition = true;
                    }

                    // all table finished in this partition
                    LOG.info("schema change finished in partition {}, table: {}", partition.getId(), olapTable.getId());
                } // end for partitions

                if (hasUnfinishedPartition) {
                    return 0;
                }

                Preconditions.checkState(unfinishedReplicaIds.isEmpty());

                // all partitions are finished
                // update state and save replica info
                Preconditions.checkState(olapTable.getState() == OlapTableState.SCHEMA_CHANGE);
                for (Partition partition : olapTable.getPartitions()) {
                    Preconditions.checkState(partition.getState() == PartitionState.SCHEMA_CHANGE);
                    long partitionId = partition.getId();
                    for (long indexId : this.changedIndexIdToSchema.keySet()) {
                        MaterializedIndex materializedIndex = partition.getIndex(indexId);
                        int schemaHash = changedIndexIdToSchemaHash.get(indexId);
                        Preconditions.checkState(materializedIndex.getState() == IndexState.SCHEMA_CHANGE);
                        for (Tablet tablet : materializedIndex.getTablets()) {
                            long tabletId = tablet.getId();
                            for (Replica replica : tablet.getReplicas()) {
                                if (replica.getState() != ReplicaState.SCHEMA_CHANGE) {
                                    // all replicas should be in state SCHEMA_CHANGE
                                    cancelMsg = String.format(
                                            "replica %d of tablet %d in backend %d state is invalid: %s [finish]",
                                            replica.getId(), tablet.getId(), replica.getBackendId(),
                                            replica.getState().name());
                                    LOG.warn(cancelMsg);
                                }

                                ReplicaPersistInfo replicaInfo = ReplicaPersistInfo.createForSchemaChange(partitionId,
                                        indexId, tabletId,
                                        replica.getBackendId(),
                                        replica.getVersion(),
                                        replica.getVersionHash(),
                                        schemaHash,
                                        replica.getDataSize(),
                                        replica.getRowCount(),
                                        replica.getLastFailedVersion(),
                                        replica.getLastFailedVersionHash(),
                                        replica.getLastSuccessVersion(),
                                        replica.getLastSuccessVersionHash());
                                this.replicaInfos.put(partitionId, replicaInfo);

                                replica.setState(ReplicaState.NORMAL);
                                replica.setSchemaHash(schemaHash);

                                // remove tasks for safety
                                AgentTaskQueue.removeTask(replica.getBackendId(), TTaskType.SCHEMA_CHANGE,
                                        tabletId);
                            } // end for replicas
                        } // end for tablets

                        // update schema hash
                        Catalog.getCurrentInvertedIndex().updateToNewSchemaHash(partitionId, indexId);
                        materializedIndex.setState(IndexState.NORMAL);
                    } // end for indices
                    partition.setState(PartitionState.NORMAL);
                } // end for partitions

                // 2. update to new schema for each index
                for (Map.Entry<Long, List<Column>> entry : changedIndexIdToSchema.entrySet()) {
                    Long indexId = entry.getKey();
                    int schemaVersion = changedIndexIdToSchemaVersion.get(indexId);
                    int schemaHash = changedIndexIdToSchemaHash.get(indexId);
                    short shortKeyColumnCount = changedIndexIdToShortKeyColumnCount.get(indexId);
                    olapTable.setIndexMeta(indexId, null, entry.getValue(), schemaVersion, schemaHash,
                            shortKeyColumnCount, newStorageType, null);
                }

                // 3. update base schema if changed
                if (this.changedIndexIdToSchema.containsKey(olapTable.getBaseIndexId())) {
                    olapTable.setNewFullSchema(this.changedIndexIdToSchema.get(olapTable.getBaseIndexId()));
                }

                // 4. update table bloom filter columns
                if (hasBfChange) {
                    olapTable.setBloomFilterInfo(bfColumns, bfFpp);
                }

                this.state = JobState.FINISHING;
                this.transactionId = Catalog.getCurrentGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId();
            }
        } finally {
            olapTable.writeUnlock();
        }

        Catalog.getCurrentCatalog().getEditLog().logFinishingSchemaChange(this);
        LOG.info("schema change job is finishing. finishing txn id: {} table {}", transactionId, tableId);
        return 1;
    }

    @Override
    public void finishJob() {
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            cancelMsg = String.format("database %d does not exist", dbId);
            LOG.warn(cancelMsg);
            return;
        }

        OlapTable olapTable = null;
        try {
            olapTable = (OlapTable) db.getTableOrThrowException(tableId, Table.TableType.OLAP);
        } catch (MetaNotFoundException e) {
            LOG.warn(e.getMessage());
            return;
        }

        olapTable.writeLock();
        try {
            olapTable.setState(OlapTableState.NORMAL);
        } finally {
            olapTable.writeUnlock();
        }

        this.finishedTime = System.currentTimeMillis();
        LOG.info("finished schema change job: {}", tableId);
    }

    @Override
    public synchronized void clear() {
        changedIndexIdToSchema = null;
        resourceInfo = null;
        replicaInfos = null;
        unfinishedReplicaIds = null;
        indexIdToTotalReplicaNum = null;
        indexIdToFinishedReplicaNum = null;
        partitionIdToFinishedIndexIds = null;
    }

    @Override
    public void replayInitJob(Database db) {
        OlapTable olapTable = (OlapTable) db.getTable(tableId);
        if (olapTable == null) {
            return;
        }
        olapTable.writeLock();
        try {
            // change the state of table/partition and replica, then add object to related List and Set
            for (Partition partition : olapTable.getPartitions()) {
                for (Map.Entry<Long, Integer> entry : changedIndexIdToSchemaHash.entrySet()) {
                    MaterializedIndex index = partition.getIndex(entry.getKey());
                    // set state to SCHEMA_CHANGE
                    for (Tablet tablet : index.getTablets()) {
                        for (Replica replica : tablet.getReplicas()) {
                            if (replica.getState() == ReplicaState.CLONE
                                    || replica.getState() == ReplicaState.DECOMMISSION) {
                                // add log here, because there should no more CLONE replica when processing alter jobs.
                                LOG.warn(String.format(
                                        "replica %d of tablet %d in backend %d state is invalid: %s",
                                        replica.getId(), tablet.getId(), replica.getBackendId(),
                                        replica.getState().name()));
                                continue;
                            }
                            replica.setState(ReplicaState.SCHEMA_CHANGE);
                        }
                    }
                    index.setState(IndexState.SCHEMA_CHANGE);

                    Catalog.getCurrentInvertedIndex().setNewSchemaHash(partition.getId(), entry.getKey(),
                                                                       entry.getValue());
                }

                partition.setState(PartitionState.SCHEMA_CHANGE);
            } // end for partitions

            olapTable.setState(OlapTableState.SCHEMA_CHANGE);

            // reset status to PENDING for resending the tasks in polling thread
            this.state = JobState.PENDING;
        } finally {
            olapTable.writeUnlock();
        }
    }

    @Override
    public void replayFinishing(Database db) {
        OlapTable olapTable = (OlapTable) db.getTable(tableId);
        if (olapTable == null) {
            return;
        }
        olapTable.writeLock();
        try {
            // set the status to normal
            for (Partition partition : olapTable.getPartitions()) {
                long partitionId = partition.getId();
                for (Map.Entry<Long, Integer> entry : changedIndexIdToSchemaHash.entrySet()) {
                    MaterializedIndex index = partition.getIndex(entry.getKey());
                    for (Tablet tablet : index.getTablets()) {
                        for (Replica replica : tablet.getReplicas()) {
                            replica.setState(ReplicaState.NORMAL);
                        }
                    }

                    index.setState(IndexState.NORMAL);

                    // update to new schema hash in inverted index
                    Catalog.getCurrentInvertedIndex().updateToNewSchemaHash(partitionId, index.getId());
                    Catalog.getCurrentInvertedIndex().deleteNewSchemaHash(partitionId, index.getId());
                }
                partition.setState(PartitionState.NORMAL);

                // update replica info
                Collection<ReplicaPersistInfo> replicaInfo = replicaInfos.get(partition.getId());
                if (replicaInfo != null) {
                    for (ReplicaPersistInfo info : replicaInfo) {
                        MaterializedIndex mIndex = (MaterializedIndex) partition.getIndex(info.getIndexId());
                        int schemaHash = info.getSchemaHash();
                        // for compatibility
                        if (schemaHash == -1) {
                            schemaHash = getSchemaHashByIndexId(info.getIndexId());
                        }
                        Tablet tablet = mIndex.getTablet(info.getTabletId());
                        if (info.getOpType() == ReplicaOperationType.SCHEMA_CHANGE) {
                            Replica replica = tablet.getReplicaByBackendId(info.getBackendId());
                            replica.updateVersionInfo(info.getVersion(), info.getVersionHash(),
                                                      info.getLastFailedVersion(),
                                                      info.getLastFailedVersionHash(),
                                                      info.getLastSuccessVersion(),
                                                      info.getLastSuccessVersionHash());
                            replica.setSchemaHash(schemaHash);
                        } else if (info.getOpType() == ReplicaOperationType.DELETE) {
                            // remove the replica from replica group
                            tablet.deleteReplicaByBackendId(info.getBackendId());
                        }
                    }
                }
            } // end for partitions

            // update schema
            for (Map.Entry<Long, List<Column>> entry : changedIndexIdToSchema.entrySet()) {
                long indexId = entry.getKey();
                int schemaVersion = getSchemaVersionByIndexId(indexId);
                int schemaHash = getSchemaHashByIndexId(indexId);
                short shortKeyColumnCount = getShortKeyColumnCountByIndexId(indexId);
                olapTable.setIndexMeta(indexId, null, entry.getValue(), schemaVersion, schemaHash,
                        shortKeyColumnCount, newStorageType, null);

                if (indexId == olapTable.getBaseIndexId()) {
                    olapTable.setNewFullSchema(entry.getValue());
                }
            }

            // bloom filter columns
            if (hasBfChange) {
                olapTable.setBloomFilterInfo(bfColumns, bfFpp);
            } // end for partitions
        } finally {
            olapTable.writeUnlock();
        }

        LOG.info("replay finishing schema change job: {}", tableId);
    }
    
    @Override
    public void replayFinish(Database db) {
        // if this is an old job, then should also update table or replica state
        if (transactionId < 0) {
            replayFinishing(db);
        }

        OlapTable olapTable = (OlapTable) db.getTable(tableId);
        if (olapTable != null) {
            olapTable.writeLock();
            try {
                olapTable.setState(OlapTableState.NORMAL);
            } finally {
                olapTable.writeUnlock();
            }
        }
        LOG.info("replay finish schema change job: {}", tableId);
    }

    @Override
    public void replayCancel(Database db) {
        // restore partition's state
        OlapTable olapTable = (OlapTable) db.getTable(tableId);
        if (olapTable == null) {
            return;
        }
        olapTable.writeLock();
        try {
            for (Partition partition : olapTable.getPartitions()) {
                long partitionId = partition.getId();
                for (Long indexId : this.changedIndexIdToSchema.keySet()) {
                    MaterializedIndex index = partition.getIndex(indexId);
                    if (index == null) {
                        continue;
                    }
                    for (Tablet tablet : index.getTablets()) {
                        for (Replica replica : tablet.getReplicas()) {
                            if (replica.getState() == ReplicaState.CLONE
                                    || replica.getState() == ReplicaState.DECOMMISSION
                                    || replica.getState() == ReplicaState.NORMAL) {
                                continue;
                            }
                            replica.setState(ReplicaState.NORMAL);
                        } // end for replicas
                    } // end for tablets

                    Preconditions.checkState(index.getState() == IndexState.SCHEMA_CHANGE, index.getState());
                    index.setState(IndexState.NORMAL);

                    // delete new schema hash in invered index
                    Catalog.getCurrentInvertedIndex().deleteNewSchemaHash(partitionId, indexId);
                } // end for indices

                Preconditions.checkState(partition.getState() == PartitionState.SCHEMA_CHANGE,
                        partition.getState());
                partition.setState(PartitionState.NORMAL);
            } // end for partitions

            olapTable.setState(OlapTableState.NORMAL);
        } finally {
            olapTable.writeUnlock();
        }
    }

    @Override
    public void getJobInfo(List<List<Comparable>> jobInfos, OlapTable tbl) {
        if (changedIndexIdToSchemaVersion == null) {
            // for compatibility
            if (state == JobState.FINISHED || state == JobState.CANCELLED) {
                List<Comparable> jobInfo = new ArrayList<Comparable>();
                jobInfo.add(tableId); // job id
                jobInfo.add(tbl.getName()); // table name
                jobInfo.add(TimeUtils.longToTimeString(createTime));
                jobInfo.add(TimeUtils.longToTimeString(finishedTime));
                jobInfo.add(FeConstants.null_string); // index name
                jobInfo.add(FeConstants.null_string); // index id
                jobInfo.add(FeConstants.null_string); // origin id
                jobInfo.add(FeConstants.null_string); // schema version
                jobInfo.add(-1); // transaction id
                jobInfo.add(state.name()); // job state
                jobInfo.add(cancelMsg);
                jobInfo.add(FeConstants.null_string); // progress
                jobInfo.add(Config.alter_table_timeout_second); // timeout
                jobInfos.add(jobInfo);
                return;
            }

            // in previous version, changedIndexIdToSchema is set to null
            // when job is finished or cancelled.
            // so if changedIndexIdToSchema == null, the job'state must be FINISHED or CANCELLED
            return;
        }

        Map<Long, String> indexProgress = new HashMap<Long, String>();
        Map<Long, String> indexState = new HashMap<Long, String>();

        // calc progress and state for each table
        for (Long indexId : changedIndexIdToSchemaVersion.keySet()) {
            if (tbl.getIndexNameById(indexId) == null) {
                // this index may be dropped, and this should be a FINISHED job, just use a dummy info to show
                indexState.put(indexId, IndexState.NORMAL.name());
                indexProgress.put(indexId, "100%");
            } else {
                int totalReplicaNum = 0;
                int finishedReplicaNum = 0;
                String idxState = IndexState.NORMAL.name();
                for (Partition partition : tbl.getPartitions()) {
                    MaterializedIndex index = partition.getIndex(indexId);
                    if (state == JobState.RUNNING) {
                        int tableReplicaNum = getTotalReplicaNumByIndexId(indexId);
                        int tableFinishedReplicaNum = getFinishedReplicaNumByIndexId(indexId);
                        Preconditions.checkState(!(tableReplicaNum == 0 && tableFinishedReplicaNum == -1));
                        Preconditions.checkState(tableFinishedReplicaNum <= tableReplicaNum,
                            tableFinishedReplicaNum + "/" + tableReplicaNum);
                        totalReplicaNum += tableReplicaNum;
                        finishedReplicaNum += tableFinishedReplicaNum;
                    }

                    if (index.getState() != IndexState.NORMAL) {
                        idxState = index.getState().name();
                    }
                }

                indexState.put(indexId, idxState);

                if (Catalog.getCurrentCatalog().isMaster() && state == JobState.RUNNING && totalReplicaNum != 0) {
                    indexProgress.put(indexId, (finishedReplicaNum * 100 / totalReplicaNum) + "%");
                } else {
                    indexProgress.put(indexId, "0%");
                }
            }
        }

        for (Long indexId : changedIndexIdToSchemaVersion.keySet()) {
            List<Comparable> jobInfo = new ArrayList<Comparable>();

            jobInfo.add(tableId);
            jobInfo.add(tbl.getName());
            jobInfo.add(TimeUtils.longToTimeString(createTime));
            jobInfo.add(TimeUtils.longToTimeString(finishedTime));
            jobInfo.add(tbl.getIndexNameById(indexId) == null ? FeConstants.null_string : tbl.getIndexNameById(indexId)); // index name
            jobInfo.add(indexId);
            jobInfo.add(indexId); // origin index id
            // index schema version and schema hash
            jobInfo.add(changedIndexIdToSchemaVersion.get(indexId) + ":" + changedIndexIdToSchemaHash.get(indexId));
            jobInfo.add(transactionId);
            jobInfo.add(state.name()); // job state
            jobInfo.add(cancelMsg);
            if (state == JobState.RUNNING) {
                jobInfo.add(indexProgress.get(indexId) == null ? FeConstants.null_string : indexProgress.get(indexId)); // progress
            } else {
                jobInfo.add(FeConstants.null_string);
            }
            jobInfo.add(Config.alter_table_timeout_second);
            jobInfos.add(jobInfo);
        } // end for indexIds
    }

    @Override
    public synchronized void write(DataOutput out) throws IOException {
        super.write(out);

        Text.writeString(out, tableName);

        // 'unfinishedReplicaIds', 'indexIdToTotalReplicaNum' and 'indexIdToFinishedReplicaNum'
        // don't need persist. build it when send tasks

        // columns
        if (changedIndexIdToSchema != null) {
            out.writeBoolean(true);
            out.writeInt(changedIndexIdToSchema.size());
            for (Entry<Long, List<Column>> entry : changedIndexIdToSchema.entrySet()) {
                out.writeLong(entry.getKey());
                out.writeInt(entry.getValue().size());
                for (Column column : entry.getValue()) {
                    column.write(out);
                }
            }
        } else {
            out.writeBoolean(false);
        }

        // schema version and hash, and short key
        if (changedIndexIdToSchemaVersion != null) {
            out.writeBoolean(true);
            out.writeInt(changedIndexIdToSchemaVersion.size());
            for (Entry<Long, Integer> entry : changedIndexIdToSchemaVersion.entrySet()) {
                out.writeLong(entry.getKey());
                // schema version
                out.writeInt(entry.getValue());
                // schema hash
                out.writeInt(changedIndexIdToSchemaHash.get(entry.getKey()));
                // short key column count
                out.writeShort(changedIndexIdToShortKeyColumnCount.get(entry.getKey()));
            }
        }

        // replicaInfos is saving for restoring schemaChangeJobFinished
        if (replicaInfos != null) {
            out.writeBoolean(true);
            out.writeInt(replicaInfos.keySet().size());
            for (long partitionId : replicaInfos.keySet()) {
                Collection<ReplicaPersistInfo> infos = replicaInfos.get(partitionId);
                out.writeLong(partitionId);
                out.writeInt(infos.size());
                for (ReplicaPersistInfo replicaPersistInfo : infos) {
                    replicaPersistInfo.write(out);
                }
            }
        } else {
            out.writeBoolean(false);
        }

        // bloom filter columns
        out.writeBoolean(hasBfChange);
        if (bfColumns != null) {
            out.writeBoolean(true);
            out.writeInt(bfColumns.size());
            for (String bfColumn : bfColumns) {
                Text.writeString(out, bfColumn);
            }
            out.writeDouble(bfFpp);
        } else {
            out.writeBoolean(false);
        }

        // storage type
        if (newStorageType == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        tableName = Text.readString(in);

        if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_48) {
            if (in.readBoolean()) {
                int count = in.readInt();
                for (int i = 0; i < count; i++) {
                    long indexId = in.readLong();
                    int columnNum = in.readInt();
                    List<Column> columns = new LinkedList<Column>();
                    for (int j = 0; j < columnNum; j++) {
                        Column column = Column.read(in);
                        columns.add(column);
                    }
                    changedIndexIdToSchema.put(indexId, columns);
                    // schema version
                    changedIndexIdToSchemaVersion.put(indexId, in.readInt());
                    // schema hash
                    changedIndexIdToSchemaHash.put(indexId, in.readInt());
                    // short key column count
                    changedIndexIdToShortKeyColumnCount.put(indexId, in.readShort());
                }
            }
        } else {
            // columns
            if (in.readBoolean()) {
                int count = in.readInt();
                for (int i = 0; i < count; i++) {
                    long indexId = in.readLong();
                    int columnNum = in.readInt();
                    List<Column> columns = new LinkedList<Column>();
                    for (int j = 0; j < columnNum; j++) {
                        Column column = Column.read(in);
                        columns.add(column);
                    }
                    changedIndexIdToSchema.put(indexId, columns);
                }
            }

            // schema version and hash, and short key
            if (in.readBoolean()) {
                int count = in.readInt();
                for (int i = 0; i < count; i++) {
                    long indexId = in.readLong();
                    // schema version
                    changedIndexIdToSchemaVersion.put(indexId, in.readInt());
                    // schema hash
                    changedIndexIdToSchemaHash.put(indexId, in.readInt());
                    // short key column count
                    changedIndexIdToShortKeyColumnCount.put(indexId, in.readShort());
                }
            }
        }

        if (in.readBoolean()) {
            int count = in.readInt();
            for (int i = 0; i < count; ++i) {
                long partitionId = in.readLong();
                int infoSize = in.readInt();
                for (int j = 0; j < infoSize; j++) {
                    ReplicaPersistInfo info = ReplicaPersistInfo.read(in);
                    replicaInfos.put(partitionId, info);
                }
            }
        }

        // bloom filter columns
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_9) {
            hasBfChange = in.readBoolean();
            if (in.readBoolean()) {
                int bfColumnCount = in.readInt();
                bfColumns = Sets.newHashSet();
                for (int i = 0; i < bfColumnCount; i++) {
                    bfColumns.add(Text.readString(in));
                }

                bfFpp = in.readDouble();
            }
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_39) {
            if (in.readBoolean()) {
                newStorageType = TStorageType.COLUMN;
            }
        }
    }

    public static SchemaChangeJob read(DataInput in) throws IOException {
        SchemaChangeJob schemaChangeJob = new SchemaChangeJob();
        schemaChangeJob.readFields(in);
        return schemaChangeJob;
    }

    public boolean equals(Object obj) {
        return true;
    }

    @Override
    public String toString() {
        return "SchemaChangeJob [tableName=" + tableName + ", type=" + type + ", state=" + state + ", dbId=" + dbId
                + ", tableId=" + tableId + ", transactionId=" + transactionId + ", isPreviousLoadFinished="
                + isPreviousLoadFinished + ", createTime=" + createTime + ", finishedTime=" + finishedTime + "]";
    }
}
