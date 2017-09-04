// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.alter;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.KeysType;
import com.baidu.palo.catalog.MaterializedIndex;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.catalog.Partition;
import com.baidu.palo.catalog.Replica;
import com.baidu.palo.catalog.Table;
import com.baidu.palo.catalog.Tablet;
import com.baidu.palo.catalog.MaterializedIndex.IndexState;
import com.baidu.palo.catalog.OlapTable.OlapTableState;
import com.baidu.palo.catalog.Partition.PartitionState;
import com.baidu.palo.catalog.Replica.ReplicaState;
import com.baidu.palo.common.Config;
import com.baidu.palo.common.FeMetaVersion;
import com.baidu.palo.common.MetaNotFoundException;
import com.baidu.palo.common.io.Text;
import com.baidu.palo.load.Load;
import com.baidu.palo.persist.ReplicaPersistInfo;
import com.baidu.palo.task.AgentTask;
import com.baidu.palo.task.AgentTaskQueue;
import com.baidu.palo.task.SchemaChangeTask;
import com.baidu.palo.thrift.TKeysType;
import com.baidu.palo.thrift.TResourceInfo;
import com.baidu.palo.thrift.TStorageType;
import com.baidu.palo.thrift.TTabletInfo;
import com.baidu.palo.thrift.TTaskType;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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

    private SchemaChangeJob() {
        this(-1, -1, null, null);
    }

    public SchemaChangeJob(long dbId, long tableId, TResourceInfo resourceInfo, String tableName) {
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

    public boolean isSchemaHashRelated(int schemaHash) {
        return changedIndexIdToSchemaHash.values().contains(schemaHash);
    }

    public synchronized int getFinishedReplicaNumByIndexId(long indexId) {
        return this.indexIdToFinishedReplicaNum.count(indexId);
    }

    private synchronized boolean isDelayDeleting() {
        if (System.currentTimeMillis() - this.finishedTime < Config.alter_delete_base_delay_second * 1000L) {
            LOG.info("delay deleting old schema for querying. table[{}-{}]", dbId, tableId);
            return true;
        }
        return false;
    }

    public boolean tryDeleteAllTableHistorySchema() {
        if (isDelayDeleting()) {
            return false;
        }

        Database db = Catalog.getInstance().getDb(dbId);
        if (db == null) {
            LOG.warn("db[{}] does not exist", dbId);
            return true;
        }

        if (!db.tryReadLock(Database.TRY_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            return false;
        }
        try {
            OlapTable olapTable = (OlapTable) db.getTable(tableId);
            if (olapTable == null) {
                LOG.warn("table[{}] does not exist in db[{}]", tableId, dbId);
                return true;
            }
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

                    // delele schema hash
                    // the real drop task is handled by report process
                    // we call 'deleteNewSchemaHash' but we delete old one actually.
                    // cause schama hash is switched when job is finished.
                    Catalog.getCurrentInvertedIndex().deleteNewSchemaHash(partitionId, indexId);
                    LOG.info("delete old schema. db[{}], table[{}], partition[{}], index[{}]",
                             dbId, tableId, partitionId, indexId);
                }
            } // end for partitions
        } finally {
            db.readUnlock();
        }
        return true;
    }

    @Override
    public void addReplicaId(long parentId, long replicaId, long backendId) {
        // here parent id is index id
        this.unfinishedReplicaIds.add(replicaId);
        this.indexIdToTotalReplicaNum.put(parentId, replicaId);
        this.backendIdToReplicaIds.put(backendId, replicaId);
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

    @Override
    public boolean sendTasks() {
        Preconditions.checkState(this.state == JobState.PENDING);
        // here we just sending tasks to AgentTaskQueue.
        // task report process will later resend this task

        LOG.info("sending schema change job[{}]", tableId);

        Database db = Catalog.getInstance().getDb(dbId);
        if (db == null) {
            String msg = "db[" + dbId + "] does not exist";
            setMsg(msg);
            LOG.warn(msg);
            return false;
        }

        if (!db.tryReadLock(Database.TRY_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            return true;
        }
        try {
            synchronized (this) {
                OlapTable olapTable = (OlapTable) db.getTable(tableId);
                if (olapTable == null) {
                    cancelMsg = "table[" + tableId + "] does not exist";
                    LOG.warn(cancelMsg);
                    return false;
                }

                Preconditions.checkNotNull(this.unfinishedReplicaIds);

                List<AgentTask> tasks = new LinkedList<AgentTask>();
                for (Partition partition : olapTable.getPartitions()) {
                    long partitionId = partition.getId();
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
                        TStorageType storageType = olapTable.getStorageTypeByIndexId(indexId);
                        KeysType keysType = olapTable.getKeysType();
                        TKeysType schemaChangeKeysType;
                        if ("DUP_KEYS" == keysType.name()) {
                            schemaChangeKeysType = TKeysType.DUP_KEYS;
                        } else if ("UNIQUE_KEYS" == keysType.name()) {
                            schemaChangeKeysType = TKeysType.UNIQUE_KEYS;
                        } else {
                            schemaChangeKeysType = TKeysType.AGG_KEYS;
                        }

                        for (Tablet tablet : alterIndex.getTablets()) {
                            long tabletId = tablet.getId();
                            for (Replica replica : tablet.getReplicas()) {
                                long backendId = replica.getBackendId();
                                long replicaId = replica.getId();
                                SchemaChangeTask schemaChangeTask =
                                        new SchemaChangeTask(resourceInfo, backendId, dbId, tableId,
                                                             partitionId, indexId, tabletId, replicaId,
                                                             alterSchema, newSchemaHash,
                                                             baseSchemaHash, newShortKeyColumnCount,
                                                             storageType, bfColumns, bfFpp, schemaChangeKeysType);
                                addReplicaId(indexId, replicaId, backendId);
                                tasks.add(schemaChangeTask);
                            }
                        } // end for tablets
                    } // end for alter indices
                } // end for partitions

                // add all schemaChangeTask to AgentTaskQueue
                for (AgentTask task : tasks) {
                    if (!AgentTaskQueue.addTask(task)) {
                        cancelMsg = "failed add schema change task[" + task.getTabletId()
                                + ":" + task.getBackendId() + "]";
                        LOG.warn(cancelMsg);
                        return false;
                    }
                }

                // change schemaChangeJob's status
                this.state = JobState.RUNNING;
            } // end synchronized block
        } finally {
            db.readUnlock();
        }

        Preconditions.checkState(this.state == JobState.RUNNING);
        LOG.info("successfully sending schema change job[{}]", tableId);
        return true;
    }

    @Override
    public synchronized void cancel(OlapTable olapTable, String msg) {
        // make sure to get db write lock before calling this

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
                            if (replica.getState() == ReplicaState.CLONE || replica.getState() == ReplicaState.NORMAL) {
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
        if (msg != null) {
            this.cancelMsg = msg;
        }

        this.finishedTime = System.currentTimeMillis();

        // 2. log
        Catalog.getInstance().getEditLog().logCancelSchemaChange(this);
        LOG.info("cancel schema change job[" + olapTable.getId() + "] finished");
    }

    @Override
    public synchronized void removeReplicaRelatedTask(long parentId, long tabletId, long replicaId, long backendId) {
        // parentId is unused here
        directRemoveReplicaTask(replicaId, backendId);

        // remove task
        AgentTaskQueue.removeTask(backendId, TTaskType.SCHEMA_CHANGE, tabletId);
    }

    @Override
    public synchronized void directRemoveReplicaTask(long replicaId, long backendId) {
        setReplicaFinished(-1L, replicaId);
        this.backendIdToReplicaIds.get(backendId).remove(replicaId);
    }

    @Override
    public void handleFinishedReplica(AgentTask task, TTabletInfo finishTabletInfo, long reportVersion)
            throws MetaNotFoundException {
        Preconditions.checkArgument(task instanceof SchemaChangeTask);
        SchemaChangeTask schemaChangeTask = (SchemaChangeTask) task;

        // check schema hash to avoid former schema change task try finishing current task
        int finishTabletInfoSchemaHash = finishTabletInfo.getSchema_hash();
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
        Database db = Catalog.getInstance().getDb(dbId);
        if (db == null) {
            throw new MetaNotFoundException("Cannot find db[" + dbId + "]");
        }
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(tableId);
            if (olapTable == null) {
                throw new MetaNotFoundException("Cannot find table[" + tableId + "]");
            }
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

            Preconditions.checkArgument(finishTabletInfo.getTablet_id() == tabletId);
            Tablet tablet = materializedIndex.getTablet(tabletId);
            if (tablet == null) {
                throw new MetaNotFoundException("Cannot find tablet[" + tabletId + "]");
            }

            Replica replica = tablet.getReplicaById(replicaId);
            if (replica == null) {
                throw new MetaNotFoundException("Cannot find replica[" + replicaId + "]");
            }
            Preconditions.checkState(replica.getState() == ReplicaState.SCHEMA_CHANGE);

            long version = finishTabletInfo.getVersion();
            long versionHash = finishTabletInfo.getVersion_hash();
            long dataSize = finishTabletInfo.getData_size();
            long rowCount = finishTabletInfo.getRow_count();
            replica.updateInfo(version, versionHash, dataSize, rowCount);
        } finally {
            db.writeUnlock();
        }

        Catalog.getCurrentSystemInfo().updateBackendReportVersion(schemaChangeTask.getBackendId(),
                                                                    reportVersion, dbId);
        setReplicaFinished(indexId, replicaId);

        LOG.info("finish schema change replica[{}]. index[{}]. tablet[{}], backend[{}]",
                 replicaId, indexId, tabletId, task.getBackendId());
    }

    @Override
    public int tryFinishJob() {
        if (this.state != JobState.RUNNING) {
            LOG.info("schema change job[{}] is not running.", tableId);
            return 0;
        }

        Database db = Catalog.getInstance().getDb(dbId);
        if (db == null) {
            LOG.warn("db[{}] does not exist", dbId);
            return -1;
        }

        if (!db.tryWriteLock(Database.TRY_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            return 0;
        }
        try {
            synchronized (this) {
                Table table = db.getTable(tableId);
                if (table == null) {
                    LOG.warn("table[{}] does not exist", tableId);
                    return -1;
                }

                Load load = Catalog.getInstance().getLoadInstance();
                OlapTable olapTable = (OlapTable) table;
                for (Partition partition : olapTable.getPartitions()) {
                    long partitionId = partition.getId();

                    // check if all tablets finished load job
                    // FIXME(cmy): this may cause endless check??
                    if (!load.checkPartitionLoadFinished(partitionId, null)) {
                        LOG.debug("partition[{}] has unfinished load job", partitionId);
                        return 0;
                    }

                    short replicationNum = olapTable.getPartitionInfo().getReplicationNum(partition.getId());
                    long committedVersion = partition.getCommittedVersion();
                    long committedVersionHash = partition.getCommittedVersionHash();

                    for (long indexId : this.changedIndexIdToSchema.keySet()) {
                        MaterializedIndex materializedIndex = partition.getIndex(indexId);
                        if (materializedIndex == null) {
                            LOG.warn("index[{}] does not exist inp partition[{}]", indexId, partitionId);
                            continue;
                        }

                        int aliveReplica = 0;
                        for (Tablet tablet : materializedIndex.getTablets()) {
                            Iterator<Replica> iterator = tablet.getReplicas().iterator();
                            while (iterator.hasNext()) {
                                Replica replica = iterator.next();
                                if (!this.backendIdToReplicaIds.get(replica.getBackendId()).contains(replica.getId())) {
                                    // replica is dead, skip  it
                                    LOG.warn("schema change job[{}] find dead replica[{}]. skip it",
                                             tableId, replica.getId());
                                    continue;
                                }

                                // if replica is still in backendIdToReplicaIds
                                // we think this replica is still alive.
                                // BackendEvent will handle dead replica
                                ++aliveReplica;

                                if (!checkBackendState(replica)) {
                                    continue;
                                }

                                // check version and versionHash
                                if (!replica.checkVersionCatchUp(committedVersion, committedVersionHash)) {
                                    continue;
                                }
                            }

                            if (aliveReplica < (replicationNum / 2 + 1)) {
                                cancelMsg = String.format("schema change job[%d] cancelled."
                                        + " tablet[%d] has few replica. num: %d.",
                                                          tableId, tablet.getId(), aliveReplica);
                                LOG.warn(cancelMsg);
                                return -1;
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
                                return 0;
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

                    // all table finished in this partition
                    LOG.debug("schema change finished in partition[" + partition.getId() + "]");

                } // end for partitions

                Preconditions.checkState(unfinishedReplicaIds.isEmpty());

                // all partitions are finished
                // update state and save replica info
                Preconditions.checkState(olapTable.getState() == OlapTableState.SCHEMA_CHANGE);
                for (Partition partition : olapTable.getPartitions()) {
                    Preconditions.checkState(partition.getState() == PartitionState.SCHEMA_CHANGE);
                    long partitionId = partition.getId();
                    for (long indexId : this.changedIndexIdToSchema.keySet()) {
                        MaterializedIndex materializedIndex = partition.getIndex(indexId);
                        Preconditions.checkState(materializedIndex.getState() == IndexState.SCHEMA_CHANGE);
                        for (Tablet tablet : materializedIndex.getTablets()) {
                            long tabletId = tablet.getId();
                            for (Replica replica : tablet.getReplicas()) {
                                if (replica.getState() == ReplicaState.SCHEMA_CHANGE) {
                                    replica.setState(ReplicaState.NORMAL);
                                    ReplicaPersistInfo replicaInfo =
                                            ReplicaPersistInfo.createForSchemaChange(partitionId, indexId, tabletId,
                                                                                     replica.getBackendId(),
                                                                                     replica.getVersion(),
                                                                                     replica.getVersionHash(),
                                                                                     replica.getDataSize(),
                                                                                     replica.getRowCount());
                                    this.replicaInfos.put(partitionId, replicaInfo);

                                    // remove tasks for safety
                                    AgentTaskQueue.removeTask(replica.getBackendId(), TTaskType.SCHEMA_CHANGE,
                                                              tabletId);
                                }
                            } // end for replicas
                        } // end for tablets

                        // update schema hash
                        Catalog.getCurrentInvertedIndex().updateToNewSchemaHash(partitionId, indexId);
                        materializedIndex.setState(IndexState.NORMAL);
                    } // end for indices
                    partition.setState(PartitionState.NORMAL);
                } // end for partitions
                olapTable.setState(OlapTableState.NORMAL);

                // 2. update to new schema for each index
                for (Map.Entry<Long, List<Column>> entry : changedIndexIdToSchema.entrySet()) {
                    Long indexId = entry.getKey();
                    int schemaVersion = changedIndexIdToSchemaVersion.get(indexId);
                    int schemaHash = changedIndexIdToSchemaHash.get(indexId);
                    short shortKeyColumnCount = changedIndexIdToShortKeyColumnCount.get(indexId);
                    olapTable.setIndexSchemaInfo(indexId, null, entry.getValue(), schemaVersion, schemaHash,
                                                 shortKeyColumnCount);
                }

                // 3. update base schema if changed
                if (this.changedIndexIdToSchema.containsKey(tableId)) {
                    table.setNewBaseSchema(this.changedIndexIdToSchema.get(tableId));
                }

                // 4. update table bloom filter columns
                if (hasBfChange) {
                    olapTable.setBloomFilterInfo(bfColumns, bfFpp);
                }

                this.finishedTime = System.currentTimeMillis();
                this.state = JobState.FINISHED;
            }
        } finally {
            db.writeUnlock();
        }

        // log schema change done operation
        Catalog.getInstance().getEditLog().logFinishSchemaChange(this);
        LOG.info("schema change job done. table [{}]", tableId);
        return 1;
    }

    @Override
    public synchronized void clear() {
        changedIndexIdToSchema = null;
        changedIndexIdToSchemaVersion = null;
        changedIndexIdToSchemaHash = null;
        changedIndexIdToShortKeyColumnCount = null;
        resourceInfo = null;
        replicaInfos = null;
        unfinishedReplicaIds = null;
        indexIdToTotalReplicaNum = null;
        indexIdToFinishedReplicaNum = null;
        backendIdToReplicaIds = null;
        partitionIdToFinishedIndexIds = null;
    }

    @Override
    public void unprotectedReplayInitJob(Database db) {
        OlapTable olapTable = (OlapTable) db.getTable(tableId);

        // change the state of table/partition and replica, then add object to related List and Set
        for (Partition partition : olapTable.getPartitions()) {
            for (Map.Entry<Long, Integer> entry : changedIndexIdToSchemaHash.entrySet()) {
                MaterializedIndex index = partition.getIndex(entry.getKey());
                // set state to SCHEMA_CHANGE
                for (Tablet tablet : index.getTablets()) {
                    for (Replica replica : tablet.getReplicas()) {
                        if (replica.getState() == ReplicaState.CLONE) {
                            // just skip it (old schema clone will be deleted)
                            continue;
                        }
                        replica.setState(ReplicaState.SCHEMA_CHANGE);
                    }
                }
                index.setState(IndexState.SCHEMA_CHANGE);

                Catalog.getCurrentInvertedIndex().setNewSchemaHash(partition.getId(), entry.getKey(), entry.getValue());
            }

            partition.setState(PartitionState.SCHEMA_CHANGE);
        } // end for partitions

        olapTable.setState(OlapTableState.SCHEMA_CHANGE);

        // reset status to PENDING for resending the tasks in polling thread
        this.state = JobState.PENDING;
    }

    @Override
    public void unprotectedReplayFinish(Database db) {
        OlapTable olapTable = (OlapTable) db.getTable(tableId);

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
                    Tablet tablet = mIndex.getTablet(info.getTabletId());
                    Replica replica = tablet.getReplicaByBackendId(info.getBackendId());
                    replica.updateInfo(info.getVersion(), info.getVersionHash(),
                                       info.getDataSize(), info.getRowCount());
                }
            }
        } // end for partitions

        // update schema
        for (Map.Entry<Long, List<Column>> entry : changedIndexIdToSchema.entrySet()) {
            long indexId = entry.getKey();
            int schemaVersion = getSchemaVersionByIndexId(indexId);
            int schemaHash = getSchemaHashByIndexId(indexId);
            short shortKeyColumnCount = getShortKeyColumnCountByIndexId(indexId);
            olapTable.setIndexSchemaInfo(indexId, null, entry.getValue(), schemaVersion, schemaHash,
                                         shortKeyColumnCount);

            if (indexId == olapTable.getId()) {
                olapTable.setNewBaseSchema(entry.getValue());
            }
        }

        // bloom filter columns
        if (hasBfChange) {
            olapTable.setBloomFilterInfo(bfColumns, bfFpp);
        }

        olapTable.setState(OlapTableState.NORMAL);
    }

    @Override
    public void unprotectedReplayCancel(Database db) {
        // restore partition's state
        OlapTable olapTable = (OlapTable) db.getTable(tableId);
        if (olapTable == null) {
            return;
        }
        for (Partition partition : olapTable.getPartitions()) {
            long partitionId = partition.getId();
            for (Long indexId : this.changedIndexIdToSchema.keySet()) {
                MaterializedIndex index = partition.getIndex(indexId);
                if (index == null) {
                    continue;
                }
                for (Tablet tablet : index.getTablets()) {
                    for (Replica replica : tablet.getReplicas()) {
                        if (replica.getState() == ReplicaState.CLONE || replica.getState() == ReplicaState.NORMAL) {
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
    }

    @Override
    public synchronized void write(DataOutput out) throws IOException {
        super.write(out);

        Text.writeString(out, tableName);

        // 'unfinishedReplicaIds', 'indexIdToTotalReplicaNum' and 'indexIdToFinishedReplicaNum'
        // don't need persist. build it when send tasks

        if (changedIndexIdToSchema != null) {
            out.writeBoolean(true);
            out.writeInt(changedIndexIdToSchema.size());
            for (Entry<Long, List<Column>> entry : changedIndexIdToSchema.entrySet()) {
                long indexId = entry.getKey();
                out.writeLong(indexId);
                out.writeInt(entry.getValue().size());
                for (Column column : entry.getValue()) {
                    column.write(out);
                }

                // schema version
                out.writeInt(changedIndexIdToSchemaVersion.get(indexId));

                // schema hash
                out.writeInt(changedIndexIdToSchemaHash.get(indexId));

                // short key column count
                out.writeShort(changedIndexIdToShortKeyColumnCount.get(indexId));
            }
        } else {
            out.writeBoolean(false);
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
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        tableName = Text.readString(in);

        boolean has = in.readBoolean();
        if (has) {
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

        has = in.readBoolean();
        if (has) {
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
    }

    public static SchemaChangeJob read(DataInput in) throws IOException {
        SchemaChangeJob schemaChangeJob = new SchemaChangeJob();
        schemaChangeJob.readFields(in);
        return schemaChangeJob;
    }

    public boolean equals(Object obj) {
        return true;
    }
}
