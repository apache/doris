// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

package com.baidu.palo.alter;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.MaterializedIndex;
import com.baidu.palo.catalog.MaterializedIndex.IndexState;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.catalog.OlapTable.OlapTableState;
import com.baidu.palo.catalog.Partition;
import com.baidu.palo.catalog.Partition.PartitionState;
import com.baidu.palo.catalog.Replica;
import com.baidu.palo.catalog.Replica.ReplicaState;
import com.baidu.palo.catalog.Tablet;
import com.baidu.palo.catalog.TabletInvertedIndex;
import com.baidu.palo.catalog.TabletMeta;
import com.baidu.palo.common.MetaNotFoundException;
import com.baidu.palo.common.io.Text;
import com.baidu.palo.common.util.ListComparator;
import com.baidu.palo.load.Load;
import com.baidu.palo.persist.ReplicaPersistInfo;
import com.baidu.palo.task.AgentTask;
import com.baidu.palo.task.AgentTaskQueue;
import com.baidu.palo.task.CreateRollupTask;
import com.baidu.palo.thrift.TKeysType;
import com.baidu.palo.thrift.TResourceInfo;
import com.baidu.palo.thrift.TStorageType;
import com.baidu.palo.thrift.TTabletInfo;
import com.baidu.palo.thrift.TTaskType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class RollupJob extends AlterJob {
    private static final Logger LOG = LogManager.getLogger(RollupJob.class);

    // for replica statistic
    private Multimap<Long, Long> partitionIdToUnfinishedReplicaIds;
    private int totalReplicaNum;

    // partition id -> (rollup tablet id -> base tablet id)
    private Map<Long, Map<Long, Long>> partitionIdToBaseRollupTabletIdMap;
    private Map<Long, MaterializedIndex> partitionIdToRollupIndex;
    // Record new replica info for catalog restore
    // partition id -> list<ReplicaPersistInfo>
    private Multimap<Long, ReplicaPersistInfo> partitionIdToReplicaInfos;

    private Set<Long> finishedPartitionIds;

    // rollup and base schema info
    private long baseIndexId;
    private long rollupIndexId;
    private String baseIndexName;
    private String rollupIndexName;

    private List<Column> rollupSchema;
    private int baseSchemaHash;
    private int rollupSchemaHash;

    private TStorageType rollupStorageType;
    private TKeysType rollupKeysType;
    private short rollupShortKeyColumnCount;

    private RollupJob() {
        super(JobType.ROLLUP);

        this.partitionIdToUnfinishedReplicaIds = HashMultimap.create();
        this.totalReplicaNum = 0;

        this.partitionIdToBaseRollupTabletIdMap = new HashMap<Long, Map<Long, Long>>();
        this.partitionIdToRollupIndex = new HashMap<Long, MaterializedIndex>();
        this.partitionIdToReplicaInfos = LinkedHashMultimap.create();

        this.rollupSchema = new LinkedList<Column>();

        this.finishedPartitionIds = new HashSet<Long>();
    }

    public RollupJob(long dbId, long tableId, long baseIndexId, long rollupIndexId,
                      String baseIndexName, String rollupIndexName, List<Column> rollupSchema,
                      int baseSchemaHash, int rollupSchemaHash, TStorageType rollupStorageType,
                      short rollupShortKeyColumnCount, TResourceInfo resourceInfo, TKeysType rollupKeysType) {
        super(JobType.ROLLUP, dbId, tableId, resourceInfo);

        // rollup and base info
        this.baseIndexId = baseIndexId;
        this.rollupIndexId = rollupIndexId;
        this.baseIndexName = baseIndexName;
        this.rollupIndexName = rollupIndexName;

        this.rollupSchema = rollupSchema;
        this.baseSchemaHash = baseSchemaHash;
        this.rollupSchemaHash = rollupSchemaHash;
        
        this.rollupStorageType = rollupStorageType;
        this.rollupKeysType = rollupKeysType;
        this.rollupShortKeyColumnCount = rollupShortKeyColumnCount;

        // init other data struct
        this.partitionIdToUnfinishedReplicaIds = ArrayListMultimap.create();
        this.totalReplicaNum = 0;

        this.partitionIdToBaseRollupTabletIdMap = new HashMap<Long, Map<Long, Long>>();
        this.partitionIdToRollupIndex = new HashMap<Long, MaterializedIndex>();
        this.partitionIdToReplicaInfos = LinkedHashMultimap.create();

        this.finishedPartitionIds = new HashSet<Long>();
    }

    public final long getBaseIndexId() {
        return baseIndexId;
    }

    public final long getRollupIndexId() {
        return rollupIndexId;
    }

    public final String getBaseIndexName() {
        return baseIndexName;
    }

    public final String getRollupIndexName() {
        return rollupIndexName;
    }

    public final int getBaseSchemaHash() {
        return baseSchemaHash;
    }

    public final int getRollupSchemaHash() {
        return rollupSchemaHash;
    }

    public final TStorageType getRollupStorageType() {
        return rollupStorageType;
    }
    
    public final TKeysType getRollupKeysType() {
        return rollupKeysType;
    }

    public final short getRollupShortKeyColumnCount() {
        return rollupShortKeyColumnCount;
    }

    public final synchronized int getTotalReplicaNum() {
        return this.totalReplicaNum;
    }

    public synchronized int getUnfinishedReplicaNum() {
        return partitionIdToUnfinishedReplicaIds.values().size();
    }

    public void setTabletIdMap(long partitionId, long rollupTabletId, long baseTabletId) {
        Map<Long, Long> tabletIdMap = partitionIdToBaseRollupTabletIdMap.get(partitionId);
        if (tabletIdMap == null) {
            tabletIdMap = new HashMap<Long, Long>();
            partitionIdToBaseRollupTabletIdMap.put(partitionId, tabletIdMap);
        }
        tabletIdMap.put(rollupTabletId, baseTabletId);
    }

    public void addRollupIndex(long partitionId, MaterializedIndex rollupIndex) {
        this.partitionIdToRollupIndex.put(partitionId, rollupIndex);
    }

    public synchronized void updateRollupReplicaInfo(long partitionId, long indexId, long tabletId, long backendId,
                                                     int schemaHash, long version, long versionHash, long rowCount,
                                                     long dataSize) throws MetaNotFoundException {
        MaterializedIndex rollupIndex = this.partitionIdToRollupIndex.get(partitionId);
        if (rollupIndex == null || rollupIndex.getId() != indexId) {
            throw new MetaNotFoundException("Cannot find rollup index[" + indexId + "]");
        }

        Tablet tablet = rollupIndex.getTablet(tabletId);
        if (tablet == null) {
            throw new MetaNotFoundException("Cannot find tablet[" + tabletId + "]");
        }

        // update replica info
        Replica replica = tablet.getReplicaByBackendId(backendId);
        if (replica == null) {
            throw new MetaNotFoundException("cannot find replica in tablet[" + tabletId + "], backend[" + backendId
                    + "]");
        }
        replica.updateInfo(version, versionHash, dataSize, rowCount);
        LOG.debug("rollup replica[{}] info updated. schemaHash:{}", replica.getId(), schemaHash);
    }

    public synchronized List<List<Comparable>> getInfos() {
        List<List<Comparable>> rollupJobInfos = new LinkedList<List<Comparable>>();
        for (Map.Entry<Long, MaterializedIndex> entry : this.partitionIdToRollupIndex.entrySet()) {
            long partitionId = entry.getKey();
            MaterializedIndex rollupIndex = entry.getValue();

            List<Comparable> partitionInfo = new ArrayList<Comparable>();

            // partition id
            partitionInfo.add(Long.valueOf(partitionId));
            // rollup index id
            partitionInfo.add(Long.valueOf(rollupIndex.getId()));
            // table state
            partitionInfo.add(rollupIndex.getState().name());

            rollupJobInfos.add(partitionInfo);
        }

        // sort by 
        // "PartitionId",  "IndexState"
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0, 1);
        Collections.sort(rollupJobInfos, comparator);

        return rollupJobInfos;
    }

    public synchronized List<List<Comparable>> getRollupIndexInfo(long partitionId) {
        List<List<Comparable>> tabletInfos = new ArrayList<List<Comparable>>();

        MaterializedIndex index = this.partitionIdToRollupIndex.get(partitionId);
        if (index == null) {
            return tabletInfos;
        }

        for (Tablet tablet : index.getTablets()) {
            long tabletId = tablet.getId();
            for (Replica replica : tablet.getReplicas()) {
                List<Comparable> tabletInfo = new ArrayList<Comparable>();
                // tabletId -- replicaId -- backendId -- version -- versionHash -- dataSize -- rowCount -- state
                tabletInfo.add(tabletId);
                tabletInfo.add(replica.getId());
                tabletInfo.add(replica.getBackendId());
                tabletInfo.add(replica.getVersion());
                tabletInfo.add(replica.getVersionHash());
                tabletInfo.add(replica.getDataSize());
                tabletInfo.add(replica.getRowCount());
                tabletInfo.add(replica.getState());
                tabletInfos.add(tabletInfo);
            }
        }

        return tabletInfos;
    }

    @Override
    public synchronized void addReplicaId(long parentId, long replicaId, long backendId) {
        this.partitionIdToUnfinishedReplicaIds.put(parentId, replicaId);
        this.backendIdToReplicaIds.put(backendId, replicaId);
        ++this.totalReplicaNum;
    }

    @Override
    public synchronized void setReplicaFinished(long parentId, long replicaId) {
        // parent id is partitionId here
        if (parentId == -1L) {
            for (long onePartitionId : partitionIdToUnfinishedReplicaIds.keySet()) {
                Collection<Long> replicaIds = partitionIdToUnfinishedReplicaIds.get(onePartitionId);
                replicaIds.remove(replicaId);
            }
        } else {
            this.partitionIdToUnfinishedReplicaIds.get(parentId).remove(replicaId);
        }
    }

    @Override
    public boolean sendTasks() {
        Preconditions.checkState(this.state == JobState.PENDING);
        // here we just rejoin tasks to AgentTaskQueue.
        // task report process will later resend these tasks

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

                LOG.info("sending create rollup job[{}] tasks.", tableId);
                for (Map.Entry<Long, MaterializedIndex> entry : this.partitionIdToRollupIndex.entrySet()) {
                    long partitionId = entry.getKey();
                    Partition partition = olapTable.getPartition(partitionId);
                    if (partition == null) {
                        continue;
                    }

                    MaterializedIndex rollupIndex = entry.getValue();

                    Map<Long, Long> tabletIdMap = this.partitionIdToBaseRollupTabletIdMap.get(partitionId);
                    for (Tablet rollupTablet : rollupIndex.getTablets()) {
                        long rollupTabletId = rollupTablet.getId();
                        List<Replica> rollupReplicas = rollupTablet.getReplicas();
                        for (Replica rollupReplica : rollupReplicas) {
                            long backendId = rollupReplica.getBackendId();
                            long rollupReplicaId = rollupReplica.getId();
                            Preconditions.checkNotNull(tabletIdMap.get(rollupTabletId)); // baseTabletId
                            CreateRollupTask createRollupTask =
                                    new CreateRollupTask(resourceInfo, backendId, dbId, tableId,
                                                         partitionId, rollupIndexId, baseIndexId,
                                                         rollupTabletId, tabletIdMap.get(rollupTabletId),
                                                         rollupReplicaId,
                                                         rollupShortKeyColumnCount,
                                                         rollupSchemaHash, baseSchemaHash,
                                                         rollupStorageType, rollupSchema,
                                                         olapTable.getCopiedBfColumns(), olapTable.getBfFpp(), 
                                                         rollupKeysType);
                            AgentTaskQueue.addTask(createRollupTask);

                            addReplicaId(partitionId, rollupReplicaId, backendId);
                        } // end for rollupReplicas
                    } // end for rollupTablets
                }

                this.state = JobState.RUNNING;
            }
        } finally {
            db.readUnlock();
        }

        Preconditions.checkState(this.state == JobState.RUNNING);
        LOG.info("successfully sending rollup job[{}]", tableId);
        return true;
    }

    @Override
    public synchronized void cancel(OlapTable olapTable, String msg) {
        // remove tasks
        for (MaterializedIndex rollupIndex : this.partitionIdToRollupIndex.values()) {
            for (Tablet rollupTablet : rollupIndex.getTablets()) {
                long rollupTabletId = rollupTablet.getId();
                List<Replica> rollupReplicas = rollupTablet.getReplicas();
                for (Replica rollupReplica : rollupReplicas) {
                    long backendId = rollupReplica.getBackendId();
                    AgentTaskQueue.removeTask(backendId, TTaskType.ROLLUP, rollupTabletId);
                }

                Catalog.getCurrentInvertedIndex().deleteTablet(rollupTabletId);
            }
            LOG.debug("rollup job[{}]'s all tasks removed", tableId);
        }

        // set state
        if (olapTable != null) {
            Preconditions.checkState(olapTable.getId() == tableId);
            for (Partition partition : olapTable.getPartitions()) {
                if (partition.getState() == PartitionState.ROLLUP) {
                    partition.setState(PartitionState.NORMAL);
                }
            }

            if (olapTable.getState() == OlapTableState.ROLLUP) {
                olapTable.setState(OlapTableState.NORMAL);
            }
        }

        this.state = JobState.CANCELLED;
        if (msg != null) {
            this.cancelMsg = msg;
        }

        this.finishedTime = System.currentTimeMillis();

        // log
        Catalog.getInstance().getEditLog().logCancelRollup(this);
        LOG.debug("log cancel rollup job[{}]", tableId);
    }

    /*
     * this is called when base replica is deleted from meta.
     * we have to find the corresponding rollup replica and mark it as finished
     */
    @Override
    public synchronized void removeReplicaRelatedTask(long parentId, long baseTabletId,
                                                      long baseReplicaId, long backendId) {
        // parent id here is partition id
        // baseReplicaId is unused here

        // 1. find rollup index
        MaterializedIndex rollupIndex = this.partitionIdToRollupIndex.get(parentId);

        // 2. find rollup tablet
        long rollupTabletId = -1L;
        Map<Long, Long> tabletIdMap = this.partitionIdToBaseRollupTabletIdMap.get(parentId);
        for (Map.Entry<Long, Long> entry : tabletIdMap.entrySet()) {
            if (entry.getValue() == baseTabletId) {
                rollupTabletId = entry.getKey();
            }
        }
        Preconditions.checkState(rollupTabletId != -1L);
        Tablet rollupTablet = rollupIndex.getTablet(rollupTabletId);
        Preconditions.checkNotNull(rollupIndex);

        // 3. find rollup replica
        Replica rollupReplica = rollupTablet.getReplicaByBackendId(backendId);
        if (rollupReplica == null) {
            LOG.debug("can not find rollup replica in rollup tablet[{}]. backend[{}]", rollupTabletId, backendId);
            return;
        }
        
        setReplicaFinished(parentId, rollupReplica.getId());
        this.backendIdToReplicaIds.get(backendId).remove(rollupReplica.getId());

        // 4. remove task
        AgentTaskQueue.removeTask(backendId, TTaskType.ROLLUP, rollupTabletId);
    }

    @Override
    public synchronized void directRemoveReplicaTask(long replicaId, long backendId) {
        setReplicaFinished(-1L, replicaId);
        this.backendIdToReplicaIds.get(backendId).remove(replicaId);
    }

    @Override
    public synchronized void handleFinishedReplica(AgentTask task, TTabletInfo finishTabletInfo, long reportVersion)
            throws MetaNotFoundException {
        // report version is unused here

        Preconditions.checkArgument(task instanceof CreateRollupTask);
        CreateRollupTask createRollupTask = (CreateRollupTask) task;

        long partitionId = createRollupTask.getPartitionId();
        long rollupIndexId = createRollupTask.getIndexId();
        long rollupTabletId = createRollupTask.getTabletId();
        long rollupReplicaId = createRollupTask.getRollupReplicaId();

        MaterializedIndex rollupIndex = this.partitionIdToRollupIndex.get(partitionId);
        if (rollupIndex == null || rollupIndex.getId() != rollupIndexId) {
            throw new MetaNotFoundException("Cannot find rollup index[" + rollupIndexId + "]");
        }
        Preconditions.checkState(rollupIndex.getState() == IndexState.ROLLUP);

        Preconditions.checkArgument(finishTabletInfo.getTablet_id() == rollupTabletId);
        Tablet rollupTablet = rollupIndex.getTablet(rollupTabletId);
        if (rollupTablet == null) {
            throw new MetaNotFoundException("Cannot find rollup tablet[" + rollupTabletId + "]");
        }

        Replica rollupReplica = rollupTablet.getReplicaById(rollupReplicaId);
        if (rollupReplica == null) {
            throw new MetaNotFoundException("Cannot find rollup replica[" + rollupReplicaId + "]");
        }
        if (rollupReplica.getState() == ReplicaState.NORMAL) {
            // FIXME(cmy): still don't know why this happend. add log to observe
            LOG.warn("rollup replica[{}]' state is already set to NORMAL. tablet[{}]. backend[{}]",
                     rollupReplicaId, rollupTabletId, task.getBackendId());
        }

        long version = finishTabletInfo.getVersion();
        long versionHash = finishTabletInfo.getVersion_hash();
        long dataSize = finishTabletInfo.getData_size();
        long rowCount = finishTabletInfo.getRow_count();
        rollupReplica.updateInfo(version, versionHash, dataSize, rowCount);

        setReplicaFinished(partitionId, rollupReplicaId);
        rollupReplica.setState(ReplicaState.NORMAL);

        LOG.info("finished rollup replica[{}]. index[{}]. tablet[{}]. backend[{}]",
                 rollupReplicaId, rollupIndexId, rollupTabletId, task.getBackendId());
    }

    @Override
    public int tryFinishJob() {
        if (this.state != JobState.RUNNING) {
            LOG.info("rollup job[{}] is not running.", tableId);
            return 0;
        }

        Database db = Catalog.getInstance().getDb(dbId);
        if (db == null) {
            String msg = "db[" + dbId + "] does not exist";
            setMsg(msg);
            LOG.warn(msg);
            return -1;
        }

        if (!db.tryWriteLock(Database.TRY_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            return 0;
        }
        try {
            synchronized (this) {
                OlapTable olapTable = (OlapTable) db.getTable(tableId);
                if (olapTable == null) {
                    cancelMsg = "db[" + dbId + "] does not exist";
                    LOG.warn(cancelMsg);
                    return -1;
                }

                for (Map.Entry<Long, MaterializedIndex> entry : this.partitionIdToRollupIndex.entrySet()) {
                    long partitionId = entry.getKey();
                    Partition partition = olapTable.getPartition(partitionId);
                    if (partition == null) {
                        LOG.warn("partition[{}] does not exist", partitionId);
                        continue;
                    }

                    // check if all tablets finished load job
                    // FIXME(cmy): this may cause endless check??
                    Load load = Catalog.getInstance().getLoadInstance();
                    if (!load.checkPartitionLoadFinished(partitionId, null)) {
                        LOG.debug("partition[{}] has unfinished load job", partitionId);
                        return 0;
                    }

                    short replicationNum = olapTable.getPartitionInfo().getReplicationNum(partition.getId());

                    // check version and versionHash
                    long committedVersion = partition.getCommittedVersion();
                    long committedVersionHash = partition.getCommittedVersionHash();
                    MaterializedIndex rollupIndex = entry.getValue();
                    for (Tablet rollupTablet : rollupIndex.getTablets()) {
                        Iterator<Replica> iterator = rollupTablet.getReplicas().iterator();
                        boolean isCatchUp = true;
                        while (iterator.hasNext()) {
                            Replica replica = iterator.next();
                            if (!this.backendIdToReplicaIds.get(replica.getBackendId()).contains(replica.getId())) {
                                // replica is dead, remove it
                                LOG.warn("rollup job[{}] find dead replica[{}] in backend[{}]. remove it",
                                         tableId, replica.getId(), replica.getBackendId());
                                iterator.remove();
                                continue;
                            }

                            if (!checkBackendState(replica)) {
                                continue;
                            }

                            // check version
                            if (!replica.checkVersionCatchUp(committedVersion, committedVersionHash)) {
                                isCatchUp = false;
                                continue;
                            }
                        }

                        if (rollupTablet.getReplicas().size() < (replicationNum / 2 + 1)) {
                            cancelMsg = String.format("rollup job[%d] cancelled. tablet[%d] has few replica."
                                    + " num: %d", tableId, rollupTablet.getId(), rollupTablet.getReplicas().size());
                            LOG.warn(cancelMsg);
                            return -1;
                        }

                        if (!isCatchUp) {
                            return 0;
                        }
                    } // end for tablets

                    // check if partition is finised
                    if (!this.partitionIdToUnfinishedReplicaIds.get(partitionId).isEmpty()) {
                        LOG.debug("partition[{}] has unfinished rollup replica: {}",
                                  partitionId, this.partitionIdToUnfinishedReplicaIds.get(partitionId).size());
                        return 0;
                    }

                    // partition is finished
                    if (!this.finishedPartitionIds.contains(partitionId)) {
                        Preconditions.checkState(rollupIndex.getState() == IndexState.ROLLUP);
                        rollupIndex.setState(IndexState.NORMAL);
                        this.finishedPartitionIds.add(partitionId);

                        // remove task for safety
                        // task may be left if some backends are down during schema change
                        for (Tablet tablet : rollupIndex.getTablets()) {
                            for (Replica replica : tablet.getReplicas()) {
                                AgentTaskQueue.removeTask(replica.getBackendId(), TTaskType.ROLLUP,
                                                          tablet.getId());
                            }
                        }

                        LOG.info("create rollup index[{}] finished in partition[{}]",
                                 rollupIndex.getId(), partitionId);
                    }
                } // end for partitions

                // all partition is finished rollup
                // add rollup index to each partition
                for (Partition partition : olapTable.getPartitions()) {
                    long partitionId = partition.getId();
                    MaterializedIndex rollupIndex = this.partitionIdToRollupIndex.get(partitionId);
                    Preconditions.checkNotNull(rollupIndex);

                    // 1. record replica info
                    for (Tablet tablet : rollupIndex.getTablets()) {
                        long tabletId = tablet.getId();
                        for (Replica replica : tablet.getReplicas()) {
                            ReplicaPersistInfo replicaInfo =
                                    ReplicaPersistInfo.createForRollup(rollupIndexId, tabletId, replica.getBackendId(),
                                                                       replica.getVersion(), replica.getVersionHash(),
                                                                       replica.getDataSize(), replica.getRowCount());
                            this.partitionIdToReplicaInfos.put(partitionId, replicaInfo);
                        }
                    } // end for tablets

                    // 2. add to partition
                    partition.createRollupIndex(rollupIndex);

                    // 3. add rollup finished version to base index
                    MaterializedIndex baseIndex = partition.getIndex(baseIndexId);
                    if (baseIndex != null) {
                        baseIndex.setRollupIndexInfo(rollupIndexId, partition.getCommittedVersion());
                    }
                    Preconditions.checkState(partition.getState() == PartitionState.ROLLUP);
                    partition.setState(PartitionState.NORMAL);
                } // end for partitions

                // set index's info
                olapTable.setIndexSchemaInfo(rollupIndexId, rollupIndexName, rollupSchema, 0,
                                             rollupSchemaHash, rollupShortKeyColumnCount);
                olapTable.setStorageTypeToIndex(rollupIndexId, rollupStorageType);
                Preconditions.checkState(olapTable.getState() == OlapTableState.ROLLUP);
                olapTable.setState(OlapTableState.NORMAL);

                this.finishedTime = System.currentTimeMillis();
                this.state = JobState.FINISHED;
            }
        } finally {
            db.writeUnlock();
        }

        // log rollup done operation
        Catalog.getInstance().getEditLog().logFinishRollup(this);
        LOG.info("rollup job[{}] done.", this.getTableId());

        return 1;
    }

    @Override
    public synchronized void clear() {
        this.resourceInfo = null;
        this.partitionIdToUnfinishedReplicaIds = null;
        this.partitionIdToBaseRollupTabletIdMap = null;
        // do not set to null. show proc use it
        this.partitionIdToRollupIndex.clear();
        this.finishedPartitionIds = null;
        this.partitionIdToReplicaInfos = null;
        this.rollupSchema = null;
    }

    @Override
    public void unprotectedReplayInitJob(Database db) {
        // set state
        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
        OlapTable olapTable = (OlapTable) db.getTable(tableId);
        for (Map.Entry<Long, MaterializedIndex> entry : this.partitionIdToRollupIndex.entrySet()) {
            Partition partition = olapTable.getPartition(entry.getKey());
            partition.setState(PartitionState.ROLLUP);

            if (!Catalog.isCheckpointThread()) {
                MaterializedIndex rollupIndex = entry.getValue();
                TabletMeta tabletMeta = new TabletMeta(dbId, tableId, entry.getKey(), rollupIndexId, rollupSchemaHash);
                for (Tablet tablet : rollupIndex.getTablets()) {
                    long tabletId = tablet.getId();
                    invertedIndex.addTablet(tabletId, tabletMeta);
                    for (Replica replica : tablet.getReplicas()) {
                        invertedIndex.addReplica(tabletId, replica);
                    }
                }
            }
        } // end for partitions
        olapTable.setState(OlapTableState.ROLLUP);

        // reset status to PENDING for resending the tasks in polling thread
        this.state = JobState.PENDING;
    }

    @Override
    public void unprotectedReplayFinish(Database db) {
        OlapTable olapTable = (OlapTable) db.getTable(tableId);

        for (Map.Entry<Long, MaterializedIndex> entry : this.partitionIdToRollupIndex.entrySet()) {
            long partitionId = entry.getKey();
            Partition partition = olapTable.getPartition(partitionId);
            MaterializedIndex rollupIndex = entry.getValue();

            for (Tablet tablet : rollupIndex.getTablets()) {
                for (Replica replica : tablet.getReplicas()) {
                    replica.setState(ReplicaState.NORMAL);
                }
            }
            rollupIndex.setState(IndexState.NORMAL);

            MaterializedIndex baseIndex = partition.getIndex(baseIndexId);
            if (baseIndex != null) {
                baseIndex.setRollupIndexInfo(rollupIndexId, partition.getCommittedVersion());
            }

            partition.createRollupIndex(rollupIndex);
            partition.setState(PartitionState.NORMAL);

            // Update database information
            Collection<ReplicaPersistInfo> replicaInfos = partitionIdToReplicaInfos.get(partitionId);
            if (replicaInfos != null) {
                for (ReplicaPersistInfo info : replicaInfos) {
                    MaterializedIndex mIndex = partition.getIndex(info.getIndexId());
                    Tablet tablet = mIndex.getTablet(info.getTabletId());
                    Replica replica = tablet.getReplicaByBackendId(info.getBackendId());
                    replica.updateInfo(info.getVersion(), info.getVersionHash(),
                                       info.getDataSize(), info.getRowCount());
                }
            }
        }

        olapTable.setIndexSchemaInfo(rollupIndexId, rollupIndexName, rollupSchema, 0,
                                     rollupSchemaHash, rollupShortKeyColumnCount);
        olapTable.setStorageTypeToIndex(rollupIndexId, rollupStorageType);
        olapTable.setState(OlapTableState.NORMAL);
    }

    @Override
    public void unprotectedReplayCancel(Database db) {
        OlapTable olapTable = (OlapTable) db.getTable(tableId);
        if (olapTable == null) {
            return;
        }

        if (!Catalog.isCheckpointThread()) {
            // remove from inverted index
            for (MaterializedIndex rollupIndex : partitionIdToRollupIndex.values()) {
                for (Tablet tablet : rollupIndex.getTablets()) {
                    Catalog.getCurrentInvertedIndex().deleteTablet(tablet.getId());
                }
            }
        }

        // set state
        for (Partition partition : olapTable.getPartitions()) {
            partition.setState(PartitionState.NORMAL);
        }
        olapTable.setState(OlapTableState.NORMAL);
    }

    @Override
    public synchronized void write(DataOutput out) throws IOException {
        super.write(out);

        // 'partitionIdToUnfinishedReplicaIds' and 'totalReplicaNum' don't need persist
        // build them when resendTasks

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

            out.writeInt(partitionIdToReplicaInfos.get(partitionId).size());
            for (ReplicaPersistInfo info : partitionIdToReplicaInfos.get(partitionId)) {
                info.write(out);
            }
        }

        // backendIdToReplicaIds don't need persisit

        out.writeLong(baseIndexId);
        out.writeLong(rollupIndexId);
        Text.writeString(out, baseIndexName);
        Text.writeString(out, rollupIndexName);

        // schema
        if (rollupSchema != null) {
            out.writeBoolean(true);
            int count = rollupSchema.size();
            out.writeInt(count);
            for (Column column : rollupSchema) {
                column.write(out);
            }
        } else {
            out.writeBoolean(false);
        }

        out.writeInt(baseSchemaHash);
        out.writeInt(rollupSchemaHash);

        out.writeShort(rollupShortKeyColumnCount);
        Text.writeString(out, rollupStorageType.name());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        int partitionNum = in.readInt();
        for (int i = 0; i < partitionNum; i++) {
            long partitionId = in.readLong();

            // tablet map
            int count = in.readInt();
            Map<Long, Long> tabletMap = new HashMap<Long, Long>();
            for (int j = 0; j < count; j++) {
                long rollupTabletId = in.readLong();
                long baseTabletId = in.readLong();
                tabletMap.put(rollupTabletId, baseTabletId);
            }
            partitionIdToBaseRollupTabletIdMap.put(partitionId, tabletMap);

            // rollup index
            MaterializedIndex rollupIndex = MaterializedIndex.read(in);
            partitionIdToRollupIndex.put(partitionId, rollupIndex);

            // replica infos
            count = in.readInt();
            for (int j = 0; j < count; j++) {
                ReplicaPersistInfo replicaInfo = ReplicaPersistInfo.read(in);
                partitionIdToReplicaInfos.put(partitionId, replicaInfo);
            }
        }

        baseIndexId = in.readLong();
        rollupIndexId = in.readLong();
        baseIndexName = Text.readString(in);
        rollupIndexName = Text.readString(in);

        // schema
        boolean hasSchema = in.readBoolean();
        if (hasSchema) {
            int count = in.readInt();
            for (int i = 0; i < count; i++) {
                Column column = Column.read(in);
                rollupSchema.add(column);
            }
        }

        baseSchemaHash = in.readInt();
        rollupSchemaHash = in.readInt();

        rollupShortKeyColumnCount = in.readShort();
        rollupStorageType = TStorageType.valueOf(Text.readString(in));
    }

    public static RollupJob read(DataInput in) throws IOException {
        RollupJob rollupJob = new RollupJob();
        rollupJob.readFields(in);
        return rollupJob;
    }

    public boolean equals(Object obj) {
        return true;
    }
}
