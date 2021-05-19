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
import org.apache.doris.catalog.Partition.PartitionState;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.persist.ReplicaPersistInfo;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.ClearAlterTask;
import org.apache.doris.task.CreateRollupTask;
import org.apache.doris.thrift.TKeysType;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTabletInfo;
import org.apache.doris.thrift.TTaskType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    // yiguolei: every job has a transactionid to identify the current time, for example
    // a load job's transactionid is 10 and a rollup job's transaction id is 12, then we could
    // find load job is occurred before rollup job
    public RollupJob(long dbId, long tableId, long baseIndexId, long rollupIndexId,
                      String baseIndexName, String rollupIndexName, List<Column> rollupSchema,
                      int baseSchemaHash, int rollupSchemaHash, TStorageType rollupStorageType,
                      short rollupShortKeyColumnCount, TResourceInfo resourceInfo, TKeysType rollupKeysType, 
                      long transactionId) {
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
        
        this.transactionId = transactionId;
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
        replica.updateVersionInfo(version, versionHash, dataSize, rowCount);
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
    
    public synchronized MaterializedIndex getRollupIndex(long partitionId) {
        MaterializedIndex index = this.partitionIdToRollupIndex.get(partitionId);
        return index;
    }

    @Override
    public synchronized void addReplicaId(long parentId, long replicaId, long backendId) {
        this.partitionIdToUnfinishedReplicaIds.put(parentId, replicaId);
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

        synchronized (this) {
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
                LOG.info("sending clear rollup job tasks for table [{}]", tableId);
                for (Partition partition : olapTable.getPartitions()) {
                    long partitionId = partition.getId();
                    // has to use rollup base index, could not use partition.getBaseIndex()
                    // because the rollup index could be created based on another rollup index
                    MaterializedIndex baseIndex = partition.getIndex(this.getBaseIndexId());
                    for (Tablet baseTablet : baseIndex.getTablets()) {
                        long baseTabletId = baseTablet.getId();
                        List<Replica> baseReplicas = baseTablet.getReplicas();
                        for (Replica baseReplica : baseReplicas) {
                            long backendId = baseReplica.getBackendId();
                            ClearAlterTask clearRollupTask = new ClearAlterTask(backendId, dbId, tableId,
                                    partitionId, baseIndexId, baseTabletId, baseSchemaHash);
                            if (AgentTaskQueue.addTask(clearRollupTask)) {
                                batchClearAlterTask.addTask(clearRollupTask);
                            } else {
                                allAddSuccess = false;
                                break;
                            }
                        } // end for rollupReplicas
                        if (!allAddSuccess) {
                            break;
                        }
                    } // end for rollupTablets
                    if (!allAddSuccess) {
                        break;
                    }
                }
                if (!allAddSuccess) {
                    for (AgentTask task : batchClearAlterTask.getAllTasks()) {
                        AgentTaskQueue.removeTask(task.getBackendId(), task.getTaskType(), task.getSignature());
                    }
                    batchClearAlterTask = null;
                }

            } finally {
                olapTable.readUnlock();
            }
        }
        LOG.info("successfully sending clear rollup job[{}]", tableId);
        return 0;
    }
    
    @Override
    public boolean sendTasks() {
        Preconditions.checkState(this.state == JobState.PENDING);
        // here we just rejoin tasks to AgentTaskQueue.
        // task report process will later resend these tasks

        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            cancelMsg = "db[" + dbId + "] does not exist";
            LOG.warn(cancelMsg);
            return false;
        }


        synchronized (this) {
            OlapTable olapTable = null;
            try {
                olapTable = (OlapTable) db.getTableOrThrowException(tableId, Table.TableType.OLAP);
            } catch (MetaNotFoundException e) {
                LOG.warn(e.getMessage());
                return false;
            }

            LOG.info("sending rollup job[{}] tasks.", tableId);
            // in palo 3.2, the rollup keys type is not serialized, when a fe follower change to fe master
            // the rollup keys type == null, so that send tasks will report error
            if (rollupKeysType == null) {
                rollupKeysType = olapTable.getKeysType().toThrift();
            }
            olapTable.readLock();
            try {
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
            } finally {
                olapTable.readUnlock();
            }

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
        if (Strings.isNullOrEmpty(cancelMsg) && !Strings.isNullOrEmpty(msg)) {
            this.cancelMsg = msg;
        }

        this.finishedTime = System.currentTimeMillis();

        // log
        Catalog.getCurrentCatalog().getEditLog().logCancelRollup(this);
        LOG.debug("cancel rollup job[{}] finished. because: {}", tableId, cancelMsg);
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
        LOG.debug("remove replica {} from backend {}", rollupReplica, backendId, new Exception());
        setReplicaFinished(parentId, rollupReplica.getId());

        // 4. remove task
        AgentTaskQueue.removeTask(backendId, TTaskType.ROLLUP, rollupTabletId);
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

        Preconditions.checkArgument(finishTabletInfo.getTabletId() == rollupTabletId);
        Tablet rollupTablet = rollupIndex.getTablet(rollupTabletId);
        if (rollupTablet == null) {
            throw new MetaNotFoundException("Cannot find rollup tablet[" + rollupTabletId + "]");
        }

        Replica rollupReplica = rollupTablet.getReplicaById(rollupReplicaId);
        if (rollupReplica == null) {
            throw new MetaNotFoundException("Cannot find rollup replica[" + rollupReplicaId + "]");
        }
        if (rollupReplica.getState() == ReplicaState.NORMAL) {
            // FIXME(cmy): still don't know why this can happen. add log to observe
            LOG.warn("rollup replica[{}]' state is already set to NORMAL. tablet[{}]. backend[{}]",
                     rollupReplicaId, rollupTabletId, task.getBackendId());
        }

        long version = finishTabletInfo.getVersion();
        long versionHash = finishTabletInfo.getVersionHash();
        long dataSize = finishTabletInfo.getDataSize();
        long rowCount = finishTabletInfo.getRowCount();
        // yiguolei: not check version here because the replica's first version will be set by rollup job
        // the version is not set now
        rollupReplica.updateVersionInfo(version, versionHash, dataSize, rowCount);

        if (finishTabletInfo.isSetPathHash()) {
            rollupReplica.setPathHash(finishTabletInfo.getPathHash());
        }

        setReplicaFinished(partitionId, rollupReplicaId);
        rollupReplica.setState(ReplicaState.NORMAL);

        LOG.info("finished rollup replica[{}]. index[{}]. tablet[{}]. backend[{}], version: {}-{}",
                rollupReplicaId, rollupIndexId, rollupTabletId, task.getBackendId(), version, versionHash);
    }

    /*
     * we make the rollup visible, but keep table's state as ROLLUP.
     * 1. Make the rollup visible, because we want that the following load jobs will load data to the new
     *    rollup, too.
     * 2. keep the table's state in ROLLUP, because we don't want another alter job being processed.
     */
    @Override
    public int tryFinishJob() {
        if (this.state != JobState.RUNNING) {
            LOG.info("rollup job[{}] is not running or finishing.", tableId);
            return 0;
        }

        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            cancelMsg = "Db[" + dbId + "] does not exist";
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
            // if all previous transaction has finished, then check base and rollup replica num
            synchronized (this) {
                for (Map.Entry<Long, MaterializedIndex> entry : this.partitionIdToRollupIndex.entrySet()) {
                    long partitionId = entry.getKey();
                    Partition partition = olapTable.getPartition(partitionId);
                    if (partition == null) {
                        LOG.warn("partition[{}] does not exist", partitionId);
                        continue;
                    }

                    short expectReplicationNum = olapTable.getPartitionInfo().getReplicaAllocation(partition.getId()).getTotalReplicaNum();
                    MaterializedIndex rollupIndex = entry.getValue();
                    for (Tablet rollupTablet : rollupIndex.getTablets()) {
                        // yiguolei: the rollup tablet only contains the replica that is healthy at rollup time
                        List<Replica> replicas = rollupTablet.getReplicas();
                        List<Replica> errorReplicas = Lists.newArrayList();
                        for (Replica replica : replicas) {
                            if (!checkBackendState(replica)) {
                                LOG.warn("backend {} state is abnormal, set replica {} as bad", replica.getBackendId(),
                                         replica.getId());
                                errorReplicas.add(replica);
                            } else if (replica.getLastFailedVersion() > 0
                                    && !partitionIdToUnfinishedReplicaIds.get(partitionId).contains(replica.getId())) {
                                // if the replica has finished converting history data,
                                // but failed during load, then it is a abnormal.
                                // remove it from replica set
                                // have to use delete replica, it will remove it from tablet inverted index
                                LOG.warn("replica [{}] last failed version > 0 and have finished history rollup job,"
                                        + " its a bad replica, remove it from rollup tablet", replica);
                                errorReplicas.add(replica);
                            }
                        }

                        for (Replica errorReplica : errorReplicas) {
                            rollupTablet.deleteReplica(errorReplica);
                            setReplicaFinished(partitionId, errorReplica.getId());
                            AgentTaskQueue.removeTask(errorReplica.getBackendId(), TTaskType.ROLLUP, rollupTablet.getId());
                        }

                        if (rollupTablet.getReplicas().size() < (expectReplicationNum / 2 + 1)) {
                            cancelMsg = String.format(
                                    "rollup job[%d] cancelled. rollup tablet[%d] has few health replica."
                                    + " num: %d", tableId, rollupTablet.getId(), replicas.size());
                            LOG.warn(cancelMsg);
                            return -1;
                        }
                    } // end for tablets

                    // check if partition is finished
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
                                            rollupSchemaHash,
                                            replica.getDataSize(), replica.getRowCount(),
                                            replica.getLastFailedVersion(),
                                            replica.getLastFailedVersionHash(),
                                            replica.getLastSuccessVersion(),
                                            replica.getLastSuccessVersionHash());
                            this.partitionIdToReplicaInfos.put(partitionId, replicaInfo);
                        }
                    } // end for tablets

                    // 2. add to partition
                    partition.createRollupIndex(rollupIndex);

                    // 3. add rollup finished version to base index
                    MaterializedIndex baseIndex = partition.getIndex(baseIndexId);
                    if (baseIndex != null) {
                        baseIndex.setRollupIndexInfo(rollupIndexId, partition.getVisibleVersion());
                    }
                    Preconditions.checkState(partition.getState() == PartitionState.ROLLUP);
                    partition.setState(PartitionState.NORMAL);
                } // end for partitions

                // set index's info
                olapTable.setIndexMeta(rollupIndexId, rollupIndexName, rollupSchema, 0, rollupSchemaHash,
                        rollupShortKeyColumnCount, rollupStorageType, KeysType.fromThrift(rollupKeysType));
                Preconditions.checkState(olapTable.getState() == OlapTableState.ROLLUP);

                this.state = JobState.FINISHING;
                this.transactionId = Catalog.getCurrentGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId();
            }
        } finally {
            olapTable.writeUnlock();
        }

        Catalog.getCurrentCatalog().getEditLog().logFinishingRollup(this);
        LOG.info("rollup job[{}] is finishing.", this.getTableId());

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
    public void replayInitJob(Database db) {
        OlapTable olapTable = (OlapTable) db.getTable(tableId);
        olapTable.writeLock();
        try {
            // set state
            TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();

            for (Map.Entry<Long, MaterializedIndex> entry : this.partitionIdToRollupIndex.entrySet()) {
                Partition partition = olapTable.getPartition(entry.getKey());
                partition.setState(PartitionState.ROLLUP);
                TStorageMedium medium = olapTable.getPartitionInfo().getDataProperty(
                        partition.getId()).getStorageMedium();

                if (!Catalog.isCheckpointThread()) {
                    MaterializedIndex rollupIndex = entry.getValue();
                    TabletMeta tabletMeta = new TabletMeta(dbId, tableId, entry.getKey(), rollupIndexId,
                            rollupSchemaHash, medium);
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
        } finally {
            olapTable.writeUnlock();
        }
    }

    @Override
    public void replayFinishing(Database db) {
        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
        OlapTable olapTable = (OlapTable) db.getTable(tableId);
        olapTable.writeLock();
        try {
            for (Map.Entry<Long, MaterializedIndex> entry : this.partitionIdToRollupIndex.entrySet()) {
                long partitionId = entry.getKey();
                MaterializedIndex rollupIndex = entry.getValue();
                Partition partition = olapTable.getPartition(partitionId);

                if (!Catalog.isCheckpointThread()) {
                    // Here we have to use replicas in inverted index to rebuild the rollupIndex's tablet.
                    // Because the rollupIndex here is read from edit log, so the replicas in it are
                    // not the same objects as in inverted index.
                    // And checkpoint thread is no need to handle inverted index
                    for (Tablet tablet : rollupIndex.getTablets()) {
                        List<Replica> copiedReplicas = Lists.newArrayList(tablet.getReplicas());
                        tablet.clearReplica();
                        for (Replica copiedReplica : copiedReplicas) {
                            Replica replica = invertedIndex.getReplica(tablet.getId(), copiedReplica.getBackendId());
                            tablet.addReplica(replica, true);
                        }
                    }
                }

                long rollupRowCount = 0L;
                for (Tablet tablet : rollupIndex.getTablets()) {
                    for (Replica replica : tablet.getReplicas()) {
                        replica.setState(ReplicaState.NORMAL);
                    }

                    // calculate rollup index row count
                    long tabletRowCount = 0L;
                    for (Replica replica : tablet.getReplicas()) {
                        long replicaRowCount = replica.getRowCount();
                        if (replicaRowCount > tabletRowCount) {
                            tabletRowCount = replicaRowCount;
                        }
                    }
                    rollupRowCount += tabletRowCount;
                }

                rollupIndex.setRowCount(rollupRowCount);
                rollupIndex.setState(IndexState.NORMAL);

                MaterializedIndex baseIndex = partition.getIndex(baseIndexId);
                if (baseIndex != null) {
                    baseIndex.setRollupIndexInfo(rollupIndexId, partition.getVisibleVersion());
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
                        replica.updateVersionInfo(info.getVersion(), info.getVersionHash(),
                                                  info.getLastFailedVersion(),
                                                  info.getLastFailedVersionHash(),
                                                  info.getLastSuccessVersion(),
                                                  info.getLastSuccessVersionHash());
                    }
                }
            }

            olapTable.setIndexMeta(rollupIndexId, rollupIndexName, rollupSchema, 0, rollupSchemaHash,
                    rollupShortKeyColumnCount, rollupStorageType, KeysType.fromThrift(rollupKeysType));
        } finally {
            olapTable.writeUnlock();
        }

        LOG.info("replay finishing the rollup job: {}", tableId);
    }
    
    @Override
    public void replayFinish(Database db) {
        // if this is an old job, then should also update table or replica's state
        if (transactionId < 0) {
            replayFinishing(db);
        }

        OlapTable olapTable = (OlapTable) db.getTable(tableId);
        olapTable.writeLock();
        try {
            olapTable.setState(OlapTableState.NORMAL);
        } finally {
            olapTable.writeUnlock();
        }
    }

    @Override
    public void replayCancel(Database db) {
        OlapTable olapTable = (OlapTable) db.getTable(tableId);
        olapTable.writeLock();
        try{
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
        } finally {
            olapTable.writeUnlock();
        }
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
        LOG.info("finished rollup job: {}", tableId);
    }

    @Override
    public void getJobInfo(List<List<Comparable>> jobInfos, OlapTable tbl) {
        List<Comparable> jobInfo = new ArrayList<Comparable>();

        // job id
        jobInfo.add(tableId);

        // table name
        jobInfo.add(tbl.getName());

        // create time
        jobInfo.add(TimeUtils.longToTimeString(createTime));

        jobInfo.add(TimeUtils.longToTimeString(finishedTime));

        // base index and rollup index name
        jobInfo.add(baseIndexName);
        jobInfo.add(rollupIndexName);
        
        // rollup id
        jobInfo.add(rollupIndexId);

        // transaction id
        jobInfo.add(transactionId);

        // job state
        jobInfo.add(state.name());

        // msg
        jobInfo.add(cancelMsg);

        // progress
        if (state == JobState.RUNNING) {
            int unfinishedReplicaNum = getUnfinishedReplicaNum();
            int totalReplicaNum = getTotalReplicaNum();
            Preconditions.checkState(unfinishedReplicaNum <= totalReplicaNum);
            jobInfo.add(((totalReplicaNum - unfinishedReplicaNum) * 100 / totalReplicaNum) + "%");
        } else {
            jobInfo.add(FeConstants.null_string);
        }
        jobInfo.add(Config.alter_table_timeout_second);

        jobInfos.add(jobInfo);
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
        // when upgrade from 3.2, rollupKeysType == null
        if (rollupKeysType != null) {
            out.writeBoolean(true);
            Text.writeString(out, rollupKeysType.name());
        } else {
            out.writeBoolean(false);
        }
    }

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
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_45) {
            boolean hasRollKeysType = in.readBoolean();
            if (hasRollKeysType) {
                rollupKeysType = TKeysType.valueOf(Text.readString(in));
            }
        }
    }

    public static RollupJob read(DataInput in) throws IOException {
        RollupJob rollupJob = new RollupJob();
        rollupJob.readFields(in);
        return rollupJob;
    }

    @Override
    public boolean equals(Object obj) {
        return true;
    }

    @Override
    public String toString() {
        return "RollupJob [baseIndexId=" + baseIndexId + ", rollupIndexId=" + rollupIndexId + ", baseIndexName="
                + baseIndexName + ", rollupIndexName=" + rollupIndexName + ", rollupSchema=" + rollupSchema
                + ", baseSchemaHash=" + baseSchemaHash + ", rollupSchemaHash=" + rollupSchemaHash + ", type=" + type
                + ", state=" + state + ", dbId=" + dbId + ", tableId=" + tableId + ", transactionId=" + transactionId
                + ", isPreviousLoadFinished=" + isPreviousLoadFinished + ", createTime=" + createTime
                + ", finishedTime=" + finishedTime + "]";
    }
}
