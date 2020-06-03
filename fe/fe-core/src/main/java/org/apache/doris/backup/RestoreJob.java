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

package org.apache.doris.backup;

import org.apache.doris.backup.BackupJobInfo.BackupIndexInfo;
import org.apache.doris.backup.BackupJobInfo.BackupPartitionInfo;
import org.apache.doris.backup.BackupJobInfo.BackupTableInfo;
import org.apache.doris.backup.BackupJobInfo.BackupTabletInfo;
import org.apache.doris.backup.RestoreFileMapping.IdChain;
import org.apache.doris.backup.Status.ErrCode;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.Config;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.CreateReplicaTask;
import org.apache.doris.task.DirMoveTask;
import org.apache.doris.task.DownloadTask;
import org.apache.doris.task.ReleaseSnapshotTask;
import org.apache.doris.task.SnapshotTask;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTaskType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.Table.Cell;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RestoreJob extends AbstractJob {
    private static final Logger LOG = LogManager.getLogger(RestoreJob.class);

    public enum RestoreJobState {
        PENDING, // Job is newly created. Check and prepare meta in catalog. Create replica if necessary.
                 // Waiting for replica creation finished synchronously, then sending snapshot tasks.
                 // then transfer to SNAPSHOTING
        SNAPSHOTING, // Waiting for snapshot finished. Than transfer to DOWNLOAD.
        DOWNLOAD, // Send download tasks.
        DOWNLOADING, // Waiting for download finished.
        COMMIT, // After download finished, all data is ready for taking effect.
                    // Send movement tasks to BE, than transfer to COMMITTING
        COMMITTING, // wait all tasks finished. Transfer to FINISHED
        FINISHED,
        CANCELLED
    }

    private String backupTimestamp;

    private BackupJobInfo jobInfo;
    private boolean allowLoad;

    private RestoreJobState state;

    private BackupMeta backupMeta;

    private RestoreFileMapping fileMapping = new RestoreFileMapping();

    private long metaPreparedTime = -1;
    private long snapshotFinishedTime = -1;
    private long downloadFinishedTime = -1;

    private int restoreReplicationNum;

    // this 2 members is to save all newly restored objs
    // tbl name -> part
    private List<Pair<String, Partition>> restoredPartitions = Lists.newArrayList();
    private List<OlapTable> restoredTbls = Lists.newArrayList();

    // save all restored partitions' version info which are already exist in catalog
    // table id -> partition id -> (version, version hash)
    private com.google.common.collect.Table<Long, Long, Pair<Long, Long>> restoredVersionInfo = HashBasedTable.create();
    // tablet id->(be id -> snapshot info)
    private com.google.common.collect.Table<Long, Long, SnapshotInfo> snapshotInfos = HashBasedTable.create();

    private Map<Long, Long> unfinishedSignatureToId = Maps.newConcurrentMap();

    // the meta version is used when reading backup meta from file.
    // we do not persist this field, because this is just a temporary solution.
    // the true meta version should be get from backup job info, which is saved when doing backup job.
    // But the earlier version of Doris do not save the meta version in backup job info, so we allow user to
    // set this 'metaVersion' in restore stmt.
    // NOTICE: because we do not persist it, this info may be lost if Frontend restart,
    // and if you don't want to losing it, backup your data again by using latest Doris version.
    private int metaVersion = -1;

    public RestoreJob() {
        super(JobType.RESTORE);
    }

    public RestoreJob(String label, String backupTs, long dbId, String dbName, BackupJobInfo jobInfo,
            boolean allowLoad, int restoreReplicationNum, long timeoutMs, int metaVersion,
            Catalog catalog, long repoId) {
        super(JobType.RESTORE, label, dbId, dbName, timeoutMs, catalog, repoId);
        this.backupTimestamp = backupTs;
        this.jobInfo = jobInfo;
        this.allowLoad = allowLoad;
        this.restoreReplicationNum = restoreReplicationNum;
        this.state = RestoreJobState.PENDING;
        this.metaVersion = metaVersion;
    }

    public RestoreJobState getState() {
        return state;
    }

    public RestoreFileMapping getFileMapping() {
        return fileMapping;
    }
    
    public int getMetaVersion() {
        return metaVersion;
    }

    public synchronized boolean finishTabletSnapshotTask(SnapshotTask task, TFinishTaskRequest request) {
        if (checkTaskStatus(task, task.getJobId(), request)) {
            return false;
        }

        Preconditions.checkState(request.isSetSnapshotPath());

        // snapshot path does not contains last 'tablet_id' and 'schema_hash' dir
        // eg:
        // /path/to/your/be/data/snapshot/20180410102311.0/
        // Full path will look like:
        // /path/to/your/be/data/snapshot/20180410102311.0/10006/352781111/
        SnapshotInfo info = new SnapshotInfo(task.getDbId(), task.getTableId(), task.getPartitionId(),
                task.getIndexId(), task.getTabletId(), task.getBackendId(),
                task.getSchemaHash(), request.getSnapshotPath(), Lists.newArrayList());

        snapshotInfos.put(task.getTabletId(), task.getBackendId(), info);
        taskProgress.remove(task.getSignature());
        Long removedTabletId = unfinishedSignatureToId.remove(task.getSignature());
        if (removedTabletId != null) {
            taskErrMsg.remove(task.getSignature());
            Preconditions.checkState(task.getTabletId() == removedTabletId, removedTabletId);
            LOG.debug("get finished snapshot info: {}, unfinished tasks num: {}, remove result: {}. {}",
                      info, unfinishedSignatureToId.size(), this, removedTabletId);
            return true;
        }
        return false;
    }


    public synchronized boolean finishTabletDownloadTask(DownloadTask task, TFinishTaskRequest request) {
        if (checkTaskStatus(task, task.getJobId(), request)) {
            return false;
        }

        Preconditions.checkState(request.isSetDownloadedTabletIds());

        for (Long tabletId : request.getDownloadedTabletIds()) {
            SnapshotInfo info = snapshotInfos.get(tabletId, task.getBackendId());
            if (info == null) {
                LOG.error("failed to find snapshot infos of tablet {} in be {}, {}",
                          tabletId, task.getBackendId(), this);
                return false;
            }
        }

        taskProgress.remove(task.getSignature());
        Long beId = unfinishedSignatureToId.remove(task.getSignature());
        if (beId == null || beId != task.getBackendId()) {
            LOG.error("invalid download task: {}. {}", task, this);
            return false;
        }

        taskErrMsg.remove(task.getSignature());
        return true;
    }

    public synchronized boolean finishDirMoveTask(DirMoveTask task, TFinishTaskRequest request) {
        if (checkTaskStatus(task, task.getJobId(), request)) {
            return false;
        }

        taskProgress.remove(task.getSignature());
        Long tabletId = unfinishedSignatureToId.remove(task.getSignature());
        if (tabletId == null || tabletId != task.getTabletId()) {
            LOG.error("invalid dir move task: {}. {}", task, this);
            return false;
        }

        taskErrMsg.remove(task.getSignature());
        return true;
    }

    private boolean checkTaskStatus(AgentTask task, long jobId, TFinishTaskRequest request) {
        Preconditions.checkState(jobId == this.jobId);
        Preconditions.checkState(dbId == task.getDbId());

        if (request.getTaskStatus().getStatusCode() != TStatusCode.OK) {
            taskErrMsg.put(task.getSignature(), Joiner.on(",").join(request.getTaskStatus().getErrorMsgs()));
            return true;
        }
        return false;
    }

    @Override
    public synchronized void replayRun() {
        LOG.info("replay run restore job: {}", this);
        switch (state) {
            case DOWNLOAD:
                replayCheckAndPrepareMeta();
                break;
            case FINISHED:
                replayWaitingAllTabletsCommitted();
                break;
            default:
                break;
        }
    }

    @Override
    public synchronized void replayCancel() {
        cancelInternal(true /* is replay */);
    }

    @Override
    public boolean isPending() {
        return state == RestoreJobState.PENDING;
    }

    @Override
    public boolean isCancelled() {
        return state == RestoreJobState.CANCELLED;
    }

    @Override
    public void run() {
        if (state == RestoreJobState.FINISHED || state == RestoreJobState.CANCELLED) {
            return;
        }

        if (System.currentTimeMillis() - createTime > timeoutMs) {
            status = new Status(ErrCode.TIMEOUT, "restore job with label: " + label + "  timeout.");
            cancelInternal(false);
            return;
        }

        // get repo if not set
        if (repo == null) {
            repo = catalog.getBackupHandler().getRepoMgr().getRepo(repoId);
            if (repo == null) {
                status = new Status(ErrCode.COMMON_ERROR, "failed to get repository: " + repoId);
                cancelInternal(false);
                return;
            }
        }

        LOG.info("run restore job: {}", this);

        checkIfNeedCancel();

        if (status.ok()) {
            switch (state) {
                case PENDING:
                    checkAndPrepareMeta();
                    break;
                case SNAPSHOTING:
                    waitingAllSnapshotsFinished();
                    break;
                case DOWNLOAD:
                    downloadSnapshots();
                    break;
                case DOWNLOADING:
                    waitingAllDownloadFinished();
                    break;
                case COMMIT:
                    commit();
                    break;
                case COMMITTING:
                    waitingAllTabletsCommitted();
                    break;
                default:
                    break;
            }
        }

        if (!status.ok()) {
            cancelInternal(false);
        }
    }

    /**
     * return true if some restored objs have been dropped.
     */
    private void checkIfNeedCancel() {
        if (state == RestoreJobState.PENDING) {
            return;
        }

        Database db = catalog.getDb(dbId);
        if (db == null) {
            status = new Status(ErrCode.NOT_FOUND, "database " + dbId + " has been dropped");
            return;
        }

        db.readLock();
        try {
            for (IdChain idChain : fileMapping.getMapping().keySet()) {
                OlapTable tbl = (OlapTable) db.getTable(idChain.getTblId());
                if (tbl == null) {
                    status = new Status(ErrCode.NOT_FOUND, "table " + idChain.getTblId() + " has been dropped");
                    return;
                }
                tbl.readLock();
                try {
                    Partition part = tbl.getPartition(idChain.getPartId());
                    if (part == null) {
                        status = new Status(ErrCode.NOT_FOUND, "partition " + idChain.getPartId() + " has been dropped");
                        return;
                    }

                    MaterializedIndex index = part.getIndex(idChain.getIdxId());
                    if (index == null) {
                        status = new Status(ErrCode.NOT_FOUND, "index " + idChain.getIdxId() + " has been dropped");
                        return;
                    }
                } finally {
                    tbl.readUnlock();
                }
            }
        } finally {
            db.readUnlock();
        }
    }

    /**
     * Restore rules as follow:
     * A. Table already exist
     *      A1. Partition already exist, generate file mapping
     *      A2. Partition does not exist, add restored partition to the table.
     *          Reset all index/tablet/replica id, and create replica on BE outside the db lock.
     * B. Table does not exist
     *      B1. Add table to the db, reset all table/index/tablet/replica id, 
     *          and create replica on BE outside the db lock.
     *          
     * All newly created table/partition/index/tablet/replica should be saved for rolling back.
     * 
     * Step:
     * 1. download and deserialize backup meta from repository.
     * 2. set all existing restored table's state to RESTORE.
     * 3. check if the expected restore objs are valid.
     * 4. create replicas if necessary.
     * 5. add restored objs to catalog.
     * 6. make snapshot for all replicas for incremental download later.
     */
    private void checkAndPrepareMeta() {
        Database db = catalog.getDb(dbId);
        if (db == null) {
            status = new Status(ErrCode.NOT_FOUND, "database " + dbId + " does not exist");
            return;
        }

        // generate job id
        jobId = catalog.getNextId();

        // deserialize meta
        if (!downloadAndDeserializeMetaInfo()) {
            return;
        }
        Preconditions.checkNotNull(backupMeta);

        // Set all restored tbls' state to RESTORE
        // Table's origin state must be NORMAL and does not have unfinished load job.
        for (BackupTableInfo tblInfo : jobInfo.tables.values()) {
            Table tbl = db.getTable(jobInfo.getAliasByOriginNameIfSet(tblInfo.name));
            if (tbl == null) {
                continue;
            }
                
            if (tbl.getType() != TableType.OLAP) {
                status = new Status(ErrCode.COMMON_ERROR, "Only support retore OLAP table: " + tbl.getName());
                return;
            }
                
            OlapTable olapTbl = (OlapTable) tbl;
            olapTbl.writeLock();
            try {
                if (olapTbl.getState() != OlapTableState.NORMAL) {
                    status = new Status(ErrCode.COMMON_ERROR,
                            "Table " + tbl.getName() + "'s state is not NORMAL: " + olapTbl.getState().name());
                    return;
                }

                if (olapTbl.existTempPartitions()) {
                    status = new Status(ErrCode.COMMON_ERROR, "Do not support restoring table with temp partitions");
                    return;
                }

                for (Partition partition : olapTbl.getPartitions()) {
                    if (!catalog.getLoadInstance().checkPartitionLoadFinished(partition.getId(), null)) {
                        status = new Status(ErrCode.COMMON_ERROR,
                                "Table " + tbl.getName() + "'s has unfinished load job");
                        return;
                    }
                }

                olapTbl.setState(OlapTableState.RESTORE);
            } finally {
                olapTbl.writeUnlock();
            }

        }

        // Check and prepare meta objects.
        AgentBatchTask batchTask = new AgentBatchTask();
        db.readLock();
        try {
            for (BackupTableInfo tblInfo : jobInfo.tables.values()) {
                Table remoteTbl = backupMeta.getTable(tblInfo.name);
                Preconditions.checkNotNull(remoteTbl);
                Table localTbl = db.getTable(jobInfo.getAliasByOriginNameIfSet(tblInfo.name));
                if (localTbl != null) {
                    // table already exist, check schema
                    if (localTbl.getType() != TableType.OLAP) {
                        status = new Status(ErrCode.COMMON_ERROR,
                                "Only support retore olap table: " + localTbl.getName());
                        return;
                    }
                    OlapTable localOlapTbl = (OlapTable) localTbl;
                    OlapTable remoteOlapTbl = (OlapTable) remoteTbl;

                    localOlapTbl.readLock();
                    try {
                        List<String> intersectPartNames = Lists.newArrayList();
                        Status st = localOlapTbl.getIntersectPartNamesWith(remoteOlapTbl, intersectPartNames);
                        if (!st.ok()) {
                            status = st;
                            return;
                        }
                        LOG.debug("get intersect part names: {}, job: {}", intersectPartNames, this);
                        if (localOlapTbl.getSignature(BackupHandler.SIGNATURE_VERSION, intersectPartNames)
                                != remoteOlapTbl.getSignature(BackupHandler.SIGNATURE_VERSION, intersectPartNames)) {
                            status = new Status(ErrCode.COMMON_ERROR, "Table " + jobInfo.getAliasByOriginNameIfSet(tblInfo.name)
                                    + " already exist but with different schema");
                            return;
                        }

                        // Table with same name and has same schema. Check partition
                        for (BackupPartitionInfo backupPartInfo : tblInfo.partitions.values()) {
                            Partition localPartition = localOlapTbl.getPartition(backupPartInfo.name);
                            if (localPartition != null) {
                                // Partition already exist.
                                PartitionInfo localPartInfo = localOlapTbl.getPartitionInfo();
                                if (localPartInfo.getType() == PartitionType.RANGE) {
                                    // If this is a range partition, check range
                                    RangePartitionInfo localRangePartInfo = (RangePartitionInfo) localPartInfo;
                                    RangePartitionInfo remoteRangePartInfo
                                            = (RangePartitionInfo) remoteOlapTbl.getPartitionInfo();
                                    Range<PartitionKey> localRange = localRangePartInfo.getRange(localPartition.getId());
                                    Range<PartitionKey> remoteRange = remoteRangePartInfo.getRange(backupPartInfo.id);
                                    if (localRange.equals(remoteRange)) {
                                        // Same partition, same range
                                        if (genFileMappingWhenBackupReplicasEqual(localPartInfo, localPartition, localTbl, backupPartInfo, tblInfo)) {
                                            return;
                                        }
                                    } else {
                                        // Same partition name, different range
                                        status = new Status(ErrCode.COMMON_ERROR, "Partition " + backupPartInfo.name
                                                + " in table " + localTbl.getName()
                                                + " has different range with partition in repository");
                                        return;
                                    }
                                } else {
                                    // If this is a single partitioned table.
                                    if (genFileMappingWhenBackupReplicasEqual(localPartInfo, localPartition, localTbl, backupPartInfo, tblInfo)) {
                                        return;
                                    }
                                }
                            } else {
                                // partitions does not exist
                                PartitionInfo localPartitionInfo = localOlapTbl.getPartitionInfo();
                                if (localPartitionInfo.getType() == PartitionType.RANGE) {
                                    // Check if the partition range can be added to the table
                                    RangePartitionInfo localRangePartitionInfo = (RangePartitionInfo) localPartitionInfo;
                                    RangePartitionInfo remoteRangePartitionInfo
                                            = (RangePartitionInfo) remoteOlapTbl.getPartitionInfo();
                                    Range<PartitionKey> remoteRange = remoteRangePartitionInfo.getRange(backupPartInfo.id);
                                    if (localRangePartitionInfo.getAnyIntersectRange(remoteRange, false) != null) {
                                        status = new Status(ErrCode.COMMON_ERROR, "Partition " + backupPartInfo.name
                                                + " in table " + localTbl.getName()
                                                + " has conflict range with existing ranges");
                                        return;
                                    } else {
                                        // this partition can be added to this table, set ids
                                        Partition restorePart = resetPartitionForRestore(localOlapTbl, remoteOlapTbl,
                                                backupPartInfo.name,
                                                db.getClusterName(),
                                                restoreReplicationNum);
                                        if (restorePart == null) {
                                            return;
                                        }
                                        restoredPartitions.add(Pair.create(localOlapTbl.getName(), restorePart));
                                    }
                                } else {
                                    // It is impossible that a single partitioned table exist without any existing partition
                                    status = new Status(ErrCode.COMMON_ERROR,
                                            "No partition exist in single partitioned table " + localOlapTbl.getName());
                                    return;
                                }
                            }
                        }
                    } finally {
                        localOlapTbl.readUnlock();
                    }
                } else {
                    // Table does not exist
                    OlapTable remoteOlapTbl = (OlapTable) remoteTbl;
                    // Retain only expected restore partitions in this table;
                    Set<String> allPartNames = remoteOlapTbl.getPartitionNames();
                    for (String partName : allPartNames) {
                        if (!tblInfo.containsPart(partName)) {
                            remoteOlapTbl.dropPartition(-1 /* db id is useless here */, partName,
                                                        true /* act like replay to disable recycle bin action */);
                        }
                    }
                    
                    // reset all ids in this table
                    Status st = remoteOlapTbl.resetIdsForRestore(catalog, db, restoreReplicationNum);
                    if (!st.ok()) {
                        status = st;
                        return;
                    }
                    
                    // DO NOT set remote table's new name here, cause we will still need the origin name later
                    // remoteOlapTbl.setName(jobInfo.getAliasByOriginNameIfSet(tblInfo.name));
                    remoteOlapTbl.setState(allowLoad ? OlapTableState.RESTORE_WITH_LOAD : OlapTableState.RESTORE);
                    LOG.debug("put remote table {} to restoredTbls", remoteOlapTbl.getName());
                    restoredTbls.add(remoteOlapTbl);
                }
            } // end of all restore tables

            LOG.debug("finished to prepare restored partitions and tables. {}", this);
            // for now, nothing is modified in catalog

            // generate create replica tasks for all restored partitions
            for (Pair<String, Partition> entry : restoredPartitions) {
                OlapTable localTbl = (OlapTable) db.getTable(entry.first);
                Preconditions.checkNotNull(localTbl, localTbl.getName());
                Partition restorePart = entry.second;
                OlapTable remoteTbl = (OlapTable) backupMeta.getTable(entry.first);
                BackupPartitionInfo backupPartitionInfo 
                        = jobInfo.getTableInfo(entry.first).getPartInfo(restorePart.getName());

                createReplicas(db, batchTask, localTbl, restorePart);

                genFileMapping(localTbl, restorePart, remoteTbl.getId(), backupPartitionInfo,
                               !allowLoad /* if allow load, do not overwrite when commit */);
            }

            // generate create replica task for all restored tables
            for (OlapTable restoreTbl : restoredTbls) {
                for (Partition restorePart : restoreTbl.getPartitions()) {
                    createReplicas(db, batchTask, restoreTbl, restorePart);
                    BackupTableInfo backupTableInfo = jobInfo.getTableInfo(restoreTbl.getName());
                    genFileMapping(restoreTbl, restorePart, backupTableInfo.id,
                                   backupTableInfo.getPartInfo(restorePart.getName()),
                                   !allowLoad /* if allow load, do not overwrite when commit */);
                }
                // set restored table's new name after all 'genFileMapping'
                restoreTbl.setName(jobInfo.getAliasByOriginNameIfSet(restoreTbl.getName()));
            }

            LOG.debug("finished to generate create replica tasks. {}", this);
        } finally {
            db.readUnlock();
        }

        // Send create replica task to BE outside the db lock
        if (batchTask.getTaskNum() > 0) {
            MarkedCountDownLatch<Long, Long> latch = new MarkedCountDownLatch<Long, Long>(batchTask.getTaskNum());
            for (AgentTask task : batchTask.getAllTasks()) {
                latch.addMark(task.getBackendId(), task.getTabletId());
                ((CreateReplicaTask) task).setLatch(latch);
                AgentTaskQueue.addTask(task);
            }
            AgentTaskExecutor.submit(batchTask);

            // estimate timeout, at most 10 min
            long timeout = Config.tablet_create_timeout_second * 1000L * batchTask.getTaskNum();
            timeout = Math.min(10 * 60 * 1000, timeout);
            boolean ok = false;
            try {
                LOG.info("begin to send create replica tasks to BE for restore. total {} tasks. timeout: {}",
                         batchTask.getTaskNum(), timeout);
                ok = latch.await(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOG.warn("InterruptedException: ", e);
                ok = false;
            }
            
            if (ok) {
                LOG.debug("finished to create all restored replcias. {}", this);
                // add restored partitions.
                // table should be in State RESTORE, so no other partitions can be
                // added to or removed from this table during the restore process.
                for (Pair<String, Partition> entry : restoredPartitions) {
                    OlapTable localTbl = (OlapTable) db.getTable(entry.first);
                    localTbl.writeLock();
                    try {
                        Partition restoredPart = entry.second;
                        OlapTable remoteTbl = (OlapTable) backupMeta.getTable(entry.first);
                        RangePartitionInfo localPartitionInfo = (RangePartitionInfo) localTbl.getPartitionInfo();
                        RangePartitionInfo remotePartitionInfo = (RangePartitionInfo) remoteTbl.getPartitionInfo();
                        BackupPartitionInfo backupPartitionInfo
                                = jobInfo.getTableInfo(entry.first).getPartInfo(restoredPart.getName());
                        long remotePartId = backupPartitionInfo.id;
                        Range<PartitionKey> remoteRange = remotePartitionInfo.getRange(remotePartId);
                        DataProperty remoteDataProperty = remotePartitionInfo.getDataProperty(remotePartId);
                        localPartitionInfo.addPartition(restoredPart.getId(), false, remoteRange,
                                remoteDataProperty, (short) restoreReplicationNum,
                                remotePartitionInfo.getIsInMemory(remotePartId));
                        localTbl.addPartition(restoredPart);
                    } finally {
                        localTbl.writeUnlock();
                    }

                }

                // add restored tables
                for (OlapTable tbl : restoredTbls) {
                    db.writeLock();
                    try {
                        if (!db.createTable(tbl)) {
                            status = new Status(ErrCode.COMMON_ERROR, "Table " + tbl.getName()
                                    + " already exist in db: " + db.getFullName());
                            return;
                        }
                    } finally {
                        db.writeUnlock();
                    }

                }
            } else {
                List<Entry<Long, Long>> unfinishedMarks = latch.getLeftMarks();
                // only show at most 10 results
                List<Entry<Long, Long>> subList = unfinishedMarks.subList(0, Math.min(unfinishedMarks.size(), 10));
                String idStr = Joiner.on(", ").join(subList);
                status = new Status(ErrCode.COMMON_ERROR,
                        "Failed to create replicas for restore. unfinished marks: " + idStr);
                return;
            }
        }

        LOG.info("finished to prepare meta. begin to make snapshot. {}", this);

        // begin to make snapshots for all replicas
        // snapshot is for incremental download
        unfinishedSignatureToId.clear();
        taskProgress.clear();
        taskErrMsg.clear();
        Multimap<Long, Long> bePathsMap = HashMultimap.create();
        batchTask = new AgentBatchTask();
        db.readLock();
        try {
            for (IdChain idChain : fileMapping.getMapping().keySet()) {
                OlapTable tbl = (OlapTable) db.getTable(idChain.getTblId());
                tbl.readLock();
                try {
                    Partition part = tbl.getPartition(idChain.getPartId());
                    MaterializedIndex index = part.getIndex(idChain.getIdxId());
                    Tablet tablet = index.getTablet(idChain.getTabletId());
                    Replica replica = tablet.getReplicaById(idChain.getReplicaId());
                    long signature = catalog.getNextId();
                    SnapshotTask task = new SnapshotTask(null, replica.getBackendId(), signature,
                            jobId, db.getId(),
                            tbl.getId(), part.getId(), index.getId(), tablet.getId(),
                            part.getVisibleVersion(), part.getVisibleVersionHash(),
                            tbl.getSchemaHashByIndexId(index.getId()), timeoutMs,
                            true /* is restore task*/);
                    batchTask.addTask(task);
                    unfinishedSignatureToId.put(signature, tablet.getId());
                    bePathsMap.put(replica.getBackendId(), replica.getPathHash());
                } finally {
                    tbl.readUnlock();
                }
            }
        } finally {
            db.readUnlock();
        }
        
        // check disk capacity
        org.apache.doris.common.Status st = Catalog.getCurrentSystemInfo().checkExceedDiskCapacityLimit(bePathsMap, true);
        if (!st.ok()) {
            status = new Status(ErrCode.COMMON_ERROR, st.getErrorMsg());
            return;
        }

        // send tasks
        for (AgentTask task : batchTask.getAllTasks()) {
            AgentTaskQueue.addTask(task);
        }
        AgentTaskExecutor.submit(batchTask);

        metaPreparedTime = System.currentTimeMillis();
        state = RestoreJobState.SNAPSHOTING;

        // No log here, PENDING state restore job will redo this method
        LOG.info("finished to prepare meta and send snapshot tasks, num: {}. {}",
                 batchTask.getTaskNum(), this);
    }

    private boolean genFileMappingWhenBackupReplicasEqual(PartitionInfo localPartInfo, Partition localPartition, Table localTbl,
                                                          BackupPartitionInfo backupPartInfo, BackupTableInfo tblInfo) {
        if (localPartInfo.getReplicationNum(localPartition.getId()) != restoreReplicationNum) {
            status = new Status(ErrCode.COMMON_ERROR, "Partition " + backupPartInfo.name
                    + " in table " + localTbl.getName()
                    + " has different replication num '"
                    + localPartInfo.getReplicationNum(localPartition.getId())
                    + "' with partition in repository, which is " + restoreReplicationNum);
            return true;
        }

        // No need to check range, just generate file mapping
        OlapTable localOlapTbl = (OlapTable) localTbl;
        genFileMapping(localOlapTbl, localPartition, tblInfo.id, backupPartInfo,
                true /* overwrite when commit */);
        restoredVersionInfo.put(localOlapTbl.getId(), localPartition.getId(),
                Pair.create(backupPartInfo.version,
                        backupPartInfo.versionHash));
        return false;
    }

    private void createReplicas(Database db, AgentBatchTask batchTask, OlapTable localTbl, Partition restorePart) {
        Set<String> bfColumns = localTbl.getCopiedBfColumns();
        double bfFpp = localTbl.getBfFpp();
        for (MaterializedIndex restoredIdx : restorePart.getMaterializedIndices(IndexExtState.VISIBLE)) {
            MaterializedIndexMeta indexMeta = localTbl.getIndexMetaByIndexId(restoredIdx.getId());
            TabletMeta tabletMeta = new TabletMeta(db.getId(), localTbl.getId(), restorePart.getId(),
                    restoredIdx.getId(), indexMeta.getSchemaHash(), TStorageMedium.HDD);
            for (Tablet restoreTablet : restoredIdx.getTablets()) {
                Catalog.getCurrentInvertedIndex().addTablet(restoreTablet.getId(), tabletMeta);
                for (Replica restoreReplica : restoreTablet.getReplicas()) {
                    Catalog.getCurrentInvertedIndex().addReplica(restoreTablet.getId(), restoreReplica);
                    CreateReplicaTask task = new CreateReplicaTask(restoreReplica.getBackendId(), dbId,
                            localTbl.getId(), restorePart.getId(), restoredIdx.getId(),
                            restoreTablet.getId(), indexMeta.getShortKeyColumnCount(),
                            indexMeta.getSchemaHash(), restoreReplica.getVersion(),
                            restoreReplica.getVersionHash(), indexMeta.getKeysType(), TStorageType.COLUMN,
                            TStorageMedium.HDD /* all restored replicas will be saved to HDD */,
                            indexMeta.getSchema(), bfColumns, bfFpp, null,
                            localTbl.getCopiedIndexes(),
                            localTbl.isInMemory(),
                            localTbl.getPartitionInfo().getTabletType(restorePart.getId()));
                    task.setInRestoreMode(true);
                    batchTask.addTask(task);
                }
            }
        }
    }

    // reset remote partition.
    // reset all id in remote partition, but DO NOT modify any exist catalog objects.
    private Partition resetPartitionForRestore(OlapTable localTbl, OlapTable remoteTbl, String partName,
            String clusterName, int restoreReplicationNum) {
        Preconditions.checkState(localTbl.getPartition(partName) == null);
        Partition remotePart = remoteTbl.getPartition(partName);
        Preconditions.checkNotNull(remotePart);
        PartitionInfo localPartitionInfo = localTbl.getPartitionInfo();
        Preconditions.checkState(localPartitionInfo.getType() == PartitionType.RANGE);

        // generate new partition id
        long newPartId = catalog.getNextId();
        remotePart.setIdForRestore(newPartId);

        // indexes
        Map<String, Long> localIdxNameToId = localTbl.getIndexNameToId();
        for (String localIdxName : localIdxNameToId.keySet()) {
            // set ids of indexes in remote partition to the local index ids
            long remoteIdxId = remoteTbl.getIndexIdByName(localIdxName);
            MaterializedIndex remoteIdx = remotePart.getIndex(remoteIdxId);
            long localIdxId = localIdxNameToId.get(localIdxName);
            remoteIdx.setIdForRestore(localIdxId);
            if (localIdxId != localTbl.getBaseIndexId()) {
                // not base table, reset
                remotePart.deleteRollupIndex(remoteIdxId);
                remotePart.createRollupIndex(remoteIdx);
            }
        }

        // save version info for creating replicas
        long visibleVersion = remotePart.getVisibleVersion();
        long visibleVersionHash = remotePart.getVisibleVersionHash();

        // tablets
        for (MaterializedIndex remoteIdx : remotePart.getMaterializedIndices(IndexExtState.VISIBLE)) {
            int schemaHash = remoteTbl.getSchemaHashByIndexId(remoteIdx.getId());
            int remotetabletSize = remoteIdx.getTablets().size();
            remoteIdx.clearTabletsForRestore();
            for (int i = 0; i < remotetabletSize; i++) {
                // generate new tablet id
                long newTabletId = catalog.getNextId();
                Tablet newTablet = new Tablet(newTabletId);
                // add tablet to index, but not add to TabletInvertedIndex
                remoteIdx.addTablet(newTablet, null /* tablet meta */, true /* is restore */);

                // replicas
                List<Long> beIds = Catalog.getCurrentSystemInfo().seqChooseBackendIds(restoreReplicationNum, true,
                                                                                      true, clusterName);
                if (beIds == null) {
                    status = new Status(ErrCode.COMMON_ERROR,
                            "failed to get enough backends for creating replica of tablet "
                                    + newTabletId + ". need: " + restoreReplicationNum);
                    return null;
                }
                for (Long beId : beIds) {
                    long newReplicaId = catalog.getNextId();
                    Replica newReplica = new Replica(newReplicaId, beId, ReplicaState.NORMAL,
                            visibleVersion, visibleVersionHash, schemaHash);
                    newTablet.addReplica(newReplica, true /* is restore */);
                }
            }
        }
        return remotePart;
    }

    // files in repo to files in local
    private void genFileMapping(OlapTable localTbl, Partition localPartition, Long remoteTblId,
            BackupPartitionInfo backupPartInfo, boolean overwrite) {
        for (MaterializedIndex localIdx : localPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
            LOG.debug("get index id: {}, index name: {}", localIdx.getId(),
                    localTbl.getIndexNameById(localIdx.getId()));
            BackupIndexInfo backupIdxInfo = backupPartInfo.getIdx(localTbl.getIndexNameById(localIdx.getId()));
            Preconditions.checkState(backupIdxInfo.tablets.size() == localIdx.getTablets().size());
            for (int i = 0; i < localIdx.getTablets().size(); i++) {
                Tablet localTablet = localIdx.getTablets().get(i);
                BackupTabletInfo backupTabletInfo = backupIdxInfo.tablets.get(i);
                LOG.debug("get tablet mapping: {} to {}, index {}", backupTabletInfo.id, localTablet.getId(), i);
                for (Replica localReplica : localTablet.getReplicas()) {
                    IdChain src = new IdChain(remoteTblId, backupPartInfo.id, backupIdxInfo.id, backupTabletInfo.id,
                            -1L /* no replica id */);
                    IdChain dest = new IdChain(localTbl.getId(), localPartition.getId(),
                            localIdx.getId(), localTablet.getId(), localReplica.getId());
                    fileMapping.putMapping(dest, src, overwrite);
                }
            }
        }
    }

    private boolean downloadAndDeserializeMetaInfo() {
        List<BackupMeta> backupMetas = Lists.newArrayList();
        Status st = repo.getSnapshotMetaFile(jobInfo.name, backupMetas,
                this.metaVersion == -1 ? jobInfo.metaVersion : this.metaVersion);
        if (!st.ok()) {
            status = st;
            return false;
        }
        Preconditions.checkState(backupMetas.size() == 1);
        backupMeta = backupMetas.get(0);
        return true;
    }

    private void replayCheckAndPrepareMeta() {
        Database db = catalog.getDb(dbId);

        // replay set all existing tables's state to RESTORE
        for (BackupTableInfo tblInfo : jobInfo.tables.values()) {
            Table tbl = db.getTable(jobInfo.getAliasByOriginNameIfSet(tblInfo.name));
            if (tbl == null) {
                continue;
            }
            OlapTable olapTbl = (OlapTable) tbl;
            tbl.writeLock();
            try {
                olapTbl.setState(OlapTableState.RESTORE);
            } finally {
                tbl.writeUnlock();
            }
        }

        // restored partitions
        for (Pair<String, Partition> entry : restoredPartitions) {
            OlapTable localTbl = (OlapTable) db.getTable(entry.first);
            Partition restorePart = entry.second;
            OlapTable remoteTbl = (OlapTable) backupMeta.getTable(entry.first);
            RangePartitionInfo localPartitionInfo = (RangePartitionInfo) localTbl.getPartitionInfo();
            RangePartitionInfo remotePartitionInfo = (RangePartitionInfo) remoteTbl.getPartitionInfo();
            BackupPartitionInfo backupPartitionInfo = jobInfo.getTableInfo(entry.first).getPartInfo(restorePart.getName());
            long remotePartId = backupPartitionInfo.id;
            Range<PartitionKey> remoteRange = remotePartitionInfo.getRange(remotePartId);
            DataProperty remoteDataProperty = remotePartitionInfo.getDataProperty(remotePartId);
            localPartitionInfo.addPartition(restorePart.getId(), false, remoteRange,
                    remoteDataProperty, (short) restoreReplicationNum,
                    remotePartitionInfo.getIsInMemory(remotePartId));
            localTbl.addPartition(restorePart);

            // modify tablet inverted index
            for (MaterializedIndex restoreIdx : restorePart.getMaterializedIndices(IndexExtState.VISIBLE)) {
                int schemaHash = localTbl.getSchemaHashByIndexId(restoreIdx.getId());
                TabletMeta tabletMeta = new TabletMeta(db.getId(), localTbl.getId(), restorePart.getId(),
                        restoreIdx.getId(), schemaHash, TStorageMedium.HDD);
                for (Tablet restoreTablet : restoreIdx.getTablets()) {
                    Catalog.getCurrentInvertedIndex().addTablet(restoreTablet.getId(), tabletMeta);
                    for (Replica restoreReplica : restoreTablet.getReplicas()) {
                        Catalog.getCurrentInvertedIndex().addReplica(restoreTablet.getId(), restoreReplica);
                    }
                }
            }
        }

        // restored tables
        for (OlapTable restoreTbl : restoredTbls) {
            db.writeLock();
            try {
                db.createTable(restoreTbl);
            } finally {
                db.writeUnlock();
            }
            restoreTbl.writeLock();
            try {
                // modify tablet inverted index
                for (Partition restorePart : restoreTbl.getPartitions()) {
                    for (MaterializedIndex restoreIdx : restorePart.getMaterializedIndices(IndexExtState.VISIBLE)) {
                        int schemaHash = restoreTbl.getSchemaHashByIndexId(restoreIdx.getId());
                        TabletMeta tabletMeta = new TabletMeta(db.getId(), restoreTbl.getId(), restorePart.getId(),
                                restoreIdx.getId(), schemaHash, TStorageMedium.HDD);
                        for (Tablet restoreTablet : restoreIdx.getTablets()) {
                            Catalog.getCurrentInvertedIndex().addTablet(restoreTablet.getId(), tabletMeta);
                            for (Replica restoreReplica : restoreTablet.getReplicas()) {
                                Catalog.getCurrentInvertedIndex().addReplica(restoreTablet.getId(), restoreReplica);
                            }
                        }
                    }
                }
            } finally {
                restoreTbl.writeUnlock();
            }
        }
        LOG.info("replay check and prepare meta. {}", this);
    }

    private void waitingAllSnapshotsFinished() {
        if (unfinishedSignatureToId.isEmpty()) {
            snapshotFinishedTime = System.currentTimeMillis();
            state = RestoreJobState.DOWNLOAD;

            catalog.getEditLog().logRestoreJob(this);
            LOG.info("finished making snapshots. {}", this);
            return;
        }

        LOG.info("waiting {} replicas to make snapshot: [{}]. {}",
                 unfinishedSignatureToId.size(), unfinishedSignatureToId, this);
        return;
    }

    private void downloadSnapshots() {
        // Categorize snapshot infos by db id.
        ArrayListMultimap<Long, SnapshotInfo> dbToSnapshotInfos = ArrayListMultimap.create();
        for (SnapshotInfo info : snapshotInfos.values()) {
            dbToSnapshotInfos.put(info.getDbId(), info);
        }

        // Send download tasks
        unfinishedSignatureToId.clear();
        taskProgress.clear();
        taskErrMsg.clear();
        AgentBatchTask batchTask = new AgentBatchTask();
        for (long dbId : dbToSnapshotInfos.keySet()) {
            List<SnapshotInfo> infos = dbToSnapshotInfos.get(dbId);

            Database db = catalog.getDb(dbId);
            if (db == null) {
                status = new Status(ErrCode.NOT_FOUND, "db " + dbId + " does not exist");
                return;
            }

            // We classify the snapshot info by backend
            ArrayListMultimap<Long, SnapshotInfo> beToSnapshots = ArrayListMultimap.create();
            for (SnapshotInfo info : infos) {
                beToSnapshots.put(info.getBeId(), info);
            }

            db.readLock();
            try {
                for (Long beId : beToSnapshots.keySet()) {
                    List<SnapshotInfo> beSnapshotInfos = beToSnapshots.get(beId);
                    int totalNum = beSnapshotInfos.size();
                    // each backend allot at most 3 tasks
                    int batchNum = Math.min(totalNum, 3);
                    // each task contains several upload sub tasks
                    int taskNumPerBatch = Math.max(totalNum / batchNum, 1);
                    LOG.debug("backend {} has {} batch, total {} tasks, {}",
                              beId, batchNum, totalNum, this);

                    List<FsBroker> brokerAddrs = Lists.newArrayList();
                    Status st = repo.getBrokerAddress(beId, catalog, brokerAddrs);
                    if (!st.ok()) {
                        status = st;
                        return;
                    }
                    Preconditions.checkState(brokerAddrs.size() == 1);

                    // allot tasks
                    int index = 0;
                    for (int batch = 0; batch < batchNum; batch++) {
                        Map<String, String> srcToDest = Maps.newHashMap();
                        int currentBatchTaskNum = (batch == batchNum - 1) ? totalNum - index : taskNumPerBatch;
                        for (int j = 0; j < currentBatchTaskNum; j++) {
                            SnapshotInfo info = beSnapshotInfos.get(index++);
                            Table tbl = db.getTable(info.getTblId());
                            if (tbl == null) {
                                status = new Status(ErrCode.NOT_FOUND, "restored table "
                                        + info.getTabletId() + " does not exist");
                                return;
                            }
                            OlapTable olapTbl = (OlapTable) tbl;
                            olapTbl.readLock();
                            try {
                                Partition part = olapTbl.getPartition(info.getPartitionId());
                                if (part == null) {
                                    status = new Status(ErrCode.NOT_FOUND, "partition "
                                            + info.getPartitionId() + " does not exist in restored table: "
                                            + tbl.getName());
                                    return;
                                }

                                MaterializedIndex idx = part.getIndex(info.getIndexId());
                                if (idx == null) {
                                    status = new Status(ErrCode.NOT_FOUND,
                                            "index " + info.getIndexId() + " does not exist in partion " + part.getName()
                                                    + "of restored table " + tbl.getName());
                                    return;
                                }

                                Tablet tablet  = idx.getTablet(info.getTabletId());
                                if (tablet == null) {
                                    status = new Status(ErrCode.NOT_FOUND,
                                            "tablet " + info.getTabletId() + " does not exist in restored table "
                                                    + tbl.getName());
                                    return;
                                }

                                Replica replica = tablet.getReplicaByBackendId(info.getBeId());
                                if (replica == null) {
                                    status = new Status(ErrCode.NOT_FOUND,
                                            "replica in be " + info.getBeId() + " of tablet "
                                                    + tablet.getId() + " does not exist in restored table "
                                                    + tbl.getName());
                                    return;
                                }

                                IdChain catalogIds = new IdChain(tbl.getId(), part.getId(), idx.getId(),
                                        info.getTabletId(), replica.getId());
                                IdChain repoIds = fileMapping.get(catalogIds);
                                if (repoIds == null) {
                                    status = new Status(ErrCode.NOT_FOUND,
                                            "failed to get id mapping of catalog ids: " + catalogIds.toString());
                                    LOG.info("current file mapping: {}", fileMapping);
                                    return;
                                }

                                String repoTabletPath = jobInfo.getFilePath(repoIds);
                                // eg:
                                // bos://location/__palo_repository_my_repo/_ss_my_ss/_ss_content/__db_10000/
                                // __tbl_10001/__part_10002/_idx_10001/__10003
                                String src = repo.getRepoPath(label, repoTabletPath);
                                SnapshotInfo snapshotInfo = snapshotInfos.get(info.getTabletId(), info.getBeId());
                                Preconditions.checkNotNull(snapshotInfo, info.getTabletId() + "-" + info.getBeId());
                                // download to previous exist snapshot dir
                                String dest = snapshotInfo.getTabletPath();
                                srcToDest.put(src, dest);
                                LOG.debug("create download src path: {}, dest path: {}", src, dest);

                            } finally {
                                olapTbl.readUnlock();
                            }
                        }
                        long signature = catalog.getNextId();
                        DownloadTask task = new DownloadTask(null, beId, signature, jobId, dbId,
                                srcToDest, brokerAddrs.get(0), repo.getStorage().getProperties());
                        batchTask.addTask(task);
                        unfinishedSignatureToId.put(signature, beId);
                    }
                }
            } finally {
                db.readUnlock();
            }
        }

        // send task
        for (AgentTask task : batchTask.getAllTasks()) {
            AgentTaskQueue.addTask(task);
        }
        AgentTaskExecutor.submit(batchTask);

        state = RestoreJobState.DOWNLOADING;

        // No edit log here
        LOG.info("finished to send download tasks to BE. num: {}. {}", batchTask.getTaskNum(), this);
    }

    private void waitingAllDownloadFinished() {
        if (unfinishedSignatureToId.isEmpty()) {
            downloadFinishedTime = System.currentTimeMillis();
            state = RestoreJobState.COMMIT;

            // backupMeta is useless now
            backupMeta = null;

            catalog.getEditLog().logRestoreJob(this);
            LOG.info("finished to download. {}", this);
        }

        LOG.info("waiting {} tasks to finish downloading from repo. {}", unfinishedSignatureToId.size(), this);
    }

    private void commit() {
        // Send task to move the download dir
        unfinishedSignatureToId.clear();
        taskProgress.clear();
        taskErrMsg.clear();
        AgentBatchTask batchTask = new AgentBatchTask();
        // tablet id->(be id -> download info)
        for (Cell<Long, Long, SnapshotInfo> cell : snapshotInfos.cellSet()) {
            SnapshotInfo info = cell.getValue();
            long signature = catalog.getNextId();
            DirMoveTask task = new DirMoveTask(null, cell.getColumnKey(), signature, jobId, dbId,
                    info.getTblId(), info.getPartitionId(), info.getTabletId(),
                    cell.getRowKey(), info.getTabletPath(), info.getSchemaHash(),
                    true /* need reload tablet header */);
            batchTask.addTask(task);
            unfinishedSignatureToId.put(signature, info.getTabletId());
        }

        // send task
        for (AgentTask task : batchTask.getAllTasks()) {
            AgentTaskQueue.addTask(task);
        }
        AgentTaskExecutor.submit(batchTask);

        state = RestoreJobState.COMMITTING;

        // No log here
        LOG.info("finished to send move dir tasks. num: {}. {}", batchTask.getTaskNum(), this);
        return;
    }

    private void waitingAllTabletsCommitted() {
        if (unfinishedSignatureToId.isEmpty()) {
            LOG.info("finished to commit all tablet. {}", this);
            Status st = allTabletCommitted(false /* not replay */);
            if (!st.ok()) {
                status = st;
            }
            return;
        }
        LOG.info("waiting {} tablets to commit. {}", unfinishedSignatureToId.size(), this);
    }

    private Status allTabletCommitted(boolean isReplay) {
        Database db = catalog.getDb(dbId);
        if (db == null) {
            return new Status(ErrCode.NOT_FOUND, "database " + dbId + " does not exist");
        }

        // set all restored partition version and version hash
        // set all tables' state to NORMAL
        setTableStateToNormal(db);
        for (long tblId : restoredVersionInfo.rowKeySet()) {
            Table tbl = db.getTable(tblId);
            if (tbl == null) {
                continue;
            }
            OlapTable olapTbl = (OlapTable) tbl;
            tbl.writeLock();
            try {
                Map<Long, Pair<Long, Long>> map = restoredVersionInfo.rowMap().get(tblId);
                for (Map.Entry<Long, Pair<Long, Long>> entry : map.entrySet()) {
                    long partId = entry.getKey();
                    Partition part = olapTbl.getPartition(partId);
                    if (part == null) {
                        continue;
                    }

                    // update partition visible version
                    part.updateVersionForRestore(entry.getValue().first, entry.getValue().second);

                    // we also need to update the replica version of these overwritten restored partitions
                    for (MaterializedIndex idx : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
                        for (Tablet tablet : idx.getTablets()) {
                            for (Replica replica : tablet.getReplicas()) {
                                if (!replica.checkVersionCatchUp(part.getVisibleVersion(),
                                        part.getVisibleVersionHash(), false)) {
                                    replica.updateVersionInfo(part.getVisibleVersion(), part.getVisibleVersionHash(),
                                            replica.getDataSize(), replica.getRowCount());
                                }
                            }
                        }
                    }

                    LOG.debug("restore set partition {} version in table {}, version: {}, version hash: {}",
                            partId, tblId, entry.getValue().first, entry.getValue().second);
                }
            } finally {
                tbl.writeUnlock();
            }
        }

        if (!isReplay) {
            restoredPartitions.clear();
            restoredTbls.clear();

            // release snapshot before clearing snapshotInfos
            releaseSnapshots();

            snapshotInfos.clear();

            finishedTime = System.currentTimeMillis();
            state = RestoreJobState.FINISHED;

            catalog.getEditLog().logRestoreJob(this);
        }
        
        LOG.info("job is finished. is replay: {}. {}", isReplay, this);
        return Status.OK;
    }

    private void releaseSnapshots() {
        if (snapshotInfos.isEmpty()) {
            return;
        }
        // we do not care about the release snapshot tasks' success or failure,
        // the GC thread on BE will sweep the snapshot, finally.
        AgentBatchTask batchTask = new AgentBatchTask();
        for (SnapshotInfo info : snapshotInfos.values()) {
            ReleaseSnapshotTask releaseTask = new ReleaseSnapshotTask(null, info.getBeId(), info.getDbId(),
                    info.getTabletId(), info.getPath());
            batchTask.addTask(releaseTask);
        }
        AgentTaskExecutor.submit(batchTask);
        LOG.info("send {} release snapshot tasks, job: {}", snapshotInfos.size(), this);
    }

    private void replayWaitingAllTabletsCommitted() {
        allTabletCommitted(true /* is replay */);
    }

    public List<String> getInfo() {
        List<String> info = Lists.newArrayList();
        info.add(String.valueOf(jobId));
        info.add(label);
        info.add(backupTimestamp);
        info.add(dbName);
        info.add(state.name());
        info.add(String.valueOf(allowLoad));
        info.add(String.valueOf(restoreReplicationNum));
        info.add(getRestoreObjs());
        info.add(TimeUtils.longToTimeString(createTime));
        info.add(TimeUtils.longToTimeString(metaPreparedTime));
        info.add(TimeUtils.longToTimeString(snapshotFinishedTime));
        info.add(TimeUtils.longToTimeString(downloadFinishedTime));
        info.add(TimeUtils.longToTimeString(finishedTime));
        info.add(Joiner.on(", ").join(unfinishedSignatureToId.entrySet()));
        info.add(Joiner.on(", ").join(taskProgress.entrySet().stream().map(
                e -> "[" + e.getKey() + ": " + e.getValue().first + "/" + e.getValue().second + "]").collect(
                        Collectors.toList())));
        info.add(Joiner.on(", ").join(taskErrMsg.entrySet().stream().map(n -> "[" + n.getKey() + ": " + n.getValue()
                + "]").collect(Collectors.toList())));
        info.add(status.toString());
        info.add(String.valueOf(timeoutMs / 1000));
        return info;
    }

    private String getRestoreObjs() {
        Preconditions.checkState(jobInfo != null);
        return jobInfo.getInfo();
    }

    @Override
    public boolean isDone() {
        if (state == RestoreJobState.FINISHED || state == RestoreJobState.CANCELLED) {
            return true;
        }
        return false;
    }

    // cancel by user
    @Override
    public synchronized Status cancel() {
        if (isDone()) {
            return new Status(ErrCode.COMMON_ERROR,
                    "Job with label " + label + " can not be cancelled. state: " + state);
        }

        status = new Status(ErrCode.COMMON_ERROR, "user cancelled, current state: " + state.name());
        cancelInternal(false);
        return Status.OK;
    }

    public void cancelInternal(boolean isReplay) {
        // We need to clean the residual due to current state
        if (!isReplay) {
            switch (state) {
                case SNAPSHOTING:
                    // remove all snapshot tasks in AgentTaskQueue
                    for (Long taskId : unfinishedSignatureToId.keySet()) {
                        AgentTaskQueue.removeTaskOfType(TTaskType.MAKE_SNAPSHOT, taskId);
                    }
                    break;
                case DOWNLOADING:
                    // remove all down tasks in AgentTaskQueue
                    for (Long taskId : unfinishedSignatureToId.keySet()) {
                        AgentTaskQueue.removeTaskOfType(TTaskType.DOWNLOAD, taskId);
                    }
                    break;
                case COMMITTING:
                    // remove all dir move tasks in AgentTaskQueue
                    for (Long taskId : unfinishedSignatureToId.keySet()) {
                        AgentTaskQueue.removeTaskOfType(TTaskType.MOVE, taskId);
                    }
                    break;
                default:
                    break;
            }
        }

        // clean restored objs
        Database db = catalog.getDb(dbId);
        if (db != null) {
            // rollback table's state to NORMAL
            setTableStateToNormal(db);

            // remove restored tbls
            for (OlapTable restoreTbl : restoredTbls) {
                LOG.info("remove restored table when cancelled: {}", restoreTbl.getName());
                restoreTbl.writeLock();
                try {
                    for (Partition part : restoreTbl.getPartitions()) {
                        for (MaterializedIndex idx : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            for (Tablet tablet : idx.getTablets()) {
                                Catalog.getCurrentInvertedIndex().deleteTablet(tablet.getId());
                            }
                        }
                    }
                } finally {
                    restoreTbl.writeUnlock();
                }
                db.writeLock();
                try {
                    db.dropTable(restoreTbl.getName());
                } finally {
                    db.writeUnlock();
                }
            }

            // remove restored partitions
            for (Pair<String, Partition> entry : restoredPartitions) {
                OlapTable restoreTbl = (OlapTable) db.getTable(entry.first);
                if (restoreTbl == null) {
                    continue;
                }
                LOG.info("remove restored partition in table {} when cancelled: {}",
                        restoreTbl.getName(), entry.second.getName());
                restoreTbl.writeLock();
                try {
                    for (MaterializedIndex idx : entry.second.getMaterializedIndices(IndexExtState.VISIBLE)) {
                        for (Tablet tablet : idx.getTablets()) {
                            Catalog.getCurrentInvertedIndex().deleteTablet(tablet.getId());
                        }
                    }

                    restoreTbl.dropPartition(dbId, entry.second.getName(), true /* is restore */);
                } finally {
                    restoreTbl.writeUnlock();
                }

            }
        }

        if (!isReplay) {
            // backupMeta is useless
            backupMeta = null;

            releaseSnapshots();

            snapshotInfos.clear();
            RestoreJobState curState = state;
            finishedTime = System.currentTimeMillis();
            state = RestoreJobState.CANCELLED;
            // log
            catalog.getEditLog().logRestoreJob(this);

            LOG.info("finished to cancel restore job. current state: {}. is replay: {}. {}",
                     curState.name(), isReplay, this);
            return;
        }

        LOG.info("finished to cancel restore job. is replay: {}. {}", isReplay, this);
    }

    private void setTableStateToNormal(Database db) {
        for (BackupTableInfo tblInfo : jobInfo.tables.values()) {
            Table tbl = db.getTable(jobInfo.getAliasByOriginNameIfSet(tblInfo.name));
            if (tbl == null) {
                continue;
            }

            if (tbl.getType() != TableType.OLAP) {
                continue;
            }

            OlapTable olapTbl = (OlapTable) tbl;
            tbl.writeLock();
            try {
                if (olapTbl.getState() == OlapTableState.RESTORE
                        || olapTbl.getState() == OlapTableState.RESTORE_WITH_LOAD) {
                    olapTbl.setState(OlapTableState.NORMAL);
                }
            } finally {
                tbl.writeUnlock();
            }
        }
    }

    public static RestoreJob read(DataInput in) throws IOException {
        RestoreJob job = new RestoreJob();
        job.readFields(in);
        return job;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        Text.writeString(out, backupTimestamp);
        jobInfo.write(out);
        out.writeBoolean(allowLoad);
        
        Text.writeString(out, state.name());

        if (backupMeta != null) {
            out.writeBoolean(true);
            backupMeta.write(out);
        } else {
            out.writeBoolean(false);
        }

        fileMapping.write(out);

        out.writeLong(metaPreparedTime);
        out.writeLong(snapshotFinishedTime);
        out.writeLong(downloadFinishedTime);

        out.writeInt(restoreReplicationNum);

        out.writeInt(restoredPartitions.size());
        for (Pair<String, Partition> entry : restoredPartitions) {
            Text.writeString(out, entry.first);
            entry.second.write(out);
        }

        out.writeInt(restoredTbls.size());
        for (OlapTable tbl : restoredTbls) {
            tbl.write(out);
        }

        out.writeInt(restoredVersionInfo.rowKeySet().size());
        for (long tblId : restoredVersionInfo.rowKeySet()) {
            out.writeLong(tblId);
            out.writeInt(restoredVersionInfo.row(tblId).size());
            for (Map.Entry<Long, Pair<Long, Long>> entry : restoredVersionInfo.row(tblId).entrySet()) {
                out.writeLong(entry.getKey());
                out.writeLong(entry.getValue().first);
                out.writeLong(entry.getValue().second);
            }
        }

        out.writeInt(snapshotInfos.rowKeySet().size());
        for (long tabletId : snapshotInfos.rowKeySet()) {
            out.writeLong(tabletId);
            Map<Long, SnapshotInfo> map = snapshotInfos.row(tabletId);
            out.writeInt(map.size());
            for (Map.Entry<Long, SnapshotInfo> entry : map.entrySet()) {
                out.writeLong(entry.getKey());
                entry.getValue().write(out);
            }
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        backupTimestamp = Text.readString(in);
        jobInfo = BackupJobInfo.read(in);
        allowLoad = in.readBoolean();

        state = RestoreJobState.valueOf(Text.readString(in));

        if (in.readBoolean()) {
            backupMeta = BackupMeta.read(in);
        }

        fileMapping = RestoreFileMapping.read(in);

        metaPreparedTime = in.readLong();
        snapshotFinishedTime = in.readLong();
        downloadFinishedTime = in.readLong();

        restoreReplicationNum = in.readInt();

        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String tblName = Text.readString(in);
            Partition part = Partition.read(in);
            restoredPartitions.add(Pair.create(tblName, part));
        }

        size = in.readInt();
        for (int i = 0; i < size; i++) {
            restoredTbls.add((OlapTable) Table.read(in));
        }

        size = in.readInt();
        for (int i = 0; i < size; i++) {
            long tblId = in.readLong();
            int innerSize = in.readInt();
            for (int j = 0; j < innerSize; j++) {
                long partId = in.readLong();
                long version = in.readLong();
                long versionHash = in.readLong();
                restoredVersionInfo.put(tblId, partId, Pair.create(version, versionHash));
            }
        }

        size = in.readInt();
        for (int i = 0; i < size; i++) {
            long tabletId = in.readLong();
            int innerSize = in.readInt();
            for (int j = 0; j < innerSize; j++) {
                long beId = in.readLong();
                SnapshotInfo info = SnapshotInfo.read(in);
                snapshotInfos.put(tabletId, beId, info);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.append(", backup ts: ").append(backupTimestamp);
        sb.append(", state: ").append(state.name());
        return sb.toString();
    }
}

