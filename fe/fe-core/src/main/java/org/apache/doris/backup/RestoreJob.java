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

import org.apache.doris.analysis.BackupStmt.BackupContent;
import org.apache.doris.backup.BackupJobInfo.BackupIndexInfo;
import org.apache.doris.backup.BackupJobInfo.BackupOlapTableInfo;
import org.apache.doris.backup.BackupJobInfo.BackupPartitionInfo;
import org.apache.doris.backup.BackupJobInfo.BackupTabletInfo;
import org.apache.doris.backup.RestoreFileMapping.IdChain;
import org.apache.doris.backup.Status.ErrCode;
import org.apache.doris.catalog.BinlogConfig;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OdbcCatalogResource;
import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Partition.PartitionState;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.ResourceMgr;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.catalog.View;
import org.apache.doris.clone.DynamicPartitionScheduler;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.DbUtil;
import org.apache.doris.common.util.DynamicPartitionUtil;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.property.S3ClientBEProperties;
import org.apache.doris.resource.Tag;
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
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TRemoteTabletSnapshot;
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
import com.google.common.collect.Table.Cell;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RestoreJob extends AbstractJob {
    private static final String PROP_RESERVE_REPLICA = "reserve_replica";
    private static final String PROP_RESERVE_DYNAMIC_PARTITION_ENABLE = "reserve_dynamic_partition_enable";
    private static final String PROP_IS_BEING_SYNCED = PropertyAnalyzer.PROPERTIES_IS_BEING_SYNCED;

    private static final Logger LOG = LogManager.getLogger(RestoreJob.class);

    // CHECKSTYLE OFF
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
    // CHECKSTYLE ON

    private String backupTimestamp;

    private BackupJobInfo jobInfo;
    private boolean allowLoad;

    private RestoreJobState state;

    private BackupMeta backupMeta;

    private RestoreFileMapping fileMapping = new RestoreFileMapping();

    private long metaPreparedTime = -1;
    private long snapshotFinishedTime = -1;
    private long downloadFinishedTime = -1;

    private ReplicaAllocation replicaAlloc;

    private boolean reserveReplica = false;
    private boolean reserveDynamicPartitionEnable = false;

    // this 2 members is to save all newly restored objs
    // tbl name -> part
    private List<Pair<String, Partition>> restoredPartitions = Lists.newArrayList();
    private List<Table> restoredTbls = Lists.newArrayList();
    private List<Resource> restoredResources = Lists.newArrayList();

    // save all restored partitions' version info which are already exist in catalog
    // table id -> partition id -> (version, version hash)
    private com.google.common.collect.Table<Long, Long, Long> restoredVersionInfo = HashBasedTable.create();
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

    private boolean isBeingSynced = false;

    // restore properties
    private Map<String, String> properties = Maps.newHashMap();

    public RestoreJob() {
        super(JobType.RESTORE);
    }

    public RestoreJob(String label, String backupTs, long dbId, String dbName, BackupJobInfo jobInfo, boolean allowLoad,
            ReplicaAllocation replicaAlloc, long timeoutMs, int metaVersion, boolean reserveReplica,
            boolean reserveDynamicPartitionEnable, boolean isBeingSynced, Env env, long repoId) {
        super(JobType.RESTORE, label, dbId, dbName, timeoutMs, env, repoId);
        this.backupTimestamp = backupTs;
        this.jobInfo = jobInfo;
        this.allowLoad = allowLoad;
        this.replicaAlloc = replicaAlloc;
        this.state = RestoreJobState.PENDING;
        this.metaVersion = metaVersion;
        this.reserveReplica = reserveReplica;
        this.reserveDynamicPartitionEnable = reserveDynamicPartitionEnable;
        this.isBeingSynced = isBeingSynced;
        properties.put(PROP_RESERVE_REPLICA, String.valueOf(reserveReplica));
        properties.put(PROP_RESERVE_DYNAMIC_PARTITION_ENABLE, String.valueOf(reserveDynamicPartitionEnable));
        properties.put(PROP_IS_BEING_SYNCED, String.valueOf(isBeingSynced));
    }

    public RestoreJob(String label, String backupTs, long dbId, String dbName, BackupJobInfo jobInfo, boolean allowLoad,
            ReplicaAllocation replicaAlloc, long timeoutMs, int metaVersion, boolean reserveReplica,
            boolean reserveDynamicPartitionEnable, boolean isBeingSynced, Env env, long repoId, BackupMeta backupMeta) {
        this(label, backupTs, dbId, dbName, jobInfo, allowLoad, replicaAlloc, timeoutMs, metaVersion, reserveReplica,
                reserveDynamicPartitionEnable, isBeingSynced, env, repoId);
        this.backupMeta = backupMeta;
    }

    public boolean isFromLocalSnapshot() {
        return repoId == Repository.KEEP_ON_LOCAL_REPO_ID;
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

    public boolean isBeingSynced() {
        return isBeingSynced;
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
                LOG.warn("failed to find snapshot infos of tablet {} in be {}, {}",
                          tabletId, task.getBackendId(), this);
                return false;
            }
        }

        taskProgress.remove(task.getSignature());
        Long beId = unfinishedSignatureToId.remove(task.getSignature());
        if (beId == null || beId != task.getBackendId()) {
            LOG.warn("invalid download task: {}. {}", task, this);
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
            LOG.warn("invalid dir move task: {}. {}", task, this);
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
        if (repo == null && !isFromLocalSnapshot()) {
            repo = env.getBackupHandler().getRepoMgr().getRepo(repoId);
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

        Database db = env.getInternalCatalog().getDbNullable(dbId);
        if (db == null) {
            status = new Status(ErrCode.NOT_FOUND, "database " + dbId + " has been dropped");
            return;
        }

        for (IdChain idChain : fileMapping.getMapping().keySet()) {
            OlapTable tbl = (OlapTable) db.getTableNullable(idChain.getTblId());
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
    }

    /**
     * Restore rules as follow:
     * OlapTable
     * A. Table already exist
     *      A1. Partition already exist, generate file mapping
     *      A2. Partition does not exist, add restored partition to the table.
     *          Reset all index/tablet/replica id, and create replica on BE outside the table lock.
     * B. Table does not exist
     *      B1. Add table to the db, reset all table/index/tablet/replica id,
     *          and create replica on BE outside the db lock.
     * View
     *      * A. View already exist. The same signature is allowed.
     *      * B. View does not exist.
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
        Database db = env.getInternalCatalog().getDbNullable(dbId);
        if (db == null) {
            status = new Status(ErrCode.NOT_FOUND, "database " + dbId + " does not exist");
            return;
        }

        // generate job id
        jobId = env.getNextId();

        // deserialize meta
        if (!downloadAndDeserializeMetaInfo()) {
            return;
        }
        Preconditions.checkNotNull(backupMeta);

        // Set all restored tbls' state to RESTORE
        // Table's origin state must be NORMAL and does not have unfinished load job.
        for (String tableName : jobInfo.backupOlapTableObjects.keySet()) {
            Table tbl = db.getTableNullable(jobInfo.getAliasByOriginNameIfSet(tableName));
            if (tbl == null) {
                continue;
            }

            if (tbl.getType() != TableType.OLAP) {
                status = new Status(ErrCode.COMMON_ERROR, "Only support retore OLAP table: " + tbl.getName());
                return;
            }

            OlapTable olapTbl = (OlapTable) tbl;
            if (!olapTbl.writeLockIfExist()) {
                continue;
            }
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
                    if (!env.getLoadInstance().checkPartitionLoadFinished(partition.getId(), null)) {
                        status = new Status(ErrCode.COMMON_ERROR,
                                "Table " + tbl.getName() + "'s has unfinished load job");
                        return;
                    }
                }

                olapTbl.setState(OlapTableState.RESTORE);
                // set restore status for partitions
                BackupOlapTableInfo tblInfo = jobInfo.backupOlapTableObjects.get(tableName);
                for (Map.Entry<String, BackupPartitionInfo> partitionEntry : tblInfo.partitions.entrySet()) {
                    String partitionName = partitionEntry.getKey();
                    Partition partition = olapTbl.getPartition(partitionName);
                    if (partition == null) {
                        continue;
                    }
                    partition.setState(PartitionState.RESTORE);
                }
            } finally {
                olapTbl.writeUnlock();
            }
        }

        for (BackupJobInfo.BackupViewInfo backupViewInfo : jobInfo.newBackupObjects.views) {
            Table tbl = db.getTableNullable(jobInfo.getAliasByOriginNameIfSet(backupViewInfo.name));
            if (tbl == null) {
                continue;
            }
            if (tbl.getType() != TableType.VIEW) {
                status = new Status(ErrCode.COMMON_ERROR,
                        "The local table " + tbl.getName()
                                + " with the same name but a different type of backup meta.");
                return;
            }
        }
        for (BackupJobInfo.BackupOdbcTableInfo backupOdbcTableInfo : jobInfo.newBackupObjects.odbcTables) {
            Table tbl = db.getTableNullable(jobInfo.getAliasByOriginNameIfSet(backupOdbcTableInfo.dorisTableName));
            if (tbl == null) {
                continue;
            }
            if (tbl.getType() != TableType.ODBC) {
                status = new Status(ErrCode.COMMON_ERROR,
                        "The local table " + tbl.getName()
                                + " with the same name but a different type of backup meta.");
                return;
            }
        }
        for (BackupJobInfo.BackupOdbcResourceInfo backupOdbcResourceInfo : jobInfo.newBackupObjects.odbcResources) {
            Resource resource = Env.getCurrentEnv().getResourceMgr().getResource(backupOdbcResourceInfo.name);
            if (resource == null) {
                continue;
            }
            if (resource.getType() != Resource.ResourceType.ODBC_CATALOG) {
                status = new Status(ErrCode.COMMON_ERROR,
                        "The local resource " + resource.getName()
                                + " with the same name but a different type of backup meta.");
                return;
            }
        }

        // Check and prepare meta objects.
        AgentBatchTask batchTask = new AgentBatchTask();
        db.readLock();
        try {
            for (Map.Entry<String, BackupOlapTableInfo> olapTableEntry : jobInfo.backupOlapTableObjects.entrySet()) {
                String tableName = olapTableEntry.getKey();
                BackupOlapTableInfo tblInfo = olapTableEntry.getValue();
                Table remoteTbl = backupMeta.getTable(tableName);
                Preconditions.checkNotNull(remoteTbl);
                Table localTbl = db.getTableNullable(jobInfo.getAliasByOriginNameIfSet(tableName));
                if (localTbl != null) {
                    // table already exist, check schema
                    if (localTbl.getType() != TableType.OLAP) {
                        status = new Status(ErrCode.COMMON_ERROR,
                                "The type of local table should be same as type of remote table: "
                                        + remoteTbl.getName());
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
                        if (!localOlapTbl.getSignature(BackupHandler.SIGNATURE_VERSION, intersectPartNames)
                                .equals(remoteOlapTbl.getSignature(
                                        BackupHandler.SIGNATURE_VERSION, intersectPartNames))) {
                            status = new Status(ErrCode.COMMON_ERROR, "Table "
                                    + jobInfo.getAliasByOriginNameIfSet(tableName)
                                    + " already exist but with different schema");
                            return;
                        }

                        // Table with same name and has same schema. Check partition
                        for (Map.Entry<String, BackupPartitionInfo> partitionEntry : tblInfo.partitions.entrySet()) {
                            String partitionName = partitionEntry.getKey();
                            BackupPartitionInfo backupPartInfo = partitionEntry.getValue();
                            Partition localPartition = localOlapTbl.getPartition(partitionName);
                            Partition remotePartition = remoteOlapTbl.getPartition(partitionName);
                            if (localPartition != null) {
                                // Partition already exist.
                                PartitionInfo localPartInfo = localOlapTbl.getPartitionInfo();
                                PartitionInfo remotePartInfo = remoteOlapTbl.getPartitionInfo();
                                ReplicaAllocation remoteReplicaAlloc = remotePartInfo.getReplicaAllocation(
                                        remotePartition.getId());
                                if (localPartInfo.getType() == PartitionType.RANGE
                                        || localPartInfo.getType() == PartitionType.LIST) {
                                    PartitionItem localItem = localPartInfo.getItem(localPartition.getId());
                                    PartitionItem remoteItem = remoteOlapTbl
                                            .getPartitionInfo().getItem(backupPartInfo.id);
                                    if (localItem.equals(remoteItem)) {
                                        // Same partition, same range
                                        if (genFileMappingWhenBackupReplicasEqual(localPartInfo, localPartition,
                                                localTbl, backupPartInfo, partitionName, tblInfo, remoteReplicaAlloc)) {
                                            return;
                                        }
                                    } else {
                                        // Same partition name, different range
                                        status = new Status(ErrCode.COMMON_ERROR, "Partition " + partitionName
                                                + " in table " + localTbl.getName()
                                                + " has different partition item with partition in repository");
                                        return;
                                    }
                                } else {
                                    // If this is a single partitioned table.
                                    if (genFileMappingWhenBackupReplicasEqual(localPartInfo, localPartition, localTbl,
                                            backupPartInfo, partitionName, tblInfo, remoteReplicaAlloc)) {
                                        return;
                                    }
                                }

                            } else {
                                // partitions does not exist
                                PartitionInfo localPartitionInfo = localOlapTbl.getPartitionInfo();
                                if (localPartitionInfo.getType() == PartitionType.RANGE
                                        || localPartitionInfo.getType() == PartitionType.LIST) {
                                    PartitionItem remoteItem = remoteOlapTbl.getPartitionInfo()
                                            .getItem(backupPartInfo.id);
                                    if (localPartitionInfo.getAnyIntersectItem(remoteItem, false) != null) {
                                        status = new Status(ErrCode.COMMON_ERROR, "Partition " + partitionName
                                                + " in table " + localTbl.getName()
                                                + " has conflict partition item with existing items");
                                        return;
                                    } else {
                                        // this partition can be added to this table, set ids
                                        ReplicaAllocation restoreReplicaAlloc = replicaAlloc;
                                        if (reserveReplica) {
                                            PartitionInfo remotePartInfo = remoteOlapTbl.getPartitionInfo();
                                            restoreReplicaAlloc = remotePartInfo.getReplicaAllocation(
                                                remotePartition.getId());
                                        }
                                        Partition restorePart = resetPartitionForRestore(localOlapTbl, remoteOlapTbl,
                                                partitionName,
                                                db.getClusterName(),
                                                restoreReplicaAlloc);
                                        if (restorePart == null) {
                                            return;
                                        }
                                        restoredPartitions.add(Pair.of(localOlapTbl.getName(), restorePart));
                                    }
                                } else {
                                    // It is impossible that a single partitioned table exist
                                    // without any existing partition
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
                            remoteOlapTbl.dropPartitionAndReserveTablet(partName);
                        }
                    }

                    // reset all ids in this table
                    Status st = remoteOlapTbl.resetIdsForRestore(env, db, replicaAlloc, reserveReplica);
                    if (!st.ok()) {
                        status = st;
                        return;
                    }

                    // Reset properties to correct values.
                    remoteOlapTbl.resetPropertiesForRestore(reserveDynamicPartitionEnable, reserveReplica,
                                                            replicaAlloc, isBeingSynced);

                    // DO NOT set remote table's new name here, cause we will still need the origin name later
                    // remoteOlapTbl.setName(jobInfo.getAliasByOriginNameIfSet(tblInfo.name));
                    remoteOlapTbl.setState(allowLoad ? OlapTableState.RESTORE_WITH_LOAD : OlapTableState.RESTORE);
                    LOG.debug("put remote table {} to restoredTbls", remoteOlapTbl.getName());
                    restoredTbls.add(remoteOlapTbl);
                }
            } // end of all restore olap tables

            // restore views
            for (BackupJobInfo.BackupViewInfo backupViewInfo : jobInfo.newBackupObjects.views) {
                String backupViewName = backupViewInfo.name;
                Table localTbl = db.getTableNullable(jobInfo.getAliasByOriginNameIfSet(backupViewName));
                View remoteView = (View) backupMeta.getTable(backupViewName);
                if (localTbl != null) {
                    Preconditions.checkState(localTbl.getType() == TableType.VIEW);
                    View localView = (View) localTbl;
                    if (!localView.getSignature(BackupHandler.SIGNATURE_VERSION)
                            .equals(remoteView.getSignature(BackupHandler.SIGNATURE_VERSION))) {
                        status = new Status(ErrCode.COMMON_ERROR, "View "
                                + jobInfo.getAliasByOriginNameIfSet(backupViewName)
                                + " already exist but with different schema");
                        return;
                    }
                } else {
                    remoteView.resetIdsForRestore(env);
                    restoredTbls.add(remoteView);
                }
            }

            // restore odbc external table
            for (BackupJobInfo.BackupOdbcTableInfo backupOdbcTableInfo : jobInfo.newBackupObjects.odbcTables) {
                String backupOdbcTableName = backupOdbcTableInfo.dorisTableName;
                Table localTbl = db.getTableNullable(jobInfo.getAliasByOriginNameIfSet(backupOdbcTableName));
                OdbcTable remoteOdbcTable = (OdbcTable) backupMeta.getTable(backupOdbcTableName);
                if (localTbl != null) {
                    Preconditions.checkState(localTbl.getType() == TableType.ODBC);
                    OdbcTable localOdbcTable = (OdbcTable) localTbl;
                    if (!localOdbcTable.getSignature(BackupHandler.SIGNATURE_VERSION)
                            .equals(remoteOdbcTable.getSignature(BackupHandler.SIGNATURE_VERSION))) {
                        status = new Status(ErrCode.COMMON_ERROR, "Odbc table "
                                + jobInfo.getAliasByOriginNameIfSet(backupOdbcTableName)
                                + " already exist but with different schema");
                        return;
                    }
                } else {
                    remoteOdbcTable.resetIdsForRestore(env);
                    restoredTbls.add(remoteOdbcTable);
                }
            }

            LOG.debug("finished to prepare restored partitions and tables. {}", this);
            // for now, nothing is modified in catalog

            // generate create replica tasks for all restored partitions
            for (Pair<String, Partition> entry : restoredPartitions) {
                OlapTable localTbl = (OlapTable) db.getTableNullable(entry.first);
                Preconditions.checkNotNull(localTbl, localTbl.getName());
                Partition restorePart = entry.second;
                OlapTable remoteTbl = (OlapTable) backupMeta.getTable(entry.first);
                BackupPartitionInfo backupPartitionInfo
                        = jobInfo.getOlapTableInfo(entry.first).getPartInfo(restorePart.getName());

                createReplicas(db, batchTask, localTbl, restorePart);

                genFileMapping(localTbl, restorePart, remoteTbl.getId(), backupPartitionInfo,
                        !allowLoad /* if allow load, do not overwrite when commit */);
            }

            // generate create replica task for all restored tables
            for (Table restoreTbl : restoredTbls) {
                if (restoreTbl.getType() == TableType.OLAP) {
                    OlapTable restoreOlapTable = (OlapTable) restoreTbl;
                    for (Partition restorePart : restoreOlapTable.getPartitions()) {
                        createReplicas(db, batchTask, restoreOlapTable, restorePart);
                        BackupOlapTableInfo backupOlapTableInfo = jobInfo.getOlapTableInfo(restoreOlapTable.getName());
                        genFileMapping(restoreOlapTable, restorePart, backupOlapTableInfo.id,
                                backupOlapTableInfo.getPartInfo(restorePart.getName()),
                                !allowLoad /* if allow load, do not overwrite when commit */);
                    }
                }
                // set restored table's new name after all 'genFileMapping'
                restoreTbl.setName(jobInfo.getAliasByOriginNameIfSet(restoreTbl.getName()));
            }

            LOG.debug("finished to generate create replica tasks. {}", this);
        } finally {
            db.readUnlock();
        }

        // check and restore resources
        checkAndRestoreResources();
        if (!status.ok()) {
            return;
        }
        LOG.debug("finished to restore resources. {}", this.jobId);

        // Send create replica task to BE outside the db lock
        boolean ok = false;
        MarkedCountDownLatch<Long, Long> latch = new MarkedCountDownLatch<Long, Long>(batchTask.getTaskNum());
        if (batchTask.getTaskNum() > 0) {
            for (AgentTask task : batchTask.getAllTasks()) {
                latch.addMark(task.getBackendId(), task.getTabletId());
                ((CreateReplicaTask) task).setLatch(latch);
                AgentTaskQueue.addTask(task);
            }
            AgentTaskExecutor.submit(batchTask);

            // estimate timeout
            long timeout = DbUtil.getCreateReplicasTimeoutMs(batchTask.getTaskNum());
            try {
                LOG.info("begin to send create replica tasks to BE for restore. total {} tasks. timeout: {}",
                        batchTask.getTaskNum(), timeout);
                ok = latch.await(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOG.warn("InterruptedException: ", e);
                ok = false;
            }
        } else {
            ok = true;
        }

        if (ok) {
            LOG.debug("finished to create all restored replcias. {}", this);
            // add restored partitions.
            // table should be in State RESTORE, so no other partitions can be
            // added to or removed from this table during the restore process.
            for (Pair<String, Partition> entry : restoredPartitions) {
                OlapTable localTbl = (OlapTable) db.getTableNullable(entry.first);
                localTbl.writeLock();
                try {
                    Partition restoredPart = entry.second;
                    OlapTable remoteTbl = (OlapTable) backupMeta.getTable(entry.first);
                    if (localTbl.getPartitionInfo().getType() == PartitionType.RANGE
                            || localTbl.getPartitionInfo().getType() == PartitionType.LIST) {

                        PartitionInfo remotePartitionInfo = remoteTbl.getPartitionInfo();
                        PartitionInfo localPartitionInfo = localTbl.getPartitionInfo();
                        BackupPartitionInfo backupPartitionInfo
                                = jobInfo.getOlapTableInfo(entry.first).getPartInfo(restoredPart.getName());
                        long remotePartId = backupPartitionInfo.id;
                        PartitionItem remoteItem = remoteTbl.getPartitionInfo().getItem(remotePartId);
                        DataProperty remoteDataProperty = remotePartitionInfo.getDataProperty(remotePartId);
                        ReplicaAllocation restoreReplicaAlloc = replicaAlloc;
                        if (reserveReplica) {
                            restoreReplicaAlloc = remotePartitionInfo.getReplicaAllocation(remotePartId);
                        }
                        localPartitionInfo.addPartition(restoredPart.getId(), false, remoteItem,
                                remoteDataProperty, restoreReplicaAlloc,
                                remotePartitionInfo.getIsInMemory(remotePartId),
                                remotePartitionInfo.getIsMutable(remotePartId));
                    }
                    localTbl.addPartition(restoredPart);
                } finally {
                    localTbl.writeUnlock();
                }

            }

            // add restored tables
            for (Table tbl : restoredTbls) {
                if (!db.writeLockIfExist()) {
                    status = new Status(ErrCode.COMMON_ERROR, "Database " + db.getFullName()
                            + " has been dropped");
                    return;
                }
                tbl.writeLock();
                try {
                    if (!db.createTable(tbl)) {
                        status = new Status(ErrCode.COMMON_ERROR, "Table " + tbl.getName()
                                + " already exist in db: " + db.getFullName());
                        return;
                    }
                } finally {
                    tbl.writeUnlock();
                    db.writeUnlock();
                }
            }
        } else {
            // only show at most 10 results
            List<String> subList = latch.getLeftMarks().stream().limit(10)
                    .map(item -> "(backendId = " + item.getKey() + ", tabletId = "  + item.getValue() + ")")
                    .collect(Collectors.toList());
            String idStr = Joiner.on(", ").join(subList);
            status = new Status(ErrCode.COMMON_ERROR,
                    "Failed to create replicas for restore. unfinished marks: " + idStr);
            return;
        }
        LOG.info("finished to prepare meta. {}", this);

        if (jobInfo.content == null || jobInfo.content == BackupContent.ALL) {
            prepareAndSendSnapshotTaskForOlapTable(db);
        }

        metaPreparedTime = System.currentTimeMillis();
        state = RestoreJobState.SNAPSHOTING;
        // No log here, PENDING state restore job will redo this method
    }

    private void prepareAndSendSnapshotTaskForOlapTable(Database db) {
        LOG.info("begin to make snapshot. {} when restore content is ALL", this);
        // begin to make snapshots for all replicas
        // snapshot is for incremental download
        unfinishedSignatureToId.clear();
        taskProgress.clear();
        taskErrMsg.clear();
        Multimap<Long, Long> bePathsMap = HashMultimap.create();
        AgentBatchTask batchTask = new AgentBatchTask();
        db.readLock();
        try {
            for (IdChain idChain : fileMapping.getMapping().keySet()) {
                OlapTable tbl = (OlapTable) db.getTableNullable(idChain.getTblId());
                tbl.readLock();
                try {
                    Partition part = tbl.getPartition(idChain.getPartId());
                    MaterializedIndex index = part.getIndex(idChain.getIdxId());
                    Tablet tablet = index.getTablet(idChain.getTabletId());
                    Replica replica = tablet.getReplicaById(idChain.getReplicaId());
                    long signature = env.getNextId();
                    SnapshotTask task = new SnapshotTask(null, replica.getBackendId(), signature, jobId, db.getId(),
                            tbl.getId(), part.getId(), index.getId(), tablet.getId(), part.getVisibleVersion(),
                            tbl.getSchemaHashByIndexId(index.getId()), timeoutMs, true /* is restore task*/);
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
        org.apache.doris.common.Status st = Env.getCurrentSystemInfo().checkExceedDiskCapacityLimit(bePathsMap, true);
        if (!st.ok()) {
            status = new Status(ErrCode.COMMON_ERROR, st.getErrorMsg());
            return;
        }

        // send tasks
        for (AgentTask task : batchTask.getAllTasks()) {
            AgentTaskQueue.addTask(task);
        }
        AgentTaskExecutor.submit(batchTask);
        LOG.info("finished to send snapshot tasks, num: {}. {}", batchTask.getTaskNum(), this);
    }

    private void checkAndRestoreResources() {
        ResourceMgr resourceMgr = Env.getCurrentEnv().getResourceMgr();
        for (BackupJobInfo.BackupOdbcResourceInfo backupOdbcResourceInfo : jobInfo.newBackupObjects.odbcResources) {
            String backupResourceName = backupOdbcResourceInfo.name;
            Resource localResource = resourceMgr.getResource(backupResourceName);
            OdbcCatalogResource remoteOdbcResource = (OdbcCatalogResource) backupMeta.getResource(backupResourceName);
            if (localResource != null) {
                if (localResource.getType() != Resource.ResourceType.ODBC_CATALOG) {
                    status = new Status(ErrCode.COMMON_ERROR, "The type of local resource "
                            + backupResourceName + " is not same as restored resource");
                    return;
                }
                OdbcCatalogResource localOdbcResource = (OdbcCatalogResource) localResource;
                if (localOdbcResource.getSignature(BackupHandler.SIGNATURE_VERSION)
                        != remoteOdbcResource.getSignature(BackupHandler.SIGNATURE_VERSION)) {
                    status = new Status(ErrCode.COMMON_ERROR, "Odbc resource "
                            + jobInfo.getAliasByOriginNameIfSet(backupResourceName)
                            + " already exist but with different properties");
                    return;
                }
            } else {
                try {
                    // restore resource
                    resourceMgr.createResource(remoteOdbcResource, false);
                } catch (DdlException e) {
                    status = new Status(ErrCode.COMMON_ERROR, e.getMessage());
                    return;
                }
                restoredResources.add(remoteOdbcResource);
            }
        }
    }

    private boolean genFileMappingWhenBackupReplicasEqual(PartitionInfo localPartInfo, Partition localPartition,
            Table localTbl, BackupPartitionInfo backupPartInfo, String partitionName, BackupOlapTableInfo tblInfo,
            ReplicaAllocation remoteReplicaAlloc) {
        short restoreReplicaNum;
        short localReplicaNum = localPartInfo.getReplicaAllocation(localPartition.getId()).getTotalReplicaNum();
        if (!reserveReplica) {
            restoreReplicaNum = replicaAlloc.getTotalReplicaNum();
        } else {
            restoreReplicaNum = remoteReplicaAlloc.getTotalReplicaNum();
        }
        if (localReplicaNum != restoreReplicaNum) {
            status = new Status(ErrCode.COMMON_ERROR, "Partition " + partitionName
                    + " in table " + localTbl.getName()
                    + " has different replication num '"
                    + localReplicaNum
                    + "' with partition in repository, which is " + restoreReplicaNum);
            return true;
        }

        // No need to check range, just generate file mapping
        OlapTable localOlapTbl = (OlapTable) localTbl;
        genFileMapping(localOlapTbl, localPartition, tblInfo.id, backupPartInfo,
                true /* overwrite when commit */);
        restoredVersionInfo.put(localOlapTbl.getId(), localPartition.getId(), backupPartInfo.version);
        return false;
    }

    private void createReplicas(Database db, AgentBatchTask batchTask, OlapTable localTbl, Partition restorePart) {
        Set<String> bfColumns = localTbl.getCopiedBfColumns();
        double bfFpp = localTbl.getBfFpp();

        BinlogConfig binlogConfig;
        localTbl.readLock();
        try {
            binlogConfig = new BinlogConfig(localTbl.getBinlogConfig());
        } finally {
            localTbl.readUnlock();
        }
        for (MaterializedIndex restoredIdx : restorePart.getMaterializedIndices(IndexExtState.VISIBLE)) {
            MaterializedIndexMeta indexMeta = localTbl.getIndexMetaByIndexId(restoredIdx.getId());
            for (Tablet restoreTablet : restoredIdx.getTablets()) {
                TabletMeta tabletMeta = new TabletMeta(db.getId(), localTbl.getId(), restorePart.getId(),
                        restoredIdx.getId(), indexMeta.getSchemaHash(), TStorageMedium.HDD);
                Env.getCurrentInvertedIndex().addTablet(restoreTablet.getId(), tabletMeta);
                for (Replica restoreReplica : restoreTablet.getReplicas()) {
                    Env.getCurrentInvertedIndex().addReplica(restoreTablet.getId(), restoreReplica);
                    CreateReplicaTask task = new CreateReplicaTask(restoreReplica.getBackendId(), dbId,
                            localTbl.getId(), restorePart.getId(), restoredIdx.getId(),
                            restoreTablet.getId(), restoreReplica.getId(), indexMeta.getShortKeyColumnCount(),
                            indexMeta.getSchemaHash(), restoreReplica.getVersion(),
                            indexMeta.getKeysType(), TStorageType.COLUMN,
                            TStorageMedium.HDD /* all restored replicas will be saved to HDD */,
                            indexMeta.getSchema(), bfColumns, bfFpp, null,
                            localTbl.getCopiedIndexes(),
                            localTbl.isInMemory(),
                            localTbl.getPartitionInfo().getTabletType(restorePart.getId()),
                            null,
                            localTbl.getCompressionType(),
                            localTbl.getEnableUniqueKeyMergeOnWrite(), localTbl.getStoragePolicy(),
                            localTbl.disableAutoCompaction(),
                            localTbl.enableSingleReplicaCompaction(),
                            localTbl.skipWriteIndexOnLoad(),
                            localTbl.getCompactionPolicy(),
                            localTbl.getTimeSeriesCompactionGoalSizeMbytes(),
                            localTbl.getTimeSeriesCompactionFileCountThreshold(),
                            localTbl.getTimeSeriesCompactionTimeThresholdSeconds(),
                            localTbl.storeRowColumn(),
                            localTbl.isDynamicSchema(),
                            binlogConfig);

                    task.setInRestoreMode(true);
                    batchTask.addTask(task);
                }
            }
        }
    }

    // reset remote partition.
    // reset all id in remote partition, but DO NOT modify any exist catalog objects.
    private Partition resetPartitionForRestore(OlapTable localTbl, OlapTable remoteTbl, String partName,
                                               String clusterName, ReplicaAllocation replicaAlloc) {
        Preconditions.checkState(localTbl.getPartition(partName) == null);
        Partition remotePart = remoteTbl.getPartition(partName);
        Preconditions.checkNotNull(remotePart);
        PartitionInfo localPartitionInfo = localTbl.getPartitionInfo();
        Preconditions.checkState(localPartitionInfo.getType() == PartitionType.RANGE
                || localPartitionInfo.getType() == PartitionType.LIST);

        // generate new partition id
        long newPartId = env.getNextId();
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

        // tablets
        for (MaterializedIndex remoteIdx : remotePart.getMaterializedIndices(IndexExtState.VISIBLE)) {
            int schemaHash = remoteTbl.getSchemaHashByIndexId(remoteIdx.getId());
            int remotetabletSize = remoteIdx.getTablets().size();
            remoteIdx.clearTabletsForRestore();
            for (int i = 0; i < remotetabletSize; i++) {
                // generate new tablet id
                long newTabletId = env.getNextId();
                Tablet newTablet = new Tablet(newTabletId);
                // add tablet to index, but not add to TabletInvertedIndex
                remoteIdx.addTablet(newTablet, null /* tablet meta */, true /* is restore */);

                // replicas
                try {
                    Map<Tag, List<Long>> beIds = Env.getCurrentSystemInfo()
                            .selectBackendIdsForReplicaCreation(replicaAlloc, null, false, false);
                    for (Map.Entry<Tag, List<Long>> entry : beIds.entrySet()) {
                        for (Long beId : entry.getValue()) {
                            long newReplicaId = env.getNextId();
                            Replica newReplica = new Replica(newReplicaId, beId, ReplicaState.NORMAL, visibleVersion,
                                    schemaHash);
                            newTablet.addReplica(newReplica, true /* is restore */);
                        }
                    }
                } catch (DdlException e) {
                    status = new Status(ErrCode.COMMON_ERROR, e.getMessage());
                    return null;
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
            Preconditions.checkState(backupIdxInfo.sortedTabletInfoList.size() == localIdx.getTablets().size());
            for (int i = 0; i < localIdx.getTablets().size(); i++) {
                Tablet localTablet = localIdx.getTablets().get(i);
                BackupTabletInfo backupTabletInfo = backupIdxInfo.sortedTabletInfoList.get(i);
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
        if (isFromLocalSnapshot()) {
            if (backupMeta != null) {
                return true;
            }

            status = new Status(ErrCode.COMMON_ERROR, "backupMeta is null");
            return false;
        }

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
        Database db;
        try {
            db = env.getInternalCatalog().getDbOrMetaException(dbId);
        } catch (MetaNotFoundException e) {
            LOG.warn("[INCONSISTENT META] replayCheckAndPrepareMeta failed", e);
            return;
        }

        // replay set all existing tables's state to RESTORE
        for (String tableName : jobInfo.backupOlapTableObjects.keySet()) {
            Table tbl = db.getTableNullable(jobInfo.getAliasByOriginNameIfSet(tableName));
            if (tbl == null) {
                continue;
            }
            OlapTable olapTbl = (OlapTable) tbl;
            tbl.writeLock();
            try {
                olapTbl.setState(OlapTableState.RESTORE);
                // set restore status for partitions
                BackupOlapTableInfo tblInfo = jobInfo.backupOlapTableObjects.get(tableName);
                for (Map.Entry<String, BackupPartitionInfo> partitionEntry : tblInfo.partitions.entrySet()) {
                    String partitionName = partitionEntry.getKey();
                    Partition partition = olapTbl.getPartition(partitionName);
                    if (partition == null) {
                        continue;
                    }
                    partition.setState(PartitionState.RESTORE);
                }
            } finally {
                tbl.writeUnlock();
            }
        }

        // restored partitions
        for (Pair<String, Partition> entry : restoredPartitions) {
            OlapTable localTbl = (OlapTable) db.getTableNullable(entry.first);
            Partition restorePart = entry.second;
            OlapTable remoteTbl = (OlapTable) backupMeta.getTable(entry.first);
            PartitionInfo localPartitionInfo = localTbl.getPartitionInfo();
            PartitionInfo remotePartitionInfo = remoteTbl.getPartitionInfo();
            BackupPartitionInfo backupPartitionInfo = jobInfo.getOlapTableInfo(entry.first)
                    .getPartInfo(restorePart.getName());
            long remotePartId = backupPartitionInfo.id;
            DataProperty remoteDataProperty = remotePartitionInfo.getDataProperty(remotePartId);
            ReplicaAllocation restoreReplicaAlloc = replicaAlloc;
            if (reserveReplica) {
                restoreReplicaAlloc = remotePartitionInfo.getReplicaAllocation(remotePartId);
            }
            localPartitionInfo.addPartition(restorePart.getId(), false, remotePartitionInfo.getItem(remotePartId),
                    remoteDataProperty, restoreReplicaAlloc,
                    remotePartitionInfo.getIsInMemory(remotePartId),
                    remotePartitionInfo.getIsMutable(remotePartId));
            localTbl.addPartition(restorePart);

            // modify tablet inverted index
            for (MaterializedIndex restoreIdx : restorePart.getMaterializedIndices(IndexExtState.VISIBLE)) {
                int schemaHash = localTbl.getSchemaHashByIndexId(restoreIdx.getId());
                for (Tablet restoreTablet : restoreIdx.getTablets()) {
                    TabletMeta tabletMeta = new TabletMeta(db.getId(), localTbl.getId(), restorePart.getId(),
                            restoreIdx.getId(), schemaHash, TStorageMedium.HDD);
                    Env.getCurrentInvertedIndex().addTablet(restoreTablet.getId(), tabletMeta);
                    for (Replica restoreReplica : restoreTablet.getReplicas()) {
                        Env.getCurrentInvertedIndex().addReplica(restoreTablet.getId(), restoreReplica);
                    }
                }
            }
        }


        // restored tables
        for (Table restoreTbl : restoredTbls) {
            db.writeLock();
            restoreTbl.writeLock();
            try {
                db.createTable(restoreTbl);
            } finally {
                restoreTbl.writeUnlock();
                db.writeUnlock();
            }
            if (restoreTbl.getType() != TableType.OLAP) {
                continue;
            }
            OlapTable olapRestoreTbl = (OlapTable) restoreTbl;
            olapRestoreTbl.writeLock();
            try {
                // modify tablet inverted index
                for (Partition restorePart : olapRestoreTbl.getPartitions()) {
                    for (MaterializedIndex restoreIdx : restorePart.getMaterializedIndices(IndexExtState.VISIBLE)) {
                        int schemaHash = olapRestoreTbl.getSchemaHashByIndexId(restoreIdx.getId());
                        for (Tablet restoreTablet : restoreIdx.getTablets()) {
                            TabletMeta tabletMeta = new TabletMeta(db.getId(), restoreTbl.getId(), restorePart.getId(),
                                    restoreIdx.getId(), schemaHash, TStorageMedium.HDD);
                            Env.getCurrentInvertedIndex().addTablet(restoreTablet.getId(), tabletMeta);
                            for (Replica restoreReplica : restoreTablet.getReplicas()) {
                                Env.getCurrentInvertedIndex().addReplica(restoreTablet.getId(), restoreReplica);
                            }
                        }
                    }
                }
            } finally {
                olapRestoreTbl.writeUnlock();
            }
        }

        // restored resource
        ResourceMgr resourceMgr = Env.getCurrentEnv().getResourceMgr();
        for (Resource resource : restoredResources) {
            resourceMgr.replayCreateResource(resource);
        }

        LOG.info("replay check and prepare meta. {}", this);
    }

    private void waitingAllSnapshotsFinished() {
        if (unfinishedSignatureToId.isEmpty()) {
            snapshotFinishedTime = System.currentTimeMillis();
            state = RestoreJobState.DOWNLOAD;

            env.getEditLog().logRestoreJob(this);
            LOG.info("finished making snapshots. {}", this);
            return;
        }

        LOG.info("waiting {} replicas to make snapshot: [{}]. {}",
                 unfinishedSignatureToId.size(), unfinishedSignatureToId, this);
        return;
    }

    private void downloadSnapshots() {
        if (isFromLocalSnapshot()) {
            downloadLocalSnapshots();
        } else {
            downloadRemoteSnapshots();
        }
    }

    private void downloadRemoteSnapshots() {
        // Categorize snapshot onfos by db id.
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

            Database db = env.getInternalCatalog().getDbNullable(dbId);
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

                    List<FsBroker> brokerAddrs = null;
                    brokerAddrs = Lists.newArrayList();
                    Status st = repo.getBrokerAddress(beId, env, brokerAddrs);
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
                            Table tbl = db.getTableNullable(info.getTblId());
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
                                    status = new Status(ErrCode.NOT_FOUND, "index " + info.getIndexId()
                                            + " does not exist in partion " + part.getName()
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
                                if (src == null) {
                                    status = new Status(ErrCode.COMMON_ERROR, "invalid src path: " + repoTabletPath);
                                    return;
                                }
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
                        long signature = env.getNextId();
                        DownloadTask task = new DownloadTask(null, beId, signature, jobId, dbId, srcToDest,
                                brokerAddrs.get(0),
                                S3ClientBEProperties.getBeFSProperties(repo.getRemoteFileSystem().getProperties()),
                                repo.getRemoteFileSystem().getStorageType(), repo.getLocation());
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

    private void downloadLocalSnapshots() {
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

            Database db = env.getInternalCatalog().getDbNullable(dbId);
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

                    // allot tasks
                    int index = 0;
                    for (int batch = 0; batch < batchNum; batch++) {
                        List<TRemoteTabletSnapshot> remoteTabletSnapshots = Lists.newArrayList();
                        int currentBatchTaskNum = (batch == batchNum - 1) ? totalNum - index : taskNumPerBatch;
                        for (int j = 0; j < currentBatchTaskNum; j++) {
                            TRemoteTabletSnapshot remoteTabletSnapshot = new TRemoteTabletSnapshot();

                            SnapshotInfo info = beSnapshotInfos.get(index++);
                            Table tbl = db.getTableNullable(info.getTblId());
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
                                    status = new Status(ErrCode.NOT_FOUND, "index " + info.getIndexId()
                                            + " does not exist in partion " + part.getName()
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
                                    return;
                                }

                                SnapshotInfo snapshotInfo = snapshotInfos.get(info.getTabletId(), info.getBeId());
                                Preconditions.checkNotNull(snapshotInfo, info.getTabletId() + "-" + info.getBeId());
                                // download to previous exist snapshot dir
                                String dest = snapshotInfo.getTabletPath();

                                Long localTabletId = info.getTabletId();
                                String localSnapshotPath = dest;
                                Long remoteTabletId = repoIds.getTabletId();
                                Long remoteBeId = jobInfo.getBeId(remoteTabletId);
                                String remoteSnapshotPath = jobInfo.getTabletSnapshotPath(remoteTabletId);
                                if (remoteSnapshotPath == null) {
                                    status = new Status(ErrCode.NOT_FOUND,
                                            "failed to get remote snapshot path of tablet: " + remoteTabletId);
                                    return;
                                }
                                Long schemaHash = jobInfo.getSchemaHash(
                                        repoIds.getTblId(), repoIds.getPartId(), repoIds.getIdxId());
                                if (schemaHash == null) {
                                    status = new Status(ErrCode.NOT_FOUND,
                                            "failed to get schema hash of table: " + repoIds.getTblId()
                                                    + ", partition: " + repoIds.getPartId()
                                                    + ", index: " + repoIds.getIdxId());
                                    return;
                                }
                                // remoteSnapshotPath = "${remoteSnapshotPath}/${remoteTabletId}/${schemaHash}"
                                remoteSnapshotPath =
                                        String.format("%s/%d/%d", remoteSnapshotPath, remoteTabletId, schemaHash);
                                TNetworkAddress remoteBeAddr = jobInfo.getBeAddr(remoteBeId);
                                if (remoteBeAddr == null) {
                                    status = new Status(ErrCode.NOT_FOUND,
                                            "failed to get remote be address of be: " + remoteBeId);
                                    return;
                                }
                                String remoteToken = jobInfo.getToken();

                                remoteTabletSnapshot.setLocalTabletId(localTabletId);
                                remoteTabletSnapshot.setLocalSnapshotPath(localSnapshotPath);
                                remoteTabletSnapshot.setRemoteTabletId(remoteTabletId);
                                remoteTabletSnapshot.setRemoteBeId(remoteBeId);
                                remoteTabletSnapshot.setRemoteBeAddr(remoteBeAddr);
                                remoteTabletSnapshot.setRemoteSnapshotPath(remoteSnapshotPath);
                                remoteTabletSnapshot.setRemoteToken(remoteToken);

                                remoteTabletSnapshots.add(remoteTabletSnapshot);
                            } finally {
                                olapTbl.readUnlock();
                            }
                        }
                        long signature = env.getNextId();
                        DownloadTask task = new DownloadTask(null, beId, signature, jobId, dbId, remoteTabletSnapshots);
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

            env.getEditLog().logRestoreJob(this);
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
            long signature = env.getNextId();
            DirMoveTask task = new DirMoveTask(null, cell.getColumnKey(), signature, jobId, dbId, info.getTblId(),
                    info.getPartitionId(), info.getTabletId(), cell.getRowKey(), info.getTabletPath(),
                    info.getSchemaHash(), true /* need reload tablet header */);
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
        Database db = env.getInternalCatalog().getDbNullable(dbId);
        if (db == null) {
            return new Status(ErrCode.NOT_FOUND, "database " + dbId + " does not exist");
        }

        // set all restored partition version and version hash
        // set all tables' state to NORMAL
        setTableStateToNormal(db, true, isReplay);
        for (long tblId : restoredVersionInfo.rowKeySet()) {
            Table tbl = db.getTableNullable(tblId);
            if (tbl == null) {
                continue;
            }
            OlapTable olapTbl = (OlapTable) tbl;
            if (!tbl.writeLockIfExist()) {
                continue;
            }
            try {
                Map<Long, Long> map = restoredVersionInfo.rowMap().get(tblId);
                for (Map.Entry<Long, Long> entry : map.entrySet()) {
                    long partId = entry.getKey();
                    Partition part = olapTbl.getPartition(partId);
                    if (part == null) {
                        continue;
                    }

                    // update partition visible version
                    part.updateVersionForRestore(entry.getValue());

                    // we also need to update the replica version of these overwritten restored partitions
                    for (MaterializedIndex idx : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
                        for (Tablet tablet : idx.getTablets()) {
                            for (Replica replica : tablet.getReplicas()) {
                                if (!replica.checkVersionCatchUp(part.getVisibleVersion(), false)) {
                                    replica.updateVersionInfo(part.getVisibleVersion(), replica.getDataSize(),
                                            replica.getRemoteDataSize(), replica.getRowCount());
                                }
                            }
                        }
                    }

                    LOG.debug("restore set partition {} version in table {}, version: {}",
                            partId, tblId, entry.getValue());
                }
            } finally {
                tbl.writeUnlock();
            }
        }

        if (!isReplay) {
            restoredPartitions.clear();
            restoredTbls.clear();
            restoredResources.clear();

            // release snapshot before clearing snapshotInfos
            releaseSnapshots();

            snapshotInfos.clear();

            finishedTime = System.currentTimeMillis();
            state = RestoreJobState.FINISHED;

            env.getEditLog().logRestoreJob(this);
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

    public List<String> getBriefInfo() {
        return getInfo(true);
    }

    public List<String> getFullInfo() {
        return getInfo(false);
    }

    public List<String> getInfo(boolean isBrief) {
        List<String> info = Lists.newArrayList();
        info.add(String.valueOf(jobId));
        info.add(label);
        info.add(backupTimestamp);
        info.add(dbName);
        info.add(state.name());
        info.add(String.valueOf(allowLoad));
        info.add(String.valueOf(replicaAlloc.getTotalReplicaNum()));
        info.add(replicaAlloc.toCreateStmt());
        info.add(String.valueOf(reserveReplica));
        info.add(String.valueOf(reserveDynamicPartitionEnable));
        if (!isBrief) {
            info.add(getRestoreObjs());
        }
        info.add(TimeUtils.longToTimeString(createTime));
        info.add(TimeUtils.longToTimeString(metaPreparedTime));
        info.add(TimeUtils.longToTimeString(snapshotFinishedTime));
        info.add(TimeUtils.longToTimeString(downloadFinishedTime));
        info.add(TimeUtils.longToTimeString(finishedTime));
        info.add(Joiner.on(", ").join(unfinishedSignatureToId.entrySet()));
        if (!isBrief) {
            info.add(Joiner.on(", ").join(taskProgress.entrySet().stream().map(
                    e -> "[" + e.getKey() + ": " + e.getValue().first + "/" + e.getValue().second + "]").collect(
                    Collectors.toList())));
            info.add(Joiner.on(", ").join(taskErrMsg.entrySet().stream().map(n -> "[" + n.getKey() + ": "
                    + n.getValue() + "]").collect(Collectors.toList())));
        }
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
        Database db = env.getInternalCatalog().getDbNullable(dbId);
        if (db != null) {
            // rollback table's state to NORMAL
            setTableStateToNormal(db, false, isReplay);

            // remove restored tbls
            for (Table restoreTbl : restoredTbls) {
                LOG.info("remove restored table when cancelled: {}", restoreTbl.getName());
                if (db.writeLockIfExist()) {
                    try {
                        if (restoreTbl.getType() == TableType.OLAP) {
                            OlapTable restoreOlapTable = (OlapTable) restoreTbl;
                            restoreOlapTable.writeLock();
                            try {
                                for (Partition part : restoreOlapTable.getPartitions()) {
                                    for (MaterializedIndex idx : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
                                        for (Tablet tablet : idx.getTablets()) {
                                            Env.getCurrentInvertedIndex().deleteTablet(tablet.getId());
                                        }
                                    }
                                }
                                db.dropTable(restoreTbl.getName());
                            } finally {
                                restoreTbl.writeUnlock();
                            }
                        }
                    } finally {
                        db.writeUnlock();
                    }
                }

            }

            // remove restored partitions
            for (Pair<String, Partition> entry : restoredPartitions) {
                OlapTable restoreTbl = (OlapTable) db.getTableNullable(entry.first);
                if (restoreTbl == null) {
                    continue;
                }
                if (!restoreTbl.writeLockIfExist()) {
                    continue;
                }
                LOG.info("remove restored partition in table {} when cancelled: {}",
                        restoreTbl.getName(), entry.second.getName());
                try {
                    restoreTbl.dropPartition(dbId, entry.second.getName(), true /* force drop */);
                } finally {
                    restoreTbl.writeUnlock();
                }
            }

            // remove restored resource
            ResourceMgr resourceMgr = Env.getCurrentEnv().getResourceMgr();
            for (Resource resource : restoredResources) {
                LOG.info("remove restored resource when cancelled: {}", resource.getName());
                resourceMgr.dropResource(resource);
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
            env.getEditLog().logRestoreJob(this);

            LOG.info("finished to cancel restore job. current state: {}. is replay: {}. {}",
                     curState.name(), isReplay, this);
            return;
        }

        LOG.info("finished to cancel restore job. is replay: {}. {}", isReplay, this);
    }

    private void setTableStateToNormal(Database db, boolean committed, boolean isReplay) {
        for (String tableName : jobInfo.backupOlapTableObjects.keySet()) {
            Table tbl = db.getTableNullable(jobInfo.getAliasByOriginNameIfSet(tableName));
            if (tbl == null) {
                continue;
            }

            if (tbl.getType() != TableType.OLAP) {
                continue;
            }

            OlapTable olapTbl = (OlapTable) tbl;
            if (!tbl.writeLockIfExist()) {
                continue;
            }
            try {
                if (olapTbl.getState() == OlapTableState.RESTORE
                        || olapTbl.getState() == OlapTableState.RESTORE_WITH_LOAD) {
                    olapTbl.setState(OlapTableState.NORMAL);
                }

                BackupOlapTableInfo tblInfo = jobInfo.backupOlapTableObjects.get(tableName);
                for (Map.Entry<String, BackupPartitionInfo> partitionEntry : tblInfo.partitions.entrySet()) {
                    String partitionName = partitionEntry.getKey();
                    Partition partition = olapTbl.getPartition(partitionName);
                    if (partition == null) {
                        continue;
                    }
                    if (partition.getState() == PartitionState.RESTORE) {
                        partition.setState(PartitionState.NORMAL);
                    }
                }
                if (committed && reserveDynamicPartitionEnable) {
                    if (DynamicPartitionUtil.isDynamicPartitionTable(tbl)) {
                        DynamicPartitionUtil.registerOrRemoveDynamicPartitionTable(db.getId(), olapTbl, isReplay);
                        Env.getCurrentEnv().getDynamicPartitionScheduler().createOrUpdateRuntimeInfo(tbl.getId(),
                                DynamicPartitionScheduler.LAST_UPDATE_TIME, TimeUtils.getCurrentFormatTime());
                    }
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

        replicaAlloc.write(out);

        out.writeInt(restoredPartitions.size());
        for (Pair<String, Partition> entry : restoredPartitions) {
            Text.writeString(out, entry.first);
            entry.second.write(out);
        }

        out.writeInt(restoredTbls.size());
        for (Table tbl : restoredTbls) {
            tbl.write(out);
        }

        out.writeInt(restoredVersionInfo.rowKeySet().size());
        for (long tblId : restoredVersionInfo.rowKeySet()) {
            out.writeLong(tblId);
            out.writeInt(restoredVersionInfo.row(tblId).size());
            for (Map.Entry<Long, Long> entry : restoredVersionInfo.row(tblId).entrySet()) {
                out.writeLong(entry.getKey());
                out.writeLong(entry.getValue());
                // It is version hash in the past,
                // but it useless but should compatible with old version so that write 0 here
                out.writeLong(0L);
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

        out.writeInt(restoredResources.size());
        for (Resource resource : restoredResources) {
            resource.write(out);
        }

        // write properties
        out.writeInt(properties.size());
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
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

        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_105) {
            int restoreReplicationNum = in.readInt();
            replicaAlloc = new ReplicaAllocation((short) restoreReplicationNum);
        } else {
            replicaAlloc = ReplicaAllocation.read(in);
        }

        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String tblName = Text.readString(in);
            Partition part = Partition.read(in);
            restoredPartitions.add(Pair.of(tblName, part));
        }

        size = in.readInt();
        for (int i = 0; i < size; i++) {
            restoredTbls.add(Table.read(in));
        }

        size = in.readInt();
        for (int i = 0; i < size; i++) {
            long tblId = in.readLong();
            int innerSize = in.readInt();
            for (int j = 0; j < innerSize; j++) {
                long partId = in.readLong();
                long version = in.readLong();
                // Useless but read it to compatible with meta
                long versionHash = in.readLong(); // CHECKSTYLE IGNORE THIS LINE
                restoredVersionInfo.put(tblId, partId, version);
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

        // restored resource
        size = in.readInt();
        for (int i = 0; i < size; i++) {
            restoredResources.add(Resource.read(in));
        }

        // read properties
        size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            properties.put(key, value);
        }
        reserveReplica = Boolean.parseBoolean(properties.get(PROP_RESERVE_REPLICA));
        reserveDynamicPartitionEnable = Boolean.parseBoolean(properties.get(PROP_RESERVE_DYNAMIC_PARTITION_ENABLE));
        isBeingSynced = Boolean.parseBoolean(properties.get(PROP_IS_BEING_SYNCED));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.append(", backup ts: ").append(backupTimestamp);
        sb.append(", state: ").append(state.name());
        return sb.toString();
    }
}

