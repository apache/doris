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

import org.apache.doris.analysis.BackupStmt;
import org.apache.doris.analysis.BackupStmt.BackupContent;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.backup.Status.ErrCode;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.View;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.property.S3ClientBEProperties;
import org.apache.doris.persist.BarrierLog;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.ReleaseSnapshotTask;
import org.apache.doris.task.SnapshotTask;
import org.apache.doris.task.UploadTask;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTaskType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class BackupJob extends AbstractJob {
    private static final Logger LOG = LogManager.getLogger(BackupJob.class);
    private static final String TABLE_COMMIT_SEQ_PREFIX = "table_commit_seq:";

    public enum BackupJobState {
        PENDING, // Job is newly created. Send snapshot tasks and save copied meta info, then transfer to SNAPSHOTING
        SNAPSHOTING, // Wait for finishing snapshot tasks. When finished, transfer to UPLOAD_SNAPSHOT
        UPLOAD_SNAPSHOT, // Begin to send upload task to BE, then transfer to UPLOADING
        UPLOADING, // Wait for finishing upload tasks. When finished, transfer to SAVE_META
        SAVE_META, // Save copied meta info to local file. When finished, transfer to UPLOAD_INFO
        UPLOAD_INFO, // Upload meta and job info file to repository. When finished, transfer to FINISHED
        FINISHED, // Job is finished.
        CANCELLED // Job is cancelled.
    }

    // all objects which need backup
    @SerializedName(value = "tableRefs")
    private List<TableRef> tableRefs = Lists.newArrayList();

    @SerializedName(value = "state")
    private BackupJobState state;

    @SerializedName(value = "snapshotFinishedTime")
    private long snapshotFinishedTime = -1;
    @SerializedName(value = "snapshotUploadFinishedTime")
    private long snapshotUploadFinishedTime = -1;

    // save task id map to the backend it be executed
    @SerializedName(value = "unfinishedTaskIds")
    private Map<Long, Long> unfinishedTaskIds = Maps.newConcurrentMap();
    // tablet id -> snapshot info
    @SerializedName(value = "snapshotInfos")
    private Map<Long, SnapshotInfo> snapshotInfos = Maps.newConcurrentMap();
    // save all related table[partition] info
    @SerializedName(value = "backupMeta")
    private BackupMeta backupMeta;
    // job info file content
    @SerializedName(value = "jobInfo")
    private BackupJobInfo jobInfo;

    // save the local dir of this backup job
    // after job is done, this dir should be deleted
    @SerializedName(value = "localJobDirPath")
    private Path localJobDirPath = null;
    // save the local file path of meta info and job info file
    @SerializedName(value = "localMetaInfoFilePath")
    private String localMetaInfoFilePath = null;
    @SerializedName(value = "localJobInfoFilePath")
    private String localJobInfoFilePath = null;
    // backup properties && table commit seq with table id
    @SerializedName(value = "properties")
    private Map<String, String> properties = Maps.newHashMap();

    @SerializedName(value = "metaInfoBytes")
    private byte[] metaInfoBytes = null;
    @SerializedName(value = "jobInfoBytes")
    private byte[] jobInfoBytes = null;

    public BackupJob() {
        super(JobType.BACKUP);
    }

    public BackupJob(String label, long dbId, String dbName, List<TableRef> tableRefs, long timeoutMs,
                     BackupContent content, Env env, long repoId) {
        super(JobType.BACKUP, label, dbId, dbName, timeoutMs, env, repoId);
        this.tableRefs = tableRefs;
        this.state = BackupJobState.PENDING;
        properties.put(BackupStmt.PROP_CONTENT, content.name());
    }

    public BackupJobState getState() {
        return state;
    }

    public BackupMeta getBackupMeta() {
        return backupMeta;
    }

    public BackupJobInfo getJobInfo() {
        return jobInfo;
    }

    public String getLocalJobInfoFilePath() {
        return localJobInfoFilePath;
    }

    public String getLocalMetaInfoFilePath() {
        return localMetaInfoFilePath;
    }

    public BackupContent getContent() {
        if (properties.containsKey(BackupStmt.PROP_CONTENT)) {
            return BackupStmt.BackupContent.valueOf(properties.get(BackupStmt.PROP_CONTENT).toUpperCase());
        }
        return BackupContent.ALL;
    }

    public synchronized boolean finishTabletSnapshotTask(SnapshotTask task, TFinishTaskRequest request) {
        Preconditions.checkState(task.getJobId() == jobId);

        if (request.getTaskStatus().getStatusCode() != TStatusCode.OK) {
            taskErrMsg.put(task.getSignature(), Joiner.on(",").join(request.getTaskStatus().getErrorMsgs()));
            // snapshot task could not finish if status_code is OLAP_ERR_VERSION_ALREADY_MERGED,
            // so cancel this job
            if (request.getTaskStatus().getStatusCode() == TStatusCode.OLAP_ERR_VERSION_ALREADY_MERGED) {
                status = new Status(ErrCode.OLAP_VERSION_ALREADY_MERGED,
                        "make snapshot failed, version already merged");
                cancelInternal();
            }
            return false;
        }

        Preconditions.checkState(request.isSetSnapshotPath());
        Preconditions.checkState(request.isSetSnapshotFiles());
        // snapshot path does not contains last 'tablet_id' and 'schema_hash' dir
        // eg:
        //      /path/to/your/be/data/snapshot/20180410102311.0.86400/
        // Full path will look like:
        //      /path/to/your/be/data/snapshot/20180410102311.0.86400/10006/352781111/
        SnapshotInfo info = new SnapshotInfo(task.getDbId(), task.getTableId(), task.getPartitionId(),
                task.getIndexId(), task.getTabletId(), task.getBackendId(),
                task.getSchemaHash(), request.getSnapshotPath(),
                request.getSnapshotFiles());

        snapshotInfos.put(task.getTabletId(), info);
        taskProgress.remove(task.getTabletId());
        Long oldValue = unfinishedTaskIds.remove(task.getTabletId());
        taskErrMsg.remove(task.getTabletId());
        LOG.debug("get finished snapshot info: {}, unfinished tasks num: {}, remove result: {}. {}",
                info, unfinishedTaskIds.size(), (oldValue != null), this);

        return oldValue != null;
    }

    public synchronized boolean finishSnapshotUploadTask(UploadTask task, TFinishTaskRequest request) {
        Preconditions.checkState(task.getJobId() == jobId);

        if (request.getTaskStatus().getStatusCode() != TStatusCode.OK) {
            taskErrMsg.put(task.getSignature(), Joiner.on(",").join(request.getTaskStatus().getErrorMsgs()));
            return false;
        }

        Preconditions.checkState(request.isSetTabletFiles());
        Map<Long, List<String>> tabletFileMap = request.getTabletFiles();
        if (tabletFileMap.isEmpty()) {
            LOG.warn("upload snapshot files failed because nothing is uploaded. be: {}. {}",
                     task.getBackendId(), this);
            return false;
        }

        // remove checksum suffix in reported file name before checking files
        Map<Long, List<String>> newTabletFileMap = Maps.newHashMap();
        for (Map.Entry<Long, List<String>> entry : tabletFileMap.entrySet()) {
            List<String> files = entry.getValue().stream()
                    .map(name -> Repository.decodeFileNameWithChecksum(name).first).collect(Collectors.toList());
            newTabletFileMap.put(entry.getKey(), files);
        }

        // check if uploaded files are correct
        for (long tabletId : newTabletFileMap.keySet()) {
            SnapshotInfo info = snapshotInfos.get(tabletId);
            List<String> tabletFiles = info.getFiles();
            List<String> uploadedFiles = newTabletFileMap.get(tabletId);

            if (tabletFiles.size() != uploadedFiles.size()) {
                LOG.warn("upload snapshot files failed because file num is wrong. "
                        + "expect: {}, actual:{}, tablet: {}, be: {}. {}",
                         tabletFiles.size(), uploadedFiles.size(), tabletId, task.getBackendId(), this);
                return false;
            }

            if (!Collections2.filter(tabletFiles, Predicates.not(Predicates.in(uploadedFiles))).isEmpty()) {
                LOG.warn("upload snapshot files failed because file is different. "
                        + "expect: [{}], actual: [{}], tablet: {}, be: {}. {}",
                         tabletFiles, uploadedFiles, tabletId, task.getBackendId(), this);
                return false;
            }

            // reset files in snapshot info with checksum filename
            info.setFiles(tabletFileMap.get(tabletId));
        }

        taskProgress.remove(task.getSignature());
        Long oldValue = unfinishedTaskIds.remove(task.getSignature());
        taskErrMsg.remove(task.getSignature());
        LOG.debug("get finished upload snapshot task, unfinished tasks num: {}, remove result: {}. {}",
                unfinishedTaskIds.size(), (oldValue != null), this);
        return oldValue != null;
    }

    @Override
    public synchronized void replayRun() {
        // Backup process does not change any current catalog state,
        // So nothing need to be done when replaying log
    }

    @Override
    public synchronized void replayCancel() {
        // nothing to do
    }

    @Override
    public boolean isPending() {
        return state == BackupJobState.PENDING;
    }

    @Override
    public boolean isCancelled() {
        return state == BackupJobState.CANCELLED;
    }

    // Polling the job state and do the right things.
    @Override
    public synchronized void run() {
        if (state == BackupJobState.FINISHED || state == BackupJobState.CANCELLED) {
            return;
        }

        // check timeout
        if (System.currentTimeMillis() - createTime > timeoutMs) {
            status = new Status(ErrCode.TIMEOUT, "");
            cancelInternal();
            return;
        }

        // get repo if not set
        if (repo == null && repoId != Repository.KEEP_ON_LOCAL_REPO_ID) {
            repo = env.getBackupHandler().getRepoMgr().getRepo(repoId);
            if (repo == null) {
                status = new Status(ErrCode.COMMON_ERROR, "failed to get repository: " + repoId);
                cancelInternal();
                return;
            }
        }

        LOG.debug("run backup job: {}", this);

        // run job base on current state
        switch (state) {
            case PENDING:
                prepareAndSendSnapshotTask();
                break;
            case SNAPSHOTING:
                waitingAllSnapshotsFinished();
                break;
            case UPLOAD_SNAPSHOT:
                uploadSnapshot();
                break;
            case UPLOADING:
                waitingAllUploadingFinished();
                break;
            case SAVE_META:
                saveMetaInfo();
                break;
            case UPLOAD_INFO:
                uploadMetaAndJobInfoFile();
                break;
            default:
                break;
        }

        // we don't want to cancel the job if we already in state UPLOAD_INFO,
        // which is the final step of backup job. just retry it.
        // if it encounters some unrecoverable errors, just retry it until timeout.
        if (!status.ok() && state != BackupJobState.UPLOAD_INFO) {
            cancelInternal();
        }
    }

    // cancel by user
    @Override
    public synchronized Status cancel() {
        if (isDone()) {
            return new Status(ErrCode.COMMON_ERROR,
                    "Job with label " + label + " can not be cancelled. state: " + state);
        }

        status = new Status(ErrCode.COMMON_ERROR, "user cancelled");
        cancelInternal();
        return Status.OK;
    }

    @Override
    public synchronized boolean isDone() {
        return state == BackupJobState.FINISHED || state == BackupJobState.CANCELLED;
    }

    private void prepareAndSendSnapshotTask() {
        Database db = env.getInternalCatalog().getDbNullable(dbId);
        if (db == null) {
            status = new Status(ErrCode.NOT_FOUND, "database " + dbId + " does not exist");
            return;
        }

        // generate job id
        jobId = env.getNextId();
        unfinishedTaskIds.clear();
        taskProgress.clear();
        taskErrMsg.clear();
        // copy all related schema at this moment
        List<Table> copiedTables = Lists.newArrayList();
        List<Resource> copiedResources = Lists.newArrayList();
        AgentBatchTask batchTask = new AgentBatchTask();
        for (TableRef tableRef : tableRefs) {
            String tblName = tableRef.getName().getTbl();
            Table tbl = db.getTableNullable(tblName);
            if (tbl == null) {
                status = new Status(ErrCode.NOT_FOUND, "table " + tblName + " does not exist");
                return;
            }
            tbl.readLock();
            try {
                switch (tbl.getType()) {
                    case OLAP:
                        OlapTable olapTable = (OlapTable) tbl;
                        checkOlapTable(olapTable, tableRef);
                        if (getContent() == BackupContent.ALL) {
                            prepareSnapshotTaskForOlapTableWithoutLock(db, (OlapTable) tbl, tableRef, batchTask);
                        }
                        prepareBackupMetaForOlapTableWithoutLock(tableRef, olapTable, copiedTables);
                        break;
                    case VIEW:
                        prepareBackupMetaForViewWithoutLock((View) tbl, copiedTables);
                        break;
                    case ODBC:
                        prepareBackupMetaForOdbcTableWithoutLock((OdbcTable) tbl, copiedTables, copiedResources);
                        break;
                    default:
                        status = new Status(ErrCode.COMMON_ERROR,
                                "backup job does not support this type of table " + tblName);
                        return;
                }
            } finally {
                tbl.readUnlock();
            }
        }

        backupMeta = new BackupMeta(copiedTables, copiedResources);

        // send tasks
        for (AgentTask task : batchTask.getAllTasks()) {
            AgentTaskQueue.addTask(task);
        }
        AgentTaskExecutor.submit(batchTask);

        state = BackupJobState.SNAPSHOTING;

        // DO NOT write log here, state will be reset to PENDING after FE restart. Then all snapshot tasks
        // will be re-generated and be sent again
        LOG.info("finished to send snapshot tasks to backend. {}", this);
    }

    private void checkOlapTable(OlapTable olapTable, TableRef backupTableRef) {
        olapTable.readLock();
        try {
            // check backup table again
            if (backupTableRef.getPartitionNames() != null) {
                for (String partName : backupTableRef.getPartitionNames().getPartitionNames()) {
                    Partition partition = olapTable.getPartition(partName);
                    if (partition == null) {
                        status = new Status(ErrCode.NOT_FOUND, "partition " + partName
                                + " does not exist  in table" + backupTableRef.getName().getTbl());
                        return;
                    }
                }
            }
        }  finally {
            olapTable.readUnlock();
        }
    }

    private void prepareSnapshotTaskForOlapTableWithoutLock(Database db, OlapTable olapTable,
            TableRef backupTableRef, AgentBatchTask batchTask) {
        // Add barrier editolog for barrier commit seq
        long dbId = db.getId();
        String dbName = db.getFullName();
        long tableId = olapTable.getId();
        String tableName = olapTable.getName();
        BarrierLog barrierLog = new BarrierLog(dbId, dbName, tableId, tableName);
        long commitSeq = env.getEditLog().logBarrier(barrierLog);
        // format as "table:{tableId}"
        String tableKey = String.format("%s%d", TABLE_COMMIT_SEQ_PREFIX, olapTable.getId());
        properties.put(tableKey, String.valueOf(commitSeq));

        // check backup table again
        if (backupTableRef.getPartitionNames() != null) {
            for (String partName : backupTableRef.getPartitionNames().getPartitionNames()) {
                Partition partition = olapTable.getPartition(partName);
                if (partition == null) {
                    status = new Status(ErrCode.NOT_FOUND, "partition " + partName
                            + " does not exist  in table" + backupTableRef.getName().getTbl());
                    return;
                }
            }
        }

        // create snapshot tasks
        List<Partition> partitions = Lists.newArrayList();
        if (backupTableRef.getPartitionNames() == null) {
            partitions.addAll(olapTable.getPartitions());
        } else {
            for (String partName : backupTableRef.getPartitionNames().getPartitionNames()) {
                Partition partition = olapTable.getPartition(partName);
                partitions.add(partition);
            }
        }

        // snapshot partitions
        for (Partition partition : partitions) {
            long visibleVersion = partition.getVisibleVersion();
            List<MaterializedIndex> indexes = partition.getMaterializedIndices(IndexExtState.VISIBLE);
            for (MaterializedIndex index : indexes) {
                int schemaHash = olapTable.getSchemaHashByIndexId(index.getId());
                List<Tablet> tablets = index.getTablets();
                for (Tablet tablet : tablets) {
                    Replica replica = chooseReplica(tablet, visibleVersion);
                    if (replica == null) {
                        status = new Status(ErrCode.COMMON_ERROR,
                                "failed to choose replica to make snapshot for tablet " + tablet.getId()
                                        + ". visible version: " + visibleVersion);
                        return;
                    }
                    SnapshotTask task = new SnapshotTask(null, replica.getBackendId(), tablet.getId(),
                            jobId, dbId, olapTable.getId(), partition.getId(),
                            index.getId(), tablet.getId(),
                            visibleVersion,
                            schemaHash, timeoutMs, false /* not restore task */);
                    batchTask.addTask(task);
                    unfinishedTaskIds.put(tablet.getId(), replica.getBackendId());
                }
            }

            LOG.info("snapshot for partition {}, version: {}",
                    partition.getId(), visibleVersion);
        }
    }

    private void checkResourceForOdbcTable(OdbcTable odbcTable) {
        if (odbcTable.getOdbcCatalogResourceName() != null) {
            String odbcResourceName = odbcTable.getOdbcCatalogResourceName();
            Resource resource = Env.getCurrentEnv().getResourceMgr()
                    .getResource(odbcResourceName);
            if (resource == null) {
                status = new Status(ErrCode.NOT_FOUND, "resource " + odbcResourceName
                        + " related to " + odbcTable.getName() + "does not exist.");
                return;
            }
        }
    }

    private void prepareBackupMetaForOlapTableWithoutLock(TableRef tableRef, OlapTable olapTable,
                                                          List<Table> copiedTables) {
        // only copy visible indexes
        List<String> reservedPartitions = tableRef.getPartitionNames() == null ? null
                : tableRef.getPartitionNames().getPartitionNames();
        OlapTable copiedTbl = olapTable.selectiveCopy(reservedPartitions, IndexExtState.VISIBLE, true);
        if (copiedTbl == null) {
            status = new Status(ErrCode.COMMON_ERROR, "failed to copy table: " + olapTable.getName());
            return;
        }

        removeUnsupportProperties(copiedTbl);
        copiedTables.add(copiedTbl);
    }

    private void prepareBackupMetaForViewWithoutLock(View view, List<Table> copiedTables) {
        View copiedView = view.clone();
        if (copiedView == null) {
            status = new Status(ErrCode.COMMON_ERROR, "failed to copy view: " + view.getName());
            return;
        }
        copiedTables.add(copiedView);
    }

    private void prepareBackupMetaForOdbcTableWithoutLock(OdbcTable odbcTable, List<Table> copiedTables,
            List<Resource> copiedResources) {
        OdbcTable copiedOdbcTable = odbcTable.clone();
        if (copiedOdbcTable == null) {
            status = new Status(ErrCode.COMMON_ERROR, "failed to copy odbc table: " + odbcTable.getName());
            return;
        }
        copiedTables.add(copiedOdbcTable);
        if (copiedOdbcTable.getOdbcCatalogResourceName() != null) {
            Resource resource = Env.getCurrentEnv().getResourceMgr()
                    .getResource(copiedOdbcTable.getOdbcCatalogResourceName());
            Resource copiedResource = resource.clone();
            if (copiedResource == null) {
                status = new Status(ErrCode.COMMON_ERROR, "failed to copy odbc resource: "
                        + resource.getName());
                return;
            }
            copiedResources.add(copiedResource);
        }
    }

    private void removeUnsupportProperties(OlapTable tbl) {
        // We cannot support the colocate attribute because the colocate information is not backed up
        // synchronously when backing up.
        tbl.setColocateGroup(null);
    }

    private void waitingAllSnapshotsFinished() {
        if (unfinishedTaskIds.isEmpty()) {
            snapshotFinishedTime = System.currentTimeMillis();
            state = BackupJobState.UPLOAD_SNAPSHOT;

            // log
            env.getEditLog().logBackupJob(this);
            LOG.info("finished to make snapshots. {}", this);
            return;
        }

        LOG.info("waiting {} tablets to make snapshot. {}", unfinishedTaskIds.size(), this);
    }

    private void uploadSnapshot() {
        if (repoId == Repository.KEEP_ON_LOCAL_REPO_ID) {
            state = BackupJobState.UPLOADING;
            return;
        }

        // reuse this set to save all unfinished tablets
        unfinishedTaskIds.clear();
        taskProgress.clear();
        taskErrMsg.clear();

        // We classify the snapshot info by backend
        ArrayListMultimap<Long, SnapshotInfo> beToSnapshots = ArrayListMultimap.create();
        for (SnapshotInfo info : snapshotInfos.values()) {
            beToSnapshots.put(info.getBeId(), info);
        }

        AgentBatchTask batchTask = new AgentBatchTask();
        for (Long beId : beToSnapshots.keySet()) {
            List<SnapshotInfo> infos = beToSnapshots.get(beId);
            int totalNum = infos.size();
            // each backend allot at most 3 tasks
            int batchNum = Math.min(totalNum, 3);
            // each task contains several upload sub tasks
            int taskNumPerBatch = Math.max(totalNum / batchNum, 1);
            LOG.info("backend {} has {} batch, total {} tasks, {}", beId, batchNum, totalNum, this);

            List<FsBroker> brokers = Lists.newArrayList();
            Status st = repo.getBrokerAddress(beId, env, brokers);
            if (!st.ok()) {
                status = st;
                return;
            }
            Preconditions.checkState(brokers.size() == 1);

            // allot tasks
            int index = 0;
            for (int batch = 0; batch < batchNum; batch++) {
                Map<String, String> srcToDest = Maps.newHashMap();
                int currentBatchTaskNum = (batch == batchNum - 1) ? totalNum - index : taskNumPerBatch;
                for (int j = 0; j < currentBatchTaskNum; j++) {
                    SnapshotInfo info = infos.get(index++);
                    String src = info.getTabletPath();
                    String dest = repo.getRepoTabletPathBySnapshotInfo(label, info);
                    if (dest == null) {
                        status = new Status(ErrCode.COMMON_ERROR, "Invalid dest path: " + info);
                        return;
                    }
                    srcToDest.put(src, dest);
                }
                long signature = env.getNextId();
                UploadTask task = new UploadTask(null, beId, signature, jobId, dbId, srcToDest,
                        brokers.get(0),
                        S3ClientBEProperties.getBeFSProperties(repo.getRemoteFileSystem().getProperties()),
                        repo.getRemoteFileSystem().getStorageType(), repo.getLocation());
                batchTask.addTask(task);
                unfinishedTaskIds.put(signature, beId);
            }
        }

        // send tasks
        for (AgentTask task : batchTask.getAllTasks()) {
            AgentTaskQueue.addTask(task);
        }
        AgentTaskExecutor.submit(batchTask);

        state = BackupJobState.UPLOADING;

        // DO NOT write log here, upload tasks will be resend after FE crashed.
        LOG.info("finished to send upload tasks. {}", this);
    }

    private void waitingAllUploadingFinished() {
        if (unfinishedTaskIds.isEmpty()) {
            snapshotUploadFinishedTime = System.currentTimeMillis();
            state = BackupJobState.SAVE_META;

            // log
            env.getEditLog().logBackupJob(this);
            LOG.info("finished uploading snapshots. {}", this);
            return;
        }

        LOG.debug("waiting {} tablets to upload snapshot. {}", unfinishedTaskIds.size(), this);
    }

    private void saveMetaInfo() {
        String createTimeStr = TimeUtils.longToTimeString(createTime, TimeUtils.DATETIME_FORMAT_WITH_HYPHEN);
        // local job dir: backup/label__createtime/
        localJobDirPath = Paths.get(BackupHandler.BACKUP_ROOT_DIR.toString(),
                                    label + "__" + createTimeStr).normalize();

        try {
            // 1. create local job dir of this backup job
            File jobDir = new File(localJobDirPath.toString());
            if (jobDir.exists()) {
                // if dir exists, delete it first
                Files.walk(localJobDirPath, FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder())
                        .map(Path::toFile).forEach(File::delete);
            }
            if (!jobDir.mkdirs()) {
                status = new Status(ErrCode.COMMON_ERROR, "Failed to create tmp dir: " + localJobDirPath);
                return;
            }

            // 2. save meta info file
            File metaInfoFile = new File(jobDir, Repository.FILE_META_INFO);
            if (!metaInfoFile.createNewFile()) {
                status = new Status(ErrCode.COMMON_ERROR,
                        "Failed to create meta info file: " + metaInfoFile.toString());
                return;
            }
            backupMeta.writeToFile(metaInfoFile);
            localMetaInfoFilePath = metaInfoFile.getAbsolutePath();
            // read meta info to metaInfoBytes
            metaInfoBytes = Files.readAllBytes(metaInfoFile.toPath());

            // 3. save job info file
            Map<Long, Long> tableCommitSeqMap = Maps.newHashMap();
            // iterate properties, convert key, value from string to long
            // key is "${TABLE_COMMIT_SEQ_PREFIX}{tableId}", only need tableId to long
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (key.startsWith(TABLE_COMMIT_SEQ_PREFIX)) {
                    long tableId = Long.parseLong(key.substring(TABLE_COMMIT_SEQ_PREFIX.length()));
                    long commitSeq = Long.parseLong(value);
                    tableCommitSeqMap.put(tableId, commitSeq);
                }
            }
            jobInfo = BackupJobInfo.fromCatalog(createTime, label, dbName, dbId,
                    getContent(), backupMeta, snapshotInfos, tableCommitSeqMap);
            LOG.debug("job info: {}. {}", jobInfo, this);
            File jobInfoFile = new File(jobDir, Repository.PREFIX_JOB_INFO + createTimeStr);
            if (!jobInfoFile.createNewFile()) {
                status = new Status(ErrCode.COMMON_ERROR, "Failed to create job info file: " + jobInfoFile.toString());
                return;
            }
            jobInfo.writeToFile(jobInfoFile);
            localJobInfoFilePath = jobInfoFile.getAbsolutePath();
            // read job info to jobInfoBytes
            jobInfoBytes = Files.readAllBytes(jobInfoFile.toPath());
        } catch (Exception e) {
            status = new Status(ErrCode.COMMON_ERROR, "failed to save meta info and job info file: " + e.getMessage());
            return;
        }

        state = BackupJobState.UPLOAD_INFO;

        // meta info and job info has been saved to local file, this can be cleaned to reduce log size
        backupMeta = null;
        jobInfo = null;

        // release all snapshots before clearing the snapshotInfos.
        if (repoId != Repository.KEEP_ON_LOCAL_REPO_ID) {
            releaseSnapshots();
        }

        snapshotInfos.clear();

        // log
        env.getEditLog().logBackupJob(this);
        LOG.info("finished to save meta the backup job info file to local.[{}], [{}] {}",
                 localMetaInfoFilePath, localJobInfoFilePath, this);
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

    private void uploadMetaAndJobInfoFile() {
        if (repoId == Repository.KEEP_ON_LOCAL_REPO_ID) {
            state = BackupJobState.FINISHED;
            Snapshot snapshot = new Snapshot(label, metaInfoBytes, jobInfoBytes);
            env.getBackupHandler().addSnapshot(label, snapshot);
            return;
        }

        String remoteMetaInfoFile = repo.assembleMetaInfoFilePath(label);
        if (!uploadFile(localMetaInfoFilePath, remoteMetaInfoFile)) {
            return;
        }

        String remoteJobInfoFile = repo.assembleJobInfoFilePath(label, createTime);
        if (!uploadFile(localJobInfoFilePath, remoteJobInfoFile)) {
            return;
        }

        finishedTime = System.currentTimeMillis();
        state = BackupJobState.FINISHED;

        // log
        env.getEditLog().logBackupJob(this);
        LOG.info("job is finished. {}", this);
    }

    private boolean uploadFile(String localFilePath, String remoteFilePath) {
        if (!validateLocalFile(localFilePath)) {
            return false;
        }

        status = repo.upload(localFilePath, remoteFilePath);
        if (!status.ok()) {
            return false;
        }
        return true;
    }

    private boolean validateLocalFile(String filePath) {
        File file = new File(filePath);
        if (!file.exists() || !file.canRead()) {
            status = new Status(ErrCode.COMMON_ERROR, "file is invalid: " + filePath);
            return false;
        }
        return true;
    }

    /*
     * Choose a replica order by replica id.
     * This is to expect to choose the same replica at each backup job.
     */
    private Replica chooseReplica(Tablet tablet, long visibleVersion) {
        List<Long> replicaIds = Lists.newArrayList();
        for (Replica replica : tablet.getReplicas()) {
            replicaIds.add(replica.getId());
        }

        Collections.sort(replicaIds);
        for (Long replicaId : replicaIds) {
            Replica replica = tablet.getReplicaById(replicaId);
            if (replica.getLastFailedVersion() < 0 && replica.getVersion() >= visibleVersion) {
                return replica;
            }
        }
        return null;
    }

    private void cancelInternal() {
        // We need to clean the residual due to current state
        switch (state) {
            case SNAPSHOTING:
                // remove all snapshot tasks in AgentTaskQueue
                for (Long taskId : unfinishedTaskIds.keySet()) {
                    AgentTaskQueue.removeTaskOfType(TTaskType.MAKE_SNAPSHOT, taskId);
                }
                break;
            case UPLOADING:
                // remove all upload tasks in AgentTaskQueue
                for (Long taskId : unfinishedTaskIds.keySet()) {
                    AgentTaskQueue.removeTaskOfType(TTaskType.UPLOAD, taskId);
                }
                break;
            default:
                break;
        }

        // clean the backup job dir
        if (localJobDirPath != null) {
            try {
                File jobDir = new File(localJobDirPath.toString());
                if (jobDir.exists()) {
                    Files.walk(localJobDirPath, FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder())
                            .map(Path::toFile).forEach(File::delete);
                }
            } catch (Exception e) {
                LOG.warn("failed to clean the backup job dir: " + localJobDirPath.toString());
            }
        }

        releaseSnapshots();

        BackupJobState curState = state;
        finishedTime = System.currentTimeMillis();
        state = BackupJobState.CANCELLED;

        // log
        env.getEditLog().logBackupJob(this);
        LOG.info("finished to cancel backup job. current state: {}. {}", curState.name(), this);
    }

    public List<String> getInfo() {
        List<String> info = Lists.newArrayList();
        info.add(String.valueOf(jobId));
        info.add(label);
        info.add(dbName);
        info.add(state.name());
        info.add(getBackupObjs());
        info.add(TimeUtils.longToTimeString(createTime));
        info.add(TimeUtils.longToTimeString(snapshotFinishedTime));
        info.add(TimeUtils.longToTimeString(snapshotUploadFinishedTime));
        info.add(TimeUtils.longToTimeString(finishedTime));
        info.add(Joiner.on(", ").join(unfinishedTaskIds.entrySet()));
        info.add(Joiner.on(", ").join(taskProgress.entrySet().stream().map(
                e -> "[" + e.getKey() + ": " + e.getValue().first + "/" + e.getValue().second + "]").collect(
                        Collectors.toList())));
        info.add(Joiner.on(", ").join(taskErrMsg.entrySet().stream().map(n -> "[" + n.getKey() + ": " + n.getValue()
                + "]").collect(Collectors.toList())));
        info.add(status.toString());
        info.add(String.valueOf(timeoutMs / 1000));
        return info;
    }

    private String getBackupObjs() {
        List<String> list = tableRefs.stream().map(n -> "[" + n.toString() + "]").collect(Collectors.toList());
        return Joiner.on(", ").join(list);
    }

    public static BackupJob read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_127) {
            BackupJob job = new BackupJob();
            job.readFields(in);
            return job;
        }
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, BackupJob.class);
    }

    @Deprecated
    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        // table refs
        int size = in.readInt();
        tableRefs = Lists.newArrayList();
        for (int i = 0; i < size; i++) {
            TableRef tblRef = new TableRef();
            tblRef.readFields(in);
            tableRefs.add(tblRef);
        }

        state = BackupJobState.valueOf(Text.readString(in));

        // times
        snapshotFinishedTime = in.readLong();
        snapshotUploadFinishedTime = in.readLong();

        // snapshot info
        size = in.readInt();
        for (int i = 0; i < size; i++) {
            SnapshotInfo snapshotInfo = new SnapshotInfo();
            snapshotInfo.readFields(in);
            snapshotInfos.put(snapshotInfo.getTabletId(), snapshotInfo);
        }

        // backup meta
        if (in.readBoolean()) {
            backupMeta = BackupMeta.read(in);
        }

        // No need to persist job info. It is generated then write to file

        // metaInfoFilePath and jobInfoFilePath
        if (in.readBoolean()) {
            localMetaInfoFilePath = Text.readString(in);
        }

        if (in.readBoolean()) {
            localJobInfoFilePath = Text.readString(in);
        }
        // read properties
        size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            properties.put(key, value);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.append(", state: ").append(state.name());
        return sb.toString();
    }
}

