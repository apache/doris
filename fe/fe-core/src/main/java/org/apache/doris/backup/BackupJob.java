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
import org.apache.doris.common.Config;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.property.S3ClientBEProperties;
import org.apache.doris.persist.BarrierLog;
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
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
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
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


public class BackupJob extends AbstractJob {
    private static final Logger LOG = LogManager.getLogger(BackupJob.class);
    private static final String TABLE_COMMIT_SEQ_PREFIX = "table_commit_seq:";
    private static final String SNAPSHOT_COMMIT_SEQ = "commit_seq";

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
    private List<TableRef> tableRefs = Lists.newArrayList();

    private volatile BackupJobState state;

    private long snapshotFinishedTime = -1;
    private long snapshotUploadFinishedTime = -1;

    // save task id map to the backend it be executed
    private Map<Long, Long> unfinishedTaskIds = Maps.newConcurrentMap();
    // tablet id -> snapshot info
    private Map<Long, SnapshotInfo> snapshotInfos = Maps.newConcurrentMap();
    // save all related table[partition] info
    private BackupMeta backupMeta;
    // job info file content
    private BackupJobInfo jobInfo;

    // save the local dir of this backup job
    // after job is done, this dir should be deleted
    private Path localJobDirPath = null;
    // save the local file path of meta info and job info file
    private String localMetaInfoFilePath = null;
    private String localJobInfoFilePath = null;
    // backup properties && table commit seq with table id
    private Map<String, String> properties = Maps.newHashMap();

    private long commitSeq = 0;

    public BackupJob() {
        super(JobType.BACKUP);
    }

    public BackupJob(JobType jobType) {
        super(jobType);
        assert jobType == JobType.BACKUP || jobType == JobType.BACKUP_COMPRESSED;
    }

    public BackupJob(String label, long dbId, String dbName, List<TableRef> tableRefs, long timeoutMs,
                     BackupContent content, Env env, long repoId, long commitSeq) {
        super(JobType.BACKUP, label, dbId, dbName, timeoutMs, env, repoId);
        this.tableRefs = tableRefs;
        this.state = BackupJobState.PENDING;
        this.commitSeq = commitSeq;
        properties.put(BackupStmt.PROP_CONTENT, content.name());
        properties.put(SNAPSHOT_COMMIT_SEQ, String.valueOf(commitSeq));
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

    private synchronized boolean tryNewTabletSnapshotTask(SnapshotTask task) {
        Table table = env.getInternalCatalog().getTableByTableId(task.getTableId());
        if (table == null) {
            return false;
        }
        OlapTable tbl = (OlapTable) table;
        tbl.readLock();
        try {
            if (tbl.getId() != task.getTableId()) {
                return false;
            }
            Partition partition = tbl.getPartition(task.getPartitionId());
            if (partition == null) {
                return false;
            }
            MaterializedIndex index = partition.getIndex(task.getIndexId());
            if (index == null) {
                return false;
            }
            Tablet tablet = index.getTablet(task.getTabletId());
            if (tablet == null) {
                return false;
            }
            Replica replica = chooseReplica(tablet, task.getVersion());
            if (replica == null) {
                return false;
            }

            //clear old task
            AgentTaskQueue.removeTaskOfType(TTaskType.MAKE_SNAPSHOT, task.getTabletId());
            unfinishedTaskIds.remove(task.getTabletId());
            taskProgress.remove(task.getTabletId());
            taskErrMsg.remove(task.getTabletId());

            SnapshotTask newTask = new SnapshotTask(null, replica.getBackendId(), task.getTabletId(),
                    task.getJobId(), task.getDbId(), tbl.getId(), task.getPartitionId(),
                    task.getIndexId(), task.getTabletId(),
                    task.getVersion(),
                    task.getSchemaHash(), timeoutMs, false /* not restore task */);
            unfinishedTaskIds.put(tablet.getId(), replica.getBackendId());

            //send task
            AgentBatchTask batchTask = new AgentBatchTask(newTask);
            AgentTaskQueue.addTask(newTask);
            AgentTaskExecutor.submit(batchTask);

        } finally {
            tbl.readUnlock();
        }

        return true;
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

            if (request.getTaskStatus().getStatusCode() == TStatusCode.TABLET_MISSING
                    && !tryNewTabletSnapshotTask(task)) {
                status = new Status(ErrCode.NOT_FOUND,
                        "make snapshot failed, failed to ge tablet, table will be dropped or truncated");
                cancelInternal();
            }

            if (request.getTaskStatus().getStatusCode() == TStatusCode.NOT_IMPLEMENTED_ERROR) {
                status = new Status(ErrCode.COMMON_ERROR,
                    "make snapshot failed, currently not support backup tablet with cooldowned remote data");
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("get finished snapshot info: {}, unfinished tasks num: {}, remove result: {}. {}",
                    info, unfinishedTaskIds.size(), (oldValue != null), this);
        }

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
        if (LOG.isDebugEnabled()) {
            LOG.debug("get finished upload snapshot task, unfinished tasks num: {}, remove result: {}. {}",
                    unfinishedTaskIds.size(), (oldValue != null), this);
        }
        return oldValue != null;
    }

    @Override
    public synchronized void replayRun() {
        // nothing to do
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

    @Override
    public boolean isFinished() {
        return state == BackupJobState.FINISHED;
    }

    @Override
    public synchronized Status updateRepo(Repository repo) {
        this.repo = repo;

        if (this.state == BackupJobState.UPLOADING) {
            for (Map.Entry<Long, Long> entry : unfinishedTaskIds.entrySet()) {
                long signature = entry.getKey();
                long beId = entry.getValue();
                AgentTask task = AgentTaskQueue.getTask(beId, TTaskType.UPLOAD, signature);
                if (task == null || task.getTaskType() != TTaskType.UPLOAD) {
                    continue;
                }
                ((UploadTask) task).updateBrokerProperties(
                                S3ClientBEProperties.getBeFSProperties(repo.getRemoteFileSystem().getProperties()));
                AgentTaskQueue.updateTask(beId, TTaskType.UPLOAD, signature, task);
            }
            LOG.info("finished to update upload job properties. {}", this);
        }
        LOG.info("finished to update repo of job. {}", this);
        return Status.OK;
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

        if (LOG.isDebugEnabled()) {
            LOG.debug("run backup job: {}", this);
        }

        if (state == BackupJobState.PENDING) {
            String pausedLabel = DebugPointUtil.getDebugParamOrDefault("FE.PAUSE_PENDING_BACKUP_JOB", "");
            if (!pausedLabel.isEmpty() && label.startsWith(pausedLabel)) {
                LOG.info("pause pending backup job by debug point: {}", this);
                return;
            }
        }

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
        AgentBatchTask batchTask = new AgentBatchTask(Config.backup_restore_batch_task_num_per_rpc);
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
                    case MATERIALIZED_VIEW:
                        break;
                    case OLAP:
                        OlapTable olapTable = (OlapTable) tbl;
                        if (!checkOlapTable(olapTable, tableRef).ok()) {
                            return;
                        }
                        if (getContent() == BackupContent.ALL) {
                            if (!prepareSnapshotTaskForOlapTableWithoutLock(
                                                    db, (OlapTable) tbl, tableRef, batchTask).ok()) {
                                return;
                            }
                        }
                        if (!prepareBackupMetaForOlapTableWithoutLock(tableRef, olapTable, copiedTables).ok()) {
                            return;
                        }
                        break;
                    case VIEW:
                        if (!prepareBackupMetaForViewWithoutLock((View) tbl, copiedTables).ok()) {
                            return;
                        }
                        break;
                    case ODBC:
                        if (!prepareBackupMetaForOdbcTableWithoutLock(
                                                    (OdbcTable) tbl, copiedTables, copiedResources).ok()) {
                            return;
                        }
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

        // Limit the max num of tablets involved in a backup job, to avoid OOM.
        if (unfinishedTaskIds.size() > Config.max_backup_tablets_per_job) {
            String msg = String.format("the num involved tablets %d exceeds the limit %d, "
                    + "which might cause the FE OOM, change config `max_backup_tablets_per_job` "
                    + "to change this limitation",
                    unfinishedTaskIds.size(), Config.max_backup_tablets_per_job);
            LOG.warn(msg);
            status = new Status(ErrCode.COMMON_ERROR, msg);
            return;
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

    private Status checkOlapTable(OlapTable olapTable, TableRef backupTableRef) {
        olapTable.readLock();
        try {
            // check backup table again
            if (backupTableRef.getPartitionNames() != null) {
                for (String partName : backupTableRef.getPartitionNames().getPartitionNames()) {
                    Partition partition = olapTable.getPartition(partName);
                    if (partition == null) {
                        status = new Status(ErrCode.NOT_FOUND, "partition " + partName
                                + " does not exist  in table" + backupTableRef.getName().getTbl());
                        return status;
                    }
                }
            }
        }  finally {
            olapTable.readUnlock();
        }
        return Status.OK;
    }

    private Status prepareSnapshotTaskForOlapTableWithoutLock(Database db, OlapTable olapTable,
            TableRef backupTableRef, AgentBatchTask batchTask) {
        // Add barrier editlog for barrier commit seq
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
                Partition partition = olapTable.getPartition(partName, false); // exclude tmp partitions
                if (partition == null) {
                    if (olapTable.getPartition(partName, true) != null) {
                        return new Status(ErrCode.NOT_FOUND, "backup tmp partition " + partName
                                + " in table " + backupTableRef.getName().getTbl() + " is not supported");
                    } else {
                        return new Status(ErrCode.NOT_FOUND, "partition " + partName
                                + " does not exist in table " + backupTableRef.getName().getTbl());
                    }
                }
            }
        }

        // create snapshot tasks
        List<Partition> partitions = Lists.newArrayList();
        if (backupTableRef.getPartitionNames() == null) {
            partitions.addAll(olapTable.getPartitions()); // no temp partitions in OlapTable.getPartitions()
        } else {
            for (String partName : backupTableRef.getPartitionNames().getPartitionNames()) {
                Partition partition = olapTable.getPartition(partName, false);  // exclude tmp partitions
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
                        return status;
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

            LOG.info("snapshot for partition {}, version: {}, job: {}",
                    partition.getId(), visibleVersion, label);
        }
        return Status.OK;
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

    private Status prepareBackupMetaForOlapTableWithoutLock(TableRef tableRef, OlapTable olapTable,
                                                          List<Table> copiedTables) {
        // only copy visible indexes
        List<String> reservedPartitions = tableRef.getPartitionNames() == null ? null
                : tableRef.getPartitionNames().getPartitionNames();
        OlapTable copiedTbl = olapTable.selectiveCopy(reservedPartitions, IndexExtState.VISIBLE, true);
        if (copiedTbl == null) {
            status = new Status(ErrCode.COMMON_ERROR, "failed to copy table: " + olapTable.getName());
            return status;
        }

        removeUnsupportProperties(copiedTbl);
        copiedTables.add(copiedTbl);
        return Status.OK;
    }

    private Status prepareBackupMetaForViewWithoutLock(View view, List<Table> copiedTables) {
        View copiedView = view.clone();
        if (copiedView == null) {
            status = new Status(ErrCode.COMMON_ERROR, "failed to copy view: " + view.getName());
            return status;
        }
        copiedTables.add(copiedView);
        return Status.OK;
    }

    private Status prepareBackupMetaForOdbcTableWithoutLock(OdbcTable odbcTable, List<Table> copiedTables,
            List<Resource> copiedResources) {
        OdbcTable copiedOdbcTable = odbcTable.clone();
        if (copiedOdbcTable == null) {
            status = new Status(ErrCode.COMMON_ERROR, "failed to copy odbc table: " + odbcTable.getName());
            return status;
        }
        copiedTables.add(copiedOdbcTable);
        if (copiedOdbcTable.getOdbcCatalogResourceName() != null) {
            Resource resource = Env.getCurrentEnv().getResourceMgr()
                    .getResource(copiedOdbcTable.getOdbcCatalogResourceName());
            Resource copiedResource = resource.clone();
            if (copiedResource == null) {
                status = new Status(ErrCode.COMMON_ERROR, "failed to copy odbc resource: "
                        + resource.getName());
                return status;
            }
            copiedResources.add(copiedResource);
        }
        return Status.OK;
    }

    private void removeUnsupportProperties(OlapTable tbl) {
        // We cannot support the colocate attribute because the colocate information is not backed up
        // synchronously when backing up.
        tbl.setColocateGroup(null);
    }

    private void waitingAllSnapshotsFinished() {
        if (unfinishedTaskIds.isEmpty()) {
            if (env.getEditLog().exceedMaxJournalSize(this)) {
                status = new Status(ErrCode.COMMON_ERROR, "backupJob is too large ");
                return;
            }

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

        AgentBatchTask batchTask = new AgentBatchTask(Config.backup_restore_batch_task_num_per_rpc);
        for (Long beId : beToSnapshots.keySet()) {
            List<SnapshotInfo> infos = beToSnapshots.get(beId);
            int totalNum = infos.size();
            // each task contains several upload sub tasks
            int taskNumPerBatch = Config.backup_upload_snapshot_batch_size;
            LOG.info("backend {} has total {} snapshots, per task batch size {}, {}",
                    beId, totalNum, taskNumPerBatch, this);

            List<FsBroker> brokers = Lists.newArrayList();
            Status st = repo.getBrokerAddress(beId, env, brokers);
            if (!st.ok()) {
                status = st;
                return;
            }
            Preconditions.checkState(brokers.size() == 1);

            // allot tasks
            for (int index = 0; index < totalNum; index += taskNumPerBatch) {
                Map<String, String> srcToDest = Maps.newHashMap();
                for (int j = 0; j < taskNumPerBatch && index + j < totalNum; j++) {
                    SnapshotInfo info = infos.get(index + j);
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

        if (LOG.isDebugEnabled()) {
            LOG.debug("waiting {} tablets to upload snapshot. {}", unfinishedTaskIds.size(), this);
        }
    }

    private void saveMetaInfo() {
        String createTimeStr = TimeUtils.longToTimeString(createTime,
                TimeUtils.getDatetimeFormatWithHyphenWithTimeZone());
        // local job dir: backup/repo__repo_id/label__createtime/
        // Add repo_id to isolate jobs from different repos.
        localJobDirPath = Paths.get(BackupHandler.BACKUP_ROOT_DIR.toString(),
                                    "repo__" + repoId, label + "__" + createTimeStr).normalize();

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
            if (LOG.isDebugEnabled()) {
                LOG.debug("job info: {}. {}", jobInfo, this);
            }
            File jobInfoFile = new File(jobDir, Repository.PREFIX_JOB_INFO + createTimeStr);
            if (!jobInfoFile.createNewFile()) {
                status = new Status(ErrCode.COMMON_ERROR, "Failed to create job info file: " + jobInfoFile.toString());
                return;
            }
            jobInfo.writeToFile(jobInfoFile);
            localJobInfoFilePath = jobInfoFile.getAbsolutePath();
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
        AgentBatchTask batchTask = new AgentBatchTask(Config.backup_restore_batch_task_num_per_rpc);
        for (SnapshotInfo info : snapshotInfos.values()) {
            ReleaseSnapshotTask releaseTask = new ReleaseSnapshotTask(null, info.getBeId(), info.getDbId(),
                    info.getTabletId(), info.getPath());
            batchTask.addTask(releaseTask);
        }
        AgentTaskExecutor.submit(batchTask);
        LOG.info("send {} release snapshot tasks, job: {}", snapshotInfos.size(), this);
    }

    private void uploadMetaAndJobInfoFile() {
        if (repoId != Repository.KEEP_ON_LOCAL_REPO_ID) {
            String remoteMetaInfoFile = repo.assembleMetaInfoFilePath(label);
            if (!uploadFile(localMetaInfoFilePath, remoteMetaInfoFile)) {
                return;
            }

            String remoteJobInfoFile = repo.assembleJobInfoFilePath(label, createTime);
            if (!uploadFile(localJobInfoFilePath, remoteJobInfoFile)) {
                return;
            }
        }

        finishedTime = System.currentTimeMillis();
        state = BackupJobState.FINISHED;

        // log
        env.getEditLog().logBackupJob(this);
        LOG.info("job is finished. {}", this);

        if (repoId == Repository.KEEP_ON_LOCAL_REPO_ID) {
            env.getBackupHandler().addSnapshot(label, this);
            return;
        }
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

        // meta info and job info not need save in log when cancel, we need to clean them here
        backupMeta = null;
        jobInfo = null;
        releaseSnapshots();
        snapshotInfos.clear();

        BackupJobState curState = state;
        finishedTime = System.currentTimeMillis();
        state = BackupJobState.CANCELLED;

        // log
        env.getEditLog().logBackupJob(this);
        LOG.info("finished to cancel backup job. current state: {}. {}", curState.name(), this);
    }

    public boolean isLocalSnapshot() {
        return repoId == Repository.KEEP_ON_LOCAL_REPO_ID;
    }

    public long getCommitSeq() {
        return commitSeq;
    }

    // read meta and job info bytes from disk, and return the snapshot
    public synchronized Snapshot getSnapshot() {
        if (state != BackupJobState.FINISHED || repoId != Repository.KEEP_ON_LOCAL_REPO_ID) {
            return null;
        }

        // Avoid loading expired meta.
        long expiredAt = createTime + timeoutMs;
        if (System.currentTimeMillis() >= expiredAt) {
            return new Snapshot(label, new byte[0], new byte[0], expiredAt, commitSeq);
        }

        try {
            File metaInfoFile = new File(localMetaInfoFilePath);
            File jobInfoFile = new File(localJobInfoFilePath);
            byte[] metaInfoBytes = Files.readAllBytes(metaInfoFile.toPath());
            byte[] jobInfoBytes = Files.readAllBytes(jobInfoFile.toPath());
            return new Snapshot(label, metaInfoBytes, jobInfoBytes, expiredAt, commitSeq);
        } catch (IOException e) {
            LOG.warn("failed to load meta info and job info file, meta info file {}, job info file {}: ",
                    localMetaInfoFilePath, localJobInfoFilePath, e);
            return null;
        }
    }

    public synchronized List<String> getInfo() {
        String unfinishedTaskIdsStr = unfinishedTaskIds.entrySet().stream()
                .map(e -> "[" + e.getKey() + "=" + e.getValue() + "]")
                .limit(100)
                .collect(Collectors.joining(", "));
        String taskProgressStr = taskProgress.entrySet().stream()
                .map(e -> "[" + e.getKey() + ": " + e.getValue().first + "/" + e.getValue().second + "]")
                .limit(100)
                .collect(Collectors.joining(", "));
        String taskErrMsgStr = taskErrMsg.entrySet().stream()
                .map(e -> "[" + e.getKey() + ": " + e.getValue() + "]")
                .limit(100)
                .collect(Collectors.joining(", "));

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
        info.add(unfinishedTaskIdsStr);
        info.add(taskProgressStr);
        info.add(taskErrMsgStr);
        info.add(status.toString());
        info.add(String.valueOf(timeoutMs / 1000));
        return info;
    }

    private String getBackupObjs() {
        List<String> list = tableRefs.stream().map(n -> "[" + n.toString() + "]").collect(Collectors.toList());
        return Joiner.on(", ").join(list);
    }

    public static BackupJob read(DataInput in) throws IOException {
        BackupJob job = new BackupJob();
        job.readFields(in);
        return job;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // For a completed job, there's no need to save it with compressed serialization as it has
        // no snapshot or backup meta info, making it small in size. This helps maintain compatibility
        // more easily.
        boolean shouldCompress = !isDone() && Config.backup_job_compressed_serialization;
        if (shouldCompress) {
            type = JobType.BACKUP_COMPRESSED;
        }
        super.write(out);
        if (shouldCompress) {
            type = JobType.BACKUP;

            int written = 0;
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            try (GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream)) {
                try (DataOutputStream stream = new DataOutputStream(gzipStream)) {
                    writeOthers(stream);
                    written = stream.size();
                }
            }
            Text text = new Text(byteStream.toByteArray());
            if (LOG.isDebugEnabled() || text.getLength() > (50 << 20)) {
                LOG.info("backup job written size {}, compressed size {}", written, text.getLength());
            }
            text.write(out);
        } else {
            writeOthers(out);
        }
    }

    public void writeOthers(DataOutput out) throws IOException {
        // table refs
        out.writeInt(tableRefs.size());
        for (TableRef tblRef : tableRefs) {
            tblRef.write(out);
        }

        // state
        Text.writeString(out, state.name());

        // times
        out.writeLong(snapshotFinishedTime);
        out.writeLong(snapshotUploadFinishedTime);

        // snapshot info
        out.writeInt(snapshotInfos.size());
        for (SnapshotInfo info : snapshotInfos.values()) {
            info.write(out);
        }

        // backup meta
        if (backupMeta == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            backupMeta.write(out);
        }

        // No need to persist job info. It is generated then write to file

        // metaInfoFilePath and jobInfoFilePath
        if (Strings.isNullOrEmpty(localMetaInfoFilePath)) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Text.writeString(out, localMetaInfoFilePath);
        }

        if (Strings.isNullOrEmpty(localJobInfoFilePath)) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Text.writeString(out, localJobInfoFilePath);
        }

        // write properties
        out.writeInt(properties.size());
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        if (type == JobType.BACKUP_COMPRESSED) {
            type = JobType.BACKUP;

            Text text = new Text();
            text.readFields(in);
            if (LOG.isDebugEnabled() || text.getLength() > (50 << 20)) {
                LOG.info("read backup job, compressed size {}", text.getLength());
            }

            ByteArrayInputStream byteStream = new ByteArrayInputStream(text.getBytes());
            try (GZIPInputStream gzipStream = new GZIPInputStream(byteStream)) {
                try (DataInputStream stream = new DataInputStream(gzipStream)) {
                    readOthers(stream);
                }
            }
        } else {
            readOthers(in);
        }
    }

    public void readOthers(DataInput in) throws IOException {
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

        if (properties.containsKey(SNAPSHOT_COMMIT_SEQ)) {
            commitSeq = Long.parseLong(properties.get(SNAPSHOT_COMMIT_SEQ));
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.append(", state: ").append(state.name());
        return sb.toString();
    }
}
