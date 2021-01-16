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

import org.apache.doris.analysis.TableRef;
import org.apache.doris.backup.Status.ErrCode;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.TimeUtils;
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class BackupJob extends AbstractJob {
    private static final Logger LOG = LogManager.getLogger(BackupJob.class);

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

    private BackupJobState state;

    private long snapshotFinishedTime = -1;
    private long snapshopUploadFinishedTime = -1;

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

    public BackupJob() {
        super(JobType.BACKUP);
    }

    public BackupJob(String label, long dbId, String dbName, List<TableRef> tableRefs, long timeoutMs,
            Catalog catalog, long repoId) {
        super(JobType.BACKUP, label, dbId, dbName, timeoutMs, catalog, repoId);
        this.tableRefs = tableRefs;
        this.state = BackupJobState.PENDING;
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

    public synchronized boolean finishTabletSnapshotTask(SnapshotTask task, TFinishTaskRequest request) {
        Preconditions.checkState(task.getJobId() == jobId);
        
        if (request.getTaskStatus().getStatusCode() != TStatusCode.OK) {
            taskErrMsg.put(task.getSignature(), Joiner.on(",").join(request.getTaskStatus().getErrorMsgs()));
            // snapshot task could not finish if status_code is OLAP_ERR_VERSION_ALREADY_MERGED,
            // so cancel this job
            if (request.getTaskStatus().getStatusCode() == TStatusCode.OLAP_ERR_VERSION_ALREADY_MERGED) {
                status = new Status(ErrCode.OLAP_VERSION_ALREADY_MERGED, "make snapshot failed, version already merged");
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
        taskErrMsg.remove(task.getTabletId());
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
        if (repo == null) {
            repo = catalog.getBackupHandler().getRepoMgr().getRepo(repoId);
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
        if (state == BackupJobState.FINISHED || state == BackupJobState.CANCELLED) {
            return true;
        }
        return false;
    }

    private void prepareAndSendSnapshotTask() {
        Database db = catalog.getDb(dbId);
        if (db == null) {
            status = new Status(ErrCode.NOT_FOUND, "database " + dbId + " does not exist");
            return;
        }

        // generate job id
        jobId = catalog.getNextId();
        AgentBatchTask batchTask = new AgentBatchTask();

        unfinishedTaskIds.clear();
        taskProgress.clear();
        taskErrMsg.clear();

        List<Table> copiedTables = Lists.newArrayList();

        for (TableRef tableRef : tableRefs) {
            String tblName = tableRef.getName().getTbl();
            Table tbl = db.getTable(tblName);
            if (tbl == null) {
                status = new Status(ErrCode.NOT_FOUND, "table " + tblName + " does not exist");
                return;
            }
            if (tbl.getType() != TableType.OLAP) {
                status = new Status(ErrCode.COMMON_ERROR, "table " + tblName + " is not OLAP table");
                return;
            }

            OlapTable olapTbl = (OlapTable) tbl;
            olapTbl.readLock();
            try {
                // check backup table again
                if (tableRef.getPartitionNames() != null) {
                    for (String partName : tableRef.getPartitionNames().getPartitionNames()) {
                        Partition partition = olapTbl.getPartition(partName);
                        if (partition == null) {
                            status = new Status(ErrCode.NOT_FOUND, "partition " + partName
                                    + " does not exist  in table" + tblName);
                            return;
                        }
                    }
                }

                // create snapshot tasks
                List<Partition> partitions = Lists.newArrayList();
                if (tableRef.getPartitionNames() == null) {
                    partitions.addAll(olapTbl.getPartitions());
                } else {
                    for (String partName : tableRef.getPartitionNames().getPartitionNames()) {
                        Partition partition = tbl.getPartition(partName);
                        partitions.add(partition);
                    }
                }

                // snapshot partitions
                for (Partition partition : partitions) {
                    long visibleVersion = partition.getVisibleVersion();
                    long visibleVersionHash = partition.getVisibleVersionHash();
                    List<MaterializedIndex> indexes = partition.getMaterializedIndices(IndexExtState.VISIBLE);
                    for (MaterializedIndex index : indexes) {
                        int schemaHash = olapTbl.getSchemaHashByIndexId(index.getId());
                        List<Tablet> tablets = index.getTablets();
                        for (Tablet tablet : tablets) {
                            Replica replica = chooseReplica(tablet, visibleVersion, visibleVersionHash);
                            if (replica == null) {
                                status = new Status(ErrCode.COMMON_ERROR,
                                        "failed to choose replica to make snapshot for tablet " + tablet.getId()
                                                + ". visible version: " + visibleVersion
                                                + ", visible version hash: " + visibleVersionHash);
                                return;
                            }
                            SnapshotTask task = new SnapshotTask(null, replica.getBackendId(), tablet.getId(),
                                    jobId, dbId, tbl.getId(), partition.getId(),
                                    index.getId(), tablet.getId(),
                                    visibleVersion, visibleVersionHash,
                                    schemaHash, timeoutMs, false /* not restore task */);
                            batchTask.addTask(task);
                            unfinishedTaskIds.put(tablet.getId(), replica.getBackendId());
                        }
                    }

                    LOG.info("snapshot for partition {}, version: {}, version hash: {}",
                            partition.getId(), visibleVersion, visibleVersionHash);
                }

                // only copy visible indexes
                List<String> reservedPartitions = tableRef.getPartitionNames() == null ? null
                        : tableRef.getPartitionNames().getPartitionNames();
                OlapTable copiedTbl = olapTbl.selectiveCopy(reservedPartitions, true, IndexExtState.VISIBLE);
                if (copiedTbl == null) {
                    status = new Status(ErrCode.COMMON_ERROR, "failed to copy table: " + tblName);
                    return;
                }
                copiedTables.add(copiedTbl);
            } finally {
                olapTbl.readUnlock();
            }

        }

        backupMeta = new BackupMeta(copiedTables);

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

    private void waitingAllSnapshotsFinished() {
        if (unfinishedTaskIds.isEmpty()) {
            snapshotFinishedTime = System.currentTimeMillis();
            state = BackupJobState.UPLOAD_SNAPSHOT;

            // log
            catalog.getEditLog().logBackupJob(this);
            LOG.info("finished to make snapshots. {}", this);
            return;
        }

        LOG.info("waiting {} tablets to make snapshot. {}", unfinishedTaskIds.size(), this);
    }

    private void uploadSnapshot() {
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
            Status st = repo.getBrokerAddress(beId, catalog, brokers);
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
                    srcToDest.put(src, dest);
                }
                long signature = catalog.getNextId();
                UploadTask task = new UploadTask(null, beId, signature, jobId, dbId, srcToDest,
                        brokers.get(0), repo.getStorage().getProperties());
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
            snapshopUploadFinishedTime = System.currentTimeMillis();
            state = BackupJobState.SAVE_META;

            // log
            catalog.getEditLog().logBackupJob(this);
            LOG.info("finished uploading snapshots. {}", this);
            return;
        }

        LOG.debug("waiting {} tablets to upload snapshot. {}", unfinishedTaskIds.size(), this);
    }

    private void saveMetaInfo() {
        String createTimeStr = TimeUtils.longToTimeString(createTime, new SimpleDateFormat(
                "yyyy-MM-dd-HH-mm-ss"));
        // local job dir: backup/label__createtime/
        localJobDirPath = Paths.get(BackupHandler.BACKUP_ROOT_DIR.toString(),
                                    label + "__" + createTimeStr).normalize();

        try {
            // 1. create local job dir of this backup job
            File jobDir = new File(localJobDirPath.toString());
            if (jobDir.exists()) {
                // if dir exists, delete it first
                Files.walk(localJobDirPath,
                           FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            }
            if (!jobDir.mkdir()) {
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
            jobInfo = BackupJobInfo.fromCatalog(createTime, label, dbName, dbId, backupMeta.getTables().values(),
                                                snapshotInfos);
            LOG.debug("job info: {}. {}", jobInfo, this);
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
        releaseSnapshots();

        snapshotInfos.clear();

        // log
        catalog.getEditLog().logBackupJob(this);
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
        catalog.getEditLog().logBackupJob(this);
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
    private Replica chooseReplica(Tablet tablet, long visibleVersion, long visibleVersionHash) {
        List<Long> replicaIds = Lists.newArrayList();
        for (Replica replica : tablet.getReplicas()) {
            replicaIds.add(replica.getId());
        }
        
        Collections.sort(replicaIds);
        for (Long replicaId : replicaIds) {
            Replica replica = tablet.getReplicaById(replicaId);
            if (replica.getLastFailedVersion() < 0 && (replica.getVersion() > visibleVersion
                    || (replica.getVersion() == visibleVersion && replica.getVersionHash() == visibleVersionHash))) {
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
                    Files.walk(localJobDirPath,
                               FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
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
        catalog.getEditLog().logBackupJob(this);
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
        info.add(TimeUtils.longToTimeString(snapshopUploadFinishedTime));
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
        BackupJob job = new BackupJob();
        job.readFields(in);
        return job;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        // table refs
        out.writeInt(tableRefs.size());
        for (TableRef tblRef : tableRefs) {
            tblRef.write(out);
        }

        // state
        Text.writeString(out, state.name());

        // times
        out.writeLong(snapshotFinishedTime);
        out.writeLong(snapshopUploadFinishedTime);

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
    }

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
        snapshopUploadFinishedTime = in.readLong();

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
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.append(", state: ").append(state.name());
        return sb.toString();
    }
}

