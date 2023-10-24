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

package org.apache.doris.master;

import org.apache.doris.alter.AlterJobV2.JobType;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Partition.PartitionState;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.load.DeleteJob;
import org.apache.doris.load.loadv2.SparkLoadJob;
import org.apache.doris.system.Backend;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.AlterInvertedIndexTask;
import org.apache.doris.task.AlterReplicaTask;
import org.apache.doris.task.CheckConsistencyTask;
import org.apache.doris.task.ClearAlterTask;
import org.apache.doris.task.CloneTask;
import org.apache.doris.task.CreateReplicaTask;
import org.apache.doris.task.DirMoveTask;
import org.apache.doris.task.DownloadTask;
import org.apache.doris.task.PublishVersionTask;
import org.apache.doris.task.PushCooldownConfTask;
import org.apache.doris.task.PushTask;
import org.apache.doris.task.SnapshotTask;
import org.apache.doris.task.StorageMediaMigrationTask;
import org.apache.doris.task.UpdateTabletMetaInfoTask;
import org.apache.doris.task.UploadTask;
import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TMasterResult;
import org.apache.doris.thrift.TPushType;
import org.apache.doris.thrift.TReportRequest;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTabletInfo;
import org.apache.doris.thrift.TTaskType;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MasterImpl {
    private static final Logger LOG = LogManager.getLogger(MasterImpl.class);

    private ReportHandler reportHandler = new ReportHandler();

    public MasterImpl() {
        reportHandler.start();
    }

    public TMasterResult finishTask(TFinishTaskRequest request) {
        TMasterResult result = new TMasterResult();
        TStatus tStatus = new TStatus(TStatusCode.OK);
        result.setStatus(tStatus);
        // check task status
        // retry task by report process
        TStatus taskStatus = request.getTaskStatus();
        if (LOG.isDebugEnabled()) {
            LOG.debug("get task report: {}", request);
        }

        if (taskStatus.getStatusCode() != TStatusCode.OK) {
            LOG.warn("finish task reports bad. request: {}", request);
        }

        // get backend
        TBackend tBackend = request.getBackend();
        String host = tBackend.getHost();
        int bePort = tBackend.getBePort();
        Backend backend = Env.getCurrentSystemInfo().getBackendWithBePort(host, bePort);
        if (backend == null) {
            tStatus.setStatusCode(TStatusCode.CANCELLED);
            List<String> errorMsgs = new ArrayList<>();
            errorMsgs.add("backend not exist.");
            tStatus.setErrorMsgs(errorMsgs);
            LOG.warn("backend does not found. host: {}, be port: {}. task: {}", host, bePort, request);
            return result;
        }

        long backendId = backend.getId();
        TTaskType taskType = request.getTaskType();
        long signature = request.getSignature();

        AgentTask task = AgentTaskQueue.getTask(backendId, taskType, signature);
        if (task == null) {
            if (taskType != TTaskType.DROP && taskType != TTaskType.RELEASE_SNAPSHOT
                    && taskType != TTaskType.CLEAR_TRANSACTION_TASK) {
                String errMsg = "cannot find task. type: " + taskType + ", backendId: " + backendId
                        + ", signature: " + signature;
                LOG.warn(errMsg);
                tStatus.setStatusCode(TStatusCode.CANCELLED);
                List<String> errorMsgs = new ArrayList<String>();
                errorMsgs.add(errMsg);
                tStatus.setErrorMsgs(errorMsgs);
            }
            return result;
        } else {
            if (taskStatus.getStatusCode() != TStatusCode.OK) {
                task.failed();
                String errMsg = "task type: " + taskType + ", status_code: " + taskStatus.getStatusCode().toString()
                        + (taskStatus.isSetErrorMsgs() ? (", status_message: " + taskStatus.getErrorMsgs()) : "")
                        + ", backendId: " + backend + ", signature: " + signature;
                task.setErrorMsg(errMsg);
                // We start to let FE perceive the task's error msg
                if (taskType != TTaskType.MAKE_SNAPSHOT && taskType != TTaskType.UPLOAD
                        && taskType != TTaskType.DOWNLOAD && taskType != TTaskType.MOVE
                        && taskType != TTaskType.CLONE && taskType != TTaskType.PUBLISH_VERSION
                        && taskType != TTaskType.CREATE && taskType != TTaskType.UPDATE_TABLET_META_INFO
                        && taskType != TTaskType.STORAGE_MEDIUM_MIGRATE) {
                    return result;
                }
            }
        }

        try {
            switch (taskType) {
                case CREATE:
                    Preconditions.checkState(request.isSetReportVersion());
                    finishCreateReplica(task, request);
                    break;
                case REALTIME_PUSH:
                    checkHasTabletInfo(request);
                    Preconditions.checkState(request.isSetReportVersion());
                    finishRealtimePush(task, request);
                    break;
                case PUBLISH_VERSION:
                    finishPublishVersion(task, request);
                    break;
                case CLEAR_ALTER_TASK:
                    finishClearAlterTask(task, request);
                    break;
                case DROP:
                    finishDropReplica(task);
                    break;
                case SCHEMA_CHANGE:
                case ROLLUP:
                    throw new RuntimeException("Schema change and rollup job is not used any more,"
                            + " use alter task instead");
                case CLONE:
                    finishClone(task, request);
                    break;
                case STORAGE_MEDIUM_MIGRATE:
                    finishStorageMediumMigrate(task, request);
                    break;
                case CHECK_CONSISTENCY:
                    finishConsistencyCheck(task, request);
                    break;
                case MAKE_SNAPSHOT:
                    finishMakeSnapshot(task, request);
                    break;
                case UPLOAD:
                    finishUpload(task, request);
                    break;
                case DOWNLOAD:
                    finishDownloadTask(task, request);
                    break;
                case MOVE:
                    finishMoveDirTask(task, request);
                    break;
                case RECOVER_TABLET:
                    finishRecoverTablet(task);
                    break;
                case ALTER:
                    finishAlterTask(task, request);
                    break;
                case ALTER_INVERTED_INDEX:
                    finishAlterInvertedIndexTask(task, request);
                    break;
                case UPDATE_TABLET_META_INFO:
                    finishUpdateTabletMeta(task, request);
                    break;
                case PUSH_COOLDOWN_CONF:
                    finishPushCooldownConfTask(task);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            tStatus.setStatusCode(TStatusCode.CANCELLED);
            String errMsg = "finish agent task error.";
            LOG.warn(errMsg, e);
            List<String> errorMsgs = new ArrayList<String>();
            errorMsgs.add(errMsg);
            tStatus.setErrorMsgs(errorMsgs);
        }

        if (tStatus.getStatusCode() == TStatusCode.OK) {
            LOG.debug("report task success. {}", request.toString());
        }

        return result;
    }

    private void checkHasTabletInfo(TFinishTaskRequest request) throws Exception {
        if (!request.isSetFinishTabletInfos() || request.getFinishTabletInfos().isEmpty()) {
            throw new Exception("tablet info is not set");
        }
    }

    private void finishCreateReplica(AgentTask task, TFinishTaskRequest request) {
        // if we get here, this task will be removed from AgentTaskQueue for certain.
        // because in this function, the only problem that cause failure is meta missing.
        // and if meta is missing, we no longer need to resend this task
        try {
            CreateReplicaTask createReplicaTask = (CreateReplicaTask) task;
            if (request.getTaskStatus().getStatusCode() != TStatusCode.OK) {
                createReplicaTask.countDownToZero(task.getBackendId() + ": "
                        + request.getTaskStatus().getErrorMsgs().toString());
            } else {
                long tabletId = createReplicaTask.getTabletId();

                if (request.isSetFinishTabletInfos()) {
                    Replica replica = Env.getCurrentInvertedIndex().getReplica(createReplicaTask.getTabletId(),
                            createReplicaTask.getBackendId());
                    replica.setPathHash(request.getFinishTabletInfos().get(0).getPathHash());

                    if (createReplicaTask.isRecoverTask()) {
                        /**
                         * This create replica task may be generated by recovery
                         * (See comment of Config.recover_with_empty_tablet)
                         * So we set replica back to good.
                         */
                        replica.setBad(false);
                        LOG.info("finish recover create replica task. set replica to good. tablet {},"
                                        + " replica {}, backend {}", tabletId, task.getBackendId(), replica.getId());
                    }
                }

                // this should be called before 'countDownLatch()'
                Env.getCurrentSystemInfo().updateBackendReportVersion(task.getBackendId(),
                        request.getReportVersion(), task.getDbId(), task.getTableId());

                createReplicaTask.countDownLatch(task.getBackendId(), task.getSignature());
                LOG.debug("finish create replica. tablet id: {}, be: {}, report version: {}",
                        tabletId, task.getBackendId(), request.getReportVersion());
            }
        } finally {
            AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.CREATE, task.getSignature());
        }
    }

    private void finishUpdateTabletMeta(AgentTask task, TFinishTaskRequest request) {
        // if we get here, this task will be removed from AgentTaskQueue for certain.
        // because in this function, the only problem that cause failure is meta missing.
        // and if meta is missing, we no longer need to resend this task
        try {
            UpdateTabletMetaInfoTask tabletTask = (UpdateTabletMetaInfoTask) task;
            if (request.getTaskStatus().getStatusCode() != TStatusCode.OK) {
                tabletTask.countDownToZero(task.getBackendId() + ": "
                        + request.getTaskStatus().getErrorMsgs().toString());
            } else {
                tabletTask.countDownLatch(task.getBackendId(), tabletTask.getTablets());
                LOG.debug("finish update tablet meta. tablet id: {}, be: {}",
                        tabletTask.getTablets(), task.getBackendId());
            }
        } finally {
            AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.UPDATE_TABLET_META_INFO, task.getSignature());
        }
    }

    private void finishRealtimePush(AgentTask task, TFinishTaskRequest request) {
        List<TTabletInfo> finishTabletInfos = request.getFinishTabletInfos();
        Preconditions.checkState(finishTabletInfos != null && !finishTabletInfos.isEmpty());

        PushTask pushTask = (PushTask) task;

        long dbId = pushTask.getDbId();
        long backendId = pushTask.getBackendId();
        long signature = task.getSignature();
        long transactionId = ((PushTask) task).getTransactionId();
        Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
        if (db == null) {
            AgentTaskQueue.removeTask(backendId, TTaskType.REALTIME_PUSH, signature);
            return;
        }

        long tableId = pushTask.getTableId();
        long partitionId = pushTask.getPartitionId();
        long pushIndexId = pushTask.getIndexId();
        long pushTabletId = pushTask.getTabletId();
        // push finish type:
        //                  numOfFinishTabletInfos  tabletId schemaHash
        // Normal:                     1                   /          /
        // SchemaChangeHandler         2                 same      diff
        // RollupHandler               2                 diff      diff
        //
        // reuse enum 'PartitionState' here as 'push finish type'
        PartitionState pushState = null;
        if (finishTabletInfos.size() == 1) {
            pushState = PartitionState.NORMAL;
        } else if (finishTabletInfos.size() == 2) {
            if (finishTabletInfos.get(0).getTabletId() == finishTabletInfos.get(1).getTabletId()) {
                pushState = PartitionState.SCHEMA_CHANGE;
            } else {
                pushState = PartitionState.ROLLUP;
            }
        } else {
            LOG.warn("invalid push report infos. finishTabletInfos' size: " + finishTabletInfos.size());
            return;
        }
        LOG.debug("push report state: {}", pushState.name());

        OlapTable olapTable = (OlapTable) db.getTableNullable(tableId);
        if (olapTable == null || !olapTable.writeLockIfExist()) {
            AgentTaskQueue.removeTask(backendId, TTaskType.REALTIME_PUSH, signature);
            LOG.warn("finish push replica error, cannot find table[" + tableId + "] when push finished");
            return;
        }
        try {
            Partition partition = olapTable.getPartition(partitionId);
            if (partition == null) {
                throw new MetaNotFoundException("cannot find partition[" + partitionId + "] when push finished");
            }

            MaterializedIndex pushIndex = partition.getIndex(pushIndexId);
            if (pushIndex == null) {
                // yiguolei: if index is dropped during load, it is not a failure.
                // throw exception here and cause the job to cancel the task
                throw new MetaNotFoundException("cannot find index[" + pushIndex + "] when push finished");
            }

            // should be done before addReplicaPersistInfos and countDownLatch
            long reportVersion = request.getReportVersion();
            Env.getCurrentSystemInfo().updateBackendReportVersion(task.getBackendId(), reportVersion,
                                                                       task.getDbId(), task.getTableId());

            List<Long> tabletIds = finishTabletInfos.stream().map(
                    tTabletInfo -> tTabletInfo.getTabletId()).collect(Collectors.toList());
            List<TabletMeta> tabletMetaList = Env.getCurrentInvertedIndex().getTabletMetaList(tabletIds);

            if (pushTask.getPushType() == TPushType.DELETE) {
                DeleteJob deleteJob = Env.getCurrentEnv().getDeleteHandler().getDeleteJob(transactionId);
                if (deleteJob == null) {
                    throw new MetaNotFoundException("cannot find delete job, job[" + transactionId + "]");
                }
                for (int i = 0; i < tabletMetaList.size(); i++) {
                    TabletMeta tabletMeta = tabletMetaList.get(i);
                    long tabletId = tabletIds.get(i);
                    Replica replica = findRelatedReplica(olapTable, partition,
                            backendId, tabletId, tabletMeta.getIndexId());
                    if (replica != null) {
                        deleteJob.addFinishedReplica(partitionId, pushTabletId, replica);
                        pushTask.countDownLatch(backendId, pushTabletId);
                    }
                }
            } else if (pushTask.getPushType() == TPushType.LOAD_V2) {
                long loadJobId = pushTask.getLoadJobId();
                org.apache.doris.load.loadv2.LoadJob job
                        = Env.getCurrentEnv().getLoadManager().getLoadJob(loadJobId);
                if (job == null) {
                    throw new MetaNotFoundException("cannot find load job, job[" + loadJobId + "]");
                }
                for (int i = 0; i < tabletMetaList.size(); i++) {
                    TabletMeta tabletMeta = tabletMetaList.get(i);
                    checkReplica(finishTabletInfos.get(i), tabletMeta);
                    long tabletId = tabletIds.get(i);
                    Replica replica = findRelatedReplica(
                            olapTable, partition, backendId, tabletId, tabletMeta.getIndexId());
                    // if the replica is under schema change, could not find the replica with aim schema hash
                    if (replica != null) {
                        ((SparkLoadJob) job).addFinishedReplica(replica.getId(), pushTabletId, backendId);
                    }
                }
            }

            AgentTaskQueue.removeTask(backendId, TTaskType.REALTIME_PUSH, signature);
            LOG.debug("finish push replica. tabletId: {}, backendId: {}", pushTabletId, backendId);
        } catch (MetaNotFoundException e) {
            AgentTaskQueue.removeTask(backendId, TTaskType.REALTIME_PUSH, signature);
            LOG.warn("finish push replica error", e);
        } finally {
            olapTable.writeUnlock();
        }
    }

    private void checkReplica(TTabletInfo tTabletInfo, TabletMeta tabletMeta)
            throws MetaNotFoundException {
        long tabletId = tTabletInfo.getTabletId();
        // during finishing stage, index's schema hash switched, when old schema hash finished
        // current index hash != old schema hash and alter job's new schema hash != old schema hash
        // the check replica will failed
        // should use tabletid not pushTabletid because in rollup state, the push tabletid != tabletid
        // and tablet meta will not contain rollupindex's schema hash
        if (tabletMeta == null || tabletMeta == TabletInvertedIndex.NOT_EXIST_TABLET_META) {
            // rollup may be dropped
            throw new MetaNotFoundException("tablet " + tabletId + " does not exist");
        }
    }

    private Replica findRelatedReplica(OlapTable olapTable, Partition partition,
                                                 long backendId, long tabletId, long indexId)
            throws MetaNotFoundException {
        // both normal index and rollingup index are in inverted index
        // this means the index is dropped during load
        if (indexId == TabletInvertedIndex.NOT_EXIST_VALUE) {
            LOG.warn("tablet[{}] may be dropped. push index[{}]", tabletId, indexId);
            return null;
        }
        MaterializedIndex index = partition.getIndex(indexId);
        if (index == null) {
            // In alter job v2 case
            // alter job is always == null, so that we could remove the condition
            // if alter job is always null, then could not covert it to a rollup
            // job, will throw exception, so just throw exception in this case
            if (olapTable.getState() == OlapTableState.ROLLUP) {
                // this happens when:
                // a rollup job is finish and a delete job is the next first job (no load job before)
                // and delete task is first send to base tablet, so it will return 2 tablets info.
                // the second tablet is rollup tablet and it is no longer exist in alterJobs queue.
                // just ignore the rollup tablet info. it will be handled in rollup tablet delete task report.

                // add log to observe
                LOG.warn("Cannot find table[{}].", olapTable.getId());
                return null;
            }
            throw new MetaNotFoundException("Could not find related replica");
        }
        Tablet tablet = index.getTablet(tabletId);
        if (tablet == null) {
            LOG.warn("could not find tablet {} in rollup index {} ", tabletId, indexId);
            return null;
        }
        Replica replica = tablet.getReplicaByBackendId(backendId);
        if (replica == null) {
            LOG.warn("could not find replica with backend {} in tablet {} in rollup index {} ",
                    backendId, tabletId, indexId);
        }
        return replica;
    }

    private void finishClearAlterTask(AgentTask task, TFinishTaskRequest request) {
        ClearAlterTask clearAlterTask = (ClearAlterTask) task;
        clearAlterTask.setFinished(true);
        AgentTaskQueue.removeTask(task.getBackendId(), task.getTaskType(), task.getSignature());
    }

    private void finishPublishVersion(AgentTask task, TFinishTaskRequest request) {
        Map<Long, Long> succTablets = null;
        if (request.isSetSuccTablets()) {
            succTablets = request.getSuccTablets();
        }
        List<Long> errorTabletIds = null;
        if (request.isSetErrorTabletIds()) {
            errorTabletIds = request.getErrorTabletIds();
        }

        if (request.isSetReportVersion()) {
            // report version is required. here we check if set, for compatibility.
            long reportVersion = request.getReportVersion();
            Env.getCurrentSystemInfo().updateBackendReportVersion(
                    task.getBackendId(), reportVersion, task.getDbId(), task.getTableId());
        }

        PublishVersionTask publishVersionTask = (PublishVersionTask) task;
        publishVersionTask.setSuccTablets(succTablets);
        publishVersionTask.addErrorTablets(errorTabletIds);
        publishVersionTask.setFinished(true);

        if (request.getTaskStatus().getStatusCode() != TStatusCode.OK) {
            // not remove the task from queue and be will retry
            return;
        }
        if (request.isSetTableIdToDeltaNumRows()) {
            publishVersionTask.setTableIdToDeltaNumRows(request.getTableIdToDeltaNumRows());
        }
        AgentTaskQueue.removeTask(publishVersionTask.getBackendId(),
                                  publishVersionTask.getTaskType(),
                                  publishVersionTask.getSignature());
    }

    private void finishDropReplica(AgentTask task) {
        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.DROP, task.getSignature());
    }

    private void finishClone(AgentTask task, TFinishTaskRequest request) {
        CloneTask cloneTask = (CloneTask) task;
        if (cloneTask.getTaskVersion() == CloneTask.VERSION_2) {
            Env.getCurrentEnv().getTabletScheduler().finishCloneTask(cloneTask, request);
        } else {
            LOG.warn("invalid clone task, ignore it. {}", task);
        }
        if (request.isSetReportVersion()) {
            long reportVersion = request.getReportVersion();
            Env.getCurrentSystemInfo().updateBackendReportVersion(
                    task.getBackendId(), reportVersion, task.getDbId(), task.getTableId());
        }
        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.CLONE, task.getSignature());
    }

    private void finishStorageMediumMigrate(AgentTask task, TFinishTaskRequest request) {
        StorageMediaMigrationTask migrationTask = (StorageMediaMigrationTask) task;
        Env.getCurrentEnv().getTabletScheduler().finishStorageMediaMigrationTask(migrationTask, request);
        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.STORAGE_MEDIUM_MIGRATE, task.getSignature());
    }

    private void finishConsistencyCheck(AgentTask task, TFinishTaskRequest request) {
        CheckConsistencyTask checkConsistencyTask = (CheckConsistencyTask) task;

        if (checkConsistencyTask.getVersion() != request.getRequestVersion()) {
            LOG.warn("check consistency task is not match. [{}-{}]",
                     checkConsistencyTask.getVersion(), request.getRequestVersion());
            return;
        }

        Env.getCurrentEnv().getConsistencyChecker().handleFinishedConsistencyCheck(checkConsistencyTask,
                                                                                     request.getTabletChecksum());
        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.CHECK_CONSISTENCY, task.getSignature());
    }

    private void finishMakeSnapshot(AgentTask task, TFinishTaskRequest request) {
        SnapshotTask snapshotTask = (SnapshotTask) task;
        if (snapshotTask.isCopyTabletTask()) {
            snapshotTask.setResultSnapshotPath(request.getSnapshotPath());
            snapshotTask.countDown(task.getBackendId(), task.getTabletId());
        } else {
            if (Env.getCurrentEnv().getBackupHandler().handleFinishedSnapshotTask(snapshotTask, request)) {
                AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.MAKE_SNAPSHOT, task.getSignature());
            }
        }
    }

    private void finishUpload(AgentTask task, TFinishTaskRequest request) {
        UploadTask uploadTask = (UploadTask) task;
        if (Env.getCurrentEnv().getBackupHandler().handleFinishedSnapshotUploadTask(uploadTask, request)) {
            AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.UPLOAD, task.getSignature());
        }
    }

    private void finishDownloadTask(AgentTask task, TFinishTaskRequest request) {
        DownloadTask downloadTask = (DownloadTask) task;
        if (Env.getCurrentEnv().getBackupHandler().handleDownloadSnapshotTask(downloadTask, request)) {
            AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.DOWNLOAD, task.getSignature());
        }
    }

    private void finishMoveDirTask(AgentTask task, TFinishTaskRequest request) {
        DirMoveTask dirMoveTask = (DirMoveTask) task;
        if (Env.getCurrentEnv().getBackupHandler().handleDirMoveTask(dirMoveTask, request)) {
            AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.MOVE, task.getSignature());
        }
    }

    private void finishRecoverTablet(AgentTask task) {
        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.RECOVER_TABLET, task.getSignature());
    }

    public TMasterResult report(TReportRequest request) throws TException {
        return reportHandler.handleReport(request);
    }

    private void finishAlterTask(AgentTask task, TFinishTaskRequest request) {
        AlterReplicaTask alterTask = (AlterReplicaTask) task;
        try {
            if (alterTask.getJobType() == JobType.ROLLUP) {
                Env.getCurrentEnv().getMaterializedViewHandler().handleFinishAlterTask(alterTask);
            } else if (alterTask.getJobType() == JobType.SCHEMA_CHANGE) {
                Env.getCurrentEnv().getSchemaChangeHandler().handleFinishAlterTask(alterTask);
            }
            alterTask.setFinished(true);
            if (request.isSetReportVersion()) {
                long reportVersion = request.getReportVersion();
                Env.getCurrentSystemInfo().updateBackendReportVersion(
                        task.getBackendId(), reportVersion, task.getDbId(), task.getTableId());
            }
        } catch (MetaNotFoundException e) {
            LOG.warn("failed to handle finish alter task: {}, {}", task.getSignature(), e.getMessage());
        }
        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.ALTER, task.getSignature());
    }

    private void finishAlterInvertedIndexTask(AgentTask task, TFinishTaskRequest request) {
        TStatus taskStatus = request.getTaskStatus();
        if (taskStatus.getStatusCode() != TStatusCode.OK) {
            LOG.warn("AlterInvertedIndexTask: {} failed, failed times: {}, remaining it in agent task queue",
                     task.getSignature(), task.getFailedTimes());
            return;
        }

        AlterInvertedIndexTask alterInvertedIndexTask = (AlterInvertedIndexTask) task;
        LOG.info("beigin finish AlterInvertedIndexTask: {}, tablet: {}, toString: {}",
                alterInvertedIndexTask.getSignature(),
                alterInvertedIndexTask.getTabletId(),
                alterInvertedIndexTask.toString());
        // TODO: more check
        alterInvertedIndexTask.setFinished(true);
        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.ALTER_INVERTED_INDEX, task.getSignature());
    }

    private void finishPushCooldownConfTask(AgentTask task) {
        PushCooldownConfTask cooldownTask = (PushCooldownConfTask) task;
        cooldownTask.setFinished(true);
        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.PUSH_COOLDOWN_CONF, task.getSignature());
    }
}
