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

import org.apache.doris.alter.AlterJob;
import org.apache.doris.alter.AlterJobV2.JobType;
import org.apache.doris.alter.MaterializedViewHandler;
import org.apache.doris.alter.RollupJob;
import org.apache.doris.alter.SchemaChangeHandler;
import org.apache.doris.alter.SchemaChangeJob;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
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
import org.apache.doris.load.AsyncDeleteJob;
import org.apache.doris.load.DeleteJob;
import org.apache.doris.load.LoadJob;
import org.apache.doris.load.loadv2.SparkLoadJob;
import org.apache.doris.persist.ReplicaPersistInfo;
import org.apache.doris.system.Backend;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.AlterReplicaTask;
import org.apache.doris.task.CheckConsistencyTask;
import org.apache.doris.task.ClearAlterTask;
import org.apache.doris.task.CloneTask;
import org.apache.doris.task.CreateReplicaTask;
import org.apache.doris.task.CreateRollupTask;
import org.apache.doris.task.DirMoveTask;
import org.apache.doris.task.DownloadTask;
import org.apache.doris.task.PublishVersionTask;
import org.apache.doris.task.PushTask;
import org.apache.doris.task.SchemaChangeTask;
import org.apache.doris.task.SnapshotTask;
import org.apache.doris.task.UpdateTabletMetaInfoTask;
import org.apache.doris.task.UploadTask;
import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TFetchResourceResult;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TMasterResult;
import org.apache.doris.thrift.TPushType;
import org.apache.doris.thrift.TReportRequest;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTabletInfo;
import org.apache.doris.thrift.TTaskType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
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
        LOG.debug("get task report: {}", request.toString());
        if (taskStatus.getStatusCode() != TStatusCode.OK) {
            LOG.warn("finish task reports bad. request: {}", request.toString());
        }

        // get backend
        TBackend tBackend = request.getBackend();
        String host = tBackend.getHost();
        int bePort = tBackend.getBePort();
        Backend backend = Catalog.getCurrentSystemInfo().getBackendWithBePort(host, bePort);
        if (backend == null) {
            tStatus.setStatusCode(TStatusCode.CANCELLED);
            List<String> errorMsgs = new ArrayList<>();
            errorMsgs.add("backend not exist.");
            tStatus.setErrorMsgs(errorMsgs);
            LOG.warn("backend does not found. host: {}, be port: {}. task: {}", host, bePort, request.toString());
            return result;
        }

        long backendId = backend.getId();
        TTaskType taskType = request.getTaskType();
        long signature = request.getSignature();
        
        AgentTask task = AgentTaskQueue.getTask(backendId, taskType, signature);
        if (task == null) {
            if (taskType != TTaskType.DROP && taskType != TTaskType.STORAGE_MEDIUM_MIGRATE
                    && taskType != TTaskType.RELEASE_SNAPSHOT && taskType != TTaskType.CLEAR_TRANSACTION_TASK) {
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
                String errMsg = "task type: " + taskType + ", status_code: " + taskStatus.getStatusCode().toString() +
                        (taskStatus.isSetErrorMsgs() ? (", status_message: " + taskStatus.getErrorMsgs()) : "") +
                        ", backendId: " + backend + ", signature: " + signature;
                task.setErrorMsg(errMsg);
                // We start to let FE perceive the task's error msg
                if (taskType != TTaskType.MAKE_SNAPSHOT && taskType != TTaskType.UPLOAD
                        && taskType != TTaskType.DOWNLOAD && taskType != TTaskType.MOVE
                        && taskType != TTaskType.CLONE && taskType != TTaskType.PUBLISH_VERSION
                        && taskType != TTaskType.CREATE && taskType != TTaskType.UPDATE_TABLET_META_INFO) {
                    return result;
                }
            }
        }
 
        try {
            List<TTabletInfo> finishTabletInfos;
            switch (taskType) {
                case CREATE:
                    Preconditions.checkState(request.isSetReportVersion());
                    finishCreateReplica(task, request);
                    break;
                case PUSH:
                    checkHasTabletInfo(request);
                    Preconditions.checkState(request.isSetReportVersion());
                    finishPush(task, request);
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
                    Preconditions.checkState(request.isSetReportVersion());
                    checkHasTabletInfo(request);
                    finishTabletInfos = request.getFinishTabletInfos();
                    finishSchemaChange(task, finishTabletInfos, request.getReportVersion());
                    break;
                case ROLLUP:
                    checkHasTabletInfo(request);
                    finishTabletInfos = request.getFinishTabletInfos();
                    finishRollup(task, finishTabletInfos);
                    break;
                case CLONE:
                    finishClone(task, request);
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
                    finishAlterTask(task);
                    break;
                case UPDATE_TABLET_META_INFO:
                    finishUpdateTabletMeta(task, request);
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
                createReplicaTask.countDownToZero(task.getBackendId() + ": " + request.getTaskStatus().getErrorMsgs().toString());
            } else {
                long tabletId = createReplicaTask.getTabletId();

                if (request.isSetFinishTabletInfos()) {
                    Replica replica = Catalog.getCurrentInvertedIndex().getReplica(createReplicaTask.getTabletId(),
                            createReplicaTask.getBackendId());
                    replica.setPathHash(request.getFinishTabletInfos().get(0).getPathHash());

                    if (createReplicaTask.isRecoverTask()) {
                        /**
                         * This create replica task may be generated by recovery(See comment of Config.recover_with_empty_tablet)
                         * So we set replica back to good.
                         */
                        replica.setBad(false);
                        LOG.info("finish recover create replica task. set replica to good. tablet {}, replica {}, backend {}",
                                tabletId, task.getBackendId(), replica.getId());
                    }
                }
                
                // this should be called before 'countDownLatch()'
                Catalog.getCurrentSystemInfo().updateBackendReportVersion(task.getBackendId(), request.getReportVersion(), task.getDbId(), task.getTableId());
                
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
                tabletTask.countDownToZero(task.getBackendId() + ": " + request.getTaskStatus().getErrorMsgs().toString());
            } else {
                tabletTask.countDownLatch(task.getBackendId(), tabletTask.getTablets());
                LOG.debug("finish update tablet meta. tablet id: {}, be: {}", tabletTask.getTablets(), task.getBackendId());
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
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
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

        OlapTable olapTable = (OlapTable) db.getTable(tableId);
        if (olapTable == null) {
            AgentTaskQueue.removeTask(backendId, TTaskType.REALTIME_PUSH, signature);
            LOG.warn("finish push replica error, cannot find table[" + tableId + "] when push finished");
            return;
        }
        olapTable.writeLock();
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
            Catalog.getCurrentSystemInfo().updateBackendReportVersion(task.getBackendId(), reportVersion,
                                                                       task.getDbId(), task.getTableId());

            List<Long> tabletIds = finishTabletInfos.stream().map(
                    tTabletInfo -> tTabletInfo.getTabletId()).collect(Collectors.toList());
            List<TabletMeta> tabletMetaList = Catalog.getCurrentInvertedIndex().getTabletMetaList(tabletIds);

            // handle load job
            // TODO yiguolei: why delete should check request version and task version?
            if (pushTask.getPushType() == TPushType.LOAD || pushTask.getPushType() == TPushType.LOAD_DELETE) {
                long loadJobId = pushTask.getLoadJobId();
                LoadJob job = Catalog.getCurrentCatalog().getLoadInstance().getLoadJob(loadJobId);
                if (job == null) {
                    throw new MetaNotFoundException("cannot find load job, job[" + loadJobId + "]");
                }

                for (int i = 0; i < tabletMetaList.size(); i++) {
                    TabletMeta tabletMeta = tabletMetaList.get(i);
                    checkReplica(finishTabletInfos.get(i), tabletMeta);
                    long tabletId = tabletIds.get(i);
                    Replica replica = findRelatedReplica(olapTable, partition, backendId, tabletId, tabletMeta.getIndexId());
                    // if the replica is under schema change, could not find the replica with aim schema hash
                    if (replica != null) {
                        job.addFinishedReplica(replica);
                    }
                }
            } else if (pushTask.getPushType() == TPushType.DELETE) {
                DeleteJob deleteJob = Catalog.getCurrentCatalog().getDeleteHandler().getDeleteJob(transactionId);
                if (deleteJob == null) {
                    throw new MetaNotFoundException("cannot find delete job, job[" + transactionId + "]");
                }
                for (int i = 0; i < tabletMetaList.size(); i++) {
                    TabletMeta tabletMeta = tabletMetaList.get(i);
                    long tabletId = tabletIds.get(i);
                    Replica replica = findRelatedReplica(olapTable, partition,
                            backendId, tabletId, tabletMeta.getIndexId());
                    if (replica != null) {
                        deleteJob.addFinishedReplica(pushTabletId, replica);
                        pushTask.countDownLatch(backendId, pushTabletId);
                    }
                }
            } else if (pushTask.getPushType() == TPushType.LOAD_V2) {
                long loadJobId = pushTask.getLoadJobId();
                org.apache.doris.load.loadv2.LoadJob job = Catalog.getCurrentCatalog().getLoadManager().getLoadJob(loadJobId);
                if (job == null) {
                    throw new MetaNotFoundException("cannot find load job, job[" + loadJobId + "]");
                }
                for (int i = 0; i < tabletMetaList.size(); i++) {
                    TabletMeta tabletMeta = tabletMetaList.get(i);
                    checkReplica(finishTabletInfos.get(i), tabletMeta);
                    long tabletId = tabletIds.get(i);
                    Replica replica = findRelatedReplica(olapTable, partition, backendId, tabletId, tabletMeta.getIndexId());
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
        int schemaHash = tTabletInfo.getSchemaHash();
        // during finishing stage, index's schema hash switched, when old schema hash finished
        // current index hash != old schema hash and alter job's new schema hash != old schema hash
        // the check replica will failed
        // should use tabletid not pushTabletid because in rollup state, the push tabletid != tabletid
        // and tablet meta will not contain rollupindex's schema hash
        if (tabletMeta == null || tabletMeta == TabletInvertedIndex.NOT_EXIST_TABLET_META) {
            // rollup may be dropped
            throw new MetaNotFoundException("tablet " + tabletId + " does not exist");
        }
        if (!tabletMeta.containsSchemaHash(schemaHash)) {
            throw new MetaNotFoundException("tablet[" + tabletId
                    + "] schemaHash is not equal to index's switchSchemaHash. "
                    + tabletMeta.toString()+ " vs. " + schemaHash);
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
            // this means the index is under rollup
            MaterializedViewHandler materializedViewHandler = Catalog.getCurrentCatalog().getRollupHandler();
            AlterJob alterJob = materializedViewHandler.getAlterJob(olapTable.getId());
            if (alterJob == null && olapTable.getState() == OlapTableState.ROLLUP) {
                // this happens when:
                // a rollup job is finish and a delete job is the next first job (no load job before)
                // and delete task is first send to base tablet, so it will return 2 tablets info.
                // the second tablet is rollup tablet and it is no longer exist in alterJobs queue.
                // just ignore the rollup tablet info. it will be handled in rollup tablet delete task report.

                // add log to observe
                LOG.warn("Cannot find table[{}].", olapTable.getId());
                return null;
            }
            RollupJob rollupJob = (RollupJob) alterJob;
            MaterializedIndex rollupIndex = rollupJob.getRollupIndex(partition.getId());

            if (rollupIndex == null) {
                LOG.warn("could not find index for tablet {}", tabletId);
                return null;
            }
            index = rollupIndex;
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
    
    private void finishPush(AgentTask task, TFinishTaskRequest request) {
        List<TTabletInfo> finishTabletInfos = request.getFinishTabletInfos();
        Preconditions.checkState(finishTabletInfos != null && !finishTabletInfos.isEmpty());
        
        PushTask pushTask = (PushTask) task;
        // if replica report already update replica version and load checker add new version push task,
        // we might get new version push task, so check task version first
        // all tablets in tablet infos should have same version and version hash
        long finishVersion = finishTabletInfos.get(0).getVersion();
        long finishVersionHash = finishTabletInfos.get(0).getVersionHash();
        long taskVersion = pushTask.getVersion();
        if (finishVersion != taskVersion) {
            LOG.debug("finish tablet version is not consistent with task. "
                    + "finish version: {}, finish version hash: {}, task: {}", 
                    finishVersion, finishVersionHash, pushTask);
            return;
        }
        
        long dbId = pushTask.getDbId();
        long backendId = pushTask.getBackendId();
        long signature = task.getSignature();
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            AgentTaskQueue.removePushTask(backendId, signature, finishVersion, finishVersionHash, 
                                          pushTask.getPushType(), pushTask.getTaskType());
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

        OlapTable olapTable = (OlapTable) db.getTable(tableId);
        if (olapTable == null) {
            AgentTaskQueue.removeTask(backendId, TTaskType.REALTIME_PUSH, signature);
            LOG.warn("finish push replica error, cannot find table[" + tableId + "] when push finished");
            return;
        }

        olapTable.writeLock();
        try {
            Partition partition = olapTable.getPartition(partitionId);
            if (partition == null) {
                throw new MetaNotFoundException("cannot find partition[" + partitionId + "] when push finished");
            }

            // update replica version and versionHash
            List<ReplicaPersistInfo> infos = new LinkedList<ReplicaPersistInfo>();
            List<Long> tabletIds = finishTabletInfos.stream().map(
                    finishTabletInfo -> finishTabletInfo.getTabletId()).collect(Collectors.toList());
            List<TabletMeta> tabletMetaList = Catalog.getCurrentInvertedIndex().getTabletMetaList(tabletIds);
            for (int i = 0; i < tabletMetaList.size(); i++) {
                TabletMeta tabletMeta = tabletMetaList.get(i);
                TTabletInfo tTabletInfo = finishTabletInfos.get(i);
                long indexId = tabletMeta.getIndexId();
                ReplicaPersistInfo info = updateReplicaInfo(olapTable, partition,
                        backendId, pushIndexId, indexId,
                        tTabletInfo, pushState);
                if (info != null) {
                    infos.add(info);
                }
            }

            // should be done before addReplicaPersistInfos and countDownLatch
            long reportVersion = request.getReportVersion();
            Catalog.getCurrentSystemInfo().updateBackendReportVersion(task.getBackendId(), reportVersion,
                                                                       task.getDbId(), task.getTableId());

            if (pushTask.getPushType() == TPushType.LOAD || pushTask.getPushType() == TPushType.LOAD_DELETE) {
                // handle load job
                long loadJobId = pushTask.getLoadJobId();
                LoadJob job = Catalog.getCurrentCatalog().getLoadInstance().getLoadJob(loadJobId);
                if (job == null) {
                    throw new MetaNotFoundException("cannot find load job, job[" + loadJobId + "]");
                }
                
                Preconditions.checkState(!infos.isEmpty());
                for (ReplicaPersistInfo info : infos) {
                    job.addReplicaPersistInfos(info);
                }
            } else if (pushTask.getPushType() == TPushType.DELETE) {
                // report delete task must match version and version hash
                if (pushTask.getVersion() != request.getRequestVersion()) {
                    throw new MetaNotFoundException("delete task is not match. [" + pushTask.getVersion() + "-"
                            + request.getRequestVersion() + "]");
                }

                if (pushTask.isSyncDelete()) {
                    pushTask.countDownLatch(backendId, signature);
                } else {
                    long asyncDeleteJobId = pushTask.getAsyncDeleteJobId();
                    Preconditions.checkState(asyncDeleteJobId != -1);
                    AsyncDeleteJob job = Catalog.getCurrentCatalog().getLoadInstance().getAsyncDeleteJob(asyncDeleteJobId);
                    if (job == null) {
                        throw new MetaNotFoundException("cannot find async delete job, job[" + asyncDeleteJobId + "]");
                    }

                    Preconditions.checkState(!infos.isEmpty());
                    for (ReplicaPersistInfo info : infos) {
                        job.addReplicaPersistInfos(info);
                    }
                }
            }

            AgentTaskQueue.removePushTask(backendId, signature, finishVersion, finishVersionHash,
                                          pushTask.getPushType(), pushTask.getTaskType());
            LOG.debug("finish push replica. tabletId: {}, backendId: {}", pushTabletId, backendId);
        } catch (MetaNotFoundException e) {
            AgentTaskQueue.removePushTask(backendId, signature, finishVersion, finishVersionHash,
                                          pushTask.getPushType(), pushTask.getTaskType());
            LOG.warn("finish push replica error", e);
        } finally {
            olapTable.writeUnlock();
        }
    }
    
    private void finishClearAlterTask(AgentTask task, TFinishTaskRequest request) {
        ClearAlterTask clearAlterTask = (ClearAlterTask) task;
        clearAlterTask.setFinished(true);
        AgentTaskQueue.removeTask(task.getBackendId(), task.getTaskType(), task.getSignature());
    }
    
    private void finishPublishVersion(AgentTask task, TFinishTaskRequest request) {
        List<Long> errorTabletIds = null;
        if (request.isSetErrorTabletIds()) {
            errorTabletIds = request.getErrorTabletIds();
        }

        if (request.isSetReportVersion()) {
            // report version is required. here we check if set, for compatibility.
            long reportVersion = request.getReportVersion();
            Catalog.getCurrentSystemInfo().updateBackendReportVersion(task.getBackendId(), reportVersion, task.getDbId(), task.getTableId());
        }

        PublishVersionTask publishVersionTask = (PublishVersionTask) task;
        publishVersionTask.addErrorTablets(errorTabletIds);
        publishVersionTask.setIsFinished(true);

        if (request.getTaskStatus().getStatusCode() != TStatusCode.OK) {
            // not remove the task from queue and be will retry
            return;
        }
        AgentTaskQueue.removeTask(publishVersionTask.getBackendId(), 
                                  publishVersionTask.getTaskType(), 
                                  publishVersionTask.getSignature());
    }
    
    private ReplicaPersistInfo updateReplicaInfo(OlapTable olapTable, Partition partition,
                                                 long backendId, long pushIndexId, long indexId,
                                                 TTabletInfo tTabletInfo, PartitionState pushState)
            throws MetaNotFoundException {
        long tabletId = tTabletInfo.getTabletId();
        int schemaHash = tTabletInfo.getSchemaHash();
        long version = tTabletInfo.getVersion();
        long versionHash = tTabletInfo.getVersionHash();
        long rowCount = tTabletInfo.getRowCount();
        long dataSize = tTabletInfo.getDataSize();

        if (indexId != pushIndexId) {
            // this may be a rollup tablet
            if (pushState != PartitionState.ROLLUP && indexId != TabletInvertedIndex.NOT_EXIST_VALUE) {
                // this probably should not happened. add log to observe(cmy)
                LOG.warn("push task report tablet[{}] with different index[{}] and is not in ROLLUP. push index[{}]",
                         tabletId, indexId, pushIndexId);
                return null;
            }

            if (indexId == TabletInvertedIndex.NOT_EXIST_VALUE) {
                LOG.warn("tablet[{}] may be dropped. push index[{}]", tabletId, pushIndexId);
                return null;
            }

            MaterializedViewHandler materializedViewHandler = Catalog.getCurrentCatalog().getRollupHandler();
            AlterJob alterJob = materializedViewHandler.getAlterJob(olapTable.getId());
            if (alterJob == null) {
                // this happens when:
                // a rollup job is finish and a delete job is the next first job (no load job before)
                // and delete task is first send to base tablet, so it will return 2 tablets info.
                // the second tablet is rollup tablet and it is no longer exist in alterJobs queue.
                // just ignore the rollup tablet info. it will be handled in rollup tablet delete task report.

                // add log to observe
                LOG.warn("Cannot find table[{}].", olapTable.getId());
                return null;
            }

            ((RollupJob) alterJob).updateRollupReplicaInfo(partition.getId(), indexId, tabletId, backendId,
                                                           schemaHash, version, versionHash, rowCount, dataSize);
            // replica info is saved in rollup job, not in load job
            return null;
        }

        int currentSchemaHash = olapTable.getSchemaHashByIndexId(pushIndexId);
        if (schemaHash != currentSchemaHash) {
            if (pushState == PartitionState.SCHEMA_CHANGE) {
                SchemaChangeHandler schemaChangeHandler = Catalog.getCurrentCatalog().getSchemaChangeHandler();
                AlterJob alterJob = schemaChangeHandler.getAlterJob(olapTable.getId());
                if (alterJob != null && schemaHash != ((SchemaChangeJob) alterJob).getSchemaHashByIndexId(pushIndexId)) {
                    // this is a invalid tablet.
                    throw new MetaNotFoundException("tablet[" + tabletId
                            + "] schemaHash is not equal to index's switchSchemaHash. "
                            + ((SchemaChangeJob) alterJob).getSchemaHashByIndexId(pushIndexId) + " vs. " + schemaHash);
                }
            } else {
                // this should not happen. observe(cmy)
                throw new MetaNotFoundException("Diff tablet[" + tabletId + "] schemaHash. index[" + pushIndexId + "]: "
                        + currentSchemaHash + " vs. " + schemaHash);
            }
        }

        MaterializedIndex materializedIndex = partition.getIndex(pushIndexId);
        if (materializedIndex == null) {
            throw new MetaNotFoundException("Cannot find index[" + pushIndexId + "]");
        }
        Tablet tablet = materializedIndex.getTablet(tabletId);
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

        LOG.debug("replica[{}] report schemaHash:{}", replica.getId(), schemaHash);
        return ReplicaPersistInfo.createForLoad(olapTable.getId(), partition.getId(), pushIndexId, tabletId,
                replica.getId(), version, versionHash, schemaHash, dataSize, rowCount);
    }

    private void finishDropReplica(AgentTask task) {
        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.DROP, task.getSignature());
    }

    private void finishSchemaChange(AgentTask task, List<TTabletInfo> finishTabletInfos, long reportVersion)
            throws MetaNotFoundException {
        Preconditions.checkArgument(finishTabletInfos != null && !finishTabletInfos.isEmpty());
        Preconditions.checkArgument(finishTabletInfos.size() == 1);

        SchemaChangeTask schemaChangeTask = (SchemaChangeTask) task;
        SchemaChangeHandler schemaChangeHandler = Catalog.getCurrentCatalog().getSchemaChangeHandler();
        schemaChangeHandler.handleFinishedReplica(schemaChangeTask, finishTabletInfos.get(0), reportVersion);
        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.SCHEMA_CHANGE, task.getSignature());
    }

    private void finishRollup(AgentTask task, List<TTabletInfo> finishTabletInfos)
            throws MetaNotFoundException {
        Preconditions.checkArgument(finishTabletInfos != null && !finishTabletInfos.isEmpty());
        Preconditions.checkArgument(finishTabletInfos.size() == 1);

        CreateRollupTask createRollupTask = (CreateRollupTask) task;
        MaterializedViewHandler materializedViewHandler = Catalog.getCurrentCatalog().getRollupHandler();
        materializedViewHandler.handleFinishedReplica(createRollupTask, finishTabletInfos.get(0), -1L);
        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.ROLLUP, task.getSignature());
    }

    private void finishClone(AgentTask task, TFinishTaskRequest request) {
        CloneTask cloneTask = (CloneTask) task;
        if (cloneTask.getTaskVersion() == CloneTask.VERSION_2) {
            Catalog.getCurrentCatalog().getTabletScheduler().finishCloneTask(cloneTask, request);
        } else {
            LOG.warn("invalid clone task, ignore it. {}", task);
        }

        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.CLONE, task.getSignature());
    }

    private void finishConsistencyCheck(AgentTask task, TFinishTaskRequest request) {
        CheckConsistencyTask checkConsistencyTask = (CheckConsistencyTask) task;

        if (checkConsistencyTask.getVersion() != request.getRequestVersion()) {
            LOG.warn("check consistency task is not match. [{}-{}]",
                     checkConsistencyTask.getVersion(), request.getRequestVersion());
            return;
        }

        Catalog.getCurrentCatalog().getConsistencyChecker().handleFinishedConsistencyCheck(checkConsistencyTask,
                                                                                     request.getTabletChecksum());
        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.CHECK_CONSISTENCY, task.getSignature());
    }

    private void finishMakeSnapshot(AgentTask task, TFinishTaskRequest request) {
        SnapshotTask snapshotTask = (SnapshotTask) task;
        if (Catalog.getCurrentCatalog().getBackupHandler().handleFinishedSnapshotTask(snapshotTask, request)) {
            AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.MAKE_SNAPSHOT, task.getSignature());
        }

    }

    private void finishUpload(AgentTask task, TFinishTaskRequest request) {
        UploadTask uploadTask = (UploadTask) task;
        if (Catalog.getCurrentCatalog().getBackupHandler().handleFinishedSnapshotUploadTask(uploadTask, request)) {
            AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.UPLOAD, task.getSignature());
        }
    }

    private void finishDownloadTask(AgentTask task, TFinishTaskRequest request) {
        DownloadTask downloadTask = (DownloadTask) task;
        if (Catalog.getCurrentCatalog().getBackupHandler().handleDownloadSnapshotTask(downloadTask, request)) {
            AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.DOWNLOAD, task.getSignature());
        }
    }

    private void finishMoveDirTask(AgentTask task, TFinishTaskRequest request) {
        DirMoveTask dirMoveTask = (DirMoveTask) task;
        if (Catalog.getCurrentCatalog().getBackupHandler().handleDirMoveTask(dirMoveTask, request)) {
            AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.MOVE, task.getSignature());
        }
    }
    
    private void finishRecoverTablet(AgentTask task) {
        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.RECOVER_TABLET, task.getSignature());
    }

    public TMasterResult report(TReportRequest request) throws TException {
        return reportHandler.handleReport(request);
    }

    public TFetchResourceResult fetchResource() {
        return Catalog.getCurrentCatalog().getAuth().toResourceThrift();
    }

    private void finishAlterTask(AgentTask task) {
        AlterReplicaTask alterTask = (AlterReplicaTask) task;
        try {
            if (alterTask.getJobType() == JobType.ROLLUP) {
                Catalog.getCurrentCatalog().getRollupHandler().handleFinishAlterTask(alterTask);
            } else if (alterTask.getJobType() == JobType.SCHEMA_CHANGE) {
                Catalog.getCurrentCatalog().getSchemaChangeHandler().handleFinishAlterTask(alterTask);
            }
            alterTask.setFinished(true);
        } catch (MetaNotFoundException e) {
            LOG.warn("failed to handle finish alter task: {}, {}", task.getSignature(), e.getMessage());
        }
        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.ALTER, task.getSignature());
    }
}
