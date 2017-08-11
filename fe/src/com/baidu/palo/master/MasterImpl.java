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

package com.baidu.palo.master;

import com.baidu.palo.alter.AlterJob;
import com.baidu.palo.alter.RollupHandler;
import com.baidu.palo.alter.RollupJob;
import com.baidu.palo.alter.SchemaChangeHandler;
import com.baidu.palo.alter.SchemaChangeJob;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.Replica;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.catalog.MaterializedIndex;
import com.baidu.palo.catalog.Tablet;
import com.baidu.palo.catalog.Partition;
import com.baidu.palo.catalog.TabletInvertedIndex;
import com.baidu.palo.catalog.Partition.PartitionState;
import com.baidu.palo.common.MetaNotFoundException;
import com.baidu.palo.load.AsyncDeleteJob;
import com.baidu.palo.load.LoadJob;
import com.baidu.palo.persist.ReplicaPersistInfo;
import com.baidu.palo.system.Backend;
import com.baidu.palo.task.AgentTask;
import com.baidu.palo.task.AgentTaskQueue;
import com.baidu.palo.task.CheckConsistencyTask;
import com.baidu.palo.task.CloneTask;
import com.baidu.palo.task.CreateReplicaTask;
import com.baidu.palo.task.CreateRollupTask;
import com.baidu.palo.task.PushTask;
import com.baidu.palo.task.RestoreTask;
import com.baidu.palo.task.SchemaChangeTask;
import com.baidu.palo.task.SnapshotTask;
import com.baidu.palo.task.UploadTask;
import com.baidu.palo.thrift.TBackend;
import com.baidu.palo.thrift.TFetchResourceResult;
import com.baidu.palo.thrift.TFinishTaskRequest;
import com.baidu.palo.thrift.TMasterResult;
import com.baidu.palo.thrift.TPushType;
import com.baidu.palo.thrift.TReportRequest;
import com.baidu.palo.thrift.TStatus;
import com.baidu.palo.thrift.TStatusCode;
import com.baidu.palo.thrift.TTabletInfo;
import com.baidu.palo.thrift.TTaskType;

import com.google.common.base.Preconditions;

import org.apache.thrift.TException;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class MasterImpl {
    private static final Logger LOG = LogManager.getLogger(MasterImpl.class);

    public MasterImpl() {

    }
    
    public TMasterResult finishTask(TFinishTaskRequest request) throws TException {
        TMasterResult result = new TMasterResult();
        TStatus tStatus = new TStatus(TStatusCode.OK);
        result.setStatus(tStatus);
        // check task status
        // retry task by report process
        TStatus taskStatus = request.getTask_status();
        LOG.debug("get task report: {}", request.toString());
        if (taskStatus.getStatus_code() != TStatusCode.OK) {
            LOG.warn("finish task reports bad. request: {}", request.toString());
        }

        // get backend
        TBackend tBackend = request.getBackend();
        String host = tBackend.getHost();
        int bePort = tBackend.getBe_port();
        Backend backend = Catalog.getCurrentSystemInfo().getBackendWithBePort(host, bePort);
        if (backend == null) {
            tStatus.setStatus_code(TStatusCode.CANCELLED);
            List<String> errorMsgs = new ArrayList<String>();
            errorMsgs.add("backend not exist.");
            tStatus.setError_msgs(errorMsgs);
            LOG.warn("backend does not found. host: {}, be port: {}. task: {}", host, bePort, request.toString());
            return result;
        }

        long backendId = backend.getId();
        TTaskType taskType = request.getTask_type();
        long signature = request.getSignature();
        
        AgentTask task = AgentTaskQueue.getTask(backendId, taskType, signature);
        if (task == null) {
            if (taskType != TTaskType.DROP && taskType != TTaskType.STORAGE_MEDIUM_MIGRATE
                    && taskType != TTaskType.CANCEL_DELETE && taskType != TTaskType.RELEASE_SNAPSHOT) {
                String errMsg = "cannot find task. type: " + taskType + ", backendId: " + backendId
                        + ", signature: " + signature;
                LOG.warn(errMsg);
                tStatus.setStatus_code(TStatusCode.CANCELLED);
                List<String> errorMsgs = new ArrayList<String>();
                errorMsgs.add(errMsg);
                tStatus.setError_msgs(errorMsgs);
            }
            return result;
        } else {
            if (taskStatus.getStatus_code() != TStatusCode.OK) {
                task.failed();
                return result;
            }
        }
 
        try {
            List<TTabletInfo> finishTabletInfos = null;
            switch (taskType) {
                case CREATE:
                    Preconditions.checkState(request.isSetReport_version());
                    finishCreateReplica(task, request.getReport_version());
                    break;
                case PUSH:
                    checkHasTabletInfo(request);
                    Preconditions.checkState(request.isSetReport_version());
                    finishPush(task, request);
                    break;
                case DROP:
                    finishDropReplica(task);
                    break;
                case SCHEMA_CHANGE:
                    Preconditions.checkState(request.isSetReport_version());
                    checkHasTabletInfo(request);
                    finishTabletInfos = request.getFinish_tablet_infos();
                    finishSchemaChange(task, finishTabletInfos, request.getReport_version());
                    break;
                case ROLLUP:
                    checkHasTabletInfo(request);
                    finishTabletInfos = request.getFinish_tablet_infos();
                    finishRollup(task, finishTabletInfos);
                    break;
                case CLONE:
                    checkHasTabletInfo(request);
                    finishTabletInfos = request.getFinish_tablet_infos();
                    finishClone(task, finishTabletInfos);
                    break;
                case CHECK_CONSISTENCY:
                    finishConsistenctCheck(task, request);
                    break;
                case MAKE_SNAPSHOT:
                    Preconditions.checkState(request.isSetSnapshot_path());
                    finishMakeSnapshot(task, request.getSnapshot_path());
                    break;
                case UPLOAD:
                    finishUpload(task);
                    break;
                case RESTORE:
                    finishRestore(task);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            tStatus.setStatus_code(TStatusCode.CANCELLED);
            String errMsg = "finish agent task error.";
            LOG.warn(errMsg, e);
            List<String> errorMsgs = new ArrayList<String>();
            errorMsgs.add(errMsg);
            tStatus.setError_msgs(errorMsgs);
        }

        if (tStatus.getStatus_code() == TStatusCode.OK) {
            LOG.debug("report task success. {}", request.toString());
        }

        return result;
    }

    private void checkHasTabletInfo(TFinishTaskRequest request) throws Exception {
        if (!request.isSetFinish_tablet_infos() || request.getFinish_tablet_infos().isEmpty()) {
            throw new Exception("tablet info is not set");
        }
    }

    private void finishCreateReplica(AgentTask task, long reportVersion) {
        // if we get here, this task will be removed from AgentTaskQueue for certain.
        // because in this function, the only problem that cause failure is meta missing.
        // and if meta is missing, we no longer need to resend this task

        CreateReplicaTask createReplicaTask = (CreateReplicaTask) task;
        long tabletId = createReplicaTask.getTabletId();

        // this should be called before 'countDownLatch()'
        Catalog.getCurrentSystemInfo().updateBackendReportVersion(task.getBackendId(), reportVersion, task.getDbId());

        createReplicaTask.countDownLatch(task.getBackendId(), task.getSignature());
        LOG.debug("finish create replica. tablet id: {}", tabletId);
        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.CREATE, task.getSignature());
    }
    
    private void finishPush(AgentTask task, TFinishTaskRequest request) {
        List<TTabletInfo> finishTabletInfos = request.getFinish_tablet_infos();
        Preconditions.checkState(finishTabletInfos != null && !finishTabletInfos.isEmpty());
        
        PushTask pushTask = (PushTask) task;
        // if replica report already update replica version and load checker add new version push task,
        // we might get new version push task, so check task version first
        // all tablets in tablet infos should have same version and version hash
        long finishVersion = finishTabletInfos.get(0).getVersion();
        long finishVersionHash = finishTabletInfos.get(0).getVersion_hash();
        long taskVersion = pushTask.getVersion();
        long taskVersionHash = pushTask.getVersionHash();
        if (finishVersion != taskVersion || finishVersionHash != taskVersionHash) {
            LOG.debug("finish tablet version is not consistent with task. "
                    + "finish version: {}, finish version hash: {}, task: {}", 
                    finishVersion, finishVersionHash, pushTask);
            return;
        }
        
        long dbId = pushTask.getDbId();
        long backendId = pushTask.getBackendId();
        long signature = task.getSignature();
        Database db = Catalog.getInstance().getDb(dbId);
        if (db == null) {
            AgentTaskQueue.removePushTask(backendId, signature, finishVersion, finishVersionHash, 
                                          pushTask.getPushType());
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
            if (finishTabletInfos.get(0).getTablet_id() == finishTabletInfos.get(1).getTablet_id()) {
                pushState = PartitionState.SCHEMA_CHANGE;
            } else {
                pushState = PartitionState.ROLLUP;
            }
        } else {
            LOG.warn("invalid push report infos. finishTabletInfos' size: " + finishTabletInfos.size());
            return;
        }

        LOG.debug("push report state: {}", pushState.name());

        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(tableId);
            if (olapTable == null) {
                throw new MetaNotFoundException("cannot find table[" + tableId + "] when push finished");
            }

            Partition partition = olapTable.getPartition(partitionId);
            if (partition == null) {
                throw new MetaNotFoundException("cannot find partition[" + partitionId + "] when push finished");
            }

            // update replica version and versionHash
            List<ReplicaPersistInfo> infos = new LinkedList<ReplicaPersistInfo>();
            for (TTabletInfo tTabletInfo : finishTabletInfos) {
                ReplicaPersistInfo info = updateReplicaInfo(olapTable, partition,
                                                            backendId, pushIndexId, pushTabletId,
                                                            tTabletInfo, pushState);
                if (info != null) {
                    infos.add(info);
                }
            }

            // should be done before addReplicaPersistInfos and countDownLatch
            long reportVersion = request.getReport_version();
            Catalog.getCurrentSystemInfo().updateBackendReportVersion(task.getBackendId(), reportVersion,
                                                                       task.getDbId());

            if (pushTask.getPushType() == TPushType.LOAD || pushTask.getPushType() == TPushType.LOAD_DELETE) {
                // handle load job
                long loadJobId = pushTask.getLoadJobId();
                LoadJob job = Catalog.getInstance().getLoadInstance().getLoadJob(loadJobId);
                if (job == null) {
                    throw new MetaNotFoundException("cannot find load job, job[" + loadJobId + "]");
                }
                
                Preconditions.checkState(!infos.isEmpty());
                for (ReplicaPersistInfo info : infos) {
                    job.addReplicaPersistInfos(info);
                }
            } else if (pushTask.getPushType() == TPushType.DELETE) {
                // report delete task must match version and version hash
                if (pushTask.getVersion() != request.getRequest_version()
                        || pushTask.getVersionHash() != request.getRequest_version_hash()) {
                    throw new MetaNotFoundException("delete task is not match. [" + pushTask.getVersion() + "-"
                            + request.getRequest_version() + "]");
                }

                if (pushTask.isSyncDelete()) {
                    pushTask.countDownLatch(backendId, signature);
                } else {
                    long asyncDeleteJobId = pushTask.getAsyncDeleteJobId();
                    Preconditions.checkState(asyncDeleteJobId != -1);
                    AsyncDeleteJob job = Catalog.getInstance().getLoadInstance().getAsyncDeleteJob(asyncDeleteJobId);
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
                                          pushTask.getPushType());
            LOG.debug("finish push replica. tabletId: {}, backendId: {}", pushTabletId, backendId);
        } catch (MetaNotFoundException e) {
            AgentTaskQueue.removePushTask(backendId, signature, finishVersion, finishVersionHash,
                                          pushTask.getPushType());
            LOG.warn("finish push replica error", e);
        } finally {

            db.writeUnlock();
        }
    }
    
    private ReplicaPersistInfo updateReplicaInfo(OlapTable olapTable, Partition partition,
                                                 long backendId, long pushIndexId, long pushTabletId,
                                                 TTabletInfo tTabletInfo, PartitionState pushState)
            throws MetaNotFoundException {
        long tabletId = tTabletInfo.getTablet_id();
        int schemaHash = tTabletInfo.getSchema_hash();
        long version = tTabletInfo.getVersion();
        long versionHash = tTabletInfo.getVersion_hash();
        long rowCount = tTabletInfo.getRow_count();
        long dataSize = tTabletInfo.getData_size();

        long indexId = Catalog.getCurrentInvertedIndex().getIndexId(tabletId);
        if (indexId != pushIndexId) {
            // this may be a rollup tablet
            if (pushState != PartitionState.ROLLUP && indexId != TabletInvertedIndex.NOT_EXIST_VALUE) {
                // this probably should not happend. add log to observe(cmy)
                LOG.warn("push task report tablet[{}] with different index[{}] and is not in ROLLUP. push index[{}]",
                         tabletId, indexId, pushIndexId);
                return null;
            }

            if (indexId == TabletInvertedIndex.NOT_EXIST_VALUE) {
                LOG.warn("tablet[{}] may be dropped. push index[{}]", tabletId, pushIndexId);
                return null;
            }

            RollupHandler rollupHandler = Catalog.getInstance().getRollupHandler();
            AlterJob alterJob = rollupHandler.getAlterJob(olapTable.getId());
            if (alterJob == null) {
                // this happends when:
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
                SchemaChangeHandler schemaChangeHandler = Catalog.getInstance().getSchemaChangeHandler();
                AlterJob alterJob = schemaChangeHandler.getAlterJob(olapTable.getId());
                if (alterJob != null && schemaHash != ((SchemaChangeJob) alterJob).getSchemaHashByIndexId(pushIndexId)) {
                    // this is a invalid tablet.
                    throw new MetaNotFoundException("tablet[" + tabletId
                            + "] schemaHash is not equal to index's switchSchemaHash. "
                            + ((SchemaChangeJob) alterJob).getSchemaHashByIndexId(pushIndexId) + " vs. " + schemaHash);
                }
            } else {
                // this should not happend. observe(cmy)
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
        replica.updateInfo(version, versionHash, dataSize, rowCount);

        LOG.debug("replica[{}] report schemaHash:{}", replica.getId(), schemaHash);
        return ReplicaPersistInfo.createForLoad(olapTable.getId(), partition.getId(), pushIndexId, tabletId,
                                                replica.getId(), version, versionHash, dataSize, rowCount);
    }

    private void finishDropReplica(AgentTask task) {
        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.DROP, task.getSignature());
    }

    private void finishSchemaChange(AgentTask task, List<TTabletInfo> finishTabletInfos, long reportVersion)
            throws MetaNotFoundException {
        Preconditions.checkArgument(finishTabletInfos != null && !finishTabletInfos.isEmpty());
        Preconditions.checkArgument(finishTabletInfos.size() == 1);

        SchemaChangeTask schemaChangeTask = (SchemaChangeTask) task;
        SchemaChangeHandler schemaChangeHandler = Catalog.getInstance().getSchemaChangeHandler();
        schemaChangeHandler.handleFinishedReplica(schemaChangeTask, finishTabletInfos.get(0), reportVersion);
        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.SCHEMA_CHANGE, task.getSignature());
    }

    private void finishRollup(AgentTask task, List<TTabletInfo> finishTabletInfos)
            throws MetaNotFoundException {
        Preconditions.checkArgument(finishTabletInfos != null && !finishTabletInfos.isEmpty());
        Preconditions.checkArgument(finishTabletInfos.size() == 1);

        CreateRollupTask createRollupTask = (CreateRollupTask) task;
        RollupHandler rollupHandler = Catalog.getInstance().getRollupHandler();
        rollupHandler.handleFinishedReplica(createRollupTask, finishTabletInfos.get(0), -1L);
        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.ROLLUP, task.getSignature());
    }

    private void finishClone(AgentTask task, List<TTabletInfo> finishTabletInfos) {
        Preconditions.checkArgument(finishTabletInfos != null && !finishTabletInfos.isEmpty());
        Preconditions.checkArgument(finishTabletInfos.size() == 1);

        CloneTask cloneTask = (CloneTask) task;
        Catalog.getInstance().getCloneInstance().finishCloneJob(cloneTask, finishTabletInfos.get(0));
        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.CLONE, task.getSignature());
    }

    private void finishConsistenctCheck(AgentTask task, TFinishTaskRequest request) {
        CheckConsistencyTask checkConsistencyTask = (CheckConsistencyTask) task;

        if (checkConsistencyTask.getVersion() != request.getRequest_version()
                || checkConsistencyTask.getVersionHash() != request.getRequest_version_hash()) {
            LOG.warn("check consisteny task is not match. [{}-{}]",
                     checkConsistencyTask.getVersion(), request.getRequest_version());
            return;
        }

        Catalog.getInstance().getConsistencyChecker().handleFinishedConsistencyCheck(checkConsistencyTask,
                                                                                     request.getTablet_checksum());
        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.CHECK_CONSISTENCY, task.getSignature());
    }

    private void finishMakeSnapshot(AgentTask task, String snapshotPath) {
        SnapshotTask snapshotTask = (SnapshotTask) task;
        Catalog.getInstance().getBackupHandler().handleFinishedSnapshot(snapshotTask, snapshotPath);
        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.MAKE_SNAPSHOT, task.getSignature());
    }

    private void finishUpload(AgentTask task) {
        UploadTask uploadTask = (UploadTask) task;
        Catalog.getInstance().getBackupHandler().handleFinishedUpload(uploadTask);
        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.UPLOAD, task.getSignature());
    }

    private void finishRestore(AgentTask task) {
        RestoreTask restoreTask = (RestoreTask) task;
        Catalog.getInstance().getBackupHandler().handleFinishedRestore(restoreTask);
        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.RESTORE, task.getSignature());
    }

    public TMasterResult report(TReportRequest request) throws TException {
        TMasterResult result = ReportHandler.handleReport(request);
        return result;
    }

    public TFetchResourceResult fetchResource() {
        return Catalog.getInstance().getUserMgr().toResourceThrift();
    }

}
