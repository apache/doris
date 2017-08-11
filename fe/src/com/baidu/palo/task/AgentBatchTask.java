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

package com.baidu.palo.task;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.common.ClientPool;
import com.baidu.palo.system.Backend;
import com.baidu.palo.thrift.BackendService;
import com.baidu.palo.thrift.TAgentServiceVersion;
import com.baidu.palo.thrift.TAgentTaskRequest;
import com.baidu.palo.thrift.TAlterTabletReq;
import com.baidu.palo.thrift.TCancelDeleteDataReq;
import com.baidu.palo.thrift.TCheckConsistencyReq;
import com.baidu.palo.thrift.TCloneReq;
import com.baidu.palo.thrift.TCreateTabletReq;
import com.baidu.palo.thrift.TDropTabletReq;
import com.baidu.palo.thrift.TNetworkAddress;
import com.baidu.palo.thrift.TPushReq;
import com.baidu.palo.thrift.TPushType;
import com.baidu.palo.thrift.TReleaseSnapshotRequest;
import com.baidu.palo.thrift.TRestoreReq;
import com.baidu.palo.thrift.TSnapshotRequest;
import com.baidu.palo.thrift.TStorageMediumMigrateReq;
import com.baidu.palo.thrift.TTaskType;
import com.baidu.palo.thrift.TUploadReq;

/*
 * This class group tasks by backend 
 */
public class AgentBatchTask implements Runnable {
    private static final Logger LOG = LogManager.getLogger(AgentBatchTask.class);

    // backendId -> AgentTask List
    private Map<Long, List<AgentTask>> backendIdToTasks;

    public AgentBatchTask() {
        this.backendIdToTasks = new HashMap<Long, List<AgentTask>>();
    }

    public AgentBatchTask(AgentTask singleTask) {
        this();
        addTask(singleTask);
    }

    public void addTask(AgentTask agentTask) {
        if (agentTask == null) {
            return;
        }
        long backendId = agentTask.getBackendId();
        if (backendIdToTasks.containsKey(backendId)) {
            List<AgentTask> tasks = backendIdToTasks.get(backendId);
            tasks.add(agentTask);
        } else {
            List<AgentTask> tasks = new LinkedList<AgentTask>();
            tasks.add(agentTask);
            backendIdToTasks.put(backendId, tasks);
        }
    }

    public List<AgentTask> getAllTasks() {
        List<AgentTask> tasks = new LinkedList<AgentTask>();
        for (Long backendId : this.backendIdToTasks.keySet()) {
            tasks.addAll(this.backendIdToTasks.get(backendId));
        }
        return tasks;
    }
    
    public int getTaskNum() {
        int num = 0;
        for (List<AgentTask> tasks : backendIdToTasks.values()) {
            num += tasks.size();
        }
        return num;
    }

    @Override
    public void run() {
        for (Long backendId : this.backendIdToTasks.keySet()) {
            BackendService.Client client = null;
            TNetworkAddress address = null;
            boolean ok = false;
            try {
                Backend backend = Catalog.getCurrentSystemInfo().getBackend(backendId);
                if (backend == null || !backend.isAlive()) {
                    continue;
                }
                List<AgentTask> tasks = this.backendIdToTasks.get(backendId);
                // create AgentClient
                address = new TNetworkAddress(backend.getHost(), backend.getBePort());
                client = ClientPool.backendPool.borrowObject(address);

                List<TAgentTaskRequest> agentTaskRequests = new LinkedList<TAgentTaskRequest>();
                for (AgentTask task : tasks) {
                    agentTaskRequests.add(toAgentTaskRequest(task));
                }
                client.submit_tasks(agentTaskRequests);

                if (LOG.isDebugEnabled()) {
                    for (AgentTask task : tasks) {
                        LOG.debug("send task: type[{}], backend[{}], signature[{}]",
                                task.getTaskType(), backendId, task.getSignature());
                    }
                }

                ok = true;
            } catch (Exception e) {
                LOG.warn("task exec error. backend[{}]", backendId, e);
            } finally {
                if (ok) {
                    ClientPool.backendPool.returnObject(address, client);
                } else {
                    ClientPool.backendPool.invalidateObject(address, client);
                }
            }
        } // end for backend
    }

    private TAgentTaskRequest toAgentTaskRequest(AgentTask task) {
        TAgentTaskRequest tAgentTaskRequest = new TAgentTaskRequest();
        tAgentTaskRequest.setProtocol_version(TAgentServiceVersion.V1);
        tAgentTaskRequest.setSignature(task.getSignature());

        TTaskType taskType = task.getTaskType();
        tAgentTaskRequest.setTask_type(taskType);
        switch (taskType) {
            case CREATE: {
                CreateReplicaTask createReplicaTask = (CreateReplicaTask) task;
                TCreateTabletReq request = createReplicaTask.toThrift();
                LOG.debug(request.toString());
                tAgentTaskRequest.setCreate_tablet_req(request);
                return tAgentTaskRequest;
            }
            case DROP: {
                DropReplicaTask dropReplicaTask = (DropReplicaTask) task;
                TDropTabletReq request = dropReplicaTask.toThrift();
                LOG.debug(request.toString());
                tAgentTaskRequest.setDrop_tablet_req(request);
                return tAgentTaskRequest;
            }
            case PUSH: {
                PushTask pushTask = (PushTask) task;
                TPushReq request = pushTask.toThrift();
                LOG.debug(request.toString());
                tAgentTaskRequest.setPush_req(request);
                if (pushTask.getPushType() == TPushType.LOAD || pushTask.getPushType() == TPushType.LOAD_DELETE) {
                    tAgentTaskRequest.setResource_info(pushTask.getResourceInfo());
                }
                tAgentTaskRequest.setPriority(pushTask.getPriority());
                return tAgentTaskRequest;
            }
            case CLONE: {
                CloneTask cloneTask = (CloneTask) task;
                TCloneReq request = cloneTask.toThrift();
                LOG.debug(request.toString());
                tAgentTaskRequest.setClone_req(request);
                return tAgentTaskRequest;
            }
            case ROLLUP: {
                CreateRollupTask rollupTask = (CreateRollupTask) task;
                TAlterTabletReq request = rollupTask.toThrift();
                LOG.debug(request.toString());
                tAgentTaskRequest.setAlter_tablet_req(request);
                tAgentTaskRequest.setResource_info(rollupTask.getResourceInfo());
                return tAgentTaskRequest;
            }
            case SCHEMA_CHANGE: {
                SchemaChangeTask schemaChangeTask = (SchemaChangeTask) task;
                TAlterTabletReq request = schemaChangeTask.toThrift();
                LOG.debug(request.toString());
                tAgentTaskRequest.setAlter_tablet_req(request);
                tAgentTaskRequest.setResource_info(schemaChangeTask.getResourceInfo());
                return tAgentTaskRequest;
            }
            case CANCEL_DELETE: {
                CancelDeleteTask cancelDeleteTask = (CancelDeleteTask) task;
                TCancelDeleteDataReq request = cancelDeleteTask.toThrift();
                LOG.debug(request.toString());
                tAgentTaskRequest.setCancel_delete_data_req(request);
                return tAgentTaskRequest;
            }
            case STORAGE_MEDIUM_MIGRATE: {
                StorageMediaMigrationTask migrationTask = (StorageMediaMigrationTask) task;
                TStorageMediumMigrateReq request = migrationTask.toThrift();
                LOG.debug(request.toString());
                tAgentTaskRequest.setStorage_medium_migrate_req(request);
                return tAgentTaskRequest;
            }
            case CHECK_CONSISTENCY: {
                CheckConsistencyTask checkConsistencyTask = (CheckConsistencyTask) task;
                TCheckConsistencyReq request = checkConsistencyTask.toThrift();
                LOG.debug(request.toString());
                tAgentTaskRequest.setCheck_consistency_req(request);
                return tAgentTaskRequest;
            }
            case MAKE_SNAPSHOT: {
                SnapshotTask snapshotTask = (SnapshotTask) task;
                TSnapshotRequest request = snapshotTask.toThrift();
                LOG.debug(request.toString());
                tAgentTaskRequest.setSnapshot_req(request);
                return tAgentTaskRequest;
            }
            case RELEASE_SNAPSHOT: {
                ReleaseSnapshotTask releaseSnapshotTask = (ReleaseSnapshotTask) task;
                TReleaseSnapshotRequest request = releaseSnapshotTask.toThrift();
                LOG.debug(request.toString());
                tAgentTaskRequest.setRelease_snapshot_req(request);
                return tAgentTaskRequest;
            }
            case UPLOAD: {
                UploadTask uploadTask = (UploadTask) task;
                TUploadReq request = uploadTask.toThrift();
                LOG.debug(request.toString());
                tAgentTaskRequest.setUpload_req(request);
                return tAgentTaskRequest;
            }
            case RESTORE: {
                RestoreTask restoreTask = (RestoreTask) task;
                TRestoreReq request = restoreTask.toThrift();
                LOG.debug(request.toString());
                tAgentTaskRequest.setRestore_req(request);
                return tAgentTaskRequest;
            }
            default:
                return null;
        }
    }
}
