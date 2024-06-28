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

package org.apache.doris.task;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.FeConstants;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TAgentServiceVersion;
import org.apache.doris.thrift.TAgentTaskRequest;
import org.apache.doris.thrift.TAlterInvertedIndexReq;
import org.apache.doris.thrift.TAlterTabletReqV2;
import org.apache.doris.thrift.TCalcDeleteBitmapRequest;
import org.apache.doris.thrift.TCheckConsistencyReq;
import org.apache.doris.thrift.TCleanTrashReq;
import org.apache.doris.thrift.TCleanUDFCacheReq;
import org.apache.doris.thrift.TClearAlterTaskRequest;
import org.apache.doris.thrift.TClearTransactionTaskRequest;
import org.apache.doris.thrift.TCloneReq;
import org.apache.doris.thrift.TCompactionReq;
import org.apache.doris.thrift.TCreateTabletReq;
import org.apache.doris.thrift.TDownloadReq;
import org.apache.doris.thrift.TDropTabletReq;
import org.apache.doris.thrift.TGcBinlogReq;
import org.apache.doris.thrift.TMoveDirReq;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPublishVersionRequest;
import org.apache.doris.thrift.TPushCooldownConfReq;
import org.apache.doris.thrift.TPushReq;
import org.apache.doris.thrift.TPushStoragePolicyReq;
import org.apache.doris.thrift.TReleaseSnapshotRequest;
import org.apache.doris.thrift.TSnapshotRequest;
import org.apache.doris.thrift.TStorageMediumMigrateReq;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.thrift.TUpdateTabletMetaInfoReq;
import org.apache.doris.thrift.TUploadReq;
import org.apache.doris.thrift.TVisibleVersionReq;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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

    // return true only if all tasks are finished.
    // NOTICE that even if AgentTask.isFinished() return false, it does not mean that task is not finished.
    // this depends on caller's logic. See comments on 'isFinished' member.
    public boolean isFinished() {
        for (List<AgentTask> tasks : this.backendIdToTasks.values()) {
            for (AgentTask agentTask : tasks) {
                if (!agentTask.isFinished()) {
                    return false;
                }
            }
        }
        return true;
    }

    // return the limit number of unfinished tasks.
    public List<AgentTask> getUnfinishedTasks(int limit) {
        List<AgentTask> res = Lists.newArrayList();
        for (List<AgentTask> tasks : this.backendIdToTasks.values()) {
            for (AgentTask agentTask : tasks) {
                if (!agentTask.isFinished()) {
                    if (res.size() < limit) {
                        res.add(agentTask);
                    }
                }
            }
        }
        return res;
    }

    public int getFinishedTaskNum() {
        int count = 0;
        for (List<AgentTask> tasks : this.backendIdToTasks.values()) {
            for (AgentTask agentTask : tasks) {
                if (agentTask.isFinished()) {
                    count++;
                }
            }
        }
        return count;
    }

    @Override
    public void run() {
        for (Long backendId : this.backendIdToTasks.keySet()) {
            BackendService.Client client = null;
            TNetworkAddress address = null;
            boolean ok = false;
            try {
                Backend backend = Env.getCurrentSystemInfo().getBackend(backendId);
                if (backend == null || !backend.isAlive()) {
                    continue;
                }
                List<AgentTask> tasks = this.backendIdToTasks.get(backendId);
                // create AgentClient
                String host = FeConstants.runningUnitTest ? "127.0.0.1" : backend.getHost();
                address = new TNetworkAddress(host, backend.getBePort());
                client = ClientPool.backendPool.borrowObject(address);
                List<TAgentTaskRequest> agentTaskRequests = new LinkedList<TAgentTaskRequest>();
                for (AgentTask task : tasks) {
                    try {
                        agentTaskRequests.add(toAgentTaskRequest(task));
                    } catch (Exception e) {
                        task.failed();
                        throw e;
                    }
                }
                client.submitTasks(agentTaskRequests);
                if (LOG.isDebugEnabled()) {
                    for (AgentTask task : tasks) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("send task: type[{}], backend[{}], signature[{}]",
                                    task.getTaskType(), backendId, task.getSignature());
                        }
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
        tAgentTaskRequest.setProtocolVersion(TAgentServiceVersion.V1);
        tAgentTaskRequest.setSignature(task.getSignature());

        TTaskType taskType = task.getTaskType();
        tAgentTaskRequest.setTaskType(taskType);
        switch (taskType) {
            case CREATE: {
                CreateReplicaTask createReplicaTask = (CreateReplicaTask) task;
                TCreateTabletReq request = createReplicaTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setCreateTabletReq(request);
                return tAgentTaskRequest;
            }
            case DROP: {
                DropReplicaTask dropReplicaTask = (DropReplicaTask) task;
                TDropTabletReq request = dropReplicaTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setDropTabletReq(request);
                return tAgentTaskRequest;
            }
            case REALTIME_PUSH: {
                PushTask pushTask = (PushTask) task;
                TPushReq request = pushTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setPushReq(request);
                tAgentTaskRequest.setPriority(pushTask.getPriority());
                return tAgentTaskRequest;
            }
            case CLONE: {
                CloneTask cloneTask = (CloneTask) task;
                TCloneReq request = cloneTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setCloneReq(request);
                return tAgentTaskRequest;
            }
            case STORAGE_MEDIUM_MIGRATE: {
                StorageMediaMigrationTask migrationTask = (StorageMediaMigrationTask) task;
                TStorageMediumMigrateReq request = migrationTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setStorageMediumMigrateReq(request);
                return tAgentTaskRequest;
            }
            case CHECK_CONSISTENCY: {
                CheckConsistencyTask checkConsistencyTask = (CheckConsistencyTask) task;
                TCheckConsistencyReq request = checkConsistencyTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setCheckConsistencyReq(request);
                return tAgentTaskRequest;
            }
            case MAKE_SNAPSHOT: {
                SnapshotTask snapshotTask = (SnapshotTask) task;
                TSnapshotRequest request = snapshotTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setSnapshotReq(request);
                return tAgentTaskRequest;
            }
            case RELEASE_SNAPSHOT: {
                ReleaseSnapshotTask releaseSnapshotTask = (ReleaseSnapshotTask) task;
                TReleaseSnapshotRequest request = releaseSnapshotTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setReleaseSnapshotReq(request);
                return tAgentTaskRequest;
            }
            case UPLOAD: {
                UploadTask uploadTask = (UploadTask) task;
                TUploadReq request = uploadTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setUploadReq(request);
                return tAgentTaskRequest;
            }
            case DOWNLOAD: {
                DownloadTask downloadTask = (DownloadTask) task;
                TDownloadReq request = downloadTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setDownloadReq(request);
                return tAgentTaskRequest;
            }
            case PUBLISH_VERSION: {
                PublishVersionTask publishVersionTask = (PublishVersionTask) task;
                TPublishVersionRequest request = publishVersionTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setPublishVersionReq(request);
                return tAgentTaskRequest;
            }
            case CLEAR_ALTER_TASK: {
                ClearAlterTask clearAlterTask = (ClearAlterTask) task;
                TClearAlterTaskRequest request = clearAlterTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setClearAlterTaskReq(request);
                return tAgentTaskRequest;
            }
            case CLEAR_TRANSACTION_TASK: {
                ClearTransactionTask clearTransactionTask = (ClearTransactionTask) task;
                TClearTransactionTaskRequest request = clearTransactionTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setClearTransactionTaskReq(request);
                return tAgentTaskRequest;
            }
            case MOVE: {
                DirMoveTask dirMoveTask = (DirMoveTask) task;
                TMoveDirReq request = dirMoveTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setMoveDirReq(request);
                return tAgentTaskRequest;
            }
            case UPDATE_TABLET_META_INFO: {
                UpdateTabletMetaInfoTask updateTabletMetaInfoTask = (UpdateTabletMetaInfoTask) task;
                TUpdateTabletMetaInfoReq request = updateTabletMetaInfoTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setUpdateTabletMetaInfoReq(request);
                return tAgentTaskRequest;
            }
            case ALTER: {
                AlterReplicaTask createRollupTask = (AlterReplicaTask) task;
                TAlterTabletReqV2 request = createRollupTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setAlterTabletReqV2(request);
                return tAgentTaskRequest;
            }
            case ALTER_INVERTED_INDEX: {
                AlterInvertedIndexTask alterInvertedIndexTask = (AlterInvertedIndexTask) task;
                TAlterInvertedIndexReq request = alterInvertedIndexTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setAlterInvertedIndexReq(request);
                return tAgentTaskRequest;
            }
            case COMPACTION: {
                CompactionTask compactionTask = (CompactionTask) task;
                TCompactionReq request = compactionTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setCompactionReq(request);
                return tAgentTaskRequest;
            }
            case PUSH_STORAGE_POLICY: {
                PushStoragePolicyTask pushStoragePolicyTask = (PushStoragePolicyTask) task;
                TPushStoragePolicyReq request = pushStoragePolicyTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setPushStoragePolicyReq(request);
                return tAgentTaskRequest;
            }
            case PUSH_COOLDOWN_CONF: {
                PushCooldownConfTask pushCooldownConfTask = (PushCooldownConfTask) task;
                TPushCooldownConfReq request = pushCooldownConfTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setPushCooldownConf(request);
                return tAgentTaskRequest;
            }
            case GC_BINLOG: {
                BinlogGcTask binlogGcTask = (BinlogGcTask) task;
                TGcBinlogReq request = binlogGcTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setGcBinlogReq(request);
                return tAgentTaskRequest;
            }
            case UPDATE_VISIBLE_VERSION: {
                UpdateVisibleVersionTask visibleTask = (UpdateVisibleVersionTask) task;
                TVisibleVersionReq request = visibleTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setVisibleVersionReq(request);
                return tAgentTaskRequest;
            }
            case CALCULATE_DELETE_BITMAP: {
                CalcDeleteBitmapTask calcDeleteBitmapTask = (CalcDeleteBitmapTask) task;
                TCalcDeleteBitmapRequest request = calcDeleteBitmapTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setCalcDeleteBitmapReq(request);
                return tAgentTaskRequest;
            }
            case CLEAN_TRASH: {
                CleanTrashTask cleanTrashTask = (CleanTrashTask) task;
                TCleanTrashReq request = cleanTrashTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setCleanTrashReq(request);
                return tAgentTaskRequest;
            }
            case CLEAN_UDF_CACHE: {
                CleanUDFCacheTask cleanUDFCacheTask = (CleanUDFCacheTask) task;
                TCleanUDFCacheReq request = cleanUDFCacheTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setCleanUdfCacheReq(request);
                return tAgentTaskRequest;
            }
            default:
                if (LOG.isDebugEnabled()) {
                    LOG.debug("could not find task type for task [{}]", task);
                }
                return null;
        }
    }
}
