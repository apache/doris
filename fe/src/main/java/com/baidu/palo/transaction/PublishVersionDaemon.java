// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.transaction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Replica;
import com.baidu.palo.catalog.Replica.ReplicaState;
import com.baidu.palo.catalog.TabletInvertedIndex;
import com.baidu.palo.common.Config;
import com.baidu.palo.common.util.Daemon;
import com.baidu.palo.task.AgentBatchTask;
import com.baidu.palo.task.AgentTaskExecutor;
import com.baidu.palo.task.AgentTaskQueue;
import com.baidu.palo.task.PublishVersionTask;
import com.baidu.palo.thrift.TPartitionVersionInfo;
import com.baidu.palo.thrift.TTaskType;
import com.google.common.collect.Sets;

public class PublishVersionDaemon extends Daemon {
    
    private static final Logger LOG = LogManager.getLogger(PublishVersionDaemon.class);
    
    public PublishVersionDaemon() {
        super("PUBLISH_VERSION");
        setInterval(Config.publish_version_interval_millis);
    }
    
    protected void runOneCycle() {
        try {
            publishVersion();
        } catch (Throwable t) {
            LOG.error("errors while publish version to all backends, {}", t);
        }
    }
    
    private void publishVersion() {
        GlobalTransactionMgr globalTransactionMgr = Catalog.getCurrentGlobalTransactionMgr();
        List<TransactionState> readyTransactionStates = globalTransactionMgr.getReadyToPublishTransactions();
        if (readyTransactionStates == null || readyTransactionStates.isEmpty()) {
            return;
        }
        // TODO yiguolei: could publish transaction state according to multi-tenant cluster info
        // but should do more work. for example, if a table is migrate from one cluster to another cluster
        // should pulish to two clusters. 
        // attention here, we publish transaction state to all backends including dead backend, if not publish to dead backend
        // then transaction manager will treat it as success
        List<Long> allBackends = Catalog.getCurrentSystemInfo().getBackendIds(false);
        if (allBackends == null || allBackends.size() == 0) {
            LOG.warn("some transaction state need to publish, but no alive backends!!!");
            return;
        }
        // every backend-transaction identified a single task
        AgentBatchTask batchTask = new AgentBatchTask();
        // traverse all ready transactions and dispatch the publish version task to all backends
        for (TransactionState transactionState : readyTransactionStates) {
            if (transactionState.hasSendTask()) {
                continue;
            }
            List<PartitionCommitInfo> partitionCommitInfos = new ArrayList<>();
            for (TableCommitInfo tableCommitInfo : transactionState.getIdToTableCommitInfos().values()) {
                partitionCommitInfos.addAll(tableCommitInfo.getIdToPartitionCommitInfo().values());
            }
            List<TPartitionVersionInfo> partitionVersionInfos = new ArrayList<>(partitionCommitInfos.size());
            for (PartitionCommitInfo commitInfo : partitionCommitInfos) {
                TPartitionVersionInfo versionInfo = new TPartitionVersionInfo(commitInfo.getPartitionId(), 
                        commitInfo.getVersion(), 
                        commitInfo.getVersionHash());
                partitionVersionInfos.add(versionInfo);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("try to publish version info partitionid [{}], version [{}], version hash [{}]", 
                            commitInfo.getPartitionId(), 
                            commitInfo.getVersion(), 
                            commitInfo.getVersionHash());
                }
            }
            Set<Long> publishBackends = transactionState.getPublishVersionTasks().keySet();
            if (publishBackends.isEmpty()) {
                // could not just add to it, should new a new object, or the back map will destroyed
                publishBackends = Sets.newHashSet();
                // this is useful if fe master transfer to another master, because publish version task is not
                // persistent to edit log, then it should publish to all backends
                publishBackends.addAll(allBackends);
            }
            for (long backendId : publishBackends) {
                PublishVersionTask task = new PublishVersionTask(backendId, 
                                                                 transactionState.getTransactionId(), 
                                                                 partitionVersionInfos);
                // add to AgentTaskQueue for handling finish report.
                // not check return value, because the add will success
                AgentTaskQueue.addTask(task);
                batchTask.addTask(task);
                transactionState.addPublishVersionTask(backendId, task);
            }
            transactionState.setHasSendTask(true);
        }
        if (!batchTask.getAllTasks().isEmpty()) {
            AgentTaskExecutor.submit(batchTask);
        }
        
        TabletInvertedIndex tabletInvertedIndex = Catalog.getCurrentInvertedIndex();
        // try to finish the transaction, if failed just retry in next loop
        for (TransactionState transactionState : readyTransactionStates) {
            Map<Long, PublishVersionTask> transTasks = transactionState.getPublishVersionTasks();
            Set<Replica> transErrorReplicas = Sets.newHashSet();
            for (PublishVersionTask publishVersionTask : transTasks.values()) {
                if (publishVersionTask.isFinished()) {
                    // sometimes backend finish publish version task, but it maybe failed to change transactionid to version for some tablets
                    // and it will upload the failed tabletinfo to fe and fe will deal with them
                    List<Long> errorTablets = publishVersionTask.getErrorTablets();
                    if (errorTablets == null || errorTablets.size() == 0) {
                        continue;
                    } else {
                        for (long tabletId : errorTablets) {
                            // tablet inverted index also contains rollingup index
                            Replica replica = tabletInvertedIndex.getReplica(tabletId, publishVersionTask.getBackendId());
                            transErrorReplicas.add(replica);
                        }
                    }
                } else {
                    // if task is not finished in time, then set all replica in the backend to error state
                    List<TPartitionVersionInfo> versionInfos = publishVersionTask.getPartitionVersionInfos();
                    Set<Long> errorPartitionIds = Sets.newHashSet();
                    for (TPartitionVersionInfo versionInfo : versionInfos) {
                        errorPartitionIds.add(versionInfo.getPartition_id());
                    }
                    if (errorPartitionIds.isEmpty()) {
                        continue;
                    }
                    List<Long> tabletIds = tabletInvertedIndex.getTabletIdsByBackendId(publishVersionTask.getBackendId());
                    for (long tabletId : tabletIds) {
                        long partitionId = tabletInvertedIndex.getPartitionId(tabletId);
                        if (errorPartitionIds.contains(partitionId)) {
                            Replica replica = tabletInvertedIndex.getReplica(tabletId, publishVersionTask.getBackendId());
                            transErrorReplicas.add(replica);
                        }
                    }
                }
            }
            // the timeout value is related with backend num
            long timeoutMillis = Math.min(Config.publish_version_timeout_second * transTasks.size() * 1000, 10000);
            // the minimal internal should be 3s
            timeoutMillis = Math.max(timeoutMillis, 3000);
            
            // should not wait clone replica or replica's that with last failed version > 0
            // if wait for them, the publish process will be very slow
            int normalReplicasNotRespond = 0;
            Set<Long> allErrorReplicas = Sets.newHashSet();
            for (Replica replica : transErrorReplicas) {
                allErrorReplicas.add(replica.getId());
                if (replica.getState() != ReplicaState.CLONE 
                        && replica.getLastFailedVersion() < 1) {
                    ++ normalReplicasNotRespond;
                }
            }
            if (normalReplicasNotRespond == 0 
                    || System.currentTimeMillis() - transactionState.getPublishVersionTime() > timeoutMillis) {
                LOG.debug("transTask num {}, error replica id num {}", transTasks.size(), transErrorReplicas.size());
                globalTransactionMgr.finishTransaction(transactionState.getTransactionId(), allErrorReplicas);
                if (transactionState.getTransactionStatus() != TransactionStatus.VISIBLE) {
                    // if finish transaction state failed, then update publish version time, should check 
                    // to finish after some interval
                    transactionState.updateSendTaskTime();
                    LOG.debug("publish version for transation {} failed, has {} error replicas during publish", 
                            transactionState, transErrorReplicas.size());
                }
            }
            if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
                for (PublishVersionTask task : transactionState.getPublishVersionTasks().values()) {
                    AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.PUBLISH_VERSION, task.getSignature());
                }
            }
        }
    }
}
