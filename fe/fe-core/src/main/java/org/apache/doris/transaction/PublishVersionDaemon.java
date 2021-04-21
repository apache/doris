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

package org.apache.doris.transaction;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.PublishVersionTask;
import org.apache.doris.thrift.TPartitionVersionInfo;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PublishVersionDaemon extends MasterDaemon {
    
    private static final Logger LOG = LogManager.getLogger(PublishVersionDaemon.class);
    
    public PublishVersionDaemon() {
        super("PUBLISH_VERSION", Config.publish_version_interval_ms);
    }
    
    @Override
    protected void runAfterCatalogReady() {
        try {
            publishVersion();
        } catch (Throwable t) {
            LOG.error("errors while publish version to all backends", t);
        }
    }
    
    private void publishVersion() throws UserException {
        GlobalTransactionMgr globalTransactionMgr = Catalog.getCurrentGlobalTransactionMgr();
        List<TransactionState> readyTransactionStates = globalTransactionMgr.getReadyToPublishTransactions();
        if (readyTransactionStates == null || readyTransactionStates.isEmpty()) {
            return;
        }

        // TODO yiguolei: could publish transaction state according to multi-tenant cluster info
        // but should do more work. for example, if a table is migrate from one cluster to another cluster
        // should publish to two clusters.
        // attention here, we publish transaction state to all backends including dead backend, if not publish to dead backend
        // then transaction manager will treat it as success
        List<Long> allBackends = Catalog.getCurrentSystemInfo().getBackendIds(false);
        if (allBackends.isEmpty()) {
            LOG.warn("some transaction state need to publish, but no backend exists");
            return;
        }
        long createPublishVersionTaskTime = System.currentTimeMillis();
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
            // public version tasks are not persisted in catalog, so publishBackends may be empty.
            // so we have to try publish to all backends;
            if (publishBackends.isEmpty()) {
                // could not just add to it, should new a new object, or the back map will destroyed
                publishBackends = Sets.newHashSet();
                publishBackends.addAll(allBackends);
            }

            for (long backendId : publishBackends) {
                PublishVersionTask task = new PublishVersionTask(backendId,
                        transactionState.getTransactionId(),
                        transactionState.getDbId(),
                        partitionVersionInfos,
                        createPublishVersionTaskTime);
                // add to AgentTaskQueue for handling finish report.
                // not check return value, because the add will success
                AgentTaskQueue.addTask(task);
                batchTask.addTask(task);
                transactionState.addPublishVersionTask(backendId, task);
            }
            transactionState.setHasSendTask(true);
            LOG.info("send publish tasks for transaction: {}", transactionState.getTransactionId());
        }
        if (!batchTask.getAllTasks().isEmpty()) {
            AgentTaskExecutor.submit(batchTask);
        }
        
        TabletInvertedIndex tabletInvertedIndex = Catalog.getCurrentInvertedIndex();
        // try to finish the transaction, if failed just retry in next loop
        for (TransactionState transactionState : readyTransactionStates) {
            Map<Long, PublishVersionTask> transTasks = transactionState.getPublishVersionTasks();
            Set<Long> publishErrorReplicaIds = Sets.newHashSet();
            List<PublishVersionTask> unfinishedTasks = Lists.newArrayList();
            for (PublishVersionTask publishVersionTask : transTasks.values()) {
                if (publishVersionTask.isFinished()) {
                    // sometimes backend finish publish version task, but it maybe failed to change transactionid to version for some tablets
                    // and it will upload the failed tabletinfo to fe and fe will deal with them
                    List<Long> errorTablets = publishVersionTask.getErrorTablets();
                    if (errorTablets == null || errorTablets.isEmpty()) {
                        continue;
                    } else {
                        for (long tabletId : errorTablets) {
                            // tablet inverted index also contains rollingup index
                            // if tablet meta not contains the tablet, skip this tablet because this tablet is dropped
                            // from fe
                            if (tabletInvertedIndex.getTabletMeta(tabletId) == null) {
                                continue;
                            }
                            Replica replica = tabletInvertedIndex.getReplica(tabletId, publishVersionTask.getBackendId());
                            if (replica != null) {
                                publishErrorReplicaIds.add(replica.getId());
                            } else {
                                LOG.info("could not find related replica with tabletid={}, backendid={}", 
                                        tabletId, publishVersionTask.getBackendId());
                            }
                        }
                    }
                } else {
                    unfinishedTasks.add(publishVersionTask);
                }
            }

            boolean shouldFinishTxn = false;
            if (!unfinishedTasks.isEmpty()) {
                if (transactionState.isPublishTimeout()) {
                    // transaction's publish is timeout, but there still has unfinished tasks.
                    // we need to collect all error replicas, and try to finish this txn.
                    for (PublishVersionTask unfinishedTask : unfinishedTasks) {
                        // set all replicas in the backend to error state
                        List<TPartitionVersionInfo> versionInfos = unfinishedTask.getPartitionVersionInfos();
                        Set<Long> errorPartitionIds = Sets.newHashSet();
                        for (TPartitionVersionInfo versionInfo : versionInfos) {
                            errorPartitionIds.add(versionInfo.getPartitionId());
                        }
                        if (errorPartitionIds.isEmpty()) {
                            continue;
                        }

                        Database db = Catalog.getCurrentCatalog().getDb(transactionState.getDbId());
                        if (db == null) {
                            LOG.warn("Database [{}] has been dropped.", transactionState.getDbId());
                            continue;
                        }


                        for (int i = 0; i < transactionState.getTableIdList().size(); i++) {
                            long tableId = transactionState.getTableIdList().get(i);
                            Table table = db.getTable(tableId);
                            if (table == null || table.getType() != Table.TableType.OLAP) {
                                LOG.warn("Table [{}] in database [{}] has been dropped.", tableId, db.getFullName());
                                continue;
                            }
                            OlapTable olapTable = (OlapTable) table;
                            olapTable.readLock();
                            try {
                                for (Long errorPartitionId : errorPartitionIds) {
                                    Partition partition = olapTable.getPartition(errorPartitionId);
                                    if (partition != null) {
                                        List<MaterializedIndex> materializedIndexList = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
                                        for (MaterializedIndex materializedIndex : materializedIndexList) {
                                            for (Tablet tablet : materializedIndex.getTablets()) {
                                                Replica replica = tablet.getReplicaByBackendId(unfinishedTask.getBackendId());
                                                if (replica != null) {
                                                    publishErrorReplicaIds.add(replica.getId());
                                                }
                                            }
                                        }
                                    }
                                }
                            } finally {
                                olapTable.readUnlock();
                            }
                        }
                    }
                    shouldFinishTxn = true;
                }
                // transaction's publish is not timeout, waiting next round.
            } else {
                // all publish tasks are finished, try to finish this txn.
                shouldFinishTxn = true;
            }
            
            if (shouldFinishTxn) {
                try {
                    // one transaction exception should not affect other transaction
                    globalTransactionMgr.finishTransaction(transactionState.getDbId(), transactionState.getTransactionId(), publishErrorReplicaIds);
                } catch (Exception e) {
                    LOG.warn("error happends when finish transaction {} ", transactionState.getTransactionId(), e);
                }
                if (transactionState.getTransactionStatus() != TransactionStatus.VISIBLE) {
                    // if finish transaction state failed, then update publish version time, should check 
                    // to finish after some interval
                    transactionState.updateSendTaskTime();
                    LOG.debug("publish version for transaction {} failed, has {} error replicas during publish",
                            transactionState, publishErrorReplicaIds.size());
                }
            }

            if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
                for (PublishVersionTask task : transactionState.getPublishVersionTasks().values()) {
                    AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.PUBLISH_VERSION, task.getSignature());
                }
            }
        } // end for readyTransactionStates
    }
}
