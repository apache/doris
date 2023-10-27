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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.PublishVersionTask;
import org.apache.doris.thrift.TPartitionVersionInfo;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

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

    private void publishVersion() {
        if (DebugPointUtil.isEnable("PublishVersionDaemon.stop_publish")) {
            return;
        }
        GlobalTransactionMgr globalTransactionMgr = Env.getCurrentGlobalTransactionMgr();
        List<TransactionState> readyTransactionStates = globalTransactionMgr.getReadyToPublishTransactions();
        if (readyTransactionStates.isEmpty()) {
            return;
        }

        // ATTN, we publish transaction state to all backends including dead backend, if not publish to dead backend
        // then transaction manager will treat it as success
        SystemInfoService infoService = Env.getCurrentSystemInfo();
        List<Long> allBackends = infoService.getAllBackendIds(false);
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
                        commitInfo.getVersion(), 0);
                partitionVersionInfos.add(versionInfo);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("try to publish version info partitionid [{}], version [{}]",
                            commitInfo.getPartitionId(),
                            commitInfo.getVersion());
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
            LOG.info("send publish tasks for transaction: {}, db: {}", transactionState.getTransactionId(),
                    transactionState.getDbId());
        }
        if (!batchTask.getAllTasks().isEmpty()) {
            AgentTaskExecutor.submit(batchTask);
        }

        Map<Long, Long> tableIdToNumDeltaRows = Maps.newHashMap();
        // try to finish the transaction, if failed just retry in next loop
        for (TransactionState transactionState : readyTransactionStates) {
            Stream<PublishVersionTask> publishVersionTaskStream = transactionState
                    .getPublishVersionTasks()
                    .values()
                    .stream()
                    .peek(task -> {
                        if (task.isFinished() && CollectionUtils.isEmpty(task.getErrorTablets())) {
                            Map<Long, Long> tableIdToDeltaNumRows =
                                    task.getTableIdToDeltaNumRows();
                            tableIdToDeltaNumRows.forEach((tableId, numRows) -> {
                                tableIdToDeltaNumRows
                                        .computeIfPresent(tableId, (id, orgNumRows) -> orgNumRows + numRows);
                                tableIdToNumDeltaRows.putIfAbsent(tableId, numRows);
                            });
                        }
                    });
            boolean hasBackendAliveAndUnfinishedTask = publishVersionTaskStream
                    .anyMatch(task -> !task.isFinished() && infoService.checkBackendAlive(task.getBackendId()));
            transactionState.setTableIdToTotalNumDeltaRows(tableIdToNumDeltaRows);

            boolean shouldFinishTxn = !hasBackendAliveAndUnfinishedTask || transactionState.isPublishTimeout()
                    || DebugPointUtil.isEnable("PublishVersionDaemon.not_wait_unfinished_tasks");
            if (shouldFinishTxn) {
                try {
                    // one transaction exception should not affect other transaction
                    globalTransactionMgr.finishTransaction(transactionState.getDbId(),
                            transactionState.getTransactionId());
                } catch (Exception e) {
                    LOG.warn("error happens when finish transaction {}", transactionState.getTransactionId(), e);
                }
                if (transactionState.getTransactionStatus() != TransactionStatus.VISIBLE) {
                    // if finish transaction state failed, then update publish version time, should check
                    // to finish after some interval
                    transactionState.updateSendTaskTime();
                    LOG.debug("publish version for transaction {} failed", transactionState);
                }
            }

            if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
                for (PublishVersionTask task : transactionState.getPublishVersionTasks().values()) {
                    AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.PUBLISH_VERSION, task.getSignature());
                }
                if (MetricRepo.isInit) {
                    long publishTime = transactionState.getPublishVersionTime() - transactionState.getCommitTime();
                    MetricRepo.HISTO_TXN_PUBLISH_LATENCY.update(publishTime);
                }
            }
        } // end for readyTransactionStates
    }
}
