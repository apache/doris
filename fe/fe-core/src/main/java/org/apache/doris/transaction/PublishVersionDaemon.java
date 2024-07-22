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
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.Config;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.PublishVersionTask;
import org.apache.doris.task.UpdateVisibleVersionTask;
import org.apache.doris.thrift.TPartitionVersionInfo;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class PublishVersionDaemon extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(PublishVersionDaemon.class);

    public PublishVersionDaemon() {
        super("PUBLISH_VERSION", Config.publish_version_interval_ms);
    }

    @Override
    protected void runAfterCatalogReady() {
        Map<Long, Long> partitionVisibleVersions = Maps.newHashMap();
        Map<Long, Set<Long>> backendPartitions = Maps.newHashMap();

        try {
            publishVersion(partitionVisibleVersions, backendPartitions);
            sendBackendVisibleVersion(partitionVisibleVersions, backendPartitions);
        } catch (Throwable t) {
            LOG.error("errors while publish version to all backends", t);
        }
    }

    private void publishVersion(Map<Long, Long> partitionVisibleVersions, Map<Long, Set<Long>> backendPartitions) {
        if (DebugPointUtil.isEnable("PublishVersionDaemon.stop_publish")) {
            return;
        }
        GlobalTransactionMgrIface globalTransactionMgr = Env.getCurrentGlobalTransactionMgr();
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
        traverseReadyTxnAndDispatchPublishVersionTask(readyTransactionStates, allBackends);
        tryFinishTxn(readyTransactionStates, infoService, globalTransactionMgr,
                partitionVisibleVersions, backendPartitions);
    }

    private void traverseReadyTxnAndDispatchPublishVersionTask(List<TransactionState> readyTransactionStates,
                                                               List<Long> allBackends) {
        long createPublishVersionTaskTime = System.currentTimeMillis();
        // every backend-transaction identified a single task
        AgentBatchTask batchTask = new AgentBatchTask();
        // for delta rows statistics to exclude rollup tablets
        Map<Long, Set<Long>> beIdToBaseTabletIds = Maps.newHashMap();
        // traverse all ready transactions and dispatch the publish version task to all backends
        for (TransactionState transactionState : readyTransactionStates) {
            if (transactionState.hasSendTask()) {
                continue;
            }
            try {
                genPublishTask(allBackends, transactionState, createPublishVersionTaskTime, beIdToBaseTabletIds,
                        batchTask);
            } catch (Throwable t) {
                LOG.error("errors while generate publish task for transaction: {}", transactionState, t);
            }
        }
        if (!batchTask.getAllTasks().isEmpty()) {
            AgentTaskExecutor.submit(batchTask);
        }
    }

    private void genPublishTask(List<Long> allBackends, TransactionState transactionState,
            long createPublishVersionTaskTime, Map<Long, Set<Long>> beIdToBaseTabletIds, AgentBatchTask batchTask) {
        Set<Long> publishBackends = Sets.newHashSet(transactionState.getPublishVersionTasks().keySet());
        publishBackends.addAll(transactionState.getInvolvedBackends());
        // public version tasks are not persisted in catalog, so publishBackends may be empty.
        // so we have to try publish to all backends;
        if (publishBackends.isEmpty()) {
            // could not just add to it, should new a new object, or the back map will destroyed
            publishBackends = Sets.newHashSet();
            publishBackends.addAll(allBackends);
        }
        if (transactionState.getTransactionId() == DebugPointUtil.getDebugParamOrDefault(
                "PublishVersionDaemon.genPublishTask.failed", "txnId", -1L)) {
            throw new NullPointerException("genPublishTask failed for txnId: " + transactionState.getTransactionId());
        }

        if (transactionState.getSubTxnIds() != null) {
            for (Entry<Long, TableCommitInfo> entry : transactionState.getSubTxnIdToTableCommitInfo().entrySet()) {
                long subTxnId = entry.getKey();
                List<TPartitionVersionInfo> partitionVersionInfos = generatePartitionVersionInfos(entry.getValue(),
                        transactionState, beIdToBaseTabletIds);
                LOG.debug("add publish task, txnId={}, subTxnId={}, backends={}, partitionVersionInfos={}",
                        transactionState.getTransactionId(), subTxnId, publishBackends, partitionVersionInfos);
                addPublishVersionTask(publishBackends, subTxnId, transactionState, partitionVersionInfos,
                        beIdToBaseTabletIds, createPublishVersionTaskTime, batchTask);
            }
        } else {
            List<TPartitionVersionInfo> partitionVersionInfos = generatePartitionVersionInfos(
                    transactionState.getIdToTableCommitInfos().values(), transactionState, beIdToBaseTabletIds);
            addPublishVersionTask(publishBackends, transactionState.getTransactionId(), transactionState,
                    partitionVersionInfos, beIdToBaseTabletIds, createPublishVersionTaskTime, batchTask);
        }
        transactionState.setSendedTask();
        LOG.info("send publish tasks for transaction: {}, db: {}", transactionState.getTransactionId(),
                transactionState.getDbId());
    }

    private void tryFinishTxn(List<TransactionState> readyTransactionStates,
                                     SystemInfoService infoService, GlobalTransactionMgrIface globalTransactionMgr,
                                     Map<Long, Long> partitionVisibleVersions, Map<Long, Set<Long>> backendPartitions) {
        for (TransactionState transactionState : readyTransactionStates) {
            try {
                // try to finish the transaction, if failed just retry in next loop
                tryFinishOneTxn(transactionState, infoService, globalTransactionMgr, partitionVisibleVersions,
                        backendPartitions);
            } catch (Throwable t) {
                LOG.error("errors while finish transaction: {}, publish tasks: {}", transactionState,
                        transactionState.getPublishVersionTasks(), t);
            }
        } // end for readyTransactionStates
    }

    private void tryFinishOneTxn(TransactionState transactionState, SystemInfoService infoService,
            GlobalTransactionMgrIface globalTransactionMgr,
            Map<Long, Long> partitionVisibleVersions, Map<Long, Set<Long>> backendPartitions) {
        Map<Long, Map<Long, Long>> tableIdToTabletDeltaRows = Maps.newHashMap();
        AtomicBoolean hasBackendAliveAndUnfinishedTask = new AtomicBoolean(false);
        Set<Long> notFinishTaskBe = Sets.newHashSet();
        transactionState.getPublishVersionTasks().forEach((key, tasks) -> {
            long beId = key;
            for (PublishVersionTask task : tasks) {
                if (task.isFinished()) {
                    calculateTaskUpdateRows(tableIdToTabletDeltaRows, task);
                } else {
                    if (infoService.checkBackendAlive(task.getBackendId())) {
                        hasBackendAliveAndUnfinishedTask.set(true);
                    }
                    notFinishTaskBe.add(beId);
                }
            }
        });

        transactionState.setTableIdToTabletDeltaRows(tableIdToTabletDeltaRows);
        if (LOG.isDebugEnabled()) {
            LOG.debug("notFinishTaskBe {}, trans {}", notFinishTaskBe, transactionState);
        }
        boolean isPublishSlow = false;
        long totalNum = transactionState.getPublishVersionTasks().keySet().size();
        boolean allUnFinishTaskIsSlow = notFinishTaskBe.stream().allMatch(beId -> {
            Backend be = infoService.getBackend(beId);
            if (be == null) {
                return false;
            }
            return be.getPublishTaskLastTimeAccumulated() > Config.publish_version_queued_limit_number;
        });
        if (totalNum - notFinishTaskBe.size() > totalNum / 2 && allUnFinishTaskIsSlow) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(" finishNum {}, txn publish tasks {}, notFinishTaskBe {}",
                        totalNum - notFinishTaskBe.size(), transactionState.getPublishVersionTasks().keySet(),
                        notFinishTaskBe);
            }
            isPublishSlow = true;
        }

        boolean shouldFinishTxn = !hasBackendAliveAndUnfinishedTask.get() || transactionState.isPublishTimeout()
                || isPublishSlow
                || DebugPointUtil.isEnable("PublishVersionDaemon.not_wait_unfinished_tasks");
        if (shouldFinishTxn) {
            try {
                // one transaction exception should not affect other transaction
                globalTransactionMgr.finishTransaction(transactionState.getDbId(),
                        transactionState.getTransactionId(), partitionVisibleVersions, backendPartitions);
            } catch (Exception e) {
                LOG.warn("error happens when finish transaction {}", transactionState.getTransactionId(), e);
            }
            if (transactionState.getTransactionStatus() != TransactionStatus.VISIBLE) {
                // if finish transaction state failed, then update publish version time, should check
                // to finish after some interval
                transactionState.updateSendTaskTime();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("publish version for transaction {} failed", transactionState);
                }
            }
        }

        if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
            transactionState.getPublishVersionTasks().values().forEach(tasks -> {
                for (PublishVersionTask task : tasks) {
                    AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.PUBLISH_VERSION, task.getSignature());
                }
            });
            transactionState.pruneAfterVisible();
            if (MetricRepo.isInit) {
                long publishTime = transactionState.getLastPublishVersionTime() - transactionState.getCommitTime();
                MetricRepo.HISTO_TXN_PUBLISH_LATENCY.update(publishTime);
            }
        }
    }

    // Merge task tablets update rows to tableToTabletsDelta.
    private void calculateTaskUpdateRows(Map<Long, Map<Long, Long>> tableIdToTabletDeltaRows, PublishVersionTask task) {
        if (CollectionUtils.isEmpty(task.getErrorTablets())) {
            for (Entry<Long, Map<Long, Long>> tableEntry : task.getTableIdToTabletDeltaRows().entrySet()) {
                if (tableIdToTabletDeltaRows.containsKey(tableEntry.getKey())) {
                    Map<Long, Long> tabletsDelta = tableIdToTabletDeltaRows.get(tableEntry.getKey());
                    for (Entry<Long, Long> tabletEntry : tableEntry.getValue().entrySet()) {
                        tabletsDelta.computeIfPresent(tabletEntry.getKey(),
                                (tabletId, origRows) -> origRows + tabletEntry.getValue());
                        tabletsDelta.putIfAbsent(tabletEntry.getKey(), tabletEntry.getValue());
                    }
                } else {
                    tableIdToTabletDeltaRows.put(tableEntry.getKey(), tableEntry.getValue());
                }
            }
        }
    }

    private Map<Long, Set<Long>> getBaseTabletIdsForEachBe(TransactionState transactionState,
            TableCommitInfo tableCommitInfo) throws MetaNotFoundException {

        OlapTable table = (OlapTable) Env.getCurrentEnv()
                .getInternalCatalog()
                .getDb(transactionState.getDbId())
                .orElseThrow(() -> new MetaNotFoundException(String.format("could not get db by id=%s",
                        transactionState.getDbId())))
                .getTable(tableCommitInfo.getTableId())
                .orElseThrow(() -> new MetaNotFoundException(String.format("could not get tbl by id=%s",
                        tableCommitInfo)));

        return tableCommitInfo
                .getIdToPartitionCommitInfo()
                .values().stream()
                .map(PartitionCommitInfo::getPartitionId)
                .map(partitionId -> Optional.ofNullable(table.getPartition(partitionId)))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(Partition::getBaseIndex)
                .map(MaterializedIndex::getTablets)
                .flatMap(Collection::stream)
                .flatMap(tablet ->
                        tablet.getBackendIds()
                                .stream().map(backendId -> Pair.of(backendId, tablet.getId())))
                .collect(Collectors.groupingBy(p -> p.first,
                        Collectors.mapping(p -> p.second, Collectors.toSet())));
    }

    private void sendBackendVisibleVersion(Map<Long, Long> partitionVisibleVersions,
            Map<Long, Set<Long>> backendPartitions) {
        if (partitionVisibleVersions.isEmpty() || backendPartitions.isEmpty()) {
            return;
        }

        long createTime = System.currentTimeMillis();
        AgentBatchTask batchTask = new AgentBatchTask();
        backendPartitions.forEach((backendId, partitionIds) -> {
            Map<Long, Long> backendPartitionVersions = partitionVisibleVersions.entrySet().stream()
                    .filter(entry -> partitionIds.contains(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            UpdateVisibleVersionTask task = new UpdateVisibleVersionTask(backendId, backendPartitionVersions,
                    createTime);
            batchTask.addTask(task);
        });
        AgentTaskExecutor.submit(batchTask);
    }

    private List<TPartitionVersionInfo> generatePartitionVersionInfos(Collection<TableCommitInfo> tableCommitInfos,
            TransactionState transactionState, Map<Long, Set<Long>> beIdToBaseTabletIds) {
        return tableCommitInfos.stream()
                .map(c -> generatePartitionVersionInfos(c, transactionState, beIdToBaseTabletIds)).flatMap(List::stream)
                .collect(Collectors.toList());
    }

    private List<TPartitionVersionInfo> generatePartitionVersionInfos(TableCommitInfo tableCommitInfo,
            TransactionState transactionState, Map<Long, Set<Long>> beIdToBaseTabletIds) {
        try {
            Map<Long, Set<Long>> map = getBaseTabletIdsForEachBe(transactionState, tableCommitInfo);
            map.forEach((beId, newSet) -> {
                beIdToBaseTabletIds.computeIfPresent(beId, (id, orgSet) -> {
                    orgSet.addAll(newSet);
                    return orgSet;
                });
                beIdToBaseTabletIds.putIfAbsent(beId, newSet);
            });
        } catch (MetaNotFoundException e) {
            LOG.warn("exception occur when trying to get rollup tablets info", e);
        }
        return tableCommitInfo.generateTPartitionVersionInfos();
    }

    private void addPublishVersionTask(Set<Long> publishBackends, long transactionId, TransactionState transactionState,
            List<TPartitionVersionInfo> partitionVersionInfos, Map<Long, Set<Long>> beIdToBaseTabletIds,
            long createPublishVersionTaskTime,
            AgentBatchTask batchTask) {
        for (Long backendId : publishBackends) {
            PublishVersionTask task = new PublishVersionTask(backendId,
                    transactionId,
                    transactionState.getDbId(),
                    partitionVersionInfos,
                    createPublishVersionTaskTime);
            task.setBaseTabletsIds(beIdToBaseTabletIds.getOrDefault(backendId, Collections.emptySet()));
            // add to AgentTaskQueue for handling finish report.
            // not check return value, because the add will success
            AgentTaskQueue.addTask(task);
            batchTask.addTask(task);
            transactionState.addPublishVersionTask(backendId, task);
        }
    }
}
