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

package org.apache.doris.cloud.transaction;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.cloud.proto.Cloud.CommitTxnRequest;
import org.apache.doris.cloud.proto.Cloud.CommitTxnResponse;
import org.apache.doris.cloud.proto.Cloud.MetaServiceCode;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.CalcDeleteBitmapAsyncPublishTask;
import org.apache.doris.thrift.TCalcDeleteBitmapPartitionInfo;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TxnCommitAttachment;
import org.apache.doris.transaction.TxnStateCallbackFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * CloudPublishDaemon asynchronously publishes committed transactions to VISIBLE state
 * in the cloud MOW async publish flow.
 *
 * Pattern follows PublishVersionDaemon:
 * 1. Iterate committed txns, dispatch CalcDeleteBitmapTask to BEs (tasks not yet sent)
 * 2. Check if all CalcDeleteBitmapTasks for a txn are finished
 * 3. If all finished, submit lightweight publish (MS commitTxn with is_lightweight_publish=true)
 *
 * CalcDeleteBitmapTask uses AgentTaskQueue auto-retry: on failure the task stays in queue
 * and is re-sent via BE heartbeat. Committed transactions always eventually succeed.
 */
public class CloudPublishDaemon extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CloudPublishDaemon.class);

    private final CommittedTxnManager committedTxnManager;
    private final TxnStateCallbackFactory callbackFactory;

    // txnId -> Map<backendId, CalcDeleteBitmapAsyncPublishTask>
    // Tracks sent CalcDeleteBitmapAsyncPublishTasks per transaction
    private final ConcurrentHashMap<Long, Map<Long, CalcDeleteBitmapAsyncPublishTask>> txnCalcTasks
            = new ConcurrentHashMap<>();

    // txnIds currently doing lightweight publish (avoid duplicate submission)
    private final Set<Long> publishingTxnIds = Sets.newConcurrentHashSet();

    // Thread pool for lightweight publish execution
    private static ArrayList<ExecutorService> publishExecutors;

    public CloudPublishDaemon(CommittedTxnManager committedTxnManager,
                              TxnStateCallbackFactory callbackFactory) {
        super("CLOUD_PUBLISH_DAEMON", Config.cloud_publish_interval_ms);
        this.committedTxnManager = committedTxnManager;
        this.callbackFactory = callbackFactory;
        initPublishExecutors();
    }

    private static synchronized void initPublishExecutors() {
        if (publishExecutors != null) {
            return;
        }
        publishExecutors = new ArrayList<>(Config.cloud_publish_thread_pool_size);
        for (int i = 0; i < Config.cloud_publish_thread_pool_size; i++) {
            publishExecutors.add(ThreadPoolManager.newDaemonFixedThreadPool(
                    1, 256, "CLOUD_PUBLISH_EXEC-" + i, true));
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            schedulePublish();
        } catch (Throwable t) {
            LOG.error("errors in cloud publish daemon", t);
        }
    }

    private void schedulePublish() {
        if (DebugPointUtil.isEnable("CloudPublishDaemon.stop_publish")) {
            return;
        }

        Collection<Long> tableIds = committedTxnManager.getTablesWithCommittedTxns();
        if (tableIds.isEmpty()) {
            return;
        }

        // Phase 1: Dispatch CalcDeleteBitmapTask for txns that haven't sent yet
        dispatchCalcDeleteBitmapTasks(tableIds);

        // Phase 2: Try to finish txns whose all tasks are done
        tryFinishTxns(tableIds);
    }

    // ========== Phase 1: Dispatch CalcDeleteBitmapAsyncPublishTask ==========

    private void dispatchCalcDeleteBitmapTasks(Collection<Long> tableIds) {
        AgentBatchTask batchTask = new AgentBatchTask();
        for (long tableId : tableIds) {
            List<CommittedTxnEntry> txnEntries = committedTxnManager.getByTableId(tableId);
            for (CommittedTxnEntry entry : txnEntries) {
                long txnId = entry.getTxnId();
                if (txnCalcTasks.containsKey(txnId)) {
                    continue; // Already sent
                }
                try {
                    Map<Long, CalcDeleteBitmapAsyncPublishTask> tasks =
                            buildAndSendCalcTasks(entry, batchTask);
                    txnCalcTasks.put(txnId, tasks);
                    LOG.info("dispatched calc delete bitmap tasks for txnId={}, beCount={}",
                            txnId, tasks.size());
                } catch (Exception e) {
                    LOG.warn("failed to dispatch calc delete bitmap tasks for txnId={}", txnId, e);
                }
            }
        }
        if (!batchTask.getAllTasks().isEmpty()) {
            AgentTaskExecutor.submit(batchTask);
        }
    }

    /**
     * Build CalcDeleteBitmapAsyncPublishTask per-BE for a committed transaction.
     * Similar to CloudGlobalTransactionMgr.sendCalcDeleteBitmaptask() but async (no latch).
     */
    private Map<Long, CalcDeleteBitmapAsyncPublishTask> buildAndSendCalcTasks(
            CommittedTxnEntry entry, AgentBatchTask batchTask) {
        long txnId = entry.getTxnId();
        long dbId = entry.getDbId();

        // Build backendId -> List<TCalcDeleteBitmapPartitionInfo> from tabletCommitInfos
        // Group tablets by (backendId, partitionId)
        // backendId -> (partitionId -> tabletIds)
        Map<Long, Map<Long, List<TabletCommitInfo>>> beToPartitionTablets = new HashMap<>();
        for (TabletCommitInfo info : entry.getTabletCommitInfos()) {
            long tabletId = info.getTabletId();
            long backendId = info.getBackendId();
            TabletMeta tabletMeta = Env.getCurrentInvertedIndex().getTabletMeta(tabletId);
            if (tabletMeta == null) {
                LOG.warn("tablet {} meta not found, skip for txnId={}", tabletId, txnId);
                continue;
            }
            long partitionId = tabletMeta.getPartitionId();
            beToPartitionTablets
                    .computeIfAbsent(backendId, k -> new HashMap<>())
                    .computeIfAbsent(partitionId, k -> new ArrayList<>())
                    .add(info);
        }

        Map<Long, Long> partitionCommitVersions = entry.getPartitionCommitVersions();
        Map<Long, CalcDeleteBitmapAsyncPublishTask> tasks = new HashMap<>();
        long signature = txnId; // Use txnId as signature

        for (Map.Entry<Long, Map<Long, List<TabletCommitInfo>>> beEntry : beToPartitionTablets.entrySet()) {
            long backendId = beEntry.getKey();
            List<TCalcDeleteBitmapPartitionInfo> partitionInfos = Lists.newArrayList();
            for (Map.Entry<Long, List<TabletCommitInfo>> partEntry : beEntry.getValue().entrySet()) {
                long partitionId = partEntry.getKey();
                Long version = partitionCommitVersions.get(partitionId);
                if (version == null) {
                    LOG.warn("partition {} commit version not found for txnId={}", partitionId, txnId);
                    continue;
                }

                List<Long> tabletIds = new ArrayList<>();
                List<Long> indexIds = new ArrayList<>();
                long partitionDbId = -1;
                long partitionTableId = -1;
                for (TabletCommitInfo tabletCommitInfo : partEntry.getValue()) {
                    TabletMeta tabletMeta = Env.getCurrentInvertedIndex()
                            .getTabletMeta(tabletCommitInfo.getTabletId());
                    if (tabletMeta == null) {
                        LOG.warn("tablet {} meta not found when building async publish task for txnId={}",
                                tabletCommitInfo.getTabletId(), txnId);
                        continue;
                    }
                    tabletIds.add(tabletCommitInfo.getTabletId());
                    indexIds.add(tabletMeta.getIndexId());
                    partitionDbId = tabletMeta.getDbId();
                    partitionTableId = tabletMeta.getTableId();
                }
                if (tabletIds.isEmpty()) {
                    continue;
                }

                TCalcDeleteBitmapPartitionInfo partitionInfo = new TCalcDeleteBitmapPartitionInfo(
                        partitionId, version, tabletIds);
                partitionInfo.setDbId(partitionDbId);
                partitionInfo.setTableId(partitionTableId);
                partitionInfo.setIndexIds(indexIds);
                partitionInfos.add(partitionInfo);
            }

            CalcDeleteBitmapAsyncPublishTask task = new CalcDeleteBitmapAsyncPublishTask(
                    backendId, txnId, dbId, partitionInfos, signature);
            AgentTaskQueue.addTask(task);
            batchTask.addTask(task);
            tasks.put(backendId, task);
        }
        return tasks;
    }

    // ========== Phase 2: Try to finish txns ==========

    private void tryFinishTxns(Collection<Long> tableIds) {
        for (long tableId : tableIds) {
            List<CommittedTxnEntry> txnEntries = committedTxnManager.getByTableId(tableId);
            for (CommittedTxnEntry entry : txnEntries) {
                long txnId = entry.getTxnId();
                Map<Long, CalcDeleteBitmapAsyncPublishTask> tasks = txnCalcTasks.get(txnId);
                if (tasks == null) {
                    continue; // Not dispatched yet
                }

                // Check if all CalcDeleteBitmapAsyncPublishTasks are finished
                boolean allFinished = true;
                for (CalcDeleteBitmapAsyncPublishTask task : tasks.values()) {
                    if (!task.isFinished()) {
                        allFinished = false;
                        break;
                    }
                }

                if (allFinished) {
                    tryFinishOneTxn(entry);
                }
            }
        }
    }

    private void tryFinishOneTxn(CommittedTxnEntry entry) {
        long txnId = entry.getTxnId();
        if (publishingTxnIds.contains(txnId)) {
            return; // Already submitting lightweight publish
        }

        publishingTxnIds.add(txnId);
        long routingKey = txnId;
        try {
            publishExecutors.get((int) (Math.abs(routingKey) % publishExecutors.size())).execute(() -> {
                try {
                    executeLightweightPublish(entry);
                } catch (Throwable e) {
                    LOG.warn("lightweight publish failed for txnId={}, will retry", txnId, e);
                } finally {
                    publishingTxnIds.remove(txnId);
                }
            });
        } catch (Throwable e) {
            LOG.warn("failed to submit lightweight publish for txnId={}", txnId, e);
            publishingTxnIds.remove(txnId);
        }
    }

    /**
     * Execute lightweight publish: call MS commitTxn with is_lightweight_publish=true.
     * Only updates visible_version+1 and TxnInfoPB status to VISIBLE.
     */
    private void executeLightweightPublish(CommittedTxnEntry entry) throws RpcException {
        long txnId = entry.getTxnId();
        long dbId = entry.getDbId();
        TxnCommitAttachment txnCommitAttachment = entry.getTxnCommitAttachment();

        CommitTxnRequest.Builder builder = CommitTxnRequest.newBuilder();
        builder.setDbId(dbId)
                .setTxnId(txnId)
                .setIs2Pc(false)
                .setCloudUniqueId(Config.cloud_unique_id)
                .setIsLightweightPublish(true);

        CommitTxnRequest commitTxnRequest = builder.build();
        CommitTxnResponse response = null;
        int retryTime = 0;

        try {
            while (retryTime < Config.metaServiceRpcRetryTimes()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("lightweight publish retryTime:{}, txnId:{}", retryTime, txnId);
                }
                MetaServiceProxy proxy = new MetaServiceProxy();
                response = proxy.commitTxn(commitTxnRequest);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("lightweight publish retryTime:{}, response code:{}", retryTime,
                            response.getStatus().getCode());
                }
                if (response.getStatus().getCode() != MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }
                // sleep random [20, 200] ms, avoid txn conflict
                LOG.info("lightweight publish KV_TXN_CONFLICT, transactionId:{}, retryTime:{}", txnId, retryTime);
                TxnUtil.backoff();
                retryTime++;
            }

            if (response == null || response.getStatus() == null) {
                throw new RpcException("", "lightweight publish failed for txn " + txnId + ", response is null");
            }
        } catch (Exception e) {
            LOG.error("lightweight publish failed, transactionId:{}, retryTime:{}", txnId, retryTime, e);
            throw new RpcException("", "lightweight publish failed for txn " + txnId + ": " + e.getMessage());
        }

        MetaServiceCode code = response.getStatus().getCode();
        if (code != MetaServiceCode.OK && code != MetaServiceCode.TXN_ALREADY_VISIBLE) {
            throw new RpcException("", "lightweight publish failed for txn " + txnId
                    + ", code=" + code + ", msg=" + response.getStatus().getMsg());
        }

        // Execute common after-commit operations (update versions, stats, produce events)
        List<Long> tabletIds = entry.getTabletCommitInfos().stream()
                .map(info -> info.getTabletId())
                .collect(java.util.stream.Collectors.toList());
        TxnUtil.afterCommitCommon(response, tabletIds);

        // Execute callback handling using common utility
        // For lightweight publish (two-phase), txnOperated is always true since RPC succeeded
        // Parse TransactionState from response for callback
        TransactionState txnState = null;
        if (response.hasTxnInfo()) {
            txnState = TxnUtil.transactionStateFromPb(response.getTxnInfo());
        }
        TxnUtil.executeCommitCallbacks(txnId, txnCommitAttachment, txnState, callbackFactory, true, true);

        // Success: clean up tracking, remove from committed set, wake up import thread
        Map<Long, CalcDeleteBitmapAsyncPublishTask> tasks = txnCalcTasks.remove(txnId);
        if (tasks != null) {
            // Remove tasks from AgentTaskQueue (they're already finished)
            for (Map.Entry<Long, CalcDeleteBitmapAsyncPublishTask> taskEntry : tasks.entrySet()) {
                AgentTaskQueue.removeTask(taskEntry.getKey(),
                        TTaskType.CALC_DELETE_BITMAP_ASYNC_PUBLISH,
                        taskEntry.getValue().getSignature());
            }
        }
        committedTxnManager.removeCommittedTxn(txnId);
        entry.markPublishSucceeded();

        LOG.info("cloud publish completed, txnId={}, dbId={}", txnId, dbId);
    }

    // Visible for testing
    public ConcurrentHashMap<Long, Map<Long, CalcDeleteBitmapAsyncPublishTask>> getTxnCalcTasks() {
        return txnCalcTasks;
    }
}
