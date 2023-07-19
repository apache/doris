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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.PublishVersionTask;
import org.apache.doris.thrift.TPartitionVersionInfo;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class PublishVersionDaemon extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(PublishVersionDaemon.class);
    private Handler[] handlers;
    private boolean handlerStarted = false;
    private final PublishQueue publishQueue = new PublishQueue();

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

    private boolean isAllBackendsOfUnfinishedTasksDead(List<PublishVersionTask> unfinishedTasks) {
        for (PublishVersionTask unfinishedTask : unfinishedTasks) {
            if (Env.getCurrentSystemInfo().checkBackendAlive(unfinishedTask.getBackendId())) {
                return false;
            }
        }
        return true;
    }

    private void publishVersion() throws Exception {
        GlobalTransactionMgr globalTransactionMgr = Env.getCurrentGlobalTransactionMgr();
        List<TransactionState> readyTransactionStates = globalTransactionMgr.getReadyToPublishTransactions();
        if (readyTransactionStates.isEmpty()) {
            return;
        }

        // ATTN, we publish transaction state to all backends including dead backend, if not publish to dead backend
        // then transaction manager will treat it as success
        List<Long> allBackends = Env.getCurrentSystemInfo().getAllBackendIds(false);
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
                    LOG.debug("try to publish version info partitionid [{}], version [{}]", commitInfo.getPartitionId(),
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
                PublishVersionTask task = new PublishVersionTask(backendId, transactionState.getTransactionId(),
                        transactionState.getDbId(), partitionVersionInfos, createPublishVersionTaskTime);
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

        if (Config.mt_enable_parallel_publish) {
            if (!handlerStarted) {
                startHandlers();
            }
            publishQueue.putQueue(readyTransactionStates);
        } else {
            if (handlerStarted) {
                stopHandlers();
            }
            tryFinishTransactions(readyTransactionStates);
        }
    }

    private static class PublishItem {
        private final List<Long> tables;
        private final Deque<TBatch> stateBatches;
        private final AtomicBoolean isRunning = new AtomicBoolean(false);
        private long lastVisitTime = 0;

        public PublishItem(List<Long> tables) {
            this.tables = tables;
            this.stateBatches = new ConcurrentLinkedDeque<>();
        }

        public void setRunning(boolean flag) {
            isRunning.set(flag);
        }

        public boolean isRunning() {
            return isRunning.get();
        }

        public Queue<TBatch> getBatches() {
            return stateBatches;
        }

        public TBatch popBatch() {
            return stateBatches.pop();
        }

        public TBatch peekBatch() {
            return stateBatches.peek();
        }

        public void addBatch(TBatch batch) {
            stateBatches.add(batch);
        }

        public void clearBatch() {
            stateBatches.forEach(TBatch::clear);
            stateBatches.clear();
        }

        public List<Long> getTables() {
            return tables;
        }

        public void updateLastVisitTime() {
            this.lastVisitTime = System.currentTimeMillis();
        }

        public boolean isExpired() {
            if (!stateBatches.isEmpty()) {
                return false;
            }
            return (System.currentTimeMillis() - lastVisitTime) / 1000 > Config.mt_publish_table_idle_time_secs;
        }
    }

    private static class TBatch {
        public List<TransactionState> states ;
        public TBatch() {
            this.states = Lists.newLinkedList();
        }

        public void add(TransactionState state) {
            states.add(state);
        }

        public void clear() {
            states.forEach(state -> state.setHasDispatched(false));
            states.clear();
        }

        public List<TransactionState> states() {
            return states;
        }
    }

    private static class PublishQueue {
        private final HashMap<List<Long>, PublishItem> pendingTransactions = new HashMap<>();
        private final List<List<Long>> tablesIndex = Lists.newArrayListWithCapacity(4096);
        private int idx = 0;
        private final ReentrantLock lock = new ReentrantLock(true);
        private final Condition condition = lock.newCondition();
        private long lastCheckTime = System.currentTimeMillis();

        public void putQueue(List<TransactionState> readyTransactionStates) {
            HashMap<List<Long>, TBatch> batchMap = Maps.newHashMap();
            for (TransactionState readyTransactionState : readyTransactionStates) {
                if (readyTransactionState.canDispatch()) {
                    List<Long> tables = readyTransactionState.getSortTables();
                    putIfAbsent(tables);
                    batchMap.computeIfAbsent(tables, key -> new TBatch()).add(readyTransactionState);
                    readyTransactionState.setHasDispatched(true);
                }
            }
            if (!batchMap.isEmpty()) {
                batchMap.forEach((k, v) -> {
                    PublishItem item = pendingTransactions.get(k);
                    item.addBatch(v);
                    item.updateLastVisitTime();
                });
                signalAll();
            }
            tryRemoveIdleTables();
        }

        public PublishItem getPublishItem() throws InterruptedException {
            String name = Thread.currentThread().getName();
            long timeMillis = System.currentTimeMillis();
            LOG.debug("{} get lock for getPublishItem", name);
            lock.lock();
            LOG.debug("{} get lock spent {} for getPublishItem", name, System.currentTimeMillis() - timeMillis);
            try {
                long s1 = System.currentTimeMillis();
                int size = pendingTransactions.size();
                if (idx >= size) {
                    idx = 0;
                }
                int index = idx;
                PublishItem result = null;
                while (index < size) {
                    PublishItem publishItem = pendingTransactions.get(tablesIndex.get(index++));
                    if (index >= size) {
                        index = 0;
                    }
                    if (!publishItem.isRunning() && !publishItem.getBatches().isEmpty()) {
                        publishItem.setRunning(true);
                        result = publishItem;
                        idx = index;
                        LOG.debug("{} get {} publish transactions. current index : {}/{}", Thread.currentThread().getName(), publishItem.getTables(), idx == 0 ? size - 1 : idx - 1, size);
                        break;
                    }
                    if (index == idx) {
                        break;
                    }
                }
                LOG.debug("{} get publish item spent {}", name, System.currentTimeMillis() - s1);
                if (Objects.isNull(result)) {
                    condition.await();
                }
                return result;
            } finally {
                lock.unlock();
            }
        }

        private void signalAll() {
            lock.lock();
            try {
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }

        public void putIfAbsent(List<Long> tables) {
            if (!pendingTransactions.containsKey(tables)) {
                String name = Thread.currentThread().getName();
                long timeMillis = System.currentTimeMillis();
                lock.lock();
                LOG.info("{} get lock spent {} for putIfAbsent", name, System.currentTimeMillis() - timeMillis);
                try {
                    pendingTransactions.put(tables, new PublishItem(tables));
                    tablesIndex.add(tables);
                } finally {
                    lock.unlock();
                }
            }
        }

        public void clear() {
            lock.lock();
            try {
                pendingTransactions.values().forEach(PublishItem::clearBatch);
                pendingTransactions.clear();
                tablesIndex.clear();
            } finally {
                lock.unlock();
            }
        }

        public void remove(List<Long> tables) {
            lock.lock();
            try {
                pendingTransactions.remove(tables);
                tablesIndex.remove(tables);
            } finally {
                lock.unlock();
            }
        }

        public void tryRemoveIdleTables() {
            long timeMillis = System.currentTimeMillis();
            if ((timeMillis - lastCheckTime) / 1000 > 3600) {
                List<PublishItem> values = Lists.newArrayList(pendingTransactions.values());
                for (PublishItem item : values) {
                    if (item.isExpired()) {
                        remove(item.getTables());
                        LOG.info("{} has expired, remove it from publish queue.", item.getTables());
                    }
                }
                lastCheckTime = System.currentTimeMillis();
            }
        }
    }

    private class Handler extends Daemon {

        public Handler(int id) {
            super("publish_version_thread_" + id, 0);
        }

        @Override
        public void runOneCycle() throws Exception {
            PublishItem publishItem = publishQueue.getPublishItem();
            if (Objects.nonNull(publishItem)) {
                TBatch batch = publishItem.peekBatch();
                List<TransactionState> states = batch.states();
                try {
                    Iterator<TransactionState> it = states.iterator();
                    while (it.hasNext()) {
                        TransactionState state = it.next();
                        LOG.debug("{} get transaction state txn id {} | {}", getName(), state.getTransactionId(), state.getTransactionStatus());
                        tryFinishTransaction(state);
                        if (state.isVisible() || state.isAborted()){
                            LOG.debug("{} remove transaction state txn id {} | {}", getName(), state.getTransactionId(), state.getTransactionStatus());
                            it.remove();
                        } else {
                            break;
                        }
                    }
                } finally {
                    if (states.isEmpty()) {
                        LOG.debug("{} pop transaction state batch", getName());
                        publishItem.popBatch();
                    }
                    publishItem.setRunning(false);
                    LOG.debug("{} have {} txn", publishItem.getTables(), publishItem.getBatches().size());
                }
            }
        }
    }

    private void startHandlers() throws Exception {
        stopHandlers();
        int handlerCount = Config.mt_parallel_publish_thread_num;
        handlers = new Handler[handlerCount];
        for (int i = 0; i < handlerCount; i++) {
            handlers[i] = new Handler(i);
            handlers[i].start();
            LOG.info(handlers[i].getName() + " has started.");
        }
        handlerStarted = true;
    }

    private void stopHandlers() throws Exception {
        if (handlers != null && handlers.length > 0) {
            for (Handler handler : handlers) {
                handler.exit();
            }
            publishQueue.signalAll();
            for (Handler handler : handlers) {
                handler.join();
                LOG.info(handler.getName() + " has stopped.");
            }
        } else {
            LOG.info("there no started publish handler threads.");
        }
        handlers = null;
        publishQueue.clear();
        handlerStarted = false;
    }

    private void tryFinishTransaction(TransactionState transactionState) {
        GlobalTransactionMgr globalTransactionMgr = Env.getCurrentGlobalTransactionMgr();
        TabletInvertedIndex tabletInvertedIndex = Env.getCurrentInvertedIndex();
        Map<Long, PublishVersionTask> transTasks = transactionState.getPublishVersionTasks();
        Set<Long> publishErrorReplicaIds = Sets.newHashSet();
        List<PublishVersionTask> unfinishedTasks = Lists.newArrayList();
        for (PublishVersionTask publishVersionTask : transTasks.values()) {
            if (publishVersionTask.isFinished()) {
                // sometimes backend finish publish version task,
                // but it maybe failed to change transactionid to version for some tablets
                // and it will upload the failed tabletinfo to fe and fe will deal with them
                List<Long> errorTablets = publishVersionTask.getErrorTablets();
                if (CollectionUtils.isNotEmpty(errorTablets)) {
                    for (long tabletId : errorTablets) {
                        // tablet inverted index also contains rollingup index
                        // if tablet meta not contains the tablet, skip this tablet because this tablet is dropped
                        // from fe
                        if (tabletInvertedIndex.getTabletMeta(tabletId) == null) {
                            continue;
                        }
                        Replica replica = tabletInvertedIndex.getReplica(
                                tabletId, publishVersionTask.getBackendId());
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

        boolean shouldFinishTxn;
        if (!unfinishedTasks.isEmpty()) {
            shouldFinishTxn = isAllBackendsOfUnfinishedTasksDead(unfinishedTasks);
            if (transactionState.isPublishTimeout() || shouldFinishTxn) {
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

                    Database db = Env.getCurrentInternalCatalog()
                            .getDbNullable(transactionState.getDbId());
                    if (db == null) {
                        LOG.warn("Database [{}] has been dropped.", transactionState.getDbId());
                        continue;
                    }

                    for (long tableId : transactionState.getTableIdList()) {
                        Table table = db.getTableNullable(tableId);
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
                                    List<MaterializedIndex> materializedIndexList
                                            = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
                                    for (MaterializedIndex materializedIndex : materializedIndexList) {
                                        for (Tablet tablet : materializedIndex.getTablets()) {
                                            Replica replica = tablet.getReplicaByBackendId(
                                                    unfinishedTask.getBackendId());
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
        } else {
            // all publish tasks are finished, try to finish this txn.
            shouldFinishTxn = true;
        }

        if (shouldFinishTxn) {
            try {
                // one transaction exception should not affect other transaction
                long nanoTime = System.nanoTime();
                globalTransactionMgr.finishTransaction(transactionState.getDbId(),
                        transactionState.getTransactionId(), publishErrorReplicaIds);
                LOG.debug("finish transaction {} spent {} ms", transactionState.getTransactionId(), (System.nanoTime() - nanoTime) / 1000000L);
            } catch (Exception e) {
                LOG.warn("error happens when finish transaction {}", transactionState.getTransactionId(), e);
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
            if (MetricRepo.isInit) {
                long publishTime = transactionState.getPublishVersionTime() - transactionState.getCommitTime();
                MetricRepo.HISTO_TXN_PUBLISH_LATENCY.update(publishTime);
            }
        }
    }

    private void tryFinishTransactions(List<TransactionState> readyTransactionStates) {
        for (TransactionState transactionState : readyTransactionStates) {
            tryFinishTransaction(transactionState);
        }
    }

}
