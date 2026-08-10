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

package org.apache.doris.clone;

import org.apache.doris.common.ThreadPoolManager;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

class TableDispatchScheduler {
    private static final Logger LOG = LogManager.getLogger(TableDispatchScheduler.class);
    private static final int MAX_ACTIVE_TABLE_WORKERS = 16;
    private static final long WORKER_KEEP_ALIVE_SECONDS = 60;

    // table id -> tablets queued for worker consumption (worker-queued state)
    private final Map<Long, Deque<TabletSchedCtx>> workerQueuedTabletsByTable = Maps.newHashMap();
    // one table can only be handled by one worker at a time
    private final Set<Long> activeTableWorkers = Sets.newHashSet();
    private final ThreadPoolExecutor tableWorkerPool;
    private final Consumer<List<TabletSchedCtx>> tabletBatchProcessor;
    private final int workerBatchSize;

    TableDispatchScheduler(Consumer<List<TabletSchedCtx>> tabletBatchProcessor, int workerBatchSize) {
        this.tabletBatchProcessor = tabletBatchProcessor;
        this.workerBatchSize = workerBatchSize;
        this.tableWorkerPool = ThreadPoolManager.newDaemonThreadPool(0, MAX_ACTIVE_TABLE_WORKERS,
                WORKER_KEEP_ALIVE_SECONDS, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                new ThreadPoolExecutor.AbortPolicy(), "table-dispatch-scheduler-worker", true);
        this.tableWorkerPool.allowCoreThreadTimeOut(true);
    }

    synchronized void clear() {
        workerQueuedTabletsByTable.clear();
        activeTableWorkers.clear();
    }

    void enqueueTablets(List<TabletSchedCtx> tabletCtxs) {
        synchronized (this) {
            for (TabletSchedCtx tabletCtx : tabletCtxs) {
                long tableId = tabletCtx.getTblId();
                Deque<TabletSchedCtx> queue = workerQueuedTabletsByTable.get(tableId);
                if (queue == null) {
                    queue = new LinkedList<>();
                    workerQueuedTabletsByTable.put(tableId, queue);
                }
                queue.addLast(tabletCtx);
            }
        }

        triggerTableWorkers();
    }

    synchronized void appendWorkerQueuedTablets(List<TabletSchedCtx> target, int limit) {
        for (Deque<TabletSchedCtx> queue : workerQueuedTabletsByTable.values()) {
            if (target.size() >= limit) {
                return;
            }
            queue.stream().limit(limit - target.size()).forEach(target::add);
        }
    }

    synchronized List<TabletSchedCtx> getWorkerQueuedTablets() {
        List<TabletSchedCtx> queuedTablets = Lists.newArrayList();
        for (Deque<TabletSchedCtx> queue : workerQueuedTabletsByTable.values()) {
            queuedTablets.addAll(queue);
        }
        return queuedTablets;
    }

    private void triggerTableWorkers() {
        while (true) {
            long tableIdToSchedule;
            synchronized (this) {
                if (activeTableWorkers.size() >= MAX_ACTIVE_TABLE_WORKERS) {
                    return;
                }

                tableIdToSchedule = pickNextTableIdToActivate();
                if (tableIdToSchedule == -1) {
                    return;
                }

                activeTableWorkers.add(tableIdToSchedule);
            }
            try {
                tableWorkerPool.execute(new TableScheduleWorker(tableIdToSchedule));
            } catch (RuntimeException e) {
                synchronized (this) {
                    activeTableWorkers.remove(tableIdToSchedule);
                }
                LOG.warn("failed to submit table schedule worker for table {}", tableIdToSchedule, e);
                return;
            }
        }
    }

    private long pickNextTableIdToActivate() {
        long tableId = -1;
        TabletSchedCtx candidate = null;
        for (Map.Entry<Long, Deque<TabletSchedCtx>> entry : workerQueuedTabletsByTable.entrySet()) {
            if (activeTableWorkers.contains(entry.getKey()) || entry.getValue().isEmpty()) {
                continue;
            }
            TabletSchedCtx head = entry.getValue().peekFirst();
            if (head == null) {
                continue;
            }

            // higher priority tablet
            if (candidate == null || head.compareTo(candidate) < 0) {
                candidate = head;
                tableId = entry.getKey();
            }
        }
        return tableId;
    }

    private synchronized List<TabletSchedCtx> pollNextTabletCtxBatch(long tableId) {
        List<TabletSchedCtx> tabletCtxs = Lists.newArrayList();
        Deque<TabletSchedCtx> queue = workerQueuedTabletsByTable.get(tableId);
        if (queue == null) {
            return tabletCtxs;
        }
        while (tabletCtxs.size() < workerBatchSize) {
            TabletSchedCtx tabletCtx = queue.pollFirst();
            if (tabletCtx == null) {
                break;
            }
            tabletCtxs.add(tabletCtx);
        }
        if (queue.isEmpty()) {
            workerQueuedTabletsByTable.remove(tableId);
        }
        return tabletCtxs;
    }

    private void onTableWorkerDone(long tableId) {
        synchronized (this) {
            activeTableWorkers.remove(tableId);
        }
        triggerTableWorkers();
    }

    private class TableScheduleWorker implements Runnable {
        private final long tableId;

        private TableScheduleWorker(long tableId) {
            this.tableId = tableId;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    List<TabletSchedCtx> tabletCtxBatch = pollNextTabletCtxBatch(tableId);
                    if (tabletCtxBatch.isEmpty()) {
                        return;
                    }
                    tabletBatchProcessor.accept(tabletCtxBatch);
                }
            } finally {
                onTableWorkerDone(tableId);
            }
        }
    }
}
