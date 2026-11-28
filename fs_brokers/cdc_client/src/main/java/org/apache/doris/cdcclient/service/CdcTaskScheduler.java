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

package org.apache.doris.cdcclient.service;

import org.apache.doris.cdcclient.common.CdcResourceManager;
import org.apache.doris.cdcclient.common.CdcResourceManager.ReaderGroup;
import org.apache.doris.cdcclient.common.ReaderQuotaUnavailableException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runs tasks in separate phase pools and retries tasks waiting for reader quota. */
public final class CdcTaskScheduler implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(CdcTaskScheduler.class);
    private static final int QUEUE_CAPACITY = 128;
    private final CdcResourceManager resources;
    private final int queueCapacity;
    private final ThreadPoolExecutor snapshots;
    private final ThreadPoolExecutor incrementals;
    private final ScheduledExecutorService waitingTaskRetryExecutor;
    private final Map<String, Task> tasks = new LinkedHashMap<>();
    // Only failed quota requests wait here; remove them before resubmitting to the executor.
    private final Map<String, Task> waitingTasks = new LinkedHashMap<>();
    private volatile boolean closed;

    private final class Task implements Runnable {
        private final String jobId;
        private final String taskId;
        private final ReaderGroup group;
        private final Runnable action;
        private final CompletableFuture<Void> result = new CompletableFuture<>();
        private boolean needsQuota;
        private Thread executionThread;

        private Task(String jobId, String taskId, ReaderGroup group, Runnable action) {
            this.jobId = jobId;
            this.taskId = taskId;
            this.group = group;
            this.action = action;
        }

        @Override
        public void run() {
            runTask(this);
        }
    }

    public CdcTaskScheduler(CdcResourceManager resources, int workers) {
        this(resources, workers, QUEUE_CAPACITY);
    }

    public CdcTaskScheduler(CdcResourceManager resources, int workers, int queueCapacity) {
        this.resources = resources;
        this.queueCapacity = queueCapacity;
        snapshots = createTaskExecutor(workers, queueCapacity, "cdc-snapshot-task");
        incrementals = createTaskExecutor(workers, queueCapacity, "cdc-incremental-task");
        waitingTaskRetryExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread thread = new Thread(r, "cdc-waiting-task-retry");
                            thread.setDaemon(true);
                            return thread;
                        });
        waitingTaskRetryExecutor.scheduleWithFixedDelay(
                this::retryWaitingTasks, 0, 1, TimeUnit.SECONDS);
    }

    private static ThreadPoolExecutor createTaskExecutor(
            int workers, int queueCapacity, String name) {
        // Include this pool's running tasks, which may rejoin the queue after a quota failure.
        return new ThreadPoolExecutor(
                workers,
                workers,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(Math.addExact(queueCapacity, workers)),
                runnable -> new Thread(runnable, name));
    }

    public synchronized CompletableFuture<Void> submit(
            String jobId, String taskId, ReaderGroup group, Runnable action) {
        Task previous = tasks.get(taskId);
        if (previous != null) {
            return previous.result;
        }
        long queued = tasks.values().stream().filter(task -> task.executionThread == null).count();
        if (closed || queued >= queueCapacity) {
            LOG.info(
                    "Rejected CDC task, jobId={}, taskId={}, group={}, queuedTasks={}, queueCapacity={}, closed={}",
                    jobId,
                    taskId,
                    group,
                    queued,
                    queueCapacity,
                    closed);
            throw new RejectedExecutionException("CDC task queue is full or closed");
        }
        Task task = new Task(jobId, taskId, group, action);
        tasks.put(taskId, task);
        executor(group).execute(task);
        return task.result;
    }

    synchronized CompletableFuture<Void> submitStream(
            String jobId, String taskId, ReaderGroup group, Runnable action) {
        // Each HTTP request owns its output stream and needs its own execution.
        if (tasks.containsKey(taskId)) {
            throw new RejectedExecutionException("CDC task already active: " + taskId);
        }
        return submit(jobId, taskId, group, action);
    }

    synchronized void cancelStream(String taskId, CompletableFuture<Void> result) {
        Task task = tasks.get(taskId);
        if (task != null && task.result == result && !result.isDone()) {
            cancel(task);
            updateWaitingIncrementalReaders();
        }
    }

    private ThreadPoolExecutor executor(ReaderGroup group) {
        return group == ReaderGroup.JOB_INCREMENTAL ? incrementals : snapshots;
    }

    private void retryWaitingTasks() {
        resources.refreshClosingReaders();
        synchronized (this) {
            if (closed) {
                return;
            }
            submitWaitingTasks(ReaderGroup.JOB_INCREMENTAL);
            submitWaitingTasks(ReaderGroup.JOB_SNAPSHOT);
        }
    }

    private void submitWaitingTasks(ReaderGroup group) {
        for (Task task : new ArrayList<>(waitingTasks.values())) {
            if (task.group != group) {
                continue;
            }
            if (!hasReaderQuota(group)) {
                continue;
            }
            waitingTasks.remove(task.taskId);
            executor(group).execute(task);
        }
    }

    private boolean hasReaderQuota(ReaderGroup group) {
        return resources.getAvailableReaderBytes(group) >= resources.getReaderReservationBytes();
    }

    private void runTask(Task task) {
        boolean retry = false;
        synchronized (this) {
            task.executionThread = Thread.currentThread();
        }
        try {
            if (task.result.isCancelled()) {
                return;
            }
            // Another task may have taken the quota while this retry was in the executor queue.
            if (task.needsQuota && !hasReaderQuota(task.group)) {
                retry = true;
                return;
            }
            task.action.run();
            task.result.complete(null);
        } catch (ReaderQuotaUnavailableException unavailable) {
            retry = true;
            if (!task.needsQuota) {
                LOG.info(
                        "CDC task waiting for reader memory quota, jobId={}, taskId={}, group={}, reason={}",
                        task.jobId,
                        task.taskId,
                        task.group,
                        unavailable.getMessage());
            }
        } catch (Throwable failure) {
            task.result.completeExceptionally(failure);
        } finally {
            synchronized (this) {
                task.executionThread = null;
                if (retry && !task.result.isCancelled() && !closed) {
                    task.needsQuota = true;
                    waitingTasks.put(task.taskId, task);
                } else {
                    tasks.remove(task.taskId, task);
                    waitingTasks.remove(task.taskId, task);
                }
                updateWaitingIncrementalReaders();
            }
        }
    }

    public synchronized void onReaderPrepared(String taskId) {
        Task task = tasks.get(taskId);
        if (task != null) {
            if (task.needsQuota) {
                LOG.info(
                        "CDC task resumed after reader memory quota wait, jobId={}, taskId={}, group={}",
                        task.jobId,
                        task.taskId,
                        task.group);
            }
            task.needsQuota = false;
            waitingTasks.remove(taskId);
            updateWaitingIncrementalReaders();
        }
    }

    public synchronized String getResourceWaitReason(String taskId) {
        Task task = tasks.get(taskId);
        if (task == null || task.result.isCancelled()) {
            return "";
        }
        if (task.needsQuota) {
            return resources.getReaderWaitReason(task.group);
        }
        if (task.executionThread == null) {
            return "Waiting for a CDC " + task.group + " task execution slot";
        }
        return "";
    }

    public synchronized BooleanSupplier getCancellationSignal(String taskId) {
        Task task = tasks.get(taskId);
        return task == null ? () -> closed : task.result::isCancelled;
    }

    public synchronized void cancel(String jobId, String taskId) {
        for (Task task : new ArrayList<>(tasks.values())) {
            if (task.jobId.equals(jobId) && (taskId == null || task.taskId.equals(taskId))) {
                cancel(task);
            }
        }
        updateWaitingIncrementalReaders();
    }

    private void cancel(Task task) {
        if (!task.result.isCancelled() && (task.executionThread == null || task.needsQuota)) {
            LOG.info(
                    "Cancelling waiting CDC task, jobId={}, taskId={}, group={}, reason={}",
                    task.jobId,
                    task.taskId,
                    task.group,
                    getResourceWaitReason(task.taskId));
        }
        task.result.cancel(false);
        if (task.executionThread != null) {
            task.executionThread.interrupt();
        } else if (waitingTasks.remove(task.taskId, task) || executor(task.group).remove(task)) {
            tasks.remove(task.taskId, task);
        }
        // A worker that already dequeued the task will observe cancellation and remove its
        // registration.
    }

    private void updateWaitingIncrementalReaders() {
        int waiting = 0;
        // A resubmitted task retains its priority until its reader is prepared.
        for (Task task : tasks.values()) {
            if (task.needsQuota
                    && task.group == ReaderGroup.JOB_INCREMENTAL
                    && !task.result.isCancelled()) {
                waiting++;
            }
        }
        resources.setWaitingIncrementalReaders(waiting);
    }

    @Override
    public synchronized void close() {
        closed = true;
        for (Task task : new ArrayList<>(tasks.values())) {
            cancel(task);
        }
        tasks.clear();
        waitingTasks.clear();
        resources.setWaitingIncrementalReaders(0);
        waitingTaskRetryExecutor.shutdown();
        snapshots.shutdown();
        incrementals.shutdown();
    }
}
