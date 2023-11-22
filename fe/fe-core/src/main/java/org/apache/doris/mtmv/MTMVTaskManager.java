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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.CustomThreadFactory;
import org.apache.doris.mtmv.MTMVUtils.TaskState;
import org.apache.doris.mtmv.metadata.MTMVJob;
import org.apache.doris.mtmv.metadata.MTMVTask;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class MTMVTaskManager {

    private static final Logger LOG = LogManager.getLogger(MTMVTaskManager.class);

    // jobId -> pending tasks, one job can dispatch many tasks
    private final Map<Long, PriorityBlockingQueue<MTMVTaskExecutor>> pendingTaskMap = Maps.newConcurrentMap();

    // jobId -> running tasks, only one task will be running for one job.
    private final Map<Long, MTMVTaskExecutor> runningTaskMap = Maps.newConcurrentMap();

    private final MTMVTaskExecutorPool taskExecutorPool = new MTMVTaskExecutorPool();

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(true);

    // keep track of all the completed tasks
    private final Deque<MTMVTask> historyTasks = Queues.newLinkedBlockingDeque();

    private ScheduledExecutorService taskScheduler = Executors.newScheduledThreadPool(1,
            new CustomThreadFactory("mtmv-task-scheduler"));

    private final AtomicInteger failedTaskCount = new AtomicInteger(0);

    public void startTaskScheduler() {
        if (taskScheduler.isShutdown()) {
            taskScheduler = Executors.newScheduledThreadPool(1, new CustomThreadFactory("mtmv-task-scheduler"));
        }
        taskScheduler.scheduleAtFixedRate(() -> {
            checkRunningTask();
            scheduledPendingTask();

        }, 0, 1, TimeUnit.SECONDS);
    }

    public void stopTaskScheduler() {
        taskScheduler.shutdown();
    }

    private void checkRunningTask() {
        writeLock();
        try {
            Iterator<Long> runningIterator = runningTaskMap.keySet().iterator();
            while (runningIterator.hasNext()) {
                Long jobId = runningIterator.next();
                MTMVTaskExecutor taskExecutor = runningTaskMap.get(jobId);
                Future<?> future = taskExecutor.getFuture();
                if (future.isDone()) {
                    runningIterator.remove();
                    addHistory(taskExecutor.getTask());
                    MTMVUtils.TaskState finalState = taskExecutor.getTask().getState();
                    if (finalState == TaskState.FAILURE) {
                        failedTaskCount.incrementAndGet();
                    }
                    //task save final state only
                    Env.getCurrentEnv().getEditLog().logCreateMTMVTask(taskExecutor.getTask());
                    taskExecutor.getJob().taskFinished();
                }
            }
        } finally {
            writeUnlock();
        }
    }

    private void scheduledPendingTask() {
        writeLock();
        try {
            int currentRunning = runningTaskMap.size();

            Iterator<Long> pendingIterator = pendingTaskMap.keySet().iterator();
            while (pendingIterator.hasNext()) {
                Long jobId = pendingIterator.next();
                MTMVTaskExecutor runningTaskExecutor = runningTaskMap.get(jobId);
                if (runningTaskExecutor == null) {
                    Queue<MTMVTaskExecutor> taskQueue = pendingTaskMap.get(jobId);
                    if (taskQueue.size() == 0) {
                        pendingIterator.remove();
                    } else {
                        if (currentRunning >= Config.max_running_mtmv_scheduler_task_num) {
                            break;
                        }
                        MTMVTaskExecutor pendingTaskExecutor = taskQueue.poll();
                        taskExecutorPool.executeTask(pendingTaskExecutor);
                        runningTaskMap.put(jobId, pendingTaskExecutor);
                        currentRunning++;
                    }
                }
            }
        } finally {
            writeUnlock();
        }
    }

    public MTMVUtils.TaskSubmitStatus submitJobTask(MTMVJob job) {
        return submitJobTask(job, new MTMVTaskExecuteParams());
    }

    private MTMVUtils.TaskSubmitStatus submitJobTask(MTMVJob job, MTMVTaskExecuteParams param) {
        return submitTask(MTMVUtils.buildTask(job), param);
    }

    private MTMVUtils.TaskSubmitStatus submitTask(MTMVTaskExecutor taskExecutor, MTMVTaskExecuteParams params) {
        // duplicate submit
        if (taskExecutor.getTask() != null) {
            return MTMVUtils.TaskSubmitStatus.FAILED;
        }

        int validPendingCount = 0;
        for (Long jobId : pendingTaskMap.keySet()) {
            if (!pendingTaskMap.get(jobId).isEmpty()) {
                validPendingCount++;
            }
        }

        if (validPendingCount >= Config.max_pending_mtmv_scheduler_task_num) {
            LOG.warn("pending task exceeds pending_scheduler_task_size:{}, reject the submit.",
                    Config.max_pending_mtmv_scheduler_task_num);
            return MTMVUtils.TaskSubmitStatus.REJECTED;
        }

        String taskId = UUID.randomUUID().toString();
        MTMVTask task = taskExecutor.initTask(taskId, MTMVUtils.getNowTimeStamp());
        task.setPriority(params.getPriority());
        LOG.info("Submit a mtmv task with id: {} of the job {}.", taskId, taskExecutor.getJob().getName());
        arrangeToPendingTask(taskExecutor);
        return MTMVUtils.TaskSubmitStatus.SUBMITTED;
    }

    private void arrangeToPendingTask(MTMVTaskExecutor task) {
        writeLock();
        try {
            long jobId = task.getJobId();
            PriorityBlockingQueue<MTMVTaskExecutor> tasks =
                    pendingTaskMap.computeIfAbsent(jobId, u -> Queues.newPriorityBlockingQueue());
            tasks.offer(task);
        } finally {
            writeUnlock();
        }
    }

    public void dealJobRemoved(MTMVJob job) {
        removePendingTask(job.getId());
        removeRunningTask(job.getId());
        if (!Config.keep_scheduler_mtmv_task_when_job_deleted) {
            clearHistoryTasksByJobName(job.getName(), false);
        }
    }

    private void removePendingTask(Long jobId) {
        pendingTaskMap.remove(jobId);
    }

    private void removeRunningTask(Long jobId) {
        MTMVTaskExecutor task = runningTaskMap.remove(jobId);
        if (task != null) {
            task.stop();
        }
    }

    public int getFailedTaskCount() {
        return failedTaskCount.get();
    }

    public Map<Long, PriorityBlockingQueue<MTMVTaskExecutor>> getPendingTaskMap() {
        return pendingTaskMap;
    }

    public Map<Long, MTMVTaskExecutor> getRunningTaskMap() {
        return runningTaskMap;
    }

    private void addHistory(MTMVTask task) {
        historyTasks.addFirst(task);
    }

    public Deque<MTMVTask> getHistoryTasks() {
        return historyTasks;
    }

    public List<MTMVTask> getHistoryTasksByJobName(String jobName) {
        return getHistoryTasks().stream().filter(u -> u.getJobName().equals(jobName))
                .collect(Collectors.toList());
    }

    public List<MTMVTask> showAllTasks() {
        return showTasksWithLock(null);
    }

    public List<MTMVTask> showTasksWithLock(String dbName) {
        List<MTMVTask> taskList = Lists.newArrayList();
        readLock();
        try {
            if (Strings.isNullOrEmpty(dbName)) {
                for (Queue<MTMVTaskExecutor> pTaskQueue : getPendingTaskMap().values()) {
                    taskList.addAll(pTaskQueue.stream().map(MTMVTaskExecutor::getTask).collect(Collectors.toList()));
                }
                taskList.addAll(
                        getRunningTaskMap().values().stream().map(MTMVTaskExecutor::getTask)
                                .collect(Collectors.toList()));
                taskList.addAll(getHistoryTasks());
            } else {
                for (Queue<MTMVTaskExecutor> pTaskQueue : getPendingTaskMap().values()) {
                    taskList.addAll(
                            pTaskQueue.stream().map(MTMVTaskExecutor::getTask).filter(u -> u.getDBName().equals(dbName))
                                    .collect(Collectors.toList()));
                }
                taskList.addAll(getRunningTaskMap().values().stream().map(MTMVTaskExecutor::getTask)
                        .filter(u -> u.getDBName().equals(dbName)).collect(Collectors.toList()));
                taskList.addAll(
                        getHistoryTasks().stream().filter(u -> u.getDBName().equals(dbName))
                                .collect(Collectors.toList()));

            }
        } finally {
            readUnlock();
        }

        return taskList.stream().sorted().collect(Collectors.toList());
    }

    public List<MTMVTask> showTasks(String dbName, String mvName) {
        return showTasksWithLock(dbName).stream().filter(u -> u.getMVName().equals(mvName))
                .collect(Collectors.toList());
    }

    public MTMVTask getTask(String taskId) throws AnalysisException {
        List<MTMVTask> tasks =
                showAllTasks().stream().filter(u -> u.getTaskId().equals(taskId)).collect(Collectors.toList());
        if (tasks.size() == 0) {
            throw new AnalysisException("Can't find the task id in the task list.");
        } else if (tasks.size() > 1) {
            throw new AnalysisException("Find more than one task id in the task list.");
        } else {
            return tasks.get(0);
        }
    }

    public void replayCreateJobTask(MTMVTask task) {
        addHistory(task);
    }

    private void clearHistoryTasksByJobName(String jobName, boolean isReplay) {
        List<String> clearTasks = Lists.newArrayList();

        Deque<MTMVTask> taskHistory = getHistoryTasks();
        for (MTMVTask task : taskHistory) {
            if (task.getJobName().equals(jobName)) {
                clearTasks.add(task.getTaskId());
            }
        }

        dropHistoryTasks(clearTasks, isReplay);
    }

    public void removeExpiredTasks() {
        long currentTime = MTMVUtils.getNowTimeStamp();
        List<String> historyToDelete = Lists.newArrayList();
        Deque<MTMVTask> taskHistory = getHistoryTasks();
        for (MTMVTask task : taskHistory) {
            long expireTime = task.getExpireTime();
            if (currentTime > expireTime) {
                historyToDelete.add(task.getTaskId());
            }
        }
        dropHistoryTasks(historyToDelete, false);
    }

    public void dropHistoryTasks(List<String> taskIds, boolean isReplay) {
        if (taskIds.isEmpty()) {
            return;
        }
        writeLock();
        try {
            Set<String> taskSet = new HashSet<>(taskIds);
            // Try to remove history tasks.
            getHistoryTasks().removeIf(mtmvTask -> taskSet.contains(mtmvTask.getTaskId()));
            if (!isReplay) {
                Env.getCurrentEnv().getEditLog().logDropMTMVTasks(taskIds);
            }
        } finally {
            writeUnlock();
        }
        LOG.info("drop task history:{}", taskIds);
    }

    private void readLock() {
        this.rwLock.readLock().lock();
    }

    private void readUnlock() {
        this.rwLock.readLock().unlock();
    }

    private void writeLock() {
        this.rwLock.writeLock().lock();
    }

    private void writeUnlock() {
        this.rwLock.writeLock().unlock();
    }
}
