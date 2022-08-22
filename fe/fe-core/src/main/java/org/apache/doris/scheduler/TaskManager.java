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

package org.apache.doris.scheduler;

import org.apache.doris.catalog.Env;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.scheduler.metadata.ChangeTask;
import org.apache.doris.scheduler.metadata.Task;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class TaskManager {

    private static final Logger LOG = LogManager.getLogger(TaskManager.class);

    // taskId -> pending Task Queue, for each Task only support 1 running task currently,
    // so the map value is priority queue need to be sorted by priority from large to small
    private final Map<Long, PriorityBlockingQueue<TaskExecutor>> pendingTaskMap = Maps.newConcurrentMap();

    // taskId -> running task, for each Task only support 1 running task currently,
    // so the map value is not queue
    private final Map<Long, TaskExecutor> runningTaskMap = Maps.newConcurrentMap();

    // Use to execute actual task
    private final TaskExecutorPool taskExecutorPool = new TaskExecutorPool();

    private final ReentrantLock taskLock = new ReentrantLock(true);

    // keep track of all the SUCCESS/FAILED/CANCEL task records
    private final Deque<Task> historyDeque = Queues.newLinkedBlockingDeque();

    private final ScheduledExecutorService taskDispatcher = Executors.newScheduledThreadPool(1);

    public void startTaskDispatcher() {
        taskDispatcher.scheduleAtFixedRate(() -> {
            if (!tryTaskLock()) {
                return;
            }
            try {
                checkRunningTask();
                scheduledPendingTask();
            } catch (Exception ex) {
                LOG.warn("failed to dispatch task.", ex);
            } finally {
                taskUnlock();
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    public SubmitResult submitTask(TaskExecutor taskExecutor, ExecuteOption option) {
        // duplicate submit
        if (taskExecutor.getTask() != null) {
            return new SubmitResult(taskExecutor.getTask().getTaskId(), SubmitResult.SubmitStatus.FAILED);
        }

        int validPendingCount = 0;
        for (Long taskId : pendingTaskMap.keySet()) {
            if (!pendingTaskMap.get(taskId).isEmpty()) {
                validPendingCount++;
            }
        }

        if (validPendingCount >= 1000) {
            LOG.warn("pending task exceeds task_runs_queue_length:{}, reject the submit.", 1000);
            return new SubmitResult(null, SubmitResult.SubmitStatus.REJECTED);
        }

        String queryId = UUID.randomUUID().toString();
        Task task = taskExecutor.initRecord(queryId, System.currentTimeMillis());
        task.setPriority(option.getPriority());
        task.setMergeRedundant(option.isMergeRedundant());
        Env.getCurrentEnv().getEditLog().logCreateScheduleTask(task);
        arrangeTask(taskExecutor, option.isMergeRedundant());
        return new SubmitResult(queryId, SubmitResult.SubmitStatus.SUBMITTED);
    }


    public boolean killTask(Long taskId, boolean clearPending) {
        if (clearPending) {
            if (!tryTaskLock()) {
                return false;
            }
            try {
                getPendingTaskMap().remove(taskId);
            } catch (Exception ex) {
                LOG.warn("failed to kill task.", ex);
            } finally {
                taskUnlock();
            }
        }
        TaskExecutor task = runningTaskMap.get(taskId);
        if (task == null) {
            return false;
        }
        ConnectContext runCtx = task.getCtx();
        if (runCtx != null) {
            runCtx.kill(false);
            return true;
        }
        return false;
    }

    // At present, only the manual and automatic tasks of the materialized view have different priorities.
    // The manual priority is higher. For manual tasks, we do not merge operations.
    // For automatic tasks, we will compare the definition, and if they are the same,
    // we will perform the merge operation.
    public void arrangeTask(TaskExecutor task, boolean mergeRedundant) {
        if (!tryTaskLock()) {
            return;
        }
        try {
            long taskId = task.getTaskId();
            PriorityBlockingQueue<TaskExecutor> tasks =
                    pendingTaskMap.computeIfAbsent(taskId, u -> Queues.newPriorityBlockingQueue());
            if (mergeRedundant) {
                TaskExecutor oldTask = getTask(tasks, task);
                if (oldTask != null) {
                    // The remove here is actually remove the old task.
                    // Note that the old task and new task may have the same definition,
                    // but other attributes may be different, such as priority, creation time.
                    // higher priority and create time will be result after merge is complete
                    // and queryId will be change.
                    boolean isRemove = tasks.remove(task);
                    if (!isRemove) {
                        LOG.warn("failed to remove task definition is [{}]", task.getTask().getDefinition());
                    }
                    if (oldTask.getTask().getPriority() > task.getTask().getPriority()) {
                        task.getTask().setPriority(oldTask.getTask().getPriority());
                    }
                    if (oldTask.getTask().getCreateTime() > task.getTask().getCreateTime()) {
                        task.getTask().setCreateTime(oldTask.getTask().getCreateTime());
                    }
                }
            }
            tasks.offer(task);
        } finally {
            taskUnlock();
        }
    }

    // Because java PriorityQueue does not provide an interface for searching by element,
    // so find it by code O(n), which can be optimized later
    @Nullable
    private TaskExecutor getTask(PriorityBlockingQueue<TaskExecutor> tasks, TaskExecutor task) {
        TaskExecutor oldTask = null;
        for (TaskExecutor t : tasks) {
            if (t.equals(task)) {
                oldTask = t;
                break;
            }
        }
        return oldTask;
    }

    // check if a running Task is complete and remove it from running task map
    public void checkRunningTask() {
        Iterator<Long> runningIterator = runningTaskMap.keySet().iterator();
        while (runningIterator.hasNext()) {
            Long taskId = runningIterator.next();
            TaskExecutor task = runningTaskMap.get(taskId);
            if (task == null) {
                LOG.warn("failed to get running Task by taskId:{}", taskId);
                runningIterator.remove();
                return;
            }
            Future<?> future = task.getFuture();
            if (future.isDone()) {
                runningIterator.remove();
                addHistory(task.getTask());
                ChangeTask changeTask = new ChangeTask(task.getTaskId(), task.getTask(), Utils.TaskState.RUNNING,
                        task.getTask().getState());
                Env.getCurrentEnv().getEditLog().logAlterScheduleTask(changeTask);
            }
        }
    }

    // schedule the pending task that can be run into running task map
    public void scheduledPendingTask() {
        int currentRunning = runningTaskMap.size();

        Iterator<Long> pendingIterator = pendingTaskMap.keySet().iterator();
        while (pendingIterator.hasNext()) {
            Long taskId = pendingIterator.next();
            TaskExecutor runningTaskExecutor = runningTaskMap.get(taskId);
            if (runningTaskExecutor == null) {
                Queue<TaskExecutor> taskQueue = pendingTaskMap.get(taskId);
                if (taskQueue.size() == 0) {
                    pendingIterator.remove();
                } else {
                    if (currentRunning >= 1000) {
                        break;
                    }
                    TaskExecutor pendingTaskExecutor = taskQueue.poll();
                    taskExecutorPool.executeTask(pendingTaskExecutor);
                    runningTaskMap.put(taskId, pendingTaskExecutor);
                    // RUNNING state persistence is for FE FOLLOWER update state
                    ChangeTask changeTask =
                            new ChangeTask(taskId, pendingTaskExecutor.getTask(), Utils.TaskState.PENDING,
                                    Utils.TaskState.RUNNING);
                    Env.getCurrentEnv().getEditLog().logAlterScheduleTask(changeTask);
                    currentRunning++;
                }
            }
        }
    }

    public boolean tryTaskLock() {
        try {
            return taskLock.tryLock(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.warn("got exception while getting task run lock", e);
            Thread.currentThread().interrupt();
        }
        return false;
    }


    public void taskUnlock() {
        this.taskLock.unlock();
    }

    public Map<Long, PriorityBlockingQueue<TaskExecutor>> getPendingTaskMap() {
        return pendingTaskMap;
    }

    public Map<Long, TaskExecutor> getRunningTaskMap() {
        return runningTaskMap;
    }

    public void addHistory(Task task) {
        historyDeque.addFirst(task);
    }

    public Deque<Task> getAllHistory() {
        return historyDeque;
    }

    public List<Task> showTasks(String dbName) {
        List<Task> taskList = Lists.newArrayList();
        if (dbName == null) {
            for (Queue<TaskExecutor> pTaskQueue : getPendingTaskMap().values()) {
                taskList.addAll(pTaskQueue.stream().map(TaskExecutor::getTask).collect(Collectors.toList()));
            }
            taskList.addAll(
                    getRunningTaskMap().values().stream().map(TaskExecutor::getTask).collect(Collectors.toList()));
            taskList.addAll(getAllHistory());
        } else {
            for (Queue<TaskExecutor> pTaskQueue : getPendingTaskMap().values()) {
                taskList.addAll(pTaskQueue.stream().map(TaskExecutor::getTask).filter(u -> u.getDbName().equals(dbName))
                        .collect(Collectors.toList()));
            }
            taskList.addAll(getRunningTaskMap().values().stream().map(TaskExecutor::getTask)
                    .filter(u -> u.getDbName().equals(dbName)).collect(Collectors.toList()));
            taskList.addAll(
                    getAllHistory().stream().filter(u -> u.getDbName().equals(dbName)).collect(Collectors.toList()));

        }
        return taskList;
    }

    public void removeExpiredTasks() {
        long currentTimeMs = System.currentTimeMillis();

        List<String> historyToDelete = Lists.newArrayList();

        if (!tryTaskLock()) {
            return;
        }
        try {
            Deque<Task> taskHistory = getAllHistory();
            Iterator<Task> iterator = taskHistory.iterator();
            while (iterator.hasNext()) {
                Task task = iterator.next();
                long expireTime = task.getExpireTime();
                if (currentTimeMs > expireTime) {
                    historyToDelete.add(task.getTaskId());
                    iterator.remove();
                }
            }
        } finally {
            taskUnlock();
        }
        LOG.info("remove run history:{}", historyToDelete);
    }

    public void clearUnfinishedTasks() {
        if (!tryTaskLock()) {
            return;
        }
        try {
            Iterator<Long> pendingIter = getPendingTaskMap().keySet().iterator();
            while (pendingIter.hasNext()) {
                Queue<TaskExecutor> tasks = getPendingTaskMap().get(pendingIter.next());
                while (!tasks.isEmpty()) {
                    TaskExecutor taskExecutor = tasks.poll();
                    taskExecutor.getTask().setErrorMessage("Fe abort the task");
                    taskExecutor.getTask().setErrorCode(-1);
                    taskExecutor.getTask().setState(Utils.TaskState.FAILED);
                    addHistory(taskExecutor.getTask());
                    ChangeTask changeTask =
                            new ChangeTask(taskExecutor.getTaskId(), taskExecutor.getTask(), Utils.TaskState.PENDING,
                                    Utils.TaskState.FAILED);
                    Env.getCurrentEnv().getEditLog().logAlterScheduleTask(changeTask);
                }
                pendingIter.remove();
            }
            Iterator<Long> runningIter = getRunningTaskMap().keySet().iterator();
            while (runningIter.hasNext()) {
                TaskExecutor taskExecutor = getRunningTaskMap().get(runningIter.next());
                taskExecutor.getTask().setErrorMessage("Fe abort the task");
                taskExecutor.getTask().setErrorCode(-1);
                taskExecutor.getTask().setState(Utils.TaskState.FAILED);
                taskExecutor.getTask().setFinishTime(System.currentTimeMillis());
                runningIter.remove();
                addHistory(taskExecutor.getTask());
                ChangeTask changeTask =
                        new ChangeTask(taskExecutor.getTaskId(), taskExecutor.getTask(), Utils.TaskState.RUNNING,
                                Utils.TaskState.FAILED);
                Env.getCurrentEnv().getEditLog().logAlterScheduleTask(changeTask);
            }
        } finally {
            taskUnlock();
        }
    }
}
