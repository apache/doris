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
import org.apache.doris.common.Config;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.scheduler.Utils.JobState;
import org.apache.doris.scheduler.Utils.TaskState;
import org.apache.doris.scheduler.metadata.ChangeJob;
import org.apache.doris.scheduler.metadata.ChangeTask;
import org.apache.doris.scheduler.metadata.Job;
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

    // jobId -> pending tasks, one job can dispatch many tasks
    private final Map<Long, PriorityBlockingQueue<TaskExecutor>> pendingTaskMap = Maps.newConcurrentMap();

    // jobId -> running tasks, only one task will be running for one job.
    private final Map<Long, TaskExecutor> runningTaskMap = Maps.newConcurrentMap();

    private final TaskExecutorPool taskExecutorPool = new TaskExecutorPool();

    private final ReentrantLock reentrantLock = new ReentrantLock(true);

    // keep track of all the tasks
    private final Deque<Task> historyQueue = Queues.newLinkedBlockingDeque();

    private final ScheduledExecutorService taskScheduler = Executors.newScheduledThreadPool(1);

    private final JobManager jobManager;

    public TaskManager(JobManager jobManager) {
        this.jobManager = jobManager;
    }

    public void startTaskScheduler() {
        taskScheduler.scheduleAtFixedRate(() -> {
            if (!tryLock()) {
                return;
            }
            try {
                checkRunningTask();
                scheduledPendingTask();
            } catch (Exception ex) {
                LOG.warn("failed to schedule task.", ex);
            } finally {
                unlock();
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    public SubmitResult submitTask(TaskExecutor taskExecutor, ExecuteOption option) {
        // duplicate submit
        if (taskExecutor.getTask() != null) {
            return new SubmitResult(taskExecutor.getTask().getTaskId(), SubmitResult.SubmitStatus.FAILED);
        }

        int validPendingCount = 0;
        for (Long jobId : pendingTaskMap.keySet()) {
            if (!pendingTaskMap.get(jobId).isEmpty()) {
                validPendingCount++;
            }
        }

        if (validPendingCount >= Config.pending_scheduler_task_size) {
            LOG.warn("pending task exceeds pending_scheduler_task_size:{}, reject the submit.",
                    Config.pending_scheduler_task_size);
            return new SubmitResult(null, SubmitResult.SubmitStatus.REJECTED);
        }

        String taskId = UUID.randomUUID().toString();
        Task task = taskExecutor.initTask(taskId, System.currentTimeMillis());
        task.setPriority(option.getPriority());
        task.setMarkJobFinished(option.isMarkJobFinished());
        Env.getCurrentEnv().getEditLog().logCreateScheduleTask(task);
        arrangeToPendingTask(taskExecutor);
        return new SubmitResult(taskId, SubmitResult.SubmitStatus.SUBMITTED);
    }

    public boolean killTask(Long jobId, boolean clearPending) {
        if (clearPending) {
            if (!tryLock()) {
                return false;
            }
            try {
                getPendingTaskMap().remove(jobId);
            } catch (Exception ex) {
                LOG.warn("failed to kill task.", ex);
            } finally {
                unlock();
            }
        }
        TaskExecutor task = runningTaskMap.get(jobId);
        if (task == null) {
            return false;
        }
        ConnectContext connectContext = task.getCtx();
        if (connectContext != null) {
            connectContext.kill(false);
            return true;
        }
        return false;
    }

    public void arrangeToPendingTask(TaskExecutor task) {
        if (!tryLock()) {
            return;
        }
        try {
            long jobId = task.getJobId();
            PriorityBlockingQueue<TaskExecutor> tasks =
                    pendingTaskMap.computeIfAbsent(jobId, u -> Queues.newPriorityBlockingQueue());
            tasks.offer(task);
        } finally {
            unlock();
        }
    }

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

    private void checkRunningTask() {
        Iterator<Long> runningIterator = runningTaskMap.keySet().iterator();
        while (runningIterator.hasNext()) {
            Long jobId = runningIterator.next();
            TaskExecutor taskExecutor = runningTaskMap.get(jobId);
            if (taskExecutor == null) {
                LOG.warn("failed to get running task by jobId:{}", jobId);
                runningIterator.remove();
                return;
            }
            Future<?> future = taskExecutor.getFuture();
            if (future.isDone()) {
                runningIterator.remove();
                addHistory(taskExecutor.getTask());
                changeAndLogTaskStatus(taskExecutor.getJobId(), taskExecutor.getTask(), TaskState.RUNNING,
                        taskExecutor.getTask().getState());

                // update the run once job status
                if (taskExecutor.getTask().isMarkJobFinished()) {
                    ChangeJob changeJob = new ChangeJob(taskExecutor.getJobId(), JobState.COMPLETE);
                    jobManager.updateJob(changeJob, false);
                }
            }
        }
    }

    private void scheduledPendingTask() {
        int currentRunning = runningTaskMap.size();

        Iterator<Long> pendingIterator = pendingTaskMap.keySet().iterator();
        while (pendingIterator.hasNext()) {
            Long jobId = pendingIterator.next();
            TaskExecutor runningTaskExecutor = runningTaskMap.get(jobId);
            if (runningTaskExecutor == null) {
                Queue<TaskExecutor> taskQueue = pendingTaskMap.get(jobId);
                if (taskQueue.size() == 0) {
                    pendingIterator.remove();
                } else {
                    if (currentRunning >= Config.running_scheduler_task_size) {
                        break;
                    }
                    TaskExecutor pendingTaskExecutor = taskQueue.poll();
                    taskExecutorPool.executeTask(pendingTaskExecutor);
                    runningTaskMap.put(jobId, pendingTaskExecutor);
                    // change status from PENDING to Running
                    changeAndLogTaskStatus(jobId, pendingTaskExecutor.getTask(), TaskState.PENDING, TaskState.RUNNING);
                    currentRunning++;
                }
            }
        }
    }

    private void changeAndLogTaskStatus(long jobId, Task task, TaskState fromStatus, TaskState toStatus) {
        ChangeTask changeTask = new ChangeTask(jobId, task, fromStatus, toStatus);
        Env.getCurrentEnv().getEditLog().logAlterScheduleTask(changeTask);
    }

    public boolean tryLock() {
        try {
            return reentrantLock.tryLock(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.warn("got exception while getting task lock", e);
            Thread.currentThread().interrupt();
        }
        return false;
    }


    public void unlock() {
        this.reentrantLock.unlock();
    }

    public Map<Long, PriorityBlockingQueue<TaskExecutor>> getPendingTaskMap() {
        return pendingTaskMap;
    }

    public Map<Long, TaskExecutor> getRunningTaskMap() {
        return runningTaskMap;
    }

    public void addHistory(Task task) {
        historyQueue.addFirst(task);
    }

    public Deque<Task> getAllHistory() {
        return historyQueue;
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

    public void replayCreateJobTask(Task task) {
        if (task.getState() == TaskState.SUCCESS || task.getState() == TaskState.FAILED) {
            if (System.currentTimeMillis() > task.getExpireTime()) {
                return;
            }
        }

        switch (task.getState()) {
            case PENDING:
                String jobName = task.getJobName();
                Job job = jobManager.getJob(jobName);
                if (job == null) {
                    LOG.warn("fail to obtain task name {} because task is null", jobName);
                    return;
                }
                TaskExecutor taskExecutor = Utils.buildTask(job);
                taskExecutor.initTask(task.getTaskId(), task.getCreateTime());
                arrangeToPendingTask(taskExecutor);
                break;
            case RUNNING:
                task.setState(TaskState.FAILED);
                addHistory(task);
                break;
            case FAILED:
            case SUCCESS:
                addHistory(task);
                break;
            default:
                break;
        }
    }

    public void replayUpdateTask(ChangeTask changeTask) {
        TaskState fromStatus = changeTask.getFromStatus();
        TaskState toStatus = changeTask.getToStatus();
        Long jobId = changeTask.getJobId();
        if (fromStatus == TaskState.PENDING) {
            Queue<TaskExecutor> taskQueue = getPendingTaskMap().get(jobId);
            if (taskQueue == null) {
                return;
            }
            if (taskQueue.size() == 0) {
                getPendingTaskMap().remove(jobId);
                return;
            }

            TaskExecutor pendingTask = taskQueue.poll();
            Task status = pendingTask.getTask();

            if (toStatus == TaskState.RUNNING) {
                if (status.getTaskId().equals(changeTask.getTaskId())) {
                    status.setState(TaskState.RUNNING);
                    getRunningTaskMap().put(jobId, pendingTask);
                }
            } else if (toStatus == TaskState.FAILED) {
                status.setErrorMessage(changeTask.getErrorMessage());
                status.setErrorCode(changeTask.getErrorCode());
                status.setState(TaskState.FAILED);
                addHistory(status);
            }
            if (taskQueue.size() == 0) {
                getPendingTaskMap().remove(jobId);
            }
        } else if (fromStatus == TaskState.RUNNING && (toStatus == TaskState.SUCCESS || toStatus == TaskState.FAILED)) {
            TaskExecutor runningTask = getRunningTaskMap().remove(jobId);
            if (runningTask == null) {
                return;
            }
            Task status = runningTask.getTask();
            if (status.getTaskId().equals(changeTask.getTaskId())) {
                if (toStatus == TaskState.FAILED) {
                    status.setErrorMessage(changeTask.getErrorMessage());
                    status.setErrorCode(changeTask.getErrorCode());
                }
                status.setState(toStatus);
                status.setFinishTime(changeTask.getFinishTime());
                addHistory(status);
            }
        } else {
            LOG.warn("Illegal  Task queryId:{} status transform from {} to {}", changeTask.getTaskId(), fromStatus,
                    toStatus);
        }
    }

    public void removeExpiredTasks() {
        long currentTimeMs = System.currentTimeMillis();

        List<String> historyToDelete = Lists.newArrayList();

        if (!tryLock()) {
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
            unlock();
        }
        LOG.info("remove task history:{}", historyToDelete);
    }

    public void clearUnfinishedTasks() {
        if (!tryLock()) {
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
                    taskExecutor.getTask().setState(TaskState.FAILED);
                    addHistory(taskExecutor.getTask());
                    changeAndLogTaskStatus(taskExecutor.getJobId(), taskExecutor.getTask(), TaskState.PENDING,
                            TaskState.FAILED);
                }
                pendingIter.remove();
            }
            Iterator<Long> runningIter = getRunningTaskMap().keySet().iterator();
            while (runningIter.hasNext()) {
                TaskExecutor taskExecutor = getRunningTaskMap().get(runningIter.next());
                taskExecutor.getTask().setErrorMessage("Fe abort the task");
                taskExecutor.getTask().setErrorCode(-1);
                taskExecutor.getTask().setState(TaskState.FAILED);
                taskExecutor.getTask().setFinishTime(System.currentTimeMillis());
                runningIter.remove();
                addHistory(taskExecutor.getTask());
                changeAndLogTaskStatus(taskExecutor.getJobId(), taskExecutor.getTask(), TaskState.RUNNING,
                        TaskState.FAILED);
            }
        } finally {
            unlock();
        }
    }
}
