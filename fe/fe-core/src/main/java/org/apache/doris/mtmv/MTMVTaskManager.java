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
import org.apache.doris.common.Config;
import org.apache.doris.mtmv.MTMVUtils.JobState;
import org.apache.doris.mtmv.MTMVUtils.TaskState;
import org.apache.doris.mtmv.MTMVUtils.TriggerMode;
import org.apache.doris.mtmv.metadata.AlterMTMVTask;
import org.apache.doris.mtmv.metadata.ChangeMTMVJob;
import org.apache.doris.mtmv.metadata.MTMVJob;
import org.apache.doris.mtmv.metadata.MTMVTask;
import org.apache.doris.qe.ConnectContext;

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

public class MTMVTaskManager {

    private static final Logger LOG = LogManager.getLogger(MTMVTaskManager.class);

    // jobId -> pending tasks, one job can dispatch many tasks
    private final Map<Long, PriorityBlockingQueue<MTMVTaskExecutor>> pendingTaskMap = Maps.newConcurrentMap();

    // jobId -> running tasks, only one task will be running for one job.
    private final Map<Long, MTMVTaskExecutor> runningTaskMap = Maps.newConcurrentMap();

    private final MTMVTaskExecutorPool taskExecutorPool = new MTMVTaskExecutorPool();

    private final ReentrantLock reentrantLock = new ReentrantLock(true);

    // keep track of all the tasks
    private final Deque<MTMVTask> historyQueue = Queues.newLinkedBlockingDeque();

    private final ScheduledExecutorService taskScheduler = Executors.newScheduledThreadPool(1);

    private final MTMVJobManager mtmvJobManager;

    public MTMVTaskManager(MTMVJobManager mtmvJobManager) {
        this.mtmvJobManager = mtmvJobManager;
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

    public MTMVUtils.TaskSubmitStatus submitTask(MTMVTaskExecutor taskExecutor, MTMVTaskExecuteParams params) {
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
        MTMVTask task = taskExecutor.initTask(taskId, System.currentTimeMillis());
        task.setPriority(params.getPriority());
        Env.getCurrentEnv().getEditLog().logCreateScheduleTask(task);
        arrangeToPendingTask(taskExecutor);
        return MTMVUtils.TaskSubmitStatus.SUBMITTED;
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
        MTMVTaskExecutor task = runningTaskMap.get(jobId);
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

    public void arrangeToPendingTask(MTMVTaskExecutor task) {
        if (!tryLock()) {
            return;
        }
        try {
            long jobId = task.getJobId();
            PriorityBlockingQueue<MTMVTaskExecutor> tasks =
                    pendingTaskMap.computeIfAbsent(jobId, u -> Queues.newPriorityBlockingQueue());
            tasks.offer(task);
        } finally {
            unlock();
        }
    }

    @Nullable
    private MTMVTaskExecutor getTask(PriorityBlockingQueue<MTMVTaskExecutor> tasks, MTMVTaskExecutor task) {
        MTMVTaskExecutor oldTask = null;
        for (MTMVTaskExecutor t : tasks) {
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
            MTMVTaskExecutor taskExecutor = runningTaskMap.get(jobId);
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

                TriggerMode triggerMode = taskExecutor.getJob().getTriggerMode();
                if (triggerMode == TriggerMode.ONCE) {
                    // update the run once job status
                    ChangeMTMVJob changeJob = new ChangeMTMVJob(taskExecutor.getJobId(), JobState.COMPLETE);
                    mtmvJobManager.updateJob(changeJob, false);
                } else if (triggerMode == TriggerMode.PERIODICAL) {
                    // just update the last modify time.
                    ChangeMTMVJob changeJob = new ChangeMTMVJob(taskExecutor.getJobId(), JobState.ACTIVE);
                    mtmvJobManager.updateJob(changeJob, false);
                }
            }
        }
    }

    private void scheduledPendingTask() {
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
                    // change status from PENDING to Running
                    changeAndLogTaskStatus(jobId, pendingTaskExecutor.getTask(), TaskState.PENDING, TaskState.RUNNING);
                    currentRunning++;
                }
            }
        }
    }

    private void changeAndLogTaskStatus(long jobId, MTMVTask task, TaskState fromStatus, TaskState toStatus) {
        AlterMTMVTask changeTask = new AlterMTMVTask(jobId, task, fromStatus, toStatus);
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

    public Map<Long, PriorityBlockingQueue<MTMVTaskExecutor>> getPendingTaskMap() {
        return pendingTaskMap;
    }

    public Map<Long, MTMVTaskExecutor> getRunningTaskMap() {
        return runningTaskMap;
    }

    public void addHistory(MTMVTask task) {
        historyQueue.addFirst(task);
    }

    public Deque<MTMVTask> getAllHistory() {
        return historyQueue;
    }

    public List<MTMVTask> showTasks(String dbName) {
        List<MTMVTask> taskList = Lists.newArrayList();
        if (dbName == null) {
            for (Queue<MTMVTaskExecutor> pTaskQueue : getPendingTaskMap().values()) {
                taskList.addAll(pTaskQueue.stream().map(MTMVTaskExecutor::getTask).collect(Collectors.toList()));
            }
            taskList.addAll(
                    getRunningTaskMap().values().stream().map(MTMVTaskExecutor::getTask).collect(Collectors.toList()));
            taskList.addAll(getAllHistory());
        } else {
            for (Queue<MTMVTaskExecutor> pTaskQueue : getPendingTaskMap().values()) {
                taskList.addAll(
                        pTaskQueue.stream().map(MTMVTaskExecutor::getTask).filter(u -> u.getDbName().equals(dbName))
                                .collect(Collectors.toList()));
            }
            taskList.addAll(getRunningTaskMap().values().stream().map(MTMVTaskExecutor::getTask)
                    .filter(u -> u.getDbName().equals(dbName)).collect(Collectors.toList()));
            taskList.addAll(
                    getAllHistory().stream().filter(u -> u.getDbName().equals(dbName)).collect(Collectors.toList()));

        }
        return taskList;
    }

    public void replayCreateJobTask(MTMVTask task) {
        if (task.getState() == TaskState.SUCCESS || task.getState() == TaskState.FAILED) {
            if (System.currentTimeMillis() > task.getExpireTime()) {
                return;
            }
        }

        switch (task.getState()) {
            case PENDING:
                String jobName = task.getJobName();
                MTMVJob job = mtmvJobManager.getJob(jobName);
                if (job == null) {
                    LOG.warn("fail to obtain task name {} because task is null", jobName);
                    return;
                }
                MTMVTaskExecutor taskExecutor = MTMVUtils.buildTask(job);
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

    public void replayUpdateTask(AlterMTMVTask changeTask) {
        TaskState fromStatus = changeTask.getFromStatus();
        TaskState toStatus = changeTask.getToStatus();
        Long jobId = changeTask.getJobId();
        if (fromStatus == TaskState.PENDING) {
            Queue<MTMVTaskExecutor> taskQueue = getPendingTaskMap().get(jobId);
            if (taskQueue == null) {
                return;
            }
            if (taskQueue.size() == 0) {
                getPendingTaskMap().remove(jobId);
                return;
            }

            MTMVTaskExecutor pendingTask = taskQueue.poll();
            MTMVTask status = pendingTask.getTask();

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
            MTMVTaskExecutor runningTask = getRunningTaskMap().remove(jobId);
            if (runningTask == null) {
                return;
            }
            MTMVTask status = runningTask.getTask();
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
            LOG.warn("Illegal  Task taskId:{} status transform from {} to {}", changeTask.getTaskId(), fromStatus,
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
            Deque<MTMVTask> taskHistory = getAllHistory();
            Iterator<MTMVTask> iterator = taskHistory.iterator();
            while (iterator.hasNext()) {
                MTMVTask task = iterator.next();
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
                Queue<MTMVTaskExecutor> tasks = getPendingTaskMap().get(pendingIter.next());
                while (!tasks.isEmpty()) {
                    MTMVTaskExecutor taskExecutor = tasks.poll();
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
                MTMVTaskExecutor taskExecutor = getRunningTaskMap().get(runningIter.next());
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
