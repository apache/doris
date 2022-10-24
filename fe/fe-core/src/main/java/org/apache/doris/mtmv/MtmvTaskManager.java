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
import org.apache.doris.mtmv.MtmvUtils.JobState;
import org.apache.doris.mtmv.MtmvUtils.TaskState;
import org.apache.doris.mtmv.MtmvUtils.TriggerMode;
import org.apache.doris.mtmv.metadata.AlterMtmvTask;
import org.apache.doris.mtmv.metadata.ChangeMvmtJob;
import org.apache.doris.mtmv.metadata.MtmvJob;
import org.apache.doris.mtmv.metadata.MtmvTask;
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

public class MtmvTaskManager {

    private static final Logger LOG = LogManager.getLogger(MtmvTaskManager.class);

    // jobId -> pending tasks, one job can dispatch many tasks
    private final Map<Long, PriorityBlockingQueue<MtmvTaskExecutor>> pendingTaskMap = Maps.newConcurrentMap();

    // jobId -> running tasks, only one task will be running for one job.
    private final Map<Long, MtmvTaskExecutor> runningTaskMap = Maps.newConcurrentMap();

    private final MtmvTaskExecutorPool taskExecutorPool = new MtmvTaskExecutorPool();

    private final ReentrantLock reentrantLock = new ReentrantLock(true);

    // keep track of all the tasks
    private final Deque<MtmvTask> historyQueue = Queues.newLinkedBlockingDeque();

    private final ScheduledExecutorService taskScheduler = Executors.newScheduledThreadPool(1);

    private final MtmvJobManager mtmvJobManager;

    public MtmvTaskManager(MtmvJobManager mtmvJobManager) {
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

    public MtmvUtils.TaskSubmitStatus submitTask(MtmvTaskExecutor taskExecutor, MtmvTaskExecuteParams params) {
        // duplicate submit
        if (taskExecutor.getTask() != null) {
            return MtmvUtils.TaskSubmitStatus.FAILED;
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
            return MtmvUtils.TaskSubmitStatus.REJECTED;
        }

        String taskId = UUID.randomUUID().toString();
        MtmvTask task = taskExecutor.initTask(taskId, System.currentTimeMillis());
        task.setPriority(params.getPriority());
        Env.getCurrentEnv().getEditLog().logCreateScheduleTask(task);
        arrangeToPendingTask(taskExecutor);
        return MtmvUtils.TaskSubmitStatus.SUBMITTED;
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
        MtmvTaskExecutor task = runningTaskMap.get(jobId);
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

    public void arrangeToPendingTask(MtmvTaskExecutor task) {
        if (!tryLock()) {
            return;
        }
        try {
            long jobId = task.getJobId();
            PriorityBlockingQueue<MtmvTaskExecutor> tasks =
                    pendingTaskMap.computeIfAbsent(jobId, u -> Queues.newPriorityBlockingQueue());
            tasks.offer(task);
        } finally {
            unlock();
        }
    }

    @Nullable
    private MtmvTaskExecutor getTask(PriorityBlockingQueue<MtmvTaskExecutor> tasks, MtmvTaskExecutor task) {
        MtmvTaskExecutor oldTask = null;
        for (MtmvTaskExecutor t : tasks) {
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
            MtmvTaskExecutor taskExecutor = runningTaskMap.get(jobId);
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
                    ChangeMvmtJob changeJob = new ChangeMvmtJob(taskExecutor.getJobId(), JobState.COMPLETE);
                    mtmvJobManager.updateJob(changeJob, false);
                } else if (triggerMode == TriggerMode.PERIODICAL) {
                    // just update the last modify time.
                    ChangeMvmtJob changeJob = new ChangeMvmtJob(taskExecutor.getJobId(), JobState.ACTIVE);
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
            MtmvTaskExecutor runningTaskExecutor = runningTaskMap.get(jobId);
            if (runningTaskExecutor == null) {
                Queue<MtmvTaskExecutor> taskQueue = pendingTaskMap.get(jobId);
                if (taskQueue.size() == 0) {
                    pendingIterator.remove();
                } else {
                    if (currentRunning >= Config.max_running_mtmv_scheduler_task_num) {
                        break;
                    }
                    MtmvTaskExecutor pendingTaskExecutor = taskQueue.poll();
                    taskExecutorPool.executeTask(pendingTaskExecutor);
                    runningTaskMap.put(jobId, pendingTaskExecutor);
                    // change status from PENDING to Running
                    changeAndLogTaskStatus(jobId, pendingTaskExecutor.getTask(), TaskState.PENDING, TaskState.RUNNING);
                    currentRunning++;
                }
            }
        }
    }

    private void changeAndLogTaskStatus(long jobId, MtmvTask task, TaskState fromStatus, TaskState toStatus) {
        AlterMtmvTask changeTask = new AlterMtmvTask(jobId, task, fromStatus, toStatus);
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

    public Map<Long, PriorityBlockingQueue<MtmvTaskExecutor>> getPendingTaskMap() {
        return pendingTaskMap;
    }

    public Map<Long, MtmvTaskExecutor> getRunningTaskMap() {
        return runningTaskMap;
    }

    public void addHistory(MtmvTask task) {
        historyQueue.addFirst(task);
    }

    public Deque<MtmvTask> getAllHistory() {
        return historyQueue;
    }

    public List<MtmvTask> showTasks(String dbName) {
        List<MtmvTask> taskList = Lists.newArrayList();
        if (dbName == null) {
            for (Queue<MtmvTaskExecutor> pTaskQueue : getPendingTaskMap().values()) {
                taskList.addAll(pTaskQueue.stream().map(MtmvTaskExecutor::getTask).collect(Collectors.toList()));
            }
            taskList.addAll(
                    getRunningTaskMap().values().stream().map(MtmvTaskExecutor::getTask).collect(Collectors.toList()));
            taskList.addAll(getAllHistory());
        } else {
            for (Queue<MtmvTaskExecutor> pTaskQueue : getPendingTaskMap().values()) {
                taskList.addAll(
                        pTaskQueue.stream().map(MtmvTaskExecutor::getTask).filter(u -> u.getDbName().equals(dbName))
                                .collect(Collectors.toList()));
            }
            taskList.addAll(getRunningTaskMap().values().stream().map(MtmvTaskExecutor::getTask)
                    .filter(u -> u.getDbName().equals(dbName)).collect(Collectors.toList()));
            taskList.addAll(
                    getAllHistory().stream().filter(u -> u.getDbName().equals(dbName)).collect(Collectors.toList()));

        }
        return taskList;
    }

    public void replayCreateJobTask(MtmvTask task) {
        if (task.getState() == TaskState.SUCCESS || task.getState() == TaskState.FAILED) {
            if (System.currentTimeMillis() > task.getExpireTime()) {
                return;
            }
        }

        switch (task.getState()) {
            case PENDING:
                String jobName = task.getJobName();
                MtmvJob job = mtmvJobManager.getJob(jobName);
                if (job == null) {
                    LOG.warn("fail to obtain task name {} because task is null", jobName);
                    return;
                }
                MtmvTaskExecutor taskExecutor = MtmvUtils.buildTask(job);
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

    public void replayUpdateTask(AlterMtmvTask changeTask) {
        TaskState fromStatus = changeTask.getFromStatus();
        TaskState toStatus = changeTask.getToStatus();
        Long jobId = changeTask.getJobId();
        if (fromStatus == TaskState.PENDING) {
            Queue<MtmvTaskExecutor> taskQueue = getPendingTaskMap().get(jobId);
            if (taskQueue == null) {
                return;
            }
            if (taskQueue.size() == 0) {
                getPendingTaskMap().remove(jobId);
                return;
            }

            MtmvTaskExecutor pendingTask = taskQueue.poll();
            MtmvTask status = pendingTask.getTask();

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
            MtmvTaskExecutor runningTask = getRunningTaskMap().remove(jobId);
            if (runningTask == null) {
                return;
            }
            MtmvTask status = runningTask.getTask();
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
            Deque<MtmvTask> taskHistory = getAllHistory();
            Iterator<MtmvTask> iterator = taskHistory.iterator();
            while (iterator.hasNext()) {
                MtmvTask task = iterator.next();
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
                Queue<MtmvTaskExecutor> tasks = getPendingTaskMap().get(pendingIter.next());
                while (!tasks.isEmpty()) {
                    MtmvTaskExecutor taskExecutor = tasks.poll();
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
                MtmvTaskExecutor taskExecutor = getRunningTaskMap().get(runningIter.next());
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
