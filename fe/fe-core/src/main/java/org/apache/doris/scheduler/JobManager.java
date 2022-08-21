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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.scheduler.Utils.JobState;
import org.apache.doris.scheduler.Utils.TriggerMode;
import org.apache.doris.scheduler.metadata.ChangeTaskRecord;
import org.apache.doris.scheduler.metadata.Job;
import org.apache.doris.scheduler.metadata.Job.JobSchedule;
import org.apache.doris.scheduler.metadata.TaskRecord;

import com.clearspring.analytics.util.Lists;
import com.clearspring.analytics.util.Preconditions;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class JobManager {
    private static final Logger LOG = LogManager.getLogger(JobManager.class);

    private final Map<Long, Job> idToJobMap;
    private final Map<String, Job> nameToJobMap;
    private final Map<Long, ScheduledFuture<?>> periodFutureMap;

    // include PENDING/RUNNING taskRun;
    private final TaskManager taskRunManager;

    // The periodScheduler is used to generate the corresponding  Task on time for the Periodical Task.
    // This scheduler can use the time wheel to optimize later.
    private final ScheduledExecutorService periodScheduler = Executors.newScheduledThreadPool(1);

    // The dispatchTaskScheduler is responsible for periodically checking whether the running  Task is completed
    // and updating the status. It is also responsible for placing pending  Task in the running  Task queue.
    // This operation need to consider concurrency.
    // This scheduler can use notify/wait to optimize later.
    private final ScheduledExecutorService dispatchScheduler = Executors.newScheduledThreadPool(1);

    // Use to concurrency control
    private final ReentrantLock taskLock;

    private final AtomicBoolean isStart = new AtomicBoolean(false);

    public JobManager() {
        idToJobMap = Maps.newConcurrentMap();
        nameToJobMap = Maps.newConcurrentMap();
        periodFutureMap = Maps.newConcurrentMap();
        taskRunManager = new TaskManager();
        taskLock = new ReentrantLock(true);
    }

    public void start() {
        if (isStart.compareAndSet(false, true)) {
            clearUnfinishedTaskRun();
            registerPeriodicalTask();
            dispatchScheduler.scheduleAtFixedRate(() -> {
                if (!taskRunManager.tryTaskRunLock()) {
                    return;
                }
                try {
                    taskRunManager.checkRunningTaskRun();
                    taskRunManager.scheduledPendingTaskRun();
                } catch (Exception ex) {
                    LOG.warn("failed to dispatch task.", ex);
                } finally {
                    taskRunManager.taskRunUnlock();
                }
            }, 0, 1, TimeUnit.SECONDS);
        }
    }

    private void registerPeriodicalTask() {
        for (Job job : nameToJobMap.values()) {
            if (job.getTriggerMode() != TriggerMode.PERIODICAL) {
                return;
            }

            JobSchedule jobSchedule = job.getSchedule();
            if (job.getState() != JobState.ACTIVE) {
                continue;
            }

            if (jobSchedule == null) {
                continue;
            }

            LocalDateTime startTime = Utils.getDatetimeFromLong(jobSchedule.getStartTime());
            Duration duration = Duration.between(LocalDateTime.now(), startTime);
            long initialDelay = duration.getSeconds();
            // if startTime < now, start scheduling now
            if (initialDelay < 0) {
                initialDelay = 0;
            }
            ScheduledFuture<?> future =
                    periodScheduler.scheduleAtFixedRate(() -> executeTask(job.getName()), initialDelay,
                            Utils.convertTimeUnitValueToSecond(jobSchedule.getPeriod(), jobSchedule.getTimeUnit()),
                            TimeUnit.SECONDS);
            periodFutureMap.put(job.getId(), future);
        }
    }

    private void clearUnfinishedTaskRun() {
        if (!taskRunManager.tryTaskRunLock()) {
            return;
        }
        try {
            Iterator<Long> pendingIter = taskRunManager.getPendingTaskRunMap().keySet().iterator();
            while (pendingIter.hasNext()) {
                Queue<Task> taskRuns = taskRunManager.getPendingTaskRunMap().get(pendingIter.next());
                while (!taskRuns.isEmpty()) {
                    Task taskRun = taskRuns.poll();
                    taskRun.getRecord().setErrorMessage("Fe abort the task");
                    taskRun.getRecord().setErrorCode(-1);
                    taskRun.getRecord().setState(Utils.TaskState.FAILED);
                    taskRunManager.getTaskRunHistory().addHistory(taskRun.getRecord());
                    ChangeTaskRecord statusChange =
                            new ChangeTaskRecord(taskRun.getTaskId(), taskRun.getRecord(), Utils.TaskState.PENDING,
                                    Utils.TaskState.FAILED);
                    // GlobalStateMgr.getCurrentState().getEditLog().logUpdateTaskRun(statusChange);
                }
                pendingIter.remove();
            }
            Iterator<Long> runningIter = taskRunManager.getRunningTaskRunMap().keySet().iterator();
            while (runningIter.hasNext()) {
                Task taskRun = taskRunManager.getRunningTaskRunMap().get(runningIter.next());
                taskRun.getRecord().setErrorMessage("Fe abort the task");
                taskRun.getRecord().setErrorCode(-1);
                taskRun.getRecord().setState(Utils.TaskState.FAILED);
                taskRun.getRecord().setFinishTime(System.currentTimeMillis());
                runningIter.remove();
                taskRunManager.getTaskRunHistory().addHistory(taskRun.getRecord());
                ChangeTaskRecord statusChange =
                        new ChangeTaskRecord(taskRun.getTaskId(), taskRun.getRecord(), Utils.TaskState.RUNNING,
                                Utils.TaskState.FAILED);
                // GlobalStateMgr.getCurrentState().getEditLog().logUpdateTaskRun(statusChange);
            }
        } finally {
            taskRunManager.taskRunUnlock();
        }
    }

    public void createJob(Job job, boolean isReplay) throws DdlException {
        if (!tryTaskLock()) {
            throw new DdlException("Failed to get task lock when create Task [" + job.getName() + "]");
        }
        try {
            if (nameToJobMap.containsKey(job.getName())) {
                throw new DdlException("Task [" + job.getName() + "] already exists");
            }
            if (!isReplay) {
                // TaskId should be assigned by the framework
                Preconditions.checkArgument(job.getId() == 0);
                // job.setId(GlobalStateMgr.getCurrentState().getNextId());
            }
            if (job.getTriggerMode() == Utils.TriggerMode.PERIODICAL) {
                job.setState(Utils.JobState.ACTIVE);
                if (!isReplay) {
                    JobSchedule schedule = job.getSchedule();
                    if (schedule == null) {
                        throw new DdlException("Task [" + job.getName() + "] has no scheduling information");
                    }
                    LocalDateTime startTime = Utils.getDatetimeFromLong(schedule.getStartTime());
                    Duration duration = Duration.between(LocalDateTime.now(), startTime);
                    long initialDelay = duration.getSeconds();
                    // if startTime < now, start scheduling now
                    if (initialDelay < 0) {
                        initialDelay = 0;
                    }
                    // this operation should only run in master
                    ScheduledFuture<?> future =
                            periodScheduler.scheduleAtFixedRate(() -> executeTask(job.getName()), initialDelay,
                                    schedule.getPeriod(), schedule.getTimeUnit());
                    periodFutureMap.put(job.getId(), future);
                }
            }
            nameToJobMap.put(job.getName(), job);
            idToJobMap.put(job.getId(), job);
            if (!isReplay) {
                // GlobalStateMgr.getCurrentState().getEditLog().logCreateTask(job);
            }
        } finally {
            taskUnlock();
        }
    }

    private boolean stopScheduler(String jobName) {
        Job job = nameToJobMap.get(jobName);
        if (job.getTriggerMode() != TriggerMode.PERIODICAL) {
            return false;
        }
        if (job.getState() == Utils.JobState.PAUSE) {
            return true;
        }
        JobSchedule jobSchedule = job.getSchedule();
        // this will not happen
        if (jobSchedule == null) {
            LOG.warn("fail to obtain scheduled info for task [{}]", job.getName());
            return true;
        }
        ScheduledFuture<?> future = periodFutureMap.get(job.getId());
        if (future == null) {
            LOG.warn("fail to obtain scheduled info for task [{}]", job.getName());
            return true;
        }
        boolean isCancel = future.cancel(true);
        if (!isCancel) {
            LOG.warn("fail to cancel scheduler for task [{}]", job.getName());
        }
        return isCancel;
    }

    public boolean killTask(String taskName, boolean clearPending) {
        Job job = nameToJobMap.get(taskName);
        if (job == null) {
            return false;
        }
        if (clearPending) {
            if (!taskRunManager.tryTaskRunLock()) {
                return false;
            }
            try {
                taskRunManager.getPendingTaskRunMap().remove(job.getId());
            } catch (Exception ex) {
                LOG.warn("failed to kill task.", ex);
            } finally {
                taskRunManager.taskRunUnlock();
            }
        }
        return taskRunManager.killTaskRun(job.getId());
    }

    public SubmitResult executeTask(String taskName) {
        return executeTask(taskName, new ExecuteOption());
    }

    public SubmitResult executeTask(String taskName, ExecuteOption option) {
        Job job = nameToJobMap.get(taskName);
        if (job == null) {
            return new SubmitResult(null, SubmitResult.SubmitStatus.FAILED);
        }
        return taskRunManager.submitTaskRun(Utils.buildTask(job), option);
    }

    public void dropTasks(List<Long> taskIdList, boolean isReplay) {
        // keep  nameToJobMap and manualTaskMap consist
        if (!tryTaskLock()) {
            return;
        }
        try {
            for (long taskId : taskIdList) {
                Job task = idToJobMap.get(taskId);
                if (task == null) {
                    LOG.warn("drop taskId {} failed because task is null", taskId);
                    continue;
                }
                if (task.getTriggerMode() == TriggerMode.PERIODICAL && !isReplay) {
                    boolean isCancel = stopScheduler(task.getName());
                    if (!isCancel) {
                        continue;
                    }
                    periodFutureMap.remove(task.getId());
                }
                killTask(task.getName(), true);
                idToJobMap.remove(task.getId());
                nameToJobMap.remove(task.getName());
            }

            if (!isReplay) {
                // GlobalStateMgr.getCurrentState().getEditLog().logDropTasks(taskIdList);
            }
        } finally {
            taskUnlock();
        }
        LOG.info("drop tasks:{}", taskIdList);
    }

    public List<Job> showJobs(String dbName) {
        List<Job> jobList = Lists.newArrayList();
        if (dbName == null) {
            jobList.addAll(nameToJobMap.values());
        } else {
            jobList.addAll(nameToJobMap.values().stream().filter(u -> u.getDbName().equals(dbName))
                    .collect(Collectors.toList()));
        }
        return jobList;
    }

    private boolean tryTaskLock() {
        try {
            return taskLock.tryLock(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.warn("got exception while getting task lock", e);
        }
        return false;
    }

    public void taskUnlock() {
        this.taskLock.unlock();
    }

    public void replayCreateTask(Job task) {
        if (task.getTriggerMode() == TriggerMode.PERIODICAL) {
            JobSchedule jobSchedule = task.getSchedule();
            if (jobSchedule == null) {
                LOG.warn("replay a null schedule period Task [{}]", task.getName());
                return;
            }
        }
        if (task.getExpireTime() > 0 && System.currentTimeMillis() > task.getExpireTime()) {
            return;
        }
        try {
            createJob(task, true);
        } catch (DdlException e) {
            LOG.warn("failed to replay create task [{}]", task.getName(), e);
        }
    }

    public void replayDropTasks(List<Long> taskIdList) {
        dropTasks(taskIdList, true);
    }

    public TaskManager getTaskManager() {
        return taskRunManager;
    }


    public long loadTasks(DataInputStream dis, long checksum) throws IOException {
        int taskCount = 0;
        try {
            String s = Text.readString(dis);
            SerializeData data = GsonUtils.GSON.fromJson(s, SerializeData.class);
            if (data != null) {
                if (data.jobs != null) {
                    for (Job job : data.jobs) {
                        replayCreateTask(job);
                    }
                    taskCount = data.jobs.size();
                }

                if (data.taskRecords != null) {
                    for (TaskRecord runStatus : data.taskRecords) {
                        replayCreateTaskRun(runStatus);
                    }
                }
            }
            checksum ^= taskCount;
            LOG.info("finished replaying TaskManager from image");
        } catch (EOFException e) {
            LOG.info("no TaskManager to replay.");
        }
        return checksum;
    }

    public long saveTasks(DataOutputStream dos, long checksum) throws IOException {
        SerializeData data = new SerializeData();
        data.jobs = new ArrayList<>(nameToJobMap.values());
        checksum ^= data.jobs.size();
        data.taskRecords = showTaskRecord(null);
        String s = GsonUtils.GSON.toJson(data);
        Text.writeString(dos, s);
        return checksum;
    }

    public List<TaskRecord> showTaskRecord(String dbName) {
        List<TaskRecord> taskRunList = Lists.newArrayList();
        if (dbName == null) {
            for (Queue<Task> pTaskRunQueue : taskRunManager.getPendingTaskRunMap().values()) {
                taskRunList.addAll(pTaskRunQueue.stream().map(Task::getRecord).collect(Collectors.toList()));
            }
            taskRunList.addAll(taskRunManager.getRunningTaskRunMap().values().stream().map(Task::getRecord)
                    .collect(Collectors.toList()));
            taskRunList.addAll(taskRunManager.getTaskRunHistory().getAllHistory());
        } else {
            for (Queue<Task> pTaskRunQueue : taskRunManager.getPendingTaskRunMap().values()) {
                taskRunList.addAll(pTaskRunQueue.stream().map(Task::getRecord).filter(u -> u.getDbName().equals(dbName))
                        .collect(Collectors.toList()));
            }
            taskRunList.addAll(taskRunManager.getRunningTaskRunMap().values().stream().map(Task::getRecord)
                    .filter(u -> u.getDbName().equals(dbName)).collect(Collectors.toList()));
            taskRunList.addAll(taskRunManager.getTaskRunHistory().getAllHistory().stream()
                    .filter(u -> u.getDbName().equals(dbName)).collect(Collectors.toList()));

        }
        return taskRunList;
    }

    public void replayCreateTaskRun(TaskRecord status) {

        if (status.getState() == Utils.TaskState.SUCCESS || status.getState() == Utils.TaskState.FAILED) {
            if (System.currentTimeMillis() > status.getExpireTime()) {
                return;
            }
        }

        switch (status.getState()) {
            case PENDING:
                String taskName = status.getTaskName();
                Job job = nameToJobMap.get(taskName);
                if (job == null) {
                    LOG.warn("fail to obtain task name {} because task is null", taskName);
                    return;
                }
                Task taskRun = Utils.buildTask(job);
                taskRun.initRecord(status.getQueryId(), status.getCreateTime());
                taskRunManager.arrangeTaskRun(taskRun, status.isMergeRedundant());
                break;
            // this will happen in build image
            case RUNNING:
                status.setState(Utils.TaskState.FAILED);
                taskRunManager.getTaskRunHistory().addHistory(status);
                break;
            case FAILED:
            case SUCCESS:
                taskRunManager.getTaskRunHistory().addHistory(status);
                break;
        }
    }

    public void replayUpdateTaskRun(ChangeTaskRecord statusChange) {
        Utils.TaskState fromStatus = statusChange.getFromStatus();
        Utils.TaskState toStatus = statusChange.getToStatus();
        Long taskId = statusChange.getTaskId();
        if (fromStatus == Utils.TaskState.PENDING) {
            Queue<Task> taskRunQueue = taskRunManager.getPendingTaskRunMap().get(taskId);
            if (taskRunQueue == null) {
                return;
            }
            if (taskRunQueue.size() == 0) {
                taskRunManager.getPendingTaskRunMap().remove(taskId);
                return;
            }

            Task pendingTaskRun = taskRunQueue.poll();
            TaskRecord status = pendingTaskRun.getRecord();

            if (toStatus == Utils.TaskState.RUNNING) {
                if (status.getQueryId().equals(statusChange.getQueryId())) {
                    status.setState(Utils.TaskState.RUNNING);
                    taskRunManager.getRunningTaskRunMap().put(taskId, pendingTaskRun);
                }
                // for fe restart, should keep logic same as clearUnfinishedTaskRun
            } else if (toStatus == Utils.TaskState.FAILED) {
                status.setErrorMessage(statusChange.getErrorMessage());
                status.setErrorCode(statusChange.getErrorCode());
                status.setState(Utils.TaskState.FAILED);
                taskRunManager.getTaskRunHistory().addHistory(status);
            }
            if (taskRunQueue.size() == 0) {
                taskRunManager.getPendingTaskRunMap().remove(taskId);
            }
        } else if (fromStatus == Utils.TaskState.RUNNING && (toStatus == Utils.TaskState.SUCCESS
                || toStatus == Utils.TaskState.FAILED)) {
            Task runningTaskRun = taskRunManager.getRunningTaskRunMap().remove(taskId);
            if (runningTaskRun == null) {
                return;
            }
            TaskRecord status = runningTaskRun.getRecord();
            if (status.getQueryId().equals(statusChange.getQueryId())) {
                if (toStatus == Utils.TaskState.FAILED) {
                    status.setErrorMessage(statusChange.getErrorMessage());
                    status.setErrorCode(statusChange.getErrorCode());
                }
                status.setState(toStatus);
                status.setFinishTime(statusChange.getFinishTime());
                taskRunManager.getTaskRunHistory().addHistory(status);
            }
        } else {
            LOG.warn("Illegal  Task queryId:{} status transform from {} to {}", statusChange.getQueryId(), fromStatus,
                    toStatus);
        }
    }

    public void replayDropTaskRuns(List<String> queryIdList) {
        Map<String, String> index = Maps.newHashMapWithExpectedSize(queryIdList.size());
        for (String queryId : queryIdList) {
            index.put(queryId, null);
        }
        taskRunManager.getTaskRunHistory().getAllHistory()
                .removeIf(runStatus -> index.containsKey(runStatus.getQueryId()));
    }

    public void removeExpiredTasks() {
        long currentTimeMs = System.currentTimeMillis();

        List<Long> taskIdToDelete = Lists.newArrayList();
        if (!tryTaskLock()) {
            return;
        }
        try {
            List<Job> jobs = showJobs(null);
            for (Job job : jobs) {
                if (job.getTriggerMode() == Utils.TriggerMode.PERIODICAL) {
                    JobSchedule jobSchedule = job.getSchedule();
                    if (jobSchedule == null) {
                        taskIdToDelete.add(job.getId());
                        LOG.warn("clean up a null schedule periodical Task [{}]", job.getName());
                        continue;
                    }
                    // active periodical task should not clean
                    if (job.getState() == Utils.JobState.ACTIVE) {
                        continue;
                    }
                }
                long expireTime = job.getExpireTime();
                if (expireTime > 0 && currentTimeMs > expireTime) {
                    taskIdToDelete.add(job.getId());
                }
            }
        } finally {
            taskUnlock();
        }
        // this will do in checkpoint thread and does not need write log
        dropTasks(taskIdToDelete, true);
    }

    public void removeExpiredTaskRuns() {
        long currentTimeMs = System.currentTimeMillis();

        List<String> historyToDelete = Lists.newArrayList();

        if (!taskRunManager.tryTaskRunLock()) {
            return;
        }
        try {
            // only SUCCESS and FAILED in taskRunHistory
            Deque<TaskRecord> taskRunHistory = taskRunManager.getTaskRunHistory().getAllHistory();
            Iterator<TaskRecord> iterator = taskRunHistory.iterator();
            while (iterator.hasNext()) {
                TaskRecord TaskRecord = iterator.next();
                long expireTime = TaskRecord.getExpireTime();
                if (currentTimeMs > expireTime) {
                    historyToDelete.add(TaskRecord.getQueryId());
                    iterator.remove();
                }
            }
        } finally {
            taskRunManager.taskRunUnlock();
        }
        LOG.info("remove run history:{}", historyToDelete);
    }

    private static class SerializeData {
        @SerializedName("jobs")
        public List<Job> jobs;

        @SerializedName("taskRecords")
        public List<TaskRecord> taskRecords;
    }

    public boolean containJob(String jobName) {
        return nameToJobMap.containsKey(jobName);
    }

    public Job getJob(String jobName) {
        return nameToJobMap.get(jobName);
    }
}
