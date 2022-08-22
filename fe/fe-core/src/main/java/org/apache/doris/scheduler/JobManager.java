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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.scheduler.Utils.JobState;
import org.apache.doris.scheduler.Utils.TriggerMode;
import org.apache.doris.scheduler.metadata.ChangeJob;
import org.apache.doris.scheduler.metadata.ChangeTask;
import org.apache.doris.scheduler.metadata.CheckpointData;
import org.apache.doris.scheduler.metadata.Job;
import org.apache.doris.scheduler.metadata.Job.JobSchedule;
import org.apache.doris.scheduler.metadata.Task;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
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

    private final TaskManager taskManager;

    // The periodScheduler is used to generate the corresponding task on time for the Periodical Task.
    // This scheduler can use the time wheel to optimize later.
    private final ScheduledExecutorService periodScheduler = Executors.newScheduledThreadPool(1);

    // The dispatchTaskScheduler is responsible for periodically checking whether the running  Task is completed
    // and updating the status. It is also responsible for placing pending  Task in the running  Task queue.
    // This operation need to consider concurrency.
    // This scheduler can use notify/wait to optimize later.

    private final ScheduledExecutorService cleaner = Executors.newScheduledThreadPool(1);

    // Use to concurrency control
    private final ReentrantLock taskLock;

    private final AtomicBoolean isStart = new AtomicBoolean(false);

    public JobManager() {
        idToJobMap = Maps.newConcurrentMap();
        nameToJobMap = Maps.newConcurrentMap();
        periodFutureMap = Maps.newConcurrentMap();
        taskManager = new TaskManager();
        taskLock = new ReentrantLock(true);
    }

    public void start() {
        if (isStart.compareAndSet(false, true)) {
            taskManager.clearUnfinishedTasks();
            registerJobs();

            cleaner.scheduleAtFixedRate(() -> {
                if (!Env.getCurrentEnv().isMaster()) {
                    return;
                }
                if (!tryLock()) {
                    return;
                }
                try {
                    removeExpiredJobs();
                    taskManager.removeExpiredTasks();
                } catch (Exception ex) {
                    LOG.warn("failed remove expired jobs and tasks.", ex);
                } finally {
                    unlock();
                }
            }, 0, 1, TimeUnit.DAYS);

            taskManager.startTaskDispatcher();
        }
    }

    private void registerJobs() {
        for (Job job : nameToJobMap.values()) {
            if (job.getState() != JobState.ACTIVE) {
                continue;
            }
            if (job.getTriggerMode() == TriggerMode.PERIODICAL) {
                JobSchedule jobSchedule = job.getSchedule();
                if (jobSchedule == null) {
                    continue;
                }

                ScheduledFuture<?> future = periodScheduler.scheduleAtFixedRate(() -> submitJobTask(job.getName()),
                        getInitialDelay(jobSchedule),
                        Utils.convertTimeUnitValueToSecond(jobSchedule.getPeriod(), jobSchedule.getTimeUnit()),
                        TimeUnit.SECONDS);
                periodFutureMap.put(job.getId(), future);
            } else if (job.getTriggerMode() == TriggerMode.ONCE) {
                ExecuteOption executeOption = new ExecuteOption();
                executeOption.setMarkJobFinished(true);
                submitJobTask(job.getName(), executeOption);
            }
        }
    }

    private long getInitialDelay(JobSchedule jobSchedule) {
        LocalDateTime startTime = Utils.getDatetimeFromLong(jobSchedule.getStartTime());
        Duration duration = Duration.between(LocalDateTime.now(), startTime);
        long initialDelay = duration.getSeconds();
        // if startTime < now, start scheduling just now
        return initialDelay < 0 ? 0 : initialDelay;
    }

    public void createJob(Job job, boolean isReplay) throws DdlException {
        if (!tryLock()) {
            throw new DdlException("Failed to get task lock when create Task [" + job.getName() + "]");
        }
        try {
            if (nameToJobMap.containsKey(job.getName())) {
                throw new DdlException("Job [" + job.getName() + "] already exists");
            }
            if (!isReplay) {
                Preconditions.checkArgument(job.getId() == 0);
                job.setId(Env.getCurrentEnv().getNextId());
            }
            if (job.getTriggerMode() == TriggerMode.PERIODICAL) {
                job.setState(JobState.ACTIVE);
                if (!isReplay) {
                    JobSchedule schedule = job.getSchedule();
                    if (schedule == null) {
                        throw new DdlException("Job [" + job.getName() + "] has no scheduling information");
                    }
                    ScheduledFuture<?> future = periodScheduler.scheduleAtFixedRate(() -> submitJobTask(job.getName()),
                            getInitialDelay(schedule), schedule.getPeriod(), schedule.getTimeUnit());
                    periodFutureMap.put(job.getId(), future);
                }
            } else if (job.getTriggerMode() == TriggerMode.ONCE) {
                job.setState(JobState.ACTIVE);
                if (!isReplay) {
                    ExecuteOption executeOption = new ExecuteOption();
                    executeOption.setMarkJobFinished(true);
                    submitJobTask(job.getName(), executeOption);
                }
            }
            nameToJobMap.put(job.getName(), job);
            idToJobMap.put(job.getId(), job);
            if (!isReplay) {
                Env.getCurrentEnv().getEditLog().logCreateScheduleJob(job);
            }
        } finally {
            unlock();
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

    public boolean killJobTask(String jobName, boolean clearPending) {
        Job job = nameToJobMap.get(jobName);
        if (job == null) {
            return false;
        }
        return taskManager.killTask(job.getId(), clearPending);
    }

    public SubmitResult submitJobTask(String jobName) {
        return submitJobTask(jobName, new ExecuteOption());
    }

    public SubmitResult submitJobTask(String jobName, ExecuteOption option) {
        Job job = nameToJobMap.get(jobName);
        if (job == null) {
            return new SubmitResult(null, SubmitResult.SubmitStatus.FAILED);
        }
        return taskManager.submitTask(Utils.buildTask(job), option);
    }

    public void changeJobs(List<Long> jobIds, boolean isReplay) {

    }

    public void dropJobs(List<Long> jobIds, boolean isReplay) {
        // keep  nameToJobMap and manualTaskMap consist
        if (!tryLock()) {
            return;
        }
        try {
            for (long jobId : jobIds) {
                Job job = idToJobMap.get(jobId);
                if (job == null) {
                    LOG.warn("drop taskId {} failed because task is null", jobId);
                    continue;
                }
                if (job.getTriggerMode() == TriggerMode.PERIODICAL && !isReplay) {
                    boolean isCancel = stopScheduler(job.getName());
                    if (!isCancel) {
                        continue;
                    }
                    periodFutureMap.remove(job.getId());
                }
                killJobTask(job.getName(), true);
                idToJobMap.remove(job.getId());
                nameToJobMap.remove(job.getName());
            }

            if (!isReplay) {
                Env.getCurrentEnv().getEditLog().logDropScheduleJob(jobIds);
            }
        } finally {
            unlock();
        }
        LOG.info("drop tasks:{}", jobIds);
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

    private boolean tryLock() {
        try {
            return taskLock.tryLock(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.warn("got exception while getting task lock", e);
        }
        return false;
    }

    public void unlock() {
        this.taskLock.unlock();
    }

    public void replayCreateJob(Job job) {
        if (job.getTriggerMode() == TriggerMode.PERIODICAL) {
            JobSchedule jobSchedule = job.getSchedule();
            if (jobSchedule == null) {
                LOG.warn("replay a null schedule period Task [{}]", job.getName());
                return;
            }
        }
        if (job.getExpireTime() > 0 && System.currentTimeMillis() > job.getExpireTime()) {
            return;
        }
        try {
            createJob(job, true);
        } catch (DdlException e) {
            LOG.warn("failed to replay create task [{}]", job.getName(), e);
        }
    }

    public void replayDropJobs(List<Long> jobIds) {
        dropJobs(jobIds, true);
    }

    public void replayUpdateJob(ChangeJob changeJob) {

    }

    public TaskManager getTaskManager() {
        return taskManager;
    }

    public void replayCreateJobTask(Task task) {
        if (task.getState() == Utils.TaskState.SUCCESS || task.getState() == Utils.TaskState.FAILED) {
            if (System.currentTimeMillis() > task.getExpireTime()) {
                return;
            }
        }

        switch (task.getState()) {
            case PENDING:
                String taskName = task.getJobName();
                Job job = nameToJobMap.get(taskName);
                if (job == null) {
                    LOG.warn("fail to obtain task name {} because task is null", taskName);
                    return;
                }
                TaskExecutor taskExecutor = Utils.buildTask(job);
                taskExecutor.initRecord(task.getTaskId(), task.getCreateTime());
                taskManager.arrangeTask(taskExecutor, task.isMergeRedundant());
                break;
            // this will happen in build image
            case RUNNING:
                task.setState(Utils.TaskState.FAILED);
                taskManager.addHistory(task);
                break;
            case FAILED:
            case SUCCESS:
                taskManager.addHistory(task);
                break;
            default:
                break;
        }
    }

    public void replayUpdateTask(ChangeTask changeTask) {
        Utils.TaskState fromStatus = changeTask.getFromStatus();
        Utils.TaskState toStatus = changeTask.getToStatus();
        Long taskId = changeTask.getTaskId();
        if (fromStatus == Utils.TaskState.PENDING) {
            Queue<TaskExecutor> taskQueue = taskManager.getPendingTaskMap().get(taskId);
            if (taskQueue == null) {
                return;
            }
            if (taskQueue.size() == 0) {
                taskManager.getPendingTaskMap().remove(taskId);
                return;
            }

            TaskExecutor pendingTask = taskQueue.poll();
            Task status = pendingTask.getTask();

            if (toStatus == Utils.TaskState.RUNNING) {
                if (status.getTaskId().equals(changeTask.getQueryId())) {
                    status.setState(Utils.TaskState.RUNNING);
                    taskManager.getRunningTaskMap().put(taskId, pendingTask);
                }
                // for fe restart, should keep logic same as clearUnfinishedTask
            } else if (toStatus == Utils.TaskState.FAILED) {
                status.setErrorMessage(changeTask.getErrorMessage());
                status.setErrorCode(changeTask.getErrorCode());
                status.setState(Utils.TaskState.FAILED);
                taskManager.addHistory(status);
            }
            if (taskQueue.size() == 0) {
                taskManager.getPendingTaskMap().remove(taskId);
            }
        } else if (fromStatus == Utils.TaskState.RUNNING && (toStatus == Utils.TaskState.SUCCESS
                || toStatus == Utils.TaskState.FAILED)) {
            TaskExecutor runningTask = taskManager.getRunningTaskMap().remove(taskId);
            if (runningTask == null) {
                return;
            }
            Task status = runningTask.getTask();
            if (status.getTaskId().equals(changeTask.getQueryId())) {
                if (toStatus == Utils.TaskState.FAILED) {
                    status.setErrorMessage(changeTask.getErrorMessage());
                    status.setErrorCode(changeTask.getErrorCode());
                }
                status.setState(toStatus);
                status.setFinishTime(changeTask.getFinishTime());
                taskManager.addHistory(status);
            }
        } else {
            LOG.warn("Illegal  Task queryId:{} status transform from {} to {}", changeTask.getQueryId(),
                    fromStatus, toStatus);
        }
    }

    public void replayDropJobTasks(List<String> taskIds) {
        Map<String, String> index = Maps.newHashMapWithExpectedSize(taskIds.size());
        for (String taskId : taskIds) {
            index.put(taskId, null);
        }
        taskManager.getAllHistory().removeIf(runStatus -> index.containsKey(runStatus.getTaskId()));
    }

    public void removeExpiredJobs() {
        long currentTimeMs = System.currentTimeMillis();

        List<Long> taskIdToDelete = Lists.newArrayList();
        if (!tryLock()) {
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
            unlock();
        }
        // this will do in checkpoint thread and does not need write log
        dropJobs(taskIdToDelete, true);
    }



    public boolean containJob(String jobName) {
        return nameToJobMap.containsKey(jobName);
    }

    public Job getJob(String jobName) {
        return nameToJobMap.get(jobName);
    }

    public long write(DataOutputStream dos, long checksum) throws IOException {
        CheckpointData data = new CheckpointData();
        data.jobs = new ArrayList<>(nameToJobMap.values());
        checksum ^= data.jobs.size();
        data.tasks = taskManager.showTasks(null);
        String s = GsonUtils.GSON.toJson(data);
        Text.writeString(dos, s);
        return checksum;
    }

    public static JobManager read(DataInputStream dis, long checksum) throws IOException {
        JobManager jobManager = new JobManager();
        try {
            String s = Text.readString(dis);
            CheckpointData data = GsonUtils.GSON.fromJson(s, CheckpointData.class);
            if (data != null) {
                if (data.jobs != null) {
                    for (Job job : data.jobs) {
                        jobManager.replayCreateJob(job);
                    }
                }

                if (data.tasks != null) {
                    for (Task runStatus : data.tasks) {
                        jobManager.replayCreateJobTask(runStatus);
                    }
                }
            }
            LOG.info("finished replaying JobManager from image");
        } catch (EOFException e) {
            LOG.info("no TaskManager to replay.");
        }
        return jobManager;
    }

}
