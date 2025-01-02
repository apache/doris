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

package org.apache.doris.job.base;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.common.TaskType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;

import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.RandomUtils;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

@Data
@Log4j2
public abstract class AbstractJob<T extends AbstractTask, C> implements Job<T, C>, Writable {
    public static final ImmutableList<Column> COMMON_SCHEMA = ImmutableList.of(
            new Column("SucceedTaskCount", ScalarType.createStringType()),
            new Column("FailedTaskCount", ScalarType.createStringType()),
            new Column("CanceledTaskCount", ScalarType.createStringType())
    );
    @SerializedName(value = "jid")
    private Long jobId;

    @SerializedName(value = "jn")
    private String jobName;

    @SerializedName(value = "js")
    private JobStatus jobStatus;

    @SerializedName(value = "cdb")
    private String currentDbName;

    @SerializedName(value = "c")
    private String comment;

    @SerializedName(value = "cu")
    private UserIdentity createUser;

    @SerializedName(value = "jc")
    private JobExecutionConfiguration jobConfig;

    @SerializedName(value = "ctms")
    private long createTimeMs;

    @SerializedName(value = "stm")
    private long startTimeMs = -1L;

    @SerializedName(value = "ftm")
    private long finishTimeMs;

    @SerializedName(value = "sql")
    String executeSql;


    @SerializedName(value = "stc")
    private AtomicLong succeedTaskCount = new AtomicLong(0);

    @SerializedName(value = "ftc")
    private AtomicLong failedTaskCount = new AtomicLong(0);

    @SerializedName(value = "ctc")
    private AtomicLong canceledTaskCount = new AtomicLong(0);

    public AbstractJob() {
    }

    public AbstractJob(Long id) {
        jobId = id;
    }

    /**
     * executeSql and runningTasks is not required for load.
     */
    public AbstractJob(Long jobId, String jobName, JobStatus jobStatus,
                       String currentDbName,
                       String comment,
                       UserIdentity createUser,
                       JobExecutionConfiguration jobConfig) {
        this(jobId, jobName, jobStatus, currentDbName, comment,
                createUser, jobConfig, System.currentTimeMillis(), null);
    }

    public AbstractJob(Long jobId, String jobName, JobStatus jobStatus,
                       String currentDbName,
                       String comment,
                       UserIdentity createUser,
                       JobExecutionConfiguration jobConfig,
                       Long createTimeMs,
                       String executeSql) {
        this.jobId = jobId;
        this.jobName = jobName;
        this.jobStatus = jobStatus;
        this.currentDbName = currentDbName;
        this.comment = comment;
        this.createUser = createUser;
        this.jobConfig = jobConfig;
        this.createTimeMs = createTimeMs;
        this.executeSql = executeSql;
    }

    private CopyOnWriteArrayList<T> runningTasks = new CopyOnWriteArrayList<>();

    private Lock createTaskLock = new ReentrantLock();

    @Override
    public void cancelAllTasks(boolean needWaitCancelComplete) throws JobException {
        if (CollectionUtils.isEmpty(runningTasks)) {
            return;
        }
        for (T task : runningTasks) {
            task.cancel(needWaitCancelComplete);
            canceledTaskCount.incrementAndGet();
        }
        runningTasks = new CopyOnWriteArrayList<>();
        logUpdateOperation();
    }

    private static final ImmutableList<String> TITLE_NAMES =
            new ImmutableList.Builder<String>()
                    .add("Id")
                    .add("Name")
                    .add("Definer")
                    .add("ExecuteType")
                    .add("RecurringStrategy")
                    .add("Status")
                    .add("ExecuteSql")
                    .add("CreateTime")
                    .add("Comment")
                    .build();

    protected static long getNextJobId() {
        return System.nanoTime() + RandomUtils.nextInt();
    }

    @Override
    public void cancelTaskById(long taskId) throws JobException {
        if (CollectionUtils.isEmpty(runningTasks)) {
            throw new JobException("no running task");
        }
        runningTasks.stream().filter(task -> task.getTaskId().equals(taskId)).findFirst()
                .orElseThrow(() -> new JobException("Not found task id: " + taskId)).cancel(true);
        runningTasks.removeIf(task -> task.getTaskId().equals(taskId));
        canceledTaskCount.incrementAndGet();
        if (jobConfig.getExecuteType().equals(JobExecuteType.ONE_TIME)) {
            updateJobStatus(JobStatus.FINISHED);
        }
    }

    public List<T> queryAllTasks() {
        List<T> tasks = new ArrayList<>();
        if (CollectionUtils.isEmpty(runningTasks)) {
            return queryTasks();
        }

        List<T> historyTasks = queryTasks();
        if (CollectionUtils.isNotEmpty(historyTasks)) {
            tasks.addAll(historyTasks);
        }
        Set<Long> loadTaskIds = tasks.stream().map(AbstractTask::getTaskId).collect(Collectors.toSet());
        runningTasks.forEach(task -> {
            if (!loadTaskIds.contains(task.getTaskId())) {
                tasks.add(task);
            }
        });
        Comparator<T> taskComparator = Comparator.comparingLong(T::getCreateTimeMs).reversed();
        tasks.sort(taskComparator);
        return tasks;
    }

    public List<T> commonCreateTasks(TaskType taskType, C taskContext) {
        if (!canCreateTask(taskType)) {
            log.info("job is not ready for scheduling, job id is {},job status is {}, taskType is {}", jobId,
                    jobStatus, taskType);
            return new ArrayList<>();
        }
        if (!isReadyForScheduling(taskContext)) {
            log.info("job is not ready for scheduling, job id is {}", jobId);
            return new ArrayList<>();
        }
        try {
            //it's better to use tryLock and add timeout limit
            createTaskLock.lock();
            if (!isReadyForScheduling(taskContext)) {
                log.info("job is not ready for scheduling, job id is {}", jobId);
                return new ArrayList<>();
            }
            List<T> tasks = createTasks(taskType, taskContext);
            tasks.forEach(task -> log.info("common create task, job id is {}, task id is {}", jobId, task.getTaskId()));
            return tasks;
        } finally {
            createTaskLock.unlock();
        }
    }

    private boolean canCreateTask(TaskType taskType) {
        JobStatus currentJobStatus = getJobStatus();

        switch (taskType) {
            case SCHEDULED:
                return currentJobStatus.equals(JobStatus.RUNNING);
            case MANUAL:
                return currentJobStatus.equals(JobStatus.RUNNING) || currentJobStatus.equals(JobStatus.PAUSED);
            default:
                throw new IllegalArgumentException("Unsupported TaskType: " + taskType);
        }
    }

    public void initTasks(Collection<? extends T> tasks, TaskType taskType) {
        tasks.forEach(task -> {
            task.setTaskType(taskType);
            task.setJobId(getJobId());
            task.setCreateTimeMs(System.currentTimeMillis());
            task.setStatus(TaskStatus.PENDING);
        });
        getRunningTasks().addAll(tasks);
        this.startTimeMs = System.currentTimeMillis();
    }

    public void checkJobParams() {
        if (null == jobId) {
            throw new IllegalArgumentException("jobId cannot be null");
        }
        if (null == jobConfig) {
            throw new IllegalArgumentException("jobConfig cannot be null");
        }
        jobConfig.checkParams();
        checkJobParamsInternal();
    }

    public void updateJobStatus(JobStatus newJobStatus) throws JobException {
        if (null == newJobStatus) {
            throw new IllegalArgumentException("jobStatus cannot be null");
        }
        if (jobStatus == newJobStatus) {
            return;
        }
        String errorMsg = String.format("Can't update job %s status to the %s status",
                jobStatus.name(), newJobStatus.name());
        if (newJobStatus.equals(JobStatus.RUNNING) && !jobStatus.equals(JobStatus.PAUSED)) {
            throw new IllegalArgumentException(errorMsg);
        }
        if (newJobStatus.equals(JobStatus.STOPPED) && !jobStatus.equals(JobStatus.RUNNING)) {
            throw new IllegalArgumentException(errorMsg);
        }
        if (newJobStatus.equals(JobStatus.FINISHED)) {
            this.finishTimeMs = System.currentTimeMillis();
        }
        if (JobStatus.PAUSED.equals(newJobStatus) || JobStatus.STOPPED.equals(newJobStatus)) {
            cancelAllTasks(JobStatus.STOPPED.equals(newJobStatus) ? false : true);
        }
        jobStatus = newJobStatus;
    }


    protected abstract void checkJobParamsInternal();

    public static AbstractJob readFields(DataInput in) throws IOException {
        String jsonJob = Text.readString(in);
        AbstractJob job = GsonUtils.GSON.fromJson(jsonJob, AbstractJob.class);
        job.runningTasks = new CopyOnWriteArrayList();
        job.createTaskLock = new ReentrantLock();
        return job;
    }

    public void logCreateOperation() {
        Env.getCurrentEnv().getEditLog().logCreateJob(this);
    }

    public void logDeleteOperation() {
        Env.getCurrentEnv().getEditLog().logDeleteJob(this);
    }

    public void logUpdateOperation() {
        Env.getCurrentEnv().getEditLog().logUpdateJob(this);
    }

    @Override
    public void onTaskFail(T task) throws JobException {
        failedTaskCount.incrementAndGet();
        updateJobStatusIfEnd(false, task.getTaskType());
        runningTasks.remove(task);
        logUpdateOperation();
    }

    @Override
    public void onTaskSuccess(T task) throws JobException {
        succeedTaskCount.incrementAndGet();
        updateJobStatusIfEnd(true, task.getTaskType());
        runningTasks.remove(task);
        logUpdateOperation();

    }


    private void updateJobStatusIfEnd(boolean taskSuccess, TaskType taskType) throws JobException {
        JobExecuteType executeType = getJobConfig().getExecuteType();
        if (executeType.equals(JobExecuteType.MANUAL) || taskType.equals(TaskType.MANUAL)) {
            return;
        }
        switch (executeType) {
            case ONE_TIME:
                updateJobStatus(JobStatus.FINISHED);
                this.finishTimeMs = System.currentTimeMillis();
                break;
            case INSTANT:
                this.finishTimeMs = System.currentTimeMillis();
                if (taskSuccess) {
                    updateJobStatus(JobStatus.FINISHED);
                } else {
                    updateJobStatus(JobStatus.STOPPED);
                }
                break;
            case RECURRING:
                TimerDefinition timerDefinition = getJobConfig().getTimerDefinition();
                if (null != timerDefinition.getEndTimeMs()
                        && timerDefinition.getEndTimeMs() < System.currentTimeMillis()
                        + timerDefinition.getIntervalUnit().getIntervalMs(timerDefinition.getInterval())) {
                    this.finishTimeMs = System.currentTimeMillis();
                    updateJobStatus(JobStatus.FINISHED);
                }
                break;
            default:
                break;
        }
    }

    /**
     * get the job's common show info, which is used to show the job information
     * eg:show jobs sql
     *
     * @return List<String> job common show info
     */
    public List<String> getCommonShowInfo() {
        List<String> commonShowInfo = new ArrayList<>();
        commonShowInfo.add(String.valueOf(jobId));
        commonShowInfo.add(jobName);
        commonShowInfo.add(createUser.getQualifiedUser());
        commonShowInfo.add(jobConfig.getExecuteType().name());
        commonShowInfo.add(jobConfig.convertRecurringStrategyToString());
        commonShowInfo.add(jobStatus.name());
        commonShowInfo.add(executeSql);
        commonShowInfo.add(TimeUtils.longToTimeString(createTimeMs));
        commonShowInfo.add(comment);
        return commonShowInfo;
    }

    public TRow getCommonTvfInfo() {
        TRow trow = new TRow();
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(jobId)));
        trow.addToColumnValue(new TCell().setStringVal(jobName));
        trow.addToColumnValue(new TCell().setStringVal(createUser.getQualifiedUser()));
        trow.addToColumnValue(new TCell().setStringVal(jobConfig.getExecuteType().name()));
        trow.addToColumnValue(new TCell().setStringVal(jobConfig.convertRecurringStrategyToString()));
        trow.addToColumnValue(new TCell().setStringVal(jobStatus.name()));
        trow.addToColumnValue(new TCell().setStringVal(executeSql));
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(createTimeMs)));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(succeedTaskCount.get())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(failedTaskCount.get())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(canceledTaskCount.get())));
        trow.addToColumnValue(new TCell().setStringVal(comment));
        return trow;
    }

    @Override
    public List<String> getShowInfo() {
        return getCommonShowInfo();
    }

    @Override
    public TRow getTvfInfo() {
        return getCommonTvfInfo();
    }

    /**
     * Generates a common error message when the execution queue is full.
     *
     * @param taskId                  The ID of the task.
     * @param queueConfigName         The name of the queue configuration.
     * @param executeThreadConfigName The name of the execution thread configuration.
     * @return A formatted error message.
     */
    protected String commonFormatMsgWhenExecuteQueueFull(Long taskId, String queueConfigName,
                                                         String executeThreadConfigName) {
        return String.format("Dispatch task failed, jobId: %d, jobName: %s, taskId: %d, the queue size is full, "
                        + "you can increase the queue size by setting the property "
                        + "%s in the fe.conf file or increase the value of "
                        + "the property %s in the fe.conf file", getJobId(), getJobName(), taskId, queueConfigName,
                executeThreadConfigName);
    }

    @Override
    public ShowResultSetMetaData getJobMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public void onRegister() throws JobException {
    }

    @Override
    public void onUnRegister() throws JobException {
    }

    @Override
    public void onReplayCreate() throws JobException {
        log.info(new LogBuilder(LogKey.SCHEDULER_JOB, getJobId()).add("msg", "replay create scheduler job").build());
    }

    @Override
    public void onReplayEnd(AbstractJob<?, C> replayJob) throws JobException {
        log.info(new LogBuilder(LogKey.SCHEDULER_JOB, getJobId()).add("msg", "replay delete scheduler job").build());
    }
}
