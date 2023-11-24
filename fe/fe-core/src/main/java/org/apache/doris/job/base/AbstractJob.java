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
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.RandomUtils;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Data
public abstract class AbstractJob<T extends AbstractTask> implements Job<T>, Writable {

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
    private Long createTimeMs;

    @SerializedName(value = "sql")
    String executeSql;

    private List<T> runningTasks = new ArrayList<>();

    @Override
    public void cancelAllTasks() throws JobException {
        if (CollectionUtils.isEmpty(runningTasks)) {
            return;
        }
        for (T task : runningTasks) {
            task.cancel();
        }
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

    @Override
    public void cancelTaskById(long taskId) throws JobException {
        if (CollectionUtils.isEmpty(runningTasks)) {
            throw new JobException("no running task");
        }
        runningTasks.stream().filter(task -> task.getTaskId().equals(taskId)).findFirst()
                .orElseThrow(() -> new JobException("no task id:" + taskId)).cancel();
    }

    public void initTasks(List<? extends AbstractTask> tasks) {
        tasks.forEach(task -> {
            task.setJobId(jobId);
            task.setTaskId(getNextId());
            task.setCreateTimeMs(System.currentTimeMillis());
            task.setStatus(TaskStatus.PENDING);
        });
        if (CollectionUtils.isEmpty(getRunningTasks())) {
            setRunningTasks(new ArrayList<>());
        }
        getRunningTasks().addAll((Collection<? extends T>) tasks);
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

    public void updateJobStatus(JobStatus newJobStatus) {
        if (null == newJobStatus) {
            throw new IllegalArgumentException("jobStatus cannot be null");
        }
        if (jobStatus == newJobStatus) {
            throw new IllegalArgumentException(String.format("Can't update job %s status to the %s status",
                    jobStatus.name(), this.jobStatus.name()));
        }
        if (newJobStatus.equals(JobStatus.RUNNING) && !jobStatus.equals(JobStatus.PAUSED)) {
            throw new IllegalArgumentException(String.format("Can't update job %s status to the %s status",
                    jobStatus.name(), this.jobStatus.name()));
        }
        if (newJobStatus.equals(JobStatus.STOPPED) && !jobStatus.equals(JobStatus.RUNNING)) {
            throw new IllegalArgumentException(String.format("Can't update job %s status to the %s status",
                    jobStatus.name(), this.jobStatus.name()));
        }
        jobStatus = newJobStatus;
    }


    protected abstract void checkJobParamsInternal();

    public static AbstractJob readFields(DataInput in) throws IOException {
        String jsonJob = Text.readString(in);
        AbstractJob<?> job = GsonUtils.GSON.fromJson(jsonJob, AbstractJob.class);
        job.setRunningTasks(new ArrayList<>());
        return job;
    }

    @Override
    public void onTaskFail(T task) {
        updateJobStatusIfEnd();
        runningTasks.remove(task);
    }

    @Override
    public void onTaskSuccess(T task) {
        updateJobStatusIfEnd();
        runningTasks.remove(task);

    }

    private void updateJobStatusIfEnd() {
        JobExecuteType executeType = getJobConfig().getExecuteType();
        if (executeType.equals(JobExecuteType.MANUAL)) {
            return;
        }
        switch (executeType) {
            case ONE_TIME:
            case INSTANT:
                Env.getCurrentEnv().getJobManager().getJob(jobId).updateJobStatus(JobStatus.FINISHED);
                break;
            case RECURRING:
                TimerDefinition timerDefinition = getJobConfig().getTimerDefinition();
                if (null != timerDefinition.getEndTimeMs()
                        && timerDefinition.getEndTimeMs() < System.currentTimeMillis()
                        + timerDefinition.getIntervalUnit().getIntervalMs(timerDefinition.getInterval())) {
                    Env.getCurrentEnv().getJobManager().getJob(jobId).updateJobStatus(JobStatus.FINISHED);
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

    @Override
    public List<String> getShowInfo() {
        return getCommonShowInfo();
    }

    @Override
    public ShowResultSetMetaData getJobMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    private static long getNextId() {
        return System.nanoTime() + RandomUtils.nextInt();
    }
}
