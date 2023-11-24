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

package org.apache.doris.job.extensions.insert;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.io.Text;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.base.JobExecuteType;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.common.TaskType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.nereids.trees.plans.commands.InsertIntoTableCommand;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.gson.annotations.SerializedName;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

@Data
@Slf4j
public class InsertJob extends AbstractJob<InsertTask> {

    @SerializedName(value = "lp")
    String labelPrefix;

    InsertIntoTableCommand command;

    StmtExecutor stmtExecutor;

    ConnectContext ctx;

    @SerializedName("tis")
    ConcurrentLinkedQueue<Long> taskIdList;

    // max save task num, do we need to config it?
    private static final int MAX_SAVE_TASK_NUM = 50;


    @Override
    public List<InsertTask> createTasks(TaskType taskType) {
        InsertTask task = new InsertTask(null, getCurrentDbName(), getExecuteSql(), getCreateUser());
        task.setJobId(getJobId());
        task.setTaskType(taskType);
        task.setTaskId(Env.getCurrentEnv().getNextId());
        ArrayList<InsertTask> tasks = new ArrayList<>();
        tasks.add(task);
        super.initTasks(tasks);
        addNewTask(task.getTaskId());
        return tasks;
    }

    public void addNewTask(long id) {

        if (CollectionUtils.isEmpty(taskIdList)) {
            taskIdList = new ConcurrentLinkedQueue<>();
            Env.getCurrentEnv().getEditLog().logUpdateJob(this);
            taskIdList.add(id);
            return;
        }
        taskIdList.add(id);
        if (taskIdList.size() >= MAX_SAVE_TASK_NUM) {
            taskIdList.poll();
        }
        Env.getCurrentEnv().getEditLog().logUpdateJob(this);
    }

    @Override
    public void cancelTaskById(long taskId) throws JobException {
        super.cancelTaskById(taskId);
    }


    @Override
    public void cancelAllTasks() throws JobException {
        super.cancelAllTasks();
    }

    @Override
    public boolean isReadyForScheduling() {
        return true;
    }


    @Override
    protected void checkJobParamsInternal() {
        if (command == null && StringUtils.isBlank(getExecuteSql())) {
            throw new IllegalArgumentException("command or sql is null,must be set");
        }
        if (null != command && !getJobConfig().getExecuteType().equals(JobExecuteType.INSTANT)) {
            throw new IllegalArgumentException("command must be null when executeType is not instant");
        }
    }

    @Override
    public List<InsertTask> queryTasks() {
        if (CollectionUtils.isEmpty(taskIdList)) {
            return new ArrayList<>();
        }
        //TODO it's will be refactor, we will storage task info in job inner and query from it
        List<Long> taskIdList = new ArrayList<>(this.taskIdList);
        Collections.reverse(taskIdList);
        List<LoadJob> loadJobs = Env.getCurrentEnv().getLoadManager().queryLoadJobsByJobIds(taskIdList);
        if (CollectionUtils.isEmpty(loadJobs)) {
            return new ArrayList<>();
        }
        List<InsertTask> tasks = new ArrayList<>();
        loadJobs.forEach(loadJob -> {
            InsertTask task;
            try {
                task = new InsertTask(loadJob.getLabel(), loadJob.getDb().getFullName(), null, getCreateUser());
            } catch (MetaNotFoundException e) {
                log.warn("load job not found,job id is {}", loadJob.getId());
                return;
            }
            task.setJobId(getJobId());
            task.setTaskId(loadJob.getId());
            task.setLoadJob(loadJob);
            tasks.add(task);
        });
        return tasks;
    }

    @Override
    public JobType getJobType() {
        return JobType.INSERT;
    }

    @Override
    public ShowResultSetMetaData getJobMetaData() {
        return super.getJobMetaData();
    }

    @Override
    public ShowResultSetMetaData getTaskMetaData() {
        return TASK_META_DATA;
    }

    @Override
    public void onTaskFail(InsertTask task) {
        getRunningTasks().remove(task);
    }

    @Override
    public void onTaskSuccess(InsertTask task) {
        super.onTaskSuccess(task);
    }

    @Override
    public List<String> getShowInfo() {
        return super.getCommonShowInfo();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    private static final ShowResultSetMetaData TASK_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("TaskId", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Label", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Status", ScalarType.createVarchar(20)))
                    .addColumn(new Column("EtlInfo", ScalarType.createVarchar(20)))
                    .addColumn(new Column("TaskInfo", ScalarType.createVarchar(20)))
                    .addColumn(new Column("ErrorMsg", ScalarType.createVarchar(20)))

                    .addColumn(new Column("CreateTimeMs", ScalarType.createVarchar(20)))
                    .addColumn(new Column("FinishTimeMs", ScalarType.createVarchar(20)))
                    .addColumn(new Column("TrackingUrl", ScalarType.createVarchar(20)))
                    .addColumn(new Column("LoadStatistic", ScalarType.createVarchar(20)))
                    .addColumn(new Column("User", ScalarType.createVarchar(20)))
                    .build();
}
