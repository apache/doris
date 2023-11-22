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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.io.Text;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.common.TaskType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.gson.annotations.SerializedName;
import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Data
public class InsertJob extends AbstractJob<InsertTask> {

    @SerializedName(value = "labelPrefix")
    String labelPrefix;


    @Override
    public List<InsertTask> createTasks(TaskType taskType) {
        InsertTask task = new InsertTask(null, null, null, null, null);
        task.setJobId(getJobId());
        task.setTaskType(taskType);
        task.setTaskId(Env.getCurrentEnv().getNextId());
        ArrayList<InsertTask> tasks = new ArrayList<>();
        tasks.add(task);
        super.initTasks(tasks);
        getRunningTasks().addAll(tasks);
        return tasks;
    }

    @Override
    public void cancel(long taskId) throws JobException {
        super.cancel();
    }


    @Override
    public void cancel() throws JobException {
        super.cancel();
    }

    @Override
    public boolean isReadyForScheduling() {
        return true;
    }


    @Override
    protected void checkJobParamsInternal() {

    }

    public static InsertJob readFields(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), InsertJob.class);
    }

    @Override
    public List<InsertTask> queryTasks() {
        return null;
    }

    @Override
    public JobType getJobType() {
        return JobType.INSERT;
    }

    @Override
    public ShowResultSetMetaData getJobMetaData() {
        return null;
    }

    @Override
    public ShowResultSetMetaData getTaskMetaData() {
        return null;
    }

    @Override
    public void onTaskFail(InsertTask task) {
        getRunningTasks().remove(task);
    }

    @Override
    public void onTaskSuccess(InsertTask task) {
        getRunningTasks().remove(task);
    }

    @Override
    public void onTaskCancel(InsertTask task) {
        getRunningTasks().remove(task);
    }


    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, JobType.INSERT.name());
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }
}
