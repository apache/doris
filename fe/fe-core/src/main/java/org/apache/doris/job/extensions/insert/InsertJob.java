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

import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.qe.ShowResultSetMetaData;

import lombok.Data;

import java.util.Arrays;
import java.util.List;

@Data
public class InsertJob extends AbstractJob {

    String labelPrefix;

    @Override
    public List<InsertTask> createTasks() {
        InsertTask task = new InsertTask(null, null, null, null, null);
        task.setJobId(getJobId());
        List<InsertTask> tasks = Arrays.asList(task);
        getRunningTasks().add(task);
        return tasks;
    }


    @Override
    public void cancel() throws JobException {

    }

    @Override
    public boolean isReadyForScheduling() {
        return true;
    }


    @Override
    protected void checkJobParamsInternal() {

    }


    @Override
    public List queryTasks() {
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
    public void onTaskFail(long taskId) {

    }

    @Override
    public void onTaskSuccess(long taskId) {

    }

    @Override
    public void onTaskCancel(long taskId) {

    }

    @Override
    public void afterTaskRun(long taskId) {

    }
}
