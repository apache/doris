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

import org.apache.doris.job.common.JobType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.qe.ShowResultSetMetaData;

import java.util.List;

/**
 * Job is the core of the scheduler module, which is used to store the Job information of the job module.
 * We can use the job to uniquely identify a Job.
 * The jobName is used to identify the job, which is not unique.
 * The jobStatus is used to identify the status of the Job, which is used to control the execution of the
 * job.
 */
public interface Job<T extends AbstractTask> {

    List<T> createTasks();

    void cancel() throws JobException;

    boolean isReadyForScheduling();


    ShowResultSetMetaData getJobMetaData();

    ShowResultSetMetaData getTaskMetaData();

    JobType getJobType();

    List<T> queryTasks();

    void onTaskFail(long taskId);

    void onTaskSuccess(long taskId);

    void onTaskCancel(long taskId);

    void afterTaskRun(long taskId);

}
