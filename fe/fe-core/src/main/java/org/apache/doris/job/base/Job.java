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
import org.apache.doris.job.common.TaskType;
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

    List<T> createTasks(TaskType taskType);

    /**
     * cancel the task by taskId
     *
     * @param taskId taskId
     * @throws JobException if the task is not in the running state,it's maybe finish,
     * it cannot be cancelled,and throw JobException
     */
    void cancel(long taskId) throws JobException;

    /**
     * when start the schedule job, we will call this method
     * if the job is not ready for scheduling, we will cancel this one scheduler
     */
    boolean isReadyForScheduling();


    /**
     * get the job's metadata title, which is used to show the job information
     * @return ShowResultSetMetaData job metadata
     */
    ShowResultSetMetaData getJobMetaData();

    /**
     * get the task metadata title, which is used to show the task information
     * eg: taskId, taskStatus, taskType, taskStartTime, taskEndTime, taskProgress
     * @return ShowResultSetMetaData task metadata
     */
    ShowResultSetMetaData getTaskMetaData();

    /**
     * JobType is used to identify the type of the job, which is used to distinguish the different types of jobs.
     * @return JobType
     */
    JobType getJobType();

    /**
     * Query the task list of this job
     */
    List<T> queryTasks();

    /**
     * cancel this job's all running task
     * @throws JobException if running task cancel failed, throw JobException
     */
    void cancel() throws JobException;

    /**
     * When the task executed result is failed, the task will call this method to notify the job
     * @param task task
     */
    void onTaskFail(T task);

    /**
     * When the task executed is success, the task will call this method to notify the job
     * @param task task
     */
    void onTaskSuccess(T task);

    /**
     * When the task executed is cancel, the task will call this method to notify the job
     * @param task task
     */
    void onTaskCancel(T task);

}
