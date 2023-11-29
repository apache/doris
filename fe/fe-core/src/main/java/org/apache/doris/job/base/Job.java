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
 * The Job interface represents a job in the scheduler module, which stores the information of a job.
 * A job can be uniquely identified using the job identifier.
 * The job name is used for identification purposes and is not necessarily unique.
 * The job status is used to control the execution of the job.
 *
 * @param <T> The type of task associated with the job, extending AbstractTask.
 */
public interface Job<T extends AbstractTask> {

    /**
     * Creates a list of tasks of the specified type for this job.
     *
     * @param taskType The type of tasks to create.
     * @return A list of tasks.
     */
    List<T> createTasks(TaskType taskType);

    /**
     * Cancels the task with the specified taskId.
     *
     * @param taskId The ID of the task to cancel.
     * @throws JobException If the task is not in the running state, it may have already
     * finished and cannot be cancelled.
     */
    void cancelTaskById(long taskId) throws JobException;

    /**
     * Checks if the job is ready for scheduling.
     * This method is called when starting the scheduled job,
     * and if the job is not ready for scheduling, the scheduler will cancel it.
     *
     * @return True if the job is ready for scheduling, false otherwise.
     */
    boolean isReadyForScheduling();

    /**
     * Retrieves the metadata for the job, which is used to display job information.
     *
     * @return The metadata for the job.
     */
    ShowResultSetMetaData getJobMetaData();

    /**
     * Retrieves the metadata for the tasks, which is used to display task information.
     * The metadata includes fields such as taskId, taskStatus, taskType, taskStartTime, taskEndTime, and taskProgress.
     *
     * @return The metadata for the tasks.
     */
    ShowResultSetMetaData getTaskMetaData();

    /**
     * Retrieves the type of the job, which is used to identify different types of jobs.
     *
     * @return The type of the job.
     */
    JobType getJobType();

    /**
     * Queries the list of tasks associated with this job.
     *
     * @return The list of tasks.
     */
    List<T> queryTasks();

    /**
     * Cancels all running tasks of this job.
     *
     * @throws JobException If cancelling a running task fails.
     */
    void cancelAllTasks() throws JobException;

    /**
     * Notifies the job when a task execution fails.
     *
     * @param task The failed task.
     */
    void onTaskFail(T task);

    /**
     * Notifies the job when a task execution is successful.
     *
     * @param task The successful task.
     */
    void onTaskSuccess(T task);

    /**
     * get the job's show info, which is used to sql show the job information
     * @return List<String> job common show info
     */
    List<String> getShowInfo();
}
