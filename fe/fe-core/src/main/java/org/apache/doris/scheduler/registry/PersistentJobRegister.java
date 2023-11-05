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

package org.apache.doris.scheduler.registry;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.scheduler.constants.JobCategory;
import org.apache.doris.scheduler.executor.JobExecutor;
import org.apache.doris.scheduler.job.Job;

import java.io.IOException;
import java.util.List;

/**
 * This interface provides a contract for registering timed scheduling events.
 * The implementation should trigger events in a timely manner using a specific algorithm.
 * The execution of the events may be asynchronous and not guarantee strict timing accuracy.
 */
public interface PersistentJobRegister {

    /**
     * Register a job
     *
     * @param name       job name,it's not unique
     * @param intervalMs job interval, unit: ms
     * @param executor   job executor @See {@link JobExecutor}
     * @return event job id
     */
    Long registerJob(String name, Long intervalMs, JobExecutor executor) throws DdlException;

    /**
     * Register a job
     *
     * @param name           job name,it's not unique
     * @param intervalMs     job interval, unit: ms
     * @param startTimeStamp job start time stamp, unit: ms
     *                       if startTimeStamp is null, event job will start immediately in the next cycle
     *                       startTimeStamp should be greater than current time
     * @param executor       event job executor @See {@link JobExecutor}
     * @return job id
     */
    Long registerJob(String name, Long intervalMs, Long startTimeStamp, JobExecutor executor) throws DdlException;


    /**
     * Register a event job
     *
     * @param name           job name,it's not unique
     * @param intervalMs     job interval, unit: ms
     * @param startTimeStamp job start time stamp, unit: ms
     *                       if startTimeStamp is null, job will start immediately in the next cycle
     *                       startTimeStamp should be greater than current time
     * @param endTimeStamp   job end time stamp, unit: ms
     *                       if endTimeStamp is null, job will never stop
     *                       endTimeStamp must be greater than startTimeStamp and endTimeStamp should be greater
     *                       than current time
     * @param executor       event job executor @See {@link JobExecutor}
     * @return event job id
     */
    Long registerJob(String name, Long intervalMs, Long startTimeStamp, Long endTimeStamp,
                     JobExecutor executor) throws DdlException;

    /**
     * if job is running, pause it
     * pause means event job will not be executed in the next cycle,but current cycle will not be interrupted
     * we can resume it by {@link #resumeJob(Long)}
     *
     * @param jodId job id
     *              if jobId not exist, return false
     */
    void pauseJob(Long jodId);

    void pauseJob(String dbName, String jobName, JobCategory jobCategory) throws DdlException;

    void resumeJob(String dbName, String jobName, JobCategory jobCategory) throws DdlException;

    /**
     * if job is running, stop it
     * stop means event job will not be executed in the next cycle and current cycle will be interrupted
     * stop not can be resumed, if you want to resume it, you should register it again
     * we will delete stopped event job
     *
     * @param jobId event job id
     */
    void stopJob(Long jobId);

    void stopJob(String dbName, String jobName, JobCategory jobCategory) throws DdlException;

    /**
     * if job is paused, resume it
     *
     * @param jobId job id
     */
    void resumeJob(Long jobId);

    Long registerJob(Job job) throws DdlException;


    /**
     * execute job task immediately,this method will not change job status and don't affect scheduler job
     * this task type should set to {@link org.apache.doris.scheduler.constants.TaskType#MANUAL_JOB_TASK}
     *
     * @param jobId       job id
     * @param contextData if you need to pass parameters to the task,
     * @param <T>         context data type
     * @return true if execute success, false if execute failed,
     * if job is not exist or job is not running, or job not support manual execute, return false
     */
    <T> boolean immediateExecuteTask(Long jobId, T contextData) throws DdlException;

    List<Job> getJobs(String dbFullName, String jobName, JobCategory jobCategory, PatternMatcher matcher);

    /**
     * close job scheduler register
     * close means job scheduler register will not accept new job
     * Jobs that have not reached the trigger time will not be executed. Jobs that have reached the trigger time will
     * have an execution time of 5 seconds, and will not be executed if the time exceeds
     */
    void close() throws IOException;

}
