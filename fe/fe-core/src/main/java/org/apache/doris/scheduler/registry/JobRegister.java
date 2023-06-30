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

import org.apache.doris.scheduler.executor.JobExecutor;

import java.io.IOException;

/**
 * This interface provides a contract for registering timed scheduling events.
 * The implementation should trigger events in a timely manner using a specific algorithm.
 * The execution of the events may be asynchronous and not guarantee strict timing accuracy.
 */
public interface JobRegister {

    /**
     * Register a job
     *
     * @param name        job name,it's not unique
     * @param intervalMs  job interval, unit: ms
     * @param executor    job executor @See {@link JobExecutor}
     * @return event job id
     */
    Long registerJob(String name, Long intervalMs, JobExecutor executor);

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
    Long registerJob(String name, Long intervalMs, Long startTimeStamp, JobExecutor executor);


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
                          JobExecutor executor);

    /**
     * if job is running, pause it
     * pause means event job will not be executed in the next cycle,but current cycle will not be interrupted
     * we can resume it by {@link #resumeJob(Long)}
     *
     * @param eventId event job id
     *                if eventId not exist, return false
     * @return true if pause success, false if pause failed
     */
    Boolean pauseJob(Long jodId);

    /**
     * if job is running, stop it
     * stop means event job will not be executed in the next cycle and current cycle will be interrupted
     * stop not can be resumed, if you want to resume it, you should register it again
     * we will delete stopped event job
     *
     * @param jobId event job id
     * @return true if stop success, false if stop failed
     */
    Boolean stopJob(Long jobId);

    /**
     * if job is paused, resume it
     *
     * @param jobId job id
     * @return true if resume success, false if resume failed
     */
    Boolean resumeJob(Long jobId);

    /**
     * close job scheduler register
     * close means job scheduler register will not accept new job
     * Jobs that have not reached the trigger time will not be executed. Jobs that have reached the trigger time will
     * have an execution time of 5 seconds, and will not be executed if the time exceeds
     */
    void close() throws IOException;

}
