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

package org.apache.doris.event.registry;

import org.apache.doris.event.executor.EventJobExecutor;

import java.io.IOException;

/**
 * This interface provides a contract for registering timed scheduling events.
 * The implementation should trigger events in a timely manner using a specific algorithm.
 * The execution of the events may be asynchronous and not guarantee strict timing accuracy.
 */
public interface EventJobSchedulerRegister {

    /**
     * Register a event job
     *
     * @param name       event job name,it's not unique
     * @param intervalMs event job interval, unit: ms
     * @param executor   event job executor @See {@link EventJobExecutor}
     * @return event job id
     */
    Long registerEventJob(String name, Long intervalMs, EventJobExecutor executor);

    /**
     * Register a event job
     *
     * @param name           event job name,it's not unique
     * @param intervalMs     event job interval, unit: ms
     * @param startTimeStamp event job start time stamp, unit: ms
     *                       if startTimeStamp is null, event job will start immediately in the next cycle
     *                       startTimeStamp should be greater than current time
     * @param executor       event job executor @See {@link EventJobExecutor}
     * @return event job id
     */
    Long registerEventJob(String name, Long intervalMs, Long startTimeStamp, EventJobExecutor executor);


    /**
     * Register a event job
     *
     * @param name           event job name,it's not unique
     * @param intervalMs     event job interval, unit: ms
     * @param startTimeStamp event job start time stamp, unit: ms
     *                       if startTimeStamp is null, event job will start immediately in the next cycle
     *                       startTimeStamp should be greater than current time
     * @param endTimeStamp   event job end time stamp, unit: ms
     *                       if endTimeStamp is null, event job will never stop
     *                       endTimeStamp must be greater than startTimeStamp and endTimeStamp should be greater
     *                       than current time
     * @param executor       event job executor @See {@link EventJobExecutor}
     * @return event job id
     */
    Long registerEventJob(String name, Long intervalMs, Long startTimeStamp, Long endTimeStamp,
                          EventJobExecutor executor);

    /**
     * if event job is running, pause it
     * pause means event job will not be executed in the next cycle,but current cycle will not be interrupted
     * we can resume it by {@link #resumeEventJob(Long)}
     *
     * @param eventId event job id
     *                if eventId not exist, return false
     * @return true if pause success, false if pause failed
     */
    Boolean pauseEventJob(Long eventId);

    /**
     * if event job is running, stop it
     * stop means event job will not be executed in the next cycle and current cycle will be interrupted
     * stop not can be resumed, if you want to resume it, you should register it again
     * we will delete stopped event job
     *
     * @param eventId event job id
     * @return true if stop success, false if stop failed
     */
    Boolean stopEventJob(Long eventId);

    /**
     * if event job is paused, resume it
     *
     * @param eventId event job id
     * @return true if resume success, false if resume failed
     */
    Boolean resumeEventJob(Long eventId);

    /**
     * close event job scheduler register
     * close means event job scheduler register will not accept new event job
     * Jobs that have not reached the trigger time will not be executed. Jobs that have reached the trigger time will
     * have an execution time of 5 seconds, and will not be executed if the time exceeds
     */
    void close() throws IOException;

}
