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

package org.apache.doris.scheduler.disruptor;

import org.apache.doris.scheduler.constants.SystemJob;
import org.apache.doris.scheduler.job.AsyncJobManager;
import org.apache.doris.scheduler.job.Job;

import com.lmax.disruptor.WorkHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

/**
 * This class represents a work handler for processing event tasks consumed by a Disruptor.
 * The work handler retrieves the associated event job and executes it if it is running.
 * If the event job is not running, the work handler logs an error message.
 * If the event job execution fails, the work handler logs an error message and pauses the event job.
 * The work handler also handles system events by scheduling batch scheduler tasks.
 */
@Slf4j
public class TimerTaskExpirationHandler implements WorkHandler<TimerTaskEvent> {

    /**
     * The event job manager used to retrieve and execute event jobs.
     */
    private AsyncJobManager asyncJobManager;

    /**
     * Constructs a new {@link TimerTaskExpirationHandler} instance with the specified event job manager.
     *
     * @param asyncJobManager The event job manager used to retrieve and execute event jobs.
     */
    public TimerTaskExpirationHandler(AsyncJobManager asyncJobManager) {
        this.asyncJobManager = asyncJobManager;
    }

    /**
     * Processes an event task by retrieving the associated event job and executing it if it is running.
     * If the event job is not running, it logs an error message.
     * If the event job execution fails, it logs an error message and pauses the event job.
     *
     * @param event The event task to be processed.
     */
    @Override
    public void onEvent(TimerTaskEvent event) {
        if (checkIsSystemEvent(event)) {
            onSystemEvent();
            return;
        }
        onEventTask(event);
    }

    /**
     * Processes an event task by retrieving the associated event job and executing it if it is running.
     *
     * @param timerTaskEvent The event task to be processed.
     */
    @SuppressWarnings("checkstyle:UnusedLocalVariable")
    public void onEventTask(TimerTaskEvent timerTaskEvent) {
        long jobId = timerTaskEvent.getJobId();
        Job job = asyncJobManager.getJob(jobId);
        if (job == null) {
            log.info("Event job is null, eventJobId: {}", jobId);
            return;
        }
        if (!job.isRunning()) {
            log.info("Event job is not running, eventJobId: {}", jobId);
            return;
        }
        log.debug("Event job is running, eventJobId: {}", jobId);
        checkJobIsExpired(job);
        try {
            // TODO: We should record the result of the event task.
            //Object result = job.getExecutor().execute();
            job.getExecutor().execute();
            job.setLatestCompleteExecuteTimestamp(System.currentTimeMillis());
        } catch (Exception e) {
            log.error("Event job execute failed, jobId: {}", jobId, e);
            job.pause(e.getMessage());
        }
    }

    /**
     * Handles a system event by scheduling batch scheduler tasks.
     */
    private void onSystemEvent() {
        try {
            asyncJobManager.batchSchedulerTasks();
        } catch (Exception e) {
            log.error("System batch scheduler execute failed", e);
        }
    }

    /**
     * Checks whether the specified event task is a system event.
     *
     * @param event The event task to be checked.
     * @return true if the event task is a system event, false otherwise.
     */
    private boolean checkIsSystemEvent(TimerTaskEvent event) {
        return Objects.equals(event.getJobId(), SystemJob.SYSTEM_SCHEDULER_JOB.getId());
    }

    private void checkJobIsExpired(Job job) {
        if (job.isExpired()) {
            job.pause();
        }
    }
}
