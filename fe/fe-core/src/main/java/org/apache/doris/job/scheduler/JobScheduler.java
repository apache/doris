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

package org.apache.doris.job.scheduler;

import org.apache.doris.common.CustomThreadFactory;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.base.JobExecuteType;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.disruptor.TaskDisruptor;
import org.apache.doris.job.executor.TimerJobSchedulerTask;
import org.apache.doris.job.manager.TaskDisruptorGroupManager;
import org.apache.doris.job.task.AbstractTask;

import io.netty.util.HashedWheelTimer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class JobScheduler<T extends AbstractJob<?>> implements Closeable {

    /**
     * scheduler tasks, it's used to scheduler job
     */
    private HashedWheelTimer timerTaskScheduler;

    private TaskDisruptor timerJobDisruptor;

    private TaskDisruptorGroupManager taskDisruptorGroupManager;

    private long latestBatchSchedulerTimerTaskTimeMs = 0L;

    private static final long BATCH_SCHEDULER_INTERVAL_SECONDS = 600;

    private final Map<Long, T> jobMap;

    public JobScheduler(Map<Long, T> jobMap) {
        this.jobMap = jobMap;
    }

    /**
     * batch scheduler interval ms time
     */
    private static final long BATCH_SCHEDULER_INTERVAL_MILLI_SECONDS = BATCH_SCHEDULER_INTERVAL_SECONDS * 1000L;

    public void start() {
        timerTaskScheduler = new HashedWheelTimer(new CustomThreadFactory("timer-task-scheduler"), 1,
                TimeUnit.SECONDS, 660);
        timerTaskScheduler.start();
        taskDisruptorGroupManager = new TaskDisruptorGroupManager();
        taskDisruptorGroupManager.init();
        this.timerJobDisruptor = taskDisruptorGroupManager.getDispatchDisruptor();
    }

    /**
     * We will cycle system scheduler tasks every 10 minutes.
     * Jobs will be re-registered after the task is completed
     */
    private void cycleSystemSchedulerTasks() {
        log.debug("re-register system scheduler timer tasks" + TimeUtils.longToTimeString(System.currentTimeMillis()));
        timerTaskScheduler.newTimeout(timeout -> {


            cycleSystemSchedulerTasks();
        }, BATCH_SCHEDULER_INTERVAL_SECONDS, TimeUnit.SECONDS);

    }

    private void batchSchedulerTimerJob() {
        jobMap.forEach((jobId, job) -> {
            if (job.getJobStatus().equals(JobStatus.RUNNING)) {
                return;
            }
            if (!job.getJobConfig().checkIsTimerJob()) {
                cycleTimerJobScheduler(job);
            }

        });
    }

    public void scheduleOneJob(T job) {
        if (job.getJobStatus().equals(JobStatus.RUNNING)) {
            return;
        }
        if (!job.getJobConfig().checkIsTimerJob()) {
            //manual job will not scheduler
            if (JobExecuteType.MANUAL.equals(job.getJobConfig().getExecuteType())) {
                return;
            }
            //todo skip streaming job,improve in the future
            if (JobExecuteType.INSTANT.equals(job.getJobConfig().getExecuteType()) && job.isReadyForScheduling()) {
                schedulerImmediateJob(job);
            }
        }
        //if it's timer job and trigger last window already start, we will scheduler it immediately
        cycleTimerJobScheduler(job);
    }

    @Override
    public void close() throws IOException {

    }


    private void cycleTimerJobScheduler(T job) {
        List<Long> delaySeconds = job.getJobConfig().getTriggerDelayTimes(System.currentTimeMillis(),
                System.currentTimeMillis(), latestBatchSchedulerTimerTaskTimeMs);
        if (CollectionUtils.isNotEmpty(delaySeconds)) {
            delaySeconds.forEach(delaySecond -> {
                TimerJobSchedulerTask<T> timerJobSchedulerTask = new TimerJobSchedulerTask<>(timerJobDisruptor, job);
                timerTaskScheduler.newTimeout(timerJobSchedulerTask, delaySecond, TimeUnit.SECONDS);
            });
        }
    }


    private void schedulerImmediateJob(T job) {
        List<? extends AbstractTask> tasks = job.createTasks();
        if (CollectionUtils.isEmpty(tasks)) {
            return;
        }
        tasks.forEach(task -> taskDisruptorGroupManager.dispatchInstantTask(task, job.getJobType()));

    }

    private void triggerJob(T job) {
        if (!job.isReadyForScheduling()) {
            return;
        }
        List<? extends AbstractTask> tasks = job.createTasks();
        tasks.forEach(task -> taskDisruptorGroupManager.dispatchInstantTask(task, job.getJobType()));
    }

    /**
     * We will get the task in the next time window, and then hand it over to the time wheel for timing trigger
     */
    private void executeTimerJobIdsWithinLastTenMinutesWindow() {
        if (jobMap.isEmpty()) {
            return;
        }
        long currentTimeMs = System.currentTimeMillis();
        long startsWindow = System.currentTimeMillis();
        long endsWindow = 1L; // fixme
        for (Map.Entry<Long, T> entry : jobMap.entrySet()) {
            T job = entry.getValue();
            List<Long> delaySeconds = job.getJobConfig().getTriggerDelayTimes(currentTimeMs, startsWindow, endsWindow);
            if (CollectionUtils.isEmpty(delaySeconds)) {
                continue;
            }

            for (Long timeMs : delaySeconds) {
                TimerJobSchedulerTask timerJobSchedulerTask = new TimerJobSchedulerTask(timerJobDisruptor, job);
                timerTaskScheduler.newTimeout(timerJobSchedulerTask, timeMs, TimeUnit.SECONDS);
            }
        }
    }
}
