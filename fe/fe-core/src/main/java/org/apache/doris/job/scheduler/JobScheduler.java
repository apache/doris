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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.CustomThreadFactory;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.base.JobExecuteType;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.common.TaskType;
import org.apache.doris.job.disruptor.TaskDisruptor;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.executor.TimerJobSchedulerTask;
import org.apache.doris.job.manager.TaskDisruptorGroupManager;
import org.apache.doris.job.task.AbstractTask;

import io.netty.util.HashedWheelTimer;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections.CollectionUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Log4j2
public class JobScheduler<T extends AbstractJob<?, C>, C> implements Closeable {

    /**
     * scheduler tasks, it's used to scheduler job
     */
    private HashedWheelTimer timerTaskScheduler;

    private TaskDisruptor timerJobDisruptor;

    private TaskDisruptorGroupManager taskDisruptorGroupManager;

    private long latestBatchSchedulerTimerTaskTimeMs = 0L;

    private static final long BATCH_SCHEDULER_INTERVAL_SECONDS = 600;

    private static final int HASHED_WHEEL_TIMER_TICKS_PER_WHEEL = 660;

    private final Map<Long, T> jobMap;

    public JobScheduler(Map<Long, T> jobMap) {
        this.jobMap = jobMap;
    }

    /**
     * batch scheduler interval ms time
     */
    private static final long BATCH_SCHEDULER_INTERVAL_MILLI_SECONDS = BATCH_SCHEDULER_INTERVAL_SECONDS * 1000L;

    /**
     * Finished job will be cleared after 24 hours
     */
    private static final long FINISHED_JOB_CLEANUP_THRESHOLD_TIME_MS =
            (Config.finished_job_cleanup_threshold_time_hour > 0
                    ? Config.finished_job_cleanup_threshold_time_hour : 24) * 3600 * 1000L;

    public void start() {
        timerTaskScheduler = new HashedWheelTimer(new CustomThreadFactory("timer-task-scheduler"), 1,
                TimeUnit.SECONDS, HASHED_WHEEL_TIMER_TICKS_PER_WHEEL);
        timerTaskScheduler.start();
        taskDisruptorGroupManager = new TaskDisruptorGroupManager();
        taskDisruptorGroupManager.init();
        this.timerJobDisruptor = taskDisruptorGroupManager.getDispatchDisruptor();
        latestBatchSchedulerTimerTaskTimeMs = System.currentTimeMillis();
        batchSchedulerTimerJob();
        cycleSystemSchedulerTasks();
    }

    /**
     * We will cycle system scheduler tasks every 10 minutes.
     * Jobs will be re-registered after the task is completed
     */
    private void cycleSystemSchedulerTasks() {
        log.info("re-register system scheduler timer tasks" + TimeUtils.longToTimeString(System.currentTimeMillis()));
        timerTaskScheduler.newTimeout(timeout -> {
            batchSchedulerTimerJob();
            cycleSystemSchedulerTasks();
        }, BATCH_SCHEDULER_INTERVAL_SECONDS, TimeUnit.SECONDS);

    }

    private void batchSchedulerTimerJob() {
        executeTimerJobIdsWithinLastTenMinutesWindow();
    }

    public void scheduleOneJob(T job) throws JobException {
        if (!job.getJobStatus().equals(JobStatus.RUNNING)) {
            return;
        }
        if (!job.getJobConfig().checkIsTimerJob()) {
            //manual job will not scheduler
            if (JobExecuteType.MANUAL.equals(job.getJobConfig().getExecuteType())) {
                if (job.getJobConfig().isImmediate()) {
                    schedulerInstantJob(job, TaskType.MANUAL, null);
                }
                return;
            }

            //todo skip streaming job,improve in the future
            if (JobExecuteType.INSTANT.equals(job.getJobConfig().getExecuteType())) {
                schedulerInstantJob(job, TaskType.SCHEDULED, null);
            }
        }
        if (job.getJobConfig().isImmediate() && JobExecuteType.ONE_TIME.equals(job.getJobConfig().getExecuteType())) {
            schedulerInstantJob(job, TaskType.SCHEDULED, null);
            return;
        }
        //RECURRING job and  immediate is true
        if (job.getJobConfig().isImmediate()) {
            job.getJobConfig().getTimerDefinition().setLatestSchedulerTimeMs(System.currentTimeMillis());
            schedulerInstantJob(job, TaskType.SCHEDULED, null);
        }
        //if it's timer job and trigger last window already start, we will scheduler it immediately
        cycleTimerJobScheduler(job, System.currentTimeMillis());
    }

    @Override
    public void close() throws IOException {
        //todo implement this later
    }


    private void cycleTimerJobScheduler(T job, long startTimeWindowMs) {
        List<Long> delaySeconds = job.getJobConfig().getTriggerDelayTimes(System.currentTimeMillis(),
                startTimeWindowMs, latestBatchSchedulerTimerTaskTimeMs);
        if (CollectionUtils.isNotEmpty(delaySeconds)) {
            delaySeconds.forEach(delaySecond -> {
                TimerJobSchedulerTask<T> timerJobSchedulerTask = new TimerJobSchedulerTask<>(timerJobDisruptor, job);
                timerTaskScheduler.newTimeout(timerJobSchedulerTask, delaySecond, TimeUnit.SECONDS);
            });
        }
    }


    public void schedulerInstantJob(T job, TaskType taskType, C context) throws JobException {
        List<? extends AbstractTask> tasks = job.commonCreateTasks(taskType, context);
        if (CollectionUtils.isEmpty(tasks)) {
            log.info("job create task is empty, skip scheduler, job id is {}, job name is {}", job.getJobId(),
                    job.getJobName());
            if (job.getJobConfig().getExecuteType().equals(JobExecuteType.INSTANT)) {
                job.setJobStatus(JobStatus.FINISHED);
            }
            return;
        }
        for (AbstractTask task : tasks) {
            if (!taskDisruptorGroupManager.dispatchInstantTask(task, job.getJobType(),
                    job.getJobConfig())) {
                throw new JobException("dispatch instant task failed, job id is "
                        + job.getJobId() + ", task id is " + task.getTaskId());
            }
            log.info("dispatch instant job, job id is {}, job name is {}, task id is {}", job.getJobId(),
                    job.getJobName(), task.getTaskId());
        }
    }

    /**
     * We will get the task in the next time window, and then hand it over to the time wheel for timing trigger
     */
    private void executeTimerJobIdsWithinLastTenMinutesWindow() {

        long lastTimeWindowMs = latestBatchSchedulerTimerTaskTimeMs;
        if (latestBatchSchedulerTimerTaskTimeMs < System.currentTimeMillis()) {
            this.latestBatchSchedulerTimerTaskTimeMs = System.currentTimeMillis();
        }
        this.latestBatchSchedulerTimerTaskTimeMs += BATCH_SCHEDULER_INTERVAL_MILLI_SECONDS;
        if (jobMap.isEmpty()) {
            return;
        }
        for (Map.Entry<Long, T> entry : jobMap.entrySet()) {
            T job = entry.getValue();
            if (job.getJobStatus().equals(JobStatus.FINISHED) || job.getJobStatus().equals(JobStatus.STOPPED)) {
                clearEndJob(job);
                continue;
            }
            if (job.getJobStatus().equals(JobStatus.RUNNING) && job.getJobConfig().checkIsTimerJob()) {
                cycleTimerJobScheduler(job, lastTimeWindowMs);
            }
        }
    }

    private void clearEndJob(T job) {
        if (job.getFinishTimeMs() + FINISHED_JOB_CLEANUP_THRESHOLD_TIME_MS > System.currentTimeMillis()) {
            return;
        }
        try {
            Env.getCurrentEnv().getJobManager().unregisterJob(job.getJobId());
        } catch (JobException e) {
            log.error("clear finish job error, job id is {}", job.getJobId(), e);
        }
    }
}
