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
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.job.common.FailureReason;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.streaming.AbstractStreamingTask;
import org.apache.doris.job.extensions.insert.streaming.StreamingInsertJob;

import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Log4j2
public class StreamingTaskScheduler extends MasterDaemon {
    private final ThreadPoolExecutor threadPool;

    {
        threadPool = new ThreadPoolExecutor(
                Config.job_streaming_task_exec_thread_num,
                Config.job_streaming_task_exec_thread_num,
                60L,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(Config.max_streaming_job_num),
                new CustomThreadFactory("streaming-task-execute"),
                new ThreadPoolExecutor.AbortPolicy()
        );
        threadPool.allowCoreThreadTimeOut(true);
    }

    private final ScheduledThreadPoolExecutor delayScheduler
                = new ScheduledThreadPoolExecutor(1, new CustomThreadFactory("streaming-task-delay-scheduler"));

    private static long DELAY_SCHEDULER_MS = 500;

    public StreamingTaskScheduler() {
        super("Streaming-task-scheduler", 1);
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            process();
        } catch (Throwable e) {
            log.warn("Failed to process one round of StreamingTaskScheduler", e);
        }
    }

    private void process() throws InterruptedException {
        List<AbstractStreamingTask> tasks = new ArrayList<>();
        LinkedBlockingDeque<AbstractStreamingTask> needScheduleTasksQueue =
                Env.getCurrentEnv().getJobManager().getStreamingTaskManager().getNeedScheduleTasksQueue();
        tasks.add(needScheduleTasksQueue.take());
        needScheduleTasksQueue.drainTo(tasks);
        scheduleTasks(tasks);
    }

    private void scheduleTasks(List<AbstractStreamingTask> tasks) {
        for (AbstractStreamingTask task : tasks) {
            threadPool.execute(() -> {
                try {
                    scheduleOneTask(task);
                } catch (Exception e) {
                    log.warn("Failed to schedule task, task id {}, job id {}",
                            task.getTaskId(), task.getJobId(), e);
                    StreamingInsertJob job =
                            (StreamingInsertJob) Env.getCurrentEnv().getJobManager().getJob(task.getJobId());
                    job.setFailureReason(new FailureReason(e.getMessage()));
                    try {
                        job.updateJobStatus(JobStatus.PAUSED);
                    } catch (JobException ex) {
                        log.warn("Failed to pause job {} after task {} scheduling failed",
                                task.getJobId(), task.getTaskId(), ex);
                    }
                }
            });
        }
    }

    private void scheduleOneTask(AbstractStreamingTask task) {
        if (DebugPointUtil.isEnable("StreamingJob.scheduleTask.exception")) {
            throw new RuntimeException("debug point StreamingJob.scheduleTask.exception");
        }
        StreamingInsertJob job = (StreamingInsertJob) Env.getCurrentEnv().getJobManager().getJob(task.getJobId());
        if (job == null) {
            log.warn("Job not found, job id: {}", task.getJobId());
            return;
        }

        // reject invalid task
        if (!job.needScheduleTask()) {
            log.info("do not need to schedule invalid task, task id: {}, job id: {}, job status: {}",
                        task.getTaskId(), task.getJobId(), job.getJobStatus());
            return;
        }
        // reject task if no more data to consume
        if (!job.hasMoreDataToConsume()) {
            String delayMsg = "No data available for consumption at the moment, will retry after "
                    + (System.currentTimeMillis() + DELAY_SCHEDULER_MS);
            job.setJobRuntimeMsg(delayMsg);
            scheduleTaskWithDelay(task, DELAY_SCHEDULER_MS);
            return;
        }
        log.info("prepare to schedule task, task id: {}, job id: {}", task.getTaskId(), task.getJobId());
        job.setLastScheduleTaskTimestamp(System.currentTimeMillis());
        Env.getCurrentEnv().getJobManager().getStreamingTaskManager().addRunningTask(task);
        // clear delay msg
        job.setJobRuntimeMsg("");
        long start = System.currentTimeMillis();
        try {
            task.execute();
            log.info("Finished executing task, task id: {}, job id: {}, cost {}ms",
                    task.getTaskId(), task.getJobId(), System.currentTimeMillis() - start);
        } catch (Exception e) {
            log.error("Failed to execute task, task id: {}, job id: {}", task.getTaskId(), task.getJobId(), e);
        }
    }

    private void scheduleTaskWithDelay(AbstractStreamingTask task, long delayMs) {
        delayScheduler.schedule(() -> {
            Env.getCurrentEnv().getJobManager().getStreamingTaskManager().registerTask(task);
        }, delayMs, TimeUnit.MILLISECONDS);
    }
}
