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
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.streaming.StreamingInsertJob;
import org.apache.doris.job.extensions.insert.streaming.StreamingInsertTask;

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
    private final LinkedBlockingDeque<StreamingInsertTask> needScheduleTasksQueue = new LinkedBlockingDeque<>();
    private final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
                    0,
                    Config.max_streaming_job_num,
                    60,
                    TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(Config.max_streaming_job_num),
                    new CustomThreadFactory("streaming-task-execute"),
                    new ThreadPoolExecutor.AbortPolicy()
            );
    private final ScheduledThreadPoolExecutor delayScheduler
                = new ScheduledThreadPoolExecutor(1, new CustomThreadFactory("streaming-task-delay-scheduler"));

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

    public void registerTask(StreamingInsertTask task) {
        needScheduleTasksQueue.add(task);
    }

    private void process() throws InterruptedException {
        List<StreamingInsertTask> tasks = new ArrayList<>();
        tasks.add(needScheduleTasksQueue.take());
        needScheduleTasksQueue.drainTo(tasks);
        scheduleTasks(tasks);
    }

    private void scheduleTasks(List<StreamingInsertTask> tasks) {
        for (StreamingInsertTask task : tasks) {
            threadPool.execute(() -> {
                try {
                    scheduleOneTask(task);
                } catch (Exception e) {
                    log.error("Failed to schedule task, task id: {}, job id: {}", task.getTaskId(), task.getJobId(), e);
                }
            });
        }
    }

    private void scheduleOneTask(StreamingInsertTask task) throws JobException {
        StreamingInsertJob job = (StreamingInsertJob) Env.getCurrentEnv().getJobManager().getJob(task.getJobId());
        if (job == null) {
            log.warn("Job not found, job id: {}", task.getJobId());
            return;
        }
        if (!job.needScheduleTask()) {
            log.info("do not need to schedule invalid task, task id: {}, job id: {}",
                        task.getTaskId(), task.getJobId());
            return;
        }
        if (job.hasMoreDataToConsume()) {
            scheduleTaskWithDelay(task, 500);
            return;
        }
        if (job.needDelayScheduleTask()) {
            scheduleTaskWithDelay(task, 500);
            return;
        }
        log.info("prepare to schedule task, task id: {}, job id: {}", task.getTaskId(), task.getJobId());
        task.execute();
    }

    private void scheduleTaskWithDelay(StreamingInsertTask task, long delayMs) {
        delayScheduler.schedule(() -> {
            needScheduleTasksQueue.add(task);
        }, delayMs, TimeUnit.MILLISECONDS);
    }
}
