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

package org.apache.doris.scheduler.job;

import org.apache.doris.scheduler.disruptor.TimerTaskDisruptor;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
public class AsyncJobManager implements Closeable {

    private final ConcurrentHashMap<Long, Job> jobMap = new ConcurrentHashMap<>(128);

    private long lastBatchSchedulerTimestamp;

    /**
     * batch scheduler interval time
     */
    private static final long BATCH_SCHEDULER_INTERVAL_MILLI_SECONDS = 10 * 60 * 1000L;

    private static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;


    private boolean isClosed = false;

    /**
     * key: jobid
     * value: timeout list  for one job
     * it's used to cancel task, if task has started, it can't be canceled
     */
    private final ConcurrentHashMap<Long, Map<Long, Timeout>> jobTimeoutMap =
            new ConcurrentHashMap<>(128);

    /**
     * scheduler tasks, it's used to scheduler job
     */
    private final HashedWheelTimer dorisTimer = new HashedWheelTimer(1, TimeUnit.SECONDS,
            660);

    /**
     * Producer and Consumer model
     * disruptor is used to handle task
     * disruptor will start a thread pool to handle task
     */
    private final TimerTaskDisruptor disruptor;

    public AsyncJobManager() {
        dorisTimer.start();
        this.disruptor = new TimerTaskDisruptor(this);
        this.lastBatchSchedulerTimestamp = System.currentTimeMillis();
        batchSchedulerTasks();
        cycleSystemSchedulerTasks();
    }

    public Long registerJob(Job job) {
        if (!job.checkJobParam()) {
            log.warn("registerJob failed, job: {} param is invalid", job);
            return null;
        }
        if (job.getStartTimestamp() != 0L) {
            job.setNextExecuteTimestamp(job.getStartTimestamp() + job.getIntervalMilliSeconds());
        } else {
            job.setNextExecuteTimestamp(System.currentTimeMillis() + job.getIntervalMilliSeconds());
        }

        if (job.getNextExecuteTimestamp() < BATCH_SCHEDULER_INTERVAL_MILLI_SECONDS + lastBatchSchedulerTimestamp) {
            List<Long> executeTimestamp = findTasksBetweenTime(job, System.currentTimeMillis(),
                    BATCH_SCHEDULER_INTERVAL_MILLI_SECONDS + lastBatchSchedulerTimestamp,
                    job.getNextExecuteTimestamp());
            if (!executeTimestamp.isEmpty()) {
                for (Long timestamp : executeTimestamp) {
                    putOneTask(job.getJobId(), timestamp);
                }
            }
        }

        jobMap.putIfAbsent(job.getJobId(), job);
        return job.getJobId();
    }

    public void unregisterJob(Long jobId) {
        jobMap.remove(jobId);
    }

    public boolean pauseJob(Long jobId) {
        if (jobMap.get(jobId) == null) {
            log.warn("pauseJob failed, jobId: {} not exist", jobId);
            return false;
        }
        cancelJobAllTask(jobId);
        jobMap.get(jobId).pause();
        return true;
    }

    public boolean resumeJob(Long jobId) {
        if (jobMap.get(jobId) == null) {
            log.warn("resumeJob failed, jobId: {} not exist", jobId);
            return false;
        }
        jobMap.get(jobId).resume();
        return true;
    }

    public boolean stopJob(Long jobId) {
        if (jobMap.get(jobId) == null) {
            log.warn("stopJob failed, jobId: {} not exist", jobId);
            return false;
        }
        cancelJobAllTask(jobId);
        jobMap.get(jobId).stop();
        return true;
    }

    public Job getJob(Long jobId) {
        return jobMap.get(jobId);
    }

    public Map<Long, Job> getAllJob() {
        return jobMap;
    }

    public boolean batchSchedulerTasks() {
        executeJobIdsWithinLastTenMinutesWindow();
        return true;
    }

    public List<Long> findTasksBetweenTime(Job job, Long startTime, Long endTime, Long nextExecuteTime) {
        List<Long> jobExecuteTimes = new ArrayList<>();
        if (System.currentTimeMillis() < startTime) {
            return jobExecuteTimes;
        }
        while (endTime >= nextExecuteTime) {
            if (job.isTaskTimeExceeded()) {
                break;
            }
            jobExecuteTimes.add(nextExecuteTime);
            nextExecuteTime = job.getExecuteTimestampAndGeneratorNext();
        }
        return jobExecuteTimes;
    }

    /**
     * We will get the task in the next time window, and then hand it over to the time wheel for timing trigger
     */
    private void executeJobIdsWithinLastTenMinutesWindow() {
        if (jobMap.isEmpty()) {
            return;
        }
        jobMap.forEach((k, v) -> {
            if (v.isRunning() && (v.getNextExecuteTimestamp() + v.getIntervalMilliSeconds()
                    < lastBatchSchedulerTimestamp + BATCH_SCHEDULER_INTERVAL_MILLI_SECONDS)) {
                List<Long> executeTimes = findTasksBetweenTime(v, lastBatchSchedulerTimestamp,
                        lastBatchSchedulerTimestamp + BATCH_SCHEDULER_INTERVAL_MILLI_SECONDS,
                        v.getNextExecuteTimestamp());
                if (!executeTimes.isEmpty()) {
                    for (Long executeTime : executeTimes) {
                        putOneTask(v.getJobId(), executeTime);
                    }
                }
            }
        });
        this.lastBatchSchedulerTimestamp += BATCH_SCHEDULER_INTERVAL_MILLI_SECONDS;
    }

    /**
     * We will cycle system scheduler tasks every 10 minutes.
     * Jobs will be re-registered after the task is completed
     */
    private void cycleSystemSchedulerTasks() {
        dorisTimer.newTimeout(timeout -> {
            batchSchedulerTasks();
            cycleSystemSchedulerTasks();
        }, BATCH_SCHEDULER_INTERVAL_MILLI_SECONDS, TimeUnit.MILLISECONDS);
    }

    public void putOneTask(Long jobId, Long startExecuteTime) {
        DorisTimerTask task = new DorisTimerTask(jobId, startExecuteTime, disruptor);
        if (isClosed) {
            log.info("putOneTask failed, scheduler is closed, jobId: {}", task.getJobId());
            return;
        }
        long delay = getDelaySecond(task.getStartTimestamp());
        Timeout timeout = dorisTimer.newTimeout(task, delay, TimeUnit.SECONDS);
        if (timeout == null) {
            log.error("putOneTask failed, jobId: {}", task.getJobId());
            return;
        }
        if (jobTimeoutMap.containsKey(task.getJobId())) {
            jobTimeoutMap.get(task.getJobId()).put(task.getTaskId(), timeout);
            return;
        }
        Map<Long, Timeout> timeoutMap = new ConcurrentHashMap<>();
        timeoutMap.put(task.getTaskId(), timeout);
        jobTimeoutMap.put(task.getJobId(), timeoutMap);
    }

    // cancel all task for one job
    // if task has started, it can't be canceled
    public void cancelJobAllTask(Long jobId) {
        if (!jobTimeoutMap.containsKey(jobId)) {
            return;
        }

        jobTimeoutMap.get(jobId).values().forEach(timeout -> {
            if (!timeout.isExpired() || timeout.isCancelled()) {
                timeout.cancel();
            }
        });
    }

    public void stopTask(Long jobId, Long taskId) {
        if (!jobTimeoutMap.containsKey(jobId)) {
            return;
        }
        cancelJobAllTask(jobId);
        jobTimeoutMap.get(jobId).remove(taskId);
    }

    // get delay time, if startTimestamp is less than now, return 0
    private long getDelaySecond(long startTimestamp) {
        long delay = 0;
        long now = System.currentTimeMillis();
        if (startTimestamp > now) {
            delay = startTimestamp - now;
        } else {
            log.warn("startTimestamp is less than now, startTimestamp: {}, now: {}", startTimestamp, now);
        }
        return delay / 1000;
    }

    @Override
    public void close() throws IOException {
        isClosed = true;
        dorisTimer.stop();
        disruptor.close();
    }
}
