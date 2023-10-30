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

package org.apache.doris.scheduler.manager;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.CustomThreadFactory;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.scheduler.constants.JobCategory;
import org.apache.doris.scheduler.constants.JobStatus;
import org.apache.doris.scheduler.disruptor.TaskDisruptor;
import org.apache.doris.scheduler.job.Job;
import org.apache.doris.scheduler.job.TimerJobTask;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
public class TimerJobManager implements Closeable, Writable {

    private final ConcurrentHashMap<Long, Job> jobMap = new ConcurrentHashMap<>(128);
    private long lastBatchSchedulerTimestamp;
    private static final long BATCH_SCHEDULER_INTERVAL_SECONDS = 600;

    /**
     * batch scheduler interval ms time
     */
    private static final long BATCH_SCHEDULER_INTERVAL_MILLI_SECONDS = BATCH_SCHEDULER_INTERVAL_SECONDS * 1000L;

    private boolean isClosed = false;

    /**
     * key: jobid
     * value: timeout list  for one job
     * it's used to cancel task, if task has started, it can't be canceled
     */
    private final ConcurrentHashMap<Long, Map<Long, Timeout>> jobTimeoutMap = new ConcurrentHashMap<>(128);

    /**
     * scheduler tasks, it's used to scheduler job
     */
    private HashedWheelTimer dorisTimer;

    /**
     * Producer and Consumer model
     * disruptor is used to handle task
     * disruptor will start a thread pool to handle task
     */
    @Setter
    private TaskDisruptor disruptor;

    public TimerJobManager() {
        this.lastBatchSchedulerTimestamp = System.currentTimeMillis();
    }

    public void start() {
        dorisTimer = new HashedWheelTimer(new CustomThreadFactory("hashed-wheel-timer"),
                1, TimeUnit.SECONDS, 660);
        dorisTimer.start();
        Long currentTimeMs = System.currentTimeMillis();
        jobMap.forEach((jobId, job) -> {
            Long nextExecuteTimeMs = findFistExecuteTime(currentTimeMs, job.getStartTimeMs(),
                    job.getIntervalMs(), job.isCycleJob());
            job.setNextExecuteTimeMs(nextExecuteTimeMs);
        });
        batchSchedulerTasks();
        cycleSystemSchedulerTasks();
    }

    public Long registerJob(Job job) throws DdlException {
        job.checkJobParam();
        checkIsJobNameUsed(job.getDbName(), job.getJobName(), job.getJobCategory());
        jobMap.putIfAbsent(job.getJobId(), job);
        initAndSchedulerJob(job);
        Env.getCurrentEnv().getEditLog().logCreateJob(job);
        return job.getJobId();
    }

    public void replayCreateJob(Job job) {
        if (jobMap.containsKey(job.getJobId())) {
            return;
        }
        jobMap.putIfAbsent(job.getJobId(), job);
        initAndSchedulerJob(job);
        log.info(new LogBuilder(LogKey.SCHEDULER_JOB, job.getJobId())
                .add("msg", "replay create scheduler job").build());
    }

    /**
     * Replay update load job.
     **/
    public void replayUpdateJob(Job job) {
        jobMap.put(job.getJobId(), job);
        if (JobStatus.RUNNING.equals(job.getJobStatus())) {
            initAndSchedulerJob(job);
        }
        log.info(new LogBuilder(LogKey.SCHEDULER_JOB, job.getJobId())
                .add("msg", "replay update scheduler job").build());
    }

    public void replayDeleteJob(Job job) {
        if (null == jobMap.get(job.getJobId())) {
            return;
        }
        jobMap.remove(job.getJobId());
        log.info(new LogBuilder(LogKey.SCHEDULER_JOB, job.getJobId())
                .add("msg", "replay delete scheduler job").build());
        Env.getCurrentEnv().getJobTaskManager().deleteJobTasks(job.getJobId());
    }

    private void checkIsJobNameUsed(String dbName, String jobName, JobCategory jobCategory) throws DdlException {
        Optional<Job> optionalJob = jobMap.values().stream().filter(job -> job.getJobCategory().equals(jobCategory))
                .filter(job -> job.getDbName().equals(dbName))
                .filter(job -> job.getJobName().equals(jobName)).findFirst();
        if (optionalJob.isPresent()) {
            throw new DdlException("Name " + jobName + " already used in db " + dbName);
        }
    }

    private void initAndSchedulerJob(Job job) {
        if (!job.getJobStatus().equals(JobStatus.RUNNING)) {
            return;
        }

        Long currentTimeMs = System.currentTimeMillis();
        Long nextExecuteTimeMs = findFistExecuteTime(currentTimeMs, job.getStartTimeMs(),
                job.getIntervalMs(), job.isCycleJob());
        job.setNextExecuteTimeMs(nextExecuteTimeMs);
        if (job.getNextExecuteTimeMs() < lastBatchSchedulerTimestamp) {
            List<Long> executeTimestamp = findTasksBetweenTime(job,
                    lastBatchSchedulerTimestamp,
                    job.getNextExecuteTimeMs());
            if (!executeTimestamp.isEmpty()) {
                for (Long timestamp : executeTimestamp) {
                    putOneTask(job.getJobId(), timestamp);
                }
            }
        }
    }

    private Long findFistExecuteTime(Long currentTimeMs, Long startTimeMs, Long intervalMs, boolean isCycleJob) {
        // if job not delay, first execute time is start time
        if (startTimeMs != 0L && startTimeMs > currentTimeMs) {
            return startTimeMs;
        }
        // if job already delay, first execute time is current time
        if (startTimeMs != 0L && startTimeMs < currentTimeMs) {
            return currentTimeMs;
        }
        // if it's cycle job and not set start tine, first execute time is current time + interval
        if (isCycleJob && startTimeMs == 0L) {
            return currentTimeMs + intervalMs;
        }
        // if it's not cycle job and already delay, first execute time is current time
        return currentTimeMs;
    }

    public void unregisterJob(Long jobId) {
        jobMap.remove(jobId);
    }

    public void pauseJob(Long jobId) {
        Job job = jobMap.get(jobId);
        if (jobMap.get(jobId) == null) {
            log.warn("pauseJob failed, jobId: {} not exist", jobId);
        }
        if (jobMap.get(jobId).getJobStatus().equals(JobStatus.PAUSED)) {
            log.warn("pauseJob failed, jobId: {} is already paused", jobId);
        }
        pauseJob(job);
    }

    public void stopJob(String dbName, String jobName, JobCategory jobCategory) throws DdlException {
        Optional<Job> optionalJob = findJob(dbName, jobName, jobCategory);

        if (!optionalJob.isPresent()) {
            throw new DdlException("Job " + jobName + " not exist in db " + dbName);
        }
        Job job = optionalJob.get();
        if (job.getJobStatus().equals(JobStatus.STOPPED)) {
            throw new DdlException("Job " + jobName + " is already stopped");
        }
        stopJob(optionalJob.get());
        Env.getCurrentEnv().getEditLog().logDeleteJob(optionalJob.get());
    }

    private void stopJob(Job job) {
        if (JobStatus.RUNNING.equals(job.getJobStatus())) {
            cancelJobAllTask(job.getJobId());
        }
        job.setJobStatus(JobStatus.STOPPED);
        jobMap.get(job.getJobId()).stop();
        Env.getCurrentEnv().getEditLog().logDeleteJob(job);
        Env.getCurrentEnv().getJobTaskManager().deleteJobTasks(job.getJobId());
    }


    public void resumeJob(String dbName, String jobName, JobCategory jobCategory) throws DdlException {
        Optional<Job> optionalJob = findJob(dbName, jobName, jobCategory);
        if (!optionalJob.isPresent()) {
            throw new DdlException("Job " + jobName + " not exist in db " + dbName);
        }
        Job job = optionalJob.get();
        if (!job.getJobStatus().equals(JobStatus.PAUSED)) {
            throw new DdlException("Job " + jobName + " is not paused");
        }
        resumeJob(job);
    }

    private void resumeJob(Job job) {
        cancelJobAllTask(job.getJobId());
        job.setJobStatus(JobStatus.RUNNING);
        jobMap.get(job.getJobId()).resume();
        initAndSchedulerJob(job);
        Env.getCurrentEnv().getEditLog().logUpdateJob(job);
    }

    public void pauseJob(String dbName, String jobName, JobCategory jobCategory) throws DdlException {
        Optional<Job> optionalJob = findJob(dbName, jobName, jobCategory);
        if (!optionalJob.isPresent()) {
            throw new DdlException("Job " + jobName + " not exist in db " + dbName);
        }
        Job job = optionalJob.get();
        if (!job.getJobStatus().equals(JobStatus.RUNNING)) {
            throw new DdlException("Job " + jobName + " is not running");
        }
        pauseJob(job);
    }

    private void pauseJob(Job job) {
        cancelJobAllTask(job.getJobId());
        job.setJobStatus(JobStatus.PAUSED);
        jobMap.get(job.getJobId()).pause();
        Env.getCurrentEnv().getEditLog().logUpdateJob(job);
    }

    public void finishJob(long jobId) {
        Job job = jobMap.get(jobId);
        if (jobMap.get(jobId) == null) {
            log.warn("update job status failed, jobId: {} not exist", jobId);
        }
        if (jobMap.get(jobId).getJobStatus().equals(JobStatus.FINISHED)) {
            return;
        }
        job.setLatestCompleteExecuteTimeMs(System.currentTimeMillis());
        cancelJobAllTask(job.getJobId());
        job.setJobStatus(JobStatus.FINISHED);
        Env.getCurrentEnv().getEditLog().logUpdateJob(job);
    }

    private Optional<Job> findJob(String dbName, String jobName, JobCategory jobCategory) {
        return jobMap.values().stream().filter(job -> checkJobMatch(job, dbName, jobName, jobCategory)).findFirst();
    }

    private boolean checkJobMatch(Job job, String dbName, String jobName, JobCategory jobCategory) {
        return job.getDbName().equals(dbName) && job.getJobName().equals(jobName)
                && job.getJobCategory().equals(jobCategory);
    }


    public void resumeJob(Long jobId) {
        if (jobMap.get(jobId) == null) {
            log.warn("resumeJob failed, jobId: {} not exist", jobId);
            return;
        }
        Job job = jobMap.get(jobId);
        resumeJob(job);
    }

    public void stopJob(Long jobId) {
        Job job = jobMap.get(jobId);
        if (null == job) {
            log.warn("stopJob failed, jobId: {} not exist", jobId);
            return;
        }
        if (job.getJobStatus().equals(JobStatus.STOPPED)) {
            log.warn("stopJob failed, jobId: {} is already stopped", jobId);
            return;
        }
        stopJob(job);
    }

    public Job getJob(Long jobId) {
        return jobMap.get(jobId);
    }

    public Map<Long, Job> getAllJob() {
        return jobMap;
    }

    public void batchSchedulerTasks() {
        executeJobIdsWithinLastTenMinutesWindow();
    }

    private List<Long> findTasksBetweenTime(Job job, Long endTimeEndWindow, Long nextExecuteTime) {

        List<Long> jobExecuteTimes = new ArrayList<>();
        if (!job.isCycleJob() && (nextExecuteTime < endTimeEndWindow)) {
            jobExecuteTimes.add(nextExecuteTime);
            return jobExecuteTimes;
        }
        if (job.isCycleJob() && (nextExecuteTime > endTimeEndWindow)) {
            return new ArrayList<>();
        }
        while (endTimeEndWindow >= nextExecuteTime) {
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
        // if the task executes for more than 10 minutes, it will be delay, so,
        // set lastBatchSchedulerTimestamp to current time
        if (lastBatchSchedulerTimestamp < System.currentTimeMillis()) {
            this.lastBatchSchedulerTimestamp = System.currentTimeMillis();
        }
        this.lastBatchSchedulerTimestamp += BATCH_SCHEDULER_INTERVAL_MILLI_SECONDS;
        if (jobMap.isEmpty()) {
            return;
        }
        jobMap.forEach((k, v) -> {
            if (v.isRunning() && (v.getNextExecuteTimeMs()
                    + v.getIntervalMs() < lastBatchSchedulerTimestamp)) {
                List<Long> executeTimes = findTasksBetweenTime(
                        v, lastBatchSchedulerTimestamp,
                        v.getNextExecuteTimeMs());
                if (!executeTimes.isEmpty()) {
                    for (Long executeTime : executeTimes) {
                        putOneTask(v.getJobId(), executeTime);
                    }
                }
            }
        });
    }

    /**
     * We will cycle system scheduler tasks every 10 minutes.
     * Jobs will be re-registered after the task is completed
     */
    private void cycleSystemSchedulerTasks() {
        log.info("re-register system scheduler tasks" + TimeUtils.longToTimeString(System.currentTimeMillis()));
        dorisTimer.newTimeout(timeout -> {
            batchSchedulerTasks();
            clearFinishJob();
            cycleSystemSchedulerTasks();
        }, BATCH_SCHEDULER_INTERVAL_SECONDS, TimeUnit.SECONDS);

    }

    /**
     * put one task to time wheel,it's well be trigger after delay milliseconds
     * if the scheduler is closed, the task will not be put into the time wheel
     * if delay is less than 0, the task will be trigger immediately
     *
     * @param jobId            job id, we will use it to find the job
     * @param startExecuteTime the task will be trigger in this time, unit is millisecond,and we will convert it to
     *                         delay seconds, we just can be second precision
     */
    public void putOneTask(Long jobId, Long startExecuteTime) {
        if (isClosed) {
            log.info("putOneTask failed, scheduler is closed, jobId: {}", jobId);
            return;
        }
        long taskId = System.nanoTime();
        TimerJobTask task = new TimerJobTask(jobId, taskId, startExecuteTime, disruptor);
        long delay = getDelaySecond(task.getStartTimestamp());
        Timeout timeout = dorisTimer.newTimeout(task, delay, TimeUnit.SECONDS);
        if (timeout == null) {
            log.error("putOneTask failed, jobId: {}", task.getJobId());
            return;
        }
        if (jobTimeoutMap.containsKey(task.getJobId())) {
            jobTimeoutMap.get(task.getJobId()).put(task.getTaskId(), timeout);
            JobTaskManager.addPrepareTaskStartTime(jobId, taskId, startExecuteTime);
            return;
        }
        Map<Long, Timeout> timeoutMap = new ConcurrentHashMap<>();
        timeoutMap.put(task.getTaskId(), timeout);
        jobTimeoutMap.put(task.getJobId(), timeoutMap);
        JobTaskManager.addPrepareTaskStartTime(jobId, taskId, startExecuteTime);
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
        JobTaskManager.clearPrepareTaskByJobId(jobId);
    }

    // get delay time, if startTimestamp is less than now, return 0
    private long getDelaySecond(long startTimestamp) {
        long delay = 0;
        long now = System.currentTimeMillis();
        if (startTimestamp > now) {
            delay = startTimestamp - now;
        } else {
            //if execute time is less than now, return 0,immediately execute
            log.warn("startTimestamp is less than now, startTimestamp: {}, now: {}", startTimestamp, now);
            return delay;
        }
        return delay / 1000;
    }

    @Override
    public void close() throws IOException {
        isClosed = true;
        dorisTimer.stop();
        disruptor.close();
    }

    /**
     * sort by job id
     *
     * @param dbFullName database name
     * @param category   job category
     * @param matcher    job name matcher
     */
    public List<Job> queryJob(String dbFullName, String jobName, JobCategory category, PatternMatcher matcher) {
        List<Job> jobs = new ArrayList<>();
        jobMap.values().forEach(job -> {
            if (matchJob(job, dbFullName, jobName, category, matcher)) {
                jobs.add(job);
            }
        });
        return jobs;
    }

    private boolean matchJob(Job job, String dbFullName, String jobName, JobCategory category, PatternMatcher matcher) {
        if (StringUtils.isNotBlank(dbFullName) && !job.getDbName().equalsIgnoreCase(dbFullName)) {
            return false;
        }
        if (StringUtils.isNotBlank(jobName) && !job.getJobName().equalsIgnoreCase(jobName)) {
            return false;
        }
        if (category != null && !job.getJobCategory().equals(category)) {
            return false;
        }
        return null == matcher || matcher.match(job.getJobName());
    }

    public void putOneJobToQueen(Long jobId) {
        long taskId = System.nanoTime();
        JobTaskManager.addPrepareTaskStartTime(jobId, taskId, System.currentTimeMillis());
        disruptor.tryPublish(jobId, taskId);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(jobMap.size());
        for (Job job : jobMap.values()) {
            job.write(out);
        }
    }

    /**
     * read job from data input, and init job
     *
     * @param in data input
     * @throws IOException io exception when read data input error
     */
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            Job job = Job.readFields(in);
            jobMap.putIfAbsent(job.getJobId(), job);
        }
    }

    /**
     * clear finish job，if  job finish time is more than @Config.finish_job_max_saved_second, we will delete it
     * this method will be called every 10 minutes, therefore, the actual maximum
     * deletion time is Config.finish_job_max_saved_second + 10 min.
     * we could to delete job in time, but it's not make sense.start
     */
    private void clearFinishJob() {
        Long now = System.currentTimeMillis();
        jobMap.values().forEach(job -> {
            if (job.isFinished() && now - job.getLatestCompleteExecuteTimeMs() > Config.finish_job_max_saved_second) {
                jobMap.remove(job.getJobId());
                Env.getCurrentEnv().getEditLog().logDeleteJob(job);
                Env.getCurrentEnv().getJobTaskManager().deleteJobTasks(job.getJobId());
                log.debug("delete finish job:{}", job.getJobId());
            }
        });

    }
}
