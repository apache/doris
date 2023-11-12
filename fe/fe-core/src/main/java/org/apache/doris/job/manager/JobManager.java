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

package org.apache.doris.job.manager;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.scheduler.JobScheduler;
import org.apache.doris.job.task.AbstractTask;

import lombok.extern.slf4j.Slf4j;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class JobManager<T extends AbstractJob<?>> implements Writable {


    private final ConcurrentHashMap<Long, T> jobMap = new ConcurrentHashMap<>(32);

    private JobScheduler jobScheduler;

    public void start() {
        jobScheduler = new JobScheduler(jobMap);
        jobScheduler.start();
    }

    Long registerJob(T job) throws JobException {
        job.checkJobParams();
        checkJobNameExist(job.getJobName(), job.getJobType());
        long id = Env.getCurrentEnv().getNextId();
        job.setJobId(id);
        replayCreateJob(job);
        //check name exist
        jobMap.put(id, job);
        //check its need to scheduler
        jobScheduler.scheduleOneJob(job);
        return id;
    }


    private void checkJobNameExist(String jobName, JobType type) throws JobException {
        if (jobMap.values().stream().anyMatch(a -> a.getJobName().equals(jobName) && a.getJobType().equals(type))) {
            throw new JobException("job name exist,jobName:" + jobName);
        }
    }

    void unregisterJob(Long jobId) throws JobException {
        checkJobExist(jobId);
        jobMap.get(jobId).setJobStatus(JobStatus.STOPPED);
        jobMap.get(jobId).cancel();
        replayDeleteJob(jobMap.get(jobId));
        jobMap.remove(jobId);
    }

    void alterJobStatus(Long jobId, JobStatus status) throws JobException {
        checkJobExist(jobId);
        jobMap.get(jobId).updateJobStatus(status);
    }

    void resumeJob(Long jobId) throws JobException {
        checkJobExist(jobId);
        replayUpdateJob(jobMap.get(jobId));
        jobMap.get(jobId).resumeJob();
        jobScheduler.scheduleOneJob(jobMap.get(jobId));
    }

    private void checkJobExist(Long jobId) throws JobException {
        if (null == jobMap.get(jobId)) {
            throw new JobException("job not exist,jobId:" + jobId);
        }
    }

    List<T> queryJobs(JobType type) {
        return jobMap.values().stream().filter(a -> a.getJobType().equals(type))
                .collect(java.util.stream.Collectors.toList());
    }

    List<? extends AbstractTask> queryTasks(Long jobId) throws JobException {
        checkJobExist(jobId);
        return jobMap.get(jobId).queryTasks();
    }

    public void replayCreateJob(T job) {
        if (jobMap.containsKey(job.getJobId())) {
            return;
        }
        jobMap.putIfAbsent(job.getJobId(), job);
        jobScheduler.scheduleOneJob(job);
        log.info(new LogBuilder(LogKey.SCHEDULER_JOB, job.getJobId())
                .add("msg", "replay create scheduler job").build());
    }

    /**
     * Replay update load job.
     **/
    public void replayUpdateJob(T job) {
        jobMap.put(job.getJobId(), job);
        if (JobStatus.RUNNING.equals(job.getJobStatus())) {
            jobScheduler.scheduleOneJob(job);
        }
        log.info(new LogBuilder(LogKey.SCHEDULER_JOB, job.getJobId())
                .add("msg", "replay update scheduler job").build());
    }

    public void replayDeleteJob(T job) {
        if (null == jobMap.get(job.getJobId())) {
            return;
        }
        jobMap.remove(job.getJobId());
        log.info(new LogBuilder(LogKey.SCHEDULER_JOB, job.getJobId())
                .add("msg", "replay delete scheduler job").build());
    }

    void cancelTask(Long jobId, Long taskId) throws JobException {
        checkJobExist(jobId);
        if (null == jobMap.get(jobId).getRunningTasks()) {
            throw new JobException("task not exist,taskId:" + taskId);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(jobMap.size());
        for (AbstractJob job : jobMap.values()) {
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
            AbstractJob job = AbstractJob.readFields(in);
            jobMap.putIfAbsent(job.getJobId(), (T) job);
        }
    }

    public T getJob(Long jobId) {
        return jobMap.get(jobId);
    }
}
