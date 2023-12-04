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
import org.apache.doris.job.common.TaskType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.scheduler.JobScheduler;
import org.apache.doris.job.task.AbstractTask;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Log4j2
public class JobManager<T extends AbstractJob<?, C>, C> implements Writable {


    private final ConcurrentHashMap<Long, T> jobMap = new ConcurrentHashMap<>(32);

    private JobScheduler jobScheduler;

    public void start() {
        jobScheduler = new JobScheduler(jobMap);
        jobScheduler.start();
    }

    public void registerJob(T job) throws JobException {
        job.checkJobParams();
        checkJobNameExist(job.getJobName());
        if (jobMap.get(job.getJobId()) != null) {
            throw new JobException("job id exist, jobId:" + job.getJobId());
        }
        Env.getCurrentEnv().getEditLog().logCreateJob(job);
        jobMap.put(job.getJobId(), job);
        //check its need to scheduler
        jobScheduler.scheduleOneJob(job);
    }


    private void checkJobNameExist(String jobName) throws JobException {
        if (jobMap.values().stream().anyMatch(a -> a.getJobName().equals(jobName))) {
            throw new JobException("job name exist, jobName:" + jobName);
        }
    }

    public void unregisterJob(Long jobId) throws JobException {
        checkJobExist(jobId);
        jobMap.get(jobId).setJobStatus(JobStatus.STOPPED);
        jobMap.get(jobId).cancelAllTasks();
        Env.getCurrentEnv().getEditLog().logDeleteJob(jobMap.get(jobId));
        jobMap.remove(jobId);
    }

    public void unregisterJob(String jobName) throws JobException {
        for (T a : jobMap.values()) {
            if (a.getJobName().equals(jobName)) {
                try {
                    unregisterJob(a.getJobId());
                } catch (JobException e) {
                    throw new JobException("unregister job error, jobName:" + jobName);
                }
            }
        }

    }

    public void alterJobStatus(Long jobId, JobStatus status) throws JobException {
        checkJobExist(jobId);
        jobMap.get(jobId).updateJobStatus(status);
        Env.getCurrentEnv().getEditLog().logUpdateJob(jobMap.get(jobId));
    }

    public void alterJobStatus(String jobName, JobStatus jobStatus) throws JobException {
        for (T a : jobMap.values()) {
            if (a.getJobName().equals(jobName)) {
                try {
                    if (jobStatus.equals(JobStatus.STOPPED)) {
                        unregisterJob(a.getJobId());
                        return;
                    }
                    alterJobStatus(a.getJobId(), jobStatus);
                } catch (JobException e) {
                    throw new JobException("unregister job error, jobName:" + jobName);
                }
            }
        }
    }

    private void checkJobExist(Long jobId) throws JobException {
        if (null == jobMap.get(jobId)) {
            throw new JobException("job not exist, jobId:" + jobId);
        }
    }

    public List<T> queryJobs(JobType type) {
        return jobMap.values().stream().filter(a -> a.getJobType().equals(type))
                .collect(java.util.stream.Collectors.toList());
    }

    public List<T> queryJobs(JobType jobType, String jobName) {
        //only query insert job,we just provide insert job
        return jobMap.values().stream().filter(a -> checkItsMatch(jobType, jobName, a))
                .collect(Collectors.toList());
    }

    /**
     * query jobs by job type
     *
     * @param jobTypes @JobType
     * @return List<AbstractJob> job list
     */
    public List<T> queryJobs(List<JobType> jobTypes) {
        return jobMap.values().stream().filter(a -> checkItsMatch(jobTypes, a))
                .collect(Collectors.toList());
    }

    private boolean checkItsMatch(JobType jobType, String jobName, T job) {
        if (null == jobType) {
            throw new IllegalArgumentException("jobType cannot be null");
        }
        if (StringUtils.isBlank(jobName)) {
            return job.getJobType().equals(jobType);
        }
        return job.getJobType().equals(jobType) && job.getJobName().equals(jobName);
    }

    private boolean checkItsMatch(List<JobType> jobTypes, T job) {
        if (null == jobTypes) {
            throw new IllegalArgumentException("jobType cannot be null");
        }
        return jobTypes.contains(job.getJobType());
    }

    public List<? extends AbstractTask> queryTasks(Long jobId) throws JobException {
        checkJobExist(jobId);
        return jobMap.get(jobId).queryAllTasks();
    }

    public void triggerJob(long jobId, C context) throws JobException {
        checkJobExist(jobId);
        jobScheduler.schedulerInstantJob(jobMap.get(jobId), TaskType.MANUAL, context);
    }

    public void replayCreateJob(T job) {
        if (jobMap.containsKey(job.getJobId())) {
            return;
        }
        jobMap.putIfAbsent(job.getJobId(), job);
        log.info(new LogBuilder(LogKey.SCHEDULER_JOB, job.getJobId())
                .add("msg", "replay create scheduler job").build());
    }

    /**
     * Replay update load job.
     **/
    public void replayUpdateJob(T job) {
        jobMap.put(job.getJobId(), job);
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

    public void cancelTaskById(String jobName, Long taskId) throws JobException {
        for (T job : jobMap.values()) {
            if (job.getJobName().equals(jobName)) {
                job.cancelTaskById(taskId);
                return;
            }
        }
        throw new JobException("job not exist, jobName:" + jobName);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(jobMap.size());
        jobMap.forEach((jobId, job) -> {
            try {
                job.write(out);
            } catch (IOException e) {
                log.error("write job error, jobId:" + jobId, e);
            }
        });
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
