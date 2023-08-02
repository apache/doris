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

package org.apache.doris.scheduler;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.scheduler.constants.JobCategory;
import org.apache.doris.scheduler.executor.JobExecutor;
import org.apache.doris.scheduler.job.Job;
import org.apache.doris.scheduler.manager.AsyncJobManager;
import org.apache.doris.scheduler.registry.PersistentJobRegister;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;

/**
 * This class registers timed scheduling events using the Netty time wheel algorithm to trigger events in a timely
 * manner.
 * After the event is triggered, it is produced by the Disruptor producer and consumed by the consumer, which is an
 * asynchronous
 * consumption model that does not guarantee strict timing accuracy.
 */
@Slf4j
public class AsyncJobRegister implements PersistentJobRegister {

    private final AsyncJobManager asyncJobManager;

    public AsyncJobRegister(AsyncJobManager asyncJobManager) {
        this.asyncJobManager = asyncJobManager;
    }

    @Override
    public Long registerJob(String name, Long intervalMs, JobExecutor executor) throws DdlException {
        return this.registerJob(name, intervalMs, null, null, executor);
    }

    @Override
    public Long registerJob(String name, Long intervalMs, Long startTimeMs, JobExecutor executor) throws DdlException {
        return this.registerJob(name, intervalMs, startTimeMs, null, executor);
    }

    @Override
    public Long registerJob(String name, Long intervalMs, Long startTimeMs, Long endTimeStamp,
                            JobExecutor executor) throws DdlException {

        Job job = new Job(name, intervalMs, startTimeMs, endTimeStamp, executor);
        return asyncJobManager.registerJob(job);
    }

    @Override
    public Long registerJob(Job job) throws DdlException {
        return asyncJobManager.registerJob(job);
    }

    @Override
    public void pauseJob(Long jobId) {
        asyncJobManager.pauseJob(jobId);
    }

    @Override
    public void pauseJob(String dbName, String jobName, JobCategory jobCategory) throws DdlException {
        asyncJobManager.pauseJob(dbName, jobName, jobCategory);
    }

    @Override
    public void resumeJob(String dbName, String jobName, JobCategory jobCategory) throws DdlException {
        asyncJobManager.resumeJob(dbName, jobName, jobCategory);
    }

    @Override
    public void stopJob(Long jobId) {
        asyncJobManager.stopJob(jobId);
    }

    @Override
    public void stopJob(String dbName, String jobName, JobCategory jobCategory) throws DdlException {
        asyncJobManager.stopJob(dbName, jobName, jobCategory);
    }

    @Override
    public void resumeJob(Long jobId) {
        asyncJobManager.resumeJob(jobId);
    }

    @Override
    public List<Job> getJobs(String dbFullName, String jobName, JobCategory jobCategory, PatternMatcher matcher) {
        return asyncJobManager.queryJob(dbFullName, jobName, jobCategory, matcher);
    }

    @Override
    public void close() throws IOException {
        asyncJobManager.close();
    }
}
