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

import org.apache.doris.scheduler.executor.JobExecutor;
import org.apache.doris.scheduler.job.AsyncJobManager;
import org.apache.doris.scheduler.job.Job;
import org.apache.doris.scheduler.registry.JobRegister;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * This class registers timed scheduling events using the Netty time wheel algorithm to trigger events in a timely
 * manner.
 * After the event is triggered, it is produced by the Disruptor producer and consumed by the consumer, which is an
 * asynchronous
 * consumption model that does not guarantee strict timing accuracy.
 */
@Slf4j
public class AsyncJobRegister implements JobRegister {

    private final AsyncJobManager asyncJobManager;

    public AsyncJobRegister() {
        this.asyncJobManager = new AsyncJobManager();
    }

    @Override
    public Long registerJob(String name, Long intervalMs, JobExecutor executor) {
        return this.registerJob(name, intervalMs, null, null, executor);
    }

    @Override
    public Long registerJob(String name, Long intervalMs, Long startTimeStamp, JobExecutor executor) {
        return this.registerJob(name, intervalMs, startTimeStamp, null, executor);
    }

    @Override
    public Long registerJob(String name, Long intervalMs, Long startTimeStamp, Long endTimeStamp,
                                 JobExecutor executor) {

        Job job = new Job(name, intervalMs, startTimeStamp, endTimeStamp, executor);
        return asyncJobManager.registerJob(job);
    }

    @Override
    public Boolean pauseJob(Long jobId) {
        return asyncJobManager.pauseJob(jobId);
    }

    @Override
    public Boolean stopJob(Long jobId) {
        return asyncJobManager.stopJob(jobId);
    }

    @Override
    public Boolean resumeJob(Long jobId) {
        return asyncJobManager.resumeJob(jobId);
    }

    @Override
    public void close() throws IOException {
        asyncJobManager.close();
    }
}
