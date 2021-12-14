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

package org.apache.doris.load.loadv2;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.Config;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.load.FailMsg;

import com.google.common.collect.Queues;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;

/**
 * LoadScheduler will schedule the pending LoadJob which belongs to LoadManager.
 * The function of execute will be called in LoadScheduler.
 * The status of LoadJob will be changed to loading after LoadScheduler.
 */
public class LoadJobScheduler extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(LoadJobScheduler.class);

    private LinkedBlockingQueue<LoadJob> needScheduleJobs = Queues.newLinkedBlockingQueue();

    public LoadJobScheduler() {
        super("Load job scheduler", Config.load_checker_interval_second * 1000);
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            process();
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of LoadJobScheduler with error message {}", e.getMessage(), e);
        }
    }

    private void process() throws InterruptedException {
        while (true) {
            if (needScheduleJobs.isEmpty()) {
                return;
            }

            if (needScheduleJobs.peek() instanceof BrokerLoadJob && !Catalog.getCurrentCatalog().getLoadingLoadTaskScheduler().hasIdleThread()) {
                LOG.info("Failed to take one broker load job from queue because of loading_load_task_scheduler is full." +
                        " Waiting for next round. You can try to increase the value of Config.async_loading_load_task_pool_size");
                return;
            }

            // take one load job from queue
            LoadJob loadJob = needScheduleJobs.poll();

            // schedule job
            try {
                loadJob.execute();
            } catch (LoadException e) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, loadJob.getId())
                        .add("error_msg", "Failed to submit etl job. Job will be cancelled")
                        .build(), e);
                loadJob.cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.ETL_SUBMIT_FAIL, e.getMessage()),
                        true, true);
            } catch (RejectedExecutionException e) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, loadJob.getId())
                        .add("error_msg", "Failed to submit etl job. Job queue is full. retry later")
                        .build(), e);
                needScheduleJobs.put(loadJob);
                return;
            }
        }
    }

    public boolean isQueueFull() {
        return needScheduleJobs.size() > Config.desired_max_waiting_jobs;
    }

    public void submitJob(LoadJob job) {
        needScheduleJobs.add(job);
    }

    public void submitJob(List<LoadJob> jobs) {
        needScheduleJobs.addAll(jobs);
    }
}
