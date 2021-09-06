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

package org.apache.doris.load.sync;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.load.sync.SyncJob.JobState;
import org.apache.doris.task.MasterTask;
import org.apache.doris.task.MasterTaskExecutor;

import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class SyncChecker extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(SyncChecker.class);

    private JobState jobState;

    // checkers for running sync jobs
    private static Map<JobState, SyncChecker> checkers = Maps.newHashMap();

    // executors for sync tasks
    private static Map<JobState, MasterTaskExecutor> executors = Maps.newHashMap();

    private SyncChecker(JobState jobState, long intervalMs) {
        super("sync checker " + jobState.name().toLowerCase(), intervalMs * 1000);
        this.jobState = jobState;
    }

    public static void init(long intervalMs) {
        checkers.put(JobState.PENDING, new SyncChecker(JobState.PENDING, intervalMs));

        int poolSize = 3;

        MasterTaskExecutor pendingTaskExecutor = new MasterTaskExecutor("sync_pending_job", poolSize, true);
        executors.put(JobState.PENDING, pendingTaskExecutor);
    }

    public static void startAll() {
        for (SyncChecker syncChecker : checkers.values()) {
            syncChecker.start();
        }
        for (MasterTaskExecutor masterTaskExecutor : executors.values()) {
            masterTaskExecutor.start();
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        LOG.debug("start check export jobs. job state: {}", jobState.name());
        switch (jobState) {
            case PENDING:
                runPendingJobs();
                break;
            default:
                LOG.warn("wrong sync job state: {}", jobState.name());
                break;
        }
    }

    private void runPendingJobs() {
        SyncJobManager syncJobMgr = Catalog.getCurrentCatalog().getSyncJobManager();
        List<SyncJob> pendingJobs = syncJobMgr.getSyncJobs(JobState.PENDING);
        for (SyncJob job : pendingJobs) {
            try {
                MasterTask task = new SyncPendingTask(job);
                if (executors.get(JobState.PENDING).submit(task)) {
                    LOG.info("run pending sync job. job: {}", job);
                }
            } catch (Exception e) {
                LOG.warn("run pending sync job error", e);
            }
        }
    }
}
