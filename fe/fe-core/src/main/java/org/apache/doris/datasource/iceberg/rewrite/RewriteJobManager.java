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

package org.apache.doris.datasource.iceberg.rewrite;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.scheduler.exception.JobException;
import org.apache.doris.scheduler.executor.TransientTaskExecutor;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manager for Iceberg rewrite jobs, similar to ExportMgr.
 * Handles job lifecycle, scheduling, and result collection.
 */
public class RewriteJobManager {
    private static final Logger LOG = LogManager.getLogger(RewriteJobManager.class);

    // Job storage
    private Map<Long, RewriteJob> rewriteIdToJob = Maps.newHashMap();

    // Lock for protecting rewrite jobs
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    public RewriteJobManager() {
    }

    /**
     * Create and start a new rewrite job
     */
    public RewriteJob createAndStartJob(String label, IcebergExternalTable icebergTable,
            RewriteDataFilePlanner.Parameters parameters,
            ConnectContext connectContext, int parallelism) throws Exception {
        long jobId = Env.getCurrentEnv().getNextId();
        RewriteJob job = new RewriteJob(jobId, label, icebergTable, connectContext);

        writeLock();
        try {
            // Add job to manager
            rewriteIdToJob.put(jobId, job);
            LOG.info("Created rewrite job: {}", job);
        } finally {
            writeUnlock();
        }

        return job;
    }

    /**
     * Start a rewrite job with groups
     */
    public void startRewriteJob(RewriteJob job, List<RewriteDataGroup> groups, int parallelism) throws Exception {
        // Initialize job with groups
        job.initializeWithGroups(groups, parallelism);

        // Register task executors with TransientTaskManager
        try {
            for (TransientTaskExecutor executor : job.getTaskExecutors()) {
                Env.getCurrentEnv().getTransientTaskManager().addMemoryTask(executor);
            }
        } catch (Exception e) {
            // If task registration fails, cancel the job
            job.updateJobState(RewriteJobState.CANCELLED, 0L, null,
                    RewriteFailMsg.CancelType.RUN_FAIL, e.getMessage());
            throw e;
        }

        LOG.info("Started rewrite job {} with {} groups, parallelism: {}",
                job.getId(), groups.size(), parallelism);
    }

    /**
     * Get job by ID
     */
    public RewriteJob getJob(long jobId) {
        readLock();
        try {
            return rewriteIdToJob.get(jobId);
        } finally {
            readUnlock();
        }
    }

    /**
     * Get all jobs
     */
    public List<RewriteJob> getJobs() {
        readLock();
        try {
            return Lists.newArrayList(rewriteIdToJob.values());
        } finally {
            readUnlock();
        }
    }

    /**
     * Cancel a rewrite job
     */
    public void cancelJob(long jobId, RewriteFailMsg.CancelType cancelType, String msg) throws UserException {
        writeLock();
        try {
            RewriteJob job = rewriteIdToJob.get(jobId);
            if (job == null) {
                throw new UserException("Rewrite job " + jobId + " not found");
            }

            if (job.isFinalState()) {
                throw new UserException("Rewrite job " + jobId + " is already in final state: " + job.getState());
            }

            try {
                job.updateJobState(RewriteJobState.CANCELLED, 0L, null, cancelType, msg);
            } catch (JobException e) {
                throw new UserException("Failed to cancel rewrite job: " + e.getMessage());
            }

            LOG.info("Cancelled rewrite job {}", jobId);
        } finally {
            writeUnlock();
        }
    }

    /**
     * Remove old completed jobs
     */
    public void removeOldJobs() {
        long currentTimeMs = System.currentTimeMillis();
        long maxAgeMs = 24 * 60 * 60 * 1000L; // 24 hours

        writeLock();
        try {
            rewriteIdToJob.entrySet().removeIf(entry -> {
                RewriteJob job = entry.getValue();
                return job.isFinalState() && (currentTimeMs - job.getCreateTimeMs()) > maxAgeMs;
            });
        } finally {
            writeUnlock();
        }
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }
}
