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

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.scheduler.exception.JobException;
import org.apache.doris.scheduler.executor.TransientTaskExecutor;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Job for managing concurrent execution of Iceberg data file rewrite
 * operations.
 * Similar to ExportJob, this class manages multiple RewriteTaskExecutor
 * instances
 * that execute rewrite operations in parallel.
 */
@Data
public class RewriteJob {
    private static final Logger LOG = LogManager.getLogger(RewriteJob.class);

    @SerializedName("id")
    private long id;

    @SerializedName("label")
    private String label;

    @SerializedName("state")
    private RewriteJobState state;

    @SerializedName("createTimeMs")
    private long createTimeMs;

    @SerializedName("startTimeMs")
    private long startTimeMs;

    @SerializedName("finishTimeMs")
    private long finishTimeMs;

    @SerializedName("progress")
    private int progress;

    @SerializedName("failMsg")
    private RewriteFailMsg failMsg;

    // Job configuration
    private IcebergExternalTable icebergTable;
    private RewriteDataFileManager.Parameters parameters;
    private ConnectContext connectContext;

    // Parallelism control
    private int parallelism;

    // Task management
    private List<RewriteTaskExecutor> taskExecutorList = Lists.newArrayList();
    private AtomicInteger finishedTaskCount = new AtomicInteger(0);
    private RewriteResult totalResult = new RewriteResult();

    // Groups to be processed
    private List<RewriteDataGroup> allGroups = Lists.newArrayList();

    public RewriteJob(long jobId, String label, IcebergExternalTable icebergTable,
            RewriteDataFileManager.Parameters parameters, ConnectContext connectContext) {
        this.id = jobId;
        this.label = label;
        this.icebergTable = icebergTable;
        this.parameters = parameters;
        this.connectContext = connectContext;
        this.state = RewriteJobState.PENDING;
        this.progress = 0;
        this.createTimeMs = System.currentTimeMillis();
        this.startTimeMs = -1;
        this.finishTimeMs = -1;
        this.failMsg = new RewriteFailMsg(RewriteFailMsg.CancelType.UNKNOWN, "");
        this.parallelism = 1; // Default parallelism
    }

    /**
     * Initialize the rewrite job with groups and create task executors
     */
    public void initializeWithGroups(List<RewriteDataGroup> groups, int parallelism) throws UserException {
        this.allGroups = Lists.newArrayList(groups);
        this.parallelism = Math.min(parallelism, groups.size());

        LOG.info("Initializing rewrite job {} with {} groups, parallelism: {}",
                id, groups.size(), this.parallelism);

        // Create task executors for each group
        generateTaskExecutors();
    }

    /**
     * Generate task executors for parallel execution
     */
    private void generateTaskExecutors() throws UserException {
        taskExecutorList = Lists.newArrayList();

        // Distribute groups among executors
        List<List<RewriteDataGroup>> groupsPerExecutor = distributeGroups();

        for (int i = 0; i < groupsPerExecutor.size(); i++) {
            List<RewriteDataGroup> executorGroups = groupsPerExecutor.get(i);
            RewriteTaskExecutor executor = new RewriteTaskExecutor(executorGroups, this);
            taskExecutorList.add(executor);

            LOG.debug("Created task executor {} with {} groups", i, executorGroups.size());
        }

        // Add empty task if no groups to process
        if (taskExecutorList.isEmpty()) {
            RewriteTaskExecutor executor = new RewriteTaskExecutor(Lists.newArrayList(), this);
            taskExecutorList.add(executor);
        }
    }

    /**
     * Distribute groups among executors based on parallelism
     */
    private List<List<RewriteDataGroup>> distributeGroups() {
        List<List<RewriteDataGroup>> groupsPerExecutor = Lists.newArrayList();

        // Initialize executor lists
        for (int i = 0; i < parallelism; i++) {
            groupsPerExecutor.add(Lists.newArrayList());
        }

        // Distribute groups round-robin
        for (int i = 0; i < allGroups.size(); i++) {
            int executorIndex = i % parallelism;
            groupsPerExecutor.get(executorIndex).add(allGroups.get(i));
        }

        return groupsPerExecutor;
    }

    /**
     * Get task executors for scheduling
     */
    public List<? extends TransientTaskExecutor> getTaskExecutors() {
        return Lists.newArrayList(taskExecutorList);
    }

    /**
     * Update job state
     */
    public synchronized void updateJobState(RewriteJobState newState, Long taskId,
            RewriteResult result, RewriteFailMsg.CancelType type, String msg)
            throws JobException {
        switch (newState) {
            case PENDING:
                throw new JobException("Cannot update job state to PENDING");
            case REWRITING:
                startRewriteJob();
                break;
            case CANCELLED:
                cancelRewriteJob(type, msg);
                break;
            case FINISHED:
                finishRewriteTask(taskId, result);
                break;
            default:
                return;
        }
    }

    private void startRewriteJob() throws JobException {
        if (state == RewriteJobState.CANCELLED || state == RewriteJobState.FINISHED) {
            throw new JobException("Job has been {}, cannot start rewriting", state);
        }

        if (state == RewriteJobState.REWRITING) {
            return;
        }

        state = RewriteJobState.REWRITING;
        startTimeMs = System.currentTimeMillis();
        LOG.info("Started rewrite job {}", id);
    }

    private void cancelRewriteJob(RewriteFailMsg.CancelType type, String msg) throws JobException {
        if (state == RewriteJobState.CANCELLED) {
            return;
        }

        if (state == RewriteJobState.FINISHED) {
            throw new JobException("Job {} has finished, cannot be cancelled", id);
        }

        if (state == RewriteJobState.PENDING) {
            startTimeMs = System.currentTimeMillis();
        }

        // Cancel all task executors
        taskExecutorList.forEach(executor -> {
            try {
                // Note: This would need integration with TransientTaskManager
                // Env.getCurrentEnv().getTransientTaskManager().cancelMemoryTask(executor.getId());
            } catch (Exception e) {
                LOG.warn("Failed to cancel rewrite task {}: {}", executor.getId(), e.getMessage());
            }
        });

        cancelRewriteJobUnprotected(type, msg);
    }

    private void cancelRewriteJobUnprotected(RewriteFailMsg.CancelType type, String msg) {
        state = RewriteJobState.CANCELLED;
        finishTimeMs = System.currentTimeMillis();
        failMsg = new RewriteFailMsg(type, msg);
        taskExecutorList.clear();
        allGroups.clear();
        LOG.info("Cancelled rewrite job {}", id);
    }

    private void finishRewriteTask(Long taskId, RewriteResult result) throws JobException {
        if (state == RewriteJobState.CANCELLED) {
            throw new JobException("Job {} has been cancelled, cannot finish task {}", id, taskId);
        }

        // Merge result
        if (result != null) {
            totalResult.merge(result);
        }

        int finished = finishedTaskCount.incrementAndGet();

        // Calculate progress
        int tmpProgress = finished * 100 / taskExecutorList.size();
        if (finished * 100 / taskExecutorList.size() >= 100) {
            progress = 99;
        } else {
            progress = tmpProgress;
        }

        // Check if all tasks finished
        if (finished == taskExecutorList.size()) {
            finishRewriteJobUnprotected();
        }
    }

    private void finishRewriteJobUnprotected() {
        progress = 100;
        state = RewriteJobState.FINISHED;
        finishTimeMs = System.currentTimeMillis();

        // Clean up resources
        taskExecutorList.clear();
        allGroups.clear();

        LOG.info("Finished rewrite job {}, total result: {}", id, totalResult);
    }

    public synchronized boolean isFinalState() {
        return state == RewriteJobState.CANCELLED || state == RewriteJobState.FINISHED;
    }

    public RewriteResult getTotalResult() {
        return totalResult;
    }

    @Override
    public String toString() {
        return "RewriteJob{" +
                "id=" + id +
                ", label='" + label + '\'' +
                ", state=" + state +
                ", progress=" + progress +
                ", createTimeMs=" + createTimeMs +
                ", startTimeMs=" + startTimeMs +
                ", finishTimeMs=" + finishTimeMs +
                ", failMsg=" + failMsg +
                '}';
    }
}
