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

package org.apache.doris.task;

import org.apache.doris.load.LoadJob;

import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;

// One pull load job
@Deprecated
public class PullLoadJob {
    private static final Logger LOG = LogManager.getLogger(PullLoadTask.class);

    public enum State {
        RUNNING,
        FINISHED,
        CANCELED,
        FAILED,
        UNKNOWN;

        public boolean isRunning() {
            return this == RUNNING;
        }
    }

    // Input params
    public final LoadJob job;
    public final List<PullLoadTask> tasks;

    public Set<Integer> finishedTask;

    // Used to
    public volatile State state;
    // Only used when this job has failed.
    public PullLoadTask failureTask;

    public PullLoadJob(LoadJob job, List<PullLoadTask> tasks) {
        this.job = job;
        this.tasks = tasks;
        finishedTask = Sets.newHashSet();
        state = State.RUNNING;
    }

    public long getId() {
        return job.getId();
    }

    public synchronized State getState() {
        return state;
    }

    public synchronized boolean isRunning() {
        return state.isRunning();
    }

    public synchronized void cancel() {
        state = State.CANCELED;
        for (PullLoadTask task : tasks) {
            task.cancel();
        }
    }

    public PullLoadTask getFailureTask() {
        return failureTask;
    }

    public synchronized void onTaskFinished(PullLoadTask task) {
        int taskId = task.taskId;
        if (!state.isRunning()) {
            LOG.info("Ignore task info after this job has been stable. taskId={}:{}", task.jobId, taskId);
            return;
        }
        if (!finishedTask.add(taskId)) {
            LOG.info("Receive duplicate task information. taskId={}:{}", task.jobId, taskId);
        }
        if (finishedTask.size() == tasks.size()) {
            state = State.FINISHED;
        }
    }

    public synchronized void onTaskFailed(PullLoadTask task) {
        if (!state.isRunning()) {
            LOG.info("Ignore task info after this job has been stable. taskId={}:{}", task.jobId, task.taskId);
            return;
        }
        state = State.FAILED;
        failureTask = task;
    }
}
