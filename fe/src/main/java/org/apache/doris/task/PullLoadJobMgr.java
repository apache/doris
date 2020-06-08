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

import org.apache.doris.common.Status;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;

@Deprecated
public class PullLoadJobMgr {
    private static final Logger LOG = LogManager.getLogger(PullLoadJobMgr.class);

    // Lock protect
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<Long, PullLoadJob> idToJobs = Maps.newHashMap();

    private final BlockingQueue<PullLoadTask> pendingTasks = Queues.newLinkedBlockingQueue();
    private ExecutorService executorService;

    private int concurrency = 10;

    public PullLoadJobMgr() {
        executorService = ThreadPoolManager.newDaemonCacheThreadPool(concurrency, "pull-load-job-mgr");
    }

    /**
     * Start Task manager to work.
     * First it will ask all backends to collect task status
     * After collected, this will start scheduler to work.
     */
    public void start() {
        for (int i = 0; i < concurrency; ++i) {
            executorService.submit(new TaskExecutor());
        }
    }

    /**
     * Submit a load job.
     * This is called when a job turn from pending to ETL.
     * Or this can be called before 'start' called.
     */
    public void submit(PullLoadJob job) {
        lock.lock();
        try {
            if (idToJobs.containsKey(job.getId())) {
                // Same job id contains
                return;
            }
            idToJobs.put(job.getId(), job);
            for (PullLoadTask task : job.tasks) {
                pendingTasks.add(task);
            }
        } finally {
            lock.unlock();
        }
    }

    public PullLoadJob.State getJobState(long jobId) {
        lock.lock();
        try {
            PullLoadJob ctx = idToJobs.get(jobId);
            if (ctx == null) {
                return PullLoadJob.State.UNKNOWN;
            }
            return ctx.state;
        } finally {
            lock.unlock();
        }
    }

    // NOTE:
    //  Must call this after job's state is not running
    //  Otherwise, the returned object will be process concurrently,
    //  this could be lead to an invalid situation
    public PullLoadJob getJob(long jobId) {
        lock.lock();
        try {
            return idToJobs.get(jobId);
        } finally {
            lock.unlock();
        }
    }

    // Cancel one job, remove all its tasks
    public void cancelJob(long jobId) {
        PullLoadJob job = null;
        lock.lock();
        try {
            job = idToJobs.remove(jobId);
        } finally {
            lock.unlock();
        }
        // Cancel job out of lock guard
        if (job != null) {
            job.cancel();
        }
    }

    // Only used when master replay job
    public void remove(long jobId) {
        lock.lock();
        try {
            idToJobs.remove(jobId);
        } finally {
            lock.unlock();
        }
    }

    public boolean isFailureCanRetry(Status status) {
        return true;
    }

    public class TaskExecutor implements Runnable {

        private void processOneTask(PullLoadTask task, PullLoadJob job) throws UserException {
            int retryTime = 3;
            for (int i = 0; i < retryTime; ++i) {
                if (!job.isRunning()) {
                    throw new UserException("Job has been cancelled.");
                }
                task.executeOnce();
                if (task.isFinished()) {
                    return;
                } else {
                    boolean needRetry = isFailureCanRetry(task.getExecuteStatus());
                    if (!needRetry) {
                        break;
                    }
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {

                    }
                }
            }
        }

        @Override
        public void run() {
            while (true) {
                PullLoadTask task;
                PullLoadJob job;
                try {
                    task = pendingTasks.take();
                    if (task == null) {
                        continue;
                    }
                    job = getJob(task.jobId);
                    if (job == null || !job.isRunning()) {
                        LOG.info("Job is not running now. taskId={}:{}", task.jobId, task.taskId);
                        continue;
                    }
                } catch (InterruptedException e) {
                    LOG.info("Interrupted when take task.");
                    continue;
                }
                try {
                    processOneTask(task, job);
                    if (task.isFinished()) {
                        job.onTaskFinished(task);
                    } else {
                        job.onTaskFailed(task);
                    }
                } catch (Throwable e) {
                    LOG.warn("Process one pull load task exception. job id: {}, task id: {}",
                            task.jobId, task.taskId, e);
                    task.onFailed(null, new Status(TStatusCode.INTERNAL_ERROR, e.getMessage()));
                    job.onTaskFailed(task);
                }
            }
        }
    }
}
