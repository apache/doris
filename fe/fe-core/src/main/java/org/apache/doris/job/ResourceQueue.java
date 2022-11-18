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

package org.apache.doris.job;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.job.AsyncJob.JobStatus;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ResourceQueue {
    private final long id;
    private String name;
    private ResourceQueueConfig config;
    private MatchingPolicy policy;
    private final AsyncJobManager jobMgr;

    // jobId -> job
    private final Map<Long, AsyncJob> allJobs;
    private final BlockingQueue<AsyncJob> pendingQueue;
    private final Map<Long, AsyncJob> runningJobs;
    private final Map<Long, AsyncJob> finishedJobs;
    private Semaphore runningSemaphore;
    private final ReadWriteLock rwlock = new ReentrantReadWriteLock();
    private final Lock rlock = rwlock.readLock();
    private final Lock wlock = rwlock.writeLock();


    public ResourceQueue(String name, AsyncJobManager jobMgr, ResourceQueueConfig config, MatchingPolicy policy) {
        this.id = Env.getCurrentEnv().getNextId();
        this.name = name;
        this.jobMgr = jobMgr;
        this.config = config;
        this.policy = policy;

        allJobs = new HashMap<>();
        pendingQueue = new LinkedBlockingQueue<>();
        runningJobs = new HashMap<>();
        finishedJobs = new HashMap<>();
        runningSemaphore = new Semaphore(config.maxConcurrency());
    }

    public boolean match(AsyncJob job) {
        rlock.lock();
        try {
            return policy.match(job.getUser());
        } finally {
            rlock.unlock();
        }
    }

    public synchronized boolean addJob(AsyncJob job) throws UserException {
        wlock.lock();
        try {
            if (allJobs.containsKey(job.jobId())) {
                job.setJobStatus(JobStatus.CANCELED);
                throw new UserException(String.format("Submit the same job(jobId=%d)", job.jobId()));
            }
            job.setJobStatus(JobStatus.PENDING);
            if (pendingQueue.size() < config.maxQueueSize() && pendingQueue.offer(job)) {
                allJobs.put(job.jobId(), job);
            }
            job.setJobStatus(JobStatus.CANCELED);
            return false;
        } finally {
            wlock.unlock();
        }
    }

    public AsyncJob cancelJob(long jobId) {
        wlock.lock();
        try {
            AsyncJob job = allJobs.get(jobId);
            if (job != null) {
                // cancel should be Idempotent
                job.cancel();
                if (finishedJobs.putIfAbsent(jobId, job) == null) {
                    if (runningJobs.remove(jobId) == null) {
                        pendingQueue.remove(job);
                    }
                }
            }
            return job;
        } finally {
            wlock.unlock();
        }
    }

    public ResourceQueueConfig getConfig() {
        return config;
    }

    public MatchingPolicy getPolicy() {
        return policy;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void alterQueue(ResourceQueueConfig config, MatchingPolicy policy) throws UserException {
        wlock.lock();
        try {
            if (pendingQueue.size() != 0 && runningJobs.size() != 0) {
                throw new UserException(
                        String.format("Should alter queue resource when empty(numPendingJobs=%d, numRunningJobs=%d)",
                                pendingQueue.size(), runningJobs.size()));
            }
            this.config = config;
            this.policy = policy;
            runningSemaphore = new Semaphore(config.maxConcurrency());
        } finally {
            wlock.unlock();
        }
    }

    public boolean canAddJob() {
        rlock.lock();
        try {
            return pendingQueue.size() < config.maxQueueSize();
        } finally {
            rlock.unlock();
        }
    }

    public int numPendingJobs() {
        rlock.lock();
        try {
            return pendingQueue.size();
        } finally {
            rlock.unlock();
        }
    }

    public int numRunningJobs() {
        rlock.lock();
        try {
            return runningJobs.size();
        } finally {
            rlock.unlock();
        }
    }

    public long queueId() {
        return id;
    }

    public void run() {
        while (true) {
            try {
                AsyncJob job = pendingQueue.take();
                runningSemaphore.acquire();
                wlock.lock();
                try {
                    // job may be canceled before adding to runningJobs.
                    if (!finishedJobs.containsKey(job.jobId())) {
                        runningJobs.put(job.jobId(), job);
                    }
                } finally {
                    wlock.unlock();
                }
                // run should be Idempotent
                job.run();
            } catch (InterruptedException e) {
                // do nothing
            }
        }
    }
}
