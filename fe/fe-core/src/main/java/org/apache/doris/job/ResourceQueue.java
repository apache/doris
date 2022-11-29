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

import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
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

    private Thread scheduleThread = null;
    private final BlockingQueue<AsyncJob> pendingQueue;
    private final Map<Long, AsyncJob> pendingJobs;
    private final Map<Long, AsyncJob> runningJobs;
    private final Map<Long, AsyncJob> finishedJobs;
    private final Map<Long, AsyncJob> allJobs;
    private Semaphore runningSemaphore;
    private final ReadWriteLock rwlock = new ReentrantReadWriteLock();
    private final Lock rlock = rwlock.readLock();
    private final Lock wlock = rwlock.writeLock();


    public ResourceQueue(String name, ResourceQueueConfig config, MatchingPolicy policy) {
        this.id = Env.getCurrentEnv().getNextId();
        this.name = name;
        this.config = config;
        this.policy = policy;

        pendingQueue = new LinkedBlockingQueue<>();
        pendingJobs = new HashMap<>();
        runningJobs = new HashMap<>();
        finishedJobs = new HashMap<>();
        allJobs = new HashMap<>();
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

    public boolean addJob(AsyncJob job) throws UserException {
        wlock.lock();
        try {
            if (allJobs.containsKey(job.jobId())) {
                job.setJobStatus(JobStatus.CANCELED);
                throw new UserException(String.format("Submit the same job(jobId=%d)", job.jobId()));
            }
            job.setJobStatus(JobStatus.PENDING);
            if (runningJobs.size() < config.maxConcurrency() || pendingJobs.size() < config.maxQueueSize()) {
                if (pendingQueue.offer(job)) {
                    allJobs.put(job.jobId(), job);
                    pendingJobs.put(job.jobId(), job);
                    return true;
                }
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
                finishedJobs.putIfAbsent(jobId, job);
                pendingQueue.remove(job);
                pendingJobs.remove(jobId);
                runningJobs.remove(jobId);
            }
            return job;
        } finally {
            wlock.unlock();
        }
    }

    public ResourceQueueConfig getConfig() {
        rlock.lock();
        try {
            return config;
        } finally {
            rlock.unlock();
        }
    }

    public MatchingPolicy getPolicy() {
        rlock.lock();
        try {
            return policy;
        } finally {
            rlock.unlock();
        }
    }

    public String getName() {
        rlock.lock();
        try {
            return name;
        } finally {
            rlock.unlock();
        }
    }

    public void setName(String name) throws UserException {
        wlock.lock();
        try {
            if (pendingJobs.size() != 0 && runningJobs.size() != 0) {
                throw new UserException(
                        String.format("Should rename resource queue when empty(numPendingJobs=%d, numRunningJobs=%d)",
                                pendingJobs.size(), runningJobs.size()));
            }
            this.name = name;
        } finally {
            wlock.unlock();
        }
    }

    public void alterQueue(ResourceQueueConfig config, MatchingPolicy policy) throws UserException {
        wlock.lock();
        try {
            if (pendingJobs.size() != 0 && runningJobs.size() != 0) {
                throw new UserException(
                        String.format("Should alter resource queue when empty(numPendingJobs=%d, numRunningJobs=%d)",
                                pendingJobs.size(), runningJobs.size()));
            }
            this.config = config;
            this.policy = policy;
            runningSemaphore = new Semaphore(config.maxConcurrency());
        } finally {
            wlock.unlock();
        }
    }

    public void dropQueue() throws UserException {
        wlock.lock();
        try {
            if (pendingJobs.size() != 0 && runningJobs.size() != 0) {
                throw new UserException(
                        String.format("Should drop resource queue when empty(numPendingJobs=%d, numRunningJobs=%d)",
                                pendingJobs.size(), runningJobs.size()));
            }
            if (scheduleThread != null) {
                scheduleThread.interrupt();
            }
        } finally {
            wlock.unlock();
        }
    }

    public boolean canAddJob() {
        rlock.lock();
        try {
            return runningJobs.size() < config.maxConcurrency() || pendingJobs.size() < config.maxQueueSize();
        } finally {
            rlock.unlock();
        }
    }

    public int numPendingJobs() {
        rlock.lock();
        try {
            return pendingJobs.size();
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

    public List<String> showQueueInfo() {
        rlock.lock();
        try {
            List<String> row = Lists.newArrayList();
            row.add(String.valueOf(id));
            row.add(name);
            row.add(String.valueOf(pendingJobs.size()));
            row.add(String.valueOf(runningJobs.size()));
            row.add(config.toString());
            row.add(policy.toString());
            return row;
        } finally {
            rlock.unlock();
        }
    }

    public void run() {
        scheduleThread = new Thread(() -> {
            while (true) {
                try {
                    AsyncJob job = pendingQueue.take();
                    runningSemaphore.acquire();
                    wlock.lock();
                    try {
                        // job may be canceled before adding to runningJobs.
                        if (!finishedJobs.containsKey(job.jobId())) {
                            pendingJobs.remove(job.jobId());
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
        }, "resource-queue-" + name);
        scheduleThread.setDaemon(true);
        scheduleThread.start();
    }
}
