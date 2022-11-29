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

import org.apache.doris.common.UserException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AsyncJobManager {
    private static volatile AsyncJobManager INSTANCE = null;

    public static AsyncJobManager get() {
        if (INSTANCE == null) {
            synchronized (AsyncJobManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new AsyncJobManager();
                }
            }
        }
        return INSTANCE;
    }

    private final ResourceQueueMgr queueMgr = ResourceQueueMgr.get();
    // jobId -> queue
    private final Map<Long, Long> jobId2Queue = new ConcurrentHashMap<>();
    // label -> jobId
    private final Map<String, Long> jobLabel2Id = new ConcurrentHashMap<>();

    public void submitAsyncJob(AsyncJob job) throws UserException {
        if (jobId2Queue.containsKey(job.jobId()) || jobLabel2Id.containsKey(job.getLabel())) {
            throw new UserException(
                    String.format("Submit the same job(jobId=%d, jobLabel=%s)", job.jobId(), job.getLabel()));
        }
        ResourceQueue queue = queueMgr.matchQueue(job);
        if (queue == null) {
            throw new UserException(
                    String.format("Can't find a matching resource queue(%s=%s, user=%s)", AsyncJob.RESOURCE_QUEUE,
                            job.getResourceQueue(), job.getUser()));
        }
        if (!queue.addJob(job)) {
            throw new UserException(String.format("Resource queue %s is full(pending=%d, running=%d)", queue.getName(),
                    queue.numPendingJobs(), queue.numRunningJobs()));
        }
        job.setMatchedQueue(queue.getName());
        jobId2Queue.put(job.jobId(), queue.queueId());
        jobLabel2Id.put(job.getLabel(), job.jobId());
    }

    public List<List<String>> showJob(long jobId) {
        return null;
    }

    public void cancelJob(long jobId) {
        Long queueId = jobId2Queue.remove(jobId);
        if (queueId != null) {
            ResourceQueue queue = queueMgr.getQueue(queueId);
            AsyncJob job = queue.cancelJob(jobId);
            if (job != null) {
                jobLabel2Id.remove(job.getLabel());
            }
        }
    }

    public void cancelJob(String jobLabel) {
        Long jobId = jobLabel2Id.get(jobLabel);
        if (jobId != null) {
            cancelJob(jobId);
        }
    }
}
