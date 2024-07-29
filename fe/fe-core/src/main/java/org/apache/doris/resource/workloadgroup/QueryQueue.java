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

package org.apache.doris.resource.workloadgroup;

import org.apache.doris.common.UserException;
import org.apache.doris.resource.workloadgroup.QueueToken.TokenState;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.PriorityQueue;
import java.util.concurrent.locks.ReentrantLock;

// note(wb) refer java BlockingQueue, but support altering capacity
// todo(wb) add wait time to profile
public class QueryQueue {

    private static final Logger LOG = LogManager.getLogger(QueryQueue.class);
    // note(wb) used unfair by default, need more test later
    private final ReentrantLock queueLock = new ReentrantLock();
    // resource group property
    private int maxConcurrency;
    private int maxQueueSize;
    private int queueTimeout; // ms
    // running property
    private volatile int currentRunningQueryNum;

    public static final String RUNNING_QUERY_NUM = "running_query_num";
    public static final String WAITING_QUERY_NUM = "waiting_query_num";

    private long wgId;

    private long propVersion;

    private PriorityQueue<QueueToken> priorityTokenQueue;

    int getCurrentRunningQueryNum() {
        return currentRunningQueryNum;
    }

    int getCurrentWaitingQueryNum() {
        try {
            queueLock.lock();
            return priorityTokenQueue.size();
        } finally {
            queueLock.unlock();
        }
    }

    long getPropVersion() {
        return propVersion;
    }

    long getWgId() {
        return wgId;
    }

    int getMaxConcurrency() {
        return maxConcurrency;
    }

    int getMaxQueueSize() {
        return maxQueueSize;
    }

    int getQueueTimeout() {
        return queueTimeout;
    }

    public QueryQueue(long wgId, int maxConcurrency, int maxQueueSize, int queueTimeout, long propVersion) {
        this.wgId = wgId;
        this.maxConcurrency = maxConcurrency;
        this.maxQueueSize = maxQueueSize;
        this.queueTimeout = queueTimeout;
        this.propVersion = propVersion;
        this.priorityTokenQueue = new PriorityQueue<QueueToken>();
    }

    public String debugString() {
        return "wgId= " + wgId + ", version=" + this.propVersion + ",maxConcurrency=" + maxConcurrency
                + ", maxQueueSize=" + maxQueueSize + ", queueTimeout=" + queueTimeout
                + ", currentRunningQueryNum=" + currentRunningQueryNum
                + ", currentWaitingQueryNum=" + priorityTokenQueue.size();
    }

    public QueueToken getToken() throws UserException {
        queueLock.lock();
        try {
            if (LOG.isDebugEnabled()) {
                LOG.info(this.debugString());
            }
            if (currentRunningQueryNum < maxConcurrency) {
                QueueToken retToken = new QueueToken(TokenState.READY_TO_RUN, queueTimeout, this);
                retToken.complete();
                currentRunningQueryNum++;
                return retToken;
            }
            if (priorityTokenQueue.size() >= maxQueueSize) {
                throw new UserException("query waiting queue is full, queue length=" + maxQueueSize);
            }
            QueueToken newQueryToken = new QueueToken(TokenState.ENQUEUE_SUCCESS, queueTimeout,
                    this);
            this.priorityTokenQueue.offer(newQueryToken);
            return newQueryToken;
        } finally {
            if (LOG.isDebugEnabled()) {
                LOG.info(this.debugString());
            }
            queueLock.unlock();
        }
    }

    public void releaseAndNotify(QueueToken releaseToken) {
        queueLock.lock();
        try {
            // NOTE:token's tokenState need to be locked by queueLock
            if (releaseToken.isReadyToRun()) {
                currentRunningQueryNum--;
            } else {
                priorityTokenQueue.remove(releaseToken);
            }
            Preconditions.checkArgument(currentRunningQueryNum >= 0);
            while (currentRunningQueryNum < maxConcurrency) {
                QueueToken queueToken = this.priorityTokenQueue.poll();
                if (queueToken == null) {
                    break;
                }
                queueToken.complete();
                currentRunningQueryNum++;
            }
        } finally {
            queueLock.unlock();
            if (LOG.isDebugEnabled()) {
                LOG.info(this.debugString());
            }
        }
    }

    public void resetQueueProperty(int maxConcurrency, int maxQueueSize, int queryWaitTimeout, long version) {
        queueLock.lock();
        try {
            this.maxConcurrency = maxConcurrency;
            this.maxQueueSize = maxQueueSize;
            this.queueTimeout = queryWaitTimeout;
            this.propVersion = version;
        } finally {
            if (LOG.isDebugEnabled()) {
                LOG.debug(this.debugString());
            }
            queueLock.unlock();
        }
    }

}
