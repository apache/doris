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

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

// note(wb) refer java BlockingQueue, but support altering capacity
// todo(wb) add wait time to profile
public class QueryQueue {

    private static final Logger LOG = LogManager.getLogger(QueryQueue.class);
    // note(wb) used unfair by default, need more test later
    private final ReentrantLock queueLock = new ReentrantLock();
    private final Condition queueLockCond = queueLock.newCondition();
    // resource group property
    private int maxConcurrency;
    private int maxQueueSize;
    private int queueTimeout; // ms
    // running property
    private volatile int currentRunningQueryNum;
    private volatile int currentWaitingQueryNum;

    public static final String RUNNING_QUERY_NUM = "running_query_num";
    public static final String WAITING_QUERY_NUM = "waiting_query_num";

    int getCurrentRunningQueryNum() {
        return currentRunningQueryNum;
    }

    int getCurrentWaitingQueryNum() {
        return currentWaitingQueryNum;
    }

    public QueryQueue(int maxConcurrency, int maxQueueSize, int queueTimeout) {
        this.maxConcurrency = maxConcurrency;
        this.maxQueueSize = maxQueueSize;
        this.queueTimeout = queueTimeout;
    }

    public String debugString() {
        return "maxConcurrency=" + maxConcurrency + ", maxQueueSize=" + maxQueueSize + ", queueTimeout=" + queueTimeout
                + ", currentRunningQueryNum=" + currentRunningQueryNum + ", currentWaitingQueryNum="
                + currentWaitingQueryNum;
    }

    public QueueOfferToken offer() throws InterruptedException {
        // to prevent hang
        // the lock shouldn't be hold for too long
        // we should catch the case when it happens
        queueLock.tryLock(5, TimeUnit.SECONDS);
        try {
            if (LOG.isDebugEnabled()) {
                LOG.info(this.debugString());
            }

            while (true) {
                if (currentRunningQueryNum < maxConcurrency) {
                    break;
                }
                // currentRunningQueryNum may bigger than maxRunningQueryNum
                // because maxRunningQueryNum can be altered
                if (currentWaitingQueryNum >= maxQueueSize) {
                    return new QueueOfferToken(false, "query waiting queue is full, queue length=" + maxQueueSize);
                }

                currentWaitingQueryNum++;
                boolean ret;
                try {
                    ret = queueLockCond.await(queueTimeout, TimeUnit.MILLISECONDS);
                } finally {
                    currentWaitingQueryNum--;
                }
                if (!ret) {
                    return new QueueOfferToken(false, "query wait timeout " + queueTimeout + " ms");
                }
            }
            currentRunningQueryNum++;
            return new QueueOfferToken(true, "offer success");
        } finally {
            if (LOG.isDebugEnabled()) {
                LOG.info(this.debugString());
            }
            queueLock.unlock();
        }
    }

    public void poll() throws InterruptedException {
        queueLock.tryLock(5, TimeUnit.SECONDS);
        try {
            currentRunningQueryNum--;
            Preconditions.checkArgument(currentRunningQueryNum >= 0);
            // maybe only when currentWaitingQueryNum != 0 need to signal
            if (currentRunningQueryNum < maxConcurrency) {
                queueLockCond.signal();
            }
        } finally {
            if (LOG.isDebugEnabled()) {
                LOG.info(this.debugString());
            }
            queueLock.unlock();
        }
    }

    public void resetQueueProperty(int maxConcurrency, int maxQueueSize, int queryWaitTimeout) {
        try {
            queueLock.tryLock(5, TimeUnit.SECONDS);
            try {
                this.maxConcurrency = maxConcurrency;
                this.maxQueueSize = maxQueueSize;
                this.queueTimeout = queryWaitTimeout;
            } finally {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(this.debugString());
                }
                queueLock.unlock();
            }
        } catch (InterruptedException e) {
            LOG.error("reset queue property failed, ", e);
            throw new RuntimeException("reset queue property failed, reason=" + e.getMessage());
        }
    }

}
