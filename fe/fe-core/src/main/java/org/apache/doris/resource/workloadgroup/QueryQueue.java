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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.resource.AdmissionControl;
import org.apache.doris.resource.workloadgroup.QueueToken.TokenState;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;
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

    public static final String RUNNING_QUERY_NUM = "running_query_num";
    public static final String WAITING_QUERY_NUM = "waiting_query_num";

    private long wgId;

    private PriorityQueue<QueueToken> waitingQueryQueue;
    private Queue<QueueToken> runningQueryQueue;

    Pair<Integer, Integer> getQueryQueueDetail() {
        try {
            queueLock.lock();
            return Pair.of(runningQueryQueue.size(), waitingQueryQueue.size());
        } finally {
            queueLock.unlock();
        }
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

    public QueryQueue(long wgId, int maxConcurrency, int maxQueueSize, int queueTimeout) {
        this.wgId = wgId;
        this.maxConcurrency = maxConcurrency;
        this.maxQueueSize = maxQueueSize;
        this.queueTimeout = queueTimeout;
        this.waitingQueryQueue = new PriorityQueue<QueueToken>();
        this.runningQueryQueue = new LinkedList<QueueToken>();
    }

    public String debugString() {
        return "wgId= " + wgId + ",maxConcurrency=" + maxConcurrency
                + ", maxQueueSize=" + maxQueueSize + ", queueTimeout=" + queueTimeout + ", currentRunningQueryNum="
                + runningQueryQueue.size() + ", currentWaitingQueryNum=" + waitingQueryQueue.size();
    }

    public int usedSlotCount() {
        int cnt = 0;
        for (Iterator iterator = runningQueryQueue.iterator(); iterator.hasNext();) {
            QueueToken queueToken = (QueueToken) iterator.next();
            cnt += queueToken.getQuerySlotCount();
        }
        return cnt;
    }

    public QueueToken getToken(int querySlotCount) throws UserException {
        if (maxConcurrency > 0 && (querySlotCount > maxConcurrency || querySlotCount < 1)) {
            throw new UserException("query slot count " + querySlotCount
                    + " should be smaller than workload group's max concurrency "
                    + maxConcurrency + " and > 0");
        }
        AdmissionControl admissionControl = Env.getCurrentEnv().getAdmissionControl();
        queueLock.lock();
        try {
            if (LOG.isDebugEnabled()) {
                LOG.info(this.debugString());
            }
            QueueToken queueToken = new QueueToken(queueTimeout, querySlotCount, this);

            boolean isReachMaxCon = runningQueryQueue.size() >= maxConcurrency;
            boolean hasFreeSlot = queueToken.getQuerySlotCount() <= maxConcurrency - usedSlotCount();
            boolean isResourceAvailable = admissionControl.checkResourceAvailable(queueToken);
            if (!isReachMaxCon && isResourceAvailable && hasFreeSlot) {
                runningQueryQueue.offer(queueToken);
                queueToken.complete();
                return queueToken;
            } else if (waitingQueryQueue.size() >= maxQueueSize) {
                throw new UserException("query waiting queue is full, queue capacity=" + maxQueueSize
                        + ", waiting num=" + waitingQueryQueue.size());
            } else {
                if (!hasFreeSlot) {
                    queueToken.setQueueMsg("NO_FREE_SLOT");
                }
                if (isReachMaxCon) {
                    queueToken.setQueueMsg("WAIT_IN_QUEUE");
                }
                queueToken.setTokenState(TokenState.ENQUEUE_SUCCESS);
                this.waitingQueryQueue.offer(queueToken);
                // if a query is added to wg's queue but not in AdmissionControl's
                // queue may be blocked by be memory later,
                // then we should put query to AdmissionControl in releaseAndNotify, it's too complicated.
                // To simplify the code logic, put all waiting query to AdmissionControl,
                // waiting query can be notified when query finish or memory is enough.
                admissionControl.addQueueToken(queueToken);
            }
            return queueToken;
        } finally {
            if (LOG.isDebugEnabled()) {
                LOG.info(this.debugString());
            }
            queueLock.unlock();
        }
    }

    public void notifyWaitQuery() {
        releaseAndNotify(null);
    }

    public void releaseAndNotify(QueueToken releaseToken) {
        AdmissionControl admissionControl = Env.getCurrentEnv().getAdmissionControl();
        queueLock.lock();
        try {
            if (releaseToken != null) {
                runningQueryQueue.remove(releaseToken);
                waitingQueryQueue.remove(releaseToken);
                admissionControl.removeQueueToken(releaseToken);
            }
            while (runningQueryQueue.size() < maxConcurrency) {
                QueueToken queueToken = waitingQueryQueue.peek();
                if (queueToken == null) {
                    break;
                }
                if (queueToken.getQuerySlotCount() > maxConcurrency - usedSlotCount()) {
                    break;
                }
                if (admissionControl.checkResourceAvailable(queueToken)) {
                    queueToken.complete();
                    runningQueryQueue.offer(queueToken);
                    waitingQueryQueue.remove();
                    admissionControl.removeQueueToken(queueToken);
                } else {
                    break;
                }
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
        } finally {
            if (LOG.isDebugEnabled()) {
                LOG.debug(this.debugString());
            }
            queueLock.unlock();
        }
    }

}
