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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

// used to mark QueryQueue offer result
// if offer failed, then need to cancel query
// and return failed reason to user client
public class QueueToken implements Comparable<QueueToken> {
    private static final Logger LOG = LogManager.getLogger(QueueToken.class);

    @Override
    public int compareTo(QueueToken other) {
        return Long.compare(this.tokenId, other.getTokenId());
    }

    public enum TokenState {
        ENQUEUE_SUCCESS,
        READY_TO_RUN,
        CANCELLED
    }

    static AtomicLong tokenIdGenerator = new AtomicLong(0);

    private long tokenId = 0;

    private TokenState tokenState;

    private long queueWaitTimeout = 0;

    private String offerResultDetail;

    private boolean isTimeout = false;

    private final ReentrantLock tokenLock = new ReentrantLock();
    private final Condition tokenCond = tokenLock.newCondition();

    private long queueStartTime = -1;
    private long queueEndTime = -1;

    public QueueToken(TokenState tokenState, long queueWaitTimeout,
            String offerResultDetail) {
        this.tokenId = tokenIdGenerator.addAndGet(1);
        this.tokenState = tokenState;
        this.queueWaitTimeout = queueWaitTimeout;
        this.offerResultDetail = offerResultDetail;
    }

    public boolean waitSignal(long queryTimeoutMillis) throws InterruptedException {
        this.tokenLock.lock();
        try {
            if (isTimeout) {
                return false;
            }
            if (tokenState == TokenState.READY_TO_RUN) {
                return true;
            }
            // If query timeout is less than queue wait timeout, then should use
            // query timeout as wait timeout
            long waitTimeout = queryTimeoutMillis > queueWaitTimeout ? queueWaitTimeout : queryTimeoutMillis;
            tokenCond.await(waitTimeout, TimeUnit.MILLISECONDS);
            if (tokenState == TokenState.CANCELLED) {
                this.offerResultDetail = "query is cancelled in queue";
                return false;
            }
            // If wait timeout and is steal not ready to run, then return false
            if (tokenState != TokenState.READY_TO_RUN) {
                LOG.warn("wait in queue timeout, timeout = {}", waitTimeout);
                isTimeout = true;
                return false;
            } else {
                return true;
            }
        } catch (Throwable t) {
            LOG.warn("meet execption when wait for signal", t);
            // If any exception happens, set isTimeout to true and return false
            // Then the caller will call returnToken to queue normally.
            offerResultDetail = "meet exeption when wait for signal";
            isTimeout = true;
            return false;
        } finally {
            this.tokenLock.unlock();
            this.setQueueTimeWhenQueueEnd();
        }
    }

    public void signalForCancel() {
        this.tokenLock.lock();
        try {
            if (this.tokenState == TokenState.ENQUEUE_SUCCESS) {
                tokenCond.signal();
                this.tokenState = TokenState.CANCELLED;
            }
        } catch (Throwable t) {
            LOG.warn("error happens when signal for cancel", t);
        } finally {
            this.tokenLock.unlock();
        }
    }

    public boolean signal() {
        this.tokenLock.lock();
        try {
            // If current token is not ENQUEUE_SUCCESS, then it maybe has error
            // not run it any more.
            if (this.tokenState != TokenState.ENQUEUE_SUCCESS || isTimeout) {
                return false;
            }
            this.tokenState = TokenState.READY_TO_RUN;
            tokenCond.signal();
            return true;
        } catch (Throwable t) {
            isTimeout = true;
            offerResultDetail = "meet exception when signal";
            LOG.warn("failed to signal token", t);
            return false;
        } finally {
            this.tokenLock.unlock();
        }
    }

    public String getOfferResultDetail() {
        return offerResultDetail;
    }

    public boolean isReadyToRun() {
        return this.tokenState == TokenState.READY_TO_RUN;
    }

    public boolean isCancelled() {
        return this.tokenState == TokenState.CANCELLED;
    }

    public void setQueueTimeWhenOfferSuccess() {
        long currentTime = System.currentTimeMillis();
        this.queueStartTime = currentTime;
        this.queueEndTime = currentTime;
    }

    public void setQueueTimeWhenQueueSuccess() {
        long currentTime = System.currentTimeMillis();
        this.queueStartTime = currentTime;
    }

    public void setQueueTimeWhenQueueEnd() {
        this.queueEndTime = System.currentTimeMillis();
    }

    public long getQueueStartTime() {
        return queueStartTime;
    }

    public long getQueueEndTime() {
        return queueEndTime;
    }

    public TokenState getTokenState() {
        return tokenState;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        QueueToken other = (QueueToken) obj;
        return tokenId == other.tokenId;
    }

    public long getTokenId() {
        return tokenId;
    }
}
