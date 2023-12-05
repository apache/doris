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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

// used to mark QueryQueue offer result
// if offer failed, then need to cancel query
// and return failed reason to user client
public class QueueToken {

    enum TokenState {
        ENQUEUE_FAILED,
        ENQUEUE_SUCCESS,
        READY_TO_RUN
    }

    static AtomicLong tokenIdGenerator = new AtomicLong(0);

    private long tokenId = 0;

    private TokenState tokenState;

    private long waitTimeout = 0;

    private long enqueueTime = 0;

    private String offerResultDetail;

    private final ReentrantLock tokenLock = new ReentrantLock();
    private final Condition tokenCond = tokenLock.newCondition();

    public QueueToken(TokenState tokenState, long waitTimeout,
            String offerResultDetail) {
        this.tokenId = tokenIdGenerator.addAndGet(1);
        this.tokenState = tokenState;
        this.waitTimeout = waitTimeout;
        this.offerResultDetail = offerResultDetail;
        this.enqueueTime = System.currentTimeMillis();
    }

    public boolean waitSignal() throws InterruptedException {
        this.tokenLock.lock();
        try {
            if (tokenState == TokenState.READY_TO_RUN) {
                return true;
            }
            if (tokenState == TokenState.ENQUEUE_FAILED) {
                return false;
            }
            tokenCond.wait(waitTimeout);
            // If wait timeout and is steal not ready to run, then return false
            if (tokenState != TokenState.READY_TO_RUN) {
                return false;
            } else {
                return true;
            }
        } finally {
            this.tokenLock.unlock();
        }
    }

    public void signal() {
        this.tokenLock.lock();
        try {
            this.tokenState = TokenState.READY_TO_RUN;
            tokenCond.signal();
        } finally {
            this.tokenLock.unlock();
        }
    }

    public Boolean enqueueSuccess() {
        return this.tokenState != TokenState.ENQUEUE_FAILED;
    }

    public String getOfferResultDetail() {
        return offerResultDetail;
    }

    public boolean isReadyToRun() {
        return this.tokenState == TokenState.READY_TO_RUN;
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

}
