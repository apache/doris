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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

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
        READY_TO_RUN
    }

    static AtomicLong tokenIdGenerator = new AtomicLong(0);

    private long tokenId = 0;

    private TokenState tokenState;

    private long queueWaitTimeout = 0;

    private long queueStartTime = -1;
    private long queueEndTime = -1;

    // string is just a placeholder, it's meanless now
    private CompletableFuture<String> future;
    private QueryQueue queue;

    public QueueToken(TokenState tokenState, long queueWaitTimeout, QueryQueue queryQueue) {
        this.tokenId = tokenIdGenerator.addAndGet(1);
        this.tokenState = tokenState;
        this.queueWaitTimeout = queueWaitTimeout;
        this.queue = queryQueue;
        this.future = new CompletableFuture();
    }

    public void get(int queryTimeout) throws UserException {
        if (tokenState == TokenState.READY_TO_RUN) {
            return;
        }
        long waitTimeout = Math.min(queueWaitTimeout, queryTimeout);
        try {
            future.get(waitTimeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException("InterruptedException when query queue", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("ExecutionException when query queue", e);
        } catch (TimeoutException e) {
            throw new UserException("query queue timeout, timeout: " + waitTimeout + " ms ");
        } catch (CancellationException e) {
            throw new UserException("query is cancelled");
        } catch (Throwable t) {
            LOG.warn("error happens when query queue", t);
            throw new RuntimeException("error happens when query queue", t);
        } finally {
            queue.removeToken(this);
        }
    }

    public void complete() {
        long currentTime = System.currentTimeMillis();
        this.queueStartTime = currentTime;
        this.queueEndTime = currentTime;
        this.tokenState = TokenState.READY_TO_RUN;
        future.complete("");
    }

    public void cancel() {
        future.cancel(true);
    }

    public void setQueueTimeWhenQueueSuccess() {
        long currentTime = System.currentTimeMillis();
        this.queueStartTime = currentTime;
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

    public boolean isReadyToRun() {
        return tokenState == TokenState.READY_TO_RUN;
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
