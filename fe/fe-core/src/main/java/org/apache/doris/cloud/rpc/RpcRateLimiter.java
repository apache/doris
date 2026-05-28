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

package org.apache.doris.cloud.rpc;

import org.apache.doris.common.Config;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RpcRateLimiter {
    private static final Logger LOG = LogManager.getLogger(RpcRateLimiter.class);

    protected static class QpsLimiter {
        protected final String methodName;
        protected volatile int maxWaitRequestNum;
        protected volatile int qps;
        private final AtomicInteger waitingRequests = new AtomicInteger(0);
        private RateLimiter rateLimiter;

        QpsLimiter(String methodName, int maxWaitRequestNum, int qps) {
            Preconditions.checkArgument(qps > 0, "qps must be > 0");
            Preconditions.checkArgument(maxWaitRequestNum > 0, "maxWaitRequestNum must be > 0");
            this.methodName = methodName;
            this.maxWaitRequestNum = maxWaitRequestNum;
            this.qps = qps;
            this.rateLimiter = RateLimiter.create(qps);
            LOG.info("Create qps limiter for method: {}, maxWaitRequestNum: {}, qps: {}", methodName,
                    maxWaitRequestNum, qps);
        }

        void acquire() throws RpcRateLimitException {
            // Count requests currently waiting for qps permit.
            int currentWaiting = waitingRequests.incrementAndGet();
            if (currentWaiting > maxWaitRequestNum) {
                waitingRequests.decrementAndGet();
                throw new RpcRateLimitException("Meta service rpc rate limit exceeded for method: " + methodName
                        + ", too many waiting requests (max=" + maxWaitRequestNum + ")");
            }
            // Try to acquire rate limiter permit with timeout
            try {
                long timeoutMs = Config.meta_service_rpc_rate_limit_wait_timeout_ms;
                if (!rateLimiter.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS)) {
                    throw new RpcRateLimitException(
                            "Meta service rpc rate limit timeout for method: " + methodName + ", rate: " + qps
                                    + ", waited " + timeoutMs + " ms");
                }
            } finally {
                waitingRequests.decrementAndGet();
            }
        }

        void update(int maxWaitRequestNum, int qps) {
            updateMaxWaitRequestNum(maxWaitRequestNum);
            updateQps(qps);
        }

        private void updateMaxWaitRequestNum(int maxWaitRequestNum) {
            Preconditions.checkArgument(maxWaitRequestNum > 0, "maxWaitRequestNum must be > 0");
            if (maxWaitRequestNum != this.maxWaitRequestNum) {
                LOG.info("Update qps limiter for method: {}, maxWaitRequestNum: from {} to {}", methodName,
                        this.maxWaitRequestNum, maxWaitRequestNum);
                this.maxWaitRequestNum = maxWaitRequestNum;
            }
        }

        private void updateQps(int qps) {
            Preconditions.checkArgument(qps > 0, "qps must be > 0");
            if (qps != this.qps) {
                LOG.info("Update qps limiter for method: {}, qps: from {} to {}", methodName, this.qps, qps);
                this.qps = qps;
                this.rateLimiter.setRate(qps);
            }
        }

        // only used for testing
        int getMaxWaitRequestNum() {
            return maxWaitRequestNum;
        }

        // only used for testing
        int getAllowWaiting() {
            return Math.max(0, maxWaitRequestNum - waitingRequests.get());
        }

        // only used for testing
        RateLimiter getRateLimiter() {
            return rateLimiter;
        }
    }

    protected static class OverloadQpsLimiter extends QpsLimiter {
        private volatile int baseQps;

        OverloadQpsLimiter(String methodName, int maxWaitRequestNum, int qps, double factor) {
            super(methodName, maxWaitRequestNum, qps);
            this.baseQps = qps;
            applyFactor(factor);
        }

        void applyFactor(double factor) {
            Preconditions.checkArgument(Double.compare(factor, 1) <= 0, "factor must be <= 1");
            int effectiveQps = Math.max(1, (int) (baseQps * factor));
            if (effectiveQps != this.qps) {
                update(maxWaitRequestNum, effectiveQps);
                LOG.info("Applied factor {} to overload limiter for method {}, base qps: {}, effective qps: {}",
                        factor, methodName, baseQps, effectiveQps);
            }
        }

        // only used for testing
        int getBaseQps() {
            return baseQps;
        }
    }

    protected static class CostLimiter  {
        private String methodName;
        private volatile int limit;
        private int currentCost;
        private final Lock lock = new ReentrantLock(true);
        private final Condition condition = lock.newCondition();

        CostLimiter(String methodName, int limit) {
            Preconditions.checkArgument(limit > 0, "limit must be > 0");
            this.methodName = methodName;
            this.limit = limit;
            this.currentCost = 0;
        }

        void setLimit(int newLimit) {
            Preconditions.checkArgument(newLimit > 0, "limit must be > 0");
            lock.lock();
            try {
                this.limit = newLimit;
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }

        void acquire(int cost) throws RpcRateLimitException {
            if (cost <= 0) {
                return;
            }
            if (cost > limit) {
                throw new RpcRateLimitException(
                        "method: " + methodName + ", cost " + cost + " exceeds the limit " + limit);
            }
            try {
                lock.lockInterruptibly();
            } catch (InterruptedException e) {
                throw new RpcRateLimitException(
                        "Meta service cost limit interrupted while acquiring lock for method: " + methodName
                                + ", requestCost: " + cost + ", currentCost: " + currentCost + ", limit: " + limit, e);
            }
            try {
                long nanos = TimeUnit.MILLISECONDS.toNanos(Config.meta_service_rpc_rate_limit_wait_timeout_ms);
                while (currentCost + cost > limit) {
                    if (nanos <= 0) {
                        throw new RpcRateLimitException(
                                "Meta service cost limit timeout for method: " + methodName + ", requestCost: "
                                        + cost + ", currentCost: " + currentCost + ", limit: " + limit);
                    }
                    nanos = condition.awaitNanos(nanos);
                }
                currentCost += cost;
            } catch (InterruptedException e) {
                throw new RpcRateLimitException(
                        "Meta service cost limit interrupted while acquiring lock for method: " + methodName
                                + ", requestCost: " + cost + ", currentCost: " + currentCost + ", limit: " + limit, e);
            } finally {
                lock.unlock();
            }
        }

        void release(int cost) {
            lock.lock();
            try {
                currentCost -= cost;
                if (currentCost < 0) {
                    currentCost = 0;
                }
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }

        // only used for testing
        int getLimit()  {
            return limit;
        }

        // only used for testing
        int getCurrentCost() {
            lock.lock();
            try {
                return currentCost;
            } finally {
                lock.unlock();
            }
        }
    }
}
