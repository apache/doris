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
import org.apache.doris.common.ConfigException;
import org.apache.doris.common.MetaServiceRpcRateLimitConfigValidator;
import org.apache.doris.metric.CloudMetrics;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.rpc.RpcException;

import com.google.common.collect.Maps;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

class MetaServiceRpcRateLimiter {
    private static final Logger LOG = LogManager.getLogger(MetaServiceRpcRateLimiter.class);

    private static final int CPU_CORES = Runtime.getRuntime().availableProcessors();
    private static final RateLimitConfigSnapshot EMPTY_SNAPSHOT =
            new RateLimitConfigSnapshot(0, "", 1, 0);

    private final ReentrantLock configLock = new ReentrantLock();
    private final ConcurrentMap<String, RateLimiterHolder> rateLimiters = Maps.newConcurrentMap();
    private volatile RateLimitConfigSnapshot currentSnapshot = EMPTY_SNAPSHOT;
    private volatile Map<String, Integer> methodQpsPerCore = Collections.emptyMap();

    long acquire(String methodName) throws RpcException {
        return acquire(methodName, 1);
    }

    long acquire(String methodName, int permits) throws RpcException {
        if (!Config.meta_service_rpc_rate_limit_enabled) {
            return 0;
        }

        RateLimiterHolder holder = getRateLimiter(methodName);
        if (holder == null) {
            return 0;
        }

        int permitsToAcquire = Math.min(Math.max(permits, 1), holder.maxPermitsInTimeout);
        // Resilience4j returns negative when the estimated wait exceeds the configured timeout.
        // Otherwise the returned wait time is within meta_service_rpc_rate_limit_wait_timeout_ms.
        long nanosToWait = holder.rateLimiter.reservePermission(permitsToAcquire);
        if (nanosToWait < 0) {
            throw new MetaServiceRateLimitException(methodName,
                    Config.meta_service_rpc_rate_limit_wait_timeout_ms);
        }
        if (nanosToWait == 0) {
            return 0;
        }

        long waitMs = TimeUnit.NANOSECONDS.toMillis(nanosToWait);
        if (LOG.isDebugEnabled()) {
            LOG.debug("meta service rpc rate limiter waits before acquiring permission, method: {}, permits: {}, "
                            + "original permits: {}, max permits in timeout: {}, limit for period: {}, "
                            + "burst seconds: {}, wait ms: {}",
                    methodName, permitsToAcquire, permits, holder.maxPermitsInTimeout, holder.limitForPeriod,
                    holder.burstSeconds,
                    waitMs);
        }
        long waitStartNs = System.nanoTime();
        try {
            TimeUnit.NANOSECONDS.sleep(nanosToWait);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RpcException("", e.getMessage(), e);
        }
        long actualWaitNs = System.nanoTime() - waitStartNs;
        if (MetricRepo.isInit && Config.isCloudMode()) {
            CloudMetrics.META_SERVICE_RPC_RATE_LIMIT_WAIT_LATENCY.getOrAdd(methodName)
                    .update(TimeUnit.NANOSECONDS.toMillis(actualWaitNs));
        }
        return actualWaitNs;
    }

    private RateLimiterHolder getRateLimiter(String methodName) throws RpcException {
        refreshConfigIfNeeded();
        RateLimitConfigSnapshot snapshot = currentSnapshot;
        int qpsPerCore = methodQpsPerCore.getOrDefault(methodName, snapshot.defaultQpsPerCore);
        if (qpsPerCore <= 0) {
            rateLimiters.remove(methodName);
            return null;
        }

        int limitForPeriod = getLimitForPeriod(methodName, qpsPerCore, snapshot.burstSeconds);
        int maxPermitsInTimeout = getMaxPermitsInTimeout(methodName, limitForPeriod, snapshot.burstSeconds,
                snapshot.waitTimeoutMs);
        RateLimiterHolder holder = rateLimiters.compute(methodName, (name, existingHolder) -> {
            if (existingHolder != null && existingHolder.matches(limitForPeriod, snapshot.burstSeconds,
                    snapshot.waitTimeoutMs)) {
                return existingHolder;
            }
            return new RateLimiterHolder(createRateLimiter(methodName, limitForPeriod,
                    snapshot.burstSeconds, snapshot.waitTimeoutMs), limitForPeriod,
                    maxPermitsInTimeout, snapshot.burstSeconds, snapshot.waitTimeoutMs);
        });
        return holder;
    }

    private void refreshConfigIfNeeded() throws RpcException {
        RateLimitConfigSnapshot latestSnapshot = RateLimitConfigSnapshot.current();
        if (latestSnapshot.equals(currentSnapshot)) {
            return;
        }

        configLock.lock();
        try {
            latestSnapshot = RateLimitConfigSnapshot.current();
            if (latestSnapshot.equals(currentSnapshot)) {
                return;
            }
            validateConfig(latestSnapshot);
            methodQpsPerCore = parseMethodQpsPerCore(latestSnapshot.qpsPerCoreConfig);
            currentSnapshot = latestSnapshot;
        } finally {
            configLock.unlock();
        }
    }

    private void validateConfig(RateLimitConfigSnapshot snapshot) throws RpcException {
        try {
            MetaServiceRpcRateLimitConfigValidator.validatePositive("meta_service_rpc_rate_limit_burst_seconds",
                    snapshot.burstSeconds);
            MetaServiceRpcRateLimitConfigValidator.validateNonNegative("meta_service_rpc_rate_limit_wait_timeout_ms",
                    snapshot.waitTimeoutMs);
        } catch (ConfigException e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    private Map<String, Integer> parseMethodQpsPerCore(String config) throws RpcException {
        try {
            return MetaServiceRpcRateLimitConfigValidator.parseQpsPerCoreConfig(config);
        } catch (ConfigException e) {
            throw new RpcException("", "invalid meta_service_rpc_rate_limit_qps_per_core_config: " + config, e);
        }
    }

    private int getLimitForPeriod(String methodName, int qpsPerCore, int burstSeconds) throws RpcException {
        long limitForPeriod = (long) qpsPerCore * CPU_CORES * burstSeconds;
        if (limitForPeriod > Integer.MAX_VALUE) {
            throw new RpcException("", "meta service rpc rate limit is too large, method: " + methodName
                    + ", qps per core: " + qpsPerCore + ", cpu cores: " + CPU_CORES
                    + ", burst seconds: " + burstSeconds);
        }
        return (int) limitForPeriod;
    }

    private int getMaxPermitsInTimeout(String methodName, int limitForPeriod, int burstSeconds, long waitTimeoutMs)
            throws RpcException {
        if (waitTimeoutMs <= 0) {
            return limitForPeriod;
        }

        long refreshPeriodMs = TimeUnit.SECONDS.toMillis(burstSeconds);
        // Keep a small margin from the timeout boundary, so a capped large batch does not fail repeatedly
        // because of scheduling jitter or an already slow permit reservation path.
        long maxPermitsInTimeout = Math.max(limitForPeriod,
                (long) Math.floor(limitForPeriod * (double) waitTimeoutMs / refreshPeriodMs * 0.95D));
        if (maxPermitsInTimeout > Integer.MAX_VALUE) {
            throw new RpcException("", "meta service rpc rate limit permits in timeout is too large, method: "
                    + methodName + ", limit for period: " + limitForPeriod
                    + ", burst seconds: " + burstSeconds + ", wait timeout ms: " + waitTimeoutMs);
        }
        return (int) maxPermitsInTimeout;
    }

    private RateLimiter createRateLimiter(String methodName, int limitForPeriod, int burstSeconds,
            long waitTimeoutMs) {
        RateLimiterConfig rateLimiterConfig = RateLimiterConfig.custom()
                .limitRefreshPeriod(Duration.ofSeconds(burstSeconds))
                .limitForPeriod(limitForPeriod)
                .timeoutDuration(Duration.ofMillis(waitTimeoutMs))
                .build();
        return RateLimiter.of("meta_service_rpc_" + methodName, rateLimiterConfig);
    }

    void reset() {
        configLock.lock();
        try {
            rateLimiters.clear();
            methodQpsPerCore = Collections.emptyMap();
            currentSnapshot = EMPTY_SNAPSHOT;
        } finally {
            configLock.unlock();
        }
    }

    private static class RateLimiterHolder {
        private final RateLimiter rateLimiter;
        private final int limitForPeriod;
        private final int maxPermitsInTimeout;
        private final int burstSeconds;
        private final long waitTimeoutMs;

        private RateLimiterHolder(RateLimiter rateLimiter, int limitForPeriod, int maxPermitsInTimeout,
                int burstSeconds, long waitTimeoutMs) {
            this.rateLimiter = rateLimiter;
            this.limitForPeriod = limitForPeriod;
            this.maxPermitsInTimeout = maxPermitsInTimeout;
            this.burstSeconds = burstSeconds;
            this.waitTimeoutMs = waitTimeoutMs;
        }

        private boolean matches(int limitForPeriod, int burstSeconds, long waitTimeoutMs) {
            return this.limitForPeriod == limitForPeriod && this.burstSeconds == burstSeconds
                    && this.waitTimeoutMs == waitTimeoutMs;
        }
    }

    private static class RateLimitConfigSnapshot {
        private final int defaultQpsPerCore;
        private final String qpsPerCoreConfig;
        private final int burstSeconds;
        private final long waitTimeoutMs;

        private RateLimitConfigSnapshot(int defaultQpsPerCore, String qpsPerCoreConfig, int burstSeconds,
                long waitTimeoutMs) {
            this.defaultQpsPerCore = defaultQpsPerCore;
            this.qpsPerCoreConfig = qpsPerCoreConfig == null ? "" : qpsPerCoreConfig;
            this.burstSeconds = burstSeconds;
            this.waitTimeoutMs = waitTimeoutMs;
        }

        private static RateLimitConfigSnapshot current() {
            return new RateLimitConfigSnapshot(Config.meta_service_rpc_rate_limit_default_qps_per_core,
                    Config.meta_service_rpc_rate_limit_qps_per_core_config,
                    Config.meta_service_rpc_rate_limit_burst_seconds,
                    Config.meta_service_rpc_rate_limit_wait_timeout_ms);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof RateLimitConfigSnapshot)) {
                return false;
            }
            RateLimitConfigSnapshot that = (RateLimitConfigSnapshot) o;
            return defaultQpsPerCore == that.defaultQpsPerCore && burstSeconds == that.burstSeconds
                    && waitTimeoutMs == that.waitTimeoutMs
                    && Objects.equals(qpsPerCoreConfig, that.qpsPerCoreConfig);
        }

        @Override
        public int hashCode() {
            return Objects.hash(defaultQpsPerCore, qpsPerCoreConfig, burstSeconds, waitTimeoutMs);
        }
    }
}
