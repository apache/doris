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

package org.apache.doris.httpv2.security;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * A bounded, FE-local limiter for browser login attempts. Keys must come from
 * the servlet connection itself; forwarded headers are deliberately ignored.
 */
public class LoginAttemptLimiter {
    public static final int MAX_FAILURES = 5;
    public static final Duration WINDOW = Duration.ofMinutes(1);
    public static final Duration BLOCK_DURATION = Duration.ofMinutes(1);

    private static final LoginAttemptLimiter INSTANCE = new LoginAttemptLimiter(System::currentTimeMillis);

    private final Cache<String, AttemptState> attempts = CacheBuilder.newBuilder()
            .maximumSize(10_000)
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .build();
    private final LongSupplier clock;

    public LoginAttemptLimiter() {
        this(System::currentTimeMillis);
    }

    LoginAttemptLimiter(LongSupplier clock) {
        this.clock = clock;
    }

    public static LoginAttemptLimiter getInstance() {
        return INSTANCE;
    }

    public boolean isAllowed(String clientAddress) {
        AttemptState state = attempts.getIfPresent(clientAddress);
        return state == null || state.isAllowed(clock.getAsLong());
    }

    public void recordFailure(String clientAddress) {
        attempts.asMap().compute(clientAddress, (ignored, state) -> {
            AttemptState current = state == null ? new AttemptState() : state;
            current.recordFailure(clock.getAsLong());
            return current;
        });
    }

    public void recordSuccess(String clientAddress) {
        attempts.invalidate(clientAddress);
    }

    public long retryAfterSeconds(String clientAddress) {
        AttemptState state = attempts.getIfPresent(clientAddress);
        return state == null ? 0 : state.retryAfterSeconds(clock.getAsLong());
    }

    private static class AttemptState {
        private long windowStartedAt;
        private int failures;
        private long blockedUntil;

        synchronized boolean isAllowed(long now) {
            return now >= blockedUntil;
        }

        synchronized void recordFailure(long now) {
            if (windowStartedAt == 0 || now - windowStartedAt >= WINDOW.toMillis()) {
                windowStartedAt = now;
                failures = 0;
            }
            failures += 1;
            if (failures >= MAX_FAILURES) {
                blockedUntil = now + BLOCK_DURATION.toMillis();
            }
        }

        synchronized long retryAfterSeconds(long now) {
            if (now >= blockedUntil) {
                return 0;
            }
            return Math.max(1, (blockedUntil - now + 999) / 1000);
        }
    }
}
