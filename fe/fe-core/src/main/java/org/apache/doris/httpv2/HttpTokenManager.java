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

package org.apache.doris.httpv2;

import org.apache.doris.common.Config;
import org.apache.doris.common.ConfigBase;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.function.LongSupplier;

/**
 * Node-local runtime state for FE http_token authentication and rotation.
 * Keeps at most two generations in memory (current and previous with a fixed 1800s grace window).
 */
public class HttpTokenManager {
    private static final Logger LOG = LogManager.getLogger(HttpTokenManager.class);

    public static final long HTTP_TOKEN_GRACE_PERIOD_SECONDS = 1800L;
    public static final long HTTP_TOKEN_GRACE_PERIOD_MS = HTTP_TOKEN_GRACE_PERIOD_SECONDS * 1000L;

    private static final HttpTokenManager INSTANCE = new HttpTokenManager();

    private static final class TokenState {
        final String currentToken;
        final String previousToken;
        final long previousExpireAtMs;
        final boolean initialized;

        TokenState(String currentToken, String previousToken, long previousExpireAtMs, boolean initialized) {
            this.currentToken = currentToken == null ? "" : currentToken;
            this.previousToken = previousToken == null ? "" : previousToken;
            this.previousExpireAtMs = previousExpireAtMs;
            this.initialized = initialized;
        }
    }

    public static final class RotateResult {
        private final boolean ok;
        private final String errorMsg;

        private RotateResult(boolean ok, String errorMsg) {
            this.ok = ok;
            this.errorMsg = errorMsg;
        }

        public static RotateResult success() {
            return new RotateResult(true, "");
        }

        public static RotateResult error(String errorMsg) {
            return new RotateResult(false, errorMsg);
        }

        public boolean isOk() {
            return ok;
        }

        public String getErrorMsg() {
            return errorMsg;
        }
    }

    private final Object rotateLock = new Object();
    private volatile TokenState state = new TokenState("", "", 0L, false);
    private volatile LongSupplier clockMs = () -> System.nanoTime() / 1_000_000L;

    public static HttpTokenManager getInstance() {
        return INSTANCE;
    }

    public void init(String token) {
        synchronized (rotateLock) {
            state = new TokenState(token == null ? "" : token.trim(), "", 0L, true);
        }
    }

    public boolean verify(String token) {
        if (Strings.isNullOrEmpty(token)) {
            return false;
        }
        TokenState snapshot = state;
        String current = snapshot.initialized ? snapshot.currentToken
                : (Config.http_token == null ? "" : Config.http_token.trim());
        if (!current.isEmpty() && constantTimeEquals(token, current)) {
            return true;
        }
        if (snapshot.initialized && !snapshot.previousToken.isEmpty()
                && clockMs.getAsLong() < snapshot.previousExpireAtMs
                && constantTimeEquals(token, snapshot.previousToken)) {
            return true;
        }
        return false;
    }

    public RotateResult rotate() {
        return rotate(null, null);
    }

    public RotateResult rotate(String baseConfPath, String customConfPath) {
        synchronized (rotateLock) {
            String configuredToken;
            try {
                configuredToken = (baseConfPath == null)
                        ? ConfigBase.readEffectiveConfigValue("http_token")
                        : ConfigBase.readEffectiveConfigValue("http_token", baseConfPath, customConfPath);
            } catch (Exception e) {
                LOG.warn("Failed to read configured http_token during rotation");
                return RotateResult.error("Failed to read configured http_token");
            }
            if (Strings.isNullOrEmpty(configuredToken)) {
                LOG.warn("Configured http_token is empty during rotation");
                return RotateResult.error("Configured http_token must not be empty");
            }

            TokenState snapshot = state;
            String current = snapshot.initialized ? snapshot.currentToken
                    : (Config.http_token == null ? "" : Config.http_token.trim());
            if (configuredToken.equals(current)) {
                if (!snapshot.initialized) {
                    state = new TokenState(current, "", 0L, true);
                }
                LOG.info("rotate_http_token succeeded (idempotent, unchanged)");
                return RotateResult.success();
            }

            long expireAtMs = clockMs.getAsLong() + HTTP_TOKEN_GRACE_PERIOD_MS;
            state = new TokenState(configuredToken, current, expireAtMs, true);
            Config.http_token = configuredToken;
            LOG.info("rotate_http_token succeeded");
            return RotateResult.success();
        }
    }

    public void setClockForTest(LongSupplier testClockMs) {
        this.clockMs = testClockMs == null ? () -> System.nanoTime() / 1_000_000L : testClockMs;
    }

    public void resetForTest(String token) {
        synchronized (rotateLock) {
            this.clockMs = () -> System.nanoTime() / 1_000_000L;
            this.state = new TokenState(token == null ? "" : token.trim(), "", 0L, true);
            Config.http_token = this.state.currentToken;
        }
    }

    private static boolean constantTimeEquals(String a, String b) {
        return MessageDigest.isEqual(
                a.getBytes(StandardCharsets.UTF_8),
                b.getBytes(StandardCharsets.UTF_8));
    }
}
