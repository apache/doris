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
import org.apache.doris.common.ConfigBase;
import org.apache.doris.common.MetaServiceRpcRateLimitConfigValidator;
import org.apache.doris.rpc.RpcException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

public class MetaServiceRpcRateLimiterTest {
    private static final int CPU_CORES = Runtime.getRuntime().availableProcessors();

    private boolean originRateLimitEnabled;
    private int originRateLimitDefaultQpsPerCore;
    private String originRateLimitQpsPerCoreConfig;
    private int originRateLimitBurstSeconds;
    private long originRateLimitWaitTimeoutMs;

    private MetaServiceRpcRateLimiter rateLimiter;

    @Before
    public void setUp() {
        originRateLimitEnabled = Config.meta_service_rpc_rate_limit_enabled;
        originRateLimitDefaultQpsPerCore = Config.meta_service_rpc_rate_limit_default_qps_per_core;
        originRateLimitQpsPerCoreConfig = Config.meta_service_rpc_rate_limit_qps_per_core_config;
        originRateLimitBurstSeconds = Config.meta_service_rpc_rate_limit_burst_seconds;
        originRateLimitWaitTimeoutMs = Config.meta_service_rpc_rate_limit_wait_timeout_ms;

        rateLimiter = new MetaServiceRpcRateLimiter();
        enableRateLimit(1, "", 1, 0);
    }

    @After
    public void tearDown() {
        Config.meta_service_rpc_rate_limit_enabled = originRateLimitEnabled;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = originRateLimitDefaultQpsPerCore;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = originRateLimitQpsPerCoreConfig;
        Config.meta_service_rpc_rate_limit_burst_seconds = originRateLimitBurstSeconds;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = originRateLimitWaitTimeoutMs;
        rateLimiter.reset();
    }

    @Test
    public void testMethodRateLimitCanBeDisabledByConfig() throws RpcException {
        enableRateLimit(1, "unlimited:0", 1, 0);

        for (int i = 0; i <= CPU_CORES; i++) {
            rateLimiter.acquire("unlimited");
        }
    }

    @Test
    public void testRateLimitDisabledBySwitch() throws RpcException {
        consumePermits("disabledSwitch", CPU_CORES);
        assertRateLimited("disabledSwitch");

        Config.meta_service_rpc_rate_limit_enabled = false;
        rateLimiter.acquire("disabledSwitch");
    }

    @Test
    public void testMethodOverrideQpsTakesEffect() throws RpcException {
        enableRateLimit(1, "fast:2", 1, 0);

        consumePermits("normal", CPU_CORES);
        assertRateLimited("normal");

        consumePermits("fast", CPU_CORES * 2);
        assertRateLimited("fast");
    }

    @Test
    public void testRateLimitIsIsolatedBetweenMethods() throws RpcException {
        consumePermits("firstMethod", CPU_CORES);
        assertRateLimited("firstMethod");

        rateLimiter.acquire("secondMethod");
    }

    @Test
    public void testWeightedAcquireConsumesMultiplePermits() throws RpcException {
        rateLimiter.acquire("weighted", CPU_CORES);

        assertRateLimited("weighted");
    }

    @Test
    public void testWeightedAcquireLargerThanTimeoutCapacityIsCappedAndSucceeds() throws RpcException {
        rateLimiter.acquire("largeWeighted", CPU_CORES + 1);

        assertRateLimited("largeWeighted");
    }

    @Test
    public void testWeightedAcquireUsesTimeoutCapacity() throws RpcException {
        enableRateLimit(1, "", 1, 3000);

        long waitNs = rateLimiter.acquire("timeoutCapacity", CPU_CORES * 2);

        Assert.assertTrue(waitNs > 0);
    }

    @Test
    public void testDynamicConfigUpdate() throws RpcException {
        consumePermits("dynamic", CPU_CORES);
        assertRateLimited("dynamic");

        Config.meta_service_rpc_rate_limit_default_qps_per_core = 0;
        rateLimiter.acquire("dynamic");
    }

    @Test
    public void testBurstWindow() throws RpcException {
        enableRateLimit(1, "burst:1", 2, 0);

        consumePermits("burst", CPU_CORES * 2);
        assertRateLimited("burst");
    }

    @Test
    public void testWaitTimeoutRejectsWithoutWaitingForNextRefreshPeriod() throws RpcException {
        enableRateLimit(1, "", 3, 1);
        consumePermits("waitTimeout", CPU_CORES * 3);

        long startTimeMs = System.currentTimeMillis();
        assertRateLimited("waitTimeout");
        Assert.assertTrue(System.currentTimeMillis() - startTimeMs < TimeUnit.SECONDS.toMillis(1));
    }

    @Test
    public void testAcquireReturnsActualWaitTime() throws RpcException {
        enableRateLimit(1, "", 1, 2000);
        consumePermits("wait", CPU_CORES);

        long waitNs = rateLimiter.acquire("wait");

        Assert.assertTrue(waitNs > 0);
    }

    @Test
    public void testInvalidQpsConfig() throws Exception {
        Field field = Config.class.getField("meta_service_rpc_rate_limit_qps_per_core_config");
        MetaServiceRpcRateLimitConfigValidator.QpsConfigHandler handler =
                new MetaServiceRpcRateLimitConfigValidator.QpsConfigHandler();

        assertConfigHandlerRejects(handler, field, "invalid", "Invalid");
        assertConfigHandlerRejects(handler, field, "method:", "Invalid");
        assertConfigHandlerRejects(handler, field, ":1", "Invalid");
        assertConfigHandlerRejects(handler, field, "method:abc", "Invalid");
        assertConfigHandlerRejects(handler, field, "method:1;method:2", "Duplicate");

        Config.meta_service_rpc_rate_limit_qps_per_core_config = "invalid";
        assertRpcException("invalid meta_service_rpc_rate_limit_qps_per_core_config",
                () -> rateLimiter.acquire("invalid"));

        Config.meta_service_rpc_rate_limit_qps_per_core_config = "method:1;method:2";
        assertRpcException("invalid meta_service_rpc_rate_limit_qps_per_core_config",
                () -> rateLimiter.acquire("method"));
    }

    @Test
    public void testInvalidBurstSecondsConfigHandler() throws Exception {
        Field field = Config.class.getField("meta_service_rpc_rate_limit_burst_seconds");
        MetaServiceRpcRateLimitConfigValidator.PositiveIntConfigHandler handler =
                new MetaServiceRpcRateLimitConfigValidator.PositiveIntConfigHandler();

        assertConfigHandlerRejects(handler, field, "0", "must be positive");
        assertConfigHandlerRejects(handler, field, "-1", "must be positive");

        handler.handle(field, " 2 ");
        Assert.assertEquals(2, Config.meta_service_rpc_rate_limit_burst_seconds);
    }

    @Test
    public void testInvalidBurstSeconds() throws RpcException {
        enableRateLimit(1, "", 0, 0);

        assertRpcException("meta_service_rpc_rate_limit_burst_seconds must be positive",
                () -> rateLimiter.acquire("invalidBurst"));
    }

    @Test
    public void testInvalidWaitTimeoutMsConfigHandler() throws Exception {
        Field field = Config.class.getField("meta_service_rpc_rate_limit_wait_timeout_ms");
        MetaServiceRpcRateLimitConfigValidator.NonNegativeLongConfigHandler handler =
                new MetaServiceRpcRateLimitConfigValidator.NonNegativeLongConfigHandler();

        assertConfigHandlerRejects(handler, field, "-1", "must be non-negative");

        handler.handle(field, " 0 ");
        Assert.assertEquals(0, Config.meta_service_rpc_rate_limit_wait_timeout_ms);
    }

    @Test
    public void testInvalidWaitTimeoutMs() throws RpcException {
        enableRateLimit(1, "", 1, -1);

        assertRpcException("meta_service_rpc_rate_limit_wait_timeout_ms must be non-negative",
                () -> rateLimiter.acquire("invalidWaitTimeout"));
    }

    @Test
    public void testLimitForPeriodOverflow() throws RpcException {
        enableRateLimit(Integer.MAX_VALUE, "", 2, 0);

        assertRpcException("meta service rpc rate limit is too large",
                () -> rateLimiter.acquire("overflow"));
    }

    private void enableRateLimit(int defaultQpsPerCore, String qpsPerCoreConfig, int burstSeconds,
            long waitTimeoutMs) {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = defaultQpsPerCore;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = qpsPerCoreConfig;
        Config.meta_service_rpc_rate_limit_burst_seconds = burstSeconds;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = waitTimeoutMs;
        rateLimiter.reset();
    }

    private void consumePermits(String methodName, int permitNum) throws RpcException {
        for (int i = 0; i < permitNum; i++) {
            rateLimiter.acquire(methodName);
        }
    }

    private void assertConfigHandlerRejects(ConfigBase.ConfHandler handler, Field field, String config,
            String expectedMessage) throws Exception {
        try {
            handler.handle(field, config);
            Assert.fail("should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(expectedMessage));
        }
    }

    private void assertRateLimited(String methodName) throws RpcException {
        assertRpcException("meta service rpc rate limited", () -> rateLimiter.acquire(methodName));
    }

    private void assertRpcException(String expectedMessage, RpcCall rpcCall) throws RpcException {
        try {
            rpcCall.run();
            Assert.fail("should throw RpcException");
        } catch (RpcException e) {
            Assert.assertTrue(e.getMessage().contains(expectedMessage));
        }
    }

    private interface RpcCall {
        void run() throws RpcException;
    }
}
