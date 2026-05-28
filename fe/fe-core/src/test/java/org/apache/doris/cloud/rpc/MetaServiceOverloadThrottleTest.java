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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

public class MetaServiceOverloadThrottleTest {

    private boolean originalEnabled;
    private double originalMinFactor;
    private double originalDecreaseMultiplier;
    private long originalCooldownMs;
    private long originalRecoveryIntervalMs;
    private double originalRecoveryStep;
    private int originalWindowSeconds;
    private int originalMinWindowRequests;
    private int originalBadTriggerCount;
    private double originalBadRateTrigger;
    private String originalThrottleMethods;

    @Before
    public void setUp() {
        originalEnabled = Config.meta_service_rpc_overload_throttle_enabled;
        originalMinFactor = Config.meta_service_rpc_overload_throttle_min_factor;
        originalDecreaseMultiplier = Config.meta_service_rpc_overload_throttle_decrease_multiplier;
        originalCooldownMs = Config.meta_service_rpc_overload_throttle_cooldown_ms;
        originalRecoveryIntervalMs = Config.meta_service_rpc_overload_throttle_recovery_interval_ms;
        originalRecoveryStep = Config.meta_service_rpc_overload_throttle_recovery_step;
        originalWindowSeconds = Config.meta_service_rpc_overload_throttle_window_seconds;
        originalMinWindowRequests = Config.meta_service_rpc_overload_throttle_min_window_requests;
        originalBadTriggerCount = Config.meta_service_rpc_overload_event_count_trigger;
        originalBadRateTrigger = Config.meta_service_rpc_overload_event_rate_trigger;
        originalThrottleMethods = Config.meta_service_rpc_overload_throttle_methods;

        Config.meta_service_rpc_overload_throttle_enabled = true;
        Config.meta_service_rpc_overload_throttle_min_factor = 0.1;
        Config.meta_service_rpc_overload_throttle_decrease_multiplier = 0.7;
        Config.meta_service_rpc_overload_throttle_cooldown_ms = 30000;
        Config.meta_service_rpc_overload_throttle_recovery_interval_ms = 5000;
        Config.meta_service_rpc_overload_throttle_recovery_step = 0.05;
        Config.meta_service_rpc_overload_throttle_window_seconds = 10;
        Config.meta_service_rpc_overload_throttle_min_window_requests = 5;
        Config.meta_service_rpc_overload_event_count_trigger = 2;
        Config.meta_service_rpc_overload_event_rate_trigger = 0.3;
        Config.meta_service_rpc_overload_throttle_methods = "";
    }

    @After
    public void tearDown() {
        Config.meta_service_rpc_overload_throttle_enabled = originalEnabled;
        Config.meta_service_rpc_overload_throttle_min_factor = originalMinFactor;
        Config.meta_service_rpc_overload_throttle_decrease_multiplier = originalDecreaseMultiplier;
        Config.meta_service_rpc_overload_throttle_cooldown_ms = originalCooldownMs;
        Config.meta_service_rpc_overload_throttle_recovery_interval_ms = originalRecoveryIntervalMs;
        Config.meta_service_rpc_overload_throttle_recovery_step = originalRecoveryStep;
        Config.meta_service_rpc_overload_throttle_window_seconds = originalWindowSeconds;
        Config.meta_service_rpc_overload_throttle_min_window_requests = originalMinWindowRequests;
        Config.meta_service_rpc_overload_event_count_trigger = originalBadTriggerCount;
        Config.meta_service_rpc_overload_event_rate_trigger = originalBadRateTrigger;
        Config.meta_service_rpc_overload_throttle_methods = originalThrottleMethods;

        resetSingleton();
    }

    private void resetSingleton() {
        try {
            Field instanceField = MetaServiceOverloadThrottle.class.getDeclaredField("instance");
            instanceField.setAccessible(true);
            instanceField.set(null, (MetaServiceOverloadThrottle) null);
        } catch (Exception e) {
            // Ignore
        }
    }

    @Test
    public void testInitialState() {
        MetaServiceOverloadThrottle throttle = MetaServiceOverloadThrottle.getInstance();
        Assert.assertEquals(MetaServiceOverloadThrottle.State.NORMAL, throttle.getState());
        Assert.assertEquals(1.0, throttle.getFactor(), 0.001);
        Assert.assertEquals(0, throttle.getWindowTotal());
        Assert.assertEquals(0, throttle.getWindowBad());
    }

    @Test
    public void testDisabledDoesNothing() {
        Config.meta_service_rpc_overload_throttle_enabled = false;
        MetaServiceOverloadThrottle throttle = MetaServiceOverloadThrottle.getInstance();

        for (int i = 0; i < 100; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.TIMEOUT);
        }
        Assert.assertEquals(MetaServiceOverloadThrottle.State.NORMAL, throttle.getState());
        Assert.assertEquals(1.0, throttle.getFactor(), 0.001);
        Assert.assertEquals(0, throttle.getWindowTotal());
    }

    @Test
    public void testSuccessKeepsNormal() {
        MetaServiceOverloadThrottle throttle = MetaServiceOverloadThrottle.getInstance();

        for (int i = 0; i < 50; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.SUCCESS);
        }
        Assert.assertEquals(MetaServiceOverloadThrottle.State.NORMAL, throttle.getState());
        Assert.assertEquals(1.0, throttle.getFactor(), 0.001);
        Assert.assertEquals(50, throttle.getWindowTotal());
        Assert.assertEquals(0, throttle.getWindowBad());
    }

    @Test
    public void testRestWindow() {
        Config.meta_service_rpc_overload_throttle_window_seconds = 2;
        MetaServiceOverloadThrottle throttle = MetaServiceOverloadThrottle.getInstance();

        for (int i = 0; i < 50; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.SUCCESS);
        }
        Assert.assertEquals(MetaServiceOverloadThrottle.State.NORMAL, throttle.getState());
        Assert.assertEquals(1.0, throttle.getFactor(), 0.001);
        Assert.assertEquals(50, throttle.getWindowTotal());
        Assert.assertEquals(0, throttle.getWindowBad());
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < 1; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.SUCCESS);
        }
        Assert.assertEquals(1, throttle.getWindowTotal());
    }

    @Test
    public void testTimeoutTriggersDecrease() {
        MetaServiceOverloadThrottle throttle = MetaServiceOverloadThrottle.getInstance();

        for (int i = 0; i < 5; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.SUCCESS);
        }
        for (int i = 0; i < 2; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.TIMEOUT);
        }
        Assert.assertEquals(7, throttle.getWindowTotal());
        Assert.assertEquals(2, throttle.getWindowBad());
        throttle.recordSignal(MetaServiceOverloadThrottle.Signal.TIMEOUT);
        Assert.assertEquals(MetaServiceOverloadThrottle.State.FAST_DECREASE, throttle.getState());
        Assert.assertEquals(0, throttle.getWindowTotal());
        Assert.assertEquals(0, throttle.getWindowBad());
        Assert.assertEquals(throttle.getFactor(), 0.7, 0.01);
    }

    @Test
    public void testBackpressureTriggersDecrease() {
        MetaServiceOverloadThrottle throttle = MetaServiceOverloadThrottle.getInstance();

        for (int i = 0; i < 5; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.SUCCESS);
        }
        for (int i = 0; i < 2; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.OVERLOAD);
        }
        Assert.assertEquals(7, throttle.getWindowTotal());
        Assert.assertEquals(2, throttle.getWindowBad());
        throttle.recordSignal(MetaServiceOverloadThrottle.Signal.TIMEOUT);
        Assert.assertEquals(MetaServiceOverloadThrottle.State.FAST_DECREASE, throttle.getState());
        Assert.assertEquals(0, throttle.getWindowTotal());
        Assert.assertEquals(0, throttle.getWindowBad());
        Assert.assertEquals(throttle.getFactor(), 0.7, 0.01);
    }

    @Test
    public void testFactorDoesNotGoBelowMin() {
        MetaServiceOverloadThrottle throttle = MetaServiceOverloadThrottle.getInstance();

        for (int round = 0; round < 50; round++) {
            for (int i = 0; i < 5; i++) {
                throttle.recordSignal(MetaServiceOverloadThrottle.Signal.SUCCESS);
            }
            for (int i = 0; i < 5; i++) {
                throttle.recordSignal(MetaServiceOverloadThrottle.Signal.TIMEOUT);
            }
        }
        Assert.assertEquals(MetaServiceOverloadThrottle.State.COOLDOWN, throttle.getState());
        Assert.assertEquals(Config.meta_service_rpc_overload_throttle_min_factor, throttle.getFactor(), 0.01);
    }

    @Test
    public void testFastDecreaseToCooldownTransition() {
        MetaServiceOverloadThrottle throttle = MetaServiceOverloadThrottle.getInstance();

        for (int i = 0; i < 5; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.SUCCESS);
        }
        for (int i = 0; i < 3; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.TIMEOUT);
        }
        Assert.assertEquals(MetaServiceOverloadThrottle.State.FAST_DECREASE, throttle.getState());

        for (int i = 0; i < 20; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.SUCCESS);
        }
        Assert.assertEquals(MetaServiceOverloadThrottle.State.COOLDOWN, throttle.getState());
    }

    @Test
    public void testCooldownToSlowRecoveryTransition() {
        MetaServiceOverloadThrottle throttle = MetaServiceOverloadThrottle.getInstance();

        triggerFastDecrease(throttle);
        transitionToCooldown(throttle);
        Assert.assertEquals(MetaServiceOverloadThrottle.State.COOLDOWN, throttle.getState());

        throttle.setCooldownStartMs(System.currentTimeMillis() - 31000);
        throttle.recordSignal(MetaServiceOverloadThrottle.Signal.SUCCESS);
        Assert.assertEquals(MetaServiceOverloadThrottle.State.SLOW_RECOVERY, throttle.getState());
    }

    @Test
    public void testSlowRecoveryIncreasesFactorGradually() {
        MetaServiceOverloadThrottle throttle = MetaServiceOverloadThrottle.getInstance();

        triggerFastDecrease(throttle);
        transitionToCooldown(throttle);

        throttle.setCooldownStartMs(System.currentTimeMillis() - 31000);
        throttle.recordSignal(MetaServiceOverloadThrottle.Signal.SUCCESS);
        Assert.assertEquals(MetaServiceOverloadThrottle.State.SLOW_RECOVERY, throttle.getState());

        double factorBefore = throttle.getFactor();
        throttle.setLastRecoveryMs(System.currentTimeMillis() - 6000);
        throttle.recordSignal(MetaServiceOverloadThrottle.Signal.SUCCESS);
        Assert.assertEquals(factorBefore + 0.05, throttle.getFactor(), 0.01);
        throttle.recordSignal(MetaServiceOverloadThrottle.Signal.SUCCESS);
        Assert.assertEquals(factorBefore + 0.05, throttle.getFactor(), 0.01);
        throttle.setLastRecoveryMs(System.currentTimeMillis() - 6000);
        throttle.recordSignal(MetaServiceOverloadThrottle.Signal.SUCCESS);
        Assert.assertEquals(factorBefore + 0.1, throttle.getFactor(), 0.01);
    }

    @Test
    public void testSlowRecoveryBackToNormal() {
        Config.meta_service_rpc_overload_throttle_recovery_step = 0.5;
        MetaServiceOverloadThrottle throttle = MetaServiceOverloadThrottle.getInstance();

        triggerFastDecrease(throttle);
        transitionToCooldown(throttle);

        throttle.setCooldownStartMs(System.currentTimeMillis() - 31000);
        throttle.recordSignal(MetaServiceOverloadThrottle.Signal.SUCCESS);
        Assert.assertEquals(MetaServiceOverloadThrottle.State.SLOW_RECOVERY, throttle.getState());

        for (int i = 0; i < 20; i++) {
            throttle.setLastRecoveryMs(System.currentTimeMillis() - 6000);
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.SUCCESS);
            if (throttle.getState() == MetaServiceOverloadThrottle.State.NORMAL) {
                break;
            }
        }

        Assert.assertEquals(MetaServiceOverloadThrottle.State.NORMAL, throttle.getState());
        Assert.assertEquals(1.0, throttle.getFactor(), 0.001);
    }

    @Test
    public void testCooldownBadSignalGoesBackToFastDecrease() {
        MetaServiceOverloadThrottle throttle = MetaServiceOverloadThrottle.getInstance();

        triggerFastDecrease(throttle);
        transitionToCooldown(throttle);
        Assert.assertEquals(MetaServiceOverloadThrottle.State.COOLDOWN, throttle.getState());

        double factorInCooldown = throttle.getFactor();
        for (int i = 0; i < 9; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.TIMEOUT);
        }

        Assert.assertEquals(MetaServiceOverloadThrottle.State.FAST_DECREASE, throttle.getState());
        Assert.assertTrue(throttle.getFactor() <= factorInCooldown);
    }

    @Test
    public void testSlowRecoveryBadSignalGoesBackToFastDecrease() {
        MetaServiceOverloadThrottle throttle = MetaServiceOverloadThrottle.getInstance();

        triggerFastDecrease(throttle);
        transitionToCooldown(throttle);
        throttle.setCooldownStartMs(System.currentTimeMillis() - 31000);
        throttle.recordSignal(MetaServiceOverloadThrottle.Signal.SUCCESS);
        Assert.assertEquals(MetaServiceOverloadThrottle.State.SLOW_RECOVERY, throttle.getState());

        for (int i = 0; i < 5; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.SUCCESS);
        }
        for (int i = 0; i < 3; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.OVERLOAD);
        }

        Assert.assertEquals(MetaServiceOverloadThrottle.State.FAST_DECREASE, throttle.getState());
    }

    private void triggerFastDecrease(MetaServiceOverloadThrottle throttle) {
        Config.meta_service_rpc_overload_event_count_trigger = 2;
        Config.meta_service_rpc_overload_throttle_min_window_requests = 5;
        Config.meta_service_rpc_overload_event_rate_trigger = 0.3;
        Config.meta_service_rpc_overload_throttle_decrease_multiplier = 0.7;
        Config.meta_service_rpc_overload_throttle_min_factor = 0.1;
        for (int i = 0; i < 5; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.SUCCESS);
        }
        for (int i = 0; i < 3; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.TIMEOUT);
        }
        Assert.assertEquals(MetaServiceOverloadThrottle.State.FAST_DECREASE, throttle.getState());
    }

    private void transitionToCooldown(MetaServiceOverloadThrottle throttle) {
        for (int i = 0; i < 20; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.SUCCESS);
        }
        Assert.assertEquals(MetaServiceOverloadThrottle.State.COOLDOWN, throttle.getState());
    }

    // ==================== Basic Functionality Tests ====================

    @Test
    public void testMixedSignalsInNormal() {
        MetaServiceOverloadThrottle throttle = MetaServiceOverloadThrottle.getInstance();
        Config.meta_service_rpc_overload_event_rate_trigger = 0.55;

        // Mix of success and bad signals - should trigger decrease
        for (int i = 0; i < 3; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.SUCCESS);
        }
        for (int i = 0; i < 2; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.TIMEOUT);
        }
        for (int i = 0; i < 2; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.OVERLOAD);
        }
        // Total: 3 success, 4 bad out of 7 = 57% bad rate > 5% threshold

        Assert.assertEquals(MetaServiceOverloadThrottle.State.FAST_DECREASE, throttle.getState());
        Assert.assertTrue(throttle.getFactor() < 1.0);
    }

    @Test
    public void testFastDecreaseStaysWhenOverloaded() {
        MetaServiceOverloadThrottle throttle = MetaServiceOverloadThrottle.getInstance();

        triggerFastDecrease(throttle);
        Assert.assertEquals(MetaServiceOverloadThrottle.State.FAST_DECREASE, throttle.getState());

        // Continue to record bad signals - should stay in FAST_DECREASE
        for (int i = 0; i < 6; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.TIMEOUT);
        }

        Assert.assertEquals(MetaServiceOverloadThrottle.State.FAST_DECREASE, throttle.getState());
    }

    // ==================== Overload Detection Tests ====================

    @Test
    public void testIsOverloaded_falseWhenBelowMinRequests() {
        Config.meta_service_rpc_overload_throttle_min_window_requests = 100;
        MetaServiceOverloadThrottle throttle = MetaServiceOverloadThrottle.getInstance();

        // Record some bad signals but below min threshold
        for (int i = 0; i < 50; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.TIMEOUT);
        }

        Assert.assertEquals(MetaServiceOverloadThrottle.State.NORMAL, throttle.getState());
    }

    @Test
    public void testIsOverloaded_falseWhenBelowBadCount() {
        Config.meta_service_rpc_overload_event_count_trigger = 10;
        MetaServiceOverloadThrottle throttle = MetaServiceOverloadThrottle.getInstance();

        // Record bad signals but below threshold
        for (int i = 0; i < 20; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.SUCCESS);
        }
        for (int i = 0; i < 5; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.TIMEOUT);
        }

        Assert.assertEquals(MetaServiceOverloadThrottle.State.NORMAL, throttle.getState());
    }

    @Test
    public void testIsOverloaded_falseWhenBelowBadRate() {
        Config.meta_service_rpc_overload_event_rate_trigger = 0.5;
        MetaServiceOverloadThrottle throttle = MetaServiceOverloadThrottle.getInstance();

        // Many successes, few timeouts - rate below threshold
        for (int i = 0; i < 20; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.SUCCESS);
        }
        for (int i = 0; i < 5; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.TIMEOUT);
        }
        // 5/25 = 20% bad rate < 50% threshold

        Assert.assertEquals(MetaServiceOverloadThrottle.State.NORMAL, throttle.getState());
    }

    @Test
    public void testIsOverloaded_trueWhenAllConditionsMet() {
        MetaServiceOverloadThrottle throttle = MetaServiceOverloadThrottle.getInstance();

        // All conditions met: min_requests=5, bad_count=2, bad_rate=5%
        for (int i = 0; i < 5; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.SUCCESS);
        }
        for (int i = 0; i < 3; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.TIMEOUT);
        }
        // 3/8 = 37.5% bad rate > 5% threshold

        Assert.assertEquals(MetaServiceOverloadThrottle.State.FAST_DECREASE, throttle.getState());
    }

    // ==================== Edge Case Tests ====================

    @Test
    public void testZeroRecoveryStep() {
        Config.meta_service_rpc_overload_throttle_recovery_step = 0.0;
        MetaServiceOverloadThrottle throttle = MetaServiceOverloadThrottle.getInstance();

        // Decrease first
        triggerFastDecrease(throttle);
        double decreasedFactor = throttle.getFactor();

        // Try to recover with zero step
        transitionToCooldown(throttle);
        throttle.setCooldownStartMs(System.currentTimeMillis() - 31000);
        throttle.recordSignal(MetaServiceOverloadThrottle.Signal.SUCCESS);

        // Set last recovery to past
        throttle.setLastRecoveryMs(System.currentTimeMillis() - 6000);
        throttle.recordSignal(MetaServiceOverloadThrottle.Signal.SUCCESS);

        // Factor should not increase with zero step
        Assert.assertEquals(decreasedFactor, throttle.getFactor(), 0.0001);
    }

    @Test
    public void testZeroCooldownPeriod() {
        Config.meta_service_rpc_overload_throttle_cooldown_ms = 0;
        MetaServiceOverloadThrottle throttle = MetaServiceOverloadThrottle.getInstance();

        // Transition to FAST_DECREASE
        triggerFastDecrease(throttle);

        // Record success - should transition immediately due to zero cooldown
        for (int i = 0; i < 5; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.SUCCESS);
        }

        // Should go to SLOW_RECOVERY directly (skip COOLDOWN)
        Assert.assertEquals(MetaServiceOverloadThrottle.State.SLOW_RECOVERY, throttle.getState());
    }

    @Test
    public void testVerySmallWindow() {
        Config.meta_service_rpc_overload_throttle_window_seconds = 0;
        MetaServiceOverloadThrottle throttle = MetaServiceOverloadThrottle.getInstance();

        // Window should reset frequently with 0 second window
        for (int i = 0; i < 10; i++) {
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.SUCCESS);
            throttle.recordSignal(MetaServiceOverloadThrottle.Signal.TIMEOUT);
        }

        // With 0 second window, should always be reset, so should not trigger overload
        Assert.assertEquals(MetaServiceOverloadThrottle.State.NORMAL, throttle.getState());
    }
}
