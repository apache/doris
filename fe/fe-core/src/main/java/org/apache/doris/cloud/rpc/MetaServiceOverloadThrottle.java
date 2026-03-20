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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * Overload throttle controller for meta-service RPCs.
 *
 * <p>Monitors RPC outcomes (success, timeout, overload) within a rolling time window
 * and adjusts a global factor (0.1–1.0) that scales the configured QPS limits in
 * {@link MetaServiceRpcLimiterManager}. Uses a state machine:
 * NORMAL → FAST_DECREASE → COOLDOWN → SLOW_RECOVERY → NORMAL.
 */
public class MetaServiceOverloadThrottle {
    private static final Logger LOG = LogManager.getLogger(MetaServiceOverloadThrottle.class);

    public enum State {
        NORMAL,
        FAST_DECREASE,
        COOLDOWN,
        SLOW_RECOVERY
    }

    public enum Signal {
        SUCCESS,
        TIMEOUT,
        OVERLOAD
    }

    private volatile State state = State.NORMAL;
    private volatile double factor = 1.0;

    private final LongAdder windowTotal = new LongAdder();
    private final LongAdder windowBad = new LongAdder();
    private final AtomicLong windowStartMs = new AtomicLong(System.currentTimeMillis());

    private volatile long cooldownStartMs = 0;
    private volatile long lastRecoveryMs = 0;

    private static volatile MetaServiceOverloadThrottle instance;
    private volatile Consumer<Double> factorChangeListener = null;

    private MetaServiceOverloadThrottle() {
    }

    public static MetaServiceOverloadThrottle getInstance() {
        if (instance == null) {
            synchronized (MetaServiceOverloadThrottle.class) {
                if (instance == null) {
                    instance = new MetaServiceOverloadThrottle();
                }
            }
        }
        return instance;
    }

    public void recordSignal(Signal signal) {
        if (!Config.meta_service_rpc_overload_throttle_enabled) {
            return;
        }

        long now = System.currentTimeMillis();
        maybeResetWindow(now);

        windowTotal.increment();
        if (signal == Signal.OVERLOAD || signal == Signal.TIMEOUT) {
            windowBad.increment();
        }

        switch (state) {
            case NORMAL:
                handleNormal(now);
                break;
            case FAST_DECREASE:
                handleFastDecrease(now);
                break;
            case COOLDOWN:
                handleCooldown(now, signal);
                break;
            case SLOW_RECOVERY:
                handleSlowRecovery(now, signal);
                break;
            default:
                break;
        }
    }

    private void handleNormal(long now) {
        if (isOverloaded()) {
            transitionTo(State.FAST_DECREASE, now);
            decreaseFactor();
        }
    }

    private void handleFastDecrease(long now) {
        if (isOverloaded()) {
            decreaseFactor();
        } else {
            transitionTo(State.COOLDOWN, now);
            cooldownStartMs = now;
        }
    }

    private void handleCooldown(long now, Signal signal) {
        if (signal == Signal.OVERLOAD || signal == Signal.TIMEOUT) {
            if (isOverloaded()) {
                transitionTo(State.FAST_DECREASE, now);
                decreaseFactor();
                return;
            }
        }

        long cooldownMs = Config.meta_service_rpc_overload_throttle_cooldown_ms;
        if (now - cooldownStartMs >= cooldownMs) {
            transitionTo(State.SLOW_RECOVERY, now);
            lastRecoveryMs = now;
        }
    }

    private void handleSlowRecovery(long now, Signal signal) {
        if (signal == Signal.OVERLOAD || signal == Signal.TIMEOUT) {
            if (isOverloaded()) {
                transitionTo(State.FAST_DECREASE, now);
                decreaseFactor();
                return;
            }
        }

        long recoveryIntervalMs = Config.meta_service_rpc_overload_throttle_recovery_interval_ms;
        if (now - lastRecoveryMs >= recoveryIntervalMs) {
            lastRecoveryMs = now;
            double step = Config.meta_service_rpc_overload_throttle_recovery_step;
            double newFactor = Math.min(1.0, factor + step);
            setFactor(newFactor);

            if (Double.compare(newFactor, 1.0) >= 0) {
                transitionTo(State.NORMAL, now);
            }
        }
    }

    private boolean isOverloaded() {
        long total = windowTotal.sum();
        long bad = windowBad.sum();
        int minRequests = Config.meta_service_rpc_overload_throttle_min_window_requests;
        int badTriggerCount = Config.meta_service_rpc_overload_event_count_trigger;
        double badRateTrigger = Config.meta_service_rpc_overload_event_rate_trigger;

        if (total < minRequests) {
            return false;
        }
        if (bad < badTriggerCount) {
            return false;
        }
        return (double) bad / total >= badRateTrigger;
    }

    private void decreaseFactor() {
        double multiplier = Config.meta_service_rpc_overload_throttle_decrease_multiplier;
        double minFactor = Config.meta_service_rpc_overload_throttle_min_factor;
        double newFactor = Math.max(minFactor, factor * multiplier);
        setFactor(newFactor);
    }

    private void setFactor(double newFactor) {
        double oldFactor = this.factor;
        this.factor = newFactor;

        if (Math.abs(newFactor - oldFactor) > 0.001) {
            LOG.info("Overload throttle factor changed: {} -> {} (state={})", oldFactor, newFactor, state);
            Consumer<Double> listener = this.factorChangeListener;
            if (listener != null) {
                listener.accept(newFactor);
            }
        }
    }

    private void transitionTo(State newState, long now) {
        State oldState = this.state;
        this.state = newState;
        if (oldState != newState) {
            LOG.info("Overload throttle state transition: {} -> {} (factor={})", oldState, newState, factor);
            resetWindow(now);
        }
    }

    private void maybeResetWindow(long now) {
        long windowMs = Config.meta_service_rpc_overload_throttle_window_seconds * 1000L;
        long startMs = windowStartMs.get();
        if (now - startMs >= windowMs) {
            // CAS ensures only one thread resets the window per interval
            if (windowStartMs.compareAndSet(startMs, now)) {
                windowTotal.reset();
                windowBad.reset();
            }
        }
    }

    private void resetWindow(long now) {
        windowStartMs.set(now);
        windowTotal.reset();
        windowBad.reset();
    }

    public void setFactorChangeListener(Consumer<Double> listener) {
        this.factorChangeListener = listener;
    }

    public State getState() {
        return state;
    }

    public double getFactor() {
        return factor;
    }

    // only for testing
    public long getWindowTotal() {
        return windowTotal.sum();
    }

    // only for testing
    public long getWindowBad() {
        return windowBad.sum();
    }

    // only for testing
    public void setCooldownStartMs(long ms) {
        this.cooldownStartMs = ms;
    }

    // only for testing
    public void setLastRecoveryMs(long ms) {
        this.lastRecoveryMs = ms;
    }
}
