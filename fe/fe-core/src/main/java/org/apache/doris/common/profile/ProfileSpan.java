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

package org.apache.doris.common.profile;

import java.util.concurrent.atomic.AtomicLong;

/**
 * ProfileSpan represents a timing span for profiling execution phases.
 * It supports three usage modes:
 * <ul>
 *   <li>Scoped: use with try-with-resources for phases completed within a single method</li>
 *   <li>Manual: call start()/finish() explicitly for phases spanning multiple methods</li>
 *   <li>Accumulative: use addElapsed() to accumulate time across multiple invocations</li>
 * </ul>
 */
public class ProfileSpan implements AutoCloseable {
    private final String name;
    private final Counter counter;
    private volatile long startTimeMs = -1;
    private final AtomicLong accumulatedValue = new AtomicLong(0);
    private final boolean accumulative;

    ProfileSpan(String name, Counter counter, boolean accumulative) {
        this.name = name;
        this.counter = counter;
        this.accumulative = accumulative;
    }

    public String getName() {
        return name;
    }

    public ProfileSpan start() {
        this.startTimeMs = System.currentTimeMillis();
        return this;
    }

    public void finish() {
        long st = startTimeMs;
        if (st != -1) {
            startTimeMs = -1;
            long elapsed = System.currentTimeMillis() - st;
            if (accumulative) {
                counter.setValue(accumulatedValue.addAndGet(elapsed));
            } else {
                counter.setValue(elapsed);
            }
        }
    }

    /** Accumulative mode: add pre-computed elapsed time */
    public void addElapsed(long elapsedMs) {
        counter.setValue(accumulatedValue.addAndGet(elapsedMs));
    }

    /** Set counter value directly (for non-time counters) */
    public void setValue(long value) {
        counter.setValue(value);
    }

    /** Thread-safe accumulation (for concurrent counting scenarios) */
    public void addValue(long delta) {
        counter.setValue(accumulatedValue.addAndGet(delta));
    }

    public long getValue() {
        return counter.getValue();
    }

    public Counter getCounter() {
        return counter;
    }

    @Override
    public void close() {
        finish();
    }
}
