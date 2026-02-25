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

import org.apache.doris.thrift.TUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProfileSpanTest {

    @Test
    public void testManualStartFinish() throws InterruptedException {
        Counter counter = new Counter(TUnit.TIME_MS, 0);
        ProfileSpan span = new ProfileSpan("test", counter, false);

        span.start();
        Thread.sleep(50);
        span.finish();

        Assertions.assertTrue(counter.getValue() >= 40,
                "Elapsed time should be at least 40ms, got: " + counter.getValue());
    }

    @Test
    public void testSetValueDirectly() {
        Counter counter = new Counter(TUnit.TIME_MS, 0);
        ProfileSpan span = new ProfileSpan("test", counter, false);

        span.setValue(42);
        Assertions.assertEquals(42, span.getValue());
        Assertions.assertEquals(42, counter.getValue());
    }

    @Test
    public void testAccumulativeMode() {
        Counter counter = new Counter(TUnit.TIME_MS, 0);
        ProfileSpan span = new ProfileSpan("test", counter, true);

        span.addElapsed(10);
        Assertions.assertEquals(10, counter.getValue());

        span.addElapsed(20);
        Assertions.assertEquals(30, counter.getValue());

        span.addElapsed(5);
        Assertions.assertEquals(35, counter.getValue());
    }

    @Test
    public void testAccumulativeStartFinish() throws InterruptedException {
        Counter counter = new Counter(TUnit.TIME_MS, 0);
        ProfileSpan span = new ProfileSpan("test", counter, true);

        // First accumulation
        span.start();
        Thread.sleep(50);
        span.finish();
        long first = counter.getValue();
        Assertions.assertTrue(first >= 40);

        // Second accumulation - should add to previous
        span.start();
        Thread.sleep(50);
        span.finish();
        long second = counter.getValue();
        Assertions.assertTrue(second >= first + 40,
                "Accumulated value should increase, got: " + second);
    }

    @Test
    public void testAddValue() {
        Counter counter = new Counter(TUnit.UNIT, 0);
        ProfileSpan span = new ProfileSpan("count", counter, true);

        span.addValue(5);
        Assertions.assertEquals(5, counter.getValue());

        span.addValue(3);
        Assertions.assertEquals(8, counter.getValue());
    }

    @Test
    public void testDoubleFinishSafe() {
        Counter counter = new Counter(TUnit.TIME_MS, 0);
        ProfileSpan span = new ProfileSpan("test", counter, false);

        span.start();
        span.finish();
        long afterFirst = counter.getValue();

        // Second finish should be a no-op
        span.finish();
        Assertions.assertEquals(afterFirst, counter.getValue());
    }

    @Test
    public void testAutoCloseable() throws InterruptedException {
        Counter counter = new Counter(TUnit.TIME_MS, 0);
        ProfileSpan span = new ProfileSpan("test", counter, false);

        try (ProfileSpan s = span) {
            s.start();
            Thread.sleep(50);
        }
        // close() calls finish()
        Assertions.assertTrue(counter.getValue() >= 40);
    }

    @Test
    public void testFinishWithoutStartIsNoOp() {
        Counter counter = new Counter(TUnit.TIME_MS, 0);
        ProfileSpan span = new ProfileSpan("test", counter, false);

        // finish without start should be safe
        span.finish();
        Assertions.assertEquals(0, counter.getValue());
    }
}
