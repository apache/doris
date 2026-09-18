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

package org.apache.doris.regression.suite

import org.junit.jupiter.api.Test

import java.time.LocalDateTime
import java.util.concurrent.TimeUnit

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue

class SuiteBoundaryWaitTest {
    @Test
    void waitCrossesBoundaryWhenRemainingTimeHasFractionalSecond() {
        LocalDateTime now = LocalDateTime.parse("2026-07-16T22:59:40.285999999")
        LocalDateTime nextHour = LocalDateTime.parse("2026-07-16T23:00:00")

        long waitMillis = Suite.calculateBoundarySleepMillis(now, nextHour, 45)

        assertTrue(now.plusNanos(TimeUnit.MILLISECONDS.toNanos(waitMillis)).isAfter(nextHour))
    }

    @Test
    void waitCrossesBoundaryWhenExpectedDurationEndsExactlyAtBoundary() {
        LocalDateTime now = LocalDateTime.parse("2026-07-16T22:59:15")
        LocalDateTime nextHour = LocalDateTime.parse("2026-07-16T23:00:00")

        long waitMillis = Suite.calculateBoundarySleepMillis(now, nextHour, 45)

        assertTrue(now.plusNanos(TimeUnit.MILLISECONDS.toNanos(waitMillis)).isAfter(nextHour))
    }

    @Test
    void noWaitWhenExpectedDurationFinishesBeforeBoundary() {
        LocalDateTime now = LocalDateTime.parse("2026-07-16T22:59:14.999")
        LocalDateTime nextHour = LocalDateTime.parse("2026-07-16T23:00:00")

        assertEquals(0, Suite.calculateBoundarySleepMillis(now, nextHour, 45))
    }
}
