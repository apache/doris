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

package org.apache.doris.nereids.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;

class DateUtilsTest {

    private static final ZoneId SHANGHAI = ZoneId.of("Asia/Shanghai");
    private static final ZoneId NEW_YORK = ZoneId.of("America/New_York");
    private static final ZoneId UTC = ZoneId.of("UTC");
    private static final ZoneId FIXED = ZoneId.of("Etc/GMT-8"); // fixed +08:00, no transitions ever

    // epoch second of a UTC instant, for building test intervals independent of the session zone.
    private static long epoch(String utcInstant) {
        return Instant.parse(utcInstant).getEpochSecond();
    }

    // fixed-offset zones can never have a transition, on any interval including the whole axis.
    @Test
    void testFixedOffsetZoneNeverHasTransition() {
        Assertions.assertFalse(DateUtils.hasZoneOffsetTransition(UTC, null, null));
        Assertions.assertFalse(DateUtils.hasZoneOffsetTransition(FIXED, null, null));
        Assertions.assertFalse(DateUtils.hasZoneOffsetTransition(UTC,
                epoch("2024-01-01T00:00:00Z"), epoch("2024-12-31T00:00:00Z")));
    }

    // Asia/Shanghai has historical DST (last transition 1991), but NONE after. This is the crux of
    // the whole design: isFixedOffset() is false for Shanghai, yet a modern interval is transition
    // free, so a naive isFixedOffset() guard would wrongly reject Shanghai pushdown. The interval
    // test must return false for a modern Shanghai window.
    @Test
    void testShanghaiModernIntervalHasNoTransition() {
        Assertions.assertFalse(DateUtils.hasZoneOffsetTransition(SHANGHAI,
                epoch("2024-01-01T00:00:00Z"), epoch("2024-12-31T00:00:00Z")));
    }

    // ...but a Shanghai interval that spans its 1991 (or 1986-1990) DST transitions must return true.
    // This is the "historical data" correctness case: an epoch range crossing the 1991-04 spring
    // transition is NOT monotonic under from_unixtime.
    @Test
    void testShanghaiHistoricalIntervalHasTransition() {
        Assertions.assertTrue(DateUtils.hasZoneOffsetTransition(SHANGHAI,
                epoch("1991-01-01T00:00:00Z"), epoch("1991-12-31T00:00:00Z")));
    }

    // Shanghai unbounded-above from a modern lower bound: no future transition ever => false. This is
    // exactly the relaxed-pushdown case from_unixtime(col) >= c uses (upper = +infinity).
    @Test
    void testShanghaiUnboundedAboveModernIsTransitionFree() {
        Assertions.assertFalse(DateUtils.hasZoneOffsetTransition(SHANGHAI,
                epoch("2024-06-01T00:00:00Z"), null));
    }

    // Shanghai unbounded-above from BEFORE 1991: the 1991 transition lies ahead => true.
    @Test
    void testShanghaiUnboundedAbovePre1991HasTransition() {
        Assertions.assertTrue(DateUtils.hasZoneOffsetTransition(SHANGHAI,
                epoch("1985-01-01T00:00:00Z"), null));
    }

    // America/New_York observes DST every year. A summer-spanning-nothing window between the March
    // and November transitions is transition free (pushdown SOUND even in a DST zone!), proving the
    // design is per-interval, not a blunt per-zone ban.
    @Test
    void testNewYorkMidSummerIntervalHasNoTransition() {
        // 2024-04-01 .. 2024-10-01 UTC: entirely between 2024-03-10 (spring) and 2024-11-03 (fall).
        Assertions.assertFalse(DateUtils.hasZoneOffsetTransition(NEW_YORK,
                epoch("2024-04-01T00:00:00Z"), epoch("2024-10-01T00:00:00Z")));
    }

    // America/New_York interval spanning the fall-back transition must return true.
    @Test
    void testNewYorkIntervalAcrossFallBackHasTransition() {
        // 2024-11-03T06:00Z is the fall-back instant; bracket it.
        Assertions.assertTrue(DateUtils.hasZoneOffsetTransition(NEW_YORK,
                epoch("2024-10-01T00:00:00Z"), epoch("2024-12-01T00:00:00Z")));
    }

    // The upper bound is INCLUSIVE: a fall-back transition landing EXACTLY on the closed upper endpoint
    // must count. At that instant the wall clock rewinds (01:59:59 -> 01:00), so from_unixtime is not
    // monotonic on an interval whose closed top is that instant -- missing it (the old exclusive
    // isBefore check) would wrongly declare monotonicity and let partition pruning derive a too-narrow
    // function range. 2024-11-03T06:00:00Z is the New_York fall-back instant.
    @Test
    void testNewYorkTransitionExactlyOnUpperBoundIsInclusive() {
        long fallBack = epoch("2024-11-03T06:00:00Z");
        Assertions.assertTrue(DateUtils.hasZoneOffsetTransition(NEW_YORK, fallBack - 3600, fallBack),
                "a transition exactly at the closed upper bound must be detected");
    }

    // Mirror of the above: the lower bound stays EXCLUSIVE. A transition exactly at the lower endpoint
    // leaves the whole interval on one side of it (every epoch in the interval is >= the transition),
    // so monotonicity is not broken and it must NOT count. 2024-11-03T06:00:00Z again.
    @Test
    void testNewYorkTransitionExactlyOnLowerBoundIsExclusive() {
        long fallBack = epoch("2024-11-03T06:00:00Z");
        Assertions.assertFalse(DateUtils.hasZoneOffsetTransition(NEW_YORK, fallBack, fallBack + 3600),
                "a transition exactly at the exclusive lower bound must not be detected");
    }

    // New_York unbounded-above from any modern date: DST recurs forever => true. This is why a DST
    // zone cannot get an unbounded (>= c) pushdown even in modern times.
    @Test
    void testNewYorkUnboundedAboveHasTransition() {
        Assertions.assertTrue(DateUtils.hasZoneOffsetTransition(NEW_YORK,
                epoch("2024-06-01T00:00:00Z"), null));
    }
}
