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

import org.apache.doris.common.profile.SummaryProfile.NestedStepTimer;
import org.apache.doris.common.profile.SummaryProfile.TimeStats;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class ProfileSpanWithStepTimerTest {
    private static final double TIMING_ERROR_MARGIN = 0.3; // Allow 30% error margin for timing tests

    private static final long MIN_SLEEP_TIME = 10; // Minimum sleep time to avoid too short durations

    private SummaryProfile summaryProfile;

    @Before
    public void setUp() {
        summaryProfile = new SummaryProfile();
    }

    @Test
    public void testBasicTiming() {
        String nodeId = "node1";
        NestedStepTimer timer = summaryProfile.createNodeTimer(nodeId);

        try (ProfileSpan span = ProfileSpan.create(summaryProfile, nodeId, "step1")) {
            sleep(100);
        }

        TimeStats stepStats = timer.getScanNodesStats().get("step1");
        Assert.assertNotNull("Stats should exist for step1", stepStats);
        Assert.assertEquals("Should have 1 execution", 1, stepStats.getCount());
        assertTimingWithinRange(100, stepStats.getSum());
    }

    @Test
    public void testNestedSpans() {
        String nodeId = "node1";
        NestedStepTimer timer = summaryProfile.createNodeTimer(nodeId);

        try (ProfileSpan outerSpan = ProfileSpan.create(summaryProfile, nodeId, "outer")) {
            sleep(50);

            try (ProfileSpan innerSpan = ProfileSpan.create(summaryProfile, nodeId, "inner")) {
                sleep(100);
            }

            sleep(50);
        }

        TimeStats outerStats = timer.getScanNodesStats().get("outer");
        TimeStats innerStats = timer.getScanNodesStats().get("inner");

        Assert.assertNotNull("Outer stats should exist", outerStats);
        Assert.assertNotNull("Inner stats should exist", innerStats);

        assertTimingWithinRange(100, innerStats.getSum());
        assertTimingWithinRange(200, outerStats.getSum());
    }

    @Test
    public void testMultipleExecutions() {
        String nodeId = "node1";
        NestedStepTimer timer = summaryProfile.createNodeTimer(nodeId);

        // First execution
        try (ProfileSpan span = ProfileSpan.create(summaryProfile, nodeId, "step1")) {
            sleep(50);
        }

        // Second execution
        try (ProfileSpan span = ProfileSpan.create(summaryProfile, nodeId, "step1")) {
            sleep(50);
        }

        TimeStats stats = timer.getScanNodesStats().get("step1");
        Assert.assertEquals("Should have 2 executions", 2, stats.getCount());
        assertTimingWithinRange(100, stats.getSum());
    }

    @Test
    public void testComplexScenario() {
        String nodeId = "node1";
        NestedStepTimer timer = summaryProfile.createNodeTimer(nodeId);

        try (ProfileSpan transaction = ProfileSpan.create(summaryProfile, nodeId, "transaction")) {
            sleep(50);

            try (ProfileSpan prepare = ProfileSpan.create(summaryProfile, nodeId, "prepare")) {
                sleep(100);
            }

            try (ProfileSpan execute = ProfileSpan.create(summaryProfile, nodeId, "execute")) {
                try (ProfileSpan subquery1 = ProfileSpan.create(summaryProfile, nodeId, "subquery1")) {
                    sleep(150);
                }

                try (ProfileSpan subquery2 = ProfileSpan.create(summaryProfile, nodeId, "subquery2")) {
                    sleep(200);
                }
            }

            try (ProfileSpan commit = ProfileSpan.create(summaryProfile, nodeId, "commit")) {
                sleep(50);
            }
        }

        Assert.assertEquals("Should have all steps", 6, timer.getScanNodesStats().size());

        verifyStepDuration("transaction", timer.getScanNodesStats(), 550);
        verifyStepDuration("prepare", timer.getScanNodesStats(), 100);
        verifyStepDuration("execute", timer.getScanNodesStats(), 350);
        verifyStepDuration("subquery1", timer.getScanNodesStats(), 150);
        verifyStepDuration("subquery2", timer.getScanNodesStats(), 200);
        verifyStepDuration("commit", timer.getScanNodesStats(), 50);
    }

    @Test
    public void testExceptionHandling() {
        String nodeId = "node1";
        NestedStepTimer timer = summaryProfile.createNodeTimer(nodeId);

        try {
            try (ProfileSpan span = ProfileSpan.create(summaryProfile, nodeId, "errorStep")) {
                sleep(100);
                throw new RuntimeException("Test exception");
            }
        } catch (RuntimeException e) {
            // Expected exception
        }

        TimeStats stats = timer.getScanNodesStats().get("errorStep");
        Assert.assertNotNull("Stats should exist despite exception", stats);
        Assert.assertEquals("Should have 1 execution", 1, stats.getCount());
        assertTimingWithinRange(100, stats.getSum());
    }

    @Test
    public void testMultipleNodes() {
        String node1 = "node1";
        String node2 = "node2";
        NestedStepTimer timer1 = summaryProfile.createNodeTimer(node1);
        NestedStepTimer timer2 = summaryProfile.createNodeTimer(node2);

        try (ProfileSpan span1 = ProfileSpan.create(summaryProfile, node1, "step1")) {
            sleep(100);
        }

        try (ProfileSpan span2 = ProfileSpan.create(summaryProfile, node2, "step1")) {
            sleep(100);
        }

        TimeStats stats1 = timer1.getScanNodesStats().get("step1");
        Assert.assertEquals("Should have 1 executions", 2, stats1.getCount());

        TimeStats stats2 = timer2.getScanNodesStats().get("step1");
        Assert.assertEquals("Should have 1 executions", 2, stats2.getCount());
    }

    private void verifyStepDuration(String step, Map<String, TimeStats> scanNodeStats, long expectedDuration) {
        TimeStats stats = scanNodeStats.get(step);
        Assert.assertNotNull("Stats should exist for " + step, stats);
        assertTimingWithinRange(expectedDuration, stats.getSum());
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void assertTimingWithinRange(long expected, long actual) {
        long allowedDelta = (long) (expected * TIMING_ERROR_MARGIN);
        long minExpected = Math.max(expected - allowedDelta, MIN_SLEEP_TIME);
        long maxExpected = expected + allowedDelta;

        Assert.assertTrue(
                String.format(
                        "Timing %d ms should be between %d ms and %d ms",
                        actual, minExpected, maxExpected
                ),
                actual >= minExpected && actual <= maxExpected
        );
    }
}
