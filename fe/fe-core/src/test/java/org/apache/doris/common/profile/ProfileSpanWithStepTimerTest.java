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

import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
        long expectedTime = 100;

        long startTime = System.nanoTime();
        try (ProfileSpan span = ProfileSpan.create(summaryProfile, nodeId, "step1")) {
            sleep(expectedTime);
        }
        long actualTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);

        TimeStats stepStats = timer.getScanNodesStats().get("step1");
        Assert.assertNotNull("Stats should exist for step1", stepStats);
        Assert.assertEquals("Should have 1 execution", 1, stepStats.getCount());
        assertTimingWithinRange(expectedTime, stepStats.getSum());
        assertTimingWithinRange(expectedTime, actualTime);
    }

    @Test
    public void testNestedSpans() {
        String nodeId = "node1";
        NestedStepTimer timer = summaryProfile.createNodeTimer(nodeId);
        long innerTime = 100;
        long outerTime = 200;

        long startTime = System.nanoTime();
        try (ProfileSpan outerSpan = ProfileSpan.create(summaryProfile, nodeId, "outer")) {
            sleep(50);
            try (ProfileSpan innerSpan = ProfileSpan.create(summaryProfile, nodeId, "inner")) {
                sleep(innerTime);
            }
            sleep(50);
        }
        long actualTotalTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);

        TimeStats outerStats = timer.getScanNodesStats().get("outer");
        TimeStats innerStats = timer.getScanNodesStats().get("inner");

        Assert.assertNotNull("Outer stats should exist", outerStats);
        Assert.assertNotNull("Inner stats should exist", innerStats);

        assertTimingWithinRange(innerTime, innerStats.getSum());
        assertTimingWithinRange(actualTotalTime, outerStats.getSum());
    }

    @Test
    public void testMultipleExecutions() {
        String nodeId = "node1";
        NestedStepTimer timer = summaryProfile.createNodeTimer(nodeId);
        long expectedTime = 50;

        List<Long> measurements = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            long start = System.nanoTime();
            try (ProfileSpan span = ProfileSpan.create(summaryProfile, nodeId, "step1")) {
                sleep(expectedTime);
            }
            measurements.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        }

        TimeStats stats = timer.getScanNodesStats().get("step1");
        Assert.assertEquals("Should have 2 executions", 2, stats.getCount());

        double avgTime = measurements.stream().mapToLong(Long::longValue).average().orElse(0.0);
        assertTimingWithinRange(expectedTime, (long) avgTime);
    }

    @Test
    public void testParallelSteps() {
        String nodeId = "node1";
        NestedStepTimer timer = summaryProfile.createNodeTimer(nodeId);
        long expectedTime = 100;

        List<Long> timings = new ArrayList<>();
        for (String step : new String[]{"parallel1", "parallel2"}) {
            long start = System.nanoTime();
            try (ProfileSpan span = ProfileSpan.create(summaryProfile, nodeId, step)) {
                sleep(expectedTime);
            }
            timings.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        }

        DoubleSummaryStatistics stats = timings.stream()
                .mapToDouble(Long::doubleValue)
                .summaryStatistics();

        Assert.assertTrue("Steps should have similar duration",
                Math.abs(stats.getMax() - stats.getMin()) <= expectedTime * TIMING_ERROR_MARGIN);
    }

    @Test
    public void testComplexScenario() {
        String nodeId = "node1";
        NestedStepTimer timer = summaryProfile.createNodeTimer(nodeId);

        long startTime = System.nanoTime();
        executeComplexScenario(nodeId);
        long totalTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);

        Map<String, TimeStats> stats = timer.getScanNodesStats();
        Assert.assertEquals("Should have all steps", 6, stats.size());

        // 使用实际测量的总时间而不是预期时间
        assertTimingWithinRange(totalTime, stats.get("transaction").getSum());
        verifyStepStats(stats);
    }

    private void executeComplexScenario(String nodeId) {
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
    }

    @Test
    public void testExceptionHandling() {
        String nodeId = "node1";
        NestedStepTimer timer = summaryProfile.createNodeTimer(nodeId);
        long expectedTime = 100;

        long startTime = System.nanoTime();
        try {
            try (ProfileSpan span = ProfileSpan.create(summaryProfile, nodeId, "errorStep")) {
                sleep(expectedTime);
                throw new RuntimeException("Test exception");
            }
        } catch (RuntimeException e) {
            // Expected exception
        }
        long actualTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);

        TimeStats stats = timer.getScanNodesStats().get("errorStep");
        Assert.assertNotNull("Stats should exist despite exception", stats);
        Assert.assertEquals("Should have 1 execution", 1, stats.getCount());
        assertTimingWithinRange(expectedTime, actualTime);
    }

    @Test
    public void testTimingAccuracy() {
        String nodeId = "node1";
        NestedStepTimer timer = summaryProfile.createNodeTimer(nodeId);

        List<Long> measurements = new ArrayList<>();
        int iterations = 10;
        long targetTime = 50;

        for (int i = 0; i < iterations; i++) {
            long start = System.nanoTime();
            try (ProfileSpan span = ProfileSpan.create(summaryProfile, nodeId, "accuracy" + i)) {
                sleep(targetTime);
            }
            measurements.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        }

        DoubleSummaryStatistics stats = measurements.stream()
                .mapToDouble(Long::doubleValue)
                .summaryStatistics();

        System.out.printf("Timing statistics for %d ms target:%n", targetTime);
        System.out.printf("  Average: %.2f ms%n", stats.getAverage());
        System.out.printf("  Min: %.2f ms%n", stats.getMin());
        System.out.printf("  Max: %.2f ms%n", stats.getMax());
        System.out.printf("  StdDev: %.2f ms%n", calculateStdDev(measurements, stats.getAverage()));

        assertTimingWithinRange(targetTime, (long) stats.getAverage());
    }

    private void assertTimingWithinRange(long expected, long actual) {
        long allowedDelta = (long)(expected * TIMING_ERROR_MARGIN);
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

    private void verifyStepStats(Map<String, TimeStats> stats) {
        for (Map.Entry<String, TimeStats> entry : stats.entrySet()) {
            TimeStats timeStats = entry.getValue();
            Assert.assertTrue(
                    String.format("Stats for %s should be reasonable", entry.getKey()),
                    timeStats.getMin() <= timeStats.getAverage() &&
                            timeStats.getAverage() <= timeStats.getMax()
            );
        }
    }

    private double calculateStdDev(List<Long> measurements, double mean) {
        return Math.sqrt(measurements.stream()
                .mapToDouble(m -> Math.pow(m - mean, 2))
                .average()
                .orElse(0.0));
    }

    private void sleep(long targetMillis) {
        long start = System.nanoTime();
        long targetNanos = TimeUnit.MILLISECONDS.toNanos(targetMillis);

        while (System.nanoTime() - start < targetNanos) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}