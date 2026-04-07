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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SummaryProfileTest {

    @Test
    public void testPlanSummaryWithQueryTrace() {
        SummaryProfile profile = new SummaryProfile();
        QueryTrace qt = profile.getQueryTrace();

        // Simulate Parse SQL Time
        qt.recordDuration("Parse SQL Time", 3);

        // Simulate Plan sub-phase durations using QueryTrace
        qt.recordDuration("Nereids Lock Table Time", 4);
        qt.recordDuration("Nereids Analysis Time", 5);
        qt.recordDuration("Nereids Rewrite Time", 6);
        qt.recordDuration("Nereids Collect Table Partition Time", 7);
        qt.recordDuration("Nereids Pre Rewrite By MV Time", 3);
        qt.recordDuration("Nereids Optimize Time", 5);
        qt.recordDuration("Nereids Translate Time", 9);
        qt.recordDuration("Nereids Distribute Time", 10);

        // Simulate Plan Time (total)
        qt.recordDuration("Plan Time", 60);

        // Simulate Schedule Time
        qt.recordDuration("Schedule Time", 12);

        // Simulate Fetch Result Time
        qt.recordDuration("Fetch Result Time", 13);

        // Update the profile
        profile.update(ImmutableMap.of());

        // Verify getters read from QueryTrace
        Assertions.assertEquals(3, profile.getParseSqlTimeMs());
        Assertions.assertEquals(60, profile.getPlanTimeMs());
        Assertions.assertEquals(4, profile.getNereidsLockTableTimeMs());
        Assertions.assertEquals(5, profile.getNereidsAnalysisTimeMs());
        Assertions.assertEquals(6, profile.getNereidsRewriteTimeMs());
        Assertions.assertEquals(7, profile.getNereidsCollectTablePartitionTimeMs());
        Assertions.assertEquals(5, profile.getNereidsOptimizeTimeMs());
        Assertions.assertEquals(9, profile.getNereidsTranslateTimeMs());
        Assertions.assertEquals(10, profile.getNereidsDistributeTimeMs());
        Assertions.assertEquals(12, profile.getScheduleTimeMs());

        // Verify populateProfile outputs to executionSummaryProfile
        RuntimeProfile executionSummary = profile.getExecutionSummary();
        Assertions.assertEquals("3ms", executionSummary.getInfoString(SummaryProfile.PARSE_SQL_TIME));
        Assertions.assertEquals("60ms", executionSummary.getInfoString(SummaryProfile.PLAN_TIME));
        Assertions.assertEquals("4ms", executionSummary.getInfoString(SummaryProfile.NEREIDS_LOCK_TABLE_TIME));
        Assertions.assertEquals("5ms", executionSummary.getInfoString(SummaryProfile.NEREIDS_ANALYSIS_TIME));
        Assertions.assertEquals("6ms", executionSummary.getInfoString(SummaryProfile.NEREIDS_REWRITE_TIME));
        Assertions.assertEquals("5ms", executionSummary.getInfoString(SummaryProfile.NEREIDS_OPTIMIZE_TIME));
        Assertions.assertEquals("9ms", executionSummary.getInfoString(SummaryProfile.NEREIDS_TRANSLATE_TIME));
        Assertions.assertEquals("10ms", executionSummary.getInfoString(SummaryProfile.NEREIDS_DISTRIBUTE_TIME));
        Assertions.assertEquals("12ms", executionSummary.getInfoString(SummaryProfile.SCHEDULE_TIME));
    }

    @Test
    public void testAutoCloseableSpan() throws Exception {
        SummaryProfile profile = new SummaryProfile();
        QueryTrace qt = profile.getQueryTrace();

        try (ProfileSpan span = qt.startSpan("Test Span")) {
            Thread.sleep(10);
        }

        long duration = qt.getDurationMs("Test Span");
        Assertions.assertTrue(duration >= 10, "Span duration should be >= 10ms, was: " + duration);
    }

    @Test
    public void testCumulativeRecordDuration() {
        SummaryProfile profile = new SummaryProfile();
        QueryTrace qt = profile.getQueryTrace();

        qt.recordDuration("Fetch Result Time", 5);
        qt.recordDuration("Fetch Result Time", 3);
        qt.recordDuration("Fetch Result Time", 7);

        Assertions.assertEquals(15, qt.getDurationMs("Fetch Result Time"));
    }

    @Test
    public void testSetText() {
        SummaryProfile profile = new SummaryProfile();
        QueryTrace qt = profile.getQueryTrace();

        qt.setText("Executed By Frontend", "Yes");

        // Verify via populateProfile
        profile.update(ImmutableMap.of());
        RuntimeProfile executionSummary = profile.getExecutionSummary();
        Assertions.assertEquals("Yes", executionSummary.getInfoString("Executed By Frontend"));
    }

    @Test
    public void testAddCounter() {
        SummaryProfile profile = new SummaryProfile();
        QueryTrace qt = profile.getQueryTrace();

        qt.addCounter("Rename File Count", 3);
        qt.addCounter("Rename File Count", 5);

        Assertions.assertEquals(8, qt.getCounterValue("Rename File Count"));
    }
}
