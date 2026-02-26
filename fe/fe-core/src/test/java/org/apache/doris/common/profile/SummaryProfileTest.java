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

import org.apache.doris.common.util.SafeStringBuilder;
import org.apache.doris.thrift.TUnit;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SummaryProfileTest {

    @Test
    public void testTracerSpanCounterHierarchy() {
        SummaryProfile profile = new SummaryProfile();
        ProfileTracer tracer = profile.getTracer();

        // Simulate the execution flow using spans
        // Parse SQL
        ProfileSpan parseSpan = tracer.startSpan(SummaryProfile.PARSE_SQL_TIME, TUnit.TIME_MS);
        parseSpan.setValue(3);

        // Plan Time (top-level)
        ProfileSpan planSpan = tracer.startSpan(SummaryProfile.PLAN_TIME, TUnit.TIME_MS);
        planSpan.setValue(60);

        // Nereids sub-phases (children of Plan Time)
        ProfileSpan lockSpan = tracer.startSpan(
                SummaryProfile.NEREIDS_LOCK_TABLE_TIME, TUnit.TIME_MS, SummaryProfile.PLAN_TIME);
        lockSpan.setValue(4);

        ProfileSpan analysisSpan = tracer.startSpan(
                SummaryProfile.NEREIDS_ANALYSIS_TIME, TUnit.TIME_MS, SummaryProfile.PLAN_TIME);
        analysisSpan.setValue(5);

        ProfileSpan rewriteSpan = tracer.startSpan(
                SummaryProfile.NEREIDS_REWRITE_TIME, TUnit.TIME_MS, SummaryProfile.PLAN_TIME);
        rewriteSpan.setValue(6);

        ProfileSpan mvSpan = tracer.startSpan(
                SummaryProfile.NEREIDS_PRE_REWRITE_BY_MV_TIME, TUnit.TIME_MS, SummaryProfile.PLAN_TIME);
        mvSpan.setValue(3);

        ProfileSpan optimizeSpan = tracer.startSpan(
                SummaryProfile.NEREIDS_OPTIMIZE_TIME, TUnit.TIME_MS, SummaryProfile.PLAN_TIME);
        optimizeSpan.setValue(5);

        ProfileSpan translateSpan = tracer.startSpan(
                SummaryProfile.NEREIDS_TRANSLATE_TIME, TUnit.TIME_MS, SummaryProfile.PLAN_TIME);
        translateSpan.setValue(9);

        ProfileSpan distributeSpan = tracer.startSpan(
                SummaryProfile.NEREIDS_DISTRIBUTE_TIME, TUnit.TIME_MS, SummaryProfile.PLAN_TIME);
        distributeSpan.setValue(10);

        // Schedule Time
        ProfileSpan scheduleSpan = tracer.startSpan(SummaryProfile.SCHEDULE_TIME, TUnit.TIME_MS);
        scheduleSpan.setValue(12);

        // Wait and Fetch Result Time
        ProfileSpan fetchSpan = tracer.startSpan(SummaryProfile.WAIT_FETCH_RESULT_TIME, TUnit.TIME_MS);
        fetchSpan.setValue(13);

        // Verify counter values via the execution summary profile
        RuntimeProfile executionSummary = profile.getExecutionSummary();
        Counter parseCounter = executionSummary.getCounterMap().get(SummaryProfile.PARSE_SQL_TIME);
        Assertions.assertNotNull(parseCounter);
        Assertions.assertEquals(3, parseCounter.getValue());

        Counter planCounter = executionSummary.getCounterMap().get(SummaryProfile.PLAN_TIME);
        Assertions.assertNotNull(planCounter);
        Assertions.assertEquals(60, planCounter.getValue());

        Counter lockCounter = executionSummary.getCounterMap().get(SummaryProfile.NEREIDS_LOCK_TABLE_TIME);
        Assertions.assertNotNull(lockCounter);
        Assertions.assertEquals(4, lockCounter.getValue());

        Counter scheduleCounter = executionSummary.getCounterMap().get(SummaryProfile.SCHEDULE_TIME);
        Assertions.assertNotNull(scheduleCounter);
        Assertions.assertEquals(12, scheduleCounter.getValue());
    }

    @Test
    public void testCounterFirstRendering() {
        SummaryProfile profile = new SummaryProfile();
        ProfileTracer tracer = profile.getTracer();

        // Add a counter
        tracer.startSpan(SummaryProfile.PARSE_SQL_TIME, TUnit.TIME_MS).setValue(5);

        // Add an info string
        profile.update(ImmutableMap.of(SummaryProfile.IS_NEREIDS, "Yes"));

        // Render and verify counters appear before info strings
        SafeStringBuilder builder = new SafeStringBuilder();
        profile.getExecutionSummary().prettyPrint(builder, "");
        String output = builder.toString();

        int counterPos = output.indexOf(SummaryProfile.PARSE_SQL_TIME);
        int infoPos = output.indexOf(SummaryProfile.IS_NEREIDS);
        Assertions.assertTrue(counterPos >= 0, "Counter should be present in output");
        Assertions.assertTrue(infoPos >= 0, "Info string should be present in output");
        Assertions.assertTrue(counterPos < infoPos,
                "Counters should appear before info strings in Execution Summary");
    }

    @Test
    public void testGetTimeFromTracer() {
        SummaryProfile profile = new SummaryProfile();
        ProfileTracer tracer = profile.getTracer();

        // Set span values directly
        tracer.startSpan(SummaryProfile.PARSE_SQL_TIME, TUnit.TIME_MS).setValue(42);
        tracer.startSpan(SummaryProfile.PLAN_TIME, TUnit.TIME_MS).setValue(100);
        tracer.startSpan(SummaryProfile.SCHEDULE_TIME, TUnit.TIME_MS).setValue(25);

        // Verify getXxxTimeMs() reads from tracer
        Assertions.assertEquals(42, profile.getParseSqlTimeMs());
        Assertions.assertEquals(100, profile.getPlanTimeMs());
        Assertions.assertEquals(25, profile.getScheduleTimeMs());
    }
}
