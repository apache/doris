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

import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionType;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Arrays;

public class SummaryProfileTest {

    @Test
    public void testPlanSummary() {
        SummaryProfile profile = new SummaryProfile();
        profile.setQueryBeginTime(1);
        profile.setParseSqlStartTime(3);
        profile.setParseSqlFinishTime(6);
        profile.setNereidsLockTableStartTime(8);
        profile.setNereidsLockTableFinishTime(10);
        profile.setNereidsAnalysisTime(15);
        profile.setNereidsRewriteTime(21);
        profile.setNereidsCollectTablePartitionFinishTime(28);
        profile.setNereidsPreRewriteByMvFinishTime(31);
        profile.setNereidsOptimizeTime(36);
        profile.setNereidsTranslateTime(45);
        profile.setNereidsDistributeTime(55);
        profile.setQueryPlanFinishTime(66);
        profile.setQueryScheduleFinishTime(78);
        profile.setQueryFetchResultFinishTime(91);

        // Record the standalone preload stage before the planner takes internal table locks.
        profile.addNereidsPreloadExternalMetadataTime(2);
        profile.addCollectTablePartitionTime(7);
        // update summary time
        profile.update(ImmutableMap.of());

        RuntimeProfile executionSummary = profile.getExecutionSummary();
        Assertions.assertEquals(executionSummary.getInfoString(SummaryProfile.PARSE_SQL_TIME), "3ms");
        Assertions.assertEquals(executionSummary.getInfoString(SummaryProfile.PLAN_TIME), "60ms");
        Assertions.assertEquals(executionSummary.getInfoString(
                SummaryProfile.NEREIDS_PRELOAD_EXTERNAL_METADATA_TIME), "2ms");
        Assertions.assertEquals(executionSummary.getInfoString(SummaryProfile.NEREIDS_LOCK_TABLE_TIME), "2ms");
        Assertions.assertEquals(executionSummary.getInfoString(SummaryProfile.NEREIDS_ANALYSIS_TIME), "5ms");
        Assertions.assertEquals(executionSummary.getInfoString(SummaryProfile.NEREIDS_REWRITE_TIME), "6ms");

        Assertions.assertEquals(executionSummary.getInfoString(
                SummaryProfile.NEREIDS_PRE_REWRITE_BY_MV_TIME), "3ms");
        Assertions.assertEquals(executionSummary.getInfoString(SummaryProfile.NEREIDS_OPTIMIZE_TIME), "5ms");
        Assertions.assertEquals(executionSummary.getInfoString(SummaryProfile.NEREIDS_TRANSLATE_TIME), "9ms");
        Assertions.assertEquals(executionSummary.getInfoString(SummaryProfile.NEREIDS_DISTRIBUTE_TIME), "10ms");
        Assertions.assertEquals(executionSummary.getInfoString(SummaryProfile.SCHEDULE_TIME), "12ms");
        Assertions.assertEquals(executionSummary.getInfoString(SummaryProfile.WAIT_FETCH_RESULT_TIME), "13ms");
    }

    @Test
    public void testPreloadExternalMetadataTimeCounter() {
        SummaryProfile profile = new SummaryProfile();

        // Verify the dedicated preload counter is accumulated independently from other planner stages.
        profile.addNereidsPreloadExternalMetadataTime(12);
        profile.addNereidsPreloadExternalMetadataTime(8);

        Assertions.assertEquals(20, profile.getNereidsPreloadExternalMetadataTimeMs());
        Assertions.assertEquals("20ms", profile.getPrettyNereidsPreloadExternalMetadataTime());
    }

    @Test
    public void testMetaVersionRateLimitWaitTime() {
        String originalCloudUniqueId = Config.cloud_unique_id;
        Config.cloud_unique_id = "test_cloud";
        try {
            SummaryProfile profile = new SummaryProfile();
            profile.addGetPartitionVersionTime(1_000_000);
            profile.addGetTableVersionTime(2_000_000);
            profile.addGetMetaVersionRateLimitWaitTime(1_000_000);
            profile.addGetMetaVersionRateLimitWaitTime(2_000_000);

            profile.update(ImmutableMap.of());

            RuntimeProfile executionSummary = profile.getExecutionSummary();
            Assertions.assertEquals(3_000_000, profile.getGetMetaVersionRateLimitWaitTime());
            Assertions.assertEquals("3.0ms", executionSummary.getInfoString(
                    SummaryProfile.GET_META_VERSION_RATE_LIMIT_WAIT_TIME));
            String metaTime = profile.getMetaTime();
            Assertions.assertTrue(metaTime.contains("\"get_partition_version_time_ms\":1"));
            Assertions.assertTrue(metaTime.contains("\"get_table_version_time_ms\":2"));
            Assertions.assertTrue(metaTime.contains("\"get_meta_version_rate_limit_wait_time_ms\":3"));
            Assertions.assertFalse(new SummaryProfile().getMetaTime().contains(
                    "get_meta_version_rate_limit_wait_time_ms"));
        } finally {
            Config.cloud_unique_id = originalCloudUniqueId;
        }
    }

    @Test
    public void testExternalTableMetaSummary() {
        SummaryProfile profile = new SummaryProfile();
        profile.addExternalTableGetTableMetaTime(2);
        profile.addExternalTableGetPartitionValuesTime(3);
        profile.addExternalTableGetPartitionsTime(5);
        profile.addExternalTableGetPartitionFilesTime(7);
        profile.addExternalTableGetFileScanTasksTime(11);

        profile.update(ImmutableMap.of());

        RuntimeProfile executionSummary = profile.getExecutionSummary();
        Assertions.assertEquals("28ms", executionSummary.getInfoString(SummaryProfile.EXTERNAL_TABLE_META_TIME));
        Assertions.assertEquals("2ms", executionSummary.getInfoString(
                SummaryProfile.EXTERNAL_TABLE_GET_TABLE_META_TIME));
        Assertions.assertEquals("3ms", executionSummary.getInfoString(
                SummaryProfile.EXTERNAL_TABLE_GET_PARTITION_VALUES_TIME));
        Assertions.assertEquals("5ms", executionSummary.getInfoString(SummaryProfile.GET_PARTITIONS_TIME));
        Assertions.assertEquals("7ms", executionSummary.getInfoString(SummaryProfile.GET_PARTITION_FILES_TIME));
        Assertions.assertEquals("11ms", executionSummary.getInfoString(
                SummaryProfile.EXTERNAL_TABLE_GET_FILE_SCAN_TASKS_TIME));
        Assertions.assertEquals(28, profile.getExternalCatalogMetaTimeMs());
    }

    @Test
    public void testOptimizeTimeFallbackWhenPreMvSkipped() {
        SummaryProfile profile = new SummaryProfile();
        profile.setQueryBeginTime(1);
        profile.setParseSqlStartTime(3);
        profile.setParseSqlFinishTime(6);
        profile.setNereidsLockTableStartTime(8);
        profile.setNereidsLockTableFinishTime(10);
        profile.setNereidsAnalysisTime(15);
        profile.setNereidsRewriteTime(21);
        profile.setNereidsOptimizeTime(36);
        profile.setNereidsTranslateTime(45);
        profile.setNereidsDistributeTime(55);
        profile.setQueryPlanFinishTime(66);

        profile.update(ImmutableMap.of());
        RuntimeProfile executionSummary = profile.getExecutionSummary();

        Assertions.assertEquals("N/A", executionSummary.getInfoString(SummaryProfile.NEREIDS_PRE_REWRITE_BY_MV_TIME));
        Assertions.assertEquals("15ms", executionSummary.getInfoString(SummaryProfile.NEREIDS_OPTIMIZE_TIME));
        Assertions.assertEquals(15, profile.getNereidsOptimizeTimeMs());
    }

    @Test
    public void testPreMvAttemptedButEmpty() {
        SummaryProfile profile = new SummaryProfile();
        profile.setQueryBeginTime(1);
        profile.setParseSqlStartTime(3);
        profile.setParseSqlFinishTime(6);
        profile.setNereidsLockTableStartTime(8);
        profile.setNereidsLockTableFinishTime(10);
        profile.setNereidsAnalysisTime(15);
        profile.setNereidsRewriteTime(21);
        profile.setNereidsCollectTablePartitionFinishTime(28);
        profile.setNereidsPreRewriteByMvFinishTime(58);
        profile.setNereidsOptimizeTime(60);
        profile.setNereidsTranslateTime(65);

        profile.update(ImmutableMap.of());
        RuntimeProfile executionSummary = profile.getExecutionSummary();

        Assertions.assertEquals("30ms", executionSummary.getInfoString(SummaryProfile.NEREIDS_PRE_REWRITE_BY_MV_TIME));
        Assertions.assertEquals("2ms", executionSummary.getInfoString(SummaryProfile.NEREIDS_OPTIMIZE_TIME));
        Assertions.assertEquals(32, profile.getNereidsOptimizeTimeMs());
    }

    @Test
    public void testReplanUsesFinalAttemptPhaseTimes() {
        SummaryProfile profile = new SummaryProfile();
        profile.setQueryBeginTime(1);
        profile.setParseSqlFinishTime(6);
        profile.setNereidsLockTableStartTime(8);
        profile.setNereidsLockTableFinishTime(10);
        profile.setNereidsAnalysisTime(15);
        profile.setQueryPlanFinishTime(66);
        profile.setQueryScheduleFinishTime(78);
        profile.addNereidsPreloadExternalMetadataTime(11);
        profile.addExternalTableGetTableMetaTime(22);
        profile.addGetPartitionVersionTime(33_000_000);
        profile.setTransactionBeginTime(TransactionType.HMS);
        profile.addHmsAddPartitionCnt(7);
        profile.update(ImmutableMap.of());
        Assertions.assertEquals("7", profile.getExecutionSummary().getInfoString(SummaryProfile.HMS_ADD_PARTITION_CNT));
        profile.clearExecutionDetails();
        profile.clearPlanDetails();
        profile.setQueryBeginTime(1000);
        profile.setQueryPlanFinishTime(1010);
        profile.setQueryScheduleFinishTime(1020);
        profile.update(ImmutableMap.of());
        Assertions.assertEquals("10ms", profile.getExecutionSummary().getInfoString(SummaryProfile.PLAN_TIME));
        Assertions.assertEquals("10ms", profile.getExecutionSummary().getInfoString(SummaryProfile.SCHEDULE_TIME));
        Assertions.assertEquals("N/A", profile.getExecutionSummary().getInfoString(SummaryProfile.NEREIDS_ANALYSIS_TIME));
        Assertions.assertEquals(10, profile.getScheduleTimeMs());
        Assertions.assertEquals(0, profile.getNereidsPreloadExternalMetadataTimeMs());
        Assertions.assertEquals(0, profile.getExternalCatalogMetaTimeMs());
        Assertions.assertEquals(0, profile.getGetPartitionVersionTimeMs());
        Assertions.assertEquals("N/A", profile.getExecutionSummary().getInfoString(SummaryProfile.HMS_ADD_PARTITION_CNT));
    }

    @Test
    public void testRedispatchExcludesFailedAttemptAndBackoff() throws Exception {
        SummaryProfile profile = new SummaryProfile();
        profile.setQueryBeginTime(1);
        profile.setParseSqlFinishTime(6);
        profile.setQueryPlanFinishTime(66);
        profile.setQueryScheduleFinishTime(78);
        profile.setQueryFetchResultFinishTime(91);
        profile.updateFragmentCompressedSize(1234);
        profile.updateFragmentRpcCount(7);
        profile.clearExecutionDetails();
        profile.setQueryScheduleStartTime(1000);
        Field assignTime = SummaryProfile.class.getDeclaredField("assignFragmentTime");
        assignTime.setAccessible(true);
        assignTime.setLong(profile, 1010);
        profile.setQueryScheduleFinishTime(1012);
        profile.update(ImmutableMap.of());
        Assertions.assertEquals(1, profile.getQueryBeginTime());
        Assertions.assertEquals("60ms", profile.getExecutionSummary().getInfoString(SummaryProfile.PLAN_TIME));
        Assertions.assertEquals("12ms", profile.getExecutionSummary().getInfoString(SummaryProfile.SCHEDULE_TIME));
        Assertions.assertEquals("N/A", profile.getExecutionSummary().getInfoString(SummaryProfile.WAIT_FETCH_RESULT_TIME));
        Assertions.assertEquals(12, profile.getScheduleTimeMs());
        Assertions.assertEquals(10, profile.getFragmentAssignTimsMs());
        Assertions.assertEquals("10ms", profile.getExecutionSummary().getInfoString(SummaryProfile.ASSIGN_FRAGMENT_TIME));
        Assertions.assertEquals(0, profile.getFragmentCompressedSizeByte());
        Assertions.assertEquals(0, profile.getFragmentRPCCount());
        Assertions.assertEquals("0", profile.getExecutionSummary().getInfoString(SummaryProfile.FRAGMENT_RPC_COUNT));
    }

    @Test
    public void testRetryCleanupAfterSerialization() {
        SummaryProfile profile = new SummaryProfile();
        profile.setQueryBeginTime(1);
        profile.setParseSqlFinishTime(6);
        profile.setQueryPlanFinishTime(66);
        profile.setQueryScheduleFinishTime(78);
        profile.updateFragmentRpcCount(7);
        profile.setTransactionBeginTime(TransactionType.HMS);
        profile.addHmsAddPartitionCnt(9);
        SummaryProfile restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(profile), SummaryProfile.class);
        restored.setRpcPhase1Latency(ImmutableMap.of(new TNetworkAddress("backend", 1234),
                Arrays.asList(1L, 2L, 3L, 4L)));
        restored.update(ImmutableMap.of());
        Assertions.assertNotEquals("{}", restored.getExecutionSummary().getInfoString(
                SummaryProfile.SCHEDULE_TIME_PER_BE));
        restored.clearExecutionDetails();
        Assertions.assertEquals("{}", restored.getExecutionSummary().getInfoString(SummaryProfile.SCHEDULE_TIME_PER_BE));
        Assertions.assertEquals(0, restored.getFragmentRPCCount());
        Assertions.assertEquals("60ms", restored.getExecutionSummary().getInfoString(SummaryProfile.PLAN_TIME));
        Assertions.assertEquals("9", restored.getExecutionSummary().getInfoString(SummaryProfile.HMS_ADD_PARTITION_CNT));
        restored.clearPlanDetails();
        Assertions.assertEquals(1, restored.getQueryBeginTime());
        Assertions.assertEquals("N/A", restored.getExecutionSummary().getInfoString(SummaryProfile.PLAN_TIME));
        Assertions.assertEquals("N/A", restored.getExecutionSummary().getInfoString(SummaryProfile.HMS_ADD_PARTITION_CNT));
        Assertions.assertEquals(7, profile.getFragmentRPCCount());
        SummaryProfile clean = new SummaryProfile();
        clean.clearExecutionDetails();
        clean.clearPlanDetails();
        Assertions.assertEquals(0, clean.getFragmentRPCCount());
    }

    @Test
    public void testFailedAttemptsSurviveCleanupWithoutDoubleCounting() {
        SummaryProfile profile = new SummaryProfile();
        TUniqueId first = new TUniqueId(1, 1);
        profile.recordFailedAttempt(first);
        profile.recordFailedAttempt(new TUniqueId(1, 1));
        profile.clearExecutionDetails();
        profile.clearPlanDetails();
        profile.recordFailedAttempt(new TUniqueId(1, 2));
        profile.update(ImmutableMap.of());
        Assertions.assertEquals("2", profile.getExecutionSummary().getInfoString(SummaryProfile.QUERY_RETRY_TIMES));
    }

    @Test
    public void testFailedAttemptDurationIsRecordedOnce() {
        SummaryProfile profile = new SummaryProfile();
        TUniqueId firstId = new TUniqueId(1, 1);
        profile.setQueryBeginTime(1000);
        try (MockedStatic<TimeUtils> clock = Mockito.mockStatic(TimeUtils.class)) {
            clock.when(TimeUtils::getStartTimeMs).thenReturn(1100L);
            profile.recordFailedAttempt(firstId);
            clock.when(TimeUtils::getStartTimeMs).thenReturn(1500L);
            profile.recordFailedAttempt(firstId);
            profile.clearExecutionDetails();
            profile.clearPlanDetails();
            profile.update(ImmutableMap.of());
            String text = profile.getExecutionSummary().getInfoString("QueryRetryDetails");
            Assertions.assertNotNull(text);
            Assertions.assertNotEquals("null", text);
            JsonObject attempts = JsonParser.parseString(text).getAsJsonObject();
            Assertions.assertEquals(1, attempts.size());
            JsonObject first = attempts.getAsJsonObject(DebugUtil.printId(firstId));
            Assertions.assertEquals(100, first.get("durationMs").getAsLong());
            Assertions.assertEquals("FAILED", first.get("state").getAsString());
        }
    }

    @Test
    public void testAttemptDurationsExcludeWaitAndSurviveStorage() {
        SummaryProfile profile = new SummaryProfile();
        TUniqueId firstId = new TUniqueId(1, 1);
        TUniqueId finalId = new TUniqueId(1, 2);
        try (MockedStatic<TimeUtils> clock = Mockito.mockStatic(TimeUtils.class)) {
            profile.startQueryAttempt(firstId, 1000);
            profile.startQueryAttempt(firstId, 1050);
            clock.when(TimeUtils::getStartTimeMs).thenReturn(1100L);
            profile.recordFailedAttempt(firstId);
            profile.clearExecutionDetails();
            profile.clearPlanDetails();
            profile.startQueryAttempt(finalId, 5000);
            clock.when(TimeUtils::getStartTimeMs).thenReturn(5300L);
            profile.recordQueryAttempt(finalId, false);
            clock.when(TimeUtils::getStartTimeMs).thenReturn(6000L);
            profile.recordQueryAttempt(finalId, false);
            SummaryProfile stored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(profile), SummaryProfile.class);
            stored.update(ImmutableMap.of());
            JsonObject attempts = JsonParser.parseString(stored.getExecutionSummary()
                    .getInfoString(SummaryProfile.QUERY_RETRY_DETAILS)).getAsJsonObject();
            Assertions.assertEquals(2, attempts.size());
            Assertions.assertEquals(100, attempts.getAsJsonObject(DebugUtil.printId(firstId))
                    .get("durationMs").getAsLong());
            JsonObject last = attempts.getAsJsonObject(DebugUtil.printId(finalId));
            Assertions.assertEquals(300, last.get("durationMs").getAsLong());
            Assertions.assertEquals("SUCCEEDED", last.get("state").getAsString());
            Assertions.assertEquals("1", stored.getExecutionSummary().getInfoString(SummaryProfile.QUERY_RETRY_TIMES));
        }
    }

    @Test
    public void testUnstartedAttemptHasNoDuration() {
        SummaryProfile profile = new SummaryProfile();
        profile.recordFailedAttempt(new TUniqueId(1, 1));
        profile.update(ImmutableMap.of());
        Assertions.assertEquals("{}", profile.getExecutionSummary().getInfoString(SummaryProfile.QUERY_RETRY_DETAILS));
    }

    @Test
    public void testLateFailureUpdatesStateWithoutExtendingDuration() {
        SummaryProfile profile = new SummaryProfile();
        TUniqueId id = new TUniqueId(1, 1);
        profile.startQueryAttempt(id, 1000);
        try (MockedStatic<TimeUtils> clock = Mockito.mockStatic(TimeUtils.class)) {
            clock.when(TimeUtils::getStartTimeMs).thenReturn(1100L);
            profile.recordQueryAttempt(id, false);
            clock.when(TimeUtils::getStartTimeMs).thenReturn(1500L);
            profile.recordFailedAttempt(id);
            JsonObject attempt = JsonParser.parseString(profile.getExecutionSummary()
                    .getInfoString(SummaryProfile.QUERY_RETRY_DETAILS)).getAsJsonObject()
                    .getAsJsonObject(DebugUtil.printId(id));
            Assertions.assertEquals(100, attempt.get("durationMs").getAsLong());
            Assertions.assertEquals("FAILED", attempt.get("state").getAsString());
        }
    }

}
