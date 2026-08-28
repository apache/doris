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
import org.apache.doris.connector.spi.ConnectorMetadataAccessEvent;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

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
    public void testConnectorMetadataAccessProfile() {
        SummaryProfile profile = new SummaryProfile();
        profile.recordConnectorMetadataAccess("hive_catalog", metadataEvent(7, 6, 4, 2, 5, 0, 3, 2, true));
        profile.recordConnectorMetadataAccess("hive_catalog", metadataEvent(5, 3, 2, 3, 6, 1, 4, 1, false));

        profile.update(ImmutableMap.of());

        Assertions.assertEquals("12ms", profile.getExecutionSummary().getInfoString(
                SummaryProfile.GET_PARTITIONS_TIME));
        Assertions.assertEquals(12, profile.getExternalCatalogMetaTimeMs());
        RuntimeProfile group = profile.getExecutionSummary().getChildMap().get(
                SummaryProfile.CONNECTOR_METADATA_ACCESS_PROFILE);
        RuntimeProfile operation = group.getChildMap().get(
                "hive_catalog: hms.get_partitions_by_names [QUERY]");
        Assertions.assertEquals(12, operation.getCounterMap().get("LogicalElapsedTime").getValue());
        Assertions.assertEquals(9, operation.getCounterMap().get("RpcElapsedTime").getValue());
        Assertions.assertEquals(4, operation.getCounterMap().get("MaxRpcElapsedTime").getValue());
        Assertions.assertEquals(2, operation.getCounterMap().get("LogicalRequests").getValue());
        Assertions.assertEquals(1, operation.getCounterMap().get("FailedRequests").getValue());
        Assertions.assertEquals(11, operation.getCounterMap().get("RequestedItems").getValue());
        Assertions.assertEquals(5, operation.getCounterMap().get("RpcAttempts").getValue());
        Assertions.assertEquals(11, operation.getCounterMap().get("RpcItems").getValue());
        Assertions.assertEquals(1, operation.getCounterMap().get("Fallbacks").getValue());
        Assertions.assertEquals(4, operation.getCounterMap().get("LargestBatchSize").getValue());
        Assertions.assertEquals(1, operation.getCounterMap().get("SmallestBatchSize").getValue());
    }

    @Test
    public void testConnectorMetadataWaitDoesNotInflateLegacyTotal() {
        SummaryProfile profile = new SummaryProfile();
        profile.recordConnectorMetadataAccess(
                "hive_catalog", metadataEvent(100, 20, 20, 1, 5, 0, 5, 5, true));
        profile.recordConnectorMetadataAccess("hive_catalog", ConnectorMetadataAccessEvent.builder()
                .operation("hms.partition_inflight_wait")
                .source("QUERY")
                .requestedItems(5)
                .logicalElapsedMillis(40)
                .success(true)
                .build());

        profile.update(ImmutableMap.of());

        Assertions.assertEquals("100ms", profile.getExecutionSummary().getInfoString(
                SummaryProfile.GET_PARTITIONS_TIME));
        Assertions.assertEquals(100, profile.getExternalCatalogMetaTimeMs());
        RuntimeProfile group = profile.getExecutionSummary().getChildMap().get(
                SummaryProfile.CONNECTOR_METADATA_ACCESS_PROFILE);
        RuntimeProfile waitOperation = group.getChildMap().get(
                "hive_catalog: hms.partition_inflight_wait [QUERY]");
        Assertions.assertEquals(40,
                waitOperation.getCounterMap().get("LogicalElapsedTime").getValue());
    }

    @Test
    public void testConnectorMetadataAccessProfileSerialization() throws Exception {
        SummaryProfile profile = new SummaryProfile();
        profile.recordConnectorMetadataAccess(
                "hive_catalog", metadataEvent(7, 5, 3, 2, 5, 0, 3, 2, true));

        ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
        profile.write(new DataOutputStream(outputBytes));
        SummaryProfile restored = SummaryProfile.read(
                new DataInputStream(new ByteArrayInputStream(outputBytes.toByteArray())));

        RuntimeProfile group = restored.getExecutionSummary().getChildMap().get(
                SummaryProfile.CONNECTOR_METADATA_ACCESS_PROFILE);
        RuntimeProfile operation = group.getChildMap().get(
                "hive_catalog: hms.get_partitions_by_names [QUERY]");
        Assertions.assertEquals(7, operation.getCounterMap().get("LogicalElapsedTime").getValue());
        Assertions.assertEquals(5, operation.getCounterMap().get("RpcElapsedTime").getValue());
        Assertions.assertEquals(3, operation.getCounterMap().get("MaxRpcElapsedTime").getValue());
    }

    private static ConnectorMetadataAccessEvent metadataEvent(long logicalElapsedMillis,
            long rpcElapsedMillis, long maxRpcElapsedMillis, int rpcCount, int requestedItems,
            int fallbackCount, int largestBatchSize, int smallestBatchSize, boolean success) {
        return ConnectorMetadataAccessEvent.builder()
                .operation("hms.get_partitions_by_names")
                .source("QUERY")
                .logicalElapsedMillis(logicalElapsedMillis)
                .rpcElapsedMillis(rpcElapsedMillis)
                .maxRpcElapsedMillis(maxRpcElapsedMillis)
                .rpcCount(rpcCount)
                .rpcItems(requestedItems)
                .requestedItems(requestedItems)
                .fallbackCount(fallbackCount)
                .largestBatchSize(largestBatchSize)
                .smallestBatchSize(smallestBatchSize)
                .success(success)
                .build();
    }
}
