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

package org.apache.doris.cloud;

import org.apache.doris.cloud.CloudWarmUpJob.PersistedTableFilterRule;
import org.apache.doris.common.Config;
import org.apache.doris.common.io.Text;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for table-filter extensions in {@link CloudWarmUpJob}:
 * canonicalize(), rebuildOnTablesFilter(), hasTableFilter(), getJobInfo(),
 * getMatchedTablesString(), dynamic table ID tracking, SHOW WARM UP JOB columns.
 */
public class CloudWarmUpJobTableFilterTest {

    private static final int COL_JOB_ID = 0;
    private static final int COL_SRC = 1;
    private static final int COL_DST = 2;
    private static final int COL_STATUS = 3;
    private static final int COL_TYPE = 4;
    private static final int COL_SYNC_MODE = 5;
    private static final int COL_CREATE_TIME = 6;
    private static final int COL_START_TIME = 7;
    private static final int COL_FINISH_BATCH = 8;
    private static final int COL_ALL_BATCH = 9;
    private static final int COL_FINISH_TIME = 10;
    private static final int COL_ERR_MSG = 11;
    private static final int COL_TABLES = 12;
    private static final int COL_TABLE_FILTER = 13;
    private static final int COL_MATCHED_TABLES = 14;
    private static final int COL_SYNC_STATS = 15;
    private static final int TOTAL_COLUMNS = 16;

    private PersistedTableFilterRule rule(String type, String pattern) {
        PersistedTableFilterRule r = new PersistedTableFilterRule();
        r.ruleType = type;
        r.pattern = pattern;
        return r;
    }

    private CloudWarmUpJob.Builder baseBuilder() {
        return new CloudWarmUpJob.Builder()
                .setJobId(1L)
                .setSrcClusterName("write_cg")
                .setDstClusterName("read_cg")
                .setJobType(CloudWarmUpJob.JobType.TABLES)
                .setSyncMode(CloudWarmUpJob.SyncMode.EVENT_DRIVEN);
    }

    private CloudWarmUpJob.Builder clusterBuilder() {
        return new CloudWarmUpJob.Builder()
                .setJobId(1L)
                .setSrcClusterName("write_cg")
                .setDstClusterName("read_cg")
                .setJobType(CloudWarmUpJob.JobType.CLUSTER)
                .setSyncMode(CloudWarmUpJob.SyncMode.EVENT_DRIVEN);
    }

    // ===== canonicalize() =====

    @Test
    public void testCanonicalizeIncludeOnly() {
        List<PersistedTableFilterRule> rules = Arrays.asList(
                rule("INCLUDE", "dw.*"),
                rule("INCLUDE", "ods.*"));
        String expr = CloudWarmUpJob.canonicalize(rules);
        Assertions.assertEquals("{\"include\":[\"dw.*\",\"ods.*\"]}", expr);
    }

    @Test
    public void testCanonicalizeWithExclude() {
        List<PersistedTableFilterRule> rules = Arrays.asList(
                rule("INCLUDE", "ods.*"),
                rule("INCLUDE", "dw.*"),
                rule("EXCLUDE", "dw.tmp_*"));
        String expr = CloudWarmUpJob.canonicalize(rules);
        Assertions.assertEquals("{\"include\":[\"dw.*\",\"ods.*\"],\"exclude\":[\"dw.tmp_*\"]}", expr);
    }

    @Test
    public void testCanonicalizeOrderIndependentAndDedup() {
        // Different order + duplicates → same canonical form (FAQ: order doesn't matter)
        List<PersistedTableFilterRule> rules1 = Arrays.asList(
                rule("INCLUDE", "ods.*"), rule("INCLUDE", "dw.*"), rule("EXCLUDE", "dw.tmp_*"));
        List<PersistedTableFilterRule> rules2 = Arrays.asList(
                rule("EXCLUDE", "dw.tmp_*"), rule("INCLUDE", "dw.*"),
                rule("INCLUDE", "ods.*"), rule("INCLUDE", "ods.*"));
        Assertions.assertEquals(
                CloudWarmUpJob.canonicalize(rules1),
                CloudWarmUpJob.canonicalize(rules2));
    }

    @Test
    public void testBuilderNormalizesPersistedTableFilterRules() {
        CloudWarmUpJob job = baseBuilder()
                .setTableFilterRules(Arrays.asList(
                        rule("EXCLUDE", "dw.tmp_*"),
                        rule("INCLUDE", "ods.*"),
                        rule("INCLUDE", "dw.*"),
                        rule("INCLUDE", "ods.*")))
                .build();

        List<PersistedTableFilterRule> normalizedRules = job.getTableFilterRules();
        Assertions.assertEquals(3, normalizedRules.size());
        Assertions.assertEquals("INCLUDE", normalizedRules.get(0).ruleType);
        Assertions.assertEquals("dw.*", normalizedRules.get(0).pattern);
        Assertions.assertEquals("INCLUDE", normalizedRules.get(1).ruleType);
        Assertions.assertEquals("ods.*", normalizedRules.get(1).pattern);
        Assertions.assertEquals("EXCLUDE", normalizedRules.get(2).ruleType);
        Assertions.assertEquals("dw.tmp_*", normalizedRules.get(2).pattern);
    }

    @Test
    public void testCanonicalizeExcludeKeyAbsentWhenNoExcludes() {
        String expr = CloudWarmUpJob.canonicalize(Arrays.asList(rule("INCLUDE", "ods.*")));
        Assertions.assertFalse(expr.contains("exclude"));
    }

    @Test
    public void testCanonicalizeEmptyRules() {
        String expr = CloudWarmUpJob.canonicalize(new ArrayList<>());
        Assertions.assertEquals("{\"include\":[]}", expr);
    }

    // ===== rebuildOnTablesFilter() =====

    @Test
    public void testRebuildOnTablesFilter() {
        CloudWarmUpJob job = baseBuilder()
                .setTableFilterRules(Arrays.asList(
                        rule("INCLUDE", "ods.*"), rule("EXCLUDE", "ods.tmp_*")))
                .build();
        job.rebuildOnTablesFilter();

        OnTablesFilter filter = job.getOnTablesFilter();
        Assertions.assertNotNull(filter);
        Assertions.assertTrue(filter.shouldWarmUp("ods", "orders"));
        Assertions.assertFalse(filter.shouldWarmUp("ods", "tmp_staging"));
        Assertions.assertFalse(filter.shouldWarmUp("dw", "something"));
    }

    @Test
    public void testRebuildOnTablesFilterAlsoComputesExpr() {
        // tableFilterExpr is transient, so after rebuild it should be recomputed from rules
        CloudWarmUpJob job = baseBuilder()
                .setTableFilterRules(Arrays.asList(
                        rule("INCLUDE", "ods.*"), rule("EXCLUDE", "ods.tmp_*")))
                .build();
        job.rebuildOnTablesFilter();
        List<String> info = job.getJobInfo(null);
        Assertions.assertEquals("{\"include\":[\"ods.*\"],\"exclude\":[\"ods.tmp_*\"]}",
                info.get(COL_TABLE_FILTER));
    }

    @Test
    public void testReadNormalizesPersistedTableFilterRules() throws IOException {
        String json = "{"
                + "\"jobId\":1,"
                + "\"jobState\":\"PENDING\","
                + "\"srcClusterName\":\"write_cg\","
                + "\"cloudClusterName\":\"read_cg\","
                + "\"JobType\":\"TABLES\","
                + "\"syncMode\":\"EVENT_DRIVEN\","
                + "\"tableFilterRules\":["
                + "{\"ruleType\":\"EXCLUDE\",\"pattern\":\"dw.tmp_*\"},"
                + "{\"ruleType\":\"INCLUDE\",\"pattern\":\"ods.*\"},"
                + "{\"ruleType\":\"INCLUDE\",\"pattern\":\"dw.*\"},"
                + "{\"ruleType\":\"INCLUDE\",\"pattern\":\"ods.*\"}"
                + "]"
                + "}";
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        Text.writeString(out, json);
        out.flush();

        CloudWarmUpJob job = CloudWarmUpJob.read(
                new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

        List<PersistedTableFilterRule> normalizedRules = job.getTableFilterRules();
        Assertions.assertEquals(3, normalizedRules.size());
        Assertions.assertEquals("INCLUDE", normalizedRules.get(0).ruleType);
        Assertions.assertEquals("dw.*", normalizedRules.get(0).pattern);
        Assertions.assertEquals("INCLUDE", normalizedRules.get(1).ruleType);
        Assertions.assertEquals("ods.*", normalizedRules.get(1).pattern);
        Assertions.assertEquals("EXCLUDE", normalizedRules.get(2).ruleType);
        Assertions.assertEquals("dw.tmp_*", normalizedRules.get(2).pattern);
        Assertions.assertEquals("{\"include\":[\"dw.*\",\"ods.*\"],\"exclude\":[\"dw.tmp_*\"]}",
                job.getTableFilterExpr());
    }

    @Test
    public void testRebuildOnTablesFilterNoRules() {
        CloudWarmUpJob job = baseBuilder().build();
        job.rebuildOnTablesFilter();
        Assertions.assertNull(job.getOnTablesFilter());
    }

    // ===== hasTableFilter() =====

    @Test
    public void testHasTableFilter() {
        CloudWarmUpJob withFilter = baseBuilder()
                .setTableFilterRules(Arrays.asList(rule("INCLUDE", "ods.*")))
                .build();
        Assertions.assertTrue(withFilter.hasTableFilter());

        CloudWarmUpJob withoutFilter = baseBuilder().build();
        Assertions.assertFalse(withoutFilter.hasTableFilter());
    }

    // ===== tableFilterExpr derived from rules (single source of truth) =====

    @Test
    public void testTableFilterExprDerivedFromRules() {
        // tableFilterExpr should be computed from rules, not set explicitly
        CloudWarmUpJob job = baseBuilder()
                .setTableFilterRules(Arrays.asList(
                        rule("INCLUDE", "dw.*"), rule("INCLUDE", "ods.*")))
                .build();
        List<String> info = job.getJobInfo(null);
        Assertions.assertEquals("{\"include\":[\"dw.*\",\"ods.*\"]}", info.get(COL_TABLE_FILTER));
    }

    @Test
    public void testTableFilterExprEmptyWhenNoRules() {
        CloudWarmUpJob job = baseBuilder().build();
        List<String> info = job.getJobInfo(null);
        Assertions.assertEquals("", info.get(COL_TABLE_FILTER));
    }

    // ===== getJobInfo() — SHOW WARM UP JOB output =====

    @Test
    public void testGetJobInfoTableLevelJob() {
        // Scenario: user creates a table-level event-driven job and runs SHOW WARM UP JOB
        CloudWarmUpJob job = baseBuilder()
                .setTableFilterRules(Arrays.asList(
                        rule("INCLUDE", "ods.*"), rule("EXCLUDE", "ods.tmp_*")))
                .setSyncEvent(CloudWarmUpJob.SyncEvent.LOAD)
                .build();
        // Simulate resolved table IDs with db.table names
        Map<Long, String> idNames = new HashMap<>();
        idNames.put(1001L, "ods.orders");
        idNames.put(1002L, "ods.products");
        idNames.put(1003L, "ods.users");
        job.setCurrentTableIdNames(idNames);

        List<String> info = job.getJobInfo(null);
        Assertions.assertEquals(TOTAL_COLUMNS, info.size());
        Assertions.assertEquals("1", info.get(COL_JOB_ID));
        Assertions.assertEquals("write_cg", info.get(COL_SRC));
        Assertions.assertEquals("read_cg", info.get(COL_DST));
        Assertions.assertEquals("PENDING", info.get(COL_STATUS));
        Assertions.assertEquals("TABLES", info.get(COL_TYPE));
        Assertions.assertTrue(info.get(COL_SYNC_MODE).contains("EVENT_DRIVEN"));
        Assertions.assertEquals("{\"include\":[\"ods.*\"],\"exclude\":[\"ods.tmp_*\"]}",
                info.get(COL_TABLE_FILTER));
        // MatchedTables should show sorted db.table names
        Assertions.assertEquals("ods.orders, ods.products, ods.users", info.get(COL_MATCHED_TABLES));
    }

    @Test
    public void testGetJobInfoMatchedTablesTruncated() {
        CloudWarmUpJob job = baseBuilder()
                .setTableFilterRules(Arrays.asList(rule("INCLUDE", "ods.*")))
                .build();
        Map<Long, String> idNames = new HashMap<>();
        int originalDisplayLimit = Config.cloud_warm_up_matched_tables_display_limit;
        Config.cloud_warm_up_matched_tables_display_limit = 3;
        int totalTables = 5;
        try {
            for (int i = 0; i < totalTables; i++) {
                idNames.put((long) i, String.format("ods.tbl_%03d", i));
            }
            job.setCurrentTableIdNames(idNames);

            String matchedTables = job.getJobInfo(null).get(COL_MATCHED_TABLES);
            Assertions.assertEquals("ods.tbl_000, ods.tbl_001, ods.tbl_002, "
                    + "... (truncated, 3 of 5 shown)", matchedTables);
            Assertions.assertFalse(matchedTables.contains("ods.tbl_003"));
            Assertions.assertEquals(totalTables, job.getCurrentTableIds().size());
        } finally {
            Config.cloud_warm_up_matched_tables_display_limit = originalDisplayLimit;
        }
    }

    @Test
    public void testMatchedTablesLogDisplayTruncated() {
        List<String> logEntries = new ArrayList<>();
        int originalDisplayLimit = Config.cloud_warm_up_matched_tables_display_limit;
        Config.cloud_warm_up_matched_tables_display_limit = 3;
        int totalTables = 5;
        try {
            for (int i = 0; i < totalTables; i++) {
                logEntries.add(String.format("%d:ods.tbl_%03d", i, i));
            }

            String matchedTables = CloudWarmUpJob.formatMatchedTablesForDisplay(logEntries);
            Assertions.assertEquals("0:ods.tbl_000, 1:ods.tbl_001, 2:ods.tbl_002, "
                    + "... (truncated, 3 of 5 shown)", matchedTables);
            Assertions.assertFalse(matchedTables.contains("3:ods.tbl_003"));
        } finally {
            Config.cloud_warm_up_matched_tables_display_limit = originalDisplayLimit;
        }
    }

    @Test
    public void testGetJobInfoClusterLevelJob() {
        // Scenario: cluster-level job without ON TABLES — TableFilter and MatchedTables are empty
        CloudWarmUpJob job = clusterBuilder()
                .setSyncEvent(CloudWarmUpJob.SyncEvent.LOAD)
                .build();
        List<String> info = job.getJobInfo(null);
        Assertions.assertEquals(TOTAL_COLUMNS, info.size());
        Assertions.assertEquals("CLUSTER", info.get(COL_TYPE));
        Assertions.assertEquals("", info.get(COL_TABLE_FILTER));
        Assertions.assertEquals("", info.get(COL_MATCHED_TABLES));
        Assertions.assertEquals("", info.get(COL_TABLES));
    }

    @Test
    public void testGetJobInfoClusterLevelEventDrivenJobShowsSyncStats() {
        CloudWarmUpJob job = clusterBuilder()
                .setSyncEvent(CloudWarmUpJob.SyncEvent.LOAD)
                .build();
        JobWarmUpStats stats = new JobWarmUpStats();
        stats.requestedSegmentNum30m = 6;
        stats.requestedSegmentSize30m = 2048;
        stats.requestedIndexNum30m = 2;
        stats.requestedIndexSize30m = 1024;
        stats.finishSegmentNum30m = 4;
        stats.finishSegmentSize30m = 1024;
        stats.finishIndexNum30m = 1;
        stats.finishIndexSize30m = 512;
        stats.failSegmentNum30m = 1;
        stats.lastTriggerTs = 5000;
        stats.lastFinishTs = 4800;
        stats.progressTriggerTs = 4200;
        stats.computeGap();

        List<String> detailed = job.getJobInfo(stats, true);
        Assertions.assertEquals(TOTAL_COLUMNS, detailed.size());
        Assertions.assertEquals("CLUSTER", detailed.get(COL_TYPE));
        Assertions.assertEquals("", detailed.get(COL_TABLE_FILTER));
        Assertions.assertEquals("", detailed.get(COL_MATCHED_TABLES));

        JsonObject detailStats = JsonParser.parseString(detailed.get(COL_SYNC_STATS)).getAsJsonObject();
        JsonObject segNum = detailStats.getAsJsonObject("seg_num");
        Assertions.assertEquals(6, segNum.get("requested_30m").getAsLong());
        Assertions.assertEquals(4, segNum.get("finish_30m").getAsLong());
        Assertions.assertEquals(2, segNum.get("gap_30m").getAsLong());
        Assertions.assertEquals(800, detailStats.get("trigger_gap_ms").getAsLong());
        String dateTimePattern = "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}";
        Assertions.assertTrue(detailStats.get("last_trigger_ts").getAsString().matches(dateTimePattern));
        Assertions.assertTrue(detailStats.get("last_finish_ts").getAsString().matches(dateTimePattern));
        Assertions.assertTrue(detailStats.get("progress_trigger_ts").getAsString().matches(dateTimePattern));
        Assertions.assertFalse(detailStats.has("window"));

        List<String> summary = job.getJobInfo(stats, false);
        JsonObject summaryStats = JsonParser.parseString(summary.get(COL_SYNC_STATS)).getAsJsonObject();
        Assertions.assertEquals("30m", summaryStats.get("window").getAsString());
        Assertions.assertEquals("3kb", summaryStats.get("src_size").getAsString());
        Assertions.assertEquals("1.5kb", summaryStats.get("dst_size").getAsString());
        Assertions.assertEquals("1.5kb", summaryStats.get("gap_size").getAsString());
        Assertions.assertEquals(800, summaryStats.get("trigger_gap_ms").getAsLong());
        Assertions.assertFalse(summaryStats.has("seg_num"));
    }

    @Test
    public void testGetJobInfoMatchedTablesEmpty() {
        // Scenario: all matched tables have been dropped → MatchedTables becomes empty
        CloudWarmUpJob job = baseBuilder()
                .setTableFilterRules(Arrays.asList(rule("INCLUDE", "ods.*")))
                .build();
        // Initially had tables, now all dropped
        job.setCurrentTableIdNames(new HashMap<>());

        List<String> info = job.getJobInfo(null);
        Assertions.assertEquals("{\"include\":[\"ods.*\"]}", info.get(COL_TABLE_FILTER));
        Assertions.assertEquals("", info.get(COL_MATCHED_TABLES));
    }

    // ===== Dynamic table ID tracking (simulating create/drop/rename) =====

    @Test
    public void testDynamicTableIdTracking() {
        // Scenario: User guide says system re-evaluates every 60s.
        // - Initial: tables 1001, 1002 matched
        // - New table 1003 created → next refresh adds it
        // - Table 1001 dropped → next refresh removes it
        CloudWarmUpJob job = baseBuilder()
                .setTableFilterRules(Arrays.asList(rule("INCLUDE", "ods.*")))
                .build();

        // Phase 1: initial resolution
        Map<Long, String> initial = new HashMap<>();
        initial.put(1001L, "ods.orders");
        initial.put(1002L, "ods.products");
        job.setCurrentTableIdNames(initial);
        Assertions.assertEquals(2, job.getCurrentTableIds().size());
        Assertions.assertTrue(job.getCurrentTableIds().contains(1001L));
        Assertions.assertTrue(job.getCurrentTableIds().contains(1002L));
        // Verify SHOW output shows db.table names
        List<String> info1 = job.getJobInfo(null);
        Assertions.assertEquals("ods.orders, ods.products", info1.get(COL_MATCHED_TABLES));

        // Phase 2: new table created + old table dropped (simulate refresh)
        Map<Long, String> afterRefresh = new HashMap<>();
        afterRefresh.put(1002L, "ods.products");
        afterRefresh.put(1003L, "ods.users");
        job.setCurrentTableIdNames(afterRefresh);
        Assertions.assertEquals(2, job.getCurrentTableIds().size());
        Assertions.assertFalse(job.getCurrentTableIds().contains(1001L));
        Assertions.assertTrue(job.getCurrentTableIds().contains(1003L));
        List<String> info2 = job.getJobInfo(null);
        Assertions.assertEquals("ods.products, ods.users", info2.get(COL_MATCHED_TABLES));

        // Phase 3: all tables dropped → empty set (Job stays RUNNING per user guide)
        job.setCurrentTableIdNames(new HashMap<>());
        Assertions.assertTrue(job.getCurrentTableIds().isEmpty());
        // TableFilter expr is still there (job not cancelled)
        Assertions.assertTrue(job.hasTableFilter());
        List<String> info3 = job.getJobInfo(null);
        Assertions.assertEquals("", info3.get(COL_MATCHED_TABLES));
    }

    // ===== Builder validation =====

    @Test
    public void testBuilderMissingRequiredFieldsThrows() {
        Assertions.assertThrows(IllegalStateException.class, () -> {
            new CloudWarmUpJob.Builder().build();
        });
    }
}
