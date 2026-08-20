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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for warmup progress observation data models:
 * - TableWarmUpWindowedStats: parse BE JSON, merge from multiple BEs
 * - JobWarmUpStats: aggregate requested/finished, compute gap, serialize
 */
public class WarmUpStatsTest {
    private static final DateTimeFormatter DATETIME_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // ==================== TableWarmUpWindowedStats ====================

    @Test
    public void testFromJsonComplete() {
        String json = "{"
                + "\"job_id\": 100,"
                + "\"requested\": {"
                + "  \"seg\": {\"num\": {\"5m\": 10, \"30m\": 50, \"1h\": 200},"
                + "           \"size\": {\"5m\": 1024, \"30m\": 5120, \"1h\": 20480}},"
                + "  \"idx\": {\"num\": {\"5m\": 3, \"30m\": 15, \"1h\": 60},"
                + "           \"size\": {\"5m\": 512, \"30m\": 2560, \"1h\": 10240}}"
                + "},"
                + "\"finish\": {"
                + "  \"seg\": {\"num\": {\"5m\": 8, \"30m\": 45, \"1h\": 190},"
                + "           \"size\": {\"5m\": 800, \"30m\": 4500, \"1h\": 19000}},"
                + "  \"idx\": {\"num\": {\"5m\": 2, \"30m\": 12, \"1h\": 55},"
                + "           \"size\": {\"5m\": 400, \"30m\": 2400, \"1h\": 9500}}"
                + "},"
                + "\"fail\": {"
                + "  \"seg\": {\"num\": {\"5m\": 1, \"30m\": 3, \"1h\": 5},"
                + "           \"size\": {\"5m\": 100, \"30m\": 300, \"1h\": 500}},"
                + "  \"idx\": {\"num\": {\"5m\": 0, \"30m\": 1, \"1h\": 2},"
                + "           \"size\": {\"5m\": 0, \"30m\": 50, \"1h\": 100}}"
                + "},"
                + "\"last_trigger_ts\": 1700000000000,"
                + "\"last_finish_ts\": 1700000001000,"
                + "\"progress_trigger_ts\": 1699999999000"
                + "}";
        JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
        TableWarmUpWindowedStats stats = TableWarmUpWindowedStats.fromJson(obj);

        // requested
        Assertions.assertEquals(10, stats.requestedSegmentNum5m);
        Assertions.assertEquals(50, stats.requestedSegmentNum30m);
        Assertions.assertEquals(200, stats.requestedSegmentNum1h);
        Assertions.assertEquals(1024, stats.requestedSegmentSize5m);
        Assertions.assertEquals(3, stats.requestedIndexNum5m);
        Assertions.assertEquals(512, stats.requestedIndexSize5m);

        // finish
        Assertions.assertEquals(8, stats.finishSegmentNum5m);
        Assertions.assertEquals(45, stats.finishSegmentNum30m);
        Assertions.assertEquals(400, stats.finishIndexSize5m);

        // fail
        Assertions.assertEquals(1, stats.failSegmentNum5m);
        Assertions.assertEquals(0, stats.failIndexNum5m);
        Assertions.assertEquals(100, stats.failSegmentSize5m);

        // timestamps
        Assertions.assertEquals(1700000000000L, stats.lastTriggerTs);
        Assertions.assertEquals(1700000001000L, stats.lastFinishTs);
        Assertions.assertEquals(1699999999000L, stats.progressTriggerTs);
    }

    @Test
    public void testFromJsonMissingSections() {
        // JSON with only requested, no finish or fail
        String json = "{"
                + "\"job_id\": 200,"
                + "\"requested\": {"
                + "  \"seg\": {\"num\": {\"5m\": 5}}"
                + "}"
                + "}";
        JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
        TableWarmUpWindowedStats stats = TableWarmUpWindowedStats.fromJson(obj);

        Assertions.assertEquals(5, stats.requestedSegmentNum5m);
        Assertions.assertEquals(0, stats.requestedSegmentNum30m);
        Assertions.assertEquals(0, stats.finishSegmentNum5m);
        Assertions.assertEquals(0, stats.failSegmentNum5m);
        Assertions.assertEquals(0, stats.lastTriggerTs);
        Assertions.assertEquals(0, stats.lastFinishTs);
        Assertions.assertEquals(0, stats.progressTriggerTs);
    }

    @Test
    public void testFromJsonEmptyObject() {
        String json = "{\"job_id\": 300}";
        JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
        TableWarmUpWindowedStats stats = TableWarmUpWindowedStats.fromJson(obj);

        Assertions.assertEquals(0, stats.requestedSegmentNum5m);
        Assertions.assertEquals(0, stats.finishSegmentNum5m);
        Assertions.assertEquals(0, stats.failSegmentNum5m);
    }

    @Test
    public void testMergeAddsCounts() {
        TableWarmUpWindowedStats a = new TableWarmUpWindowedStats();
        a.requestedSegmentNum5m = 10;
        a.requestedSegmentSize5m = 1000;
        a.finishSegmentNum5m = 8;
        a.failSegmentNum5m = 1;
        a.lastTriggerTs = 100;
        a.lastFinishTs = 200;
        a.progressTriggerTs = 500;

        TableWarmUpWindowedStats b = new TableWarmUpWindowedStats();
        b.requestedSegmentNum5m = 20;
        b.requestedSegmentSize5m = 2000;
        b.finishSegmentNum5m = 15;
        b.failSegmentNum5m = 2;
        b.lastTriggerTs = 150;
        b.lastFinishTs = 180;
        b.progressTriggerTs = 300;

        a.merge(b);

        Assertions.assertEquals(30, a.requestedSegmentNum5m);
        Assertions.assertEquals(3000, a.requestedSegmentSize5m);
        Assertions.assertEquals(23, a.finishSegmentNum5m);
        Assertions.assertEquals(3, a.failSegmentNum5m);
        Assertions.assertEquals(150, a.lastTriggerTs); // max
        Assertions.assertEquals(200, a.lastFinishTs);   // max
        Assertions.assertEquals(300, a.progressTriggerTs); // min positive
    }

    @Test
    public void testMergeProgressTriggerTsIgnoresMissingValues() {
        TableWarmUpWindowedStats a = new TableWarmUpWindowedStats();
        a.progressTriggerTs = 500;

        TableWarmUpWindowedStats missing = new TableWarmUpWindowedStats();
        a.merge(missing);
        Assertions.assertEquals(500, a.progressTriggerTs);

        TableWarmUpWindowedStats b = new TableWarmUpWindowedStats();
        b.progressTriggerTs = 300;
        missing.merge(b);
        Assertions.assertEquals(300, missing.progressTriggerTs);
    }

    // ==================== JobWarmUpStats ====================

    @Test
    public void testMergeRequestedAccumulates() {
        JobWarmUpStats job = new JobWarmUpStats();

        TableWarmUpWindowedStats src1 = new TableWarmUpWindowedStats();
        src1.requestedSegmentNum5m = 10;
        src1.requestedSegmentSize5m = 1000;
        src1.requestedIndexNum5m = 3;
        src1.lastTriggerTs = 100;

        TableWarmUpWindowedStats src2 = new TableWarmUpWindowedStats();
        src2.requestedSegmentNum5m = 20;
        src2.requestedSegmentSize5m = 2000;
        src2.requestedIndexNum5m = 5;
        src2.lastTriggerTs = 200;

        job.mergeRequested(src1);
        job.mergeRequested(src2);

        Assertions.assertEquals(30, job.requestedSegmentNum5m);
        Assertions.assertEquals(3000, job.requestedSegmentSize5m);
        Assertions.assertEquals(8, job.requestedIndexNum5m);
        Assertions.assertEquals(200, job.lastTriggerTs);
    }

    @Test
    public void testMergeFinishedAccumulates() {
        JobWarmUpStats job = new JobWarmUpStats();

        TableWarmUpWindowedStats dst = new TableWarmUpWindowedStats();
        dst.finishSegmentNum5m = 7;
        dst.finishSegmentSize5m = 700;
        dst.failSegmentNum5m = 2;
        dst.failSegmentSize5m = 200;
        dst.lastFinishTs = 300;
        dst.progressTriggerTs = 250;

        job.mergeFinished(dst);

        Assertions.assertEquals(7, job.finishSegmentNum5m);
        Assertions.assertEquals(700, job.finishSegmentSize5m);
        Assertions.assertEquals(2, job.failSegmentNum5m);
        Assertions.assertEquals(200, job.failSegmentSize5m);
        Assertions.assertEquals(300, job.lastFinishTs);
        Assertions.assertEquals(250, job.progressTriggerTs);
    }

    @Test
    public void testComputeGap() {
        JobWarmUpStats job = new JobWarmUpStats();
        job.requestedSegmentNum5m = 100;
        job.requestedSegmentNum30m = 500;
        job.requestedSegmentNum1h = 2000;
        job.requestedSegmentSize5m = 10240;
        job.requestedIndexNum5m = 30;

        job.finishSegmentNum5m = 80;
        job.finishSegmentNum30m = 450;
        job.finishSegmentNum1h = 1900;
        job.finishSegmentSize5m = 8192;
        job.finishIndexNum5m = 25;
        job.lastTriggerTs = 5000;
        job.progressTriggerTs = 3000;

        job.computeGap();

        Assertions.assertEquals(20, job.gapSegmentNum5m);
        Assertions.assertEquals(50, job.gapSegmentNum30m);
        Assertions.assertEquals(100, job.gapSegmentNum1h);
        Assertions.assertEquals(2048, job.gapSegmentSize5m);
        Assertions.assertEquals(5, job.gapIndexNum5m);
        Assertions.assertEquals(2000, job.triggerGapMs);
    }

    @Test
    public void testComputeGapNegative() {
        // Finished can exceed requested in windowed metrics (timing variance)
        JobWarmUpStats job = new JobWarmUpStats();
        job.requestedSegmentNum5m = 10;
        job.finishSegmentNum5m = 15;

        job.computeGap();

        Assertions.assertEquals(-5, job.gapSegmentNum5m);
    }

    @Test
    public void testToJsonStringStructure() {
        JobWarmUpStats job = new JobWarmUpStats();
        job.requestedSegmentNum5m = 100;
        job.finishSegmentNum5m = 80;
        job.failSegmentNum5m = 5;
        job.gapSegmentNum5m = 20;
        job.requestedSegmentSize5m = 1048576; // 1 MB
        job.finishSegmentSize5m = 524288;     // 512 KB
        job.gapSegmentSize5m = 524288;

        String jsonStr = job.toJsonString();
        JsonObject root = JsonParser.parseString(jsonStr).getAsJsonObject();

        // Verify structure
        Assertions.assertTrue(root.has("seg_num"));
        Assertions.assertTrue(root.has("seg_size"));
        Assertions.assertTrue(root.has("idx_num"));
        Assertions.assertTrue(root.has("idx_size"));
        Assertions.assertTrue(root.has("last_trigger_ts"));
        Assertions.assertTrue(root.has("last_finish_ts"));
        Assertions.assertTrue(root.has("progress_trigger_ts"));
        Assertions.assertTrue(root.has("trigger_gap_ms"));
        Assertions.assertFalse(root.has("window"));
        Assertions.assertFalse(root.has("src_size"));
        Assertions.assertFalse(root.has("dst_size"));
        Assertions.assertFalse(root.has("gap_size"));

        // seg_num values
        JsonObject segNum = root.getAsJsonObject("seg_num");
        Assertions.assertEquals(100, segNum.get("requested_5m").getAsLong());
        Assertions.assertEquals(80, segNum.get("finish_5m").getAsLong());
        Assertions.assertEquals(20, segNum.get("gap_5m").getAsLong());
        Assertions.assertEquals(5, segNum.get("fail_5m").getAsLong());

        // seg_size values are human-readable strings (via ByteSizeValue)
        JsonObject segSize = root.getAsJsonObject("seg_size");
        Assertions.assertEquals("1mb", segSize.get("requested_5m").getAsString());
        Assertions.assertEquals("512kb", segSize.get("finish_5m").getAsString());
    }

    @Test
    public void testToJsonStringZeroTimestamps() {
        JobWarmUpStats job = new JobWarmUpStats();
        // All zeros
        String jsonStr = job.toJsonString();
        JsonObject root = JsonParser.parseString(jsonStr).getAsJsonObject();

        // Zero timestamps should be empty strings
        Assertions.assertEquals("", root.get("last_trigger_ts").getAsString());
        Assertions.assertEquals("", root.get("last_finish_ts").getAsString());
        Assertions.assertEquals("", root.get("progress_trigger_ts").getAsString());
        Assertions.assertEquals(0, root.get("trigger_gap_ms").getAsLong());

        // Zero counts
        JsonObject segNum = root.getAsJsonObject("seg_num");
        Assertions.assertEquals(0, segNum.get("requested_5m").getAsLong());
        Assertions.assertEquals(0, segNum.get("gap_5m").getAsLong());
    }

    @Test
    public void testToJsonStringFormatsTimestampsWithDate() {
        long lastTriggerTs = 1700000000000L;
        long lastFinishTs = 1700000001000L;
        long progressTriggerTs = 1699999999000L;

        JobWarmUpStats job = new JobWarmUpStats();
        job.lastTriggerTs = lastTriggerTs;
        job.lastFinishTs = lastFinishTs;
        job.progressTriggerTs = progressTriggerTs;

        JsonObject root = JsonParser.parseString(job.toJsonString()).getAsJsonObject();

        Assertions.assertEquals(formatExpectedDateTime(lastTriggerTs),
                root.get("last_trigger_ts").getAsString());
        Assertions.assertEquals(formatExpectedDateTime(lastFinishTs),
                root.get("last_finish_ts").getAsString());
        Assertions.assertEquals(formatExpectedDateTime(progressTriggerTs),
                root.get("progress_trigger_ts").getAsString());
        Assertions.assertTrue(root.get("last_trigger_ts").getAsString()
                .matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"));
    }

    @Test
    public void testToSummaryJsonStringMergesDataAndIndexSize() {
        JobWarmUpStats job = new JobWarmUpStats();
        job.requestedSegmentSize30m = 1048576; // 1 MB
        job.requestedIndexSize30m = 1048576;   // 1 MB
        job.finishSegmentSize30m = 524288;     // 512 KB
        job.finishIndexSize30m = 524288;       // 512 KB
        job.lastTriggerTs = 5000;
        job.progressTriggerTs = 4500;
        job.computeGap();

        String jsonStr = job.toSummaryJsonString();
        JsonObject root = JsonParser.parseString(jsonStr).getAsJsonObject();

        Assertions.assertEquals("30m", root.get("window").getAsString());
        Assertions.assertEquals("2mb", root.get("src_size").getAsString());
        Assertions.assertEquals("1mb", root.get("dst_size").getAsString());
        Assertions.assertEquals("1mb", root.get("gap_size").getAsString());
        Assertions.assertEquals(500, root.get("trigger_gap_ms").getAsLong());
        Assertions.assertFalse(root.has("seg_num"));
        Assertions.assertFalse(root.has("seg_size"));
        Assertions.assertFalse(root.has("idx_num"));
        Assertions.assertFalse(root.has("idx_size"));
        Assertions.assertFalse(root.has("last_trigger_ts"));
        Assertions.assertFalse(root.has("last_finish_ts"));
        Assertions.assertFalse(root.has("data_size"));
        Assertions.assertFalse(root.has("index_size"));
    }

    @Test
    public void testHumanReadableSizeInJson() {
        JobWarmUpStats job = new JobWarmUpStats();
        job.requestedSegmentSize5m = 500;           // 500 B
        job.finishSegmentSize5m = 1536;              // 1.5 KB
        job.gapSegmentSize5m = 1048576;              // 1.0 MB
        job.failSegmentSize5m = 1073741824L;         // 1.0 GB

        String jsonStr = job.toJsonString();
        JsonObject segSize = JsonParser.parseString(jsonStr).getAsJsonObject()
                .getAsJsonObject("seg_size");

        Assertions.assertEquals("500b", segSize.get("requested_5m").getAsString());
        Assertions.assertEquals("1.5kb", segSize.get("finish_5m").getAsString());
        Assertions.assertEquals("1mb", segSize.get("gap_5m").getAsString());
        Assertions.assertEquals("1gb", segSize.get("fail_5m").getAsString());
    }

    @Test
    public void testEndToEndSourceAndTargetAggregation() {
        // Simulate: 2 source BEs + 1 target BE → aggregate into JobWarmUpStats

        // Source BE1
        String srcJson1 = "{"
                + "\"job_id\": 42,"
                + "\"requested\": {"
                + "  \"seg\": {\"num\": {\"5m\": 50, \"30m\": 200, \"1h\": 800},"
                + "           \"size\": {\"5m\": 5000, \"30m\": 20000, \"1h\": 80000}},"
                + "  \"idx\": {\"num\": {\"5m\": 10, \"30m\": 40, \"1h\": 160},"
                + "           \"size\": {\"5m\": 1000, \"30m\": 4000, \"1h\": 16000}}"
                + "},"
                + "\"last_trigger_ts\": 1000"
                + "}";

        // Source BE2
        String srcJson2 = "{"
                + "\"job_id\": 42,"
                + "\"requested\": {"
                + "  \"seg\": {\"num\": {\"5m\": 30, \"30m\": 120, \"1h\": 500},"
                + "           \"size\": {\"5m\": 3000, \"30m\": 12000, \"1h\": 50000}},"
                + "  \"idx\": {\"num\": {\"5m\": 6, \"30m\": 24, \"1h\": 100},"
                + "           \"size\": {\"5m\": 600, \"30m\": 2400, \"1h\": 10000}}"
                + "},"
                + "\"last_trigger_ts\": 1200"
                + "}";

        // Target BE
        String dstJson = "{"
                + "\"job_id\": 42,"
                + "\"finish\": {"
                + "  \"seg\": {\"num\": {\"5m\": 70, \"30m\": 300, \"1h\": 1250},"
                + "           \"size\": {\"5m\": 7000, \"30m\": 30000, \"1h\": 125000}},"
                + "  \"idx\": {\"num\": {\"5m\": 14, \"30m\": 60, \"1h\": 250},"
                + "           \"size\": {\"5m\": 1400, \"30m\": 6000, \"1h\": 25000}}"
                + "},"
                + "\"fail\": {"
                + "  \"seg\": {\"num\": {\"5m\": 2, \"30m\": 5, \"1h\": 10},"
                + "           \"size\": {\"5m\": 200, \"30m\": 500, \"1h\": 1000}},"
                + "  \"idx\": {\"num\": {\"5m\": 0, \"30m\": 1, \"1h\": 3},"
                + "           \"size\": {\"5m\": 0, \"30m\": 100, \"1h\": 300}}"
                + "},"
                + "\"last_finish_ts\": 1100,"
                + "\"progress_trigger_ts\": 900"
                + "}";

        // Parse and merge source BEs
        TableWarmUpWindowedStats src = TableWarmUpWindowedStats.fromJson(
                JsonParser.parseString(srcJson1).getAsJsonObject());
        src.merge(TableWarmUpWindowedStats.fromJson(
                JsonParser.parseString(srcJson2).getAsJsonObject()));

        // Parse target BE
        TableWarmUpWindowedStats dst = TableWarmUpWindowedStats.fromJson(
                JsonParser.parseString(dstJson).getAsJsonObject());

        // Aggregate
        JobWarmUpStats job = new JobWarmUpStats();
        job.mergeRequested(src);
        job.mergeFinished(dst);
        job.computeGap();

        // Verify aggregated requested (50+30=80, 200+120=320, ...)
        Assertions.assertEquals(80, job.requestedSegmentNum5m);
        Assertions.assertEquals(320, job.requestedSegmentNum30m);
        Assertions.assertEquals(1300, job.requestedSegmentNum1h);
        Assertions.assertEquals(8000, job.requestedSegmentSize5m);
        Assertions.assertEquals(16, job.requestedIndexNum5m);
        Assertions.assertEquals(1200, job.lastTriggerTs); // max(1000, 1200)

        // Verify finished
        Assertions.assertEquals(70, job.finishSegmentNum5m);
        Assertions.assertEquals(300, job.finishSegmentNum30m);
        Assertions.assertEquals(2, job.failSegmentNum5m);
        Assertions.assertEquals(1100, job.lastFinishTs);
        Assertions.assertEquals(900, job.progressTriggerTs);

        // Verify gap
        Assertions.assertEquals(10, job.gapSegmentNum5m);   // 80 - 70
        Assertions.assertEquals(20, job.gapSegmentNum30m);   // 320 - 300
        Assertions.assertEquals(50, job.gapSegmentNum1h);    // 1300 - 1250
        Assertions.assertEquals(1000, job.gapSegmentSize5m); // 8000 - 7000
        Assertions.assertEquals(2, job.gapIndexNum5m);       // 16 - 14
        Assertions.assertEquals(300, job.triggerGapMs);       // 1200 - 900
    }

    @Test
    public void testClusterLevelEventDrivenJobAggregatesStatsByJobId() {
        CloudWarmUpJob job = new CloudWarmUpJob.Builder()
                .setJobId(77L)
                .setSrcClusterName("write_cg")
                .setDstClusterName("read_cg")
                .setJobType(CloudWarmUpJob.JobType.CLUSTER)
                .setSyncMode(CloudWarmUpJob.SyncMode.EVENT_DRIVEN)
                .setSyncEvent(CloudWarmUpJob.SyncEvent.LOAD)
                .build();

        TableWarmUpWindowedStats src = new TableWarmUpWindowedStats();
        src.requestedSegmentNum30m = 6;
        src.requestedSegmentSize30m = 2048;
        src.requestedIndexNum30m = 2;
        src.requestedIndexSize30m = 1024;
        src.lastTriggerTs = 1000;

        TableWarmUpWindowedStats dst = new TableWarmUpWindowedStats();
        dst.finishSegmentNum30m = 4;
        dst.finishSegmentSize30m = 1024;
        dst.finishIndexNum30m = 1;
        dst.finishIndexSize30m = 512;
        dst.failSegmentNum30m = 1;
        dst.failSegmentSize30m = 128;
        dst.lastFinishTs = 1200;

        Map<Long, TableWarmUpWindowedStats> srcStats = new HashMap<>();
        srcStats.put(77L, src);
        Map<Long, TableWarmUpWindowedStats> dstStats = new HashMap<>();
        dstStats.put(77L, dst);
        Map<String, Map<Long, TableWarmUpWindowedStats>> clusterStats = new HashMap<>();
        clusterStats.put("write_cg", srcStats);
        clusterStats.put("read_cg", dstStats);

        JobWarmUpStats stats = new CacheHotspotManager(null).aggregateStatsForJob(job, clusterStats);

        Assertions.assertEquals(6, stats.requestedSegmentNum30m);
        Assertions.assertEquals(4, stats.finishSegmentNum30m);
        Assertions.assertEquals(2, stats.gapSegmentNum30m);
        Assertions.assertEquals(2, stats.requestedIndexNum30m);
        Assertions.assertEquals(1, stats.finishIndexNum30m);
        Assertions.assertEquals(1, stats.gapIndexNum30m);
        Assertions.assertEquals(1536, stats.gapSegmentSize30m + stats.gapIndexSize30m);
        Assertions.assertEquals(1000, stats.lastTriggerTs);
        Assertions.assertEquals(1200, stats.lastFinishTs);
    }

    private static String formatExpectedDateTime(long epochMs) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMs), ZoneId.systemDefault())
                .format(DATETIME_FMT);
    }
}
