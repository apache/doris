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

package org.apache.doris.job.offset.jdbc;

import org.apache.doris.common.Config;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.cdc.split.BinlogSplit;
import org.apache.doris.job.cdc.split.SnapshotSplit;
import org.apache.doris.job.common.DataSourceType;
import org.apache.doris.job.extensions.insert.streaming.StreamingInsertJob;
import org.apache.doris.job.util.StreamingJobUtils;
import org.apache.doris.persist.gson.GsonUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcTvfSourceOffsetProviderTest {

    @Test
    public void testEnsureInitializedNormalizesMysqlJdbcUrl() throws Exception {
        JdbcTvfSourceOffsetProvider provider = new JdbcTvfSourceOffsetProvider();
        Map<String, String> properties = new HashMap<>();
        properties.put(DataSourceConfigKeys.TYPE, "mysql");
        properties.put(DataSourceConfigKeys.JDBC_URL, "jdbc:mysql://127.0.0.1:3306/test");
        properties.put(DataSourceConfigKeys.TABLE, "source_table");

        provider.ensureInitialized(1L, properties);

        Assertions.assertEquals("jdbc:mysql://127.0.0.1:3306/test?yearIsDateType=false"
                        + "&tinyInt1isBit=false&useUnicode=true&characterEncoding=utf-8",
                provider.getSourceProperties().get(DataSourceConfigKeys.JDBC_URL));
        Assertions.assertEquals("true",
                provider.getSourceProperties().get(DataSourceConfigKeys.SKIP_SNAPSHOT_BACKFILL));
        Assertions.assertEquals("jdbc:mysql://127.0.0.1:3306/test",
                properties.get(DataSourceConfigKeys.JDBC_URL));
    }

    @Test
    public void testCompactPrefixImageAndLegacyReader() throws Exception {
        JdbcTvfSourceOffsetProvider provider = new JdbcTvfSourceOffsetProvider();
        String json = compact(2, "100");
        // The old parser still receives the original list-of-string-maps protocol.
        JdbcOffset legacy = (JdbcOffset) new JdbcSourceOffsetProvider().deserializeOffset(json);
        Assertions.assertEquals("public.orders:2", legacy.getSplits().get(0).getSplitId());
        Assertions.assertEquals("100", ((SnapshotSplit) legacy.getSplits().get(0)).getHighWatermark().get("lsn"));
        provider.updateOffset(provider.deserializeOffset(json));
        String image = provider.getPersistInfo();
        Assertions.assertFalse(image.contains("\"sp\""));

        JdbcSourceOffsetProvider oldImage = GsonUtils.GSON.fromJson(image, JdbcSourceOffsetProvider.class);
        Map<String, List<SnapshotSplit>> oldDefinitions = definitions(4);
        Map<String, Map<String, String>> committed = oldImage.getChunkHighWatermarkMap().values().iterator().next();
        oldImage.recalculateRemainingSplits(Map.of("orders", committed), oldDefinitions);
        Assertions.assertEquals(1, oldImage.getFinishedSplits().size());
        Assertions.assertEquals(3, oldImage.getRemainingSplits().size());

        JdbcTvfSourceOffsetProvider restored = new JdbcTvfSourceOffsetProvider();
        restored.restoreFromPersistInfo(image);
        restored.recalculateRemainingSplits(Map.of("orders", committed), definitions(4));
        Assertions.assertEquals(3, restored.getFinishedSplits().size());
        Assertions.assertEquals("public.orders:3", restored.getRemainingSplits().get(0).getSplitId());
        Assertions.assertEquals(Map.of("lsn", "100"), restored.getFinishedSplits().get(1).getHighWatermark());
        Assertions.assertNull(restored.getCurrentOffset());
        Assertions.assertEquals(image, restored.getPersistInfo());
    }

    @Test
    public void testCommitCompactsLargeSnapshotInBothModesAndFirstBinlogClearsIt() throws Exception {
        String oldCloudId = Config.cloud_unique_id;
        String oldMode = Config.deploy_mode;
        try {
            for (boolean cloud : List.of(false, true)) {
                Config.cloud_unique_id = cloud ? "compact-test" : "";
                Config.deploy_mode = cloud ? "cloud" : "local";
                TestProvider provider = new TestProvider();
                provider.sourceProperties.put(DataSourceConfigKeys.SKIP_SNAPSHOT_BACKFILL, "true");
                List<SnapshotSplit> batch = definitions(10000).get("orders");
                provider.getRemainingSplits().addAll(batch);
                JdbcOffset running = (JdbcOffset) provider.getNextOffset(null,
                        Map.of(DataSourceConfigKeys.SNAPSHOT_PARALLELISM, "10000"));
                provider.result = new ArrayList<>();
                provider.result.add(rows(compact(0, "100")).get(0));
                for (int i = 1; i < batch.size(); i++) {
                    provider.result.add(Map.of("splitId", batch.get(i).getSplitId(), "lsn", "200"));
                }
                Collections.swap(provider.result, 1, provider.result.size() - 1);
                String json = provider.getCommitOffsetJson(running, 1L, List.of());
                Assertions.assertTrue(json.length() < 1000);
                Assertions.assertEquals(10000, provider.getRemainingSplits().size());
                JdbcOffset end = (JdbcOffset) provider.deserializeOffset(json);
                provider.updateOffset(end);
                provider.updateOffset(provider.deserializeOffset(json));
                Assertions.assertTrue(provider.getRemainingSplits().isEmpty());
                Assertions.assertEquals(10000, provider.getFinishedSplits().size());
                Assertions.assertTrue(provider.getPersistInfo().length() < 1000);
                Assertions.assertEquals(Map.of("lsn", "100"),
                        ((SnapshotSplit) running.getSplits().get(0)).getHighWatermark());
                provider.applyEndOffsetToTask(running, end);
                Assertions.assertEquals(Map.of("lsn", "100"), batch.get(0).getHighWatermark());
                Assertions.assertTrue(running.showRange().contains("\"highWatermark\":{\"lsn\":\"100\"}"));
                Assertions.assertTrue(running.showRange().contains("public.orders:9999"));

                BinlogSplit binlog = new BinlogSplit();
                binlog.setFinishedSplits(new ArrayList<>(provider.getFinishedSplits()));
                JdbcOffset binlogOffset = new JdbcOffset(List.of(binlog));
                for (int attempt = 0; attempt < 2; attempt++) {
                    BinlogSplit request = new ObjectMapper().convertValue(
                            provider.buildTaskMeta(binlogOffset), BinlogSplit.class);
                    Assertions.assertEquals(batch.size(), request.getFinishedSplits().size());
                    for (int i = 0; i < batch.size(); i++) {
                        SnapshotSplit sent = request.getFinishedSplits().get(i);
                        Assertions.assertEquals(batch.get(i).getSplitId(), sent.getSplitId());
                        Assertions.assertEquals(Map.of("lsn", "100"), sent.getHighWatermark());
                        Assertions.assertEquals(Map.of("lsn", "100"), batch.get(i).getHighWatermark());
                    }
                }

                provider.updateOffset(provider.deserializeOffset(
                        "[{\"splitId\":\"binlog-split\",\"lsn\":\"200\"}]"));
                Assertions.assertFalse(provider.getCurrentOffset().snapshotSplit());
                Assertions.assertTrue(provider.getFinishedSplits().isEmpty());
                Assertions.assertFalse(provider.getPersistInfo().contains("_compact"));
            }
        } finally {
            Config.cloud_unique_id = oldCloudId;
            Config.deploy_mode = oldMode;
        }
    }

    @Test
    public void testOldJournalsKeepLegacyProgressAfterUpgrade() throws Exception {
        String oldCloudId = Config.cloud_unique_id;
        String oldMode = Config.deploy_mode;
        try {
            for (boolean cloud : List.of(false, true)) {
                Config.cloud_unique_id = cloud ? "compact-test" : "";
                Config.deploy_mode = cloud ? "cloud" : "local";
                TestProvider provider = new TestProvider();
                provider.updateOffset(provider.deserializeOffset(
                        "[{\"splitId\":\"public.orders:0\",\"lsn\":\"100\"}]"));
                provider.updateOffset(provider.deserializeOffset(
                        "[{\"splitId\":\"public.orders:1\",\"lsn\":\"900\"}]"));
                provider.sourceProperties.put(DataSourceConfigKeys.SKIP_SNAPSHOT_BACKFILL, "true");
                JdbcOffset next = new JdbcOffset(List.of(split(2)));
                Assertions.assertFalse(provider.buildTaskMeta(next)
                        .containsKey(DataSourceConfigKeys.COMPACT_SNAPSHOT_OFFSET));
                provider.result = rows("[{\"splitId\":\"public.orders:2\",\"lsn\":\"1000\"}]");
                String json = provider.getCommitOffsetJson(next, 3L, List.of());
                Assertions.assertEquals(cloud ? 3 : 1, rows(json).size());
                Assertions.assertFalse(json.contains("_compact"));
                provider.updateOffset(provider.deserializeOffset(json));
                Assertions.assertEquals(3, provider.chunkHighWatermarkMap.values().iterator().next().size());
                Assertions.assertFalse(provider.buildTaskMeta(new JdbcOffset(List.of(split(3))))
                        .containsKey(DataSourceConfigKeys.COMPACT_SNAPSHOT_OFFSET));
            }
        } finally {
            Config.cloud_unique_id = oldCloudId;
            Config.deploy_mode = oldMode;
        }
    }

    @Test
    public void testOldClientCanAdvanceAlreadyFixedWatermark() throws Exception {
        TestProvider provider = new TestProvider();
        provider.sourceProperties.put(DataSourceConfigKeys.SKIP_SNAPSHOT_BACKFILL, "true");
        provider.updateOffset(provider.deserializeOffset(compact(1, "100")));
        provider.result = rows("[{\"splitId\":\"public.orders:2\",\"lsn\":\"200\"}]");
        String json = provider.getCommitOffsetJson(new JdbcOffset(List.of(split(2))), 2L, List.of());
        Assertions.assertEquals(rows(compact(2, "100")), rows(json));
        provider.updateOffset(provider.deserializeOffset(json));
        JdbcTvfSourceOffsetProvider restored = new JdbcTvfSourceOffsetProvider();
        restored.restoreFromPersistInfo(provider.getPersistInfo());
        Map<String, Map<String, String>> persisted = restored.chunkHighWatermarkMap.values().iterator().next();
        restored.recalculateRemainingSplits(Map.of("orders", persisted), definitions(4));
        Assertions.assertEquals(3, restored.getFinishedSplits().size());
        Assertions.assertEquals("public.orders:3", restored.getRemainingSplits().get(0).getSplitId());
        restored.updateOffset(restored.deserializeOffset(compact(3, "100")));
        Assertions.assertEquals(1, restored.chunkHighWatermarkMap.values().iterator().next().size());
    }

    @Test
    public void testMixedCumulativeArrayDoesNotDependOnEntryOrder() throws Exception {
        JdbcTvfSourceOffsetProvider provider = new JdbcTvfSourceOffsetProvider();
        provider.updateOffset(provider.deserializeOffset(
                "[{\"splitId\":\"public.orders:3\",\"lsn\":\"300\"},"
                + "{\"splitId\":\"public.orders:1\",\"lsn\":\"100\",\"_compact\":\"1\"},"
                + "{\"splitId\":\"public.orders:2\",\"lsn\":\"200\"}]"));
        Map<String, Map<String, String>> persisted = provider.chunkHighWatermarkMap.values().iterator().next();
        Assertions.assertEquals(3, persisted.size());
        provider.recalculateRemainingSplits(Map.of("orders", persisted), definitions(5));
        Assertions.assertEquals(4, provider.getFinishedSplits().size());
    }

    @Test
    public void testRepeatedLegacyResultPreservesCompactPrefix() throws Exception {
        JdbcTvfSourceOffsetProvider provider = new JdbcTvfSourceOffsetProvider();
        provider.updateOffset(provider.deserializeOffset(compact(1, "100")));
        provider.updateOffset(provider.deserializeOffset("[{\"splitId\":\"public.orders:1\",\"lsn\":\"200\"}]"));
        JdbcTvfSourceOffsetProvider restored = new JdbcTvfSourceOffsetProvider();
        restored.restoreFromPersistInfo(provider.getPersistInfo());
        Map<String, Map<String, String>> persisted = restored.chunkHighWatermarkMap.values().iterator().next();
        restored.recalculateRemainingSplits(Map.of("orders", persisted), definitions(3));
        Assertions.assertEquals(2, restored.getFinishedSplits().size());
        Assertions.assertEquals("public.orders:2", restored.getRemainingSplits().get(0).getSplitId());
        Assertions.assertEquals(Map.of("lsn", "100"), restored.getFinishedSplits().get(0).getHighWatermark());
        Assertions.assertNull(restored.getCurrentOffset());
    }

    @Test
    public void testCompactProgressCannotEnableBackfillAndMissingMetaFails() throws Exception {
        JdbcTvfSourceOffsetProvider provider = new JdbcTvfSourceOffsetProvider();
        provider.updateOffset(provider.deserializeOffset(compact(1, "100")));
        Assertions.assertThrows(org.apache.doris.job.exception.JobException.class,
                () -> provider.ensureInitialized(1L, Map.of(DataSourceConfigKeys.TYPE, "mysql",
                        DataSourceConfigKeys.JDBC_URL, "jdbc:mysql://127.0.0.1/test",
                        DataSourceConfigKeys.TABLE, "orders", DataSourceConfigKeys.SKIP_SNAPSHOT_BACKFILL, "false")));
        Assertions.assertThrows(IllegalStateException.class, () -> provider.recalculateRemainingSplits(
                Map.of("orders", provider.chunkHighWatermarkMap.values().iterator().next()),
                Map.of("orders", new ArrayList<>(List.of(split(0), split(2))))));
    }

    @Test
    public void testRecalculateSplitsDoesNotUpdateCommittedProgress() throws Exception {
        for (boolean compact : List.of(false, true)) {
            JdbcTvfSourceOffsetProvider provider = new JdbcTvfSourceOffsetProvider();
            provider.updateOffset(provider.deserializeOffset(
                    "[{\"splitId\":\"public.orders:4\",\"lsn\":\"900\"}]"));
            JdbcOffset current = provider.getCurrentOffset();
            String persisted = provider.getPersistInfo();
            provider.snapshotParallelism = 1;
            Map<String, String> first = compact
                    ? Map.of("lsn", "100", "_compact", "1") : Map.of("lsn", "100");
            Map<String, Map<String, Map<String, String>>> watermarks = Map.of("orders", Map.of(
                    "public.orders:1", first, "public.orders:3", Map.of("lsn", "300")));
            Map<String, List<SnapshotSplit>> definitions = definitions(5);
            List<SnapshotSplit> splits = definitions.get("orders");

            for (int attempt = 0; attempt < 2; attempt++) {
                List<SnapshotSplit> next = provider.recalculateRemainingSplits(watermarks, definitions);
                Assertions.assertEquals(compact ? List.of(splits.get(0), splits.get(1), splits.get(3))
                        : List.of(splits.get(1), splits.get(3)), provider.getFinishedSplits());
                Assertions.assertEquals(compact ? List.of(splits.get(2), splits.get(4))
                        : List.of(splits.get(0), splits.get(2), splits.get(4)), provider.getRemainingSplits());
                Assertions.assertEquals(List.of(splits.get(compact ? 2 : 0)), next);
                Assertions.assertEquals(Map.of("lsn", "100"), splits.get(1).getHighWatermark());
                Assertions.assertEquals(Map.of("lsn", "300"), splits.get(3).getHighWatermark());
                Assertions.assertEquals(5, definitions.get("orders").size());
                Assertions.assertSame(current, provider.getCurrentOffset());
                Assertions.assertEquals(persisted, provider.getPersistInfo());
            }
        }
    }

    @Test
    public void testReplaySnapshotImageRestoresPhase() throws Exception {
        for (String mode : List.of(DataSourceConfigKeys.OFFSET_INITIAL, DataSourceConfigKeys.OFFSET_SNAPSHOT)) {
            for (int end : List.of(1, 3)) {
                JdbcTvfSourceOffsetProvider provider = new JdbcTvfSourceOffsetProvider();
                provider.updateOffset(provider.deserializeOffset(compact(end, "100")));
                JdbcTvfSourceOffsetProvider restored = new JdbcTvfSourceOffsetProvider();
                restored.restoreFromPersistInfo(provider.getPersistInfo());
                restored.sourceProperties.put(DataSourceConfigKeys.OFFSET, mode);
                Map<String, List<SnapshotSplit>> definitions = definitions(4);
                definitions.get("orders").get(3).setSplitEnd(null);
                StreamingInsertJob job = Mockito.mock(StreamingInsertJob.class);
                Mockito.when(job.getSyncTables()).thenReturn(List.of("orders"));
                Mockito.when(job.getJobId()).thenReturn(1L);

                try (MockedStatic<StreamingJobUtils> utils = Mockito.mockStatic(StreamingJobUtils.class)) {
                    utils.when(() -> StreamingJobUtils.restoreSplitsToJob(1L)).thenReturn(definitions);
                    restored.replayIfNeed(job);
                    Assertions.assertTrue(restored.getCurrentOffset().snapshotSplit());
                    Assertions.assertEquals(1, restored.getCurrentOffset().getSplits().size());
                    SnapshotSplit current = (SnapshotSplit) restored.getCurrentOffset().getSplits().get(0);
                    Assertions.assertEquals("public.orders:" + end, current.getSplitId());
                    Assertions.assertEquals("public.orders", current.getTableId());
                    Assertions.assertEquals(List.of("id"), current.getSplitKey());
                    Assertions.assertArrayEquals(definitions.get("orders").get(end).getSplitStart(),
                            current.getSplitStart());
                    Assertions.assertArrayEquals(definitions.get("orders").get(end).getSplitEnd(),
                            current.getSplitEnd());
                    Assertions.assertTrue(restored.getShowCurrentOffset().contains("\"lsn\":\"100\""));
                    Assertions.assertEquals(end + 1, restored.getFinishedSplits().size());
                    Assertions.assertEquals(3 - end, restored.getRemainingSplits().size());
                    boolean complete = end == 3 && DataSourceConfigKeys.OFFSET_SNAPSHOT.equals(mode);
                    Assertions.assertEquals(complete, restored.hasReachedEnd());
                    if (!complete) {
                        JdbcOffset next = (JdbcOffset) restored.getNextOffset(null, Collections.emptyMap());
                        Assertions.assertEquals(end < 3, next.snapshotSplit());
                        if (end < 3) {
                            Assertions.assertEquals("public.orders:2", next.getSplits().get(0).getSplitId());
                        } else {
                            Assertions.assertEquals(4,
                                    ((BinlogSplit) next.getSplits().get(0)).getFinishedSplits().size());
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testReplayLastSnapshotBuildsFirstBinlogWithWatermarks() throws Exception {
        JdbcTvfSourceOffsetProvider provider = new JdbcTvfSourceOffsetProvider();
        provider.updateOffset(provider.deserializeOffset(compact(1, "100")));
        JdbcTvfSourceOffsetProvider restored = new JdbcTvfSourceOffsetProvider();
        restored.restoreFromPersistInfo(provider.getPersistInfo());
        restored.sourceProperties.put(DataSourceConfigKeys.OFFSET, "initial");
        Map<String, List<SnapshotSplit>> definitions = definitions(2);
        definitions.get("orders").get(1).setSplitEnd(null);
        StreamingInsertJob job = Mockito.mock(StreamingInsertJob.class);
        Mockito.when(job.getSyncTables()).thenReturn(List.of("orders"));
        Mockito.when(job.getJobId()).thenReturn(1L);
        try (MockedStatic<StreamingJobUtils> utils = Mockito.mockStatic(StreamingJobUtils.class)) {
            utils.when(() -> StreamingJobUtils.restoreSplitsToJob(1L)).thenReturn(definitions);
            restored.replayIfNeed(job);
            JdbcOffset next = (JdbcOffset) restored.getNextOffset(null, Collections.emptyMap());
            Assertions.assertFalse(next.snapshotSplit());
            BinlogSplit binlog = (BinlogSplit) next.getSplits().get(0);
            Assertions.assertEquals(2, binlog.getFinishedSplits().size());
            for (SnapshotSplit split : binlog.getFinishedSplits()) {
                Assertions.assertEquals(Map.of("lsn", "100"), split.getHighWatermark());
            }
            BinlogSplit request = new ObjectMapper().convertValue(restored.buildTaskMeta(next), BinlogSplit.class);
            Assertions.assertEquals(2, request.getFinishedSplits().size());
            for (int i = 0; i < request.getFinishedSplits().size(); i++) {
                SnapshotSplit sent = request.getFinishedSplits().get(i);
                SnapshotSplit original = binlog.getFinishedSplits().get(i);
                Assertions.assertEquals(original.getSplitId(), sent.getSplitId());
                Assertions.assertArrayEquals(original.getSplitStart(), sent.getSplitStart());
                Assertions.assertArrayEquals(original.getSplitEnd(), sent.getSplitEnd());
                Assertions.assertEquals(Map.of("lsn", "100"), sent.getHighWatermark());
                Assertions.assertEquals(Map.of("lsn", "100"), original.getHighWatermark());
            }
        }
    }

    @Test
    public void testCompactMysqlLagUsesFixedStartUntilBinlogCommit() throws Exception {
        JdbcTvfSourceOffsetProvider provider = new JdbcTvfSourceOffsetProvider();
        provider.sourceType = DataSourceType.MYSQL;
        provider.sourceProperties.put(DataSourceConfigKeys.OFFSET, DataSourceConfigKeys.OFFSET_INITIAL);
        provider.remainingSplits.addAll(definitions(2).get("orders"));
        Assertions.assertNull(provider.getLagReferenceOffset());
        provider.updateOffset(provider.deserializeOffset("[{\"splitId\":\"public.orders:1\","
                + "\"file\":\"mysql-bin.000001\",\"pos\":\"100\",\"_compact\":\"1\"}]"));
        Map<String, String> start = Map.of("file", "mysql-bin.000001", "pos", "100");
        Assertions.assertEquals(start, provider.getLagReferenceOffset());
        Assertions.assertEquals(start, provider.finishedSplits.get(0).getHighWatermark());
        provider.getLagReferenceOffset().put("pos", "999");
        Assertions.assertEquals(start, provider.getLagReferenceOffset());

        provider.currentOffset = new JdbcOffset(List.of(new BinlogSplit()));
        Assertions.assertEquals(start, provider.getLagReferenceOffset());
        provider.updateOffset(provider.deserializeOffset("[{\"splitId\":\"binlog-split\","
                + "\"file\":\"mysql-bin.000001\",\"pos\":\"200\"}]"));
        Assertions.assertEquals("200", provider.getLagReferenceOffset().get("pos"));
    }

    @Test
    public void testCompactLagPreservesPostgresAndSnapshotOnlyBehavior() throws Exception {
        JdbcTvfSourceOffsetProvider provider = new JdbcTvfSourceOffsetProvider();
        provider.sourceType = DataSourceType.POSTGRES;
        provider.sourceProperties.put(DataSourceConfigKeys.OFFSET, DataSourceConfigKeys.OFFSET_INITIAL);
        provider.remainingSplits.addAll(definitions(2).get("orders"));
        provider.updateOffset(provider.deserializeOffset(compact(1, "100")));
        Assertions.assertNull(provider.getLagReferenceOffset());

        provider.sourceType = DataSourceType.MYSQL;
        provider.sourceProperties.put(DataSourceConfigKeys.OFFSET, DataSourceConfigKeys.OFFSET_SNAPSHOT);
        Assertions.assertNull(provider.getLagReferenceOffset());
    }

    @Test
    public void testLegacyBinlogRequestKeepsEachSplitWatermark() throws Exception {
        JdbcTvfSourceOffsetProvider provider = new JdbcTvfSourceOffsetProvider();
        provider.remainingSplits.addAll(definitions(2).get("orders"));
        JdbcOffset running = (JdbcOffset) provider.getNextOffset(null,
                Map.of(DataSourceConfigKeys.SNAPSHOT_PARALLELISM, "2"));
        JdbcOffset end = (JdbcOffset) provider.deserializeOffset("[{\"splitId\":\"public.orders:0\",\"lsn\":\"100\"},"
                + "{\"splitId\":\"public.orders:1\",\"lsn\":\"200\"}]");
        provider.updateOffset(end);
        provider.applyEndOffsetToTask(running, end);
        Assertions.assertEquals(running.showRange(), provider.getShowCurrentOffset());
        Assertions.assertEquals(Map.of("lsn", "100"),
                ((SnapshotSplit) running.getSplits().get(0)).getHighWatermark());
        Assertions.assertEquals(Map.of("lsn", "200"),
                ((SnapshotSplit) running.getSplits().get(1)).getHighWatermark());
        BinlogSplit binlog = new BinlogSplit();
        binlog.setFinishedSplits(new ArrayList<>(provider.getFinishedSplits()));

        Map<String, Object> meta = provider.buildTaskMeta(new JdbcOffset(List.of(binlog)));
        Assertions.assertEquals(new JdbcOffset(List.of(binlog)).generateMeta(), meta);
        BinlogSplit request = new ObjectMapper().convertValue(meta, BinlogSplit.class);
        Assertions.assertEquals(Map.of("lsn", "100"), request.getFinishedSplits().get(0).getHighWatermark());
        Assertions.assertEquals(Map.of("lsn", "200"), request.getFinishedSplits().get(1).getHighWatermark());
        SnapshotSplit finished = request.getFinishedSplits().get(1);
        Assertions.assertEquals("public.orders", finished.getTableId());
        Assertions.assertEquals(List.of("id"), finished.getSplitKey());
        Assertions.assertArrayEquals(new Object[] {10}, finished.getSplitStart());
        Assertions.assertArrayEquals(new Object[] {20}, finished.getSplitEnd());
    }

    @Test
    public void testBackfillJobsKeepLegacyRequestsAndCommitFormat() throws Exception {
        TestProvider provider = new TestProvider();
        Map<String, String> properties = new HashMap<>(Map.of(DataSourceConfigKeys.TYPE, "mysql",
                DataSourceConfigKeys.JDBC_URL, "jdbc:mysql://127.0.0.1/test", DataSourceConfigKeys.TABLE, "orders",
                DataSourceConfigKeys.SKIP_SNAPSHOT_BACKFILL, "false"));
        provider.ensureInitialized(1L, properties);
        JdbcOffset task = new JdbcOffset(List.of(split(0)));
        Assertions.assertFalse(provider.buildTaskMeta(task).containsKey(DataSourceConfigKeys.COMPACT_SNAPSHOT_OFFSET));
        provider.result = rows("[{\"splitId\":\"public.orders:0\",\"lsn\":\"100\"}]");
        String json = provider.getCommitOffsetJson(task, 1L, List.of());
        Assertions.assertFalse(json.contains("_compact"));
        properties.remove(DataSourceConfigKeys.SKIP_SNAPSHOT_BACKFILL);
        provider.ensureInitialized(1L, properties);
        Assertions.assertEquals("1", provider.buildTaskMeta(task).get(DataSourceConfigKeys.COMPACT_SNAPSHOT_OFFSET));
    }

    @Test
    public void testFirstMinimumStaysFixedWhileProgressAdvances() throws Exception {
        TestProvider provider = new TestProvider();
        provider.sourceProperties.put(DataSourceConfigKeys.SKIP_SNAPSHOT_BACKFILL, "true");
        provider.getRemainingSplits().addAll(definitions(3).get("orders"));
        provider.result = new ArrayList<>(rows(compact(1, "100")));
        provider.result.add(Map.of("splitId", "public.orders:0", "lsn", "200"));
        String first = provider.getCommitOffsetJson(new JdbcOffset(List.of(split(0), split(1))), 1L, List.of());
        Assertions.assertEquals(rows(compact(1, "100")), rows(first));
        provider.updateOffset(provider.deserializeOffset(first));
        Assertions.assertEquals(2, provider.getFinishedSplits().size());
        JdbcOffset next = (JdbcOffset) provider.getNextOffset(null, Collections.emptyMap());
        Assertions.assertEquals("public.orders:2", next.getSplits().get(0).getSplitId());
        Assertions.assertFalse(provider.buildTaskMeta(next).containsKey(DataSourceConfigKeys.COMPACT_SNAPSHOT_OFFSET));
        provider.result = List.of(Map.of("splitId", "public.orders:2", "lsn", "150"));
        String second = provider.getCommitOffsetJson(next, 2L, List.of());
        Assertions.assertEquals(rows(compact(2, "100")), rows(second));
        provider.updateOffset(provider.deserializeOffset(second));
        Assertions.assertTrue(provider.getRemainingSplits().isEmpty());
        Assertions.assertEquals(3, provider.getFinishedSplits().size());
        for (SnapshotSplit split : provider.getFinishedSplits()) {
            Assertions.assertEquals(Map.of("lsn", "100"), split.getHighWatermark());
        }
    }

    @Test
    public void testFirstBatchRequiresMinimumWatermarkFromClient() throws Exception {
        TestProvider provider = new TestProvider();
        provider.sourceProperties.put(DataSourceConfigKeys.SKIP_SNAPSHOT_BACKFILL, "true");
        provider.result = rows("[{\"splitId\":\"public.orders:0\",\"lsn\":\"200\"},"
                + "{\"splitId\":\"public.orders:1\",\"lsn\":\"100\"}]");
        IllegalStateException error = Assertions.assertThrows(IllegalStateException.class,
                () -> provider.getCommitOffsetJson(new JdbcOffset(List.of(split(0), split(1))), 1L, List.of()));
        Assertions.assertTrue(error.getMessage().contains("upgrade the CDC client"));
        Assertions.assertTrue(provider.chunkHighWatermarkMap.isEmpty());
    }

    private static class TestProvider extends JdbcTvfSourceOffsetProvider {
        private List<Map<String, String>> result;

        @Override
        protected List<Map<String, String>> fetchTaskEndOffset(long taskId, List<Long> backendIds) {
            return result;
        }
    }

    private static List<Map<String, String>> rows(String json) throws Exception {
        return new ObjectMapper().readValue(json,
                new TypeReference<List<Map<String, String>>>() {});
    }

    private static String compact(int id, String lsn) {
        return "[{\"splitId\":\"public.orders:" + id + "\",\"lsn\":\"" + lsn + "\",\"_compact\":\"1\"}]";
    }

    private static Map<String, List<SnapshotSplit>> definitions(int count) {
        List<SnapshotSplit> splits = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            splits.add(split(i));
        }
        return new HashMap<>(Map.of("orders", splits));
    }

    private static SnapshotSplit split(int id) {
        return new SnapshotSplit("public.orders:" + id, "public.orders", List.of("id"),
                id == 0 ? null : new Object[] {id * 10}, new Object[] {(id + 1) * 10}, null);
    }
}
