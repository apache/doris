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
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.job.cdc.split.BinlogSplit;
import org.apache.doris.job.cdc.split.SnapshotSplit;
import org.apache.doris.job.extensions.insert.streaming.StreamingInsertJob;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class JdbcSourceOffsetProviderOffsetTest {

    @Test
    public void testSnapshotOffsetUsesConfiguredPersistInterval() {
        int oldInterval = Config.streaming_job_snapshot_offset_persist_interval_sec;
        try {
            Config.streaming_job_snapshot_offset_persist_interval_sec = 123;
            JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider();
            provider.currentOffset = new JdbcOffset(
                    Collections.singletonList(snapshotSplit("source_table:0")));

            Assert.assertTrue(provider.shouldPersistOffset(0L, 1_000L));
            Assert.assertFalse(provider.shouldPersistOffset(1_000L, 123_999L));
            Assert.assertTrue(provider.shouldPersistOffset(1_000L, 124_000L));
        } finally {
            Config.streaming_job_snapshot_offset_persist_interval_sec = oldInterval;
        }
    }

    @Test
    public void testBinlogOffsetPersistsImmediately() {
        JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider();
        provider.currentOffset = new JdbcOffset(Collections.singletonList(
                new BinlogSplit(Collections.singletonMap("lsn", "100"))));

        Assert.assertTrue(provider.shouldPersistOffset(1_000L, 1_001L));
    }

    @Test
    public void testEndOffsetAdvancesWhenCurrentOffsetIsAhead() {
        assertEndOffsetAdvancesWhenCurrentOffsetIsAhead(new TestJdbcSourceOffsetProvider(-1));
    }

    @Test
    public void testTvfEndOffsetAdvancesWhenCurrentOffsetIsAhead() {
        assertEndOffsetAdvancesWhenCurrentOffsetIsAhead(new TestJdbcTvfSourceOffsetProvider(-1));
    }

    @Test
    public void testEndOffsetRemainsWhenItIsAheadOfCurrentOffset() {
        JdbcSourceOffsetProvider provider = new TestJdbcSourceOffsetProvider(1);
        Map<String, String> currentOffset = Collections.singletonMap("lsn", "100");
        Map<String, String> endOffset = Collections.singletonMap("lsn", "200");
        provider.setEndBinlogOffset(endOffset);
        provider.setHasMoreData(false);
        provider.updateOffset(new JdbcOffset(
                Collections.singletonList(new BinlogSplit(currentOffset))));

        Assert.assertTrue(provider.hasMoreDataToConsume());
        Assert.assertEquals(endOffset, provider.getEndBinlogOffset());
    }

    @Test
    public void testStaleCompareDoesNotOverwriteRefreshedEndOffset() {
        JdbcSourceOffsetProvider provider = new RefreshingEndOffsetProvider();
        provider.setEndBinlogOffset(Collections.singletonMap("lsn", "100"));
        provider.updateOffset(new JdbcOffset(
                Collections.singletonList(new BinlogSplit(Collections.singletonMap("lsn", "200")))));

        Assert.assertTrue(provider.hasMoreDataToConsume());
        Assert.assertTrue(provider.hasMoreData);
        Assert.assertEquals(Collections.singletonMap("lsn", "300"), provider.getEndBinlogOffset());
    }

    @Test
    public void testStaleCompareDoesNotOverwriteAlteredCurrentOffsetState() {
        JdbcSourceOffsetProvider provider = new AlteringCurrentOffsetProvider();
        provider.setEndBinlogOffset(Collections.singletonMap("lsn", "200"));
        provider.updateOffset(new JdbcOffset(
                Collections.singletonList(new BinlogSplit(Collections.singletonMap("lsn", "200")))));

        Assert.assertTrue(provider.hasMoreDataToConsume());
        Assert.assertTrue(provider.hasMoreData);
        Assert.assertEquals(Collections.singletonMap("lsn", "100"),
                ((BinlogSplit) provider.currentOffset.getSplits().get(0)).getStartingOffset());
    }

    @Test
    public void testValidBinlogOffsetClearsSnapshotState() {
        assertValidBinlogOffsetClearsSnapshotState(new TestJdbcSourceOffsetProvider(-1));
    }

    @Test
    public void testTvfValidBinlogOffsetClearsSnapshotState() {
        assertValidBinlogOffsetClearsSnapshotState(new TestJdbcTvfSourceOffsetProvider(-1));
    }

    @Test
    public void testEmptyBinlogOffsetKeepsPreviousState() {
        assertEmptyBinlogOffsetKeepsPreviousState(new TestJdbcSourceOffsetProvider(-1));
    }

    @Test
    public void testTvfEmptyBinlogOffsetKeepsPreviousState() {
        assertEmptyBinlogOffsetKeepsPreviousState(new TestJdbcTvfSourceOffsetProvider(-1));
    }

    @Test
    public void testRepeatedValidBinlogOffsetCleanupIsIdempotent() {
        assertRepeatedValidBinlogOffsetCleanupIsIdempotent(new TestJdbcSourceOffsetProvider(-1));
    }

    @Test
    public void testTvfRepeatedValidBinlogOffsetCleanupIsIdempotent() {
        assertRepeatedValidBinlogOffsetCleanupIsIdempotent(new TestJdbcTvfSourceOffsetProvider(-1));
    }

    @Test
    public void testBinlogOffsetRestoredFromPersistInfo() throws Exception {
        JdbcSourceOffsetProvider source = new TestJdbcSourceOffsetProvider(-1);
        source.updateOffset(new JdbcOffset(Collections.singletonList(
                new BinlogSplit(Collections.singletonMap("lsn", "200")))));
        StreamingInsertJob job = mockJobWithPersistInfo(source.getPersistInfo());
        JdbcSourceOffsetProvider restored = new JdbcSourceOffsetProvider();

        restored.replayIfNeed(job);

        Assert.assertNotNull(restored.currentOffset);
        Assert.assertFalse(restored.currentOffset.snapshotSplit());
        Assert.assertEquals("200", ((BinlogSplit) restored.currentOffset.getSplits().get(0))
                .getStartingOffset().get("lsn"));
        Assert.assertTrue(restored.chunkHighWatermarkMap.isEmpty());
    }

    @Test
    public void testTvfBinlogOffsetRestoredFromPersistInfo() throws Exception {
        JdbcSourceOffsetProvider source = new TestJdbcTvfSourceOffsetProvider(-1);
        source.updateOffset(new JdbcOffset(Collections.singletonList(
                new BinlogSplit(Collections.singletonMap("lsn", "200")))));
        StreamingInsertJob job = mockJobWithPersistInfo(source.getPersistInfo());
        JdbcTvfSourceOffsetProvider restored = new JdbcTvfSourceOffsetProvider();

        restored.restoreFromPersistInfo(source.getPersistInfo());
        restored.replayIfNeed(job);

        Assert.assertNotNull(restored.currentOffset);
        Assert.assertFalse(restored.currentOffset.snapshotSplit());
        Assert.assertEquals("200", ((BinlogSplit) restored.currentOffset.getSplits().get(0))
                .getStartingOffset().get("lsn"));
        Assert.assertTrue(restored.chunkHighWatermarkMap.isEmpty());
    }

    private static void assertEndOffsetAdvancesWhenCurrentOffsetIsAhead(JdbcSourceOffsetProvider provider) {
        Map<String, String> staleEndOffset = Collections.singletonMap("lsn", "100");
        Map<String, String> committedOffset = Collections.singletonMap("lsn", "200");
        provider.setEndBinlogOffset(staleEndOffset);
        provider.setHasMoreData(false);

        provider.updateOffset(new JdbcOffset(
                Collections.singletonList(new BinlogSplit(committedOffset))));

        Assert.assertEquals(staleEndOffset, provider.getEndBinlogOffset());
        Assert.assertFalse(provider.hasMoreDataToConsume());
        Assert.assertEquals(committedOffset, provider.getEndBinlogOffset());
        Assert.assertEquals("{\"lsn\":\"200\"}", provider.getShowMaxOffset());
    }

    private static void assertValidBinlogOffsetClearsSnapshotState(JdbcSourceOffsetProvider provider) {
        seedSnapshotState(provider);
        Map<String, String> binlogOffset = Collections.singletonMap("lsn", "200");

        provider.updateOffset(new JdbcOffset(
                Collections.singletonList(new BinlogSplit(binlogOffset))));

        Assert.assertTrue(provider.chunkHighWatermarkMap.isEmpty());
        Assert.assertTrue(provider.remainingSplits.isEmpty());
        Assert.assertTrue(provider.finishedSplits.isEmpty());
        assertProgressCleared(provider.committedSplitProgress);
        assertProgressCleared(provider.cdcSplitProgress);
        Assert.assertEquals("table-schemas", provider.tableSchemas);
        Map<String, String> expectedPersist = new HashMap<>(binlogOffset);
        expectedPersist.put(JdbcSourceOffsetProvider.SPLIT_ID, BinlogSplit.BINLOG_SPLIT_ID);
        Assert.assertEquals(expectedPersist, provider.binlogOffsetPersist);
        String persistInfo = provider.getPersistInfo();
        Assert.assertFalse(persistInfo.contains("source_table:0"));
        Assert.assertFalse(persistInfo.contains("source_table:1"));
        Assert.assertTrue(persistInfo.contains("table-schemas"));
    }

    private static void assertEmptyBinlogOffsetKeepsPreviousState(JdbcSourceOffsetProvider provider) {
        seedSnapshotState(provider);
        JdbcOffset previousOffset = new JdbcOffset(
                Collections.singletonList(snapshotSplit("source_table:0")));
        provider.currentOffset = previousOffset;
        provider.hasMoreData = false;

        provider.updateOffset(new JdbcOffset(
                Collections.singletonList(new BinlogSplit(Collections.emptyMap()))));

        Assert.assertSame(previousOffset, provider.currentOffset);
        Assert.assertFalse(provider.hasMoreData);
        Assert.assertFalse(provider.chunkHighWatermarkMap.isEmpty());
        Assert.assertFalse(provider.remainingSplits.isEmpty());
        Assert.assertFalse(provider.finishedSplits.isEmpty());
        Assert.assertEquals("source_table", provider.committedSplitProgress.getCurrentSplittingTable());
        Assert.assertEquals("source_table", provider.cdcSplitProgress.getCurrentSplittingTable());
        Assert.assertNull(provider.binlogOffsetPersist);
    }

    private static void assertRepeatedValidBinlogOffsetCleanupIsIdempotent(
            JdbcSourceOffsetProvider provider) {
        seedSnapshotState(provider);
        JdbcOffset binlogOffset = new JdbcOffset(Collections.singletonList(
                new BinlogSplit(Collections.singletonMap("lsn", "200"))));

        provider.updateOffset(binlogOffset);
        String firstPersistInfo = provider.getPersistInfo();
        Map<String, Map<String, Map<String, String>>> clearedHighWatermarkMap =
                provider.chunkHighWatermarkMap;
        provider.updateOffset(binlogOffset);

        Assert.assertEquals(firstPersistInfo, provider.getPersistInfo());
        Assert.assertSame(clearedHighWatermarkMap, provider.chunkHighWatermarkMap);
        Assert.assertTrue(provider.chunkHighWatermarkMap.isEmpty());
        Assert.assertTrue(provider.remainingSplits.isEmpty());
        Assert.assertTrue(provider.finishedSplits.isEmpty());
        assertProgressCleared(provider.committedSplitProgress);
        assertProgressCleared(provider.cdcSplitProgress);
    }

    private static StreamingInsertJob mockJobWithPersistInfo(String persistInfo) {
        StreamingInsertJob job = new ReplayStreamingInsertJob();
        Deencapsulation.setField(job, "jobId", 9001L);
        Deencapsulation.setField(job, "syncTables", Collections.emptyList());
        job.setOffsetProviderPersist(persistInfo);
        return job;
    }

    private static void seedSnapshotState(JdbcSourceOffsetProvider provider) {
        SnapshotSplit remaining = snapshotSplit("source_table:1");
        SnapshotSplit finished = snapshotSplit("source_table:0");
        provider.remainingSplits.add(remaining);
        provider.finishedSplits.add(finished);
        provider.chunkHighWatermarkMap
                .computeIfAbsent("source_db.source_table", key -> new HashMap<>())
                .put(finished.getSplitId(), finished.getHighWatermark());
        provider.committedSplitProgress = splitProgress();
        provider.cdcSplitProgress = splitProgress();
        provider.tableSchemas = "table-schemas";
    }

    private static SnapshotSplit snapshotSplit(String splitId) {
        return new SnapshotSplit(
                splitId,
                "source_db.source_table",
                Collections.singletonList("id"),
                new Object[]{1L},
                new Object[]{2L},
                Collections.singletonMap("lsn", "100"));
    }

    private static JdbcSourceOffsetProvider.SplitProgress splitProgress() {
        JdbcSourceOffsetProvider.SplitProgress progress = new JdbcSourceOffsetProvider.SplitProgress();
        progress.setCurrentSplittingTable("source_table");
        progress.setNextSplitStart(new Object[]{2L});
        progress.setNextSplitId(2);
        return progress;
    }

    private static void assertProgressCleared(JdbcSourceOffsetProvider.SplitProgress progress) {
        Assert.assertNotNull(progress);
        Assert.assertNull(progress.getCurrentSplittingTable());
        Assert.assertNull(progress.getNextSplitStart());
        Assert.assertNull(progress.getNextSplitId());
    }

    private static class TestJdbcSourceOffsetProvider extends JdbcSourceOffsetProvider {
        private final int compareResult;

        TestJdbcSourceOffsetProvider(int compareResult) {
            this.compareResult = compareResult;
        }

        @Override
        protected int compareOffset(Map<String, String> offsetFirst, Map<String, String> offsetSecond) {
            return compareResult;
        }
    }

    private static class ReplayStreamingInsertJob extends StreamingInsertJob {
        ReplayStreamingInsertJob() {
            super();
        }
    }

    private static class TestJdbcTvfSourceOffsetProvider extends JdbcTvfSourceOffsetProvider {
        private final int compareResult;

        TestJdbcTvfSourceOffsetProvider(int compareResult) {
            this.compareResult = compareResult;
        }

        @Override
        protected int compareOffset(Map<String, String> offsetFirst, Map<String, String> offsetSecond) {
            return compareResult;
        }
    }

    private static class RefreshingEndOffsetProvider extends JdbcSourceOffsetProvider {
        @Override
        protected int compareOffset(Map<String, String> offsetFirst, Map<String, String> offsetSecond) {
            synchronized (splitsLock) {
                endBinlogOffset = Collections.singletonMap("lsn", "300");
                hasMoreData = false;
            }
            return -1;
        }
    }

    private static class AlteringCurrentOffsetProvider extends JdbcSourceOffsetProvider {
        @Override
        protected int compareOffset(Map<String, String> offsetFirst, Map<String, String> offsetSecond) {
            updateOffset(new JdbcOffset(
                    Collections.singletonList(new BinlogSplit(Collections.singletonMap("lsn", "100")))));
            return 0;
        }
    }
}
