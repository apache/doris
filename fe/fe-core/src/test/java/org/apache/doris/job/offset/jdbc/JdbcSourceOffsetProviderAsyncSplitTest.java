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

import org.apache.doris.job.cdc.split.BinlogSplit;
import org.apache.doris.job.cdc.split.SnapshotSplit;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.util.StreamingJobUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

/**
 * Tests the async split state machine in {@link JdbcSourceOffsetProvider}:
 * advanceSplits / noMoreSplits / updateOffset / dedup. RPC and the system-table
 * UPSERT are stubbed so the test focuses purely on in-memory state transitions.
 */
public class JdbcSourceOffsetProviderAsyncSplitTest {

    /** Records each rpcFetchSplitsBatch invocation; used for argument assertions. */
    static final class RpcCall {
        final String table;
        final Object[] startVal;
        final Integer splitId;

        RpcCall(String table, Object[] startVal, Integer splitId) {
            this.table = table;
            this.startVal = startVal;
            this.splitId = splitId;
        }
    }

    /** Provider under test with rpcFetchSplitsBatch stubbed to dequeue prepared batches. */
    static final class TestableProvider extends JdbcSourceOffsetProvider {
        final Deque<List<SnapshotSplit>> mockBatches = new ArrayDeque<>();
        final List<RpcCall> rpcCalls = new ArrayList<>();

        TestableProvider() {
            super();
            // Default to initial mode so initOnCreate() takes the splitting path.
            this.sourceProperties.put(
                    org.apache.doris.job.cdc.DataSourceConfigKeys.OFFSET,
                    org.apache.doris.job.cdc.DataSourceConfigKeys.OFFSET_INITIAL);
        }

        // initOnCreate() now opens the remote reader for every mode; stub it out so the unit test
        // doesn't issue a real backend RPC.
        @Override
        protected void initSourceReader() {
        }

        @Override
        protected List<SnapshotSplit> rpcFetchSplitsBatch(String table, Object[] startVal, Integer splitId) {
            rpcCalls.add(new RpcCall(table, startVal, splitId));
            if (mockBatches.isEmpty()) {
                return Collections.emptyList();
            }
            return mockBatches.poll();
        }

        @Override
        public Long getJobId() {
            return 999L;
        }
    }

    private TestableProvider provider;
    private MockedStatic<StreamingJobUtils> utilsMock;

    @BeforeEach
    public void setup() {
        provider = new TestableProvider();
        utilsMock = Mockito.mockStatic(StreamingJobUtils.class);
        utilsMock.when(() -> StreamingJobUtils.upsertChunkList(
                        ArgumentMatchers.anyLong(),
                        ArgumentMatchers.anyString(),
                        ArgumentMatchers.any()))
                .then(invocation -> null);
    }

    @AfterEach
    public void tearDown() {
        if (utilsMock != null) {
            utilsMock.close();
        }
    }

    /** Helper to build a SnapshotSplit. start/end are wrapped in Object[] only when non-null. */
    private static SnapshotSplit split(String tableId, int chunkId, Long start, Long end) {
        return new SnapshotSplit(
                tableId + ":" + chunkId,
                tableId,
                Collections.singletonList("id"),
                start == null ? null : new Object[]{start},
                end == null ? null : new Object[]{end},
                Collections.singletonMap("file", "binlog.000001"));
    }

    // ===== initOnCreate / noMoreSplits =====

    @Test
    public void testInitWithEmptySyncTablesIsAllDone() throws JobException {
        provider.initOnCreate(Collections.emptyList());
        Assertions.assertTrue(provider.noMoreSplits());
    }

    @Test
    public void testInitWithSyncTablesNotDone() throws JobException {
        provider.initOnCreate(Arrays.asList("db.tbl_a"));
        Assertions.assertNotNull(provider.committedSplitProgress);
        Assertions.assertNotNull(provider.cdcSplitProgress);
        Assertions.assertNull(provider.cdcSplitProgress.getCurrentSplittingTable());
        Assertions.assertFalse(provider.noMoreSplits());
    }

    @Test
    public void testNoMoreSplitsTrueAfterBinlogTransition() throws JobException {
        // Mirrors restart-via-binlogOffsetPersist replay path: currentOffset=BinlogSplit
        // with finishedSplits/remainingSplits empty. Without the BinlogSplit fast-path
        // computeCdcRemainingTables would see every syncTable as untouched and let the
        // scheduler re-cut snapshot chunks after snapshot phase is over.
        provider.initOnCreate(Arrays.asList("db.tbl_a", "db.tbl_b"));
        provider.setCurrentOffset(new JdbcOffset(Collections.singletonList(new BinlogSplit())));
        Assertions.assertTrue(provider.noMoreSplits());
    }

    @Test
    public void testNoMoreSplitsStillFalseDuringSnapshot() throws JobException {
        // Safety regression: currentOffset=snapshotSplit + untouched table -> fast-path
        // must NOT fire (otherwise advanceSplits would never be called).
        provider.initOnCreate(Arrays.asList("db.tbl_a"));
        provider.setCurrentOffset(new JdbcOffset(
                Collections.singletonList(split("db.tbl_a", 0, null, 100L))));
        Assertions.assertFalse(provider.noMoreSplits());
    }

    // ===== advanceSplits =====

    @Test
    public void testAdvanceFirstCallPicksFirstTableWithNullStart() throws JobException {
        provider.initOnCreate(Arrays.asList("db.tbl_a", "db.tbl_b"));
        provider.mockBatches.add(Arrays.asList(
                split("db.tbl_a", 0, null, 100L),
                split("db.tbl_a", 1, 100L, 200L)));

        provider.advanceSplits();

        Assertions.assertEquals(2, provider.remainingSplits.size());
        Assertions.assertEquals("tbl_a", provider.cdcSplitProgress.getCurrentSplittingTable());
        Assertions.assertArrayEquals(new Object[]{200L}, provider.cdcSplitProgress.getNextSplitStart());
        Assertions.assertEquals(Integer.valueOf(2), provider.cdcSplitProgress.getNextSplitId());

        Assertions.assertEquals(1, provider.rpcCalls.size());
        RpcCall first = provider.rpcCalls.get(0);
        Assertions.assertEquals("tbl_a", first.table);
        Assertions.assertNull(first.startVal, "first call should pass null nextSplitStart (= START_BOUND)");
        Assertions.assertNull(first.splitId);
    }

    @Test
    public void testAdvanceContinuesOnSameTableAfterFirstBatch() throws JobException {
        provider.initOnCreate(Arrays.asList("db.tbl_a"));
        provider.mockBatches.add(Arrays.asList(split("db.tbl_a", 0, null, 100L)));
        provider.mockBatches.add(Arrays.asList(split("db.tbl_a", 1, 100L, 200L)));

        provider.advanceSplits();
        provider.advanceSplits();

        Assertions.assertEquals(2, provider.rpcCalls.size());
        RpcCall second = provider.rpcCalls.get(1);
        Assertions.assertEquals("tbl_a", second.table);
        Assertions.assertArrayEquals(new Object[]{100L}, second.startVal);
        Assertions.assertEquals(Integer.valueOf(1), second.splitId);
        Assertions.assertEquals(2, provider.remainingSplits.size());
    }

    @Test
    public void testAdvanceTableDoneSwitchesToNextTable() throws JobException {
        provider.initOnCreate(Arrays.asList("db.tbl_a", "db.tbl_b"));
        // tbl_a's last chunk: splitEnd=null
        provider.mockBatches.add(Arrays.asList(split("db.tbl_a", 0, null, null)));
        // 2nd advance picks tbl_b
        provider.mockBatches.add(Arrays.asList(split("db.tbl_b", 0, null, 50L)));

        provider.advanceSplits();
        Assertions.assertNull(provider.cdcSplitProgress.getCurrentSplittingTable(), "after tbl_a done, currentSplittingTable should clear");
        Assertions.assertFalse(provider.noMoreSplits(), "tbl_b still pending");

        provider.advanceSplits();

        Assertions.assertEquals(2, provider.rpcCalls.size());
        Assertions.assertEquals("tbl_b", provider.rpcCalls.get(1).table);
        Assertions.assertEquals("tbl_b", provider.cdcSplitProgress.getCurrentSplittingTable());
        Assertions.assertArrayEquals(new Object[]{50L}, provider.cdcSplitProgress.getNextSplitStart());
    }

    @Test
    public void testAllSyncTablesDoneMakesNoMoreSplitsTrue() throws JobException {
        provider.initOnCreate(Arrays.asList("db.tbl_a"));
        provider.mockBatches.add(Arrays.asList(split("db.tbl_a", 0, null, null)));

        provider.advanceSplits();

        Assertions.assertTrue(provider.noMoreSplits());
        Assertions.assertEquals(1, provider.remainingSplits.size());
    }

    @Test
    public void testAdvanceSplitsDedupsBySplitId() throws JobException {
        provider.initOnCreate(Arrays.asList("db.tbl_a"));
        // Pre-existing split with same splitId; simulates a defensive dedup target
        // (e.g. on FE restart after RPC succeeded but state wasn't fully advanced).
        provider.remainingSplits.add(split("db.tbl_a", 0, null, 100L));
        provider.mockBatches.add(Arrays.asList(split("db.tbl_a", 0, null, 100L)));

        provider.advanceSplits();

        Assertions.assertEquals(1, provider.remainingSplits.size(), "duplicate splitId should be filtered out");
    }

    @Test
    public void testAdvanceWithEmptyBatchIsNoop() throws JobException {
        provider.initOnCreate(Arrays.asList("db.tbl_a"));
        // mockBatches empty → rpcFetchSplitsBatch returns empty list
        provider.advanceSplits();

        Assertions.assertEquals(0, provider.remainingSplits.size());
        // currentSplittingTable was set then RPC returned empty; we leave it set
        // (next advance retries on same table from null start). Just assert no progress.
        Assertions.assertNull(provider.cdcSplitProgress.getNextSplitStart());
        Assertions.assertNull(provider.cdcSplitProgress.getNextSplitId());
    }

    // ===== updateOffset advances committedSplitProgress =====

    /** Build a commit-shaped SnapshotSplit: only splitId + HW are present (BE strips others). */
    private static SnapshotSplit commitSplit(String splitId) {
        SnapshotSplit s = new SnapshotSplit();
        s.setSplitId(splitId);
        s.setHighWatermark(Collections.singletonMap("file", "binlog.000002"));
        return s;
    }

    @Test
    public void testUpdateOffsetAdvancesCommittedProgressOnMidChunk() throws JobException {
        provider.initOnCreate(Arrays.asList("db.tbl_a"));
        provider.mockBatches.add(Arrays.asList(
                split("db.tbl_a", 0, null, 100L),
                split("db.tbl_a", 1, 100L, 200L)));
        provider.advanceSplits();

        // Task commits chunk #0; updateOffset will copy splitEnd back from remainingSplits.
        JdbcOffset endOffset = new JdbcOffset(Collections.singletonList(commitSplit("db.tbl_a:0")));
        provider.updateOffset(endOffset);

        Assertions.assertEquals(1, provider.finishedSplits.size());
        Assertions.assertEquals(1, provider.remainingSplits.size());

        JdbcSourceOffsetProvider.SplitProgress committed = provider.committedSplitProgress;
        Assertions.assertEquals("tbl_a", committed.getCurrentSplittingTable());
        Assertions.assertArrayEquals(new Object[]{100L}, committed.getNextSplitStart());
        Assertions.assertEquals(Integer.valueOf(1), committed.getNextSplitId());
    }

    @Test
    public void testUpdateOffsetLastChunkClearsCommittedProgress() throws JobException {
        provider.initOnCreate(Arrays.asList("db.tbl_a"));
        provider.mockBatches.add(Arrays.asList(split("db.tbl_a", 0, null, null)));
        provider.advanceSplits();

        JdbcOffset endOffset = new JdbcOffset(Collections.singletonList(commitSplit("db.tbl_a:0")));
        provider.updateOffset(endOffset);

        JdbcSourceOffsetProvider.SplitProgress committed = provider.committedSplitProgress;
        Assertions.assertNull(committed.getCurrentSplittingTable());
        Assertions.assertNull(committed.getNextSplitStart());
        Assertions.assertNull(committed.getNextSplitId());
        Assertions.assertEquals(1, provider.finishedSplits.size());
        Assertions.assertEquals(0, provider.remainingSplits.size());
    }

    @Test
    public void testUpdateOffsetReplayPathSkipsWhenSplitMissing() throws JobException {
        provider.initOnCreate(Arrays.asList("db.tbl_a"));
        // remainingSplits is empty (simulates editlog replay path).

        JdbcOffset endOffset = new JdbcOffset(Collections.singletonList(commitSplit("db.tbl_a:0")));
        provider.updateOffset(endOffset);

        // committed progress untouched; finishedSplits not added (we have nothing to fill in).
        Assertions.assertNull(provider.committedSplitProgress.getCurrentSplittingTable());
        Assertions.assertEquals(0, provider.finishedSplits.size());
    }

    // ===== computeCdcRemainingTables (covered indirectly via noMoreSplits) =====

    @Test
    public void testTouchedTablesRemovedFromRemaining() throws JobException {
        provider.initOnCreate(Arrays.asList("db.tbl_a", "db.tbl_b", "db.tbl_c"));
        provider.mockBatches.add(Arrays.asList(split("db.tbl_a", 0, null, null)));
        provider.advanceSplits();

        // tbl_a is now done (in remainingSplits + currentSplittingTable cleared).
        // 2 more tables remain; noMoreSplits should still be false.
        Assertions.assertFalse(provider.noMoreSplits());
        Assertions.assertNull(provider.cdcSplitProgress.getCurrentSplittingTable());

        // 2nd advance picks tbl_b
        provider.mockBatches.add(Arrays.asList(split("db.tbl_b", 0, null, null)));
        provider.advanceSplits();
        Assertions.assertEquals("tbl_b", provider.rpcCalls.get(1).table);

        // 3rd advance picks tbl_c
        provider.mockBatches.add(Arrays.asList(split("db.tbl_c", 0, null, null)));
        provider.advanceSplits();
        Assertions.assertEquals("tbl_c", provider.rpcCalls.get(2).table);

        Assertions.assertTrue(provider.noMoreSplits());
    }

    // ===== findResumeMidSplit (replay helper) =====

    @Test
    public void testFindResumeMidSplitSingleTableFullyCutReturnsNull() {
        SnapshotSplit s0 = split("db.tbl_a", 0, null, 100L);
        SnapshotSplit s1 = split("db.tbl_a", 1, 100L, null);     // last, splitEnd=null
        SnapshotSplit mid = JdbcSourceOffsetProvider.findResumeMidSplit(
                Collections.singletonList("db.tbl_a"),
                Arrays.asList(s0, s1),
                Collections.emptyList());
        Assertions.assertNull(mid);
    }

    @Test
    public void testFindResumeMidSplitSingleTableCutToMid() {
        SnapshotSplit s0 = split("db.tbl_a", 0, null, 100L);
        SnapshotSplit s1 = split("db.tbl_a", 1, 100L, 200L);     // largest id, splitEnd non-null
        SnapshotSplit mid = JdbcSourceOffsetProvider.findResumeMidSplit(
                Collections.singletonList("db.tbl_a"),
                Arrays.asList(s0, s1),
                Collections.emptyList());
        Assertions.assertNotNull(mid);
        Assertions.assertEquals("db.tbl_a:1", mid.getSplitId());
        Assertions.assertArrayEquals(new Object[]{200L}, mid.getSplitEnd());
    }

    @Test
    public void testFindResumeMidSplitMultiTableOnlyOneMid() {
        // tbl_a fully cut; tbl_b cut to mid; tbl_c untouched
        SnapshotSplit a0 = split("db.tbl_a", 0, null, null);
        SnapshotSplit b0 = split("db.tbl_b", 0, null, 50L);
        SnapshotSplit mid = JdbcSourceOffsetProvider.findResumeMidSplit(
                Arrays.asList("db.tbl_a", "db.tbl_b", "db.tbl_c"),
                Collections.singletonList(a0),
                Collections.singletonList(b0));
        Assertions.assertNotNull(mid);
        Assertions.assertEquals("db.tbl_b:0", mid.getSplitId());
    }

    @Test
    public void testFindResumeMidSplitMaxIdSpreadAcrossLists() {
        // last id is in remainingSplits (id=2), not finishedSplits (id=0,1)
        SnapshotSplit f0 = split("db.tbl_a", 0, null, 100L);
        SnapshotSplit f1 = split("db.tbl_a", 1, 100L, 200L);
        SnapshotSplit r2 = split("db.tbl_a", 2, 200L, 300L);
        SnapshotSplit mid = JdbcSourceOffsetProvider.findResumeMidSplit(
                Collections.singletonList("db.tbl_a"),
                Arrays.asList(f0, f1),
                Collections.singletonList(r2));
        Assertions.assertNotNull(mid);
        Assertions.assertEquals("db.tbl_a:2", mid.getSplitId());
        Assertions.assertArrayEquals(new Object[]{300L}, mid.getSplitEnd());
    }

    @Test
    public void testFindResumeMidSplitEmptyInputs() {
        Assertions.assertNull(JdbcSourceOffsetProvider.findResumeMidSplit(
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));
        Assertions.assertNull(JdbcSourceOffsetProvider.findResumeMidSplit(
                Collections.singletonList("db.tbl_a"),
                Collections.emptyList(), Collections.emptyList()));
    }

    @Test
    public void testFindResumeMidSplitBareSyncTableQualifiedSplitTableId() {
        // Production layout: cachedSyncTables = bare ("tbl_a"); SnapshotSplit.tableId = qualified
        // ("schema.tbl_a"). Map keys must normalize to bare on both sides, otherwise the lookup
        // misses and resume returns null even when there is a mid-cut to continue from.
        SnapshotSplit s0 = split("schema.tbl_a", 0, null, 100L);
        SnapshotSplit s1 = split("schema.tbl_a", 1, 100L, 200L);
        SnapshotSplit mid = JdbcSourceOffsetProvider.findResumeMidSplit(
                Collections.singletonList("tbl_a"),
                Arrays.asList(s0, s1),
                Collections.emptyList());
        Assertions.assertNotNull(mid);
        Assertions.assertEquals("schema.tbl_a:1", mid.getSplitId());
    }

    @Test
    public void testFindResumeMidSplitSyncTablesContainsUntouchedTable() {
        // syncTables lists tbl_a and tbl_b; only tbl_a appears in splits, fully cut.
        // tbl_b is untouched (no splits) -> still returns null (no mid).
        SnapshotSplit a0 = split("db.tbl_a", 0, null, null);
        SnapshotSplit mid = JdbcSourceOffsetProvider.findResumeMidSplit(
                Arrays.asList("db.tbl_a", "db.tbl_b"),
                Collections.singletonList(a0),
                Collections.emptyList());
        Assertions.assertNull(mid);
    }

    // ===== splitIdOf validation =====

    @Test
    public void testSplitIdOfHappyPath() {
        Assertions.assertEquals(0, JdbcSourceOffsetProvider.splitIdOf("db.tbl_a:0"));
        Assertions.assertEquals(42, JdbcSourceOffsetProvider.splitIdOf("db.tbl_a:42"));
        // table with colon in its qualifier: lastIndexOf(':') takes the trailing one.
        Assertions.assertEquals(7, JdbcSourceOffsetProvider.splitIdOf("schema:tbl:7"));
    }

    @Test
    public void testSplitIdOfNoColonThrows() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            JdbcSourceOffsetProvider.splitIdOf("db.tbl_a_0");
        });
    }

    @Test
    public void testSplitIdOfTrailingColonThrows() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            JdbcSourceOffsetProvider.splitIdOf("db.tbl_a:");
        });
    }

    @Test
    public void testSplitIdOfNonNumericSuffixThrows() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            JdbcSourceOffsetProvider.splitIdOf("db.tbl_a:abc");
        });
    }

    @Test
    public void testSplitIdOfNullThrows() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            JdbcSourceOffsetProvider.splitIdOf(null);
        });
    }

    // ===== mode gate =====

    @Test
    public void testNoMoreSplitsLatestModeAlwaysTrue() {
        provider.sourceProperties.put(
                org.apache.doris.job.cdc.DataSourceConfigKeys.OFFSET,
                org.apache.doris.job.cdc.DataSourceConfigKeys.OFFSET_LATEST);
        // Even if cachedSyncTables is populated (e.g. by replayIfNeed), latest mode
        // must report noMoreSplits=true so scheduler skips advanceSplits entirely.
        provider.cachedSyncTables = Arrays.asList("db.tbl_a", "db.tbl_b");
        Assertions.assertTrue(provider.noMoreSplits());
    }

    @Test
    public void testNoMoreSplitsSnapshotModeStillRespectsState() throws JobException {
        provider.sourceProperties.put(
                org.apache.doris.job.cdc.DataSourceConfigKeys.OFFSET,
                org.apache.doris.job.cdc.DataSourceConfigKeys.OFFSET_SNAPSHOT);
        provider.initOnCreate(Arrays.asList("db.tbl_a"));
        Assertions.assertFalse(provider.noMoreSplits(), "snapshot mode with un-split tables must return false");
    }
}
