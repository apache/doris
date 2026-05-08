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

import org.apache.doris.job.cdc.split.SnapshotSplit;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.util.StreamingJobUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;

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

    @Before
    public void setup() {
        provider = new TestableProvider();
        utilsMock = Mockito.mockStatic(StreamingJobUtils.class);
        utilsMock.when(() -> StreamingJobUtils.upsertChunkList(anyLong(), anyString(), any()))
                .then(invocation -> null);
    }

    @org.junit.After
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

    // ===== initSplitProgress / noMoreSplits =====

    @Test
    public void testInitWithEmptySyncTablesIsAllDone() {
        provider.initSplitProgress(Collections.emptyList());
        Assert.assertTrue(provider.noMoreSplits());
    }

    @Test
    public void testInitWithSyncTablesNotDone() {
        provider.initSplitProgress(Arrays.asList("db.tbl_a"));
        Assert.assertNotNull(provider.committedSplitProgress);
        Assert.assertNotNull(provider.cdcSplitProgress);
        Assert.assertNull(provider.cdcSplitProgress.getCurrentSplittingTable());
        Assert.assertFalse(provider.noMoreSplits());
    }

    // ===== advanceSplits =====

    @Test
    public void testAdvanceFirstCallPicksFirstTableWithNullStart() throws JobException {
        provider.initSplitProgress(Arrays.asList("db.tbl_a", "db.tbl_b"));
        provider.mockBatches.add(Arrays.asList(
                split("db.tbl_a", 0, null, 100L),
                split("db.tbl_a", 1, 100L, 200L)));

        provider.advanceSplits();

        Assert.assertEquals(2, provider.remainingSplits.size());
        Assert.assertEquals("db.tbl_a", provider.cdcSplitProgress.getCurrentSplittingTable());
        Assert.assertArrayEquals(new Object[]{200L}, provider.cdcSplitProgress.getNextSplitStart());
        Assert.assertEquals(Integer.valueOf(2), provider.cdcSplitProgress.getNextSplitId());

        Assert.assertEquals(1, provider.rpcCalls.size());
        RpcCall first = provider.rpcCalls.get(0);
        Assert.assertEquals("db.tbl_a", first.table);
        Assert.assertNull("first call should pass null nextSplitStart (= START_BOUND)", first.startVal);
        Assert.assertNull(first.splitId);
    }

    @Test
    public void testAdvanceContinuesOnSameTableAfterFirstBatch() throws JobException {
        provider.initSplitProgress(Arrays.asList("db.tbl_a"));
        provider.mockBatches.add(Arrays.asList(split("db.tbl_a", 0, null, 100L)));
        provider.mockBatches.add(Arrays.asList(split("db.tbl_a", 1, 100L, 200L)));

        provider.advanceSplits();
        provider.advanceSplits();

        Assert.assertEquals(2, provider.rpcCalls.size());
        RpcCall second = provider.rpcCalls.get(1);
        Assert.assertEquals("db.tbl_a", second.table);
        Assert.assertArrayEquals(new Object[]{100L}, second.startVal);
        Assert.assertEquals(Integer.valueOf(1), second.splitId);
        Assert.assertEquals(2, provider.remainingSplits.size());
    }

    @Test
    public void testAdvanceTableDoneSwitchesToNextTable() throws JobException {
        provider.initSplitProgress(Arrays.asList("db.tbl_a", "db.tbl_b"));
        // tbl_a's last chunk: splitEnd=null
        provider.mockBatches.add(Arrays.asList(split("db.tbl_a", 0, null, null)));
        // 2nd advance picks tbl_b
        provider.mockBatches.add(Arrays.asList(split("db.tbl_b", 0, null, 50L)));

        provider.advanceSplits();
        Assert.assertNull("after tbl_a done, currentSplittingTable should clear",
                provider.cdcSplitProgress.getCurrentSplittingTable());
        Assert.assertFalse("tbl_b still pending", provider.noMoreSplits());

        provider.advanceSplits();

        Assert.assertEquals(2, provider.rpcCalls.size());
        Assert.assertEquals("db.tbl_b", provider.rpcCalls.get(1).table);
        Assert.assertEquals("db.tbl_b", provider.cdcSplitProgress.getCurrentSplittingTable());
        Assert.assertArrayEquals(new Object[]{50L}, provider.cdcSplitProgress.getNextSplitStart());
    }

    @Test
    public void testAllSyncTablesDoneMakesNoMoreSplitsTrue() throws JobException {
        provider.initSplitProgress(Arrays.asList("db.tbl_a"));
        provider.mockBatches.add(Arrays.asList(split("db.tbl_a", 0, null, null)));

        provider.advanceSplits();

        Assert.assertTrue(provider.noMoreSplits());
        Assert.assertEquals(1, provider.remainingSplits.size());
    }

    @Test
    public void testAdvanceSplitsDedupsBySplitId() throws JobException {
        provider.initSplitProgress(Arrays.asList("db.tbl_a"));
        // Pre-existing split with same splitId; simulates a defensive dedup target
        // (e.g. on FE restart after RPC succeeded but state wasn't fully advanced).
        provider.remainingSplits.add(split("db.tbl_a", 0, null, 100L));
        provider.mockBatches.add(Arrays.asList(split("db.tbl_a", 0, null, 100L)));

        provider.advanceSplits();

        Assert.assertEquals("duplicate splitId should be filtered out",
                1, provider.remainingSplits.size());
    }

    @Test
    public void testAdvanceWithEmptyBatchIsNoop() throws JobException {
        provider.initSplitProgress(Arrays.asList("db.tbl_a"));
        // mockBatches empty → rpcFetchSplitsBatch returns empty list
        provider.advanceSplits();

        Assert.assertEquals(0, provider.remainingSplits.size());
        // currentSplittingTable was set then RPC returned empty; we leave it set
        // (next advance retries on same table from null start). Just assert no progress.
        Assert.assertNull(provider.cdcSplitProgress.getNextSplitStart());
        Assert.assertNull(provider.cdcSplitProgress.getNextSplitId());
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
        provider.initSplitProgress(Arrays.asList("db.tbl_a"));
        provider.mockBatches.add(Arrays.asList(
                split("db.tbl_a", 0, null, 100L),
                split("db.tbl_a", 1, 100L, 200L)));
        provider.advanceSplits();

        // Task commits chunk #0; updateOffset will copy splitEnd back from remainingSplits.
        JdbcOffset endOffset = new JdbcOffset(Collections.singletonList(commitSplit("db.tbl_a:0")));
        provider.updateOffset(endOffset);

        Assert.assertEquals(1, provider.finishedSplits.size());
        Assert.assertEquals(1, provider.remainingSplits.size());

        JdbcSourceOffsetProvider.SplitProgress committed = provider.committedSplitProgress;
        Assert.assertEquals("db.tbl_a", committed.getCurrentSplittingTable());
        Assert.assertArrayEquals(new Object[]{100L}, committed.getNextSplitStart());
        Assert.assertEquals(Integer.valueOf(1), committed.getNextSplitId());
    }

    @Test
    public void testUpdateOffsetLastChunkClearsCommittedProgress() throws JobException {
        provider.initSplitProgress(Arrays.asList("db.tbl_a"));
        provider.mockBatches.add(Arrays.asList(split("db.tbl_a", 0, null, null)));
        provider.advanceSplits();

        JdbcOffset endOffset = new JdbcOffset(Collections.singletonList(commitSplit("db.tbl_a:0")));
        provider.updateOffset(endOffset);

        JdbcSourceOffsetProvider.SplitProgress committed = provider.committedSplitProgress;
        Assert.assertNull(committed.getCurrentSplittingTable());
        Assert.assertNull(committed.getNextSplitStart());
        Assert.assertNull(committed.getNextSplitId());
        Assert.assertEquals(1, provider.finishedSplits.size());
        Assert.assertEquals(0, provider.remainingSplits.size());
    }

    @Test
    public void testUpdateOffsetReplayPathSkipsWhenSplitMissing() throws JobException {
        provider.initSplitProgress(Arrays.asList("db.tbl_a"));
        // remainingSplits is empty (simulates editlog replay path).

        JdbcOffset endOffset = new JdbcOffset(Collections.singletonList(commitSplit("db.tbl_a:0")));
        provider.updateOffset(endOffset);

        // committed progress untouched; finishedSplits not added (we have nothing to fill in).
        Assert.assertNull(provider.committedSplitProgress.getCurrentSplittingTable());
        Assert.assertEquals(0, provider.finishedSplits.size());
    }

    // ===== computeCdcRemainingTables (covered indirectly via noMoreSplits) =====

    @Test
    public void testTouchedTablesRemovedFromRemaining() throws JobException {
        provider.initSplitProgress(Arrays.asList("db.tbl_a", "db.tbl_b", "db.tbl_c"));
        provider.mockBatches.add(Arrays.asList(split("db.tbl_a", 0, null, null)));
        provider.advanceSplits();

        // tbl_a is now done (in remainingSplits + currentSplittingTable cleared).
        // 2 more tables remain; noMoreSplits should still be false.
        Assert.assertFalse(provider.noMoreSplits());
        Assert.assertNull(provider.cdcSplitProgress.getCurrentSplittingTable());

        // 2nd advance picks tbl_b
        provider.mockBatches.add(Arrays.asList(split("db.tbl_b", 0, null, null)));
        provider.advanceSplits();
        Assert.assertEquals("db.tbl_b", provider.rpcCalls.get(1).table);

        // 3rd advance picks tbl_c
        provider.mockBatches.add(Arrays.asList(split("db.tbl_c", 0, null, null)));
        provider.advanceSplits();
        Assert.assertEquals("db.tbl_c", provider.rpcCalls.get(2).table);

        Assert.assertTrue(provider.noMoreSplits());
    }
}
