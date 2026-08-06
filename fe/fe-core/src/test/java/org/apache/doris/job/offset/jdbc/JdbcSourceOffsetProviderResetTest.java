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

import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.cdc.split.BinlogSplit;
import org.apache.doris.job.cdc.split.SnapshotSplit;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link JdbcSourceOffsetProvider#resetToInitialState()}.
 */
public class JdbcSourceOffsetProviderResetTest {

    @Test
    public void testResetClearsAllStateAfterBinlogPhase() {
        JdbcSourceOffsetProvider provider = createProviderInBinlogPhase();

        provider.resetToInitialState();

        // All split/binlog state must be cleared
        Assert.assertNull(provider.currentOffset);
        Assert.assertNull(provider.binlogOffsetPersist);
        Assert.assertNull(provider.endBinlogOffset);
        Assert.assertNull(provider.tableSchemas);
        Assert.assertTrue(provider.chunkHighWatermarkMap.isEmpty());
        Assert.assertTrue(provider.remainingSplits.isEmpty());
        Assert.assertTrue(provider.finishedSplits.isEmpty());
        Assert.assertTrue(provider.hasMoreData);
        Assert.assertEquals(0, provider.boundBackendId);
        assertProgressCleared(provider.committedSplitProgress);
        assertProgressCleared(provider.cdcSplitProgress);
    }

    @Test
    public void testResetPreservesConfiguration() {
        JdbcSourceOffsetProvider provider = createProviderInBinlogPhase();
        provider.setJobId(42L);
        provider.setCloudCluster("test-cluster");

        provider.resetToInitialState();

        Assert.assertEquals(Long.valueOf(42L), provider.getJobId());
        Assert.assertEquals("test-cluster", provider.getCloudCluster());
        Assert.assertNotNull(provider.getSourceProperties());
    }

    @Test
    public void testResetPreservesCachedSyncTables() {
        JdbcSourceOffsetProvider provider = createProviderInBinlogPhase();
        List<String> syncTables = new ArrayList<>();
        syncTables.add("orders");
        syncTables.add("users");
        provider.cachedSyncTables = syncTables;

        provider.resetToInitialState();

        // cachedSyncTables preserved — replayIfNeed re-sets it from the job
        Assert.assertSame(syncTables, provider.cachedSyncTables);
    }

    @Test
    public void testNoMoreSplitsReturnsFalseAfterReset() {
        JdbcSourceOffsetProvider provider = createProviderInBinlogPhase();
        // Before reset: binlog phase, noMoreSplits() returns true
        Assert.assertTrue(provider.noMoreSplits());

        // Configure sourceProperties so checkNeedSplitChunks returns true (offset=initial)
        provider.getSourceProperties().put(DataSourceConfigKeys.OFFSET, DataSourceConfigKeys.OFFSET_INITIAL);
        provider.cachedSyncTables = new ArrayList<>();
        provider.cachedSyncTables.add("orders");

        provider.resetToInitialState();

        // After reset with offset=initial and a table to split: noMoreSplits() returns false
        // because cdcSplitProgress is cleared (currentSplittingTable == null) but
        // computeCdcRemainingTables() finds "orders" untouched.
        Assert.assertFalse(provider.noMoreSplits());
    }

    @Test
    public void testResetIsIdempotent() {
        JdbcSourceOffsetProvider provider = createProviderInBinlogPhase();

        provider.resetToInitialState();
        provider.resetToInitialState();

        Assert.assertNull(provider.currentOffset);
        Assert.assertNull(provider.binlogOffsetPersist);
        Assert.assertTrue(provider.hasMoreData);
    }

    @Test
    public void testResetClearsSnapshotPhaseState() {
        JdbcSourceOffsetProvider provider = createProviderInSnapshotPhase();

        provider.resetToInitialState();

        Assert.assertNull(provider.currentOffset);
        Assert.assertTrue(provider.remainingSplits.isEmpty());
        Assert.assertTrue(provider.finishedSplits.isEmpty());
        Assert.assertTrue(provider.chunkHighWatermarkMap.isEmpty());
        assertProgressCleared(provider.committedSplitProgress);
        assertProgressCleared(provider.cdcSplitProgress);
    }

    // --- helpers ---

    private static JdbcSourceOffsetProvider createProviderInBinlogPhase() {
        Map<String, String> sourceProps = new HashMap<>();
        sourceProps.put(DataSourceConfigKeys.OFFSET, DataSourceConfigKeys.OFFSET_LATEST);
        JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider(1L,
                org.apache.doris.job.common.DataSourceType.MYSQL, sourceProps);

        // Simulate binlog phase state
        Map<String, String> binlogOffset = new HashMap<>();
        binlogOffset.put("file", "binlog.000003");
        binlogOffset.put("pos", "154");
        BinlogSplit binlogSplit = new BinlogSplit(binlogOffset);
        provider.currentOffset = new JdbcOffset(Collections.singletonList(binlogSplit));
        provider.binlogOffsetPersist = new HashMap<>(binlogOffset);
        provider.binlogOffsetPersist.put(JdbcSourceOffsetProvider.SPLIT_ID, BinlogSplit.BINLOG_SPLIT_ID);
        provider.endBinlogOffset = Collections.singletonMap("file", "binlog.000005");
        provider.tableSchemas = "{\"orders\":{\"id\":\"INT\"}}";
        provider.hasMoreData = false;
        provider.boundBackendId = 12345L;

        // Add some snapshot history
        SnapshotSplit finished = new SnapshotSplit("orders:0", "test_db.orders",
                Collections.singletonList("id"), new Object[]{0L}, new Object[]{100L},
                Collections.singletonMap("file", "binlog.000001"));
        provider.finishedSplits.add(finished);
        provider.chunkHighWatermarkMap
                .computeIfAbsent("test_db.orders", k -> new HashMap<>())
                .put("orders:0", finished.getHighWatermark());

        provider.committedSplitProgress = new JdbcSourceOffsetProvider.SplitProgress();
        provider.committedSplitProgress.setCurrentSplittingTable("orders");
        provider.committedSplitProgress.setNextSplitStart(new Object[]{100L});
        provider.committedSplitProgress.setNextSplitId(1);

        provider.cdcSplitProgress = new JdbcSourceOffsetProvider.SplitProgress();
        provider.cdcSplitProgress.setCurrentSplittingTable("orders");
        provider.cdcSplitProgress.setNextSplitStart(new Object[]{100L});
        provider.cdcSplitProgress.setNextSplitId(1);

        return provider;
    }

    private static JdbcSourceOffsetProvider createProviderInSnapshotPhase() {
        Map<String, String> sourceProps = new HashMap<>();
        sourceProps.put(DataSourceConfigKeys.OFFSET, DataSourceConfigKeys.OFFSET_INITIAL);
        JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider(2L,
                org.apache.doris.job.common.DataSourceType.MYSQL, sourceProps);

        SnapshotSplit remaining = new SnapshotSplit("orders:1", "test_db.orders",
                Collections.singletonList("id"), new Object[]{100L}, new Object[]{200L},
                null);
        SnapshotSplit finished = new SnapshotSplit("orders:0", "test_db.orders",
                Collections.singletonList("id"), new Object[]{0L}, new Object[]{100L},
                Collections.singletonMap("file", "binlog.000001"));

        provider.remainingSplits.add(remaining);
        provider.finishedSplits.add(finished);
        provider.chunkHighWatermarkMap
                .computeIfAbsent("test_db.orders", k -> new HashMap<>())
                .put("orders:0", finished.getHighWatermark());

        provider.currentOffset = new JdbcOffset(Collections.singletonList(finished));

        provider.committedSplitProgress = new JdbcSourceOffsetProvider.SplitProgress();
        provider.committedSplitProgress.setCurrentSplittingTable("orders");
        provider.committedSplitProgress.setNextSplitStart(new Object[]{100L});
        provider.committedSplitProgress.setNextSplitId(1);

        provider.cdcSplitProgress = provider.committedSplitProgress.copy();

        return provider;
    }

    private static void assertProgressCleared(JdbcSourceOffsetProvider.SplitProgress progress) {
        Assert.assertNotNull(progress);
        Assert.assertNull(progress.getCurrentSplittingTable());
        Assert.assertNull(progress.getNextSplitStart());
        Assert.assertNull(progress.getNextSplitId());
    }
}
