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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.cdc.split.BinlogSplit;
import org.apache.doris.job.cdc.split.SnapshotSplit;
import org.apache.doris.job.common.DataSourceType;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.streaming.StreamingInsertJob;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.job.util.StreamingJobUtils;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcSourceOffsetProviderOffsetTest {

    @Test
    public void testTableChangeReplayRestoresUncommittedChunksWithoutWatermarks() throws Exception {
        JdbcSourceOffsetProvider source = provider(DataSourceType.POSTGRES);
        source.updateOffset(new JdbcOffset(Collections.singletonList(new BinlogSplit(Map.of("lsn", "100")))));
        String retainedSchema = "[{\"i\":\"public.retained\",\"c\":{}}]";
        source.setTableSchemas(retainedSchema);
        source.applyIncludeTablesChange(List.of("retained", "added"), Collections.singletonList("added"));
        SnapshotSplit chunk = new SnapshotSplit("public.added:0", "public.added",
                Collections.singletonList("id"), null, new Object[] {10}, null);
        StreamingInsertJob job = mockJobWithPersistInfo(source.getPersistInfo());
        Deencapsulation.setField(job, "syncTables", List.of("retained", "added"));
        job.setJobStatus(JobStatus.PENDING);
        try (MockedStatic<StreamingJobUtils> utilities = Mockito.mockStatic(StreamingJobUtils.class)) {
            Map<String, List<SnapshotSplit>> saved = new HashMap<>();
            saved.put("added", new ArrayList<>(Collections.singletonList(chunk)));
            saved.put("removed", new ArrayList<>(Collections.singletonList(
                    new SnapshotSplit("public.removed:0", "public.removed", Collections.singletonList("id"),
                            null, null, null))));
            utilities.when(() -> StreamingJobUtils.restoreSplitsToJob(job.getJobId())).thenReturn(saved);
            JdbcSourceOffsetProvider restored = new JdbcSourceOffsetProvider() {
                @Override
                protected List<SnapshotSplit> rpcFetchSplitsBatch(
                        String table, Object[] start, Integer splitId) {
                    throw new AssertionError("Replay must not fetch splits to recover schemas");
                }
            };
            restored.setSourceType(DataSourceType.POSTGRES);
            restored.replayIfNeed(job);
            Assertions.assertEquals(1, restored.pendingSplitCount());
            Assertions.assertTrue(restored.currentOffset.snapshotSplit());
            Assertions.assertEquals("public.added:0", restored.remainingSplits.get(0).getSplitId());
            Assertions.assertEquals("added", restored.cdcSplitProgress.getCurrentSplittingTable());
            Assertions.assertArrayEquals(new Object[] {10}, restored.cdcSplitProgress.getNextSplitStart());
            Assertions.assertEquals("100", restored.getBinlogOffsetPersist().get("lsn"));
            Assertions.assertFalse(restored.noMoreSplits());
            Assertions.assertEquals(retainedSchema, restored.getTableSchemas());
            Assertions.assertFalse(restored.isTableSchemasDirty());
            SnapshotSplit committed = new SnapshotSplit("public.added:0", "public.added",
                    null, null, null, Map.of("lsn", "110"));
            restored.updateOffset(new JdbcOffset(Collections.singletonList(committed)));
            Assertions.assertEquals("public.added:0", restored.finishedSplits.get(0).getSplitId());
        }
    }

    @Test
    public void testLatestTableChangeSnapshotsOnlyAddedTablesAndKeepsCommittedOffset() throws Exception {
        JdbcSourceOffsetProvider provider = provider(DataSourceType.MYSQL);
        provider.getSourceProperties().put(DataSourceConfigKeys.OFFSET, DataSourceConfigKeys.OFFSET_LATEST);
        provider.updateOffset(new JdbcOffset(Collections.singletonList(
                new BinlogSplit(Map.of("file", "mysql-bin.000001", "pos", "100")))));
        provider.setTableSchemas("[{\"i\":\"db.old_table\",\"c\":{}}]");
        provider.applyIncludeTablesChange(Arrays.asList("old_table", "added_table"),
                Collections.singletonList("added_table"));
        Assertions.assertTrue(provider.hasPendingIncludeTablesChange());
        Assertions.assertTrue(provider.isSnapshotPhase());
        Assertions.assertFalse(provider.noMoreSplits());
        Assertions.assertEquals("100", provider.getBinlogOffsetPersist().get("pos"));
        Assertions.assertEquals(Collections.singletonList("added_table"), provider.getAddedTables());
        Assertions.assertEquals("100", provider.getLagReferenceOffset().get("pos"));
        Assertions.assertThrows(JobException.class,
                () -> provider.validateAlterIncludeTables(List.of("old_table")));
    }

    @Test
    public void testAddedSnapshotWaitsForRealSplitsBeforeResumingIncrementalOffset() throws Exception {
        JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider() {
            @Override
            protected int compareOffset(Map<String, String> first, Map<String, String> second) {
                throw new AssertionError("Pending table changes must not compare incremental offsets");
            }
        };
        provider.setSourceType(DataSourceType.POSTGRES);
        provider.updateOffset(new JdbcOffset(List.of(new BinlogSplit(Map.of("lsn", "100")))));
        provider.endBinlogOffset = Map.of("lsn", "200");
        provider.applyIncludeTablesChange(List.of("added"), List.of("added"));
        Assertions.assertFalse(provider.currentOffset.snapshotSplit());
        Assertions.assertEquals("100", ((BinlogSplit) provider.currentOffset.getSplits().get(0))
                .getStartingOffset().get("lsn"));
        Assertions.assertFalse(provider.hasMoreDataToConsume());

        SnapshotSplit first = new SnapshotSplit("public.added:0", "public.added", List.of("id"),
                null, new Object[] {10}, Map.of("lsn", "110"));
        provider.remainingSplits.add(first);
        provider.cdcSplitProgress.setCurrentSplittingTable("added");
        provider.hasMoreData = false;
        Assertions.assertTrue(provider.hasMoreDataToConsume());
        Assertions.assertEquals(List.of(first), ((JdbcOffset) provider.getNextOffset(null,
                Collections.emptyMap())).getSplits());
        provider.updateOffset(new JdbcOffset(List.of(first)));
        Assertions.assertFalse(provider.hasMoreDataToConsume());

        SnapshotSplit last = new SnapshotSplit("public.added:1", "public.added", List.of("id"),
                new Object[] {10}, null, Map.of("lsn", "120"));
        provider.remainingSplits.add(last);
        provider.cdcSplitProgress.setCurrentSplittingTable(null);
        Assertions.assertTrue(provider.hasMoreDataToConsume());
        provider.updateOffset(new JdbcOffset(List.of(last)));
        Assertions.assertTrue(provider.hasMoreDataToConsume());
        BinlogSplit next = (BinlogSplit) ((JdbcOffset) provider.getNextOffset(null,
                Collections.emptyMap())).getSplits().get(0);
        Assertions.assertEquals("100", next.getStartingOffset().get("lsn"));
    }

    @Test
    public void testAlterTablesRequiresFirstIncrementalCommit() throws Exception {
        for (String mode : List.of(DataSourceConfigKeys.OFFSET_INITIAL, DataSourceConfigKeys.OFFSET_SNAPSHOT,
                DataSourceConfigKeys.OFFSET_LATEST)) {
            JdbcSourceOffsetProvider provider = provider(DataSourceType.POSTGRES);
            provider.getSourceProperties().put(DataSourceConfigKeys.OFFSET, mode);
            Assertions.assertThrows(JobException.class,
                    () -> provider.validateAlterIncludeTables(List.of("retained")));
        }
        JdbcSourceOffsetProvider provider = provider(DataSourceType.POSTGRES);
        provider.getSourceProperties().put(DataSourceConfigKeys.OFFSET, DataSourceConfigKeys.OFFSET_INITIAL);
        provider.cachedSyncTables = List.of("retained");
        SnapshotSplit finished = new SnapshotSplit("public.retained:0", "public.retained", List.of("id"),
                null, null, Map.of("lsn", "100"));
        provider.remainingSplits.add(finished);
        provider.updateOffset(new JdbcOffset(List.of(finished)));
        Assertions.assertFalse(provider.isSnapshotPhase());
        Assertions.assertThrows(JobException.class,
                () -> provider.validateAlterIncludeTables(List.of("retained")));
        BinlogSplit next = (BinlogSplit) ((JdbcOffset) provider.getNextOffset(null,
                Collections.emptyMap())).getSplits().get(0);
        Assertions.assertNull(next.getStartingOffset());
        Assertions.assertEquals(List.of(finished), next.getFinishedSplits());
        provider.updateOffset(new JdbcOffset(List.of(new BinlogSplit(Map.of("lsn", "110")))));
        Assertions.assertDoesNotThrow(() -> provider.validateAlterIncludeTables(List.of("retained", "added")));
    }

    @Test
    public void testRemovingAddedTableWaitsForIncrementalCommitAfterReplay() throws Exception {
        JdbcSourceOffsetProvider provider = provider(DataSourceType.POSTGRES);
        provider.updateOffset(new JdbcOffset(List.of(new BinlogSplit(Map.of("lsn", "100")))));
        provider.applyIncludeTablesChange(List.of("retained", "added"), List.of("added"));
        Assertions.assertThrows(JobException.class,
                () -> provider.validateAlterIncludeTables(List.of("retained")));
        SnapshotSplit chunk = new SnapshotSplit("public.added:0", "public.added", List.of("id"),
                null, null, null);
        StreamingInsertJob job = mockJobWithPersistInfo(provider.getPersistInfo());
        Deencapsulation.setField(job, "syncTables", List.of("retained", "added"));
        job.setJobStatus(JobStatus.PAUSED);
        try (MockedStatic<StreamingJobUtils> utilities = Mockito.mockStatic(StreamingJobUtils.class)) {
            utilities.when(() -> StreamingJobUtils.restoreSplitsToJob(job.getJobId())).thenReturn(new HashMap<>());
            JdbcSourceOffsetProvider restored = provider(DataSourceType.POSTGRES);
            restored.replayIfNeed(job);
            Assertions.assertNull(restored.currentOffset);
            Assertions.assertFalse(restored.hasMoreDataToConsume());
            utilities.when(() -> StreamingJobUtils.restoreSplitsToJob(job.getJobId())).thenReturn(
                    new HashMap<>(Map.of("added", new ArrayList<>(List.of(chunk)))));
            restored.replayIfNeed(job);
            Assertions.assertTrue(restored.currentOffset.snapshotSplit());
            Assertions.assertTrue(restored.hasMoreDataToConsume());
            Assertions.assertThrows(JobException.class,
                    () -> restored.validateAlterIncludeTables(List.of("retained")));
            SnapshotSplit committed = new SnapshotSplit("public.added:0", "public.added", List.of("id"),
                    null, null, Map.of("lsn", "120"));
            restored.updateOffset(new JdbcOffset(List.of(committed)));
            Assertions.assertFalse(restored.isSnapshotPhase());
            Assertions.assertTrue(restored.hasMoreDataToConsume());
            job.setOffsetProviderPersist(restored.getPersistInfo());
            restored.replayIfNeed(job);
            Assertions.assertNull(restored.currentOffset);
            Assertions.assertEquals(1, restored.finishedSplits.size());
            Assertions.assertTrue(restored.hasMoreDataToConsume());
            Assertions.assertThrows(JobException.class,
                    () -> restored.validateAlterIncludeTables(List.of("retained")));
            BinlogSplit next = (BinlogSplit) ((JdbcOffset) restored.getNextOffset(null,
                    Collections.emptyMap())).getSplits().get(0);
            Assertions.assertEquals("100", next.getStartingOffset().get("lsn"));
            restored.updateOffset(new JdbcOffset(List.of(new BinlogSplit(Map.of("lsn", "130")))));
            Assertions.assertFalse(restored.hasPendingIncludeTablesChange());
            Assertions.assertDoesNotThrow(() -> restored.validateAlterIncludeTables(List.of("retained")));
        }
    }

    @Test
    public void testAppendTablesBeforeSnapshotStarts() throws Exception {
        JdbcSourceOffsetProvider provider = provider(DataSourceType.POSTGRES);
        provider.updateOffset(new JdbcOffset(List.of(new BinlogSplit(Map.of("lsn", "100")))));
        provider.applyIncludeTablesChange(List.of("retained", "added"), List.of("added"));

        provider.validateAlterIncludeTables(List.of("retained", "added", "next"));
        provider.applyIncludeTablesChange(List.of("retained", "added", "next"), List.of("next"));

        Assertions.assertEquals(List.of("added", "next"), provider.getAddedTables());
        Assertions.assertEquals(List.of("retained", "added", "next"), provider.cachedSyncTables);
        Assertions.assertFalse(provider.noMoreSplits());
        Assertions.assertFalse(provider.hasMoreDataToConsume());
        Assertions.assertEquals("100", provider.getBinlogOffsetPersist().get("lsn"));
        Assertions.assertThrows(JobException.class,
                () -> provider.validateAlterIncludeTables(List.of("retained", "added")));
        Assertions.assertThrows(JobException.class,
                () -> provider.validateAlterIncludeTables(List.of("retained", "next")));
    }

    @Test
    public void testAppendAndRemoveRetainedTablePreservesSnapshotProgressAfterReplay() throws Exception {
        JdbcSourceOffsetProvider provider = provider(DataSourceType.POSTGRES);
        provider.updateOffset(new JdbcOffset(List.of(new BinlogSplit(Map.of("lsn", "100")))));
        provider.setTableSchemas("[{\"i\":\"public.retained\",\"c\":{}},{\"i\":\"public.added\",\"c\":{}}]");
        provider.applyIncludeTablesChange(List.of("retained", "added"), List.of("added"));
        provider.committedSplitProgress = new JdbcSourceOffsetProvider.SplitProgress();
        SnapshotSplit first = new SnapshotSplit("public.added:0", "public.added", List.of("id"),
                null, new Object[] {10}, Map.of("lsn", "110"));
        SnapshotSplit second = new SnapshotSplit("public.added:1", "public.added", List.of("id"),
                new Object[] {10}, new Object[] {20}, null);
        provider.remainingSplits.addAll(List.of(first, second));
        provider.updateOffset(new JdbcOffset(List.of(first)));
        provider.resumeCdcSplitProgressFromSplits();

        // Remove the original table and append another table while the first added table is mid-snapshot.
        provider.validateAlterIncludeTables(List.of("added", "next"));
        provider.applyIncludeTablesChange(List.of("added", "next"), List.of("next"));
        Assertions.assertEquals(List.of("added", "next"), provider.getAddedTables());
        Assertions.assertEquals(List.of(first), provider.finishedSplits);
        Assertions.assertEquals(List.of(second), provider.remainingSplits);
        Assertions.assertEquals("added", provider.cdcSplitProgress.getCurrentSplittingTable());
        Assertions.assertArrayEquals(new Object[] {20}, provider.cdcSplitProgress.getNextSplitStart());
        Assertions.assertEquals(2, provider.cdcSplitProgress.getNextSplitId());
        Assertions.assertArrayEquals(new Object[] {10}, provider.committedSplitProgress.getNextSplitStart());
        Assertions.assertFalse(provider.getTableSchemas().contains("public.retained"));
        Assertions.assertTrue(provider.getTableSchemas().contains("public.added"));

        StreamingInsertJob job = mockJobWithPersistInfo(provider.getPersistInfo());
        Deencapsulation.setField(job, "syncTables", List.of("added", "next"));
        try (MockedStatic<StreamingJobUtils> utilities = Mockito.mockStatic(StreamingJobUtils.class)) {
            utilities.when(() -> StreamingJobUtils.restoreSplitsToJob(job.getJobId())).thenAnswer(invocation ->
                    new HashMap<>(Map.of("added", new ArrayList<>(List.of(first, second)))));
            JdbcSourceOffsetProvider restored = provider(DataSourceType.POSTGRES);
            restored.replayIfNeed(job);
            Assertions.assertEquals(List.of("added", "next"), restored.getAddedTables());
            Assertions.assertEquals(List.of(first), restored.finishedSplits);
            Assertions.assertEquals(List.of(second), restored.remainingSplits);
            Assertions.assertEquals(List.of(second), restored.currentOffset.getSplits());
            Assertions.assertArrayEquals(new Object[] {20}, restored.cdcSplitProgress.getNextSplitStart());
            Assertions.assertEquals(2, restored.cdcSplitProgress.getNextSplitId());
            Assertions.assertEquals(Map.of("lsn", "110"),
                    restored.chunkHighWatermarkMap.get("public.added").get("public.added:0"));
            Assertions.assertEquals("100", restored.getBinlogOffsetPersist().get("lsn"));
            Assertions.assertEquals(List.of(second), ((JdbcOffset) restored.getNextOffset(null,
                    Collections.emptyMap())).getSplits());
        }
    }

    @Test
    public void testAppendAfterSnapshotCompletionSchedulesNewTable() throws Exception {
        SnapshotSplit next = new SnapshotSplit("public.next:0", "public.next", List.of("id"),
                null, null, null);
        JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider() {
            @Override
            protected List<SnapshotSplit> rpcFetchSplitsBatch(
                    String table, Object[] start, Integer splitId) {
                Assertions.assertEquals("next", table);
                Assertions.assertNull(start);
                Assertions.assertNull(splitId);
                return List.of(next);
            }
        };
        provider.setSourceType(DataSourceType.POSTGRES);
        provider.updateOffset(new JdbcOffset(List.of(new BinlogSplit(Map.of("lsn", "100")))));
        provider.applyIncludeTablesChange(List.of("retained", "added"), List.of("added"));
        SnapshotSplit finished = new SnapshotSplit("public.added:0", "public.added", List.of("id"),
                null, null, Map.of("lsn", "110"));
        provider.remainingSplits.add(finished);
        provider.updateOffset(new JdbcOffset(List.of(finished)));
        Assertions.assertTrue(provider.noMoreSplits());

        // The completed table remains in this round until an incremental offset is committed.
        provider.validateAlterIncludeTables(List.of("retained", "added", "next"));
        provider.applyIncludeTablesChange(List.of("retained", "added", "next"), List.of("next"));
        Assertions.assertFalse(provider.noMoreSplits());
        Assertions.assertFalse(provider.hasMoreDataToConsume());
        Assertions.assertThrows(JobException.class,
                () -> provider.validateAlterIncludeTables(List.of("retained", "next")));
        try (MockedStatic<StreamingJobUtils> utilities = Mockito.mockStatic(StreamingJobUtils.class)) {
            provider.advanceSplits();
            Assertions.assertEquals(List.of(finished), provider.finishedSplits);
            Assertions.assertEquals(List.of(next), ((JdbcOffset) provider.getNextOffset(null,
                    Collections.emptyMap())).getSplits());
            Assertions.assertTrue(provider.noMoreSplits());
        }
        provider.updateOffset(new JdbcOffset(List.of(new SnapshotSplit("public.next:0", "public.next",
                List.of("id"), null, null, Map.of("lsn", "120")))));
        BinlogSplit incremental = (BinlogSplit) ((JdbcOffset) provider.getNextOffset(null,
                Collections.emptyMap())).getSplits().get(0);
        Assertions.assertEquals("100", incremental.getStartingOffset().get("lsn"));
        provider.updateOffset(new JdbcOffset(List.of(new BinlogSplit(Map.of("lsn", "130")))));
        Assertions.assertFalse(provider.hasPendingIncludeTablesChange());
        Assertions.assertDoesNotThrow(() -> provider.validateAlterIncludeTables(List.of("retained")));
    }

    @Test
    public void testTableChangeUsesFreshBinlogSplitAndClearsRoundOnIncrementalCommit() throws Exception {
        JdbcSourceOffsetProvider provider = provider(DataSourceType.MYSQL);
        BinlogSplit old = new BinlogSplit(Map.of("file", "mysql-bin.000001", "pos", "100"));
        old.setEndingOffset(Map.of("file", "mysql-bin.000001", "pos", "150"));
        JdbcOffset committedOffset = new JdbcOffset(Collections.singletonList(old));
        provider.updateOffset(committedOffset);
        provider.applyIncludeTablesChange(Collections.singletonList("added_table"),
                Collections.singletonList("added_table"));
        Assertions.assertSame(committedOffset, provider.currentOffset);
        SnapshotSplit completed = new SnapshotSplit("db.added_table:0", "db.added_table",
                Collections.singletonList("id"), null, null, Map.of("pos", "200"));
        provider.finishedSplits.add(completed);
        BinlogSplit next = (BinlogSplit) ((JdbcOffset) provider.getNextOffset(null, Collections.emptyMap()))
                .getSplits().get(0);
        Assertions.assertEquals("100", next.getStartingOffset().get("pos"));
        Assertions.assertNull(next.getEndingOffset());
        Assertions.assertTrue(next.getFinishedSplits() == null || next.getFinishedSplits().isEmpty());
        provider.updateOffset(new JdbcOffset(Collections.singletonList(
                new BinlogSplit(Map.of("file", "mysql-bin.000001", "pos", "250")))));
        Assertions.assertFalse(provider.hasPendingIncludeTablesChange());
        Assertions.assertTrue(provider.shouldPersistOffset(1, 2));
        Assertions.assertEquals("250", provider.getBinlogOffsetPersist().get("pos"));
    }

    @Test
    public void testRemoveOnlyTableChangeKeepsPendingWithoutSnapshot() throws Exception {
        JdbcSourceOffsetProvider provider = provider(DataSourceType.POSTGRES);
        provider.updateOffset(new JdbcOffset(Collections.singletonList(new BinlogSplit(Map.of("lsn", "100")))));
        provider.applyIncludeTablesChange(Collections.singletonList("retained"), Collections.emptyList());
        Assertions.assertTrue(provider.hasPendingIncludeTablesChange());
        Assertions.assertFalse(provider.isSnapshotPhase());
        Assertions.assertTrue(provider.hasMoreDataToConsume());
        Assertions.assertEquals("100", ((BinlogSplit) ((JdbcOffset) provider.getNextOffset(null,
                Collections.emptyMap())).getSplits().get(0)).getStartingOffset().get("lsn"));
        JdbcSourceOffsetProvider restored = GsonUtils.GSON.fromJson(provider.getPersistInfo(),
                JdbcSourceOffsetProvider.class);
        Assertions.assertNotNull(restored.getAddedTables());
        Assertions.assertTrue(restored.getAddedTables().isEmpty());
        StreamingInsertJob job = mockJobWithPersistInfo(provider.getPersistInfo());
        Deencapsulation.setField(job, "syncTables", List.of("retained"));
        try (MockedStatic<StreamingJobUtils> utilities = Mockito.mockStatic(StreamingJobUtils.class)) {
            utilities.when(() -> StreamingJobUtils.restoreSplitsToJob(job.getJobId())).thenReturn(new HashMap<>());
            restored.replayIfNeed(job);
            Assertions.assertNull(restored.currentOffset);
            Assertions.assertTrue(restored.hasMoreDataToConsume());
            JdbcOffset nextTaskOffset = (JdbcOffset) restored.getNextOffset(null, Collections.emptyMap());
            Assertions.assertFalse(nextTaskOffset.snapshotSplit());
            Assertions.assertEquals("100", ((BinlogSplit) nextTaskOffset.getSplits().get(0))
                    .getStartingOffset().get("lsn"));
        }
    }

    @Test
    public void testValidateAlterOffsetAcceptsSourceSpecificPositions() throws Exception {
        JdbcSourceOffsetProvider postgres = provider(DataSourceType.POSTGRES);
        postgres.validateAlterOffset("{\"lsn\":12345678,\"txId\":\"7\"}");
        postgres.validateAlterOffset("{\"lsn\":\"12345678\"}");

        JdbcSourceOffsetProvider mysql = provider(DataSourceType.MYSQL);
        mysql.validateAlterOffset("{\"file\":\"mysql-bin.000001\",\"pos\":154,\"kind\":\"SPECIFIC\"}");
        mysql.validateAlterOffset("{\"file\":\"mysql-bin.000001\",\"pos\":\"154\"}");
        mysql.validateAlterOffset("{\"gtids\":\"24bc7850-2c16-11ef-a0c9-0242ac120002:1-9\"}");
        mysql.validateAlterOffset(
                "{\"gtids\":\"24bc7850-2c16-11ef-a0c9-0242ac120002:1-9\",\"kind\":\"SPECIFIC\"}");

        JdbcSourceOffsetProvider oceanBase = provider(DataSourceType.OCEANBASE);
        oceanBase.validateAlterOffset("{\"file\":\"mysql-bin.000001\",\"pos\":\"154\"}");
        oceanBase.validateAlterOffset("{\"gtids\":\"24bc7850-2c16-11ef-a0c9-0242ac120002:1-9\"}");
    }

    @Test
    public void testValidateAlterOffsetRejectsInvalidSourceSpecificPositions() {
        assertInvalidAlterOffsets(provider(DataSourceType.POSTGRES),
                "{}",
                "{\"file\":\"mysql-bin.000001\",\"pos\":\"154\"}",
                "{\"lsn\":null}",
                "{\"lsn\":\"abc\"}",
                "{\"lsn\":\"-1\"}",
                "{\"lsn\":\"9223372036854775808\"}");

        assertInvalidAlterOffsets(provider(DataSourceType.MYSQL),
                "{}",
                "{\"lsn\":\"12345678\"}",
                "{\"file\":\"mysql-bin.000001\"}",
                "{\"file\":\"\",\"pos\":\"154\"}",
                "{\"file\":\"mysql-bin.000001\",\"pos\":null}",
                "{\"file\":\"mysql-bin.000001\",\"pos\":\"abc\"}",
                "{\"file\":\"mysql-bin.000001\",\"pos\":\"-1\"}",
                "{\"file\":\"mysql-bin.000001\",\"pos\":\"9223372036854775808\"}",
                "{\"gtids\":\"\"}",
                "{\"gtids\":123}",
                "{\"gtids\":\"uuid:1-10\",\"file\":\"mysql-bin.000001\",\"pos\":\"abc\"}");
    }

    @Test
    public void testValidateAlterOffsetRejectsNonSpecificKind() {
        for (DataSourceType type : new DataSourceType[] {DataSourceType.MYSQL, DataSourceType.OCEANBASE}) {
            assertInvalidAlterOffsets(provider(type),
                    "{\"file\":\"mysql-bin.000001\",\"pos\":154,\"kind\":\"LATEST\"}",
                    "{\"file\":\"mysql-bin.000001\",\"pos\":154,\"kind\":\"EARLIEST\"}",
                    "{\"file\":\"mysql-bin.000001\",\"pos\":154,\"kind\":\"TIMESTAMP\"}",
                    "{\"gtids\":\"uuid:1-10\",\"kind\":\"BOGUS\"}",
                    "{\"gtids\":\"uuid:1-10\",\"kind\":null}");
        }
    }

    @Test
    public void testTaskParallelismKeepsUncommittedSplitsForNextTask() {
        JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider();
        provider.snapshotParallelism = 10000;
        provider.remainingSplits.addAll(Arrays.asList(
                snapshotSplit("source_table:0"), snapshotSplit("source_table:1"), snapshotSplit("source_table:2")));
        Map<String, String> taskProperties = Collections.singletonMap(DataSourceConfigKeys.SNAPSHOT_PARALLELISM, "2");

        JdbcOffset first = (JdbcOffset) provider.getNextOffset(null, taskProperties);
        Assertions.assertEquals(2, first.getSplits().size());
        Assertions.assertEquals(3, provider.pendingSplitCount());
        Assertions.assertEquals(10000, provider.snapshotParallelism);
        // The next task must receive the same uncommitted splits.
        JdbcOffset nextTask = (JdbcOffset) provider.getNextOffset(null, taskProperties);
        Assertions.assertEquals(first.getSplits(), nextTask.getSplits());

        provider.updateOffset(first);
        JdbcOffset second = (JdbcOffset) provider.getNextOffset(null, taskProperties);
        Assertions.assertEquals(1, second.getSplits().size());
        Assertions.assertEquals("source_table:2", second.getSplits().get(0).getSplitId());
        Assertions.assertEquals(1, provider.pendingSplitCount());

        provider.updateOffset(second);
        Assertions.assertEquals(0, provider.pendingSplitCount());
        JdbcOffset binlog = (JdbcOffset) provider.getNextOffset(null, taskProperties);
        Assertions.assertFalse(binlog.snapshotSplit());
        Assertions.assertEquals(3, ((BinlogSplit) binlog.getSplits().get(0)).getFinishedSplits().size());
    }

    @Test
    public void testSnapshotOffsetUsesConfiguredPersistInterval() {
        int oldInterval = Config.streaming_job_snapshot_offset_persist_interval_sec;
        try {
            Config.streaming_job_snapshot_offset_persist_interval_sec = 123;
            JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider();
            provider.currentOffset = new JdbcOffset(
                    Collections.singletonList(snapshotSplit("source_table:0")));

            Assertions.assertTrue(provider.shouldPersistOffset(0L, 1_000L));
            Assertions.assertFalse(provider.shouldPersistOffset(1_000L, 123_999L));
            Assertions.assertTrue(provider.shouldPersistOffset(1_000L, 124_000L));
        } finally {
            Config.streaming_job_snapshot_offset_persist_interval_sec = oldInterval;
        }
    }

    @Test
    public void testBinlogOffsetPersistsImmediately() {
        JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider();
        provider.currentOffset = new JdbcOffset(Collections.singletonList(
                new BinlogSplit(Collections.singletonMap("lsn", "100"))));

        Assertions.assertTrue(provider.shouldPersistOffset(1_000L, 1_001L));
    }

    @Test
    public void testSchemaRemainsPendingUntilJournalAcknowledgement() {
        JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider();
        provider.setTableSchemas("schema-A");
        Assertions.assertEquals("schema-A", GsonUtils.GSON.fromJson(
                provider.getPersistInfo(), JdbcSourceOffsetProvider.class).getTableSchemas());
        // Preparing a record, or receiving the same schema again, must not acknowledge it.
        provider.setTableSchemas("schema-A");
        Assertions.assertEquals("schema-A", GsonUtils.GSON.fromJson(
                provider.getPersistInfo(), JdbcSourceOffsetProvider.class).getTableSchemas());
        Assertions.assertTrue(provider.isTableSchemasDirty());

        provider.markTableSchemasPersisted(provider.getTableSchemas());
        JsonObject delta = GsonUtils.GSON.fromJson(provider.getPersistInfoWithoutTableSchemas(), JsonObject.class);
        Assertions.assertFalse(delta.has("ts"));
        Assertions.assertFalse(provider.isTableSchemasDirty());
        Assertions.assertEquals("schema-A", provider.getTableSchemas());

        provider.setTableSchemas("schema-A");
        Assertions.assertTrue(provider.isTableSchemasDirty());
        provider.setTableSchemas("schema-B");
        Assertions.assertEquals("schema-B", GsonUtils.GSON.fromJson(
                provider.getPersistInfo(), JdbcSourceOffsetProvider.class).getTableSchemas());
        // A journal acknowledgement for the old schema must not clear a newer schema's dirty flag.
        provider.markTableSchemasPersisted("schema-A");
        Assertions.assertTrue(provider.isTableSchemasDirty());
        provider.markTableSchemasPersisted("schema-B");
        Assertions.assertFalse(provider.isTableSchemasDirty());
    }

    @Test
    public void testSchemaFreePersistInfoRetainsOffsetAndSplitProgress() {
        JdbcSourceOffsetProvider provider = GsonUtils.GSON.fromJson(
                "{\"chw\":{\"db.table\":{\"db.table:0\":{\"lsn\":\"90\"}}},"
                        + "\"bop\":{\"lsn\":\"100\"},\"ts\":\"schema-A\",\"csp\":{\"ni\":2}}",
                JdbcSourceOffsetProvider.class);
        String fullState = provider.getPersistInfo();

        JsonObject persisted = GsonUtils.GSON.fromJson(provider.getPersistInfoWithoutTableSchemas(), JsonObject.class);

        Assertions.assertFalse(persisted.has("ts"));
        Assertions.assertEquals("100", persisted.getAsJsonObject("bop").get("lsn").getAsString());
        Assertions.assertEquals("90", persisted.getAsJsonObject("chw").getAsJsonObject("db.table")
                .getAsJsonObject("db.table:0").get("lsn").getAsString());
        Assertions.assertEquals(2, persisted.getAsJsonObject("csp").get("ni").getAsInt());
        Assertions.assertEquals(fullState, provider.getPersistInfo());
    }

    @Test
    public void testPersistedStateMergesWithoutRuntimeInitialization() {
        SourceOffsetProvider merger = new JdbcSourceOffsetProvider();
        String previous = "{\"bop\":{\"lsn\":\"100\"},\"ts\":\"schema-A\"}";
        String merged = merger.mergePersistInfo(previous, "{\"bop\":{\"lsn\":\"200\"}}");
        JdbcSourceOffsetProvider provider = GsonUtils.GSON.fromJson(merged, JdbcSourceOffsetProvider.class);
        Assertions.assertEquals("schema-A", provider.getTableSchemas());
        Assertions.assertEquals("200", provider.getBinlogOffsetPersist().get("lsn"));
        Assertions.assertNull(provider.getCurrentOffset());

        String explicitNull = merger.mergePersistInfo(merged,
                "{\"bop\":{\"lsn\":\"300\"},\"ts\":null}");
        JdbcSourceOffsetProvider recovered = GsonUtils.GSON.fromJson(explicitNull, JdbcSourceOffsetProvider.class);
        Assertions.assertEquals("schema-A", recovered.getTableSchemas());
        Assertions.assertEquals("300", recovered.getBinlogOffsetPersist().get("lsn"));
        String updated = "{\"bop\":{\"lsn\":\"400\"},\"ts\":\"schema-B\"}";
        Assertions.assertEquals(updated, merger.mergePersistInfo(explicitNull, updated));
        Assertions.assertEquals(updated, merger.mergePersistInfo(null, updated));
    }

    @Test
    public void testTvfPersistedStateWithoutSchemaReplacesPreviousSchema() {
        SourceOffsetProvider provider = new JdbcTvfSourceOffsetProvider();
        String previous = "{\"bop\":{\"lsn\":\"100\"},\"ts\":\"schema-A\"}";
        for (String incoming : new String[] {
                "{\"bop\":{\"lsn\":\"200\"}}", "{\"bop\":{\"lsn\":\"200\"},\"ts\":null}"}) {
            JdbcSourceOffsetProvider recovered = GsonUtils.GSON.fromJson(
                    provider.mergePersistInfo(previous, incoming), JdbcSourceOffsetProvider.class);
            Assertions.assertNull(recovered.getTableSchemas());
            Assertions.assertEquals("200", recovered.getBinlogOffsetPersist().get("lsn"));
        }
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

        Assertions.assertTrue(provider.hasMoreDataToConsume());
        Assertions.assertEquals(endOffset, provider.getEndBinlogOffset());
    }

    @Test
    public void testStaleCompareDoesNotOverwriteRefreshedEndOffset() {
        JdbcSourceOffsetProvider provider = new RefreshingEndOffsetProvider();
        provider.setEndBinlogOffset(Collections.singletonMap("lsn", "100"));
        provider.updateOffset(new JdbcOffset(
                Collections.singletonList(new BinlogSplit(Collections.singletonMap("lsn", "200")))));

        Assertions.assertTrue(provider.hasMoreDataToConsume());
        Assertions.assertTrue(provider.hasMoreData);
        Assertions.assertEquals(Collections.singletonMap("lsn", "300"), provider.getEndBinlogOffset());
    }

    @Test
    public void testStaleCompareDoesNotOverwriteAlteredCurrentOffsetState() {
        JdbcSourceOffsetProvider provider = new AlteringCurrentOffsetProvider();
        provider.setEndBinlogOffset(Collections.singletonMap("lsn", "200"));
        provider.updateOffset(new JdbcOffset(
                Collections.singletonList(new BinlogSplit(Collections.singletonMap("lsn", "200")))));

        Assertions.assertTrue(provider.hasMoreDataToConsume());
        Assertions.assertTrue(provider.hasMoreData);
        Assertions.assertEquals(Collections.singletonMap("lsn", "100"),
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

        Assertions.assertNotNull(restored.currentOffset);
        Assertions.assertFalse(restored.currentOffset.snapshotSplit());
        Assertions.assertEquals("200", ((BinlogSplit) restored.currentOffset.getSplits().get(0))
                .getStartingOffset().get("lsn"));
        Assertions.assertTrue(restored.chunkHighWatermarkMap.isEmpty());
    }

    @Test
    public void testTvfBinlogOffsetRestoredFromPersistInfo() throws Exception {
        JdbcSourceOffsetProvider source = new TestJdbcTvfSourceOffsetProvider(-1);
        source.updateOffset(new JdbcOffset(Collections.singletonList(
                new BinlogSplit(Collections.singletonMap("lsn", "200")))));
        StreamingInsertJob job = mockJobWithPersistInfo(source.getPersistInfo());
        JdbcTvfSourceOffsetProvider restored = new JdbcTvfSourceOffsetProvider();
        restored.updateOffset(new JdbcOffset(Collections.singletonList(
                new BinlogSplit(Collections.singletonMap("lsn", "100")))));

        restored.restoreFromPersistInfo(source.getPersistInfo());
        restored.replayIfNeed(job);

        Assertions.assertNotNull(restored.currentOffset);
        Assertions.assertFalse(restored.currentOffset.snapshotSplit());
        Assertions.assertEquals("200", ((BinlogSplit) restored.currentOffset.getSplits().get(0))
                .getStartingOffset().get("lsn"));
        Assertions.assertTrue(restored.chunkHighWatermarkMap.isEmpty());
    }

    private static void assertEndOffsetAdvancesWhenCurrentOffsetIsAhead(JdbcSourceOffsetProvider provider) {
        Map<String, String> staleEndOffset = Collections.singletonMap("lsn", "100");
        Map<String, String> committedOffset = Collections.singletonMap("lsn", "200");
        provider.setEndBinlogOffset(staleEndOffset);
        provider.setHasMoreData(false);

        provider.updateOffset(new JdbcOffset(
                Collections.singletonList(new BinlogSplit(committedOffset))));

        Assertions.assertEquals(staleEndOffset, provider.getEndBinlogOffset());
        Assertions.assertFalse(provider.hasMoreDataToConsume());
        Assertions.assertEquals(committedOffset, provider.getEndBinlogOffset());
        Assertions.assertEquals("{\"lsn\":\"200\"}", provider.getShowMaxOffset());
    }

    private static void assertValidBinlogOffsetClearsSnapshotState(JdbcSourceOffsetProvider provider) {
        seedSnapshotState(provider);
        Map<String, String> binlogOffset = Collections.singletonMap("lsn", "200");

        provider.updateOffset(new JdbcOffset(
                Collections.singletonList(new BinlogSplit(binlogOffset))));

        Assertions.assertTrue(provider.chunkHighWatermarkMap.isEmpty());
        Assertions.assertTrue(provider.remainingSplits.isEmpty());
        Assertions.assertTrue(provider.finishedSplits.isEmpty());
        assertProgressCleared(provider.committedSplitProgress);
        assertProgressCleared(provider.cdcSplitProgress);
        Assertions.assertEquals("table-schemas", provider.tableSchemas);
        Map<String, String> expectedPersist = new HashMap<>(binlogOffset);
        expectedPersist.put(JdbcSourceOffsetProvider.SPLIT_ID, BinlogSplit.BINLOG_SPLIT_ID);
        Assertions.assertEquals(expectedPersist, provider.binlogOffsetPersist);
        String persistInfo = provider.getPersistInfo();
        Assertions.assertFalse(persistInfo.contains("source_table:0"));
        Assertions.assertFalse(persistInfo.contains("source_table:1"));
        Assertions.assertTrue(persistInfo.contains("table-schemas"));
    }

    private static void assertEmptyBinlogOffsetKeepsPreviousState(JdbcSourceOffsetProvider provider) {
        seedSnapshotState(provider);
        JdbcOffset previousOffset = new JdbcOffset(
                Collections.singletonList(snapshotSplit("source_table:0")));
        provider.currentOffset = previousOffset;
        provider.hasMoreData = false;

        provider.updateOffset(new JdbcOffset(
                Collections.singletonList(new BinlogSplit(Collections.emptyMap()))));

        Assertions.assertSame(previousOffset, provider.currentOffset);
        Assertions.assertFalse(provider.hasMoreData);
        Assertions.assertFalse(provider.chunkHighWatermarkMap.isEmpty());
        Assertions.assertFalse(provider.remainingSplits.isEmpty());
        Assertions.assertFalse(provider.finishedSplits.isEmpty());
        Assertions.assertEquals("source_table", provider.committedSplitProgress.getCurrentSplittingTable());
        Assertions.assertEquals("source_table", provider.cdcSplitProgress.getCurrentSplittingTable());
        Assertions.assertNull(provider.binlogOffsetPersist);
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

        Assertions.assertEquals(firstPersistInfo, provider.getPersistInfo());
        Assertions.assertSame(clearedHighWatermarkMap, provider.chunkHighWatermarkMap);
        Assertions.assertTrue(provider.chunkHighWatermarkMap.isEmpty());
        Assertions.assertTrue(provider.remainingSplits.isEmpty());
        Assertions.assertTrue(provider.finishedSplits.isEmpty());
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
        Assertions.assertNotNull(progress);
        Assertions.assertNull(progress.getCurrentSplittingTable());
        Assertions.assertNull(progress.getNextSplitStart());
        Assertions.assertNull(progress.getNextSplitId());
    }

    private static JdbcSourceOffsetProvider provider(DataSourceType sourceType) {
        JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider();
        provider.setSourceType(sourceType);
        return provider;
    }

    private static void assertInvalidAlterOffsets(JdbcSourceOffsetProvider provider, String... offsets) {
        for (String offset : offsets) {
            Assertions.assertThrows(AnalysisException.class,
                    () -> provider.validateAlterOffset(offset), offset);
        }
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
