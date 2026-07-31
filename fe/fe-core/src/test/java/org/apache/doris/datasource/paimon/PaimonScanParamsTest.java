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

package org.apache.doris.datasource.paimon;

import org.apache.doris.analysis.TableScanParams;

import com.google.common.collect.ImmutableMap;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;

public class PaimonScanParamsTest {

    @Test
    public void testValidateKnownScanOptions() {
        PaimonScanParams.validateOptions(ImmutableMap.of(
                "scan.snapshot-id", "1",
                "scan.plan-sort-partition", "true"));
    }

    @Test
    public void testValidateRelationScopedReaderOptions() {
        PaimonScanParams.validateOptions(ImmutableMap.of(
                "read.batch-size", "4096",
                "file-reader-async-threshold", "16 MB",
                "file-index.read.enabled", "false",
                "source.split.target-size", "64 MB",
                "source.split.open-file-cost", "1 MB",
                "scan.manifest.parallelism", "1",
                "scan.plan-sort-partition", "true"));

        for (Map<String, String> options : new Map[] {
                ImmutableMap.of("read.batch-size", "0"),
                ImmutableMap.of("read.batch-size", "-1"),
                ImmutableMap.of("read.batch-size", "65537"),
                ImmutableMap.of("file-reader-async-threshold", "512 KB"),
                ImmutableMap.of("file-reader-async-threshold", "2 GB")
        }) {
            Assert.assertThrows(IllegalArgumentException.class,
                    () -> PaimonScanParams.validateOptions(options));
        }
    }

    @Test
    public void testPlanningOptionsDoNotReuseMetadataProjection() {
        Assert.assertTrue(PaimonScanParams.hasOnlyReaderOptions(ImmutableMap.of(
                "file-index.read.enabled", "false",
                "source.split.target-size", "64 MB")));
        Assert.assertFalse(PaimonScanParams.hasOnlyReaderOptions(
                ImmutableMap.of("scan.manifest.parallelism", "1")));
        Assert.assertFalse(PaimonScanParams.hasOnlyReaderOptions(
                ImmutableMap.of("scan.plan-sort-partition", "true")));
    }

    @Test
    public void testManifestParallelismUsesClusterStableBounds() {
        PaimonScanParams.validateOptions(ImmutableMap.of(
                "scan.manifest.parallelism", String.valueOf(PaimonReaderOptions.MAX_MANIFEST_PARALLELISM)));

        for (int invalid : new int[] {0, -1, PaimonReaderOptions.MAX_MANIFEST_PARALLELISM + 1}) {
            IllegalArgumentException exception = Assert.assertThrows(
                    IllegalArgumentException.class,
                    () -> PaimonScanParams.validateOptions(ImmutableMap.of(
                            "scan.manifest.parallelism", String.valueOf(invalid))));
            Assert.assertTrue(exception.getMessage().contains("scan.manifest.parallelism"));
        }
    }

    @Test
    public void testRelationReaderOptionsAreAppliedWithoutMutatingBaseTable() {
        Table table = Mockito.mock(Table.class);
        Table copied = Mockito.mock(Table.class);
        Mockito.when(table.copy(ArgumentMatchers.anyMap())).thenReturn(copied);
        Mockito.when(copied.options()).thenReturn(ImmutableMap.of(
                "read.batch-size", "8192",
                "file-reader-async-threshold", "32 MB"));

        Assert.assertSame(copied, PaimonScanParams.applyOptions(table, ImmutableMap.of(
                "read.batch-size", "8192",
                "file-reader-async-threshold", "32 MB")));
        Mockito.verify(table).copy(ImmutableMap.of(
                "read.batch-size", "8192",
                "file-reader-async-threshold", "32 MB"));
    }

    @Test
    public void testRejectUnknownAndConflictingOptions() {
        IllegalArgumentException typo = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> PaimonScanParams.validateOptions(
                        ImmutableMap.of("scan.snapsh0t-id", "1")));
        Assert.assertTrue(typo.getMessage().contains("scan.snapsh0t-id"));

        IllegalArgumentException conflict = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> PaimonScanParams.validateOptions(ImmutableMap.of(
                        "scan.snapshot-id", "1",
                        "scan.tag-name", "tag1")));
        Assert.assertTrue(conflict.getMessage().contains("Only one"));
    }

    @Test
    public void testRejectIncompatibleStartupOptionsAndFallbackBranch() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> PaimonScanParams.validateOptions(ImmutableMap.of(
                        "scan.snapshot-id", "1",
                        "scan.creation-time-millis", "1000")));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> PaimonScanParams.validateOptions(ImmutableMap.of(
                        "scan.mode", "latest",
                        "scan.snapshot-id", "1")));
        IllegalArgumentException fallback = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> PaimonScanParams.validateOptions(
                        ImmutableMap.of("scan.fallback-branch", "archive")));
        Assert.assertTrue(fallback.getMessage().contains("scan.fallback-branch"));
    }

    @Test
    public void testRejectMissingCreationTimeAndNonBatchOptions() {
        IllegalArgumentException missingTime = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> PaimonScanParams.validateOptions(
                        ImmutableMap.of("scan.mode", "from-creation-timestamp")));
        Assert.assertTrue(missingTime.getMessage().contains("scan.creation-time-millis"));

        for (String option : new String[] {"scan.bounded.watermark", "scan.max-splits-per-task"}) {
            IllegalArgumentException unsupported = Assert.assertThrows(
                    IllegalArgumentException.class,
                    () -> PaimonScanParams.validateOptions(ImmutableMap.of(option, "1")));
            Assert.assertTrue(unsupported.getMessage().contains(option));
        }
    }

    @Test
    public void testPinOptionsValidatesBeforeClearingInheritedState() {
        IllegalArgumentException unsupported = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> PaimonScanParams.pinOptionsToSnapshot(
                        ImmutableMap.of("scan.bounded.watermark", "1"), 10));
        Assert.assertTrue(unsupported.getMessage().contains("scan.bounded.watermark"));
    }

    @Test
    public void testApplyPositionClearsInheritedStartupState() {
        Table table = Mockito.mock(Table.class);
        Table copied = Mockito.mock(Table.class);
        Mockito.when(table.copy(ArgumentMatchers.anyMap())).thenReturn(copied);
        Mockito.when(copied.options()).thenReturn(Collections.emptyMap());

        Assert.assertSame(copied, PaimonScanParams.applyOptions(
                table, ImmutableMap.of("scan.creation-time-millis", "1000")));

        Mockito.verify(table).copy(ArgumentMatchers.argThat(applied ->
                "1000".equals(applied.get("scan.creation-time-millis"))
                        && containsNull(applied, "scan.mode")
                        && containsNull(applied, "scan.snapshot-id")
                        && containsNull(applied, "scan.tag-name")
                        && containsNull(applied, "scan.timestamp")
                        && containsNull(applied, "scan.timestamp-millis")
                        && containsNull(applied, "scan.watermark")
                        && containsNull(applied, "scan.version")
                        && containsNull(applied, "scan.file-creation-time-millis")
                        && containsNull(applied, "scan.bounded.watermark")
                        && containsNull(applied, "incremental-between")
                        && containsNull(applied, "incremental-between-timestamp")
                        && containsNull(applied, "incremental-between-scan-mode")
                        && containsNull(applied, "incremental-to-auto-tag")));
    }

    @Test
    public void testApplyModeClearsInheritedPositions() {
        Table table = Mockito.mock(Table.class);
        Table copied = Mockito.mock(Table.class);
        Mockito.when(table.copy(ArgumentMatchers.anyMap())).thenReturn(copied);
        Mockito.when(copied.options()).thenReturn(Collections.emptyMap());

        PaimonScanParams.applyOptions(table, ImmutableMap.of("scan.mode", "latest"));

        Mockito.verify(table).copy(ArgumentMatchers.argThat(applied ->
                "latest".equals(applied.get("scan.mode"))
                        && containsNull(applied, "scan.snapshot-id")
                        && containsNull(applied, "scan.tag-name")
                        && containsNull(applied, "scan.timestamp")
                        && containsNull(applied, "scan.timestamp-millis")
                        && containsNull(applied, "scan.watermark")
                        && containsNull(applied, "scan.version")
                        && containsNull(applied, "scan.file-creation-time-millis")
                        && containsNull(applied, "scan.creation-time-millis")));
    }

    @Test
    public void testIsolationClearsFallbackReadStateKeys() {
        Table table = Mockito.mock(Table.class);
        Table copied = Mockito.mock(Table.class);
        Mockito.when(table.copy(ArgumentMatchers.anyMap())).thenReturn(copied);
        Mockito.when(copied.options()).thenReturn(Collections.emptyMap());

        PaimonScanParams.applyOptions(table, ImmutableMap.of("scan.snapshot-id", "1"));

        Mockito.verify(table).copy(ArgumentMatchers.argThat(applied ->
                containsNull(applied, "log.scan")
                        && containsNull(applied, "log.scan.timestamp-millis")));

        Map<String, String> incremental = PaimonScanParams.isolateIncrementalRead(
                ImmutableMap.of("incremental-between", "1,2"));
        Assert.assertTrue(containsNull(incremental, "log.scan"));
        Assert.assertTrue(containsNull(incremental, "log.scan.timestamp-millis"));
    }

    @Test
    public void testSystemTableCapabilityMatrixIncludesRangeAwareReaders() {
        Assert.assertFalse(PaimonScanParams.supportsIncrementalRead("files"));
        for (String type : new String[] {"partitions", "ro"}) {
            Assert.assertTrue(type, PaimonScanParams.supportsIncrementalRead(type));
            Assert.assertFalse(type, PaimonScanParams.requiresPaimonReader(type));
        }
        Assert.assertFalse(PaimonScanParams.supportsOptions("buckets"));
        Assert.assertFalse(PaimonScanParams.supportsOptions("files"));
        Assert.assertTrue(PaimonScanParams.supportsOptions("table_indexes"));
        Assert.assertTrue(PaimonScanParams.requiresPaimonReader("audit_log"));

        Assert.assertThrows(IllegalArgumentException.class,
                () -> PaimonScanParams.validateSystemTable("files", new TableScanParams(
                        TableScanParams.OPTIONS,
                        ImmutableMap.of("scan.creation-time-millis", "1234"),
                        Collections.emptyList())));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> PaimonScanParams.validateSystemTable("files", new TableScanParams(
                        TableScanParams.OPTIONS,
                        ImmutableMap.of("scan.file-creation-time-millis", "1234"),
                        Collections.emptyList())));
    }

    @Test
    public void testSchemaSelectingOptionsRequirePinnedReaderSchema() {
        Assert.assertTrue(PaimonScanParams.selectsSchema(
                ImmutableMap.of("scan.snapshot-id", "1")));
        Assert.assertTrue(PaimonScanParams.selectsSchema(
                ImmutableMap.of("scan.mode", "latest")));
        Assert.assertFalse(PaimonScanParams.selectsSchema(
                ImmutableMap.of("scan.plan-sort-partition", "true")));
    }

    @Test
    public void testMutableSelectorResolvesOnlyOncePerRelation() {
        FileStoreTable baseTable = Mockito.mock(FileStoreTable.class);
        FileStoreTable selectedTable = Mockito.mock(FileStoreTable.class);
        Snapshot firstSnapshot = Mockito.mock(Snapshot.class);
        Mockito.when(baseTable.copy(ArgumentMatchers.anyMap())).thenReturn(selectedTable);
        TableScanParams scanParams = new TableScanParams(
                TableScanParams.OPTIONS,
                ImmutableMap.of("scan.tag-name", "mutable_tag"),
                Collections.emptyList());

        try (MockedStatic<TimeTravelUtil> timeTravel = Mockito.mockStatic(TimeTravelUtil.class)) {
            timeTravel.when(() -> TimeTravelUtil.tryTravelOrLatest(selectedTable))
                    .thenReturn(firstSnapshot);
            Map<String, String> first = scanParams.getOrResolveMapParams(
                    options -> PaimonScanParams.resolveOptions(baseTable, options));
            Map<String, String> second = scanParams.getOrResolveMapParams(
                    options -> PaimonScanParams.resolveOptions(baseTable, options));

            Assert.assertSame(first, second);
            Assert.assertEquals("mutable_tag", second.get("scan.tag-name"));
            Assert.assertFalse(second.containsKey("scan.snapshot-id"));
            timeTravel.verify(() -> TimeTravelUtil.tryTravelOrLatest(selectedTable), Mockito.times(1));
        }
    }

    @Test
    public void testTagSelectorRetainsTagMetadataInsteadOfExpiredSnapshotPath() {
        FileStoreTable baseTable = Mockito.mock(FileStoreTable.class);
        FileStoreTable selectedTable = Mockito.mock(FileStoreTable.class);
        Snapshot taggedSnapshot = Mockito.mock(Snapshot.class);
        Mockito.when(baseTable.copy(ArgumentMatchers.anyMap())).thenReturn(selectedTable);

        try (MockedStatic<TimeTravelUtil> timeTravel = Mockito.mockStatic(TimeTravelUtil.class)) {
            timeTravel.when(() -> TimeTravelUtil.tryTravelOrLatest(selectedTable))
                    .thenReturn(taggedSnapshot);

            Map<String, String> resolved = PaimonScanParams.resolveOptions(
                    baseTable, ImmutableMap.of("scan.tag-name", "retained_tag"));

            Assert.assertEquals("retained_tag", resolved.get("scan.tag-name"));
            Assert.assertFalse(resolved.containsKey("scan.snapshot-id"));
        }
    }

    @Test
    public void testTagValuedVersionRetainsCanonicalTagMetadata() {
        FileStoreTable baseTable = Mockito.mock(FileStoreTable.class);
        FileStoreTable selectedTable = Mockito.mock(FileStoreTable.class);
        Snapshot taggedSnapshot = Mockito.mock(Snapshot.class);
        Mockito.when(baseTable.copy(ArgumentMatchers.anyMap())).thenReturn(selectedTable);
        Mockito.when(selectedTable.options()).thenReturn(ImmutableMap.of("scan.tag-name", "retained_tag"));

        try (MockedStatic<TimeTravelUtil> timeTravel = Mockito.mockStatic(TimeTravelUtil.class)) {
            timeTravel.when(() -> TimeTravelUtil.tryTravelOrLatest(selectedTable))
                    .thenReturn(taggedSnapshot);

            Map<String, String> resolved = PaimonScanParams.resolveOptions(
                    baseTable, ImmutableMap.of("scan.version", "retained_tag"));

            Assert.assertEquals("retained_tag", resolved.get("scan.tag-name"));
            Assert.assertFalse(resolved.containsKey("scan.version"));
            Assert.assertFalse(resolved.containsKey("scan.snapshot-id"));
        }
    }

    @Test
    public void testFileCreationTimePinsLatestSnapshotAndKeepsFilter() {
        FileStoreTable baseTable = Mockito.mock(FileStoreTable.class);
        Snapshot latestSnapshot = Mockito.mock(Snapshot.class);
        Mockito.when(latestSnapshot.id()).thenReturn(19L);
        Mockito.when(baseTable.latestSnapshot()).thenReturn(java.util.Optional.of(latestSnapshot));

        Map<String, String> resolved = PaimonScanParams.resolveOptions(
                baseTable,
                ImmutableMap.of("scan.file-creation-time-millis", "1234"));

        Assert.assertEquals("19", resolved.get("scan.snapshot-id"));
        Assert.assertEquals(Long.valueOf(1234L),
                PaimonScanParams.getPinnedFileCreationTime(resolved).orElse(null));
        Assert.assertFalse(resolved.containsKey("scan.file-creation-time-millis"));
    }

    @Test
    public void testModeOnlyLatestPinsEmptyStatementState() {
        FileStoreTable emptyTable = Mockito.mock(FileStoreTable.class);
        FileStoreTable selectedTable = Mockito.mock(FileStoreTable.class);
        Mockito.when(emptyTable.options()).thenReturn(Collections.emptyMap());
        Mockito.when(emptyTable.copy(ArgumentMatchers.anyMap())).thenReturn(selectedTable);

        try (MockedStatic<TimeTravelUtil> timeTravel = Mockito.mockStatic(TimeTravelUtil.class)) {
            timeTravel.when(() -> TimeTravelUtil.tryTravelOrLatest(selectedTable)).thenReturn(null);
            Map<String, String> resolved = PaimonScanParams.resolveOptions(
                    emptyTable, ImmutableMap.of("scan.mode", "latest"));

            Assert.assertTrue(PaimonScanParams.isPinnedEmptyScan(resolved));
            Assert.assertFalse(resolved.containsKey("scan.mode"));
        }
    }

    @Test
    public void testCompactedFullPinsCompactedSnapshotInsteadOfLatest() {
        FileStoreTable baseTable = Mockito.mock(FileStoreTable.class);
        FileStoreTable selectedTable = Mockito.mock(FileStoreTable.class);
        Snapshot latestSnapshot = Mockito.mock(Snapshot.class);
        Snapshot appendSnapshot = Mockito.mock(Snapshot.class);
        Snapshot compactSnapshot = Mockito.mock(Snapshot.class);
        SnapshotManager snapshotManager = Mockito.mock(SnapshotManager.class);
        Mockito.when(latestSnapshot.id()).thenReturn(19L);
        Mockito.when(appendSnapshot.commitKind()).thenReturn(Snapshot.CommitKind.APPEND);
        Mockito.when(compactSnapshot.commitKind()).thenReturn(Snapshot.CommitKind.COMPACT);
        Mockito.when(baseTable.copy(ArgumentMatchers.anyMap())).thenReturn(selectedTable);
        Mockito.when(selectedTable.coreOptions()).thenReturn(new CoreOptions(Collections.emptyMap()));
        Mockito.when(selectedTable.snapshotManager()).thenReturn(snapshotManager);
        Mockito.when(snapshotManager.pickOrLatest(ArgumentMatchers.any())).thenAnswer(invocation -> {
            java.util.function.Predicate<Snapshot> selector = invocation.getArgument(0);
            Assert.assertFalse(selector.test(appendSnapshot));
            Assert.assertTrue(selector.test(compactSnapshot));
            return 17L;
        });

        try (MockedStatic<TimeTravelUtil> timeTravel = Mockito.mockStatic(TimeTravelUtil.class)) {
            timeTravel.when(() -> TimeTravelUtil.tryTravelOrLatest(selectedTable)).thenReturn(latestSnapshot);

            Map<String, String> resolved = PaimonScanParams.resolveOptions(
                    baseTable, ImmutableMap.of("scan.mode", "compacted-full"));

            Assert.assertEquals("17", resolved.get("scan.snapshot-id"));
        }
    }

    @Test
    public void testCompactedFullHonorsFullCompactionDeltaCommits() {
        FileStoreTable baseTable = Mockito.mock(FileStoreTable.class);
        FileStoreTable selectedTable = Mockito.mock(FileStoreTable.class);
        Snapshot nonFullCompaction = Mockito.mock(Snapshot.class);
        Snapshot fullCompaction = Mockito.mock(Snapshot.class);
        SnapshotManager snapshotManager = Mockito.mock(SnapshotManager.class);
        Mockito.when(nonFullCompaction.commitKind()).thenReturn(Snapshot.CommitKind.COMPACT);
        Mockito.when(nonFullCompaction.commitIdentifier()).thenReturn(4L);
        Mockito.when(fullCompaction.commitKind()).thenReturn(Snapshot.CommitKind.COMPACT);
        Mockito.when(fullCompaction.commitIdentifier()).thenReturn(6L);
        Mockito.when(baseTable.copy(ArgumentMatchers.anyMap())).thenReturn(selectedTable);
        Mockito.when(selectedTable.coreOptions()).thenReturn(new CoreOptions(ImmutableMap.of(
                "changelog-producer", "full-compaction",
                "full-compaction.delta-commits", "3")));
        Mockito.when(selectedTable.snapshotManager()).thenReturn(snapshotManager);
        Mockito.when(snapshotManager.pickOrLatest(ArgumentMatchers.any())).thenAnswer(invocation -> {
            java.util.function.Predicate<Snapshot> selector = invocation.getArgument(0);
            Assert.assertFalse(selector.test(nonFullCompaction));
            Assert.assertTrue(selector.test(fullCompaction));
            return 17L;
        });

        Map<String, String> resolved = PaimonScanParams.resolveOptions(
                baseTable, ImmutableMap.of("scan.mode", "compacted-full"));

        Assert.assertEquals("17", resolved.get("scan.snapshot-id"));
    }

    @Test
    public void testPlanningOptionsCanBePinnedToStatementSnapshot() {
        Map<String, String> resolved = PaimonScanParams.pinOptionsToSnapshot(
                ImmutableMap.of("scan.manifest.parallelism", "1"), 7L);

        Assert.assertEquals("7", resolved.get("scan.snapshot-id"));
        Assert.assertEquals("1", resolved.get("scan.manifest.parallelism"));
    }

    private static boolean containsNull(Map<String, String> options, String key) {
        return options.containsKey(key) && options.get(key) == null;
    }
}
