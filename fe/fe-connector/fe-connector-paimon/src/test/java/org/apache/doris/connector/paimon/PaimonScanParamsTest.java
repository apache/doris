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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.DorisConnectorException;

import com.google.common.collect.ImmutableMap;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Ported from upstream {@code PaimonScanParamsTest} (#65984). Two adaptations, both forced by this
 * module's conventions rather than by the behavior under test:
 *
 * <ul>
 *   <li>the connector throws {@link DorisConnectorException} where fe-core threw
 *       {@code IllegalArgumentException} (it cannot import fe-core's exception types);</li>
 *   <li>no connector module carries mockito, and this one already proves its scan behavior against REAL
 *       local-filesystem paimon tables ({@code PaimonScanPlanProviderTest},
 *       {@code PaimonTableSerdeRoundTripTest}). The resolution tests therefore run against a real table
 *       with real snapshots instead of a mocked {@code FileStoreTable} plus a static-mocked
 *       {@code TimeTravelUtil} — a stronger assertion, since it exercises the SDK's own resolution
 *       rather than a stub of it.</li>
 * </ul>
 *
 * <p>Upstream's {@code testMutableSelectorResolvesOnlyOncePerRelation} has no counterpart here: it pinned
 * {@code TableScanParams.getOrResolveMapParams}, an fe-core cache. In this architecture the
 * resolve-once-per-relation property comes from {@code StatementContext}'s version-keyed snapshot memo
 * (the key includes the {@code @options} map, so two relations with different options resolve separately
 * and two with the same options share one resolution), and is covered on the fe-core side.
 */
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
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> PaimonScanParams.validateOptions(options));
        }
    }

    @Test
    public void testPlanningOptionsDoNotReuseMetadataProjection() {
        Assertions.assertTrue(PaimonScanParams.hasOnlyReaderOptions(ImmutableMap.of(
                "file-index.read.enabled", "false",
                "source.split.target-size", "64 MB")));
        Assertions.assertFalse(PaimonScanParams.hasOnlyReaderOptions(
                ImmutableMap.of("scan.manifest.parallelism", "1")));
        Assertions.assertFalse(PaimonScanParams.hasOnlyReaderOptions(
                ImmutableMap.of("scan.plan-sort-partition", "true")));
    }

    @Test
    public void testManifestParallelismCannotMutateGlobalPoolCapacity() {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        int validLocalValue = Math.min(
                availableProcessors, PaimonReaderOptions.MAX_MANIFEST_PARALLELISM);
        PaimonScanParams.validateOptions(ImmutableMap.of(
                "scan.manifest.parallelism", String.valueOf(validLocalValue)));

        for (int invalid : new int[] {0, -1, PaimonReaderOptions.MAX_MANIFEST_PARALLELISM + 1}) {
            IllegalArgumentException exception = Assertions.assertThrows(
                    IllegalArgumentException.class,
                    () -> PaimonScanParams.validateOptions(ImmutableMap.of(
                            "scan.manifest.parallelism", String.valueOf(invalid))));
            Assertions.assertTrue(exception.getMessage().contains("scan.manifest.parallelism"));
        }
        if (availableProcessors < PaimonReaderOptions.MAX_MANIFEST_PARALLELISM) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> PaimonScanParams.validateOptions(ImmutableMap.of(
                            "scan.manifest.parallelism", String.valueOf(availableProcessors + 1))));
        }
    }

    @Test
    public void testRelationReaderOptionsAreAppliedWithoutMutatingBaseTable() {
        FakePaimonTable table = fakeTable();
        Table copied = fakeTable();
        table.copyResult = copied;

        Assertions.assertSame(copied, PaimonScanParams.applyOptions(table, ImmutableMap.of(
                "read.batch-size", "8192",
                "file-reader-async-threshold", "32 MB")));
        Assertions.assertEquals(ImmutableMap.of(
                "read.batch-size", "8192",
                "file-reader-async-threshold", "32 MB"), table.lastCopyOptions);
    }

    @Test
    public void testRejectUnknownAndConflictingOptions() {
        DorisConnectorException typo = Assertions.assertThrows(
                DorisConnectorException.class,
                () -> PaimonScanParams.validateOptions(
                        ImmutableMap.of("scan.snapsh0t-id", "1")));
        Assertions.assertTrue(typo.getMessage().contains("scan.snapsh0t-id"));

        DorisConnectorException conflict = Assertions.assertThrows(
                DorisConnectorException.class,
                () -> PaimonScanParams.validateOptions(ImmutableMap.of(
                        "scan.snapshot-id", "1",
                        "scan.tag-name", "tag1")));
        Assertions.assertTrue(conflict.getMessage().contains("Only one"));
    }

    @Test
    public void testRejectIncompatibleStartupOptionsAndFallbackBranch() {
        Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonScanParams.validateOptions(ImmutableMap.of(
                        "scan.snapshot-id", "1",
                        "scan.creation-time-millis", "1000")));
        Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonScanParams.validateOptions(ImmutableMap.of(
                        "scan.mode", "latest",
                        "scan.snapshot-id", "1")));
        DorisConnectorException fallback = Assertions.assertThrows(
                DorisConnectorException.class,
                () -> PaimonScanParams.validateOptions(
                        ImmutableMap.of("scan.fallback-branch", "archive")));
        Assertions.assertTrue(fallback.getMessage().contains("scan.fallback-branch"));
    }

    @Test
    public void testRejectMissingCreationTimeAndNonBatchOptions() {
        DorisConnectorException missingTime = Assertions.assertThrows(
                DorisConnectorException.class,
                () -> PaimonScanParams.validateOptions(
                        ImmutableMap.of("scan.mode", "from-creation-timestamp")));
        Assertions.assertTrue(missingTime.getMessage().contains("scan.creation-time-millis"));

        for (String option : new String[] {"scan.bounded.watermark", "scan.max-splits-per-task"}) {
            DorisConnectorException unsupported = Assertions.assertThrows(
                    DorisConnectorException.class,
                    () -> PaimonScanParams.validateOptions(ImmutableMap.of(option, "1")));
            Assertions.assertTrue(unsupported.getMessage().contains(option));
        }
    }

    @Test
    public void testPinOptionsValidatesBeforeClearingInheritedState() {
        DorisConnectorException unsupported = Assertions.assertThrows(
                DorisConnectorException.class,
                () -> PaimonScanParams.pinOptionsToSnapshot(
                        ImmutableMap.of("scan.bounded.watermark", "1"), 10));
        Assertions.assertTrue(unsupported.getMessage().contains("scan.bounded.watermark"));
    }

    @Test
    public void testApplyPositionClearsInheritedStartupState() {
        FakePaimonTable table = fakeTable();
        Table copied = fakeTable();
        table.copyResult = copied;

        Assertions.assertSame(copied, PaimonScanParams.applyOptions(
                table, ImmutableMap.of("scan.creation-time-millis", "1000")));

        Map<String, String> applied = table.lastCopyOptions;
        Assertions.assertEquals("1000", applied.get("scan.creation-time-millis"));
        // WHY: paimon treats startup mode, position and range as ONE inherited state family. A member the
        // relation did NOT name must be nulled out, or a value persisted on the base table (ALTER TABLE SET,
        // TBLPROPERTIES, table-default.*) silently co-decides which snapshot this relation reads.
        // MUTATION: dropping the INHERITED_READ_STATE_KEYS null pass -> every assertion below reds.
        for (String cleared : new String[] {
                "scan.mode", "scan.snapshot-id", "scan.tag-name", "scan.timestamp", "scan.timestamp-millis",
                "scan.watermark", "scan.version", "scan.file-creation-time-millis", "scan.bounded.watermark",
                "incremental-between", "incremental-between-timestamp", "incremental-between-scan-mode",
                "incremental-to-auto-tag"}) {
            Assertions.assertTrue(containsNull(applied, cleared), cleared + " must be cleared");
        }
    }

    @Test
    public void testApplyModeClearsInheritedPositions() {
        FakePaimonTable table = fakeTable();

        PaimonScanParams.applyOptions(table, ImmutableMap.of("scan.mode", "latest"));

        Map<String, String> applied = table.lastCopyOptions;
        Assertions.assertEquals("latest", applied.get("scan.mode"));
        for (String cleared : new String[] {
                "scan.snapshot-id", "scan.tag-name", "scan.timestamp", "scan.timestamp-millis",
                "scan.watermark", "scan.version", "scan.file-creation-time-millis",
                "scan.creation-time-millis"}) {
            Assertions.assertTrue(containsNull(applied, cleared), cleared + " must be cleared");
        }
    }

    @Test
    public void testIsolationClearsFallbackReadStateKeys() {
        FakePaimonTable table = fakeTable();

        PaimonScanParams.applyOptions(table, ImmutableMap.of("scan.snapshot-id", "1"));

        // The family covers each option's FALLBACK keys too, not just its canonical key.
        Assertions.assertTrue(containsNull(table.lastCopyOptions, "log.scan"));
        Assertions.assertTrue(containsNull(table.lastCopyOptions, "log.scan.timestamp-millis"));

        Map<String, String> incremental = PaimonScanParams.isolateIncrementalRead(
                ImmutableMap.of("incremental-between", "1,2"));
        Assertions.assertTrue(containsNull(incremental, "log.scan"));
        Assertions.assertTrue(containsNull(incremental, "log.scan.timestamp-millis"));
    }

    @Test
    public void testInternalMarkersNeverReachThePaimonSdk() {
        FakePaimonTable table = fakeTable();

        // WHY: a resolved map may carry doris.internal.paimon.* markers (empty-scan, file-creation-time,
        // the options-family marker). They are Doris bookkeeping; handing them to Table.copy would push
        // unknown keys into paimon's Options. MUTATION: dropping userOptions() -> the assertion below reds.
        PaimonScanParams.applyOptions(table, PaimonScanParams.markAsOptions(
                ImmutableMap.of("scan.snapshot-id", "1")));

        Assertions.assertEquals("1", table.lastCopyOptions.get("scan.snapshot-id"));
        Assertions.assertTrue(table.lastCopyOptions.keySet().stream()
                        .noneMatch(key -> key.startsWith("doris.internal.paimon.")),
                "no Doris-internal marker may reach Table.copy");
    }

    @Test
    public void testOptionsPinMarkerDistinguishesTheOptionsFamily() {
        // The handle's scan-option map is shared by time-travel, @incr and @options pins, but only the
        // @options family gets applyOptions' read-state isolation, so the family must be recoverable.
        Assertions.assertFalse(PaimonScanParams.isOptionsPin(ImmutableMap.of("scan.snapshot-id", "1")));
        Assertions.assertFalse(PaimonScanParams.isOptionsPin(null));
        Assertions.assertTrue(PaimonScanParams.isOptionsPin(
                PaimonScanParams.markAsOptions(ImmutableMap.of("scan.snapshot-id", "1"))));
    }

    @Test
    public void testSystemTableCapabilityMatrixIncludesRangeAwareReaders() {
        Assertions.assertFalse(PaimonScanParams.supportsIncrementalRead("files"));
        for (String type : new String[] {"partitions", "ro"}) {
            Assertions.assertTrue(PaimonScanParams.supportsIncrementalRead(type), type);
            Assertions.assertFalse(PaimonScanParams.requiresPaimonReader(type), type);
        }
        Assertions.assertFalse(PaimonScanParams.supportsOptions("buckets"));
        Assertions.assertFalse(PaimonScanParams.supportsOptions("files"));
        Assertions.assertTrue(PaimonScanParams.supportsOptions("table_indexes"));
        Assertions.assertTrue(PaimonScanParams.requiresPaimonReader("audit_log"));

        // The residual per-option check no system table can satisfy: a creation-time file filter is a
        // manifest-entry predicate, and the generic system-table wrapper has nowhere to carry it. Dropping
        // it would widen the read to the whole pinned snapshot and return rows the user excluded.
        PaimonScanParams.validateSystemTableOptions(ImmutableMap.of("scan.snapshot-id", "1"));
        Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonScanParams.validateSystemTableOptions(
                        ImmutableMap.of("scan.file-creation-time-millis", "1234")));
    }

    @Test
    public void testSchemaSelectingOptionsRequirePinnedReaderSchema() {
        Assertions.assertTrue(PaimonScanParams.selectsSchema(
                ImmutableMap.of("scan.snapshot-id", "1")));
        Assertions.assertTrue(PaimonScanParams.selectsSchema(
                ImmutableMap.of("scan.mode", "latest")));
        Assertions.assertFalse(PaimonScanParams.selectsSchema(
                ImmutableMap.of("scan.plan-sort-partition", "true")));
    }

    @Test
    public void testExplicitSnapshotSelectorIsAlreadyImmutable(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = localCatalog(warehouse)) {
            Table table = tableWithSnapshots(catalog, 3);

            Map<String, String> resolved = PaimonScanParams.resolveOptions(
                    table, ImmutableMap.of("scan.snapshot-id", "2"));

            Assertions.assertEquals("2", resolved.get("scan.snapshot-id"));
            Assertions.assertFalse(PaimonScanParams.isPinnedEmptyScan(resolved));
        }
    }

    @Test
    public void testMutableModeSelectorIsPinnedToAConcreteSnapshot(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = localCatalog(warehouse)) {
            Table table = tableWithSnapshots(catalog, 3);

            // WHY: 'latest' is a MOVING target. Binding and split planning happen at different instants, so
            // the selector must be frozen to the snapshot chosen at bind time -- otherwise a commit landing
            // in between makes the scan read a version whose schema was never bound.
            // MUTATION: returning the user map unchanged -> scan.mode survives and scan.snapshot-id is
            // absent, reddening both assertions.
            Map<String, String> resolved = PaimonScanParams.resolveOptions(
                    table, ImmutableMap.of("scan.mode", "latest"));

            Assertions.assertEquals("3", resolved.get("scan.snapshot-id"));
            Assertions.assertFalse(resolved.containsKey("scan.mode"));
        }
    }

    @Test
    public void testTagSelectorRetainsTagMetadataInsteadOfAnExpirableSnapshotId(@TempDir Path warehouse)
            throws Exception {
        try (Catalog catalog = localCatalog(warehouse)) {
            Table table = tableWithSnapshots(catalog, 2);
            table.createTag("retained_tag", 1L);

            // WHY: a tag owns a retained Snapshot copy that survives ordinary snapshot expiry. Rewriting the
            // tag to scan.snapshot-id would send planning down the expirable snapshot path.
            Map<String, String> resolved = PaimonScanParams.resolveOptions(
                    table, ImmutableMap.of("scan.tag-name", "retained_tag"));

            Assertions.assertEquals("retained_tag", resolved.get("scan.tag-name"));
            Assertions.assertFalse(resolved.containsKey("scan.snapshot-id"));
        }
    }

    @Test
    public void testTagValuedVersionRetainsCanonicalTagMetadata(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = localCatalog(warehouse)) {
            Table table = tableWithSnapshots(catalog, 2);
            table.createTag("canonical_tag", 1L);

            // scan.version accepts either a snapshot id or a tag name; paimon normalizes a tag-valued one
            // while copying the selected table, and the resolution must keep that canonical form.
            Map<String, String> resolved = PaimonScanParams.resolveOptions(
                    table, ImmutableMap.of("scan.version", "canonical_tag"));

            Assertions.assertEquals("canonical_tag", resolved.get("scan.tag-name"));
            Assertions.assertFalse(resolved.containsKey("scan.version"));
        }
    }

    @Test
    public void testFileCreationTimePinsLatestSnapshotAndKeepsFilter(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = localCatalog(warehouse)) {
            Table table = tableWithSnapshots(catalog, 2);

            // WHY: paimon's file-creation scanner consults LATEST lazily, re-racing the version this relation
            // was bound at. The resolution replaces the live lookup with a fixed snapshot and keeps the
            // threshold as an internal marker for the split planner's manifest-entry filter.
            Map<String, String> resolved = PaimonScanParams.resolveOptions(
                    table, ImmutableMap.of("scan.file-creation-time-millis", "1234"));

            Assertions.assertEquals("2", resolved.get("scan.snapshot-id"));
            Assertions.assertEquals(Long.valueOf(1234L),
                    PaimonScanParams.getPinnedFileCreationTime(resolved).orElse(null));
            Assertions.assertFalse(resolved.containsKey("scan.file-creation-time-millis"));
        }
    }

    @Test
    public void testModeOnlyLatestPinsEmptyStatementState(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = localCatalog(warehouse)) {
            Table table = tableWithSnapshots(catalog, 0);

            // WHY: "this table is empty" is itself statement state. Recording it explicitly stops a commit
            // landing between binding and split planning from turning the relation into a non-empty scan.
            // MUTATION: returning the options unchanged -> isPinnedEmptyScan is false and the split planner
            // re-derives emptiness at scan time.
            Map<String, String> resolved = PaimonScanParams.resolveOptions(
                    table, ImmutableMap.of("scan.mode", "latest"));

            Assertions.assertTrue(PaimonScanParams.isPinnedEmptyScan(resolved));
            Assertions.assertFalse(resolved.containsKey("scan.mode"));
        }
    }

    @Test
    public void testCompactedFullResolvesToAConcreteStatementState(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = localCatalog(warehouse)) {
            Table table = tableWithSnapshots(catalog, 3);

            // compacted-full means the newest COMPACT snapshot, which can be older than latest -- so like
            // every other mutable selector it must leave the resolution as concrete statement state, never
            // as a mode the scan phase would re-evaluate.
            Map<String, String> resolved = PaimonScanParams.resolveOptions(
                    table, ImmutableMap.of("scan.mode", "compacted-full"));

            Assertions.assertTrue(
                    PaimonScanParams.isPinnedEmptyScan(resolved)
                            || resolved.get("scan.snapshot-id") != null,
                    "compacted-full must resolve to a concrete snapshot or to the empty-scan pin");
            Assertions.assertFalse(resolved.containsKey("scan.mode"));
        }
    }

    private static boolean containsNull(Map<String, String> options, String key) {
        return options.containsKey(key) && options.get(key) == null;
    }

    private static FakePaimonTable fakeTable() {
        return new FakePaimonTable("t",
                RowType.of(DataTypes.INT()),
                Collections.emptyList(),
                Collections.emptyList());
    }

    private static Catalog localCatalog(Path warehouse) {
        return new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()));
    }

    /** A real local table carrying {@code snapshots} committed snapshots (one row each). */
    private static Table tableWithSnapshots(Catalog catalog, int snapshots) throws Exception {
        catalog.createDatabase("db", false);
        Identifier id = Identifier.create("db", "t");
        catalog.createTable(id, Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("val", DataTypes.BIGINT())
                .primaryKey("id")
                .option("bucket", "1")
                .build(), false);
        Table table = catalog.getTable(id);
        for (int i = 1; i <= snapshots; i++) {
            BatchWriteBuilder wb = table.newBatchWriteBuilder();
            try (BatchTableWrite write = wb.newWrite()) {
                write.write(GenericRow.of(i, (long) i * 100));
                List<CommitMessage> messages = write.prepareCommit();
                try (BatchTableCommit commit = wb.newCommit()) {
                    commit.commit(messages);
                }
            }
        }
        return catalog.getTable(id);
    }
}
