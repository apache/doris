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

import org.apache.doris.connector.spi.ConnectorTableStatistics;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * Unit tests for FIX-TABLE-STATS: {@link PaimonConnectorMetadata#getTableStatistics}.
 *
 * <p>Before the fix the connector inherited the default {@code ConnectorStatisticsOps} (returns
 * {@code Optional.empty()}), so every paimon table — normal AND system — reported row count -1
 * (UNKNOWN), degrading the Nereids cost model (join-reorder force-disabled) and SHOW/info_schema.
 * The fix overrides it to sum {@code split.rowCount()} via the {@code PaimonCatalogOps.rowCount}
 * seam (faked here — {@code FakePaimonTable.newReadBuilder()} throws, the whole reason for the
 * seam). Each test FAILS before the fix (default empty) and PASSES after, and encodes WHY.
 */
public class PaimonConnectorMetadataStatisticsTest {

    private static PaimonConnectorMetadata metadataWith(RecordingPaimonCatalogOps ops) {
        return new PaimonConnectorMetadata(ops, PaimonCatalogProperties.of(Collections.emptyMap()), new RecordingConnectorContext());
    }

    private static PaimonConnectorMetadata metadataWith(
            RecordingPaimonCatalogOps ops, RecordingConnectorContext context) {
        return new PaimonConnectorMetadata(ops, PaimonCatalogProperties.of(Collections.emptyMap()), context);
    }

    private static RowType rowType(String... columnNames) {
        RowType.Builder builder = RowType.builder();
        for (String name : columnNames) {
            builder.field(name, DataTypes.INT());
        }
        return builder.build();
    }

    private static FileStoreTable fileStoreTable(String name, Map<String, String> options) {
        TableSchema schema = new TableSchema(
                0,
                Collections.singletonList(new DataField(0, "id", new IntType())),
                0,
                Collections.emptyList(),
                Collections.emptyList(),
                options,
                null);
        return new AppendOnlyFileStoreTable(
                LocalFileIO.create(), new Path("memory://" + name), schema, CatalogEnvironment.empty());
    }

    @Test
    public void positiveRowCountReturnedAsStatistics() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.rowCount = 42;
        FakePaimonTable fake = new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList());
        ops.table = fake;
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        handle.setPaimonTable(fake);

        Optional<ConnectorTableStatistics> stats = metadataWith(ops).getTableStatistics(null, handle);

        // WHY: a real positive count must reach the FE cost model (not -1), else
        // StatsCalculator force-disables join-reorder for the whole query. dataSize stays UNKNOWN(-1)
        // (legacy computed no base-table dataSize here). Asserting lastRowCountTable == fake proves
        // the metadata layer planned the RESOLVED table the handle denotes, not some other handle.
        // MUTATION: inheriting the default empty -> not present -> red.
        Assertions.assertTrue(stats.isPresent(), "a positive row count must be reported, not UNKNOWN");
        Assertions.assertEquals(42L, stats.get().getRowCount());
        Assertions.assertEquals(-1L, stats.get().getDataSize());
        Assertions.assertTrue(ops.log.contains("rowCount"), "the row-count seam must be invoked");
        Assertions.assertSame(fake, ops.lastRowCountTable,
                "stats must be computed from the table the handle resolves to");
    }

    @Test
    public void zeroRowCountMapsToUnknownEmpty() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.rowCount = 0;
        FakePaimonTable fake = new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList());
        ops.table = fake;
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        handle.setPaimonTable(fake);

        // WHY: legacy mapped 0 -> UNKNOWN(-1) (rowCount > 0 ? rowCount : UNKNOWN); the FE treats a
        // present 0 as a real cardinality, which would corrupt cost estimates. So 0 MUST surface as
        // empty, not (0,-1). MUTATION: dropping the >0 gate (returning (0,-1)) -> present -> red.
        Assertions.assertFalse(metadataWith(ops).getTableStatistics(null, handle).isPresent(),
                "0 rows must map to UNKNOWN (empty), matching legacy");
    }

    @Test
    public void planningFailureReturnsEmptyNotThrow() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.throwOnRowCount = true;
        FakePaimonTable fake = new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList());
        ops.table = fake;
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        handle.setPaimonTable(fake);

        // WHY: stats collection is best-effort (runs in background analysis / SHOW paths); a transient
        // remote planning failure must NOT propagate as a query-killing exception — it must degrade to
        // UNKNOWN(-1). This is the deliberate divergence from legacy's propagate-up behavior.
        // MUTATION: letting the exception propagate -> the assertDoesNotThrow fails -> red.
        Optional<ConnectorTableStatistics> stats = Assertions.assertDoesNotThrow(
                () -> metadataWith(ops).getTableStatistics(null, handle));
        Assertions.assertFalse(stats.isPresent(), "a planning failure must degrade to UNKNOWN, not throw");
    }

    @Test
    public void unsafeReaderOptionsAreRejectedBeforeRowCountPlanning() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable fake = new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList());
        fake.setOptions(Collections.singletonMap("scan.manifest.parallelism", "0"));
        ops.table = fake;
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        handle.setPaimonTable(fake);

        Optional<ConnectorTableStatistics> stats = metadataWith(ops).getTableStatistics(null, handle);

        Assertions.assertFalse(stats.isPresent());
        Assertions.assertFalse(ops.log.contains("rowCount"),
                "unsafe manifest settings must fail before row-count planning starts");
    }

    @Test
    public void acceptedManifestParallelismIsRuntimeCappedBeforeRowCountPlanning() {
        int localCapacity = Runtime.getRuntime().availableProcessors();
        org.junit.jupiter.api.Assumptions.assumeTrue(
                localCapacity < PaimonReaderOptions.MAX_MANIFEST_PARALLELISM);
        FileStoreTable table = fileStoreTable("t1", Collections.singletonMap(
                "scan.manifest.parallelism", String.valueOf(localCapacity + 1)));
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.table = table;
        ops.rowCount = 9;
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        handle.setPaimonTable(table);

        Optional<ConnectorTableStatistics> stats = metadataWith(ops).getTableStatistics(null, handle);

        // Catalog validation is hardware-neutral, but row-count planning executes locally and must
        // observe the same CPU cap as scan planning before touching Paimon's global manifest pool.
        Assertions.assertTrue(stats.isPresent());
        Assertions.assertEquals(String.valueOf(localCapacity),
                ops.lastRowCountTable.options().get("scan.manifest.parallelism"));
    }

    @Test
    public void snapshotStatisticsApplyRuntimeCapBeforeRowCountPlanning() {
        int localCapacity = Runtime.getRuntime().availableProcessors();
        org.junit.jupiter.api.Assumptions.assumeTrue(
                localCapacity < PaimonReaderOptions.MAX_MANIFEST_PARALLELISM);
        FileStoreTable table = fileStoreTable("t1_snapshot", Collections.singletonMap(
                "scan.manifest.parallelism", String.valueOf(localCapacity + 1)));
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.table = table;
        ops.rowCount = 11;
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        handle.setPaimonTable(table);
        ConnectorMvccSnapshot snapshot = ConnectorMvccSnapshot.builder()
                .snapshotId(7L)
                .property("scan.snapshot-id", "7")
                .build();

        Optional<ConnectorTableStatistics> stats =
                metadataWith(ops).getTableStatistics(null, handle, snapshot);

        // Non-OPTIONS pins copy the snapshot selector first, so the runtime cap must run again on
        // that resulting table rather than assuming relation option validation already handled it.
        Assertions.assertTrue(stats.isPresent());
        Assertions.assertEquals(String.valueOf(localCapacity),
                ops.lastRowCountTable.options().get("scan.manifest.parallelism"));
    }

    @Test
    public void pinnedEmptySnapshotStatisticsDoNotReopenLatestTable() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.rowCount = 17;
        FakePaimonTable table = new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList());
        ops.table = table;
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        handle.setPaimonTable(table);
        ConnectorMvccSnapshot emptyFence = ConnectorMvccSnapshot.builder()
                .snapshotId(-1L)
                .properties(PaimonScanParams.pinOptionsToSnapshot(Collections.emptyMap(), -1L))
                .build();

        Optional<ConnectorTableStatistics> stats =
                metadataWith(ops).getTableStatistics(null, handle, emptyFence);

        Assertions.assertFalse(stats.isPresent());
        Assertions.assertFalse(ops.log.contains("rowCount"),
                "a pinned-empty statement must not count rows committed after its fence");
    }

    @Test
    public void systemTableUsesResolvedSysTable() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.rowCount = 7;
        FakePaimonTable sysFake = new FakePaimonTable(
                "t1$snapshots", rowType("snapshot_id"), Collections.emptyList(), Collections.emptyList());
        FakePaimonTable dataFake = new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList());
        ops.table = dataFake;
        ops.sysTable = sysFake;
        PaimonTableHandle sysHandle = PaimonTableHandle.forSystemTable("db1", "t1", "snapshots", false);
        sysHandle.setPaimonTable(sysFake);

        Optional<ConnectorTableStatistics> stats = metadataWith(ops).getTableStatistics(null, sysHandle);

        // WHY: PluginDrivenSysExternalTable inherits the same fetchRowCount, and resolveTable is
        // sys-aware, so the single override must serve system tables too (closes Finding 5.1). A
        // future refactor that special-cased only normal tables would leave sys tables at -1.
        // MUTATION: not handling sys handles / planning the wrong table -> rowCount!=7 or wrong table -> red.
        Assertions.assertTrue(stats.isPresent());
        Assertions.assertEquals(7L, stats.get().getRowCount());
        Assertions.assertSame(sysFake, ops.lastRowCountTable,
                "a sys handle must plan its OWN synthetic table's splits");
    }

    @Test
    public void systemTableStatisticsValidatePinnedSourceGeneration() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.rowCount = 7;
        FakePaimonTable pinnedSource = new FakePaimonTable(
                "pinned", rowType("id"), Collections.emptyList(), Collections.emptyList());
        pinnedSource.setOptions(Collections.singletonMap("scan.manifest.parallelism", "0"));
        FakePaimonTable reloadedSource = new FakePaimonTable(
                "reloaded", rowType("id"), Collections.emptyList(), Collections.emptyList());
        reloadedSource.setOptions(Collections.singletonMap("scan.manifest.parallelism", "1"));
        ops.sysTable = new FakePaimonTable(
                "t1$snapshots", rowType("snapshot_id"), Collections.emptyList(), Collections.emptyList());
        PaimonTableHandle base = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        base.setPaimonTable(pinnedSource);
        PaimonTableHandle system = (PaimonTableHandle) metadataWith(ops)
                .getSysTableHandle(null, base, "snapshots").orElseThrow(AssertionError::new);
        ops.table = reloadedSource;

        Optional<ConnectorTableStatistics> stats = metadataWith(ops).getTableStatistics(null, system);

        Assertions.assertFalse(stats.isPresent());
        Assertions.assertFalse(ops.log.contains("rowCount"),
                "statistics must reject the pinned unsafe source before planning the system wrapper");
    }

    @Test
    public void transientFreeSystemStatisticsReloadSourceInsideAuthenticator() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.rowCount = 7;
        ops.table = new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList());
        ops.sysTable = new FakePaimonTable(
                "t1$snapshots", rowType("snapshot_id"), Collections.emptyList(), Collections.emptyList());
        RecordingConnectorContext context = new RecordingConnectorContext();
        context.failAuthOnInvocation = 2;
        PaimonTableHandle handle = PaimonTableHandle.forSystemTable(
                "db1", "t1", "snapshots", false);

        Optional<ConnectorTableStatistics> stats =
                metadataWith(ops, context).getTableStatistics(null, handle);

        Assertions.assertFalse(stats.isPresent(),
                "an authenticated source reload failure must degrade best-effort statistics to UNKNOWN");
        Assertions.assertEquals(2, context.authCount);
        Assertions.assertFalse(ops.log.contains("rowCount"),
                "statistics planning must not continue after source authentication fails");
    }
}
