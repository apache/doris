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

import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

/**
 * Tests for {@link PaimonScanPlanProvider#resolveTable}, pinning the transient-Table reload
 * fallback on the scan path (P5-T06). The scan path reads the handle's transient Paimon
 * {@link Table}, which becomes null after any Java serialization round-trip (cross-node /
 * plan-reuse); the reload mirrors the proven fallback in
 * {@link PaimonConnectorMetadata#getColumnHandles}.
 *
 * <p>Driven directly against {@code resolveTable} (package-private) rather than {@code planScan}
 * end-to-end: {@link FakePaimonTable#newReadBuilder()} throws, so the full scan cannot be driven
 * offline. The seam fully covers the remote {@code getTable} call, so each test uses a
 * {@link RecordingPaimonCatalogOps} fake and a {@code null} real catalog — entirely offline.
 */
public class PaimonScanPlanProviderTest {

    private static RowType rowType(String... columnNames) {
        RowType.Builder builder = RowType.builder();
        for (String name : columnNames) {
            builder.field(name, DataTypes.INT());
        }
        return builder.build();
    }

    @Test
    public void resolveTableReloadsWhenTransientTableNull() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        Table reloaded = new FakePaimonTable(
                "t1",
                rowType("id", "name"),
                Collections.emptyList(),
                Collections.emptyList());
        ops.table = reloaded;
        // A handle whose transient Table is null (e.g. after serialization across the FE/BE
        // boundary or plan reuse) — the scan path must reload via the seam rather than NPE.
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        Assertions.assertNull(handle.getPaimonTable(), "precondition: transient table is null");

        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(Collections.emptyMap(), ops);
        Table table = provider.resolveTable(handle);

        // WHY: this is the serde-survival safety net. With a null transient Table, the scan path's
        // only way to read rowType()/serialize the table for BE is to re-fetch it from the catalog
        // seam. MUTATION: removing the `if (table == null) { table = catalogOps.getTable(id); }`
        // block -> returns null -> downstream NPE on table.rowType() -> red. The recorded getTable
        // call proves the reload happened.
        Assertions.assertSame(reloaded, table,
                "scan path must return the table reloaded from the seam when the transient ref is null");
        Assertions.assertTrue(ops.log.contains("getTable:db1.t1"),
                "reload-fallback must re-fetch the table from the seam when the transient ref is null");
    }

    @Test
    public void resolveTableForSysHandleReloadsViaFourArgSysIdentifier() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        // Base table (served for a 2-arg Identifier) has DIFFERENT columns than the sys table, so a
        // wrong-Identifier reload (base table) is detectable by the captured Identifier's sys name.
        ops.table = new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList());
        ops.sysTable = new FakePaimonTable(
                "t1$snapshots", rowType("snapshot_id", "schema_id"),
                Collections.emptyList(), Collections.emptyList());

        // A deserialized SYSTEM handle: sysTableName set, transient Table lost (null) — exactly the
        // FE/BE serialization or plan-reuse case the scan path must survive.
        PaimonTableHandle sysHandle = PaimonTableHandle.forSystemTable(
                "db1", "t1", "snapshots", false);
        Assertions.assertNull(sysHandle.getPaimonTable(), "precondition: transient table is null");

        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(Collections.emptyMap(), ops);
        Table resolved = provider.resolveTable(sysHandle);

        // WHY: BLOCKER fix — the scan path's own resolveTable used to ALWAYS reload via the 2-arg
        // base Identifier, so a deserialized sys handle would silently resolve and scan the BASE
        // table (wrong rows) instead of the system table. The reload must be sys-aware (4-arg sys
        // Identifier), mirroring the metadata side, via the single shared PaimonTableResolver.
        // MUTATION: reverting the scan resolveTable to Identifier.create(db,table) -> the base table
        // is returned, the captured Identifier's sys name is null -> red.
        Assertions.assertSame(ops.sysTable, resolved,
                "scan path must reload the SYSTEM table (not the base table) for a sys handle");
        Assertions.assertNotNull(ops.lastGetTableId, "reload must have hit the seam");
        Assertions.assertEquals("snapshots", ops.lastGetTableId.getSystemTableName(),
                "the scan reload must use the 4-arg sys Identifier carrying the sys-table name");
        Assertions.assertEquals("main", ops.lastGetTableId.getBranchName(),
                "the sys Identifier branch must be hardcoded 'main' (legacy parity)");
    }

    /** Builds a native-eligible RawFile (parquet suffix). The numeric fields are irrelevant to the
     * native-vs-JNI routing decision under test, only the path suffix matters. */
    private static RawFile parquetRawFile(String path) {
        return new RawFile(path, 0L, 100L, 100L, "parquet", 0L, 0L);
    }

    @Test
    public void forceJniSysTableSplitDoesNotTakeNativePathEvenWithRawFiles() {
        // A binlog/audit_log sys handle: forceJni=true. Its DataSplit WOULD support native (raw
        // parquet files present), but the binlog/audit_log read semantics (pack/merge, rowkind/
        // sequence-number projection) are not reproducible by the native ORC/Parquet reader.
        Optional<java.util.List<RawFile>> rawFiles = Optional.of(
                Arrays.asList(parquetRawFile("/data/part-0.parquet")));

        // WHY: legacy forces binlog/audit_log to JNI (PaimonScanNode.shouldForceJniForSystemTable,
        // captured as handle.isForceJni()). Without the gate the native path would silently return
        // wrong rows. MUTATION: dropping the `!forceJni` guard in shouldUseNativeReader ->
        // returns true here (native) -> red.
        Assertions.assertFalse(
                PaimonScanPlanProvider.shouldUseNativeReader(/*forceJni*/ true, rawFiles),
                "a forceJni (binlog/audit_log) sys split must route to JNI, never native, "
                        + "even when its raw files would otherwise support the native reader");
    }

    @Test
    public void nonForcedSplitWithRawFilesStillTakesNativePath() {
        // A normal table (or a non-forced DataTable sys table like "ro"): forceJni=false. With raw
        // files that support the native reader, it must still be allowed the native path.
        Optional<java.util.List<RawFile>> rawFiles = Optional.of(
                Arrays.asList(parquetRawFile("/data/part-0.parquet")));

        // WHY: the gate must be the forceJni flag ONLY — over-forcing JNI for non-forced splits
        // would regress the native fast path for normal tables and "ro". MUTATION: gating native on
        // anything stricter (e.g. isSystemTable) -> returns false here -> red.
        Assertions.assertTrue(
                PaimonScanPlanProvider.shouldUseNativeReader(/*forceJni*/ false, rawFiles),
                "a non-forced split with native-eligible raw files must still take the native path");
    }

    @Test
    public void nonForcedSplitWithoutNativeFilesTakesJni() {
        // Sanity: even when not forced, a split whose raw files are absent must not go native.
        // MUTATION: making shouldUseNativeReader ignore supportNativeReader -> returns true -> red.
        Assertions.assertFalse(
                PaimonScanPlanProvider.shouldUseNativeReader(/*forceJni*/ false, Optional.empty()),
                "a split without convertible raw files must route to JNI regardless of forceJni");
    }

    @Test
    public void resolveScanTableAppliesSnapshotPinViaCopy() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable base = new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList());
        // The pinned (copied) table is a DISTINCT instance so we can prove the scan uses the COPY,
        // not the un-pinned base.
        FakePaimonTable pinned = new FakePaimonTable(
                "t1@5", rowType("id"), Collections.emptyList(), Collections.emptyList());
        base.copyResult = pinned;

        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        handle.setPaimonTable(base);
        // A snapshot-pinned handle: applySnapshot would have produced exactly this scanOptions map.
        PaimonTableHandle pinnedHandle = handle.withScanOptions(
                Collections.singletonMap("scan.snapshot-id", "5"));

        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(Collections.emptyMap(), ops);
        Table scanTable = provider.resolveScanTable(pinnedHandle);

        // WHY: a snapshot-pinned handle must read at the pinned version on BOTH the planned-splits
        // and the JNI serialized-table paths. The scan provider applies the pin by layering the
        // handle's scanOptions onto the resolved table via Table.copy(scanOptions). MUTATION:
        // skipping the copy (using the un-pinned resolveTable result) -> scanTable is `base`, not
        // `pinned`, and lastCopyOptions stays null -> red; passing the wrong options -> the
        // scan.snapshot-id assertion below -> red.
        Assertions.assertSame(pinned, scanTable,
                "the scan path must use the snapshot-pinned (copied) table, not the un-pinned base");
        Assertions.assertEquals(Collections.singletonMap("scan.snapshot-id", "5"),
                base.lastCopyOptions,
                "the scan path must layer the handle's scanOptions via Table.copy(scanOptions)");
    }

    @Test
    public void resolveScanTableWithoutScanOptionsDoesNotCopy() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable base = new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList());
        // A normal (un-pinned) handle: empty scanOptions.
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        handle.setPaimonTable(base);

        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(Collections.emptyMap(), ops);
        Table scanTable = provider.resolveScanTable(handle);

        // WHY: a normal read must NOT call Table.copy at all — copying with empty options is wasted
        // work and, more importantly, the un-pinned path must return the resolved table verbatim.
        // MUTATION: unconditionally calling copy(scanOptions) -> lastCopyOptions becomes non-null
        // (and FakePaimonTable.copy would be hit) -> red.
        Assertions.assertSame(base, scanTable,
                "an un-pinned handle must return the resolved table without a copy");
        Assertions.assertNull(base.lastCopyOptions,
                "an un-pinned handle must NOT invoke Table.copy");
    }

    @Test
    public void resolveTableUsesTransientWithoutReload() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = new FakePaimonTable(
                "t1",
                rowType("id", "name"),
                Collections.emptyList(),
                Collections.emptyList());
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        handle.setPaimonTable(table);

        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(Collections.emptyMap(), ops);
        Table resolved = provider.resolveTable(handle);

        // WHY: the fast path — when the transient Table is already present, resolveTable must use it
        // and NOT make a redundant remote getTable call. MUTATION: always reloading would record a
        // getTable entry -> red. This pins the reload as a fallback, not the default.
        Assertions.assertSame(table, resolved);
        Assertions.assertTrue(ops.log.isEmpty(),
                "with a present transient table, no remote getTable reload must happen");
    }
}
