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

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.BackendStorageKind;
import org.apache.doris.filesystem.properties.BackendStorageProperties;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.schema.external.TField;
import org.apache.doris.thrift.schema.external.TFieldPtr;
import org.apache.doris.thrift.schema.external.TSchema;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.system.ReadOptimizedTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    @Test
    public void resolveTableRunsInsideAuthenticatorWhenContextPresent() {
        // M-11 (D-052): with a real ConnectorContext the scan-path reload must run inside
        // executeAuthenticated, so the FE-injected Kerberos UGI applies. Under failAuth the wrapped
        // reload aborts BEFORE the getTable seam runs. MUTATION: an un-wrapped resolveTable would call
        // catalogOps.getTable directly -> "getTable:db1.t1" logged despite the auth failure -> red.
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.table = new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList());
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;
        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(Collections.emptyMap(), ops, ctx);
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());

        Assertions.assertThrows(RuntimeException.class, () -> provider.resolveTable(handle));
        Assertions.assertTrue(ops.log.isEmpty(),
                "auth failure must abort BEFORE the scan resolveTable getTable seam runs");
        Assertions.assertEquals(1, ctx.authCount);
    }

    @Test
    public void resolveTableEntersAuthenticatorOnHappyPath() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.table = new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList());
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(Collections.emptyMap(), ops, ctx);
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());

        provider.resolveTable(handle); // null transient -> reload via the wrapped seam
        Assertions.assertEquals(Collections.singletonList("getTable:db1.t1"), ops.log);
        Assertions.assertEquals(1, ctx.authCount);
    }

    @Test
    public void planScanEnumeratesSplitsInsideAuthScope(@TempDir Path warehouse) throws Exception {
        // scan.plan() reads paimon's snapshot/manifest files remotely, on the planning thread — on a
        // Kerberos filesystem catalog that read runs on the PLUGIN's UGI copy, which only the plugin doAs
        // logs in (iceberg fourth-locus parity; iceberg CI proof: SELECT after INSERT failing SASL at the
        // plan-time manifest read). So planScan must wrap the enumeration in executeAuthenticated IN
        // ADDITION to resolveTable's load wrap. MUTATION: dropping the planSplits wrap -> authCount stays
        // 1 (load only) -> red.
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "t");
            catalog.createTable(id, Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("val", DataTypes.BIGINT())
                    .primaryKey("id")
                    .option("bucket", "1")
                    .build(), false);
            Table table = catalog.getTable(id);
            BatchWriteBuilder wb = table.newBatchWriteBuilder();
            try (BatchTableWrite write = wb.newWrite()) {
                write.write(GenericRow.of(1, 100L));
                List<CommitMessage> messages = write.prepareCommit();
                try (BatchTableCommit commit = wb.newCommit()) {
                    commit.commit(messages);
                }
            }

            RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
            ops.table = table;
            RecordingConnectorContext ctx = new RecordingConnectorContext();
            PaimonScanPlanProvider provider = new PaimonScanPlanProvider(Collections.emptyMap(), ops, ctx);
            PaimonTableHandle handle = new PaimonTableHandle(
                    "db", "t", Collections.emptyList(), Collections.emptyList());

            List<ConnectorScanRange> ranges = provider.planScan(
                    sessionWithProps(Collections.emptyMap()), handle,
                    Collections.emptyList(), Optional.empty());

            Assertions.assertFalse(ranges.isEmpty(), "one committed row must plan at least one split");
            Assertions.assertEquals(2, ctx.authCount,
                    "planScan must run BOTH the table load (resolveTable) AND the split enumeration "
                            + "(scan.plan(), the remote manifest read) inside executeAuthenticated");
        }
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
                PaimonScanPlanProvider.shouldUseNativeReader(
                        /*forceJni*/ true, /*forceJniScanner*/ false, rawFiles),
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
                PaimonScanPlanProvider.shouldUseNativeReader(
                        /*forceJni*/ false, /*forceJniScanner*/ false, rawFiles),
                "a non-forced split with native-eligible raw files must still take the native path");
    }

    @Test
    public void nonForcedSplitWithoutNativeFilesTakesJni() {
        // Sanity: even when not forced, a split whose raw files are absent must not go native.
        // MUTATION: making shouldUseNativeReader ignore supportNativeReader -> returns true -> red.
        Assertions.assertFalse(
                PaimonScanPlanProvider.shouldUseNativeReader(
                        /*forceJni*/ false, /*forceJniScanner*/ false, Optional.empty()),
                "a split without convertible raw files must route to JNI regardless of forceJni");
    }

    @Test
    public void forceJniScannerRoutesNativeEligibleSplitToJni() {
        // FIX-FORCE-JNI-SCANNER (M-1): a normal (non-name-forced) split whose raw files DO support the
        // native reader must STILL route to JNI when the session sets force_jni_scanner=true — this is the
        // user escape hatch legacy honors (PaimonScanNode.getSplits gate: !forceJniScanner && ...). Without
        // it the native-reader bug the user is trying to dodge stays on the native path.
        Optional<java.util.List<RawFile>> rawFiles = Optional.of(
                Arrays.asList(parquetRawFile("/data/part-0.parquet")));

        // WHY: routing-correctness — force_jni_scanner is a sibling of the handle name-force, ANDed into
        // the same native gate. MUTATION: dropping the `!forceJniScanner` conjunct in shouldUseNativeReader
        // -> this native-eligible split goes native despite force_jni_scanner=true -> red.
        Assertions.assertFalse(
                PaimonScanPlanProvider.shouldUseNativeReader(
                        /*forceJni*/ false, /*forceJniScanner*/ true, rawFiles),
                "force_jni_scanner=true must route even native-eligible ORC/Parquet splits to JNI");
    }

    // ---- FIX-URI-NORMALIZE (B-7DF data file + B-7DV deletion vector) ----

    @Test
    public void nativeRangeNormalizesBothDataAndDeletionVectorPaths() {
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(
                new HashMap<>(), new RecordingPaimonCatalogOps(), ctx);
        RawFile file = parquetRawFile("oss://bkt/warehouse/db/t/part-0.parquet");
        DeletionFile dv = new DeletionFile(
                "oss://bkt/warehouse/db/t/index/dv-0.index", 8L, 16L, 4L);

        PaimonScanRange range = provider.buildNativeRange(
                file, dv, "parquet", Collections.emptyMap(), Collections.emptyMap(), 0L, 100L, 64L * 1024 * 1024);

        // WHY: BE's scheme-dispatched S3 file factory only opens canonical s3://. An un-normalized
        // oss:// DATA-file path fails the native ORC/Parquet read outright; an un-normalized oss:// DV
        // path silently drops the deletion vector so DELETEd rows reappear (merge-on-read corruption).
        // BOTH must route through ConnectorContext.normalizeStorageUri (legacy PaimonScanNode normalizes
        // both via the 2-arg LocationPath.of). MUTATION: dropping normalizeUri on either site -> that
        // path stays oss:// -> red.
        Assertions.assertEquals("s3://bkt/warehouse/db/t/part-0.parquet",
                range.getPath().orElse(null), "data-file path must be normalized to s3://");
        Assertions.assertEquals("s3://bkt/warehouse/db/t/index/dv-0.index",
                range.getProperties().get("paimon.deletion_file.path"),
                "deletion-vector path must be normalized to s3://");
        Assertions.assertEquals(2, ctx.normalizeCount,
                "both the data-file and the DV path must be routed through normalizeStorageUri");
    }

    @Test
    public void nativeRangeWithoutDeletionVectorNormalizesOnlyDataPath() {
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(
                new HashMap<>(), new RecordingPaimonCatalogOps(), ctx);

        PaimonScanRange range = provider.buildNativeRange(
                parquetRawFile("oss://bkt/a/part-0.parquet"), null, "parquet",
                Collections.emptyMap(), Collections.emptyMap(), 0L, 100L, 64L * 1024 * 1024);

        // WHY: a DV-less native split must still normalize its data-file path and must NOT emit a DV
        // descriptor. MUTATION: emitting a deletion_file for a null DV, or skipping data normalization -> red.
        Assertions.assertEquals("s3://bkt/a/part-0.parquet", range.getPath().orElse(null));
        Assertions.assertFalse(range.getProperties().containsKey("paimon.deletion_file.path"),
                "no deletion vector -> no deletion_file descriptor");
        Assertions.assertEquals(1, ctx.normalizeCount);
    }

    @Test
    public void nativeRangeWithoutContextPreservesRawPath() {
        // 3-arg ctor leaves context == null (the offline harness path): no normalization machinery is
        // available, so the raw path is preserved without NPE. The real oss://->s3:// rewrite is
        // covered by DefaultConnectorContextNormalizeUriTest (fe-core).
        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(
                new HashMap<>(), new RecordingPaimonCatalogOps());

        PaimonScanRange range = provider.buildNativeRange(
                parquetRawFile("oss://bkt/a/part-0.parquet"), null, "parquet",
                Collections.emptyMap(), Collections.emptyMap(), 0L, 100L, 64L * 1024 * 1024);

        // MUTATION: NPE on null context, or fabricating a normalized path from nothing -> red.
        Assertions.assertEquals("oss://bkt/a/part-0.parquet", range.getPath().orElse(null));
    }

    @Test
    public void buildNativeRangeThreadsVendedTokenToBothPaths() {
        // FIX-REST-VENDED-URI-NORMALIZE (P9-1, BLOCKER): the per-table vended token must reach the
        // engine's normalize seam on BOTH the data-file AND the deletion-vector path, so a REST
        // object-store read (whose catalog static storage map is empty by design) normalizes via the
        // vended credentials instead of throwing "No storage properties found for schema: oss". The
        // positive RESTTokenFileIO extraction needs a live REST stack (E2E-gated); here we pin that
        // whatever token the scan computes is threaded VERBATIM to each normalize call.
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(
                new HashMap<>(), new RecordingPaimonCatalogOps(), ctx);
        Map<String, String> vendedToken = new HashMap<>();
        vendedToken.put("fs.oss.accessKeyId", "STS.ak");
        vendedToken.put("fs.oss.accessKeySecret", "sk");
        RawFile file = parquetRawFile("oss://bkt/warehouse/db/t/part-0.parquet");
        DeletionFile dv = new DeletionFile(
                "oss://bkt/warehouse/db/t/index/dv-0.index", 8L, 16L, 4L);

        PaimonScanRange range = provider.buildNativeRange(
                file, dv, "parquet", Collections.emptyMap(), vendedToken, 0L, 100L, 64L * 1024 * 1024);

        // WHY: the engine seam normalizes against the VENDED map (the REST static map is empty). If the
        // connector dropped the token (reverting to the 1-arg seam) or substituted an empty map, a REST
        // native read would reach BE with an un-openable oss:// (data) or a silently-dropped DV
        // (merge-on-read corruption). MUTATION: 1-arg normalize (token lost -> lastVendedToken null), or
        // passing Collections.emptyMap() instead of the token -> assertSame red.
        Assertions.assertEquals("s3://bkt/warehouse/db/t/part-0.parquet", range.getPath().orElse(null));
        Assertions.assertEquals("s3://bkt/warehouse/db/t/index/dv-0.index",
                range.getProperties().get("paimon.deletion_file.path"));
        Assertions.assertEquals(2, ctx.normalizeCount,
                "both the data-file and the DV path must route through the vended-aware normalize");
        Assertions.assertSame(vendedToken, ctx.lastVendedToken,
                "the per-table vended token must be threaded to the normalize seam (not empty/null)");
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
    public void resolveScanTableResetsStalePinForIncrementalRead(@TempDir Path warehouse) throws Exception {
        // A REAL paimon table (not FakePaimonTable, whose copy() is a no-op recorder that cannot
        // reproduce paimon's merge/remove/immutability) that PERSISTS a stale scan.snapshot-id/scan.mode
        // in its schema options — legal & mutable via TBLPROPERTIES / ALTER TABLE SET / table-default.*.
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "t");
            catalog.createTable(id, Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("val", DataTypes.BIGINT())
                    .primaryKey("id")
                    .option("bucket", "1")
                    .option("scan.snapshot-id", "1")
                    .option("scan.mode", "from-snapshot")
                    .build(), false);
            Table base = catalog.getTable(id);
            Assertions.assertEquals("1", base.options().get("scan.snapshot-id"),
                    "fixture precondition: the base table must persist a stale scan.snapshot-id");

            RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
            PaimonTableHandle handle = new PaimonTableHandle(
                    "db", "t", Collections.emptyList(), Collections.emptyList());
            handle.setPaimonTable(base);
            // applySnapshot's INCREMENTAL pin produces exactly this scanOptions map (incremental-between
            // ONLY — the null resets are NOT carried through the SPI; they are reapplied at copy time).
            PaimonTableHandle incrHandle = handle.withScanOptions(
                    Collections.singletonMap("incremental-between", "3,5"));

            PaimonScanPlanProvider provider = new PaimonScanPlanProvider(Collections.emptyMap(), ops);
            Table scanTable = provider.resolveScanTable(incrHandle);

            // WHY (FIX-INCR-SCAN-RESET): an @incr read over a base table that persists a stale
            // scan.snapshot-id must reset it to null BEFORE Table.copy (the single chokepoint shared by
            // the native/JNI scan path planScanInternal and the JNI serialized-table path
            // getScanNodeProperties). Without the reset, paimon 1.3.1 THROWS at copy()
            // ("[incremental-between] must be null when you set [scan.snapshot-id,scan.tag-name]") — so
            // resolveScanTable would throw before reaching these assertions — or silently downgrades to
            // FROM_SNAPSHOT at the stale id (wrong @incr rows). With the reset, the stale pin is removed
            // and the incremental window survives. MUTATION: dropping applyResetsIfIncremental in
            // resolveScanTable -> copy throws (or returns a table still carrying scan.snapshot-id) -> red.
            Assertions.assertFalse(scanTable.options().containsKey("scan.snapshot-id"),
                    "the stale persisted scan.snapshot-id must be reset (removed) for an @incr read");
            Assertions.assertEquals("3,5", scanTable.options().get("incremental-between"),
                    "the @incr window (incremental-between) must survive the copy");
        }
    }

    @Test
    public void getScanNodePropertiesAlwaysEmitsPredicateForNoFilterScan(@TempDir Path warehouse)
            throws Exception {
        // RC-2 (CI 968828): a paimon scan with NO pushed-down filter must STILL emit the paimon.predicate
        // param. PaimonJniScanner.getPredicates() deserializes it UNCONDITIONALLY, so when the key is
        // absent the JNI reader NPEs ("encodedStr is null") on every no-WHERE force_jni read. Legacy
        // PaimonScanNode.createScanRangeLocations always serialized the (possibly empty) predicate list.
        // MUTATION: re-gating props.put("paimon.predicate", ...) on filter.isPresent() && !isEmpty (the
        // pre-fix behavior) -> the key is absent for a no-filter scan -> red.
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "t");
            catalog.createTable(id, Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("val", DataTypes.BIGINT())
                    .primaryKey("id")
                    .option("bucket", "1")
                    .build(), false);
            Table base = catalog.getTable(id);

            RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
            PaimonTableHandle handle = new PaimonTableHandle(
                    "db", "t", Collections.emptyList(), Collections.emptyList());
            handle.setPaimonTable(base);

            PaimonScanPlanProvider provider = new PaimonScanPlanProvider(Collections.emptyMap(), ops);
            Map<String, String> props = provider.getScanNodeProperties(
                    null, handle, Collections.emptyList(), Optional.empty());

            String encoded = props.get("paimon.predicate");
            Assertions.assertNotNull(encoded,
                    "a no-filter scan must still emit paimon.predicate (else the BE JNI reader NPEs on "
                            + "deserialize(null) -> 'encodedStr is null')");
            // Round-trips (same Base64 + paimon InstantiationUtil the BE PaimonUtils.deserialize uses) to
            // an EMPTY predicate list, so ReadBuilder.withFilter(emptyList) applies no filter.
            byte[] decoded = Base64.getDecoder().decode(encoded.getBytes(StandardCharsets.UTF_8));
            Object obj = InstantiationUtil.deserializeObject(decoded, getClass().getClassLoader());
            Assertions.assertTrue(obj instanceof List, "paimon.predicate must deserialize to a List");
            Assertions.assertTrue(((List<?>) obj).isEmpty(),
                    "a no-filter scan's predicate list must deserialize to empty (no filter applied)");
        }
    }

    @Test
    public void getScanNodePropertiesEmitsPathPartitionKeysForPartitionedTable(@TempDir Path warehouse)
            throws Exception {
        // RC (CI 968880): a partitioned paimon table must declare path_partition_keys so
        // PluginDrivenScanNode.getPathPartitionKeys excludes the partition columns from the file/decode
        // set. Paimon stores partition columns IN the data file and the per-split columnsFromPath already
        // appends them; without path_partition_keys the BE both DECODES dt from the ORC file AND APPENDS
        // it -> a row-count double-fill that aborts the native OrcReader (DCHECK block rows != dt rows).
        // MUTATION: dropping the props.put("path_partition_keys", ...) -> the key is absent -> red.
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "t");
            catalog.createTable(id, Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("dt", DataTypes.STRING())
                    .partitionKeys("dt")
                    .option("bucket", "-1")
                    .build(), false);
            Table base = catalog.getTable(id);

            RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
            PaimonTableHandle handle = new PaimonTableHandle(
                    "db", "t", Collections.emptyList(), Collections.emptyList());
            handle.setPaimonTable(base);

            PaimonScanPlanProvider provider = new PaimonScanPlanProvider(Collections.emptyMap(), ops);
            Map<String, String> props = provider.getScanNodeProperties(
                    null, handle, Collections.emptyList(), Optional.empty());

            Assertions.assertEquals("dt", props.get("path_partition_keys"),
                    "a partitioned paimon table must declare its partition columns as path_partition_keys "
                            + "so the BE excludes them from the file decode set (else double-fill -> crash)");
        }
    }

    @Test
    public void getScanNodePropertiesEmitsSchemaEvolutionForReadOptimizedSysTable(@TempDir Path warehouse)
            throws Exception {
        // RC (BE SIGSEGV, CI 4b983431bda): a native read of a paimon $ro (read-optimized) system table
        // must STILL emit the paimon.schema_evolution dict, so BE sets history_schema_info and matches
        // file<->table columns BY FIELD ID. A $ro table resolves to a paimon ReadOptimizedTable, which
        // is NOT an instanceof FileStoreTable (it WRAPS one), so buildSchemaEvolutionParam used to skip
        // it and emit nothing. With no history_schema_info, BE's gen_table_info_node_by_field_id falls
        // into the legacy name-matching branch by_parquet_name(tuple_descriptor, ...), where the paimon
        // reader passes a still-null _tuple_descriptor (get_tuple_descriptor() is only populated later in
        // _do_init_reader, after on_before_init_reader) and dereferences it
        // (table_schema_change_helper.cpp:94) -> SIGSEGV that aborts the whole BE. Legacy PaimonScanNode
        // set history_schema_info for ANY paimon table (incl. $ro) in doInitialize; this restores that
        // parity by building the dict from the BASE FileStoreTable that $ro reads.
        // MUTATION: reverting resolveSchemaDictTable to pass the $ro table straight to
        // buildSchemaEvolutionParam (its instanceof FileStoreTable guard returns empty) -> the key is
        // absent -> red.
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "t");
            catalog.createTable(id, Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .primaryKey("id")
                    .option("bucket", "1")
                    .build(), false);
            FileStoreTable base = (FileStoreTable) catalog.getTable(id);
            // $ro resolves to a ReadOptimizedTable wrapping the base FileStoreTable.
            ReadOptimizedTable roTable = new ReadOptimizedTable(base);

            RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
            // The sys reload (4-arg sys Identifier) serves the $ro table; the base reload the fix issues
            // for the schema dict (2-arg base Identifier) serves the base FileStoreTable.
            ops.sysTable = roTable;
            ops.table = base;
            // A deserialized $ro handle: sysTableName="ro", forceJni=false (only binlog/audit_log force
            // JNI), transient Table lost so resolveScanTable reloads the ReadOptimizedTable.
            PaimonTableHandle handle = PaimonTableHandle.forSystemTable("db", "t", "ro", false);

            PaimonScanPlanProvider provider = new PaimonScanPlanProvider(Collections.emptyMap(), ops);
            Map<String, String> props = provider.getScanNodeProperties(
                    null, handle, Collections.emptyList(), Optional.empty());

            String encoded = props.get("paimon.schema_evolution");
            Assertions.assertNotNull(encoded,
                    "a native $ro read must emit paimon.schema_evolution so BE sets history_schema_info "
                            + "and uses field-id matching; without it BE falls into the name-matching "
                            + "branch and dereferences a null tuple descriptor -> BE SIGSEGV");
            // Decodes to current_schema_id = -1 (latest sentinel) and a non-empty history dictionary,
            // exactly what BE's field-id matcher consumes.
            TFileScanRangeParams params = new TFileScanRangeParams();
            PaimonScanPlanProvider.applySchemaEvolutionParam(params, encoded);
            Assertions.assertTrue(params.isSetHistorySchemaInfo() && !params.getHistorySchemaInfo().isEmpty(),
                    "the $ro schema dict must carry history_schema_info entries");
            Assertions.assertEquals(-1L, params.getCurrentSchemaId(),
                    "the current/target schema id must be the -1 sentinel (legacy parity)");
        }
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

    // ---------------------------------------------------------------------
    // FIX-CPP-READER — split serialization format must match the BE reader
    // ---------------------------------------------------------------------

    /** FE Java-serde leg, byte-identical to PaimonScanPlanProvider.encodeObjectToString (private). */
    private static String feJavaEncode(Object obj) throws Exception {
        byte[] bytes = InstantiationUtil.serializeObject(obj);
        return new String(Base64.getEncoder().encode(bytes), StandardCharsets.UTF_8);
    }

    /**
     * Builds a REAL paimon {@link DataSplit} offline: a local FileSystemCatalog over LocalFileIO
     * under the @TempDir warehouse, a real keyed table, two committed rows, then plan().splits().
     * (Same local-catalog recipe proven by PaimonTableSerdeRoundTripTest; this adds the write step.)
     */
    private static DataSplit buildRealDataSplit(Path warehouse) throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "t");
            catalog.createTable(id, Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("val", DataTypes.BIGINT())
                    .primaryKey("id")
                    .option("bucket", "1")
                    .build(), false);
            Table table = catalog.getTable(id);

            BatchWriteBuilder wb = table.newBatchWriteBuilder();
            try (BatchTableWrite write = wb.newWrite()) {
                write.write(GenericRow.of(1, 100L));
                write.write(GenericRow.of(2, 200L));
                List<CommitMessage> messages = write.prepareCommit();
                try (BatchTableCommit commit = wb.newCommit()) {
                    commit.commit(messages);
                }
            }

            for (Split s : table.newReadBuilder().newScan().plan().splits()) {
                if (s instanceof DataSplit) {
                    return (DataSplit) s;
                }
            }
            throw new IllegalStateException("test fixture produced no DataSplit");
        }
    }

    @Test
    public void cppReaderFlagSelectsNativeBinaryForDataSplit(@TempDir Path warehouse) throws Exception {
        DataSplit dataSplit = buildRealDataSplit(warehouse);

        String nativeWire = PaimonScanPlanProvider.encodeSplit(dataSplit, /*cppReader*/ true);
        byte[] bytes = Base64.getDecoder().decode(nativeWire.getBytes(StandardCharsets.UTF_8));
        DataSplit roundTripped = DataSplit.deserialize(
                new DataInputViewStreamWrapper(new ByteArrayInputStream(bytes)));

        // WHY: when enable_paimon_cpp_reader is on, BE's PaimonCppReader runs the NATIVE
        // paimon::Split::Deserialize over the blob. So FE must emit DataSplit.serialize (native
        // binary), NOT Java object serde — else BE dies with "paimon-cpp deserialize split failed".
        // The native wire must (a) decode back to an equal DataSplit (the format BE consumes), and
        // (b) DIFFER from the Java-serde wire (proves the format actually switched).
        // MUTATION: dropping the cppReader branch -> both encodings equal / native deserialize fails -> red.
        Assertions.assertEquals(dataSplit, roundTripped,
                "native-format wire must round-trip via DataSplit.deserialize (what BE cpp reader decodes)");
        Assertions.assertNotEquals(feJavaEncode(dataSplit), nativeWire,
                "flag-on must produce the native binary format, not Java object serialization");
    }

    @Test
    public void cppReaderFlagOffKeepsJavaSerialization(@TempDir Path warehouse) throws Exception {
        DataSplit dataSplit = buildRealDataSplit(warehouse);

        // WHY: default reads (flag off) must be byte-for-byte the existing Java object serialization
        // for the Java JNI reader — no behavior change when the cpp reader is disabled.
        // MUTATION: always-native -> the encoding differs from the Java leg -> red.
        Assertions.assertEquals(feJavaEncode(dataSplit),
                PaimonScanPlanProvider.encodeSplit(dataSplit, /*cppReader*/ false),
                "flag-off must keep the Java object serialization byte-for-byte");
    }

    /** A non-DataSplit Split (the only abstract method is rowCount(); Split is Serializable). */
    private static final class NonDataSplitStub implements Split {
        private static final long serialVersionUID = 1L;

        @Override
        public long rowCount() {
            return 0;
        }
    }

    @Test
    public void nonDataSplitStaysJavaSerializedEvenWithCppFlag() throws Exception {
        NonDataSplitStub stub = new NonDataSplitStub();

        // WHY: the native binary format only exists for DataSplit. System splits (the nonDataSplits
        // loop) and the no-raw-file JNI fallback have no native form, so they MUST stay Java-serialized
        // even when the flag is on (legacy's `split instanceof DataSplit` gate). MUTATION: removing the
        // instanceof guard -> ClassCastException / wrong format applied to a non-DataSplit -> red.
        Assertions.assertEquals(feJavaEncode(stub),
                PaimonScanPlanProvider.encodeSplit(stub, /*cppReader*/ true),
                "a non-DataSplit must never take the native format, even with the cpp flag on");
    }

    @Test
    public void countPushdownSplitDetectedOnlyWhenAggCountAndMergedCountAvailable(
            @TempDir Path warehouse) throws Exception {
        // FIX-COUNT-PUSHDOWN (M-2): a freshly written PK-table split has a precomputed merged
        // (post-merge / post-deletion-vector) row count, so a COUNT(*) over it can be served from
        // metadata instead of materializing rows.
        DataSplit dataSplit = buildRealDataSplit(warehouse);
        Assertions.assertTrue(dataSplit.mergedRowCountAvailable(),
                "precondition: a freshly written PK split has a precomputed merged row count");
        Assertions.assertEquals(2L, dataSplit.mergedRowCount(), "two rows were written");

        // WHY: the count branch must fire ONLY when BOTH the agg is COUNT (countPushdown) AND the SDK
        // precomputed the post-merge count — mirrors legacy `applyCountPushdown &&
        // dataSplit.mergedRowCountAvailable()`. MUTATION: dropping `countPushdown &&` (or hard-coding
        // the helper to false) -> one of these two assertions flips -> red.
        Assertions.assertTrue(PaimonScanPlanProvider.isCountPushdownSplit(true, dataSplit),
                "a count query over a split with a precomputed merged count must push the count down");
        Assertions.assertFalse(PaimonScanPlanProvider.isCountPushdownSplit(false, dataSplit),
                "without count pushdown a split must take the normal scan path, never the count branch");
    }

    @Test
    public void countPushdownCollapsesMultipleSplitsToOneRangeBearingSummedTotal(
            @TempDir Path warehouse) throws Exception {
        // A PARTITIONED PK table with TWO partitions of DIFFERENT row counts (pt=1 -> 2 rows, pt=2 ->
        // 3 rows) yields TWO count-eligible DataSplits with ASYMMETRIC mergedRowCounts (2 and 3). This
        // is deliberately multi-split with asymmetric counts so the test pins BOTH halves of the fix's
        // collapse-to-one (design D-054): (a) collapse N->1 — exactly ONE range despite >=2 eligible
        // splits, and (b) cross-split summation — the one range carries 2+3=5, a total NOT reachable
        // from any single split (so first-split-only / last-split-wins / per-split-emit all go red).
        // (A single-split fixture would make these assertions degenerate — sum==first==last for N=1.)
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "t");
            catalog.createTable(id, Schema.newBuilder()
                    .column("pt", DataTypes.INT())
                    .column("id", DataTypes.INT())
                    .column("val", DataTypes.BIGINT())
                    .partitionKeys("pt")
                    .primaryKey("pt", "id")
                    .option("bucket", "1")
                    .build(), false);
            Table table = catalog.getTable(id);
            BatchWriteBuilder wb = table.newBatchWriteBuilder();
            try (BatchTableWrite write = wb.newWrite()) {
                write.write(GenericRow.of(1, 1, 100L));        // pt=1: 2 rows
                write.write(GenericRow.of(1, 2, 200L));
                write.write(GenericRow.of(2, 1, 300L));        // pt=2: 3 rows
                write.write(GenericRow.of(2, 2, 400L));
                write.write(GenericRow.of(2, 3, 500L));
                List<CommitMessage> messages = write.prepareCommit();
                try (BatchTableCommit commit = wb.newCommit()) {
                    commit.commit(messages);
                }
            }

            RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
            ops.table = table;
            PaimonScanPlanProvider provider = new PaimonScanPlanProvider(Collections.emptyMap(), ops);
            PaimonTableHandle handle = new PaimonTableHandle(
                    "db", "t", Collections.emptyList(), Collections.emptyList());
            ConnectorSession session = sessionWithProps(Collections.emptyMap());
            List<ConnectorColumnHandle> noColumns = Collections.emptyList();

            // Precondition: the read plan really does produce >=2 count-eligible DataSplits (else the
            // collapse assertion below would be degenerate). This guards the fixture itself.
            int eligibleSplits = 0;
            for (Split s : table.newReadBuilder().newScan().plan().splits()) {
                if (s instanceof DataSplit
                        && PaimonScanPlanProvider.isCountPushdownSplit(true, (DataSplit) s)) {
                    ++eligibleSplits;
                }
            }
            Assertions.assertTrue(eligibleSplits >= 2,
                    "fixture precondition: two partitions must yield >=2 count-eligible splits, got "
                            + eligibleSplits);

            // count pushdown ON: collapse-to-one — exactly ONE range carrying the SUMMED total (5).
            // MUTATION (collapse): per-split emit -> >=2 ranges carry row_count -> countRanges!=1 -> red.
            // MUTATION (sum): `countSum = split.mergedRowCount()` (first/last-wins instead of +=) -> "2"
            // or "3" instead of "5" -> red. So both halves of design D-054 are pinned.
            List<ConnectorScanRange> withCount = provider.planScan(
                    session, handle, noColumns, Optional.empty(), -1, null, /*countPushdown*/ true);
            int countRanges = 0;
            String emittedCount = null;
            for (ConnectorScanRange r : withCount) {
                String v = r.getProperties().get("paimon.row_count");
                if (v != null) {
                    ++countRanges;
                    emittedCount = v;
                }
            }
            Assertions.assertEquals(1, countRanges,
                    "count pushdown must collapse >=2 eligible splits into exactly ONE count range");
            Assertions.assertEquals("5", emittedCount,
                    "the single count range must carry the cross-split SUM (2 + 3 = 5), "
                            + "a total unreachable from any single split");

            // count pushdown OFF: no range may carry a pushed-down row count (normal scan; BE counts).
            // MUTATION: emitting row_count regardless of the flag -> red.
            List<ConnectorScanRange> withoutCount = provider.planScan(
                    session, handle, noColumns, Optional.empty(), -1, null, /*countPushdown*/ false);
            for (ConnectorScanRange r : withoutCount) {
                Assertions.assertFalse(r.getProperties().containsKey("paimon.row_count"),
                        "without count pushdown no range may carry a pushed-down row count");
            }
        }
    }

    @Test
    public void jniAndCountRangesCarryRealFileFormatNotJni(@TempDir Path warehouse) throws Exception {
        // FIX-JNI-FILE-FORMAT (P7-1): a JNI-serialized split (the default reader path AND the COUNT(*)
        // collapse range) must emit the REAL data-file format in fileDesc.file_format, NOT "jni" — BE's
        // paimon_cpp_reader backfills paimon FILE_FORMAT/MANIFEST_FORMAT from it (an invalid "jni" breaks
        // the manifest read). JNI routing is gated by the paimon.split property, NOT this string, so the
        // real format is safe to emit (legacy PaimonScanNode.setPaimonParams does the same). The table is
        // created with explicit file.format=orc so the asserted value is the table option (distinct from
        // the "parquet" fallback) — proving the real option is read, not a constant.
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "t");
            catalog.createTable(id, Schema.newBuilder()
                    .column("pt", DataTypes.INT())
                    .column("id", DataTypes.INT())
                    .column("val", DataTypes.BIGINT())
                    .partitionKeys("pt")
                    .primaryKey("pt", "id")
                    .option("bucket", "1")
                    .option("file.format", "orc")
                    .build(), false);
            Table table = catalog.getTable(id);
            BatchWriteBuilder wb = table.newBatchWriteBuilder();
            try (BatchTableWrite write = wb.newWrite()) {
                write.write(GenericRow.of(1, 1, 100L));
                write.write(GenericRow.of(1, 2, 200L));
                List<CommitMessage> messages = write.prepareCommit();
                try (BatchTableCommit commit = wb.newCommit()) {
                    commit.commit(messages);
                }
            }

            RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
            ops.table = table;
            PaimonScanPlanProvider provider = new PaimonScanPlanProvider(Collections.emptyMap(), ops);
            PaimonTableHandle handle = new PaimonTableHandle(
                    "db", "t", Collections.emptyList(), Collections.emptyList());
            List<ConnectorColumnHandle> noColumns = Collections.emptyList();

            // (a) JNI data range: force_jni_scanner=true routes the native-eligible ORC split to JNI
            // (buildJniScanRange). Its file_format must be the table's "orc", not "jni".
            ConnectorSession forceJni = sessionWithProps(
                    Collections.singletonMap("force_jni_scanner", "true"));
            List<ConnectorScanRange> jniRanges = provider.planScan(
                    forceJni, handle, noColumns, Optional.empty(), -1, null, /*countPushdown*/ false);
            Assertions.assertFalse(jniRanges.isEmpty(), "force_jni scan must emit >=1 JNI range");
            for (ConnectorScanRange r : jniRanges) {
                Assertions.assertTrue(r.getProperties().containsKey("paimon.split"),
                        "force_jni_scanner=true must route the split to the JNI path");
                // MUTATION: buildJniScanRange .fileFormat("jni") -> not "orc" -> red.
                Assertions.assertEquals("orc", ((PaimonScanRange) r).getFileFormat(),
                        "a JNI range must carry the real data-file format, not \"jni\"");
            }

            // (b) COUNT(*) collapse range (buildCountRange): same real-format requirement; also pins that
            // defaultFileFormat is threaded into buildCountRange's new parameter from the call site.
            ConnectorSession plain = sessionWithProps(Collections.emptyMap());
            List<ConnectorScanRange> countRanges = provider.planScan(
                    plain, handle, noColumns, Optional.empty(), -1, null, /*countPushdown*/ true);
            PaimonScanRange countRange = null;
            for (ConnectorScanRange r : countRanges) {
                if (r.getProperties().containsKey("paimon.row_count")) {
                    countRange = (PaimonScanRange) r;
                }
            }
            Assertions.assertNotNull(countRange, "count pushdown must emit a collapsed count range");
            // MUTATION: buildCountRange .fileFormat("jni"), or dropping the threaded defaultFileFormat -> red.
            Assertions.assertEquals("orc", countRange.getFileFormat(),
                    "the COUNT(*) collapse range must carry the real data-file format, not \"jni\"");
        }
    }

    @Test
    public void jniAndCountRangesUseFileSuffixNotAlteredTableDefault(@TempDir Path warehouse) throws Exception {
        // FIX-L11: the JNI-serialized split (default reader path) and the COUNT(*) collapse range must derive
        // file_format from the split's FIRST data-file SUFFIX (legacy PaimonScanNode.getFileFormat(getPathString)
        // -> dataSplitFileFormat), NOT the table-level file.format option. These DIVERGE for an altered /
        // mixed-format table: the option is changed to parquet while historical data files remain .orc. HEAD
        // regressed to emitting the bare table default, so BE's paimon_cpp_reader would backfill the WRONG
        // format for those files. WHY it matters: unlike jniAndCountRangesCarryRealFileFormatNotJni (where the
        // table default == the .orc suffix, so it cannot distinguish default from suffix), this test forces a
        // mismatch and thus is the one that actually goes RED if the emission points revert to defaultFileFormat.
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "t");
            catalog.createTable(id, Schema.newBuilder()
                    .column("pt", DataTypes.INT())
                    .column("id", DataTypes.INT())
                    .column("val", DataTypes.BIGINT())
                    .partitionKeys("pt")
                    .primaryKey("pt", "id")
                    .option("bucket", "1")
                    .option("file.format", "orc")   // on-disk data files are written as .orc
                    .build(), false);
            Table table = catalog.getTable(id);
            BatchWriteBuilder wb = table.newBatchWriteBuilder();
            try (BatchTableWrite write = wb.newWrite()) {
                write.write(GenericRow.of(1, 1, 100L));
                write.write(GenericRow.of(1, 2, 200L));
                List<CommitMessage> messages = write.prepareCommit();
                try (BatchTableCommit commit = wb.newCommit()) {
                    commit.commit(messages);
                }
            }

            // Overlay file.format=parquet as a dynamic option: the table now REPORTS parquet as its default
            // (the connector reads table.options().file.format at plan time), while the committed data files
            // stay .orc. copy() overlays read-time options only; it does NOT rewrite the on-disk files, and
            // reads still decode each file by its own recorded format.
            Table altered = table.copy(Collections.singletonMap("file.format", "parquet"));
            Assertions.assertEquals("parquet", altered.options().get("file.format"),
                    "precondition: the altered table default must be parquet (distinct from the .orc files)");

            RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
            ops.table = altered;
            PaimonScanPlanProvider provider = new PaimonScanPlanProvider(Collections.emptyMap(), ops);
            PaimonTableHandle handle = new PaimonTableHandle(
                    "db", "t", Collections.emptyList(), Collections.emptyList());
            List<ConnectorColumnHandle> noColumns = Collections.emptyList();

            // (a) JNI data range: force_jni routes the .orc split to buildJniScanRange. Its file_format must be
            // "orc" (the real data-file suffix), NOT "parquet" (the altered table default).
            ConnectorSession forceJni = sessionWithProps(
                    Collections.singletonMap("force_jni_scanner", "true"));
            List<ConnectorScanRange> jniRanges = provider.planScan(
                    forceJni, handle, noColumns, Optional.empty(), -1, null, /*countPushdown*/ false);
            Assertions.assertFalse(jniRanges.isEmpty(), "force_jni scan must emit >=1 JNI range");
            for (ConnectorScanRange r : jniRanges) {
                Assertions.assertTrue(r.getProperties().containsKey("paimon.split"),
                        "force_jni_scanner=true must route the split to the JNI path");
                // MUTATION: buildJniScanRange .fileFormat(defaultFileFormat) -> "parquet" -> red.
                Assertions.assertEquals("orc", ((PaimonScanRange) r).getFileFormat(),
                        "a JNI range must carry the real data-file suffix (orc), not the altered table default");
            }

            // (b) COUNT(*) collapse range (buildCountRange): same suffix-over-default requirement.
            ConnectorSession plain = sessionWithProps(Collections.emptyMap());
            List<ConnectorScanRange> countRanges = provider.planScan(
                    plain, handle, noColumns, Optional.empty(), -1, null, /*countPushdown*/ true);
            PaimonScanRange countRange = null;
            for (ConnectorScanRange r : countRanges) {
                if (r.getProperties().containsKey("paimon.row_count")) {
                    countRange = (PaimonScanRange) r;
                }
            }
            Assertions.assertNotNull(countRange, "count pushdown must emit a collapsed count range");
            // MUTATION: buildCountRange .fileFormat(defaultFileFormat) -> "parquet" -> red.
            Assertions.assertEquals("orc", countRange.getFileFormat(),
                    "the COUNT(*) collapse range must carry the real data-file suffix (orc), not the altered default");
        }
    }

    // ---- FIX-NATIVE-SUBSPLIT (M-3) ----

    private static final long MB = 1024L * 1024L;

    /** Asserts the [start,length] ranges tile [0, fileLength) with no gap/overlap and positive lengths. */
    private static void assertContiguousTiling(List<long[]> ranges, long fileLength) {
        long expectedStart = 0;
        for (long[] r : ranges) {
            Assertions.assertEquals(expectedStart, r[0],
                    "ranges must tile contiguously with no gap/overlap");
            Assertions.assertTrue(r[1] > 0, "every range length must be positive");
            expectedStart += r[1];
        }
        Assertions.assertEquals(fileLength, expectedStart, "ranges must cover exactly [0, fileLength)");
    }

    @Test
    public void computeFileSplitOffsetsTilesWithOneTenthTailGuard() {
        // 250MB / 64MB: the >1.1D guard keeps the 58MB remainder in the LAST range (no tiny 5th split) —
        // byte-identical to legacy FileSplitter.splitFile. MUTATION: naive ceilDiv -> a 5th 58MB-or-tiny
        // split / wrong last length -> red.
        List<long[]> s = PaimonScanPlanProvider.computeFileSplitOffsets(250 * MB, 64 * MB);
        Assertions.assertEquals(4, s.size(),
                "250MB/64MB -> 4 ranges (the 1.1x tail guard absorbs the 58MB remainder)");
        assertContiguousTiling(s, 250 * MB);
        Assertions.assertEquals(64 * MB, s.get(0)[1]);
        Assertions.assertEquals(58 * MB, s.get(3)[1], "last range absorbs the remainder (58MB < 1.1x target)");

        // 256MB / 64MB: exact multiple -> 4 even ranges (the last is exactly 64MB, not 0).
        List<long[]> even = PaimonScanPlanProvider.computeFileSplitOffsets(256 * MB, 64 * MB);
        Assertions.assertEquals(4, even.size());
        assertContiguousTiling(even, 256 * MB);
        Assertions.assertEquals(64 * MB, even.get(3)[1]);
    }

    @Test
    public void computeFileSplitOffsetsKeepsSmallOrEmptyFilesCorrect() {
        // fileLen <= 1.1*target -> ONE whole-file range (the 1.1x guard avoids a tiny tail).
        List<long[]> small = PaimonScanPlanProvider.computeFileSplitOffsets(70 * MB, 64 * MB);
        Assertions.assertEquals(1, small.size(), "70MB <= 1.1*64MB -> one whole-file range");
        Assertions.assertArrayEquals(new long[] {0L, 70 * MB}, small.get(0));

        // zero/negative length -> no range (legacy FileSplitter skips empty files).
        Assertions.assertTrue(PaimonScanPlanProvider.computeFileSplitOffsets(0L, 64L).isEmpty());
        Assertions.assertTrue(PaimonScanPlanProvider.computeFileSplitOffsets(-5L, 64L).isEmpty());

        // non-positive target -> single whole-file range (defensive; never happens on the connector path).
        List<long[]> defensive = PaimonScanPlanProvider.computeFileSplitOffsets(123L, 0L);
        Assertions.assertEquals(1, defensive.size());
        Assertions.assertArrayEquals(new long[] {0L, 123L}, defensive.get(0));
    }

    @Test
    public void determineTargetSplitSizeMirrorsLegacyHeuristic() {
        long init = 32 * MB;   // max_initial_file_split_size default
        long max = 64 * MB;    // max_file_split_size default
        long initNum = 200;    // max_initial_file_split_num default
        long maxNum = 100000;  // max_file_split_num default

        // file_split_size > 0 wins outright (legacy: the explicit override short-circuit).
        Assertions.assertEquals(7L,
                PaimonScanPlanProvider.determineTargetSplitSize(7L, init, max, initNum, maxNum, 999L * MB));
        // total below max*initNum (64MB*200 = 12800MB) -> initial split size (32MB).
        Assertions.assertEquals(init,
                PaimonScanPlanProvider.determineTargetSplitSize(0L, init, max, initNum, maxNum, 1024L * MB));
        // total at/above max*initNum -> max split size (64MB).
        Assertions.assertEquals(max,
                PaimonScanPlanProvider.determineTargetSplitSize(0L, init, max, initNum, maxNum, 20000L * MB));
        // max_file_split_num floor raises the size above the heuristic: ceil(total/maxNum) > 64MB.
        long hugeTotal = 10_000_000L * MB;   // ceil(/100000) = 100MB > 64MB
        Assertions.assertEquals((hugeTotal + maxNum - 1L) / maxNum,
                PaimonScanPlanProvider.determineTargetSplitSize(0L, init, max, initNum, maxNum, hugeTotal),
                "max_file_split_num floor (ceil(total/maxNum)) must raise the target above 64MB");
    }

    @Test
    public void nativeFileIsSubSplitWhenFileSplitSizeForcesIt(@TempDir Path warehouse) throws Exception {
        // An append-only (no-PK) table yields a native-eligible raw file; a small file_split_size forces
        // that single file to slice into >=2 contiguous sub-ranges end-to-end through planScan.
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "t");
            catalog.createTable(id, Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("val", DataTypes.BIGINT())
                    .build(), false);   // no primary key -> append-only -> convertToRawFiles present
            Table table = catalog.getTable(id);
            BatchWriteBuilder wb = table.newBatchWriteBuilder();
            try (BatchTableWrite write = wb.newWrite()) {
                for (int i = 0; i < 200; i++) {
                    write.write(GenericRow.of(i, (long) i * 10));
                }
                List<CommitMessage> messages = write.prepareCommit();
                try (BatchTableCommit commit = wb.newCommit()) {
                    commit.commit(messages);
                }
            }

            // Precondition: exactly ONE native raw file, so the contiguous-tiling check is over one file.
            List<RawFile> rawFiles = new ArrayList<>();
            for (Split s : table.newReadBuilder().newScan().plan().splits()) {
                if (s instanceof DataSplit) {
                    ((DataSplit) s).convertToRawFiles().ifPresent(rawFiles::addAll);
                }
            }
            Assertions.assertEquals(1, rawFiles.size(),
                    "fixture precondition: append-only commit must yield exactly one native raw file");
            long fileLength = rawFiles.get(0).length();
            Assertions.assertTrue(fileLength > 0, "fixture raw file must be non-empty");
            long splitSize = Math.max(1L, fileLength / 3);   // ~3 sub-ranges

            RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
            ops.table = table;
            PaimonScanPlanProvider provider = new PaimonScanPlanProvider(Collections.emptyMap(), ops);
            PaimonTableHandle handle = new PaimonTableHandle(
                    "db", "t", Collections.emptyList(), Collections.emptyList());
            List<ConnectorColumnHandle> noColumns = Collections.emptyList();

            // Small file_split_size -> the single native file MUST sub-split into >=2 contiguous ranges.
            // WHY: this is the whole fix — one scanner per large file becomes N parallel sub-ranges.
            // MUTATION: neuter computeFileSplitOffsets to a single whole-file range -> nativeRanges==1 -> red.
            ConnectorSession splitting = sessionWithProps(
                    Collections.singletonMap("file_split_size", String.valueOf(splitSize)));
            List<ConnectorScanRange> ranges = provider.planScan(
                    splitting, handle, noColumns, Optional.empty(), -1, null, /*countPushdown*/ false);
            List<ConnectorScanRange> nativeRanges = new ArrayList<>();
            for (ConnectorScanRange r : ranges) {
                if (r.getPath().isPresent()) {   // native ranges carry a file path; JNI ranges do not
                    nativeRanges.add(r);
                }
            }
            Assertions.assertTrue(nativeRanges.size() >= 2,
                    "a small file_split_size must sub-split the native file into >=2 ranges, got "
                            + nativeRanges.size());
            nativeRanges.sort(Comparator.comparingLong(ConnectorScanRange::getStart));
            long expectedStart = 0;
            for (ConnectorScanRange r : nativeRanges) {
                Assertions.assertEquals(expectedStart, r.getStart(),
                        "native sub-ranges must tile [0, fileLength) contiguously");
                Assertions.assertTrue(r.getLength() > 0, "every sub-range length must be positive");
                Assertions.assertEquals(fileLength, r.getFileSize(),
                        "every sub-range must report the WHOLE file size, not the sub-range length");
                expectedStart += r.getLength();
            }
            Assertions.assertEquals(fileLength, expectedStart,
                    "native sub-ranges must cover exactly [0, fileLength)");

            // Contrast: with the default (large) split size the small file stays a SINGLE native range.
            List<ConnectorScanRange> whole = provider.planScan(
                    sessionWithProps(Collections.emptyMap()), handle, noColumns, Optional.empty(),
                    -1, null, false);
            long wholeNative = whole.stream().filter(r -> r.getPath().isPresent()).count();
            Assertions.assertEquals(1, wholeNative,
                    "with the default 32MB+ split size the small fixture file stays one native range");
        }
    }

    @Test
    public void buildNativeRangesAttachesSameDeletionVectorToEverySubRange() {
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(
                new HashMap<>(), new RecordingPaimonCatalogOps(), ctx);
        RawFile file = parquetRawFile("oss://bkt/a/part-0.parquet");
        DeletionFile dv = new DeletionFile("oss://bkt/a/index/dv-0.index", 8L, 16L, 4L);
        long target = Math.max(1L, file.length() / 3);   // force the file to sub-split into >=2 ranges

        List<PaimonScanRange> ranges = provider.buildNativeRanges(
                file, dv, "parquet", Collections.emptyMap(), Collections.emptyMap(), target, 64L * 1024 * 1024);

        // WHY: the load-bearing correctness claim of FIX-NATIVE-SUBSPLIT — a paimon deletion vector is a
        // bitmap of GLOBAL file row positions, so EVERY sub-range of a DV-bearing file must carry the
        // same (unmodified) deletion file. If sub-ranges 2..N dropped it, their deleted rows would
        // reappear (merge-on-read corruption). MUTATION: attaching the DV only to the first (or last)
        // sub-range, or dropping it on sub-ranges -> a sub-range with a null/!= deletion_file.path -> red.
        Assertions.assertTrue(ranges.size() >= 2,
                "fixture must sub-split into >=2 ranges, got " + ranges.size());
        String expectedDv = ranges.get(0).getProperties().get("paimon.deletion_file.path");
        Assertions.assertNotNull(expectedDv,
                "the DV-bearing file's sub-ranges must carry a deletion file");
        for (PaimonScanRange r : ranges) {
            Assertions.assertEquals(expectedDv, r.getProperties().get("paimon.deletion_file.path"),
                    "every native sub-range must carry the same deletion vector (global-row-position DV)");
        }
    }

    @Test
    public void buildNativeRangesKeepsFileWholeWhenTargetNonPositive() {
        // Under COUNT(*) pushdown the native arm passes target size 0 so a native split that was NOT
        // siphoned to the count arm (no precomputed merged count) is kept WHOLE — legacy parity
        // (splittable=!applyCountPushdown). MUTATION: sub-splitting under count pushdown -> >1 range -> red.
        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(
                new HashMap<>(), new RecordingPaimonCatalogOps());
        RawFile file = parquetRawFile("oss://bkt/a/part-0.parquet");

        List<PaimonScanRange> ranges = provider.buildNativeRanges(
                file, null, "parquet", Collections.emptyMap(), Collections.emptyMap(), 0L, 64L * 1024 * 1024);

        Assertions.assertEquals(1, ranges.size(),
                "a non-positive target (COUNT(*) pushdown) must keep the file as one whole-file range");
        Assertions.assertEquals(0L, ranges.get(0).getStart());
        Assertions.assertEquals(file.length(), ranges.get(0).getLength(),
                "the whole-file range must span the entire file");
    }

    private static ConnectorSession sessionWithProps(Map<String, String> sessionProps) {
        return new ConnectorSession() {
            @Override
            public String getQueryId() {
                return "q";
            }

            @Override
            public String getUser() {
                return "u";
            }

            @Override
            public String getTimeZone() {
                return "UTC";
            }

            @Override
            public String getLocale() {
                return "en_US";
            }

            @Override
            public long getCatalogId() {
                return 0;
            }

            @Override
            public String getCatalogName() {
                return "c";
            }

            @Override
            public <T> T getProperty(String name, Class<T> type) {
                return null;
            }

            @Override
            public Map<String, String> getCatalogProperties() {
                return Collections.emptyMap();
            }

            @Override
            public Map<String, String> getSessionProperties() {
                return sessionProps;
            }
        };
    }

    @Test
    public void isCppReaderEnabledReadsSessionProperty() {
        // WHY: pins the EXACT session key ("enable_paimon_cpp_reader", byte-identical to
        // SessionVariable.ENABLE_PAIMON_CPP_READER) and the default-false semantics. The format choice
        // hinges on reading this flag correctly. MUTATION: wrong key, or defaulting true -> red.
        Assertions.assertTrue(PaimonScanPlanProvider.isCppReaderEnabled(
                sessionWithProps(Collections.singletonMap("enable_paimon_cpp_reader", "true"))));
        Assertions.assertFalse(PaimonScanPlanProvider.isCppReaderEnabled(
                sessionWithProps(Collections.singletonMap("enable_paimon_cpp_reader", "false"))));
        Assertions.assertFalse(PaimonScanPlanProvider.isCppReaderEnabled(
                sessionWithProps(Collections.emptyMap())), "absent flag must default to false");
        Assertions.assertFalse(PaimonScanPlanProvider.isCppReaderEnabled(null),
                "a null session must default to false");
    }

    @Test
    public void isForceJniScannerEnabledReadsSessionProperty() {
        // FIX-FORCE-JNI-SCANNER (M-1): pins the EXACT session key ("force_jni_scanner", byte-identical to
        // SessionVariable.FORCE_JNI_SCANNER) and the default-false semantics. Both native sites (the split
        // router and the schema-evolution emit gate) hinge on reading this flag correctly. MUTATION: wrong
        // key, or defaulting true -> red.
        Assertions.assertTrue(PaimonScanPlanProvider.isForceJniScannerEnabled(
                sessionWithProps(Collections.singletonMap("force_jni_scanner", "true"))));
        Assertions.assertFalse(PaimonScanPlanProvider.isForceJniScannerEnabled(
                sessionWithProps(Collections.singletonMap("force_jni_scanner", "false"))));
        Assertions.assertFalse(PaimonScanPlanProvider.isForceJniScannerEnabled(
                sessionWithProps(Collections.emptyMap())), "absent flag must default to false");
        Assertions.assertFalse(PaimonScanPlanProvider.isForceJniScannerEnabled(null),
                "a null session must default to false");
    }

    // ---------------------------------------------------------------------
    // FIX-REST-VENDED — per-table vended credentials overlaid as location.*
    // ---------------------------------------------------------------------

    @Test
    public void extractVendedTokenEmptyForNullAndNonRestFileIO() {
        // WHY: vended credentials must be attempted ONLY for REST tables (RESTTokenFileIO). A null
        // table, a table with no FileIO, and a table with a non-REST FileIO must ALL yield nothing —
        // never leak/attempt a token. MUTATION: vending for any FileIO type -> red. (The positive
        // RESTTokenFileIO branch needs a live REST stack -> covered by the fe-core bridge test + E2E.)
        Assertions.assertTrue(PaimonScanPlanProvider.extractVendedToken(null).isEmpty(),
                "a null table yields no vended token");

        FakePaimonTable nullFileIo = new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList());
        Assertions.assertTrue(PaimonScanPlanProvider.extractVendedToken(nullFileIo).isEmpty(),
                "a table with no FileIO yields no vended token");

        FakePaimonTable nonRest = new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList());
        nonRest.fileIO = LocalFileIO.create();   // a real, non-REST FileIO
        Assertions.assertTrue(PaimonScanPlanProvider.extractVendedToken(nonRest).isEmpty(),
                "a non-RESTTokenFileIO table must yield no vended token");
    }

    /** A ConnectorContext whose getStorageProperties() (typed fe-filesystem seam, P1-T04) and
     * vendStorageCredentials return fixed normalized maps. The engine's real StorageProperties
     * binding/normalization is exercised by the fe-core DefaultConnectorContextStoragePropsTest /
     * DefaultConnectorContextVendTest; here we pin the connector wiring (static creds sourced from
     * toBackendProperties().toMap(), overlay order, and that the raw catalog aliases are NOT shipped). */
    private static ConnectorContext scanContext(Map<String, String> backendStatic, Map<String, String> vended) {
        return new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "c";
            }

            @Override
            public long getCatalogId() {
                return 0;
            }

            @Override
            public List<StorageProperties> getStorageProperties() {
                return backendStatic.isEmpty()
                        ? Collections.emptyList()
                        : Collections.singletonList(fakeBackendStorage(backendStatic));
            }

            @Override
            public Map<String, String> vendStorageCredentials(Map<String, String> raw) {
                return vended;
            }
        };
    }

    /**
     * A fe-filesystem {@link StorageProperties} whose {@code toBackendProperties().toMap()} returns the
     * given BE-canonical map — mirrors how a real object-store binding (e.g. S3FileSystemProperties IS-A
     * {@link BackendStorageProperties}) hands BE creds to the connector. The connector consumes ONLY this
     * typed seam for static creds (P1-T04), so the fake exercises exactly that path. (HDFS has no typed BE
     * model in fe-filesystem yet, so a real HDFS catalog yields no entry here — see DV-004 / R-007.)
     */
    private static StorageProperties fakeBackendStorage(Map<String, String> beMap) {
        BackendStorageProperties backend = new BackendStorageProperties() {
            @Override
            public BackendStorageKind backendKind() {
                return BackendStorageKind.S3_COMPATIBLE;
            }

            @Override
            public Map<String, String> toMap() {
                return beMap;
            }
        };
        return new StorageProperties() {
            @Override
            public String providerName() {
                return "fake";
            }

            @Override
            public StorageKind kind() {
                return StorageKind.OBJECT_STORAGE;
            }

            @Override
            public FileSystemType type() {
                return FileSystemType.S3;
            }

            @Override
            public Map<String, String> rawProperties() {
                return Collections.emptyMap();
            }

            @Override
            public Map<String, String> matchedProperties() {
                return Collections.emptyMap();
            }

            @Override
            public Optional<BackendStorageProperties> toBackendProperties() {
                return Optional.of(backend);
            }
        };
    }

    @Test
    public void getScanNodePropertiesNormalizesStaticCreds() {
        FakePaimonTable table = new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList());
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        handle.setPaimonTable(table);

        // The connector holds the RAW catalog aliases; the engine seam returns the BE-canonical map.
        Map<String, String> props = new HashMap<>();
        props.put("s3.access_key", "raw-ak");
        props.put("s3.secret_key", "raw-sk");

        Map<String, String> backendStatic = new HashMap<>();
        backendStatic.put("AWS_ACCESS_KEY", "ak");
        backendStatic.put("AWS_SECRET_KEY", "sk");
        backendStatic.put("AWS_ENDPOINT", "ep");

        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(
                props, new RecordingPaimonCatalogOps(),
                scanContext(backendStatic, Collections.emptyMap()));

        Map<String, String> scanProps = provider.getScanNodeProperties(
                null, handle, Collections.emptyList(), Optional.empty());

        // WHY (BLOCKER B-9): BE's native (FILE_S3) reader understands ONLY AWS_* keys. The connector
        // must ship the engine-normalized canonical creds under location.*, NOT the raw catalog aliases
        // (s3.access_key/…) which BE cannot read (403 on a private bucket). MUTATION: re-introducing the
        // raw passthrough -> location.s3.access_key present / location.AWS_ACCESS_KEY absent -> red.
        Assertions.assertEquals("ak", scanProps.get("location.AWS_ACCESS_KEY"));
        Assertions.assertEquals("sk", scanProps.get("location.AWS_SECRET_KEY"));
        Assertions.assertEquals("ep", scanProps.get("location.AWS_ENDPOINT"));
        Assertions.assertFalse(scanProps.containsKey("location.s3.access_key"),
                "the raw catalog alias must NOT reach BE (that is the B-9 bug)");
        Assertions.assertFalse(scanProps.containsKey("location.s3.secret_key"),
                "the raw catalog alias must NOT reach BE (that is the B-9 bug)");
    }

    @Test
    public void getScanNodePropertiesOverlaysVendedCreds() {
        FakePaimonTable table = new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList());
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        handle.setPaimonTable(table);

        // Static (engine-normalized) creds; vended (REST per-table) creds collide on AWS_ACCESS_KEY /
        // AWS_ENDPOINT and must WIN (legacy precedence: vended overlays static).
        Map<String, String> backendStatic = new HashMap<>();
        backendStatic.put("AWS_ACCESS_KEY", "static-ak");
        backendStatic.put("AWS_ENDPOINT", "static-ep");

        Map<String, String> vended = new HashMap<>();
        vended.put("AWS_ACCESS_KEY", "vended-ak");
        vended.put("AWS_SECRET_KEY", "vended-sk");
        vended.put("AWS_TOKEN", "vended-tok");
        vended.put("AWS_ENDPOINT", "vended-ep");

        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(
                new HashMap<>(), new RecordingPaimonCatalogOps(), scanContext(backendStatic, vended));

        Map<String, String> scanProps = provider.getScanNodeProperties(
                null, handle, Collections.emptyList(), Optional.empty());

        // WHY (BLOCKER): native-reader REST tables must receive normalized vended AWS_* creds under
        // location.*; without them BE hits the object store with no credentials (403). Vended overlays
        // static (legacy precedence). MUTATION: no overlay loop / context not threaded -> AWS_* absent
        // -> red; overlaying static AFTER vended -> the colliding location.AWS_ACCESS_KEY/ENDPOINT keep
        // the static value -> red.
        Assertions.assertEquals("vended-ak", scanProps.get("location.AWS_ACCESS_KEY"));
        Assertions.assertEquals("vended-sk", scanProps.get("location.AWS_SECRET_KEY"));
        Assertions.assertEquals("vended-tok", scanProps.get("location.AWS_TOKEN"));
        Assertions.assertEquals("vended-ep", scanProps.get("location.AWS_ENDPOINT"),
                "vended creds must overlay (win over) the static location key on collision");
    }

    @Test
    public void getScanNodePropertiesNoContextNoStorageProps() {
        FakePaimonTable table = new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList());
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        handle.setPaimonTable(table);

        Map<String, String> props = new HashMap<>();
        props.put("s3.access_key", "raw-ak");
        // 2-arg ctor -> context == null (the offline harness path).
        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(props, new RecordingPaimonCatalogOps());

        Map<String, String> scanProps = provider.getScanNodeProperties(
                null, handle, Collections.emptyList(), Optional.empty());

        // WHY: the connector cannot normalize static creds without the engine seam, so with no context
        // (offline only — production always wires one) it emits NO storage props — never the broken raw
        // aliases that BE cannot read. MUTATION: NPE on null context, or re-adding the raw passthrough
        // -> location.s3.access_key present -> red.
        Assertions.assertFalse(scanProps.containsKey("location.s3.access_key"),
                "no context -> the raw alias must not be shipped to BE");
        Assertions.assertFalse(scanProps.containsKey("location.AWS_ACCESS_KEY"),
                "no context -> no normalized overlay");
    }

    @Test
    public void getScanNodePropertiesSkipsStoragePropsWithoutBackendMappingAndMergesRest() {
        FakePaimonTable table = new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList());
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        handle.setPaimonTable(table);

        Map<String, String> beMap = new HashMap<>();
        beMap.put("AWS_ACCESS_KEY", "ak");
        beMap.put("AWS_ENDPOINT", "ep");
        // A typed list mixing a backend WITHOUT a BE model (toBackendProperties() empty — the real HDFS
        // case, see DV-004/R-007) and a real object-store backend. Exercises the two facets the single-entry
        // tests miss: the .ifPresent skip and the multi-entry putAll merge.
        List<StorageProperties> storage =
                Arrays.asList(fakeStorageWithoutBackend(), fakeBackendStorage(beMap));

        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(
                new HashMap<>(), new RecordingPaimonCatalogOps(), scanContextWithStorage(storage));

        Map<String, String> scanProps = provider.getScanNodeProperties(
                null, handle, Collections.emptyList(), Optional.empty());

        // WHY: a StorageProperties with no BE model (Optional.empty()) must be SKIPPED, never crash, while
        // a real object-store entry alongside it still ships its AWS_* under location.* (the merge loop).
        // MUTATION: .ifPresent -> .get()/.orElseThrow() -> NoSuchElementException on the empty entry -> red;
        // dropping the iteration / merge -> location.AWS_ACCESS_KEY absent -> red.
        Assertions.assertEquals("ak", scanProps.get("location.AWS_ACCESS_KEY"));
        Assertions.assertEquals("ep", scanProps.get("location.AWS_ENDPOINT"));
    }

    /** A ConnectorContext whose getStorageProperties() returns the given typed list verbatim (no vended). */
    private static ConnectorContext scanContextWithStorage(List<StorageProperties> storage) {
        return new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "c";
            }

            @Override
            public long getCatalogId() {
                return 0;
            }

            @Override
            public List<StorageProperties> getStorageProperties() {
                return storage;
            }
        };
    }

    /** A fe-filesystem {@link StorageProperties} with NO backend model — toBackendProperties() defaults to
     * Optional.empty() (the real HDFS case: HdfsFileSystemProvider has no typed BE binding, DV-004/R-007). */
    private static StorageProperties fakeStorageWithoutBackend() {
        return new StorageProperties() {
            @Override
            public String providerName() {
                return "no-be";
            }

            @Override
            public StorageKind kind() {
                return StorageKind.HDFS_COMPATIBLE;
            }

            @Override
            public FileSystemType type() {
                return FileSystemType.HDFS;
            }

            @Override
            public Map<String, String> rawProperties() {
                return Collections.emptyMap();
            }

            @Override
            public Map<String, String> matchedProperties() {
                return Collections.emptyMap();
            }
        };
    }

    // ---- FIX-JDBC-DRIVER-URL (B-8a): BE-bound driver_url resolution + paimon.jdbc.* alias ----

    /** A ConnectorContext whose getEnvironment() returns a fixed map (for jdbc_drivers_dir resolution). */
    private static ConnectorContext envContext(Map<String, String> env) {
        return new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "c";
            }

            @Override
            public long getCatalogId() {
                return 0;
            }

            @Override
            public Map<String, String> getEnvironment() {
                return env;
            }
        };
    }

    @Test
    public void backendOptionsResolveBareDriverUrl() {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.catalog.type", "jdbc");
        props.put("jdbc.driver_url", "mysql.jar");
        props.put("jdbc.driver_class", "com.mysql.cj.jdbc.Driver");
        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(
                props, new RecordingPaimonCatalogOps(),
                envContext(Collections.singletonMap("jdbc_drivers_dir", "/opt/drivers")));

        Map<String, String> opts = provider.getBackendPaimonOptions();

        // WHY (BLOCKER B-8a): BE does new URL(value) (JdbcDriverUtils.registerDriver); a bare
        // "mysql.jar" throws MalformedURLException, so FE must ship a full scheme-bearing URL.
        // MUTATION: forwarding the raw value -> "mysql.jar" (no scheme) -> red.
        Assertions.assertEquals("file:///opt/drivers/mysql.jar", opts.get("jdbc.driver_url"));
        Assertions.assertEquals("com.mysql.cj.jdbc.Driver", opts.get("jdbc.driver_class"));
    }

    @Test
    public void backendOptionsHonorPaimonJdbcAlias() {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.catalog.type", "jdbc");
        props.put("paimon.jdbc.driver_url", "mysql.jar");
        props.put("paimon.jdbc.driver_class", "com.mysql.cj.jdbc.Driver");
        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(
                props, new RecordingPaimonCatalogOps(),
                envContext(Collections.singletonMap("jdbc_drivers_dir", "/opt/drivers")));

        Map<String, String> opts = provider.getBackendPaimonOptions();

        // WHY (BLOCKER B-8a): the startsWith("jdbc.") filter drops the paimon.jdbc.* alias form
        // entirely, so BE never receives the driver. The fix reads either alias and emits the
        // canonical jdbc.* key (BE PaimonJdbcDriverUtils accepts both). MUTATION: dropping the alias
        // -> driver_url/class absent -> red.
        Assertions.assertEquals("file:///opt/drivers/mysql.jar", opts.get("jdbc.driver_url"));
        Assertions.assertEquals("com.mysql.cj.jdbc.Driver", opts.get("jdbc.driver_class"));
        Assertions.assertFalse(opts.containsKey("paimon.jdbc.driver_url"),
                "the raw paimon.jdbc.* alias key must not be shipped to BE");
    }

    @Test
    public void backendOptionsResolveWhenBothAliasesSet() {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.catalog.type", "jdbc");
        // Both alias forms present with DIFFERENT values. firstNonBlank(JDBC_DRIVER_URL) order is
        // {paimon.jdbc.driver_url, jdbc.driver_url} -> the paimon.jdbc.* value wins (legacy priority).
        props.put("paimon.jdbc.driver_url", "postgres.jar");
        props.put("jdbc.driver_url", "mysql.jar");
        props.put("paimon.jdbc.driver_class", "org.postgresql.Driver");
        props.put("jdbc.driver_class", "com.mysql.cj.jdbc.Driver");
        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(
                props, new RecordingPaimonCatalogOps(),
                envContext(Collections.singletonMap("jdbc_drivers_dir", "/opt/drivers")));

        Map<String, String> opts = provider.getBackendPaimonOptions();

        // WHY: the forwarding loop copies the raw jdbc.driver_url="mysql.jar"; the explicit
        // alias-aware put must OVERRIDE it with the resolved paimon.jdbc.* value (priority parity),
        // and the raw mysql.jar must NOT leak through. MUTATION: dropping the override (or flipping
        // the alias priority) -> jdbc.driver_url ends as the raw/wrong "mysql.jar" -> red.
        Assertions.assertEquals("file:///opt/drivers/postgres.jar", opts.get("jdbc.driver_url"));
        Assertions.assertEquals("org.postgresql.Driver", opts.get("jdbc.driver_class"));
        Assertions.assertFalse(opts.values().contains("mysql.jar"),
                "the raw lower-priority alias value must not survive in the BE options");
    }

    @Test
    public void backendOptionsPreserveSchemeBearingDriverUrl() {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.catalog.type", "jdbc");
        props.put("jdbc.driver_url", "file:///custom/path/mysql.jar");
        props.put("jdbc.driver_class", "com.mysql.cj.jdbc.Driver");
        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(
                props, new RecordingPaimonCatalogOps(), envContext(Collections.emptyMap()));

        // A value already carrying a scheme is shipped unchanged (no double-prefixing).
        Assertions.assertEquals("file:///custom/path/mysql.jar",
                provider.getBackendPaimonOptions().get("jdbc.driver_url"));
    }

    @Test
    public void backendOptionsEmptyForNonJdbcFlavor() {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.catalog.type", "filesystem");
        props.put("jdbc.driver_url", "mysql.jar");
        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(
                props, new RecordingPaimonCatalogOps(), envContext(Collections.emptyMap()));

        // Regression guard: the driver_url logic must not leak into non-JDBC flavors.
        Assertions.assertTrue(provider.getBackendPaimonOptions().isEmpty());
    }

    @Test
    public void backendOptionsForwardJniIoManagerRegardlessOfMetastore() {
        Map<String, String> props = new HashMap<>();
        // filesystem (non-jdbc) metastore: the common Paimon primary-key merge-read case #65332
        // targets. The three JNI IOManager options MUST still reach BE.
        props.put("paimon.catalog.type", "filesystem");
        props.put("paimon.doris.enable_jni_io_manager", "true");
        props.put("paimon.doris.jni_io_manager.tmp_dir", "/tmp/doris-paimon");
        props.put("paimon.doris.jni_io_manager.impl_class", "org.example.CustomIOManager");
        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(
                props, new RecordingPaimonCatalogOps(), envContext(Collections.emptyMap()));

        Map<String, String> opts = provider.getBackendPaimonOptions();

        // WHY (#65332): BE's PaimonJniScanner spills through the Paimon IOManager only when FE ships
        // doris.enable_jni_io_manager (BE re-adds the paimon. prefix). Before the fix a non-jdbc
        // catalog returned emptyMap(), so the flag never reached BE and primary-key merge reads
        // could OOM. The "paimon." connector prefix must be stripped exactly once.
        // MUTATION: gating the JNI collection behind the jdbc check (or dropping the prefix strip)
        // -> keys absent/misnamed -> red.
        Assertions.assertEquals("true", opts.get("doris.enable_jni_io_manager"));
        Assertions.assertEquals("/tmp/doris-paimon", opts.get("doris.jni_io_manager.tmp_dir"));
        Assertions.assertEquals("org.example.CustomIOManager", opts.get("doris.jni_io_manager.impl_class"));
        Assertions.assertEquals(3, opts.size());
    }

    @Test
    public void backendOptionsForwardFileReaderAsyncOptOut() {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.catalog.type", "filesystem");
        props.put("paimon.jni.enable_file_reader_async", "false");
        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(
                props, new RecordingPaimonCatalogOps(), envContext(Collections.emptyMap()));

        Map<String, String> opts = provider.getBackendPaimonOptions();

        // WHY (#65365): BE's PaimonJniScanner disables paimon's async file reader only when FE ships
        // jni.enable_file_reader_async=false (BE re-adds the paimon. prefix). Upstream added the key to
        // the legacy PaimonScanNode forwarding list, which this branch deleted with the fe-core paimon
        // subsystem — the connector list is now the only path to BE.
        // MUTATION: dropping the key from BACKEND_PAIMON_JNI_OPTIONS -> flag never reaches BE -> red.
        Assertions.assertEquals("false", opts.get("jni.enable_file_reader_async"));
        Assertions.assertEquals(1, opts.size());
    }

    // ---- FIX-SCHEMA-EVOLUTION (B-1a): native-reader schema dictionary ----

    @Test
    public void buildSchemaInfoCarriesFieldIdsNamesAndScalarTag() {
        // WHY (B-1a): BE matches file<->table columns BY paimon field id; the id+name on each top-level
        // field are the join keys. Scalars carry a single placeholder tag because BE reads type.type only
        // as a nested-vs-scalar discriminator. MUTATION: dropping setId/setName -> ids/names absent -> BE
        // can't field-id-match -> falls back to by-name (the silent wrong-rows bug).
        List<DataField> fields = Arrays.asList(
                new DataField(7, "id", DataTypes.INT()),
                new DataField(9, "name", DataTypes.STRING()));

        TSchema schema = PaimonScanPlanProvider.buildSchemaInfo(3L, fields, false);

        Assertions.assertEquals(3L, schema.getSchemaId());
        List<TFieldPtr> top = schema.getRootField().getFields();
        Assertions.assertEquals(2, top.size());
        Assertions.assertEquals(7, top.get(0).getFieldPtr().getId());
        Assertions.assertEquals("id", top.get(0).getFieldPtr().getName());
        Assertions.assertEquals(TPrimitiveType.STRING, top.get(0).getFieldPtr().getType().getType());
        Assertions.assertEquals(9, top.get(1).getFieldPtr().getId());
        Assertions.assertEquals("name", top.get(1).getFieldPtr().getName());
    }

    @Test
    public void buildSchemaInfoNestedShapesAndStructChildIds() {
        // WHY (B-1a): the e2e case is a struct-field rename, so STRUCT children MUST carry their own
        // paimon ids/names; ARRAY/MAP/STRUCT tags must be exact (BE checks them); array element / map kv
        // are matched structurally (no id). MUTATION: wrong nesting tag or missing struct-child id -> BE
        // SCHEMA_ERROR or by-name fallback inside nested types.
        DataType struct = DataTypes.ROW(
                DataTypes.FIELD(10, "f1", DataTypes.INT()),
                DataTypes.FIELD(11, "f2", DataTypes.STRING()));
        List<DataField> fields = Arrays.asList(
                new DataField(1, "arr", DataTypes.ARRAY(DataTypes.INT())),
                new DataField(2, "m", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                new DataField(3, "s", struct));

        List<TFieldPtr> top = PaimonScanPlanProvider.buildSchemaInfo(0L, fields, false)
                .getRootField().getFields();

        TField arr = top.get(0).getFieldPtr();
        Assertions.assertEquals(TPrimitiveType.ARRAY, arr.getType().getType());
        Assertions.assertEquals(1, arr.getId());
        TField elem = arr.getNestedField().getArrayField().getItemField().getFieldPtr();
        Assertions.assertEquals(TPrimitiveType.STRING, elem.getType().getType());
        Assertions.assertFalse(elem.isSetId(), "array element is matched structurally, not by id");

        TField map = top.get(1).getFieldPtr();
        Assertions.assertEquals(TPrimitiveType.MAP, map.getType().getType());
        Assertions.assertNotNull(map.getNestedField().getMapField().getKeyField().getFieldPtr());
        Assertions.assertNotNull(map.getNestedField().getMapField().getValueField().getFieldPtr());

        TField st = top.get(2).getFieldPtr();
        Assertions.assertEquals(TPrimitiveType.STRUCT, st.getType().getType());
        List<TFieldPtr> sub = st.getNestedField().getStructField().getFields();
        Assertions.assertEquals(10, sub.get(0).getFieldPtr().getId());
        Assertions.assertEquals("f1", sub.get(0).getFieldPtr().getName());
        Assertions.assertEquals(11, sub.get(1).getFieldPtr().getId());
        Assertions.assertEquals("f2", sub.get(1).getFieldPtr().getName());
    }

    @Test
    public void schemaEvolutionRoundTripAppliesCurrentAndHistory() {
        // WHY (B-1a): end-to-end transport — getScanNodeProperties serializes the dictionary, the bridge
        // hands it to populateScanLevelParams which sets current_schema_id + history_schema_info on the
        // real params. The rename a->new_a keeps field id 0 stable across schema versions, so BE reads the
        // renamed column instead of NULL. MUTATION: applySchemaEvolutionParam not copying the fields ->
        // params unset -> BE !__isset.history_schema_info -> by-name fallback -> silent wrong rows.
        TSchema current = PaimonScanPlanProvider.buildSchemaInfo(
                -1L, Arrays.asList(new DataField(0, "new_a", DataTypes.INT())), true);
        TSchema schema0 = PaimonScanPlanProvider.buildSchemaInfo(
                0L, Arrays.asList(new DataField(0, "a", DataTypes.INT())), false);
        TSchema schema1 = PaimonScanPlanProvider.buildSchemaInfo(
                1L, Arrays.asList(new DataField(0, "new_a", DataTypes.INT())), false);
        List<TSchema> history = new ArrayList<>(Arrays.asList(current, schema0, schema1));

        String encoded = PaimonScanPlanProvider.encodeSchemaEvolution(-1L, history);
        TFileScanRangeParams params = new TFileScanRangeParams();
        PaimonScanPlanProvider.applySchemaEvolutionParam(params, encoded);

        Assertions.assertTrue(params.isSetCurrentSchemaId());
        Assertions.assertEquals(-1L, params.getCurrentSchemaId());
        Assertions.assertEquals(3, params.getHistorySchemaInfo().size());
        // id 0 is stable across the rename -> by-id match (rename-safe), not by-name (NULL).
        Assertions.assertEquals(0, params.getHistorySchemaInfo().get(1)
                .getRootField().getFields().get(0).getFieldPtr().getId());
        Assertions.assertEquals("a", params.getHistorySchemaInfo().get(1)
                .getRootField().getFields().get(0).getFieldPtr().getName());
        Assertions.assertEquals(0, params.getHistorySchemaInfo().get(2)
                .getRootField().getFields().get(0).getFieldPtr().getId());
        Assertions.assertEquals("new_a", params.getHistorySchemaInfo().get(2)
                .getRootField().getFields().get(0).getFieldPtr().getName());
    }

    @Test
    public void buildSchemaInfoLowercasesTopLevelButPreservesNestedNames() {
        // WHY (BLOCKER): the -1/current entry is the BE table-side StructNode key; BE keys it VERBATIM and
        // the native reader looks up the LOWERCASE Doris slot name, so a mixed-case column ("MyCol") must
        // be lowercased ("mycol") or BE throws std::out_of_range — regressing even never-evolved reads.
        // But nested struct field names must stay paimon-cased (legacy is asymmetric: parseSchema lowers
        // top-level, paimonTypeToDorisType keeps nested). MUTATION: no toLowerCase -> "MyCol" key -> crash;
        // lowercasing nested too -> "innerfield" diverges from legacy.
        DataType struct = DataTypes.ROW(DataTypes.FIELD(5, "InnerField", DataTypes.INT()));
        List<DataField> fields = Arrays.asList(
                new DataField(0, "MyCol", DataTypes.INT()),
                new DataField(1, "S", struct));

        List<TFieldPtr> top = PaimonScanPlanProvider.buildSchemaInfo(-1L, fields, true)
                .getRootField().getFields();

        Assertions.assertEquals("mycol", top.get(0).getFieldPtr().getName(), "top-level name lowercased");
        Assertions.assertEquals("s", top.get(1).getFieldPtr().getName(), "top-level name lowercased");
        // nested struct child keeps its paimon casing (legacy parity; matched downstream via to_lower).
        Assertions.assertEquals("InnerField", top.get(1).getFieldPtr()
                .getNestedField().getStructField().getFields().get(0).getFieldPtr().getName(),
                "nested struct field name must stay paimon-cased");

        // historical entries are fully paimon-cased (the file-side value, BE looks up by id then to_lowers).
        List<TFieldPtr> hist = PaimonScanPlanProvider.buildSchemaInfo(0L, fields, false)
                .getRootField().getFields();
        Assertions.assertEquals("MyCol", hist.get(0).getFieldPtr().getName(),
                "historical entry keeps paimon casing");
    }

    @Test
    public void selectCurrentSchemaFieldsCarriesAddColumnAfterSnapshot() {
        // WHY (CI 969249 crash): the -1/current entry MUST contain every requested scan slot, or BE's
        // children_column_exists (table_schema_change_helper.h:166) DCHECK-aborts the whole BE. A paimon
        // ALTER TABLE ADD COLUMN bumps the table schema WITHOUT a new snapshot, so the resolved
        // (snapshot-pinned) table.schema() can lag the latest schema the FE slots come from. Building the
        // -1 entry from the resolved schema alone (the old code) dropped the added column -> crash. Keying
        // off the requested columns + a fresh-latest fallback carries it with its REAL field id (so newer
        // files that DO have it still read data; older files fill NULL). MUTATION: drop the latest fallback
        // -> "name" missing from the result -> the production DCHECK abort.
        List<DataField> resolved = Arrays.asList(new DataField(0, "id", DataTypes.INT()));
        List<DataField> latest = Arrays.asList(
                new DataField(0, "id", DataTypes.INT()),
                new DataField(1, "name", DataTypes.STRING()));

        List<DataField> current = PaimonScanPlanProvider.selectCurrentSchemaFields(
                resolved, latest, Arrays.asList("id", "name"));

        Assertions.assertEquals(2, current.size());
        Assertions.assertEquals("id", current.get(0).name());
        Assertions.assertEquals(0, current.get(0).id());
        Assertions.assertEquals("name", current.get(1).name(), "added column must be present (no crash)");
        Assertions.assertEquals(1, current.get(1).id(), "added column carries its real latest field id");
    }

    @Test
    public void selectCurrentSchemaFieldsResolvedWinsOnNameCollisionForTimeTravelRename() {
        // WHY: a time-travel read pins an OLD snapshot whose schema has the pre-rename name; the FE slots
        // use that pinned name. The -1 entry must key by the pinned name + pinned field id so BE matches
        // the file's field id. The resolved (pinned) schema must therefore WIN over the latest (renamed)
        // schema on a name collision, and the pinned old name must resolve in the pinned schema BEFORE the
        // latest fallback is consulted. MUTATION: prefer latest -> "full_name" keyed -> the pinned-name
        // slot "fullname" misses children -> crash / NULL.
        List<DataField> pinned = Arrays.asList(new DataField(5, "fullname", DataTypes.STRING()));
        List<DataField> latest = Arrays.asList(new DataField(5, "full_name", DataTypes.STRING()));

        List<DataField> current = PaimonScanPlanProvider.selectCurrentSchemaFields(
                pinned, latest, Arrays.asList("fullname"));

        Assertions.assertEquals(1, current.size());
        Assertions.assertEquals("fullname", current.get(0).name(), "pinned name wins for time travel");
        Assertions.assertEquals(5, current.get(0).id());
    }

    @Test
    public void selectCurrentSchemaFieldsFailsLoudOnUnknownRequestedColumn() {
        // WHY (Rule 12 / fail loud): a requested slot absent from BOTH the resolved and latest schema is a
        // genuine FE/connector inconsistency; surface it as a clean per-query failure rather than silently
        // dropping it (which would re-create the BE children mismatch). MUTATION: silent skip -> crash.
        List<DataField> resolved = Arrays.asList(new DataField(0, "id", DataTypes.INT()));
        Assertions.assertThrows(RuntimeException.class, () ->
                PaimonScanPlanProvider.selectCurrentSchemaFields(
                        resolved, Collections.emptyList(), Arrays.asList("id", "ghost")));
    }

    @Test
    public void selectCurrentSchemaFieldsEmptyColumnsReturnsResolved() {
        // WHY: a count-only scan projects no slots, so there is nothing to mismatch; fall back to the
        // resolved schema's fields verbatim (no behavior change for that path).
        List<DataField> resolved = Arrays.asList(
                new DataField(0, "id", DataTypes.INT()),
                new DataField(1, "name", DataTypes.STRING()));

        Assertions.assertSame(resolved, PaimonScanPlanProvider.selectCurrentSchemaFields(
                resolved, Collections.emptyList(), Collections.emptyList()));
    }

    @Test
    public void getScanNodePropertiesSkipsSchemaEvolutionForNonFileStoreTable() {
        // WHY: only paimon FileStoreTables take the native path; sys-tables / fakes read via JNI and never
        // consult history_schema_info. The FileStoreTable guard must skip them (and not NPE / CCE).
        // MUTATION: dropping the guard -> ClassCastException / a wrong dictionary emitted for a JNI scan.
        FakePaimonTable table = new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList());
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        handle.setPaimonTable(table);
        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(
                new HashMap<>(), new RecordingPaimonCatalogOps());

        Map<String, String> scanProps = provider.getScanNodeProperties(
                null, handle, Collections.emptyList(), Optional.empty());

        Assertions.assertFalse(scanProps.containsKey("paimon.schema_evolution"),
                "non-DataTable (JNI path) must not emit the native schema dictionary");
    }

    // ==================== FIX-A1: split weight (FE BE-assignment proportional weight) ====================

    @Test
    public void scanRangeBuilderDefaultsTargetSplitSizeToSentinel() {
        // A range built WITHOUT a denominator reports the -1 SPI sentinel (so PluginDrivenSplit keeps
        // standard()); selfSplitWeight defaults to a real 0 (a valid empty-file/sys weight). A range WITH
        // both round-trips them. MUTATION: defaulting targetSplitSize to 0 -> a 0 denominator -> red.
        PaimonScanRange noWeight = new PaimonScanRange.Builder().build();
        Assertions.assertEquals(-1L, noWeight.getTargetSplitSize(),
                "targetSplitSize default must be the -1 sentinel, not 0 (0 is an invalid denominator)");

        PaimonScanRange weighted = new PaimonScanRange.Builder()
                .selfSplitWeight(7L).targetSplitSize(99L).build();
        Assertions.assertEquals(7L, weighted.getSelfSplitWeight());
        Assertions.assertEquals(99L, weighted.getTargetSplitSize());
    }

    @Test
    public void buildNativeRangeSetsProportionalWeightFromLengthAndDv() {
        // Legacy PaimonSplit(LocationPath,...).selfSplitWeight = sub-range length, += deletionFile.length()
        // when a DV is attached (PaimonSplit:72,112). The native FE weight reproduces that and carries the
        // scan-level denominator. MUTATION: dropping the native .selfSplitWeight(...) -> weight 0 -> red.
        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(
                new HashMap<>(), new RecordingPaimonCatalogOps());
        RawFile file = parquetRawFile("/data/part-0.parquet");
        DeletionFile dv = new DeletionFile("/data/dv-0.index", 8L, 16L, 4L);

        PaimonScanRange withDv = provider.buildNativeRange(
                file, dv, "parquet", Collections.emptyMap(), Collections.emptyMap(), 0L, 64L, 64 * MB);
        Assertions.assertEquals(64L + dv.length(), withDv.getSelfSplitWeight(),
                "native weight = sub-range length + the deletion-vector length");
        Assertions.assertEquals(64 * MB, withDv.getTargetSplitSize(),
                "native range must carry the weight denominator");

        PaimonScanRange noDv = provider.buildNativeRange(
                file, null, "parquet", Collections.emptyMap(), Collections.emptyMap(), 0L, 70L, 64 * MB);
        Assertions.assertEquals(70L, noDv.getSelfSplitWeight(),
                "a DV-less native range weight is just the sub-range length");
    }

    @Test
    public void buildNativeRangesThreadsDenominatorDistinctFromFileSplitTarget() {
        // Positional-swap guard: the file-split target and the weight denominator are two adjacent long
        // params. Splitting must follow the FILE-SPLIT target while every sub-range carries the DENOMINATOR.
        // MUTATION: swapping the two args -> wrong split count AND wrong targetSplitSize -> red.
        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(
                new HashMap<>(), new RecordingPaimonCatalogOps());
        RawFile file = parquetRawFile("/data/part-0.parquet");        // length 100
        long fileSplitTarget = Math.max(1L, file.length() / 3);       // 33 -> >=2 sub-ranges
        long denominator = 64 * MB;                                   // numerically distinct from 33

        List<PaimonScanRange> ranges = provider.buildNativeRanges(
                file, null, "parquet", Collections.emptyMap(), Collections.emptyMap(),
                fileSplitTarget, denominator);

        Assertions.assertEquals(
                PaimonScanPlanProvider.computeFileSplitOffsets(file.length(), fileSplitTarget).size(),
                ranges.size(),
                "sub-splitting must follow the file-split target, not the denominator");
        Assertions.assertTrue(ranges.size() >= 2, "fixture must sub-split into >=2 ranges");
        for (PaimonScanRange r : ranges) {
            Assertions.assertEquals(denominator, r.getTargetSplitSize(),
                    "every native sub-range must carry the weight denominator");
        }
    }

    @Test
    public void buildNativeRangesCarriesDenominatorEvenWhenFileSplitSizeZero() {
        // Under COUNT(*) pushdown the file-split size is 0 (whole-file range), but the denominator is
        // computed independently, so the single range still gets a positive denominator (non-standard
        // weight) and the whole-file length as its weight.
        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(
                new HashMap<>(), new RecordingPaimonCatalogOps());
        RawFile file = parquetRawFile("/data/part-0.parquet");

        List<PaimonScanRange> ranges = provider.buildNativeRanges(
                file, null, "parquet", Collections.emptyMap(), Collections.emptyMap(), 0L, 64 * MB);

        Assertions.assertEquals(1, ranges.size(), "a non-positive target keeps the file whole");
        Assertions.assertEquals(64 * MB, ranges.get(0).getTargetSplitSize(),
                "a whole-file (count-pushdown) range still carries the weight denominator");
        Assertions.assertEquals(file.length(), ranges.get(0).getSelfSplitWeight(),
                "the whole-file range weight is the full file length");
    }

    @Test
    public void resolveSplitWeightDenominatorMatchesLegacyFormula() {
        // Legacy getFileSplitSize()>0 ? getFileSplitSize() : getMaxSplitSize() (PaimonScanNode:499);
        // getMaxSplitSize() = max_file_split_size, default 64MB.
        Map<String, String> withSplitSize = new HashMap<>();
        withSplitSize.put("file_split_size", "1234");
        Assertions.assertEquals(1234L,
                PaimonScanPlanProvider.resolveSplitWeightDenominator(sessionWithProps(withSplitSize)),
                "file_split_size>0 is used as the denominator");

        Assertions.assertEquals(64 * MB,
                PaimonScanPlanProvider.resolveSplitWeightDenominator(sessionWithProps(Collections.emptyMap())),
                "unset file_split_size falls back to max_file_split_size (64MB default)");

        Map<String, String> withMax = new HashMap<>();
        withMax.put("max_file_split_size", "777");
        Assertions.assertEquals(777L,
                PaimonScanPlanProvider.resolveSplitWeightDenominator(sessionWithProps(withMax)),
                "unset file_split_size uses the configured max_file_split_size");
    }

    // ============= FIX-B-R2-be: memoize the schema-evolution dict's per-schema-id reads =============

    /** A real single-schema (schema_id=0) keyed FileStoreTable under the @TempDir warehouse. */
    private static FileStoreTable createSingleSchemaTable(Catalog catalog) throws Exception {
        catalog.createDatabase("db", false);
        Identifier id = Identifier.create("db", "t");
        catalog.createTable(id, Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .primaryKey("id")
                .option("bucket", "1")
                .build(), false);
        return (FileStoreTable) catalog.getTable(id);
    }

    private static PaimonTableHandle plainHandle() {
        return new PaimonTableHandle("db", "t", Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void schemaEvolutionDictPopulatesSharedMemo(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            FileStoreTable base = createSingleSchemaTable(catalog);
            RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
            ops.table = base;
            PaimonTableHandle handle = plainHandle();
            PaimonSchemaAtMemo memo = new PaimonSchemaAtMemo(PaimonSchemaAtMemo.DEFAULT_MAX_SIZE);
            PaimonScanPlanProvider provider =
                    new PaimonScanPlanProvider(Collections.emptyMap(), ops, null, memo);

            provider.getScanNodeProperties(null, handle, Collections.emptyList(), Optional.empty());

            // WHY: the K committed-schema reads of the dict build (listAllIds loop) must go through the
            // shared memo so repeated scans don't re-read the schema files (FIX-B-R2-be); the -1 current
            // entry does NOT (it reads the live table). K=1 for a fresh single-schema table. MUTATION:
            // reading schemaManager.schema(id) directly -> memo never populated -> size 0 -> red.
            int k = base.schemaManager().listAllIds().size();
            Assertions.assertEquals(1, k, "a fresh table has exactly one committed schema (id 0)");
            Assertions.assertEquals(k, memo.size(),
                    "every committed-schema read in the dict build must populate the shared memo");
        }
    }

    @Test
    public void schemaEvolutionDictReadsFromMemoOnHit(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            FileStoreTable base = createSingleSchemaTable(catalog);
            PaimonTableHandle handle = plainHandle();

            // The real (unseeded) dict.
            RecordingPaimonCatalogOps opsReal = new RecordingPaimonCatalogOps();
            opsReal.table = base;
            String encodedReal = new PaimonScanPlanProvider(Collections.emptyMap(), opsReal, null,
                    new PaimonSchemaAtMemo(PaimonSchemaAtMemo.DEFAULT_MAX_SIZE))
                    .getScanNodeProperties(null, handle, Collections.emptyList(), Optional.empty())
                    .get("paimon.schema_evolution");

            // Pre-seed a SHARED memo for (handle, schema 0) with a SENTINEL whose fields differ from the
            // real schema, so a cache HIT is positively observable in the emitted dict.
            PaimonSchemaAtMemo seeded = new PaimonSchemaAtMemo(PaimonSchemaAtMemo.DEFAULT_MAX_SIZE);
            seeded.getOrLoad(handle, 0L, () -> new PaimonCatalogOps.PaimonSchemaSnapshot(
                    Collections.singletonList(new DataField(0, "sentinel_from_memo", DataTypes.INT())),
                    Collections.emptyList(), Collections.emptyList()));
            RecordingPaimonCatalogOps opsSeeded = new RecordingPaimonCatalogOps();
            opsSeeded.table = base;
            String encodedSeeded = new PaimonScanPlanProvider(Collections.emptyMap(), opsSeeded, null, seeded)
                    .getScanNodeProperties(null, handle, Collections.emptyList(), Optional.empty())
                    .get("paimon.schema_evolution");

            // WHY: the dict build must RETURN the cached value for schema 0 (skip the real
            // schemaManager.schema(0) read), so the seeded sentinel field surfaces in the dict and the
            // encoded string differs from the unseeded real dict. MUTATION: reading directly (bypassing the
            // memo) -> the seed is ignored -> encodedSeeded == encodedReal -> red.
            Assertions.assertNotNull(encodedReal);
            Assertions.assertNotEquals(encodedReal, encodedSeeded,
                    "a pre-seeded memo entry must surface in the dict, proving the build read from the memo");
        }
    }

    @Test
    public void schemaEvolutionDictByteIdenticalWithMemo(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            FileStoreTable base = createSingleSchemaTable(catalog);
            PaimonTableHandle handle = plainHandle();

            RecordingPaimonCatalogOps opsA = new RecordingPaimonCatalogOps();
            opsA.table = base;
            // 2-arg ctor: fresh per-instance memo => first build is a direct read => pre-fix behavior.
            String encodedA = new PaimonScanPlanProvider(Collections.emptyMap(), opsA)
                    .getScanNodeProperties(null, handle, Collections.emptyList(), Optional.empty())
                    .get("paimon.schema_evolution");

            RecordingPaimonCatalogOps opsB = new RecordingPaimonCatalogOps();
            opsB.table = base;
            // 4-arg ctor with a shared memo (first build is also a direct read, then cached).
            String encodedB = new PaimonScanPlanProvider(Collections.emptyMap(), opsB, null,
                    new PaimonSchemaAtMemo(PaimonSchemaAtMemo.DEFAULT_MAX_SIZE))
                    .getScanNodeProperties(null, handle, Collections.emptyList(), Optional.empty())
                    .get("paimon.schema_evolution");

            // WHY: the memo changes only HOW fields are read, never WHAT is emitted -> the dict must be
            // byte-identical to the non-memo path (no order/dedup/membership change -> zero BE-crash
            // surface). MUTATION: the memo altering the emitted entries -> encodedA != encodedB -> red.
            Assertions.assertNotNull(encodedA);
            Assertions.assertEquals(encodedA, encodedB,
                    "the memoized dict must be byte-identical to the non-memo (pre-fix) emission");
        }
    }

    @Test
    public void schemaEvolutionDictSkippedUnderForceJniLeavesMemoEmpty(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            FileStoreTable base = createSingleSchemaTable(catalog);

            // (a) a force-jni handle (binlog): the whole dict is gated off, so the memo must stay empty.
            RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
            ops.sysTable = base;
            ops.table = base;
            PaimonTableHandle binlog = PaimonTableHandle.forSystemTable("db", "t", "binlog", true);
            PaimonSchemaAtMemo memo = new PaimonSchemaAtMemo(PaimonSchemaAtMemo.DEFAULT_MAX_SIZE);
            Map<String, String> props = new PaimonScanPlanProvider(Collections.emptyMap(), ops, null, memo)
                    .getScanNodeProperties(null, binlog, Collections.emptyList(), Optional.empty());
            Assertions.assertFalse(props.containsKey("paimon.schema_evolution"),
                    "a force-jni handle skips the schema dict");
            Assertions.assertEquals(0, memo.size(),
                    "force-jni must not consult/populate the schema memo (guards a read moved above the gate)");

            // (b) force_jni_scanner=true session on a plain handle: same gate.
            RecordingPaimonCatalogOps ops2 = new RecordingPaimonCatalogOps();
            ops2.table = base;
            PaimonSchemaAtMemo memo2 = new PaimonSchemaAtMemo(PaimonSchemaAtMemo.DEFAULT_MAX_SIZE);
            Map<String, String> props2 = new PaimonScanPlanProvider(Collections.emptyMap(), ops2, null, memo2)
                    .getScanNodeProperties(
                            sessionWithProps(Collections.singletonMap("force_jni_scanner", "true")),
                            plainHandle(), Collections.emptyList(), Optional.empty());
            Assertions.assertFalse(props2.containsKey("paimon.schema_evolution"));
            Assertions.assertEquals(0, memo2.size());
        }
    }

    @Test
    public void getScanPlanProviderInjectsSharedSchemaMemo(@TempDir Path warehouse) {
        Map<String, String> props = new HashMap<>();
        props.put("warehouse", warehouse.toUri().toString());
        PaimonConnector connector = new PaimonConnector(props, new RecordingConnectorContext());
        PaimonScanPlanProvider p1 = (PaimonScanPlanProvider) connector.getScanPlanProvider();
        PaimonScanPlanProvider p2 = (PaimonScanPlanProvider) connector.getScanPlanProvider();

        // WHY: the cross-scan memo benefit hinges on getScanPlanProvider() injecting the connector's SHARED
        // memo (the 4-arg ctor). MUTATION: dropping the schemaAtMemo arg -> each provider gets a fresh memo
        // -> not the same instance -> red (and the fix would silently no-op cross-scan memoization).
        Assertions.assertSame(p1.schemaAtMemoForTest(), p2.schemaAtMemoForTest(),
                "getScanPlanProvider() must inject the connector's shared schema memo, not a fresh one");
    }
}
