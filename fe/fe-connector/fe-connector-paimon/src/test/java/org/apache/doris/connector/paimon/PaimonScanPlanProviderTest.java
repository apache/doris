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
import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
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
                file, dv, "parquet", Collections.emptyMap());

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
                parquetRawFile("oss://bkt/a/part-0.parquet"), null, "parquet", Collections.emptyMap());

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
                parquetRawFile("oss://bkt/a/part-0.parquet"), null, "parquet", Collections.emptyMap());

        // MUTATION: NPE on null context, or fabricating a normalized path from nothing -> red.
        Assertions.assertEquals("oss://bkt/a/part-0.parquet", range.getPath().orElse(null));
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

    /** A ConnectorContext whose getBackendStorageProperties / vendStorageCredentials return fixed
     * normalized maps. The engine's real StorageProperties normalization is exercised by the fe-core
     * DefaultConnectorContextBackendStoragePropsTest / DefaultConnectorContextVendTest; here we pin the
     * connector wiring (overlay order + that the raw catalog aliases are NOT shipped). */
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
            public Map<String, String> getBackendStorageProperties() {
                return backendStatic;
            }

            @Override
            public Map<String, String> vendStorageCredentials(Map<String, String> raw) {
                return vended;
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
}
