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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;
import org.apache.doris.connector.api.scan.ConnectorSplitSource;
import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.BackendStorageKind;
import org.apache.doris.filesystem.properties.BackendStorageProperties;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;
import org.apache.doris.thrift.schema.external.TFieldPtr;

import com.google.common.collect.ImmutableSet;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

/**
 * Tests for {@link IcebergScanPlanProvider}. T01 pinned the capability constants + that {@code planScan}
 * resolves the table through the {@link IcebergCatalogOps} seam inside the auth context. T02 adds the real
 * split planning: predicate pushdown ({@link IcebergPredicateConverter}), {@code createTableScan}, and
 * {@code TableScanUtil}-based split enumeration. The provider is exercised against a REAL in-memory iceberg
 * table ({@link InMemoryCatalog} + appended {@link DataFile} metadata — no Parquet I/O, fully offline) so
 * {@code table.newScan().planFiles()} returns genuine {@code FileScanTask}s. No Mockito.
 */
public class IcebergScanPlanProviderTest {

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    private static final Schema PART_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "p", Types.IntegerType.get()));

    // --- in-memory iceberg table helpers (offline; DataFile metadata only, no real data files) ---

    private static Table createTable(String name, Schema schema, PartitionSpec spec) {
        return createTable(name, schema, spec, Collections.emptyMap());
    }

    private static Table createTable(String name, Schema schema, PartitionSpec spec, Map<String, String> props) {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        return catalog.createTable(TableIdentifier.of("db1", name), schema, spec, null, props);
    }

    private static DataFile dataFile(PartitionSpec spec, String path, long sizeBytes, List<Long> splitOffsets,
            String partitionPath) {
        return dataFile(spec, path, sizeBytes, splitOffsets, partitionPath, FileFormat.PARQUET);
    }

    private static DataFile dataFile(PartitionSpec spec, String path, long sizeBytes, List<Long> splitOffsets,
            String partitionPath, FileFormat format) {
        DataFiles.Builder builder = DataFiles.builder(spec)
                .withPath(path)
                .withFileSizeInBytes(sizeBytes)
                .withRecordCount(Math.max(1, sizeBytes / 100))
                .withFormat(format);
        if (splitOffsets != null) {
            builder.withSplitOffsets(splitOffsets);
        }
        if (partitionPath != null) {
            builder.withPartitionPath(partitionPath);
        }
        return builder.build();
    }

    /** Run a range's BE-param population end-to-end (the generic node pre-sets table_format_type). */
    private static TFileRangeDesc populate(ConnectorScanRange range) {
        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        formatDesc.setTableFormatType(range.getTableFormatType());
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        range.populateRangeParams(formatDesc, rangeDesc);
        rangeDesc.setTableFormatParams(formatDesc);
        return rangeDesc;
    }

    private static ConnectorScanRange byPath(List<ConnectorScanRange> ranges, String suffix) {
        return ranges.stream().filter(r -> r.getPath().get().endsWith(suffix)).findFirst()
                .orElseThrow(() -> new AssertionError("no range ending in " + suffix));
    }

    private static RecordingIcebergCatalogOps opsReturning(Table table) {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = table;
        return ops;
    }

    private static ConnectorExpression eqInt(String col, int value) {
        return new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef(col, ConnectorType.of("INT")),
                new ConnectorLiteral(ConnectorType.of("INT"), (long) value));
    }

    // --- T01 capability + seam/auth tests (unchanged contract; now backed by a real empty table) ---

    @Test
    public void getScanRangeTypeIsFileScan() {
        IcebergScanPlanProvider provider =
                new IcebergScanPlanProvider(Collections.emptyMap(), new RecordingIcebergCatalogOps());
        // WHY: iceberg is file-based, so BE must build a TFileScanRange. MUTATION: JDBC_SCAN / CUSTOM -> red.
        Assertions.assertEquals(ConnectorScanRangeType.FILE_SCAN, provider.getScanRangeType());
    }

    @Test
    public void ignorePartitionPruneShortCircuitIsTrue() {
        IcebergScanPlanProvider provider =
                new IcebergScanPlanProvider(Collections.emptyMap(), new RecordingIcebergCatalogOps());
        // WHY: iceberg is predicate-driven — it re-plans through its own SDK from the pushed predicate and
        // never consults requiredPartitions (same as the legacy IcebergScanNode / paimon). So a GENUINE FE
        // prune-to-zero must scan-all rather than short-circuit to zero rows. MUTATION: default false -> red.
        Assertions.assertTrue(provider.ignorePartitionPruneShortCircuit());
    }

    @Test
    public void supportsSystemTableTimeTravelIsTrue() {
        IcebergScanPlanProvider provider =
                new IcebergScanPlanProvider(Collections.emptyMap(), new RecordingIcebergCatalogOps());
        // WHY: iceberg metadata tables legally time-travel (t$snapshots FOR TIME AS OF ..., t$files@branch).
        // legacy IcebergScanNode.createTableScan honors useRef/useSnapshot for sys tables with no
        // isSystemTable gate, and this provider retains+honors the pin. So the generic
        // PluginDrivenScanNode sys-table guard must let pinned iceberg sys reads through — unlike paimon,
        // whose binlog/audit_log sys tables keep the SPI default false. MUTATION: drop the override
        // (inherit default false) -> the fe-core guard would reject t$snapshots FOR TIME AS OF -> red.
        Assertions.assertTrue(provider.supportsSystemTableTimeTravel());
    }

    @Test
    public void planScanResolvesTableViaSeamAndEmptyTableReturnsNoSplits() {
        // An empty table (no snapshot) plans no files -> no ranges; proves the (db, table) coordinates were
        // threaded through the seam to loadTable, and the real scan path tolerates an empty table.
        RecordingIcebergCatalogOps ops = opsReturning(createTable("t1", SCHEMA, PartitionSpec.unpartitioned()));
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), ops);

        List<ConnectorScanRange> ranges = provider.planScan(
                null, new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty());

        Assertions.assertTrue(ranges.isEmpty());
        Assertions.assertEquals("db1", ops.lastLoadDb);
        Assertions.assertEquals("t1", ops.lastLoadTable);
    }

    @Test
    public void planScanResolvesTableInsideAuthContext() {
        // The remote loadTable must sit INSIDE context.executeAuthenticated so the FE-injected Kerberos UGI
        // applies (mirrors IcebergConnectorMetadata + paimon's PaimonScanPlanProvider.resolveTable).
        RecordingIcebergCatalogOps ops = opsReturning(createTable("t1", SCHEMA, PartitionSpec.unpartitioned()));
        RecordingConnectorContext context = new RecordingConnectorContext();
        IcebergScanPlanProvider provider =
                new IcebergScanPlanProvider(Collections.emptyMap(), ops, context);

        provider.planScan(null, new IcebergTableHandle("db1", "t1"),
                Collections.emptyList(), Optional.empty());

        // MUTATION: resolving the table OUTSIDE the auth wrap -> authCount stays 0 -> red.
        Assertions.assertEquals(1, context.authCount);
        Assertions.assertEquals("db1", ops.lastLoadDb);
    }

    // --- T02 split-enumeration + predicate-pushdown tests ---

    @Test
    public void planScanEnumeratesOneRangePerDataFile() {
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 1024, null, null))
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f2.parquet", 2048, null, null))
                .commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        List<ConnectorScanRange> ranges = provider.planScan(
                null, new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty());

        // Small files (< target split size) are not sub-split: one range per data file carrying the file's
        // path / size and the whole-file byte range. MUTATION: returning emptyList (T01 skeleton) -> red.
        Assertions.assertEquals(2, ranges.size());
        ranges.sort((a, b) -> a.getPath().get().compareTo(b.getPath().get()));
        Assertions.assertEquals("s3://b/db/t1/f1.parquet", ranges.get(0).getPath().get());
        Assertions.assertEquals(0L, ranges.get(0).getStart());
        Assertions.assertEquals(1024L, ranges.get(0).getLength());
        Assertions.assertEquals(1024L, ranges.get(0).getFileSize());
        Assertions.assertEquals(2048L, ranges.get(1).getLength());
    }

    @Test
    public void planScanRewriteFileScopeKeepsOnlyRawScopedFiles() {
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        // oss:// data-file paths: the context normalizes oss:// -> s3:// for the BE-facing range path, so this
        // test proves the rewrite scope matches the RAW iceberg path (oss://) and NOT the normalized BE path.
        table.newAppend()
                .appendFile(dataFile(table.spec(), "oss://b/db/t1/f1.parquet", 1024, null, null))
                .appendFile(dataFile(table.spec(), "oss://b/db/t1/f2.parquet", 2048, null, null))
                .appendFile(dataFile(table.spec(), "oss://b/db/t1/f3.parquet", 4096, null, null))
                .commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(
                Collections.emptyMap(), opsReturning(table), new RecordingConnectorContext());

        // A rewrite group bin-packed to f1 + f3 only; f2 must be dropped.
        IcebergTableHandle scoped = new IcebergTableHandle("db1", "t1").withRewriteFileScope(
                ImmutableSet.of("oss://b/db/t1/f1.parquet", "oss://b/db/t1/f3.parquet"));
        List<ConnectorScanRange> ranges = provider.planScan(
                null, scoped, Collections.emptyList(), Optional.empty());

        // WHY: each rewrite group's INSERT-SELECT must scan EXACTLY its bin-packed files, identified by the raw
        // iceberg path. MUTATION: dropping the `scope != null && !scope.contains(...)` guard -> f2 leaks in
        // (3 files) -> red, an over-read whose RewriteFiles commit would replace more than the group (duplicate
        // rows). MUTATION (the normalization landmine): matching the normalized BE path (range .path(), s3://)
        // instead of dataFile.path() (oss://) -> the oss:// scope matches NOTHING -> 0 ranges -> red.
        Set<String> keptRawPaths = ranges.stream()
                .map(r -> ((IcebergScanRange) r).getOriginalPath())
                .collect(ImmutableSet.toImmutableSet());
        Assertions.assertEquals(
                ImmutableSet.of("oss://b/db/t1/f1.parquet", "oss://b/db/t1/f3.parquet"), keptRawPaths,
                "rewrite scope must keep ONLY its files, matched by raw path; f2 dropped");
        // The kept files are still normalized for BE (the scope filter runs on the raw path BEFORE normalize).
        Assertions.assertTrue(ranges.stream().allMatch(r -> r.getPath().get().startsWith("s3://")),
                "kept ranges still carry the scheme-normalized BE path");
    }

    @Test
    public void planScanNullRewriteScopeReadsAllFiles() {
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 1024, null, null))
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f2.parquet", 2048, null, null))
                .commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        // WHY: a bare handle (no rewrite scope) is EVERY normal scan; it must read all files, not zero. MUTATION:
        // the scope guard firing on a null scope (e.g. `scope.isEmpty()` instead of `scope != null`) -> 0 ranges
        // -> red.
        List<ConnectorScanRange> ranges = provider.planScan(
                null, new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty());
        Assertions.assertEquals(2, ranges.size());
    }

    @Test
    public void planScanSplitsLargeFileByFileSplitSizeSessionVar() {
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        long mb = 1024L * 1024L;
        // 96MB file with row-group split offsets at 0 / 32MB / 64MB.
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/big.parquet", 96 * mb,
                        Arrays.asList(0L, 32 * mb, 64 * mb), null))
                .commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));
        // file_split_size = 32MB forces splitting at that granularity (mirrors SessionVariable override path).
        ConnectorSession session = new FakeScanSession("UTC",
                Collections.singletonMap("file_split_size", Long.toString(32 * mb)));

        List<ConnectorScanRange> ranges = provider.planScan(
                session, new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty());

        // The iceberg SDK TableScanUtil tiles the file into contiguous byte ranges covering the whole file.
        // MUTATION: ignoring file_split_size (always whole file) -> single range -> red.
        Assertions.assertTrue(ranges.size() > 1, "expected the 96MB file to split, got " + ranges.size());
        long expectedStart = 0;
        long totalLength = 0;
        for (ConnectorScanRange r : ranges) {
            Assertions.assertEquals("s3://b/db/t1/big.parquet", r.getPath().get());
            Assertions.assertEquals(96 * mb, r.getFileSize());
            Assertions.assertEquals(expectedStart, r.getStart(), "ranges must tile contiguously from 0");
            expectedStart += r.getLength();
            totalLength += r.getLength();
        }
        Assertions.assertEquals(96 * mb, totalLength, "the split ranges must cover the whole file exactly");
    }

    // ── M-2: size-proportional BE scheduling weight (selfSplitWeight / targetSplitSize) ──

    @Test
    public void planScanRangesCarrySizeProportionalWeight() {
        // M-2: each data-file range must carry a size-based weight numerator (selfSplitWeight == the split byte
        // length when there are no deletes) and a positive scan-level denominator (targetSplitSize ==
        // determineTargetFileSplitSize) so FederationBackendPolicy schedules by bytes, not by split count. Two
        // differently-sized files -> different weights, identical denominator -> proportional (NOT standard()).
        // MUTATION: dropping .selfSplitWeight / .targetSplitSize in buildRange (range stays -1/-1) -> red.
        long mb = 1024L * 1024L;
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 1024, null, null))
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f2.parquet", 2048, null, null))
                .commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        List<ConnectorScanRange> ranges = provider.planScan(
                null, new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty());

        ranges.sort((a, b) -> a.getPath().get().compareTo(b.getPath().get()));
        // selfSplitWeight == the whole-file byte length (small files are not sub-split, no deletes). The two
        // differ -> the weight tracks bytes. MUTATION: dropping the += delete or the .selfSplitWeight set -> red.
        Assertions.assertEquals(1024L, ranges.get(0).getSelfSplitWeight());
        Assertions.assertEquals(2048L, ranges.get(1).getSelfSplitWeight());
        // denominator == determineTargetFileSplitSize: a ~3KB table stays at the 32MB max_initial_file_split_size
        // default, identical across ranges so the weights are comparable. A positive denominator is also what
        // flips PluginDrivenSplit off SplitWeight.standard(). MUTATION: -1 (unset) -> red.
        Assertions.assertEquals(32 * mb, ranges.get(0).getTargetSplitSize());
        Assertions.assertEquals(32 * mb, ranges.get(1).getTargetSplitSize());
    }

    @Test
    public void planScanSelfSplitWeightIncludesDeleteFileSizes() {
        // M-2 parity: legacy IcebergSplit.setDeleteFileFilters adds each merge-on-read delete file's byte size to
        // selfSplitWeight (a data file carrying deletes costs more to read). data 512 + one 128-byte position
        // delete -> weight 640. MUTATION: selfSplitWeight = task.length() only (drop the += delete size) -> 512.
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned(),
                Collections.singletonMap("format-version", "2"));
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 512, null, null))
                .commit();
        table.newRowDelta()
                .addDeletes(positionDeleteFile("s3://b/db/t1/pos.parquet", FileFormat.PARQUET, null, null))
                .commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        List<ConnectorScanRange> ranges = provider.planScan(new FakeScanSession("UTC", Collections.emptyMap()),
                new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty());

        // The position delete attaches to f1's scan task (higher sequence number, same partition).
        Assertions.assertEquals(1, ranges.size());
        Assertions.assertEquals(640L, ranges.get(0).getSelfSplitWeight(),
                "selfSplitWeight must be data-file length (512) + delete file size (128)");
    }

    @Test
    public void planScanFileSplitSizeUsesFileSplitSizeAsWeightDenominator() {
        // M-2: in the file_split_size>0 path the splits are sliced to that granularity, so file_split_size is the
        // weight denominator. (Legacy left targetSplitSize=0 here -> divide-by-zero -> clamp 1.0; the generic
        // PluginDrivenSplit guards target>0, and file_split_size gives correct proportional weighting.) Using
        // 16MB != the 32MB heuristic default pins that the path does NOT fall back to determineTargetFileSplitSize.
        // MUTATION: denominator -1 (unset) or determineTargetFileSplitSize (32MB) -> red.
        long mb = 1024L * 1024L;
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/big.parquet", 96 * mb,
                        Arrays.asList(0L, 32 * mb, 64 * mb), null))
                .commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));
        ConnectorSession session = new FakeScanSession("UTC",
                Collections.singletonMap("file_split_size", Long.toString(16 * mb)));

        List<ConnectorScanRange> ranges = provider.planScan(
                session, new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty());

        Assertions.assertFalse(ranges.isEmpty());
        for (ConnectorScanRange r : ranges) {
            Assertions.assertEquals(16 * mb, r.getTargetSplitSize(),
                    "file_split_size path must use file_split_size as the weight denominator");
        }
    }

    @Test
    public void planScanPushesPredicateAndPrunesPartition() {
        PartitionSpec spec = PartitionSpec.builderFor(PART_SCHEMA).identity("p").build();
        Table table = createTable("pt", PART_SCHEMA, spec);
        table.newAppend()
                .appendFile(dataFile(spec, "s3://b/db/pt/p1.parquet", 512, null, "p=1"))
                .appendFile(dataFile(spec, "s3://b/db/pt/p2.parquet", 512, null, "p=2"))
                .commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        // WHERE p = 1 must push to the scan and prune the p=2 file -> only the p=1 data file is enumerated.
        // This proves the converted predicate reaches scan.filter and is honoured by iceberg planning.
        // MUTATION: not applying the filter (scan all) -> 2 ranges -> red.
        List<ConnectorScanRange> filtered = provider.planScan(
                null, new IcebergTableHandle("db1", "pt"), Collections.emptyList(),
                Optional.of(eqInt("p", 1)));
        Assertions.assertEquals(1, filtered.size());
        Assertions.assertEquals("s3://b/db/pt/p1.parquet", filtered.get(0).getPath().get());

        // Sanity: with no predicate, both partitions' files are enumerated.
        List<ConnectorScanRange> all = provider.planScan(
                null, new IcebergTableHandle("db1", "pt"), Collections.emptyList(), Optional.empty());
        Assertions.assertEquals(2, all.size());
    }

    @Test
    public void planScanUnpushablePredicateScansAllFiles() {
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 1024, null, null))
                .commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        // A predicate the converter drops (id = 'abc' : string -> INTEGER fails) leaves no filter on the scan,
        // so all files are scanned (safe over-approximation) rather than crashing or pruning everything.
        ConnectorExpression unpushable = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("id", ConnectorType.of("INT")),
                new ConnectorLiteral(ConnectorType.of("VARCHAR"), "abc"));
        List<ConnectorScanRange> ranges = provider.planScan(
                null, new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.of(unpushable));
        Assertions.assertEquals(1, ranges.size());
    }

    @Test
    public void resolveSessionZoneHonorsDorisTimezoneAliases() {
        // Doris stores SET time_zone='CST' un-canonicalized; legacy resolves it via the alias map to +08:00
        // (Asia/Shanghai), NOT America/Chicago. A plain ZoneId.of("CST") would throw -> UTC fallback ->
        // 8h-shifted timestamptz pushdown -> wrong file pruning. MUTATION: dropping the alias map -> red.
        Assertions.assertEquals(ZoneId.of("Asia/Shanghai"),
                IcebergScanPlanProvider.resolveSessionZone(new FakeScanSession("CST", Collections.emptyMap())));
        Assertions.assertEquals(ZoneId.of("Asia/Shanghai"),
                IcebergScanPlanProvider.resolveSessionZone(new FakeScanSession("PRC", Collections.emptyMap())));
        // A JDK SHORT_ID alias still resolves (mirrors TimeUtils putAll(ZoneId.SHORT_IDS)).
        Assertions.assertEquals(ZoneId.of(ZoneId.SHORT_IDS.get("EST")),
                IcebergScanPlanProvider.resolveSessionZone(new FakeScanSession("EST", Collections.emptyMap())));
        // A plain IANA name resolves as-is.
        Assertions.assertEquals(ZoneId.of("America/New_York"),
                IcebergScanPlanProvider.resolveSessionZone(
                        new FakeScanSession("America/New_York", Collections.emptyMap())));
        // null / blank / genuinely-invalid -> UTC (no crash).
        Assertions.assertEquals(ZoneOffset.UTC,
                IcebergScanPlanProvider.resolveSessionZone(new FakeScanSession(null, Collections.emptyMap())));
        Assertions.assertEquals(ZoneOffset.UTC,
                IcebergScanPlanProvider.resolveSessionZone(
                        new FakeScanSession("Not/AZone", Collections.emptyMap())));
    }

    // --- T03 BE-ready range params: per-file carriers + path_partition_keys + native format ---

    @Test
    public void planScanPopulatesPerFilePartitionAndFormatCarriers() {
        PartitionSpec spec = PartitionSpec.builderFor(PART_SCHEMA).identity("p").build();
        Table table = createTable("pt", PART_SCHEMA, spec);
        table.newAppend()
                .appendFile(dataFile(spec, "s3://b/db/pt/p=1/a.parquet", 512, null, "p=1", FileFormat.PARQUET))
                .appendFile(dataFile(spec, "s3://b/db/pt/p=2/b.orc", 512, null, "p=2", FileFormat.ORC))
                .commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        List<ConnectorScanRange> ranges = provider.planScan(
                null, new IcebergTableHandle("db1", "pt"), Collections.emptyList(), Optional.empty());
        Assertions.assertEquals(2, ranges.size());

        // parquet file -> per-file FORMAT_PARQUET + its own partition spec-id/data-json/columns-from-path.
        // MUTATION: T02's bare range (no carriers) -> iceberg_params unset / format JNI -> red.
        TFileRangeDesc parquet = populate(byPath(ranges, "a.parquet"));
        TIcebergFileDesc fdp = parquet.getTableFormatParams().getIcebergParams();
        Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET, parquet.getFormatType());
        Assertions.assertEquals(2, fdp.getFormatVersion());
        Assertions.assertEquals("s3://b/db/pt/p=1/a.parquet", fdp.getOriginalFilePath());
        Assertions.assertTrue(fdp.isSetPartitionSpecId());
        Assertions.assertEquals("[\"1\"]", fdp.getPartitionDataJson());
        Assertions.assertEquals(Collections.singletonList("p"), parquet.getColumnsFromPathKeys());
        Assertions.assertEquals(Collections.singletonList("1"), parquet.getColumnsFromPath());
        Assertions.assertEquals(Collections.singletonList(false), parquet.getColumnsFromPathIsNull());

        // orc file -> per-file FORMAT_ORC (proves the format is taken per data file, not table-uniform) +
        // its own partition value "2".
        TFileRangeDesc orc = populate(byPath(ranges, "b.orc"));
        Assertions.assertEquals(TFileFormatType.FORMAT_ORC, orc.getFormatType());
        Assertions.assertEquals("[\"2\"]", orc.getTableFormatParams().getIcebergParams().getPartitionDataJson());
        Assertions.assertEquals(Collections.singletonList("2"), orc.getColumnsFromPath());
    }

    @Test
    public void getScanNodePropertiesEmitsPathPartitionKeysAndRealDataFormat() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "P", Types.IntegerType.get()),
                Types.NestedField.required(3, "region", Types.StringType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("P").identity("region").build();
        Table table = createTable("pt", schema, spec);
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        Map<String, String> props = provider.getScanNodeProperties(
                null, new IcebergTableHandle("db1", "pt"), Collections.emptyList(), Optional.empty());

        // path_partition_keys = case-preserved, comma-joined identity columns (the CI #968880 double-fill guard);
        // MUTATION: omitting path_partition_keys -> BE double-fills partition columns -> DCHECK -> this red.
        // MUTATION: re-lowercasing "P" -> "p,region" != "P,region" -> red.
        Assertions.assertEquals("P,region", props.get("path_partition_keys"));
        // A base table MUST NOT report jni here: BE reads the scan-level format to pick FileScannerV2 vs V1
        // (_should_use_file_scanner_v2) before it can see any split, so "jni" would pin every iceberg scan to
        // the V1 tree -- the only one carrying TableSchemaChangeHelper, whose StructNode DCHECK then SIGABRTs
        // the BE on a Top-N lazy-materialization rowid slot (TeamCity 995122). No write.format.default and no
        // snapshot on this fixture -> IcebergUtils.getFileFormat parity default = parquet.
        // MUTATION: reverting to a hardcoded "jni" -> red here, and V2 is silently lost in production.
        Assertions.assertEquals("parquet", props.get("file_format_type"));
    }

    @Test
    public void getScanNodePropertiesResolvesOrcTableFormat() {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Table table = createTable("orct", schema, PartitionSpec.unpartitioned(),
                Collections.singletonMap(TableProperties.DEFAULT_FILE_FORMAT, "orc"));
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        Map<String, String> props = provider.getScanNodeProperties(
                null, new IcebergTableHandle("db1", "orct"), Collections.emptyList(), Optional.empty());

        // Pins that the scan-level format is RESOLVED from the table, not a constant: an orc table must say orc
        // so BE's genSlotToSchemaIdMapForOrc / V2 selection see the truth (legacy getFileFormatType parity).
        // MUTATION: hardcoding "parquet" instead of IcebergWriterHelper.getFileFormat(table) -> red.
        Assertions.assertEquals("orc", props.get("file_format_type"));
    }

    @Test
    public void getScanNodePropertiesOmitsPathPartitionKeysForUnpartitionedTable() {
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        Map<String, String> props = provider.getScanNodeProperties(
                null, new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty());

        // No identity partition columns -> the key must be absent (NOT an empty string, which the parent would
        // split into a single "" key). MUTATION: always emitting the key -> red.
        Assertions.assertFalse(props.containsKey("path_partition_keys"));
    }

    @Test
    public void getScanNodePropertiesEmitsFieldIdSchemaEvolutionDictKeyedOffRequestedColumns() throws Exception {
        // T06: the provider must emit iceberg.schema_evolution (the field-id dict) UNCONDITIONALLY, keyed off the
        // pruned requested columns, and populateScanLevelParams must round-trip it onto the real params. Without
        // it BE name-matches schema-evolved files (NULL/garbage on rename) or DCHECK-aborts. MUTATION: not
        // emitting the prop -> absent -> red. MUTATION: keying off all columns -> the entry includes "name" -> red.
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));
        List<ConnectorColumnHandle> columns =
                Collections.singletonList(new IcebergColumnHandle("id", 1));

        Map<String, String> props = provider.getScanNodeProperties(
                null, new IcebergTableHandle("db1", "t1"), columns, Optional.empty());
        Assertions.assertTrue(props.containsKey("iceberg.schema_evolution"));

        // Round-trip through populateScanLevelParams (the exact path PluginDrivenScanNode drives).
        TFileScanRangeParams params = new TFileScanRangeParams();
        provider.populateScanLevelParams(params, props);
        Assertions.assertEquals(-1L, params.getCurrentSchemaId());
        Assertions.assertEquals(1, params.getHistorySchemaInfoSize());
        // Keyed off the requested column "id" only (CI #969249: the -1 entry == the scan slots).
        Assertions.assertEquals(1, params.getHistorySchemaInfo().get(0).getRootField().getFieldsSize());
        Assertions.assertEquals("id", params.getHistorySchemaInfo().get(0).getRootField()
                .getFields().get(0).getFieldPtr().getName());
    }

    @Test
    public void getScanNodePropertiesForcesEqualityDeleteKeyColumnIntoDict() throws Exception {
        // #65502: an equality-delete KEY column is a hidden scan dependency — BE resolves a key that is missing
        // from an OLD data file via the field-id dict (to get the column type + iceberg initial default); without
        // the entry it backfills the key as NULL and mis-applies the delete. So a query that does NOT project the
        // key column must still ship it in the dict. Here the table declares identifier field "id" (what an
        // equality-delete writer keys on); projecting ONLY "name", the emitted dict must carry BOTH "name" AND the
        // unprojected "id". MUTATION: keying the normal dict off the pruned columns verbatim (dropping
        // withEqualityDeleteKeyColumns) -> "id" absent -> red.
        Schema schema = new Schema(
                Arrays.asList(
                        Types.NestedField.required(1, "id", Types.IntegerType.get()),
                        Types.NestedField.optional(2, "name", Types.StringType.get())),
                Collections.singleton(1));
        Table table = createTable("eqdel", schema, PartitionSpec.unpartitioned(),
                Collections.singletonMap("format-version", "2"));
        // Sanity: the identifier field must survive table creation (iceberg reassigns ids but keeps the key).
        Assertions.assertFalse(table.schema().identifierFieldIds().isEmpty(),
                "the declared identifier field must be preserved on the created table");
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        // Project ONLY the non-key data column "name".
        List<ConnectorColumnHandle> columns = Collections.singletonList(new IcebergColumnHandle("name", 2));
        Map<String, String> props = provider.getScanNodeProperties(
                null, new IcebergTableHandle("db1", "eqdel"), columns, Optional.empty());

        TFileScanRangeParams params = new TFileScanRangeParams();
        provider.populateScanLevelParams(params, props);
        List<String> top = new ArrayList<>();
        for (TFieldPtr ptr : params.getHistorySchemaInfo().get(0).getRootField().getFields()) {
            top.add(ptr.getFieldPtr().getName());
        }
        Assertions.assertTrue(top.contains("name"), "the projected column must be present, got " + top);
        Assertions.assertTrue(top.contains("id"),
                "the unprojected equality-delete key column must be force-included (#65502), got " + top);
    }

    @Test
    public void getScanNodePropertiesEmitsSchemaEvolutionDictForPartitionedTableToo() {
        // The dict is emitted alongside path_partition_keys (it is unconditional, like legacy
        // createScanRangeLocations). MUTATION: gating the dict on unpartitioned -> absent here -> red.
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "p", Types.IntegerType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("p").build();
        Table table = createTable("pt", schema, spec);
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        Map<String, String> props = provider.getScanNodeProperties(
                null, new IcebergTableHandle("db1", "pt"), Collections.emptyList(), Optional.empty());

        Assertions.assertTrue(props.containsKey("iceberg.schema_evolution"));
        Assertions.assertEquals("p", props.get("path_partition_keys"));
    }

    // --- T06 [D-065]: getScanNodeProperties sys-handle guard (skip dict + path_partition_keys) ---

    @Test
    public void getScanNodePropertiesForUnpinnedSysHandleSkipsDictAndPathKeysWithoutThrowing() {
        // [D-065] A system-table handle ($snapshots/$files/...) must NOT take the base-table props path:
        // its requested columns are METADATA columns (committed_at/snapshot_id/...) absent from the base
        // data schema, so building the field-id dict from the base schema throws "requested column not
        // found" (IcebergSchemaUtils.buildCurrentSchema). The metadata-table schema travels inside the
        // serialized JNI split (planSystemTableScan), so BE needs neither the dict nor base
        // path_partition_keys. MUTATION: removing the isSystemTable() guard -> the no-pin else branch
        // throws here -> red.
        PartitionSpec spec = PartitionSpec.builderFor(PART_SCHEMA).identity("p").build();
        Table table = createTable("pt", PART_SCHEMA, spec);
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));
        // Requested columns are metadata-table columns, NOT present in the base (id, p) schema.
        List<ConnectorColumnHandle> metaColumns = Arrays.asList(
                new IcebergColumnHandle("committed_at", 1),
                new IcebergColumnHandle("snapshot_id", 2));

        Map<String, String> props = provider.getScanNodeProperties(
                null, IcebergTableHandle.forSystemTable("db1", "pt", "snapshots", -1L, null, -1L),
                metaColumns, Optional.empty());

        Assertions.assertFalse(props.containsKey("iceberg.schema_evolution"),
                "a sys handle must not emit the field-id schema-evolution dict (BE sys JNI reader ignores it; "
                        + "building it from the base schema + meta columns throws)");
        Assertions.assertFalse(props.containsKey("path_partition_keys"),
                "a sys (metadata) table is not base-spec partitioned -> no path_partition_keys");
        Assertions.assertEquals("jni", props.get("file_format_type"),
                "sys scan still flows through the JNI path");
    }

    @Test
    public void getScanNodePropertiesForPinnedSysHandleSkipsDict() {
        // A sys handle RETAINS its time-travel pin (forSystemTable), so without the guard the dict branch
        // takes the hasSnapshotPin() path and silently builds a BASE-schema dict (no throw, but wrong/
        // meaningless for a metadata scan). The guard must suppress it for the pinned case too.
        // MUTATION: guarding only the unpinned branch -> the pinned base-schema dict leaks here -> red.
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 1024, null, null)).commit();
        long s1 = table.currentSnapshot().snapshotId();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        Map<String, String> props = provider.getScanNodeProperties(
                null, IcebergTableHandle.forSystemTable("db1", "t1", "files", s1, null, -1L),
                Collections.emptyList(), Optional.empty());

        Assertions.assertFalse(props.containsKey("iceberg.schema_evolution"),
                "a pinned sys handle must also skip the schema-evolution dict (a base-schema dict is wrong "
                        + "for a metadata scan)");
        Assertions.assertEquals("jni", props.get("file_format_type"));
    }

    @Test
    public void getScanNodePropertiesForSysHandleStillEmitsLocationCreds() {
        // T07 gap-fill: both existing sys getScanNodeProperties tests run context==null, so the location.*
        // credential blocks (which sit OUTSIDE the two if(!systemTable) skips, D-065) never execute -> cred
        // SURVIVAL for a sys handle is never positively asserted. BE still needs creds to read the metadata
        // files (legacy IcebergScanNode.getLocationProperties has no isSystemTable branch). MUTATION: folding
        // the location.* blocks inside if(!systemTable) (a plausible "tidy-up") strips creds from sys
        // metadata scans -> BE 403 -> every existing test stays green, this one -> red.
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        RecordingConnectorContext context = new RecordingConnectorContext();
        Map<String, String> beStatic = new HashMap<>();
        beStatic.put("AWS_ACCESS_KEY", "ak");
        beStatic.put("AWS_SECRET_KEY", "sk");
        context.storageProperties = Collections.singletonList(fakeBackendStorage(beStatic));
        IcebergScanPlanProvider provider =
                new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table), context);
        // Requested columns are metadata columns absent from the base schema (the dict-throw trap).
        List<ConnectorColumnHandle> metaColumns = Arrays.asList(
                new IcebergColumnHandle("committed_at", 1),
                new IcebergColumnHandle("snapshot_id", 2));

        Map<String, String> props = provider.getScanNodeProperties(
                null, IcebergTableHandle.forSystemTable("db1", "t1", "snapshots", -1L, null, -1L),
                metaColumns, Optional.empty());

        Assertions.assertEquals("ak", props.get("location.AWS_ACCESS_KEY"),
                "a sys handle must still emit location.* creds (BE reads the metadata files; legacy parity)");
        Assertions.assertEquals("sk", props.get("location.AWS_SECRET_KEY"));
        Assertions.assertFalse(props.containsKey("iceberg.schema_evolution"), "sys still skips the dict");
        Assertions.assertFalse(props.containsKey("path_partition_keys"), "sys still skips path_partition_keys");
        Assertions.assertEquals("jni", props.get("file_format_type"));
    }

    // ---------------------------------------------------------------------
    // $position_deletes (upstream #65135 port): the ONE sys table BE reads with a native reader
    // ---------------------------------------------------------------------

    private static Table tableWithPositionDelete(DeleteFile deleteFile) {
        return tableWithPositionDelete(deleteFile, Collections.emptyMap());
    }

    /** Deletion vectors are a v3 feature — a DV commit on a v2 table is rejected by the SDK. */
    private static Table tableWithPositionDeleteV3(DeleteFile deleteFile) {
        return tableWithPositionDelete(deleteFile, Collections.singletonMap("format-version", "3"));
    }

    private static Table tableWithPositionDelete(DeleteFile deleteFile, Map<String, String> props) {
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned(), props);
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 512, null, null))
                .commit();
        table.newRowDelta().addDeletes(deleteFile).commit();
        return table;
    }

    private static TFileRangeDesc positionDeleteRangeDesc(List<ConnectorScanRange> ranges) {
        Assertions.assertEquals(1, ranges.size(), "one delete file -> exactly one range");
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        rangeDesc.setPath(ranges.get(0).getPath().orElse(null));
        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        ((IcebergScanRange) ranges.get(0)).populateRangeParams(formatDesc, rangeDesc);
        rangeDesc.setTableFormatParams(formatDesc);
        return rangeDesc;
    }

    private static List<ConnectorScanRange> planPositionDeletes(Table table, List<ConnectorColumnHandle> cols) {
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));
        return provider.planScan(new FakeScanSession("UTC", Collections.emptyMap()),
                IcebergTableHandle.forSystemTable("db1", "t1", "position_deletes", -1L, null, -1L),
                cols, Optional.empty());
    }

    @Test
    public void planScanPositionDeletesEmitsNativeRangeMatchingTheBeRoutingContract() {
        // WHY: this is THE contract. BE routes a range into iceberg_position_delete_sys_table_reader iff
        // table_format_type=="iceberg" AND the TOP-LEVEL iceberg_params.content is 1 or 3 AND the range
        // format_type is PARQUET/ORC (file_scanner.cpp:103-113). Miss any one and the range silently falls
        // back to the ordinary iceberg reader, which parses the delete file AS A DATA FILE — wrong rows, no
        // error. It also asserts delete_files.size()==1, which BE DCHECKs (reader :179).
        // MUTATION: not setting the top-level content (only the delete-file one) -> BE misroutes -> red;
        // leaving the range on the FORMAT_JNI default -> red; emitting serialized_split (the other sys
        // tables' shape) -> red; emitting 2+ delete descriptors -> red.
        Table table = tableWithPositionDelete(
                positionDeleteFile("s3://b/db/t1/pos.parquet", FileFormat.PARQUET, null, null));

        TFileRangeDesc rangeDesc = positionDeleteRangeDesc(planPositionDeletes(table, Collections.emptyList()));
        TIcebergFileDesc fileDesc = rangeDesc.getTableFormatParams().getIcebergParams();

        Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET, rangeDesc.getFormatType(),
                "a native position-delete range must NOT stay on the FORMAT_JNI default");
        Assertions.assertTrue(fileDesc.isSetContent(), "top-level content is BE's sole routing key");
        Assertions.assertEquals(1, fileDesc.getContent(), "1 = POSITION_DELETES");
        Assertions.assertFalse(fileDesc.isSetSerializedSplit(),
                "the native path must not emit the JNI serialized_split");
        Assertions.assertEquals(-1L, rangeDesc.getTableFormatParams().getTableLevelRowCount());
        Assertions.assertEquals(1, fileDesc.getDeleteFilesSize(),
                "BE asserts exactly one delete descriptor");
        TIcebergDeleteFileDesc deleteDesc = fileDesc.getDeleteFiles().get(0);
        Assertions.assertEquals(1, deleteDesc.getContent(), "the delete descriptor's content must agree");
        Assertions.assertEquals("s3://b/db/t1/pos.parquet", deleteDesc.getOriginalPath(),
                "original_path is the RAW delete-file path (the delete_file_path output column)");
        Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET, deleteDesc.getFileFormat());
        // A path-parsed "partition" key would collide with the metadata table's own `partition` slot.
        Assertions.assertFalse(rangeDesc.isSetColumnsFromPath(), "columns-from-path must be unset");
        Assertions.assertFalse(rangeDesc.isSetColumnsFromPathKeys());
    }

    @Test
    public void planScanPositionDeletesDeletionVectorEmitsContent3AndBlobLocation() {
        // WHY: a V3 deletion vector is a puffin blob, and BE distinguishes it from a plain position-delete
        // file by content==3 ALONE — never by file format, which is why a DV still travels as FORMAT_PARQUET
        // (legacy getNativePositionDeleteFileFormat). BE then expands one output row per set bit, reading the
        // blob at content_offset/content_size_in_bytes and reporting file_path = referenced_data_file_path;
        // without those three fields it cannot materialize a single row.
        // MUTATION: mapping PUFFIN to a "FORMAT_PUFFIN"/JNI instead of FORMAT_PARQUET -> red; emitting
        // content=1 for a DV -> BE runs the parquet position-delete branch on a puffin file -> red;
        // dropping referenced_data_file_path/offset/size -> red.
        Table table = tableWithPositionDeleteV3(deletionVectorFile("s3://b/db/t1/dv.puffin", 4L, 40L));

        TFileRangeDesc rangeDesc = positionDeleteRangeDesc(planPositionDeletes(table, Collections.emptyList()));
        TIcebergFileDesc fileDesc = rangeDesc.getTableFormatParams().getIcebergParams();

        Assertions.assertEquals(3, fileDesc.getContent(), "3 = DELETION_VECTOR (top-level routing)");
        Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET, rangeDesc.getFormatType(),
                "a puffin DV still travels as FORMAT_PARQUET; BE keys on content, not format");
        TIcebergDeleteFileDesc deleteDesc = fileDesc.getDeleteFiles().get(0);
        Assertions.assertEquals(3, deleteDesc.getContent());
        Assertions.assertEquals("s3://b/db/t1/f1.parquet", deleteDesc.getReferencedDataFilePath(),
                "BE reports file_path from the referenced data file for DV rows");
        Assertions.assertEquals(4L, deleteDesc.getContentOffset());
        Assertions.assertEquals(40L, deleteDesc.getContentSizeInBytes());
    }

    @Test
    public void planScanPositionDeletesRejectsAvroDeleteFile() {
        // WHY: there is no native AVRO position-delete reader. Failing loud beats mis-routing the range to
        // the parquet reader, which would fault or return garbage. Message text is pinned by upstream's own
        // unit test (assertEquals, not contains). MUTATION: defaulting an unknown format to FORMAT_PARQUET
        // -> no throw -> red.
        Table table = tableWithPositionDelete(
                positionDeleteFile("s3://b/db/t1/pos.avro", FileFormat.AVRO, null, null));

        UnsupportedOperationException e = Assertions.assertThrows(UnsupportedOperationException.class,
                () -> planPositionDeletes(table, Collections.emptyList()));
        Assertions.assertEquals("Unsupported Iceberg position delete file format: AVRO", e.getMessage());
    }

    @Test
    public void getScanNodePropertiesForPositionDeletesSysHandleEmitsDict() {
        // WHY (D-065 narrowed): every OTHER sys table rides the JNI serialized-split path, where the
        // metadata-table schema travels inside the serialized FileScanTask — so the field-id dict is
        // correctly skipped. $position_deletes has no serialized task: BOTH native readers resolve the `row`
        // column through params.history_schema_info. Without the dict, scanner v1 hard-errors ("Iceberg
        // position delete system table row schema is missing") and scanner v2 SILENTLY degrades to name
        // matching, mis-reading renamed columns under schema evolution. The dict must also be built from the
        // METADATA table's own schema — feeding the base schema keyed off meta columns is exactly what makes
        // IcebergSchemaUtils throw "requested column not found".
        // MUTATION: widening the guard back to `if (!systemTable)` -> no dict -> red.
        Table table = tableWithPositionDelete(
                positionDeleteFile("s3://b/db/t1/pos.parquet", FileFormat.PARQUET, null, null));
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));
        // Metadata-table columns, absent from the base (id, name) schema — the dict-throw trap.
        List<ConnectorColumnHandle> metaColumns = Arrays.asList(
                new IcebergColumnHandle("file_path", 1),
                new IcebergColumnHandle("pos", 2));

        Map<String, String> props = provider.getScanNodeProperties(
                null, IcebergTableHandle.forSystemTable("db1", "t1", "position_deletes", -1L, null, -1L),
                metaColumns, Optional.empty());

        Assertions.assertTrue(props.containsKey("iceberg.schema_evolution"),
                "position_deletes reads natively and needs the field-id dict to resolve `row`");
        Assertions.assertFalse(props.containsKey("path_partition_keys"),
                "a metadata table is still not base-spec partitioned -> no path_partition_keys");
    }

    @Test
    public void getScanNodePropertiesForPositionDeletesLoadsTheBaseTableOnlyOnce() {
        // WHY: the dict branch needs the METADATA table, and the obvious way to get one is resolveSysTable().
        // That helper carries NO auth wrap on purpose — its javadoc pins that its sole caller
        // (planSystemTableScan) owns the executeAuthenticated scope. getScanNodeProperties has no such scope,
        // so calling it here would issue an UNAUTHENTICATED loadTable and fail a kerberized catalog at plan
        // time (and pay a second round-trip). Building the metadata table from the base table this method
        // already resolved — a pure local construction — avoids both. Counting loads is the observable proxy:
        // a second load here means resolveSysTable (or any other fresh resolve) crept back in.
        // MUTATION: reverting to resolveSysTable(session, iceHandle) -> loadCount 2 -> red.
        Table table = tableWithPositionDelete(
                positionDeleteFile("s3://b/db/t1/pos.parquet", FileFormat.PARQUET, null, null));
        RecordingIcebergCatalogOps ops = opsReturning(table);
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), ops);

        Map<String, String> props = provider.getScanNodeProperties(
                null, IcebergTableHandle.forSystemTable("db1", "t1", "position_deletes", -1L, null, -1L),
                Collections.singletonList(new IcebergColumnHandle("row", 1)), Optional.empty());

        Assertions.assertTrue(props.containsKey("iceberg.schema_evolution"), "the dict must still be emitted");
        Assertions.assertEquals(1, Collections.frequency(ops.log, "loadTable:db1.t1"),
                "the base table must be loaded exactly once (no unauthenticated second resolve)");
    }

    // --- T07: MVCC / time-travel scan-time pin + Option-A field-id dict ---

    @Test
    public void planScanPinnedToOlderSnapshotReadsOnlyThatSnapshotsFiles() {
        // S1 appends f1; S2 appends f2 (appends accumulate, so S2 sees both). A pin to S1 must read ONLY f1
        // (legacy createTableScan -> scan.useSnapshot(id)). MUTATION: ignoring the pin (reading latest) -> 2
        // ranges -> red.
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 1024, null, null)).commit();
        long s1 = table.currentSnapshot().snapshotId();
        long schemaIdS1 = table.currentSnapshot().schemaId();
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db/t1/f2.parquet", 1024, null, null)).commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        List<ConnectorScanRange> latest = provider.planScan(
                null, new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty());
        Assertions.assertEquals(2, latest.size());

        List<ConnectorScanRange> pinned = provider.planScan(
                null, new IcebergTableHandle("db1", "t1").withSnapshot(s1, null, schemaIdS1),
                Collections.emptyList(), Optional.empty());
        Assertions.assertEquals(1, pinned.size());
        Assertions.assertTrue(pinned.get(0).getPath().get().endsWith("f1.parquet"));
    }

    @Test
    public void planScanPinnedToTagReadsViaUseRefNotSnapshotId() {
        // The handle carries BOTH a ref (tag1 -> S1) AND the LATEST snapshot id (s2). The scan must pin by REF
        // (useRef), so it reads only f1. MUTATION: pinning by snapshotId (useSnapshot(s2)) -> reads both -> red.
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 1024, null, null)).commit();
        long s1 = table.currentSnapshot().snapshotId();
        long schemaIdS1 = table.currentSnapshot().schemaId();
        table.manageSnapshots().createTag("tag1", s1).commit();
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db/t1/f2.parquet", 1024, null, null)).commit();
        long s2 = table.currentSnapshot().snapshotId();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        List<ConnectorScanRange> pinned = provider.planScan(
                null, new IcebergTableHandle("db1", "t1").withSnapshot(s2, "tag1", schemaIdS1),
                Collections.emptyList(), Optional.empty());
        Assertions.assertEquals(1, pinned.size());
        Assertions.assertTrue(pinned.get(0).getPath().get().endsWith("f1.parquet"));
    }

    @Test
    public void countPushdownFollowsTheSnapshotPin() {
        // f1=1000/100=10 records (S1); + f2=2000/100=20 -> latest total-records 30. Pinned to S1 the count is
        // read from S1's summary (10), via scan.snapshot(). MUTATION: counting the latest snapshot -> 30 -> red.
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 1000, null, null)).commit();
        long s1 = table.currentSnapshot().snapshotId();
        long schemaIdS1 = table.currentSnapshot().schemaId();
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db/t1/f2.parquet", 2000, null, null)).commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        List<ConnectorScanRange> pinned = provider.planScan(
                null, new IcebergTableHandle("db1", "t1").withSnapshot(s1, null, schemaIdS1),
                Collections.emptyList(), Optional.empty(), -1L, Collections.emptyList(), true);
        Assertions.assertEquals(1, pinned.size());
        Assertions.assertEquals(10L, pinned.get(0).getPushDownRowCount());
    }

    @Test
    public void getScanNodePropertiesUnderPinEmitsFullPinnedSchemaDict() throws Exception {
        // T07 Option A: under a time-travel pin the field-id dict is built from the FULL pinned schema (covering
        // every BE slot), NOT the pruned `columns`. The pinned schema (S1) has id+name; after a rename the latest
        // has id+fullname. Even with a PRUNED columns=[id], the dict must carry the renamed slot "name" (the
        // pinned name) so BE's StructNode never misses it. MUTATION: keying off `columns` under a pin -> "name"
        // dropped -> dict has only "id" -> red.
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 1024, null, null)).commit();
        long s1 = table.currentSnapshot().snapshotId();
        long schemaIdS1 = table.currentSnapshot().schemaId();
        table.updateSchema().renameColumn("name", "fullname").commit();
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db/t1/f2.parquet", 1024, null, null)).commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        Map<String, String> props = provider.getScanNodeProperties(
                null, new IcebergTableHandle("db1", "t1").withSnapshot(s1, null, schemaIdS1),
                Collections.singletonList(new IcebergColumnHandle("id", 1)), Optional.empty());

        TFileScanRangeParams params = new TFileScanRangeParams();
        provider.populateScanLevelParams(params, props);
        Assertions.assertEquals(-1L, params.getCurrentSchemaId());
        Assertions.assertEquals(2, params.getHistorySchemaInfo().get(0).getRootField().getFieldsSize());
        Assertions.assertEquals("id", params.getHistorySchemaInfo().get(0).getRootField()
                .getFields().get(0).getFieldPtr().getName());
        Assertions.assertEquals("name", params.getHistorySchemaInfo().get(0).getRootField()
                .getFields().get(1).getFieldPtr().getName());
    }

    @Test
    public void getScanNodePropertiesUnderTopnLazyMatEmitsFullLatestSchemaDict() throws Exception {
        // M-4: under Top-N lazy materialization BE reads the sort key first, then re-fetches the OTHER
        // (non-projected) columns of the surviving rows by the synthesized row-id. So the field-id dict must
        // span the FULL latest schema, NOT the pruned `columns` — else a lazily re-fetched, schema-evolved
        // column has no field-id entry and the native read drops/mis-reads it (legacy
        // initSchemaInfoForAllColumn parity). SCHEMA = id+name; even with a PRUNED columns=[id], the topn dict
        // must carry "name". MUTATION: dropping the isTopnLazyMaterialize() branch -> keyed off `columns` ->
        // dict has only "id" -> red.
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        Map<String, String> props = provider.getScanNodeProperties(
                null, new IcebergTableHandle("db1", "t1").withTopnLazyMaterialize(true),
                Collections.singletonList(new IcebergColumnHandle("id", 1)), Optional.empty());

        TFileScanRangeParams params = new TFileScanRangeParams();
        provider.populateScanLevelParams(params, props);
        Assertions.assertEquals(2, params.getHistorySchemaInfo().get(0).getRootField().getFieldsSize());
        Assertions.assertEquals("id", params.getHistorySchemaInfo().get(0).getRootField()
                .getFields().get(0).getFieldPtr().getName());
        Assertions.assertEquals("name", params.getHistorySchemaInfo().get(0).getRootField()
                .getFields().get(1).getFieldPtr().getName());
    }

    @Test
    public void getScanNodePropertiesPinTakesPrecedenceOverTopnLazyMat() throws Exception {
        // A time-travel pin + Top-N lazy mat must take the PIN branch (pinned schema, full columns), not the
        // latest-schema topn branch: BE reads at the pinned snapshot, so lazily re-fetched columns resolve
        // against the PINNED schema's field-ids. Pinned S1 = id+name; latest (after rename) = id+fullname.
        // With pin+topn the dict must carry the PINNED "name", never the latest "fullname". MUTATION:
        // ordering the topn branch before the pin branch -> "fullname" leaks -> red.
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 1024, null, null)).commit();
        long s1 = table.currentSnapshot().snapshotId();
        long schemaIdS1 = table.currentSnapshot().schemaId();
        table.updateSchema().renameColumn("name", "fullname").commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        Map<String, String> props = provider.getScanNodeProperties(
                null, new IcebergTableHandle("db1", "t1").withSnapshot(s1, null, schemaIdS1)
                        .withTopnLazyMaterialize(true),
                Collections.singletonList(new IcebergColumnHandle("id", 1)), Optional.empty());

        TFileScanRangeParams params = new TFileScanRangeParams();
        provider.populateScanLevelParams(params, props);
        Assertions.assertEquals(2, params.getHistorySchemaInfo().get(0).getRootField().getFieldsSize());
        Assertions.assertEquals("name", params.getHistorySchemaInfo().get(0).getRootField()
                .getFields().get(1).getFieldPtr().getName());
    }

    @Test
    public void planScanReadsRealFormatVersionAndEmitsV3RowLineage() {
        Map<String, String> v3 = new HashMap<>();
        v3.put("format-version", "3");
        Table table = createTable("v3t", SCHEMA, PartitionSpec.unpartitioned(), v3);
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db/v3t/f.parquet", 512, null, null))
                .commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        List<ConnectorScanRange> ranges = provider.planScan(
                null, new IcebergTableHandle("db1", "v3t"), Collections.emptyList(), Optional.empty());
        Assertions.assertEquals(1, ranges.size());
        TIcebergFileDesc fd = populate(ranges.get(0)).getTableFormatParams().getIcebergParams();

        // format_version read from real table metadata (NOT hard-coded). v3 always emits row lineage (>= -1 for
        // files carried over from a v2->v3 upgrade). MUTATION: hard-coding v2 / never emitting lineage -> red.
        Assertions.assertEquals(3, fd.getFormatVersion());
        Assertions.assertTrue(fd.isSetFirstRowId());
        Assertions.assertTrue(fd.isSetLastUpdatedSequenceNumber());
    }

    // ── commit-bridge supply (S4 part 2): a v3 scan stashes each data file's non-equality deletes by raw path ──

    @Test
    public void planScanStashesRewritableDeletesKeyedByRawDataFilePathForV3() {
        // A v3 scan over a data file that already has a deletion vector must stash that DV keyed on the data
        // file's RAW path, so a same-statement DELETE/MERGE write can hand it to the BE. MUTATION: not stashing
        // (or keying on the normalized path) -> the write supplies nothing -> the BE resurrects the deleted rows.
        Map<String, String> v3 = new HashMap<>();
        v3.put("format-version", "3");
        Table table = createTable("v3dv", SCHEMA, PartitionSpec.unpartitioned(), v3);
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 512, null, null))
                .commit();
        table.newRowDelta()
                .addDeletes(deletionVectorFile("s3://b/db/t1/dv.puffin", 16L, 64L))
                .commit();

        IcebergRewritableDeleteStash stash = new IcebergRewritableDeleteStash();
        IcebergScanPlanProvider provider =
                new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table), null, null, stash);
        provider.planScan(new FakeScanSession("UTC", Collections.emptyMap()),
                new IcebergTableHandle("db1", "v3dv"), Collections.emptyList(), Optional.empty());

        Map<String, List<TIcebergDeleteFileDesc>> sets = stash.retrieveAndRemove("q");
        Assertions.assertNotNull(sets, "a v3 scan with a live DV must stash a supply for queryId 'q'");
        // Keyed on the RAW data-file path (== originalPath), the string the BE matches a rewritable set against.
        Assertions.assertTrue(sets.containsKey("s3://b/db/t1/f1.parquet"),
                "stash must key on the raw data-file path, got keys: " + sets.keySet());
        List<TIcebergDeleteFileDesc> descs = sets.get("s3://b/db/t1/f1.parquet");
        Assertions.assertEquals(1, descs.size());
        Assertions.assertEquals(3, descs.get(0).getContent(), "the DV is content 3");
    }

    @Test
    public void planScanDoesNotStashForVersionTwo() {
        // v2 deletes are plain position-delete files (no DV union); the rewritable supply is a v3-only concept.
        // A real position delete is committed so the assertion proves the formatVersion>=3 GATE, not an absence
        // of deletes. MUTATION: dropping the v3 gate -> this v2 position delete would be stashed -> red.
        Table table = createTable("v2pd", SCHEMA, PartitionSpec.unpartitioned(),
                Collections.singletonMap("format-version", "2"));
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 512, null, null))
                .commit();
        table.newRowDelta()
                .addDeletes(positionDeleteFile("s3://b/db/t1/pos.parquet", FileFormat.PARQUET, null, null))
                .commit();

        IcebergRewritableDeleteStash stash = new IcebergRewritableDeleteStash();
        IcebergScanPlanProvider provider =
                new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table), null, null, stash);
        provider.planScan(new FakeScanSession("UTC", Collections.emptyMap()),
                new IcebergTableHandle("db1", "v2pd"), Collections.emptyList(), Optional.empty());

        Assertions.assertEquals(0, stash.size(), "a v2 scan must not stash any rewritable supply");
    }

    @Test
    public void planScanWithoutStashIsInert() {
        // The offline 2-arg ctor leaves the stash null; a v3 scan must not NPE — it simply skips stashing.
        Map<String, String> v3 = new HashMap<>();
        v3.put("format-version", "3");
        Table table = createTable("v3ns", SCHEMA, PartitionSpec.unpartitioned(), v3);
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 512, null, null))
                .commit();
        table.newRowDelta()
                .addDeletes(deletionVectorFile("s3://b/db/t1/dv.puffin", 16L, 64L))
                .commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        List<ConnectorScanRange> ranges = provider.planScan(new FakeScanSession("UTC", Collections.emptyMap()),
                new IcebergTableHandle("db1", "v3ns"), Collections.emptyList(), Optional.empty());
        Assertions.assertEquals(1, ranges.size());
    }

    @Test
    public void planScanRejectsUnsupportedFileFormatFailLoud() {
        // Legacy IcebergScanNode.getFileFormatType() throws DdlException("Unsupported format name: <fmt> ...") at plan
        // start for a non-orc/parquet table; the connector must keep that fail-loud guard instead of silently
        // shipping the file to BE's iceberg JNI reader (which expects a serialized system-table split).
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f.avro", 512, null, null, FileFormat.AVRO))
                .commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        // MUTATION: leaving the else-branch silent (FORMAT_JNI default) -> no throw -> red.
        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class, () -> provider.planScan(
                null, new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty()));
        Assertions.assertTrue(ex.getMessage().contains("Unsupported format name: avro"),
                "message should mirror legacy: " + ex.getMessage());
    }

    // --- FIX-M3 streaming (file-count) split generation ---------------------------------------------------

    /** A session that sets the file-count batch gate vars (num_files_in_batch_mode / enable). */
    private static ConnectorSession batchSession(long numFilesInBatchMode, boolean enableBatchMode) {
        Map<String, String> props = new HashMap<>();
        props.put("num_files_in_batch_mode", String.valueOf(numFilesInBatchMode));
        props.put("enable_external_table_batch_mode", String.valueOf(enableBatchMode));
        return new FakeScanSession("UTC", props);
    }

    private static Table threeFileTable(Map<String, String> tableProps) {
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned(), tableProps);
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 1024, null, null))
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f2.parquet", 2048, null, null))
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f3.parquet", 4096, null, null))
                .commit();
        return table;
    }

    private static Table threeFileTable() {
        return threeFileTable(Collections.emptyMap());
    }

    private static IcebergScanPlanProvider providerOver(Table table) {
        return new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));
    }

    private static ConnectorSession emptySession() {
        return new FakeScanSession("UTC", Collections.emptyMap());
    }

    private static List<ConnectorScanRange> drain(ConnectorSplitSource source) throws IOException {
        List<ConnectorScanRange> out = new ArrayList<>();
        try (ConnectorSplitSource s = source) {
            while (s.hasNext()) {
                out.add(s.next());
            }
        }
        return out;
    }

    @Test
    public void streamingSplitEstimateBelowThresholdStaysSynchronous() {
        // 3 matched files < threshold(5) -> stay on the synchronous planScan path (small scans need no streaming).
        IcebergScanPlanProvider provider = providerOver(threeFileTable());
        long estimate = provider.streamingSplitEstimate(batchSession(5, true),
                new IcebergTableHandle("db1", "t1"), Optional.empty(), false);
        Assertions.assertEquals(-1, estimate, "below threshold must not stream");
    }

    @Test
    public void streamingSplitEstimateAtThresholdStreamsAndReturnsFileCount() {
        // 3 matched files == threshold(3): the gate is INCLUSIVE (legacy >=), and the estimate is the file count
        // (the BE concurrency hint). MUTATION: `>=` -> `>` drops the boundary -> -1 -> red.
        IcebergScanPlanProvider provider = providerOver(threeFileTable());
        long estimate = provider.streamingSplitEstimate(batchSession(3, true),
                new IcebergTableHandle("db1", "t1"), Optional.empty(), false);
        Assertions.assertEquals(3, estimate, "at threshold must stream and report the matched file count");
    }

    @Test
    public void streamingSplitEstimateDisabledBySessionVarStaysSynchronous() {
        // enable_external_table_batch_mode=false short-circuits even though 3 >= threshold(2). MUTATION: dropping
        // the enable guard -> 3 -> red. This is the session var that was silently dead pre-fix.
        IcebergScanPlanProvider provider = providerOver(threeFileTable());
        long estimate = provider.streamingSplitEstimate(batchSession(2, false),
                new IcebergTableHandle("db1", "t1"), Optional.empty(), false);
        Assertions.assertEquals(-1, estimate, "batch mode disabled must not stream");
    }

    @Test
    public void streamingSplitEstimateEmptyTableStaysSynchronous() {
        // No snapshot (no append) -> nothing to stream. MUTATION: dropping the snapshot==null guard -> NPE/red.
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));
        long estimate = provider.streamingSplitEstimate(batchSession(0, true),
                new IcebergTableHandle("db1", "t1"), Optional.empty(), false);
        Assertions.assertEquals(-1, estimate, "empty table must not stream");
    }

    @Test
    public void streamingSplitEstimateSystemTableStaysSynchronous() {
        // System tables take the JNI serialized-split path, never streaming. MUTATION: dropping the isSystemTable
        // guard -> attempts to count a metadata table -> wrong/red.
        IcebergScanPlanProvider provider = providerOver(threeFileTable());
        long estimate = provider.streamingSplitEstimate(batchSession(2, true),
                IcebergTableHandle.forSystemTable("db1", "t1", "snapshots", -1, null, -1), Optional.empty(), false);
        Assertions.assertEquals(-1, estimate, "system table must not stream");
    }

    @Test
    public void streamingSplitEstimateV3StaysSynchronous() {
        // Format-version >= 3 carries the commit-bridge rewritable-delete stash that the write side reads at
        // write-plan time; streaming would fill it too late (BE-pull time) and resurrect deleted rows. So v3 is
        // gated onto the eager path. MUTATION: `>= 3` -> `> 3` (or dropping the guard) -> 3 -> red (correctness).
        Table table = threeFileTable(Collections.singletonMap("format-version", "3"));
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));
        long estimate = provider.streamingSplitEstimate(batchSession(2, true),
                new IcebergTableHandle("db1", "t1"), Optional.empty(), false);
        Assertions.assertEquals(-1, estimate, "v3 (deletion-vector) tables must not stream");
    }

    @Test
    public void streamingSplitEstimateServableCountPushdownStaysSynchronous() {
        // A servable COUNT(*) collapses to one range (never streamed). 3 files, no deletes -> count servable from
        // the snapshot summary. MUTATION: dropping the countPushdown short-circuit -> 3 -> red.
        IcebergScanPlanProvider provider = providerOver(threeFileTable());
        long estimate = provider.streamingSplitEstimate(batchSession(2, true),
                new IcebergTableHandle("db1", "t1"), Optional.empty(), true);
        Assertions.assertEquals(-1, estimate, "servable count pushdown must not stream");
    }

    @Test
    public void streamSplitsProducesOneLazyRangePerFile() throws IOException {
        // The lazy source yields exactly one range per data file (3), with the raw paths preserved. This is the
        // streamed counterpart of planScan's eager enumeration.
        IcebergScanPlanProvider provider = providerOver(threeFileTable());
        List<ConnectorScanRange> ranges = drain(provider.streamSplits(emptySession(),
                new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty(), -1L));
        Set<String> paths = ranges.stream()
                .map(r -> ((IcebergScanRange) r).getOriginalPath()).collect(ImmutableSet.toImmutableSet());
        Assertions.assertEquals(
                ImmutableSet.of("s3://b/db/t1/f1.parquet", "s3://b/db/t1/f2.parquet", "s3://b/db/t1/f3.parquet"), paths,
                "streaming must yield one range per file");
    }

    @Test
    public void streamSplitsRewriteScopeSkipsUnscopedFilesViaLookahead() throws IOException {
        // A rewrite scope keeps only f1 + f3; the source's look-ahead must skip f2 in hasNext(). MUTATION:
        // dropping the rewrite-scope skip -> f2 leaks (3 ranges) -> red; a broken look-ahead (no skip) would
        // surface a null -> red.
        Table table = threeFileTable(Collections.emptyMap());
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));
        IcebergTableHandle scoped = new IcebergTableHandle("db1", "t1").withRewriteFileScope(
                ImmutableSet.of("s3://b/db/t1/f1.parquet", "s3://b/db/t1/f3.parquet"));
        List<ConnectorScanRange> ranges = drain(provider.streamSplits(emptySession(),
                scoped, Collections.emptyList(), Optional.empty(), -1L));
        Set<String> paths = ranges.stream()
                .map(r -> ((IcebergScanRange) r).getOriginalPath()).collect(ImmutableSet.toImmutableSet());
        Assertions.assertEquals(
                ImmutableSet.of("s3://b/db/t1/f1.parquet", "s3://b/db/t1/f3.parquet"), paths,
                "rewrite scope must skip f2 in the streamed source");
    }

    @Test
    public void streamSplitsNextThrowsWhenExhausted() throws IOException {
        // next() past the end must throw (the engine pulls only while hasNext()). MUTATION: dropping the hasNext
        // guard in next() -> NPE/wrong instead of NoSuchElementException -> red.
        IcebergScanPlanProvider provider = providerOver(threeFileTable());
        ConnectorSplitSource source = provider.streamSplits(new FakeScanSession("UTC", Collections.emptyMap()),
                new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty(), -1L);
        while (source.hasNext()) {
            source.next();
        }
        Assertions.assertThrows(NoSuchElementException.class, source::next);
        source.close();
    }

    @Test
    public void streamSplitsCloseBeforeIterationDoesNotThrow() throws IOException {
        // The engine may close the source without ever pulling (e.g. needMoreSplit() false from the start). With
        // the lazy iterator the iterator is still null; close() must null-guard it and still release the
        // underlying planFiles() iterable. MUTATION: dropping the `iterator != null` guard in close() -> NPE -> red.
        IcebergScanPlanProvider provider = providerOver(threeFileTable());
        ConnectorSplitSource source = provider.streamSplits(emptySession(),
                new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty(), -1L);
        Assertions.assertDoesNotThrow(source::close);
    }

    /** A minimal {@link ConnectorSession} exposing a time zone + session split-size properties (no Mockito). */
    private static final class FakeScanSession implements ConnectorSession {
        private final String timeZone;
        private final Map<String, String> sessionProperties;

        FakeScanSession(String timeZone, Map<String, String> sessionProperties) {
            this.timeZone = timeZone;
            this.sessionProperties = sessionProperties;
        }

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
            return timeZone;
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
            return sessionProperties;
        }
    }

    // --- T04: merge-on-read delete files (convertDelete classification + path normalize + EXPLAIN read-back) ---

    private static DeleteFile positionDeleteFile(String path, FileFormat format, Long lower, Long upper) {
        FileMetadata.Builder builder = FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofPositionDeletes()
                .withPath(path)
                .withFormat(format)
                .withFileSizeInBytes(128L)
                .withRecordCount(4L);
        if (lower != null || upper != null) {
            int posField = MetadataColumns.DELETE_FILE_POS.fieldId();
            Map<Integer, ByteBuffer> lowerMap = lower == null ? null : Collections.singletonMap(posField,
                    Conversions.toByteBuffer(MetadataColumns.DELETE_FILE_POS.type(), lower));
            Map<Integer, ByteBuffer> upperMap = upper == null ? null : Collections.singletonMap(posField,
                    Conversions.toByteBuffer(MetadataColumns.DELETE_FILE_POS.type(), upper));
            builder.withMetrics(new Metrics(4L, null, null, null, null, lowerMap, upperMap));
        }
        return builder.build();
    }

    private static DeleteFile deletionVectorFile(String path, long offset, long size) {
        return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofPositionDeletes()
                .withPath(path)
                .withFormat(FileFormat.PUFFIN)
                .withFileSizeInBytes(256L)
                .withRecordCount(4L)
                .withReferencedDataFile("s3://b/db/t1/f1.parquet")
                .withContentOffset(offset)
                .withContentSizeInBytes(size)
                .build();
    }

    private static DeleteFile equalityDeleteFile(String path, FileFormat format, int... fieldIds) {
        return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofEqualityDeletes(fieldIds)
                .withPath(path)
                .withFormat(format)
                .withFileSizeInBytes(128L)
                .withRecordCount(4L)
                .build();
    }

    private static IcebergScanPlanProvider provider() {
        return new IcebergScanPlanProvider(Collections.emptyMap(), new RecordingIcebergCatalogOps());
    }

    private static TIcebergDeleteFileDesc deleteDesc(String path, int content) {
        TIcebergDeleteFileDesc d = new TIcebergDeleteFileDesc();
        d.setPath(path);
        d.setContent(content);
        return d;
    }

    @Test
    public void convertDeletePositionDeleteCarriesBoundsAndFormat() {
        // POSITION_DELETES (non-PUFFIN) -> content 1, parquet/orc format, [lower,upper] bounds decoded from the
        // delete file's DELETE_FILE_POS bounds. MUTATION: wrong content id / dropped bounds / wrong format -> red.
        DeleteFile delete = positionDeleteFile("s3://b/db/t1/pos.parquet", FileFormat.PARQUET, 3L, 17L);
        TIcebergDeleteFileDesc d = provider().convertDelete(delete, Collections.emptyMap()).toThrift();

        Assertions.assertEquals(1, d.getContent());
        Assertions.assertEquals("s3://b/db/t1/pos.parquet", d.getPath());
        Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET, d.getFileFormat());
        Assertions.assertEquals(3L, d.getPositionLowerBound());
        Assertions.assertEquals(17L, d.getPositionUpperBound());
        Assertions.assertFalse(d.isSetFieldIds());
        Assertions.assertFalse(d.isSetContentOffset());
    }

    @Test
    public void convertDeletePositionDeleteWithoutBoundsLeavesThemUnset() {
        // No DELETE_FILE_POS bounds present -> position_lower/upper_bound stay unset (legacy emits them only
        // when present; it stores a -1 sentinel and skips emission). MUTATION: emitting 0/-1 -> red.
        DeleteFile delete = positionDeleteFile("s3://b/db/t1/pos.orc", FileFormat.ORC, null, null);
        TIcebergDeleteFileDesc d = provider().convertDelete(delete, Collections.emptyMap()).toThrift();

        Assertions.assertEquals(TFileFormatType.FORMAT_ORC, d.getFileFormat());
        Assertions.assertFalse(d.isSetPositionLowerBound());
        Assertions.assertFalse(d.isSetPositionUpperBound());
    }

    @Test
    public void convertDeleteDeletionVectorCarriesBlobRefAndUnsetsFormat() {
        // A PUFFIN position delete is a DELETION VECTOR -> content 3, content_offset/size set, file_format UNSET
        // (legacy setDeleteFileFormat skips PUFFIN). MUTATION: classifying it as content 1 / emitting a format
        // for the puffin blob -> red (BE would mis-read the DV blob).
        DeleteFile delete = deletionVectorFile("s3://b/db/t1/dv.puffin", 16L, 64L);
        TIcebergDeleteFileDesc d = provider().convertDelete(delete, Collections.emptyMap()).toThrift();

        Assertions.assertEquals(3, d.getContent());
        Assertions.assertFalse(d.isSetFileFormat());
        Assertions.assertEquals(16L, d.getContentOffset());
        Assertions.assertEquals(64L, d.getContentSizeInBytes());
    }

    @Test
    public void convertDeleteEqualityDeleteCarriesFieldIds() {
        // EQUALITY_DELETES -> content 2 + the equality field-ids from delete metadata (correct independent of
        // the T06 data-schema dictionary). MUTATION: wrong content id / dropped field-ids -> red (BE projects
        // the wrong columns for the equality match).
        DeleteFile delete = equalityDeleteFile("s3://b/db/t1/eq.parquet", FileFormat.PARQUET, 1, 2);
        TIcebergDeleteFileDesc d = provider().convertDelete(delete, Collections.emptyMap()).toThrift();

        Assertions.assertEquals(2, d.getContent());
        Assertions.assertEquals(Arrays.asList(1, 2), d.getFieldIds());
        Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET, d.getFileFormat());
        Assertions.assertFalse(d.isSetPositionLowerBound());
        Assertions.assertFalse(d.isSetContentOffset());
    }

    @Test
    public void convertDeleteNormalizesDeletePathViaContext() {
        // Delete paths live inside iceberg_params (the parent does not normalize them), so the connector must
        // route them through the engine seam (legacy LocationPath.toStorageLocation). BE's S3 factory only
        // opens s3://, so an un-normalized oss:// deletion path silently drops merge-on-read deletes -> wrong
        // rows. MUTATION: handing BE the raw oss:// path -> red.
        RecordingConnectorContext context = new RecordingConnectorContext();
        IcebergScanPlanProvider provider =
                new IcebergScanPlanProvider(Collections.emptyMap(), new RecordingIcebergCatalogOps(), context);
        DeleteFile delete = positionDeleteFile("oss://bucket/db/t1/pos.parquet", FileFormat.PARQUET, null, null);

        TIcebergDeleteFileDesc d = provider.convertDelete(delete, Collections.emptyMap()).toThrift();

        Assertions.assertEquals("s3://bucket/db/t1/pos.parquet", d.getPath());
        Assertions.assertTrue(context.normalizedUris.contains("oss://bucket/db/t1/pos.parquet"));
    }

    @Test
    public void getDeleteFilesReadsBackAllPathsIncludingEquality() {
        // The VERBOSE EXPLAIN read-back returns EVERY delete path (incl equality) — the equality/non-equality
        // split legacy keeps in deleteFilesByReferencedDataFile is only for the write/rewrite path, not this
        // deleteFileNum count. MUTATION: filtering out equality deletes here -> red.
        TIcebergFileDesc fd = new TIcebergFileDesc();
        fd.setDeleteFiles(Arrays.asList(
                deleteDesc("s3://b/pos.parquet", 1),
                deleteDesc("s3://b/eq.parquet", 2),
                deleteDesc("s3://b/dv.puffin", 3)));
        TTableFormatFileDesc tf = new TTableFormatFileDesc();
        tf.setIcebergParams(fd);

        Assertions.assertEquals(Arrays.asList("s3://b/pos.parquet", "s3://b/eq.parquet", "s3://b/dv.puffin"),
                provider().getDeleteFiles(tf));
    }

    @Test
    public void getDeleteFilesEmptyWhenNoIcebergParamsOrNoDeletes() {
        IcebergScanPlanProvider provider = provider();
        // null params / no iceberg params / iceberg params without delete_files all -> empty (legacy guards).
        Assertions.assertTrue(provider.getDeleteFiles(null).isEmpty());
        Assertions.assertTrue(provider.getDeleteFiles(new TTableFormatFileDesc()).isEmpty());
        TTableFormatFileDesc tf = new TTableFormatFileDesc();
        tf.setIcebergParams(new TIcebergFileDesc());
        Assertions.assertTrue(provider.getDeleteFiles(tf).isEmpty());
    }

    @Test
    public void planScanAttachesRealPositionDeleteEndToEnd() {
        // End-to-end on a real v2 table: a position delete committed via RowDelta must reach delete_files,
        // proving task.deletes() flows through planScan -> buildRange -> populateRangeParams. MUTATION:
        // never reading task.deletes() (T03 behavior) -> delete_files empty -> red.
        Map<String, String> v2 = Collections.singletonMap(TableProperties.FORMAT_VERSION, "2");
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned(), v2);
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 1024, null, null))
                .commit();
        DeleteFile posDelete = FileMetadata.deleteFileBuilder(table.spec())
                .ofPositionDeletes()
                .withPath("s3://b/db/t1/pos-delete.parquet")
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(128L)
                .withRecordCount(2L)
                .withReferencedDataFile("s3://b/db/t1/f1.parquet")
                .build();
        table.newRowDelta().addDeletes(posDelete).commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        List<ConnectorScanRange> ranges = provider.planScan(
                null, new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty());
        Assertions.assertEquals(1, ranges.size());

        TFileRangeDesc rangeDesc = populate(ranges.get(0));
        TIcebergFileDesc fd = rangeDesc.getTableFormatParams().getIcebergParams();
        Assertions.assertEquals(1, fd.getDeleteFilesSize());
        TIcebergDeleteFileDesc d = fd.getDeleteFiles().get(0);
        Assertions.assertEquals("s3://b/db/t1/pos-delete.parquet", d.getPath());
        Assertions.assertEquals(1, d.getContent());
        // The EXPLAIN read-back sees the same delete path.
        Assertions.assertEquals(Collections.singletonList("s3://b/db/t1/pos-delete.parquet"),
                provider.getDeleteFiles(rangeDesc.getTableFormatParams()));
    }

    // --- data-path normalization (the gap the T04 parity review surfaced; legacy createIcebergSplit:852) ---

    @Test
    public void planScanNormalizesDataFilePathButKeepsOriginalFilePathRaw() {
        // The range path BE opens MUST be scheme-normalized (oss/cos/obs/s3a -> s3), mirroring legacy
        // createIcebergSplit:852 (2-arg LocationPath.of) + paimon FIX-URI-NORMALIZE — otherwise BE's s3-only
        // factory cannot open an object-store data file at the P6.6 cutover. But original_file_path stays RAW:
        // BE matches position-delete entries against the raw iceberg path (legacy setOriginalFilePath:304).
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend()
                .appendFile(dataFile(table.spec(), "oss://bucket/db/t1/f.parquet", 1024, null, null))
                .commit();
        RecordingConnectorContext context = new RecordingConnectorContext();
        IcebergScanPlanProvider provider =
                new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table), context);

        List<ConnectorScanRange> ranges = provider.planScan(
                null, new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty());
        Assertions.assertEquals(1, ranges.size());

        // MUTATION: emitting the raw oss:// range path (the pre-fix behavior) -> red.
        Assertions.assertEquals("s3://bucket/db/t1/f.parquet", ranges.get(0).getPath().get());
        Assertions.assertTrue(context.normalizedUris.contains("oss://bucket/db/t1/f.parquet"));
        // MUTATION: normalizing original_file_path too -> BE position-delete matching breaks -> red.
        TIcebergFileDesc fd = populate(ranges.get(0)).getTableFormatParams().getIcebergParams();
        Assertions.assertEquals("oss://bucket/db/t1/f.parquet", fd.getOriginalFilePath());
    }

    // --- T05: COUNT(*) pushdown (getCountFromSnapshot + collapse-to-one count range, mirrors paimon) ---

    private static final IcebergTableHandle T1 = new IcebergTableHandle("db1", "t1");

    private static List<ConnectorScanRange> planCount(IcebergScanPlanProvider provider, ConnectorSession session,
            boolean countPushdown) {
        // The COUNT-pushdown-aware 7-arg overload the generic PluginDrivenScanNode invokes (limit/
        // requiredPartitions are unused by the iceberg read path).
        return provider.planScan(session, T1, Collections.emptyList(), Optional.empty(),
                -1L, Collections.emptyList(), countPushdown);
    }

    @Test
    public void countPushdownCollapsesToSingleRangeWithTotalRecords() {
        // No-delete table; record counts 1024/100 + 2048/100 + 3072/100 = 10+20+30 = total-records 60.
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 1024, null, null))
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f2.parquet", 2048, null, null))
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f3.parquet", 3072, null, null))
                .commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        List<ConnectorScanRange> ranges = planCount(provider, null, true);

        // Collapse to ONE range carrying the full snapshot count (paimon parity; the legacy >10000 parallel
        // multi-split trim is dropped). MUTATION: ignoring countPushdown (T04 behavior) -> 3 ranges, each
        // count -1 -> red.
        Assertions.assertEquals(1, ranges.size());
        Assertions.assertEquals(60L, ranges.get(0).getPushDownRowCount());
        // Kept whole (NOT byte-tiled): the representative is a whole-file FileScanTask (start 0). MUTATION:
        // running splitFiles in the count path -> a sub-range could start != 0 / more than one range -> red.
        Assertions.assertEquals(0L, ranges.get(0).getStart());
        // table_level_row_count carries the total to BE's count reader.
        Assertions.assertEquals(60L, populate(ranges.get(0)).getTableFormatParams().getTableLevelRowCount());
    }

    @Test
    public void countPushdownNotAppliedWithEqualityDeletesScansAll() {
        Map<String, String> v2 = Collections.singletonMap(TableProperties.FORMAT_VERSION, "2");
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned(), v2);
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 1024, null, null))
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f2.parquet", 1024, null, null))
                .commit();
        // An equality delete makes total-equality-deletes != "0" -> count NOT pushable.
        table.newRowDelta()
                .addDeletes(equalityDeleteFile("s3://b/db/t1/eq.parquet", FileFormat.PARQUET, 1))
                .commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        List<ConnectorScanRange> ranges = planCount(provider, null, true);

        // Equality deletes -> getCountFromSnapshot returns -1 -> fall back to the normal scan (every data file,
        // each count -1 so BE reads & counts). MUTATION: pushing the count anyway -> 1 range / a count >= 0 -> red.
        Assertions.assertEquals(2, ranges.size());
        for (ConnectorScanRange range : ranges) {
            Assertions.assertEquals(-1L, range.getPushDownRowCount());
        }
    }

    @Test
    public void countPushdownWithPositionDeletesNetsOutWhenIgnoringDangling() {
        Map<String, String> v2 = Collections.singletonMap(TableProperties.FORMAT_VERSION, "2");
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned(), v2);
        // 1000/100 = 10 data records.
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 1000, null, null))
                .commit();
        DeleteFile posDelete = FileMetadata.deleteFileBuilder(table.spec())
                .ofPositionDeletes()
                .withPath("s3://b/db/t1/pos.parquet")
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(128L)
                .withRecordCount(3L)   // total-position-deletes 3
                .withReferencedDataFile("s3://b/db/t1/f1.parquet")
                .build();
        table.newRowDelta().addDeletes(posDelete).commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));
        ConnectorSession session = new FakeScanSession("UTC",
                Collections.singletonMap("ignore_iceberg_dangling_delete", "true"));

        List<ConnectorScanRange> ranges = planCount(provider, session, true);

        // total-records(10) - total-position-deletes(3) = 7, pushable only because the session ignores dangling
        // deletes. MUTATION: returning total-records (10) / not honoring the session flag -> wrong count -> red.
        Assertions.assertEquals(1, ranges.size());
        Assertions.assertEquals(7L, ranges.get(0).getPushDownRowCount());
        Assertions.assertEquals(7L, populate(ranges.get(0)).getTableFormatParams().getTableLevelRowCount());
    }

    @Test
    public void countPushdownWithPositionDeletesScansAllWhenNotIgnoringDangling() {
        Map<String, String> v2 = Collections.singletonMap(TableProperties.FORMAT_VERSION, "2");
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned(), v2);
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 1000, null, null))
                .commit();
        DeleteFile posDelete = FileMetadata.deleteFileBuilder(table.spec())
                .ofPositionDeletes()
                .withPath("s3://b/db/t1/pos.parquet")
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(128L)
                .withRecordCount(3L)
                .withReferencedDataFile("s3://b/db/t1/f1.parquet")
                .build();
        table.newRowDelta().addDeletes(posDelete).commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        // No ignore flag (null session -> default false): position deletes present -> not pushable.
        List<ConnectorScanRange> ranges = planCount(provider, null, true);

        // MUTATION: pushing the count without the ignore flag -> a count >= 0 / single collapsed range -> red.
        Assertions.assertFalse(ranges.isEmpty());
        for (ConnectorScanRange range : ranges) {
            Assertions.assertEquals(-1L, range.getPushDownRowCount());
        }
    }

    @Test
    public void countPushdownEmptyTableProducesNoRanges() {
        // Empty table (no snapshot) -> getCountFromSnapshot 0, but no representative file -> no range -> BE gets
        // 0 ranges -> COUNT returns 0 (legacy returns empty splits too). MUTATION: emitting a synthetic count
        // range with no path -> red (no file to build from).
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        List<ConnectorScanRange> ranges = planCount(provider, null, true);

        Assertions.assertTrue(ranges.isEmpty());
    }

    @Test
    public void countPushdownFalseDoesNormalMultiRangeScan() {
        // The 7-arg overload with countPushdown=false must behave exactly like the normal scan (no collapse).
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 1024, null, null))
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f2.parquet", 1024, null, null))
                .commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        List<ConnectorScanRange> ranges = planCount(provider, null, false);

        // MUTATION: collapsing on countPushdown=false -> 1 range -> red.
        Assertions.assertEquals(2, ranges.size());
        for (ConnectorScanRange range : ranges) {
            Assertions.assertEquals(-1L, range.getPushDownRowCount());
        }
    }

    // --- T08: manifest-level scan planning (gated by meta.cache.iceberg.manifest.enable) ---

    private static Map<String, String> manifestCacheProps() {
        Map<String, String> m = new HashMap<>();
        m.put("meta.cache.iceberg.manifest.enable", "true");
        return m;
    }

    /** A provider whose manifest-level path (and IcebergManifestCache) is enabled. */
    private static IcebergScanPlanProvider manifestProvider(Map<String, String> props, Table table,
            IcebergManifestCache cache) {
        return new IcebergScanPlanProvider(props, opsReturning(table), null, cache);
    }

    private static List<String> sortedPaths(List<ConnectorScanRange> ranges) {
        List<String> paths = new ArrayList<>();
        for (ConnectorScanRange r : ranges) {
            paths.add(r.getPath().get());
        }
        Collections.sort(paths);
        return paths;
    }

    @Test
    public void planScanManifestCacheEnabledMatchesSdkPathAndConsumesCache() {
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend()
                .appendFile(dataFile(PartitionSpec.unpartitioned(), "/d/a.parquet", 100, null, null))
                .appendFile(dataFile(PartitionSpec.unpartitioned(), "/d/b.parquet", 200, null, null))
                .appendFile(dataFile(PartitionSpec.unpartitioned(), "/d/c.parquet", 300, null, null))
                .commit();
        IcebergTableHandle handle = new IcebergTableHandle("db1", "t1");

        // Gate OFF (default): the iceberg SDK splitFiles path (T02).
        List<ConnectorScanRange> sdk = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table))
                .planScan(null, handle, Collections.emptyList(), Optional.empty());

        // Gate ON: the manifest-level path that reads manifests through the cache.
        IcebergManifestCache cache = new IcebergManifestCache();
        List<ConnectorScanRange> manifest = manifestProvider(manifestCacheProps(), table, cache)
                .planScan(emptySession(), handle, Collections.emptyList(), Optional.empty());

        // WHY: the manifest-level path must enumerate the SAME data files as the SDK path. MUTATION: a mistake
        // in the ported planning (wrong manifest/metrics/residual handling) drops or duplicates files -> red.
        Assertions.assertEquals(sortedPaths(sdk), sortedPaths(manifest));
        Assertions.assertEquals(3, manifest.size());
        // The cache was actually CONSUMED (the data manifest was read + stored). MUTATION: silently using the SDK
        // path despite the enable flag -> cache stays empty -> red.
        Assertions.assertTrue(cache.size() > 0, "the manifest cache must be populated by the gated path");
    }

    @Test
    public void planScanManifestCachePrunesPartitionLikeSdk() {
        PartitionSpec spec = PartitionSpec.builderFor(PART_SCHEMA).identity("p").build();
        Table table = createTable("pt", PART_SCHEMA, spec);
        table.newAppend()
                .appendFile(dataFile(spec, "/d/p1.parquet", 100, null, "p=1"))
                .appendFile(dataFile(spec, "/d/p2.parquet", 100, null, "p=2"))
                .commit();
        IcebergTableHandle handle = new IcebergTableHandle("db1", "pt");
        Optional<ConnectorExpression> wherePeq1 = Optional.of(eqInt("p", 1));

        List<ConnectorScanRange> sdk = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table))
                .planScan(null, handle, Collections.emptyList(), wherePeq1);
        IcebergManifestCache cache = new IcebergManifestCache();
        List<ConnectorScanRange> manifest = manifestProvider(manifestCacheProps(), table, cache)
                .planScan(emptySession(), handle, Collections.emptyList(), wherePeq1);

        // WHY: partition pruning (ManifestEvaluator + residual) must keep only p=1 in BOTH paths. MUTATION:
        // dropping the residual/metrics prune in the manifest path -> p=2 leaks in -> sizes differ -> red.
        Assertions.assertEquals(sortedPaths(sdk), sortedPaths(manifest));
        Assertions.assertEquals(1, manifest.size());
        Assertions.assertTrue(manifest.get(0).getPath().get().endsWith("p1.parquet"));
    }

    @Test
    public void planScanManifestCacheEmptyTableReturnsNoRanges() {
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        IcebergManifestCache cache = new IcebergManifestCache();
        List<ConnectorScanRange> ranges = manifestProvider(manifestCacheProps(), table, cache)
                .planScan(null, new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty());
        // An empty table has no snapshot; the manifest path returns no ranges (legacy parity). MUTATION:
        // NPE-ing on a null snapshot -> red.
        Assertions.assertTrue(ranges.isEmpty());
        Assertions.assertEquals(0, cache.size());
    }

    @Test
    public void planScanManifestGateDisabledByTtlZeroUsesSdkPath() {
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend()
                .appendFile(dataFile(PartitionSpec.unpartitioned(), "/d/a.parquet", 100, null, null))
                .commit();
        Map<String, String> props = manifestCacheProps();
        props.put("meta.cache.iceberg.manifest.ttl-second", "0"); // CacheSpec.isCacheEnabled: ttl==0 disables
        IcebergManifestCache cache = new IcebergManifestCache();
        List<ConnectorScanRange> ranges = manifestProvider(props, table, cache)
                .planScan(null, new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty());
        // enable=true but ttl-second=0 -> gate off -> SDK path -> the cache stays empty. MUTATION: ignoring the
        // ttl!=0 sub-condition -> the manifest path runs -> cache populated -> red.
        Assertions.assertEquals(1, ranges.size());
        Assertions.assertEquals(0, cache.size());
    }

    private static int deleteCount(ConnectorScanRange range) {
        TFileRangeDesc d = populate(range);
        if (!d.getTableFormatParams().isSetIcebergParams()
                || !d.getTableFormatParams().getIcebergParams().isSetDeleteFiles()) {
            return 0;
        }
        return d.getTableFormatParams().getIcebergParams().getDeleteFiles().size();
    }

    @Test
    public void planScanManifestCacheAssociatesDeletesLikeSdk() {
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend()
                .appendFile(dataFile(PartitionSpec.unpartitioned(), "/d/a.parquet", 100, null, null))
                .commit();
        // The position delete is committed in a LATER snapshot (higher sequence number) so it applies to the
        // earlier data file — exactly the case DeleteFileIndex.forDataFile(seq, file) resolves.
        table.newRowDelta()
                .addDeletes(positionDeleteFile("/d/a-pos-del.parquet", FileFormat.PARQUET, null, null))
                .commit();
        IcebergTableHandle handle = new IcebergTableHandle("db1", "t1");

        List<ConnectorScanRange> sdk = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table))
                .planScan(null, handle, Collections.emptyList(), Optional.empty());
        IcebergManifestCache cache = new IcebergManifestCache();
        List<ConnectorScanRange> manifest = manifestProvider(manifestCacheProps(), table, cache)
                .planScan(emptySession(), handle, Collections.emptyList(), Optional.empty());

        Assertions.assertEquals(1, sdk.size());
        Assertions.assertEquals(1, manifest.size());
        // WHY: the manifest-level path must associate the position delete with the data file via the VENDORED
        // DeleteFileIndex (the whole reason it is vendored), matching the SDK path. MUTATION: the vendored
        // DeleteFileIndex failing to attach the delete -> 0 -> red.
        Assertions.assertEquals(1, deleteCount(sdk.get(0)), "the SDK path associates the position delete");
        Assertions.assertEquals(deleteCount(sdk.get(0)), deleteCount(manifest.get(0)));
        // Both the data manifest and the delete manifest were read through the cache.
        Assertions.assertTrue(cache.size() >= 2, "the data + delete manifests must both be cached");
    }

    // --- T09: vended credentials (extractVendedToken + static/vended location.* + URI threading) ---

    @Test
    public void extractVendedTokenMergesIoPropsAndStorageCredentials() {
        FakeIcebergTable table = fakeTable("t1");
        Map<String, String> ioProps = new HashMap<>();
        ioProps.put("s3.endpoint", "ep");
        StorageCredential cred =
                StorageCredential.create("s3://b", Collections.singletonMap("s3.access-key-id", "ak"));
        table.setIo(new VendedFileIO(ioProps, Collections.singletonList(cred)));

        Map<String, String> token = IcebergScanPlanProvider.extractVendedToken(table, true);

        // WHY: legacy IcebergVendedCredentialsProvider.extractRawVendedCredentials = io.properties() UNION every
        // SupportsStorageCredentials.credentials().config(). MUTATION: dropping the credentials merge ->
        // s3.access-key-id absent -> red.
        Assertions.assertEquals("ep", token.get("s3.endpoint"));
        Assertions.assertEquals("ak", token.get("s3.access-key-id"));
    }

    @Test
    public void extractVendedTokenReturnsIoPropsWhenFileIoHasNoStorageCredentials() {
        FakeIcebergTable table = fakeTable("t1");
        table.setIo(new PropsOnlyFileIO(Collections.singletonMap("s3.endpoint", "ep")));

        // WHY: a non-SupportsStorageCredentials FileIO still contributes its own properties (legacy reads
        // io.properties() unconditionally) and must not crash on the absent credentials() call. MUTATION:
        // unconditional cast to SupportsStorageCredentials -> ClassCastException -> red.
        Assertions.assertEquals(Collections.singletonMap("s3.endpoint", "ep"),
                IcebergScanPlanProvider.extractVendedToken(table, true));
    }

    @Test
    public void extractVendedTokenEmptyWhenFlagDisabled() {
        FakeIcebergTable table = fakeTable("t1");
        StorageCredential cred =
                StorageCredential.create("s3://b", Collections.singletonMap("s3.access-key-id", "ak"));
        table.setIo(new VendedFileIO(Collections.emptyMap(), Collections.singletonList(cred)));

        // WHY: the catalog flag gates extraction (legacy isVendedCredentialsEnabled) BEFORE touching io() — a
        // non-REST / flag-off catalog must extract NOTHING even if the FileIO happens to vend creds. MUTATION:
        // ignoring vendedEnabled -> ak extracted -> red.
        Assertions.assertTrue(IcebergScanPlanProvider.extractVendedToken(table, false).isEmpty());
    }

    @Test
    public void extractVendedTokenEmptyForNullTable() {
        Assertions.assertTrue(IcebergScanPlanProvider.extractVendedToken(null, true).isEmpty());
    }

    @Test
    public void getScanNodePropertiesEmitsStaticStorageCredsAsLocation() {
        FakeIcebergTable table = fakeTable("t1");
        RecordingConnectorContext context = new RecordingConnectorContext();
        Map<String, String> beStatic = new HashMap<>();
        beStatic.put("AWS_ACCESS_KEY", "ak");
        beStatic.put("AWS_SECRET_KEY", "sk");
        beStatic.put("AWS_ENDPOINT", "ep");
        context.storageProperties = Collections.singletonList(fakeBackendStorage(beStatic));
        IcebergScanPlanProvider provider =
                new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table), context);

        Map<String, String> props = provider.getScanNodeProperties(
                null, new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty());

        // WHY (B-9): BE's native (FILE_S3) reader understands ONLY AWS_* canonical keys; the connector must ship
        // the engine-normalized creds under location.*, never the raw aliases (403 on a private bucket).
        // MUTATION: dropping the static-creds block (the gap this task fixes) -> location.AWS_ACCESS_KEY absent
        // -> red.
        Assertions.assertEquals("ak", props.get("location.AWS_ACCESS_KEY"));
        Assertions.assertEquals("sk", props.get("location.AWS_SECRET_KEY"));
        Assertions.assertEquals("ep", props.get("location.AWS_ENDPOINT"));
    }

    @Test
    public void getScanNodePropertiesOverlaysVendedCredsOverStatic() {
        FakeIcebergTable table = fakeTable("t1");
        // A non-empty FileIO props map -> a non-empty vended token when the flag is on.
        table.setIo(new PropsOnlyFileIO(Collections.singletonMap("s3.endpoint", "x")));
        RecordingConnectorContext context = new RecordingConnectorContext();
        Map<String, String> beStatic = new HashMap<>();
        beStatic.put("AWS_ACCESS_KEY", "static-ak");
        beStatic.put("AWS_ENDPOINT", "static-ep");
        context.storageProperties = Collections.singletonList(fakeBackendStorage(beStatic));
        Map<String, String> vended = new HashMap<>();
        vended.put("AWS_ACCESS_KEY", "vended-ak");
        vended.put("AWS_SECRET_KEY", "vended-sk");
        vended.put("AWS_ENDPOINT", "vended-ep");
        context.vendedBeProps = vended;
        IcebergScanPlanProvider provider =
                new IcebergScanPlanProvider(restVendedFlagOn(), opsReturning(table), context);

        Map<String, String> props = provider.getScanNodeProperties(
                null, new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty());

        // WHY: vended creds must overlay (win over) the static location key on collision (legacy precedence).
        // MUTATION: overlaying static AFTER vended (or no vended overlay) -> location.AWS_ACCESS_KEY != vended-ak
        // -> red.
        Assertions.assertEquals("vended-ak", props.get("location.AWS_ACCESS_KEY"));
        Assertions.assertEquals("vended-sk", props.get("location.AWS_SECRET_KEY"));
        Assertions.assertEquals("vended-ep", props.get("location.AWS_ENDPOINT"));
    }

    @Test
    public void getScanNodePropertiesOmitsVendedWhenFlagDisabled() {
        FakeIcebergTable table = fakeTable("t1");
        // Even a credential-bearing FileIO must yield no vended overlay when the catalog flag is off.
        table.setIo(new PropsOnlyFileIO(Collections.singletonMap("s3.endpoint", "x")));
        RecordingConnectorContext context = new RecordingConnectorContext();
        context.vendedBeProps = Collections.singletonMap("AWS_ACCESS_KEY", "vended-ak");
        // No vended flag -> default false.
        IcebergScanPlanProvider provider =
                new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table), context);

        Map<String, String> props = provider.getScanNodeProperties(
                null, new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty());

        // WHY: the catalog flag gates the vended overlay (legacy isVendedCredentialsEnabled). Flag off -> empty
        // token -> vendStorageCredentials returns empty -> no vended location.*. MUTATION: ignoring the flag ->
        // location.AWS_ACCESS_KEY=vended-ak present -> red.
        Assertions.assertFalse(props.containsKey("location.AWS_ACCESS_KEY"), "flag off -> no vended overlay");
    }

    @Test
    public void getScanNodePropertiesOmitsVendedWhenFlagSetButNonRestFlavor() {
        FakeIcebergTable table = fakeTable("t1");
        table.setIo(new PropsOnlyFileIO(Collections.singletonMap("s3.endpoint", "x")));
        RecordingConnectorContext context = new RecordingConnectorContext();
        context.vendedBeProps = Collections.singletonMap("AWS_ACCESS_KEY", "vended-ak");
        // The vended flag is set, but the catalog flavor is HMS (not REST). Legacy isVendedCredentialsEnabled is
        // `instanceof IcebergRestProperties && flag`, so a non-REST catalog NEVER vends even with the flag set.
        Map<String, String> hmsWithRestFlag = new HashMap<>();
        hmsWithRestFlag.put(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE, IcebergConnectorProperties.TYPE_HMS);
        hmsWithRestFlag.put(IcebergConnectorProperties.REST_VENDED_CREDENTIALS_ENABLED, "true");
        IcebergScanPlanProvider provider =
                new IcebergScanPlanProvider(hmsWithRestFlag, opsReturning(table), context);

        Map<String, String> props = provider.getScanNodeProperties(
                null, new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty());

        // WHY: legacy gates vended on the REST metastore type (instanceof IcebergRestProperties), not just the
        // flag; a misconfigured non-REST catalog carrying the flag must NOT vend (parity). MUTATION: gating on
        // the flag alone -> location.AWS_ACCESS_KEY=vended-ak present -> red.
        Assertions.assertFalse(props.containsKey("location.AWS_ACCESS_KEY"),
                "non-REST flavor -> no vended overlay even with the flag set");
    }

    @Test
    public void getScanNodePropertiesNoContextEmitsNoLocation() {
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        // 2-arg ctor -> context == null (offline harness path).
        IcebergScanPlanProvider provider =
                new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        Map<String, String> props = provider.getScanNodeProperties(
                null, new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty());

        // WHY: with no context the connector cannot normalize creds, so it emits NO location.* (never raw
        // aliases). MUTATION: NPE on null context, or emitting location.* -> red.
        Assertions.assertTrue(props.keySet().stream().noneMatch(k -> k.startsWith("location.")),
                "no context -> no location.* keys");
    }

    @Test
    public void getScanNodePropertiesSkipsStorageWithoutBackendModelAndMergesRest() {
        FakeIcebergTable table = fakeTable("t1");
        RecordingConnectorContext context = new RecordingConnectorContext();
        Map<String, String> beMap = new HashMap<>();
        beMap.put("AWS_ACCESS_KEY", "ak");
        beMap.put("AWS_ENDPOINT", "ep");
        // A typed list mixing a backend WITHOUT a BE model (toBackendProperties() empty — the HDFS case) and a
        // real object-store backend: exercises the .ifPresent skip and the multi-entry putAll merge.
        context.storageProperties = Arrays.asList(fakeStorageWithoutBackend(), fakeBackendStorage(beMap));
        IcebergScanPlanProvider provider =
                new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table), context);

        Map<String, String> props = provider.getScanNodeProperties(
                null, new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty());

        // WHY: a StorageProperties with no BE model (Optional.empty) must be SKIPPED, never crash, while a real
        // object-store entry alongside it still ships its AWS_* under location.* (the merge loop). MUTATION:
        // .ifPresent -> .get()/.orElseThrow() -> NoSuchElementException on the empty entry -> red.
        Assertions.assertEquals("ak", props.get("location.AWS_ACCESS_KEY"));
        Assertions.assertEquals("ep", props.get("location.AWS_ENDPOINT"));
    }

    @Test
    public void planScanThreadsVendedTokenIntoDataAndDeletePathNormalize() {
        Map<String, String> v2 = Collections.singletonMap(TableProperties.FORMAT_VERSION, "2");
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned(), v2);
        table.newAppend()
                .appendFile(dataFile(table.spec(), "oss://b/db/t1/f1.parquet", 1024, null, null))
                .commit();
        table.newRowDelta()
                .addDeletes(positionDeleteFile("oss://b/db/t1/pos.parquet", FileFormat.PARQUET, null, null))
                .commit();
        RecordingConnectorContext context = new RecordingConnectorContext();
        // No vended flag -> the extracted token is empty; the in-memory FileIO's properties() throws (a test
        // artifact, unlike a real REST FileIO), so flag-on extraction is exercised separately by the
        // extractVendedToken / getScanNodeProperties overlay tests with an injected FileIO. Here we prove the
        // PLUMBING: planScan routes BOTH the data and delete paths through the 2-arg normalizeStorageUri,
        // passing the per-table vended token (empty here, but the MAP, not null).
        IcebergScanPlanProvider provider =
                new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table), context);

        List<ConnectorScanRange> ranges = provider.planScan(
                null, new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty());
        Assertions.assertEquals(1, ranges.size());

        // WHY: both the data-file path and the delete-file path must route through the 2-arg
        // normalizeStorageUri carrying the per-table vended token (T09), so REST object-store paths normalize
        // via the vended map. MUTATION: dropping the normalization -> the data/delete paths stay oss://
        // (un-normalized) -> red; reverting to the 1-arg normalize -> the recording fake's 1-arg form folds to
        // a NULL token -> lastVendedToken == null != the extracted (empty) map -> red.
        Assertions.assertEquals("s3://b/db/t1/f1.parquet", ranges.get(0).getPath().get());
        TIcebergFileDesc fd = populate(ranges.get(0)).getTableFormatParams().getIcebergParams();
        Assertions.assertEquals("s3://b/db/t1/pos.parquet", fd.getDeleteFiles().get(0).getPath());
        Assertions.assertEquals(IcebergScanPlanProvider.extractVendedToken(table, false), context.lastVendedToken);
    }

    @Test
    public void convertDeleteNormalizesDeletePathViaVendedToken() {
        RecordingConnectorContext context = new RecordingConnectorContext();
        IcebergScanPlanProvider provider =
                new IcebergScanPlanProvider(Collections.emptyMap(), new RecordingIcebergCatalogOps(), context);
        DeleteFile delete = positionDeleteFile("oss://bucket/db/t1/pos.parquet", FileFormat.PARQUET, null, null);
        Map<String, String> token = Collections.singletonMap("s3.access-key-id", "ak");

        TIcebergDeleteFileDesc d = provider.convertDelete(delete, token).toThrift();

        // WHY: convertDelete must thread the vended token into the 2-arg normalize (T09). MUTATION: passing no
        // token / the 1-arg normalize -> lastVendedToken != token -> red.
        Assertions.assertEquals("s3://bucket/db/t1/pos.parquet", d.getPath());
        Assertions.assertEquals(token, context.lastVendedToken);
    }

    // --- T10 parity gap-fills (audit wf_9d88fe61-5c7) ---

    @Test
    public void planScanDefaultSplitHeuristicTilesAndMaxFileSplitNumCapCollapses() {
        // PP-1: the DEFAULT split heuristic (determineTargetFileSplitSize, splitFiles:738) + the
        // max_file_split_num cap escalation were never exercised by a range count — the existing split test
        // forces the file_split_size override branch (:727), and the small-file tests never sub-split. Drive
        // BOTH branches on one 96MB file WITHOUT split offsets, so the iceberg fixed-size splitter tiles it by
        // the heuristic's target size directly (an offset-aware file would cut at every row group and ignore a
        // larger target, making the cap's effect invisible to a count assertion).
        long mb = 1024L * 1024L;
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/big.parquet", 96 * mb, null, null))
                .commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        // (1) DEFAULT heuristic (NO file_split_size override): total 96MB << maxSplitSize*maxInitialSplitNum
        // (12.8GB) so no escalation -> 32MB initial target; the 100000-file cap is far below 32MB -> the file
        // tiles into >1 contiguous ranges. MUTATION: bypassing determineTargetFileSplitSize (whole file) -> 1
        // range -> red. (This is the default branch, distinct from the override branch the existing test drives.)
        List<ConnectorScanRange> def = provider.planScan(
                null, new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty());
        Assertions.assertTrue(def.size() > 1, "default heuristic must tile the 96MB file, got " + def.size());
        def.sort((a, b) -> Long.compare(a.getStart(), b.getStart()));
        long expectedStart = 0;
        long total = 0;
        for (ConnectorScanRange r : def) {
            Assertions.assertEquals(expectedStart, r.getStart(), "default-heuristic ranges must tile contiguously");
            expectedStart += r.getLength();
            total += r.getLength();
        }
        Assertions.assertEquals(96 * mb, total, "the default-heuristic ranges must cover the whole file");

        // (2) max_file_split_num=1 forces minSplitSizeForMaxNum = ceil(96MB/1) = 96MB, so the cap raises the
        // target to the whole file -> exactly ONE range. This is the ONLY test driving the cap escalation
        // (Math.max(result, minSplitSizeForMaxNum)). MUTATION: dropping the cap -> target stays 32MB -> >1 -> red.
        ConnectorSession capOne = new FakeScanSession("UTC", Collections.singletonMap("max_file_split_num", "1"));
        List<ConnectorScanRange> capped = provider.planScan(
                capOne, new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty());
        Assertions.assertEquals(1, capped.size(), "max_file_split_num=1 must collapse to one whole-file range");
        Assertions.assertEquals(0L, capped.get(0).getStart());
        Assertions.assertEquals(96 * mb, capped.get(0).getLength());
    }

    @Test
    public void planScanPartitionBearingFileWithNoIdentityValuesEmitsNoColumnsFromPath() {
        // NF-1: a table partitioned by a NON-identity transform (bucket) is partition-bearing (spec id set) but
        // has ZERO identity columns-from-path — the T03 Bug2 shape (partition evolution / bucket-only spec). The
        // range must report isPartitionBearing()==true (so the engine does NOT fall back to Hive path-parsing,
        // which throws on iceberg's non-key=value layout) yet emit an EMPTY partition-values list and NO
        // columns-from-path. The carrier unit test pins isPartitionBearing in isolation; this drives the empty
        // path end-to-end through buildRange. MUTATION: deriving isPartitionBearing from a non-empty values list
        // -> false here -> the engine path-parses -> red.
        PartitionSpec spec = PartitionSpec.builderFor(PART_SCHEMA).bucket("id", 4).build();
        Table table = createTable("ev", PART_SCHEMA, spec);
        table.newAppend()
                .appendFile(dataFile(spec, "s3://b/db/ev/f.parquet", 512, null, "id_bucket=0", FileFormat.PARQUET))
                .commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        List<ConnectorScanRange> ranges = provider.planScan(
                null, new IcebergTableHandle("db1", "ev"), Collections.emptyList(), Optional.empty());
        Assertions.assertEquals(1, ranges.size());
        ConnectorScanRange range = ranges.get(0);
        Assertions.assertTrue(range.isPartitionBearing(), "a bucket-partitioned file is partition-bearing");
        Assertions.assertTrue(range.getPartitionValues().isEmpty(), "no identity columns -> empty partition values");

        TFileRangeDesc rd = populate(range);
        Assertions.assertTrue(rd.getTableFormatParams().getIcebergParams().isSetPartitionSpecId(),
                "partition-bearing -> spec id is emitted");
        Assertions.assertFalse(rd.isSetColumnsFromPathKeys(), "no identity values -> no columns-from-path keys");
        Assertions.assertFalse(rd.isSetColumnsFromPath());
        Assertions.assertFalse(rd.isSetColumnsFromPathIsNull());
    }

    @Test
    public void convertDeletePositionDeleteTreatsStoredMinusOneBoundAsUnset() {
        // G1: a genuinely-STORED -1L DELETE_FILE_POS bound (distinct from an ABSENT bounds map) must collapse to
        // UNSET (readPositionBound:483 `value == -1L -> null`), mirroring legacy IcebergDeleteFileFilter's -1
        // sentinel. The existing no-bounds test passes a null map (early return), never reaching the value==-1L
        // arm. MUTATION: dropping the `|| value == -1L` arm (emitting -1 as a real bound) -> red.
        DeleteFile bothMinusOne = positionDeleteFile("s3://b/db/t1/pos.parquet", FileFormat.PARQUET, -1L, -1L);
        TIcebergDeleteFileDesc d = provider().convertDelete(bothMinusOne, Collections.emptyMap()).toThrift();
        Assertions.assertEquals(1, d.getContent());
        Assertions.assertFalse(d.isSetPositionLowerBound());
        Assertions.assertFalse(d.isSetPositionUpperBound());

        // Mixed: only the -1L bound is dropped; a real lower bound still emits.
        DeleteFile mixed = positionDeleteFile("s3://b/db/t1/pos2.parquet", FileFormat.PARQUET, 3L, -1L);
        TIcebergDeleteFileDesc m = provider().convertDelete(mixed, Collections.emptyMap()).toThrift();
        Assertions.assertEquals(3L, m.getPositionLowerBound());
        Assertions.assertFalse(m.isSetPositionUpperBound());
    }

    @Test
    public void extractVendedTokenCredentialWinsOnKeyCollision() {
        // VC-2: extractVendedToken seeds io.properties() then putAll(credential.config()), so on a DUPLICATE key
        // the server-vended StorageCredential WINS (legacy IcebergVendedCredentialsProvider ordering). The
        // existing merge test uses disjoint keys and cannot pin this precedence. MUTATION: seeding credentials
        // first then overlaying io.properties() -> s3.access-key-id == "io-ak" -> red.
        FakeIcebergTable table = fakeTable("t1");
        Map<String, String> ioProps = new HashMap<>();
        ioProps.put("s3.access-key-id", "io-ak");   // colliding key, io value
        ioProps.put("s3.endpoint", "io-ep");        // disjoint key, must survive
        StorageCredential cred =
                StorageCredential.create("s3://b", Collections.singletonMap("s3.access-key-id", "cred-ak"));
        table.setIo(new VendedFileIO(ioProps, Collections.singletonList(cred)));

        Map<String, String> token = IcebergScanPlanProvider.extractVendedToken(table, true);
        Assertions.assertEquals("cred-ak", token.get("s3.access-key-id"));
        Assertions.assertEquals("io-ep", token.get("s3.endpoint"));
    }

    @Test
    public void planScanCombinesPartitionPruneDeleteAndPathNormalizeOnOneRange() {
        // G2 + E2E-1 + E2E-2: a real-query shape legacy builds in a single createIcebergSplit pass — a partitioned
        // v2 object-store table with a position delete, scanned under WHERE p=1. The existing delete e2e tests all
        // use UNPARTITIONED tables, so the co-existence of partition + delete + normalization carriers on the ONE
        // range a predicate leaves was never pinned. The surviving p=1 range must carry TOGETHER: (a) a
        // scheme-normalized data path with a RAW original_file_path, (b) partition columns-from-path, (c) its
        // position delete (also scheme-normalized). MUTATION: a predicate path skipping delete attachment, or the
        // partition block clobbering the delete block (or vice-versa), drops one of these -> red.
        PartitionSpec spec = PartitionSpec.builderFor(PART_SCHEMA).identity("p").build();
        Map<String, String> v2 = Collections.singletonMap(TableProperties.FORMAT_VERSION, "2");
        Table table = createTable("pt", PART_SCHEMA, spec, v2);
        table.newAppend()
                .appendFile(dataFile(spec, "oss://b/db/pt/p=1/a.parquet", 512, null, "p=1"))
                .appendFile(dataFile(spec, "oss://b/db/pt/p=2/b.parquet", 512, null, "p=2"))
                .commit();
        // Position delete on the p=1 data file, committed in a LATER snapshot (higher seq) and tagged to the p=1
        // partition so DeleteFileIndex.forDataFile resolves it to a.parquet only.
        DeleteFile posDelete = FileMetadata.deleteFileBuilder(spec)
                .ofPositionDeletes()
                .withPath("oss://b/db/pt/p=1/a-pos-del.parquet")
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(128L)
                .withRecordCount(2L)
                .withPartitionPath("p=1")
                .withReferencedDataFile("oss://b/db/pt/p=1/a.parquet")
                .build();
        table.newRowDelta().addDeletes(posDelete).commit();
        RecordingConnectorContext context = new RecordingConnectorContext();
        IcebergScanPlanProvider provider =
                new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table), context);

        List<ConnectorScanRange> ranges = provider.planScan(
                null, new IcebergTableHandle("db1", "pt"), Collections.emptyList(), Optional.of(eqInt("p", 1)));

        // (a) predicate pruned p=2 -> exactly the p=1 data file survives, scheme-normalized; original raw.
        Assertions.assertEquals(1, ranges.size());
        ConnectorScanRange range = ranges.get(0);
        Assertions.assertEquals("s3://b/db/pt/p=1/a.parquet", range.getPath().get());
        TFileRangeDesc rd = populate(range);
        TIcebergFileDesc fd = rd.getTableFormatParams().getIcebergParams();
        Assertions.assertEquals("oss://b/db/pt/p=1/a.parquet", fd.getOriginalFilePath());
        // (b) partition columns-from-path on the surviving range.
        Assertions.assertEquals(Collections.singletonList("p"), rd.getColumnsFromPathKeys());
        Assertions.assertEquals(Collections.singletonList("1"), rd.getColumnsFromPath());
        Assertions.assertEquals(Collections.singletonList(false), rd.getColumnsFromPathIsNull());
        Assertions.assertEquals("[\"1\"]", fd.getPartitionDataJson());
        // (c) the position delete attached to THIS range, path scheme-normalized.
        Assertions.assertEquals(1, fd.getDeleteFilesSize());
        Assertions.assertEquals("s3://b/db/pt/p=1/a-pos-del.parquet", fd.getDeleteFiles().get(0).getPath());
        Assertions.assertEquals(1, fd.getDeleteFiles().get(0).getContent());
    }

    // --- T09 helpers ---

    private static FakeIcebergTable fakeTable(String name) {
        // write.format.default: getScanNodeProperties now resolves the scan-level format off the table (it
        // drives BE's FileScannerV2-vs-V1 pick -- see getScanNodePropertiesEmitsPathPartitionKeysAndRealDataFormat).
        // A declared format keeps these credential tests off IcebergUtils' last-resort planFiles() inference,
        // which FakeIcebergTable deliberately refuses; a real table normally declares it too.
        return new FakeIcebergTable(name, SCHEMA, PartitionSpec.unpartitioned(),
                "s3://b/" + name, Collections.singletonMap(TableProperties.DEFAULT_FILE_FORMAT, "parquet"));
    }

    /** Catalog props with BOTH the REST flavor and the vended flag on — the two-part legacy gate. */
    private static Map<String, String> restVendedFlagOn() {
        Map<String, String> props = new HashMap<>();
        props.put(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE, IcebergConnectorProperties.TYPE_REST);
        props.put(IcebergConnectorProperties.REST_VENDED_CREDENTIALS_ENABLED, "true");
        return props;
    }

    /** A fe-filesystem {@link StorageProperties} whose toBackendProperties().toMap() returns the given
     * BE-canonical map — mirrors how a real object-store binding hands BE creds to the connector (P1-T04). */
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

    /** A fe-filesystem {@link StorageProperties} with NO backend model — toBackendProperties() defaults to
     * Optional.empty() (the HDFS case: no typed BE binding in fe-filesystem). */
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

    // --- T05: system-table (JNI) serialized-split scan path (planScan on an iceberg $sys handle) ---

    @Test
    public void planScanForSystemTableSerializesEachFileScanTaskAsJniSplit() {
        // A $snapshots handle plans through the metadata table (MetadataTableUtils.createMetadataTableInstance):
        // each metadata FileScanTask is serialized (SerializationUtil.serializeToBase64) and emitted as a JNI
        // split carrying ONLY serialized_split + FORMAT_JNI + table_level_row_count=-1, mirroring legacy
        // IcebergScanNode.doGetSystemTableSplits + setIcebergParams. MUTATION: routing the sys handle through
        // the normal data-file path (resolveTable + buildRange) -> the range carries the f1.parquet path and no
        // serialized_split -> red.
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 1024, null, null)).commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        List<ConnectorScanRange> ranges = provider.planScan(
                null, IcebergTableHandle.forSystemTable("db1", "t1", "snapshots", -1L, null, -1L),
                Collections.emptyList(), Optional.empty());

        Assertions.assertFalse(ranges.isEmpty(), "the $snapshots metadata table must plan at least one split");
        for (ConnectorScanRange range : ranges) {
            String serialized = ((IcebergScanRange) range).getSerializedSplit();
            Assertions.assertNotNull(serialized, "every sys split must carry a serialized FileScanTask");
            Assertions.assertFalse(serialized.isEmpty());
            TFileRangeDesc rangeDesc = populate(range);
            Assertions.assertEquals(TFileFormatType.FORMAT_JNI, rangeDesc.getFormatType());
            Assertions.assertEquals(serialized,
                    rangeDesc.getTableFormatParams().getIcebergParams().getSerializedSplit());
            Assertions.assertEquals(-1L, rangeDesc.getTableFormatParams().getTableLevelRowCount());
        }
    }

    @Test
    public void planScanForSystemTableSplitDeserializesThroughTheBeJniReaderPath() throws Exception {
        // The strongest FE-reachable byte-shape parity check: the serialized_split must be consumable EXACTLY
        // as BE's IcebergSysTableJniScanner consumes it —
        // SerializationUtil.deserializeFromBase64(...).asDataTask().rows() — and must carry the METADATA-table
        // schema ($snapshots), not the base table's. (Cross-version / classloader interop is P6.8 docker e2e.)
        // MUTATION: serializing anything other than the FileScanTask (e.g. the DataFile) -> deserialize /
        // asDataTask() fails or yields the wrong schema -> red.
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 1024, null, null)).commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        List<ConnectorScanRange> ranges = provider.planScan(
                null, IcebergTableHandle.forSystemTable("db1", "t1", "snapshots", -1L, null, -1L),
                Collections.emptyList(), Optional.empty());

        long snapshotRows = 0;
        for (ConnectorScanRange range : ranges) {
            FileScanTask task =
                    SerializationUtil.deserializeFromBase64(((IcebergScanRange) range).getSerializedSplit());
            // the deserialized task exposes the $snapshots metadata schema, not the base table's columns.
            Assertions.assertNotNull(task.schema().findField("snapshot_id"),
                    "the serialized split must carry the metadata-table ($snapshots) schema");
            Assertions.assertNull(task.schema().findField("name"),
                    "the serialized split must NOT carry the base table's columns");
            try (CloseableIterable<StructLike> rows = task.asDataTask().rows()) {
                Iterator<StructLike> it = rows.iterator();
                while (it.hasNext()) {
                    it.next();
                    snapshotRows++;
                }
            }
        }
        Assertions.assertEquals(1L, snapshotRows, "one commit -> the $snapshots table has one row");
    }

    @Test
    public void planScanForSystemTableHonorsTheSnapshotPin() throws Exception {
        // Iceberg system tables are legal time-travel targets (deviation (1)): the connector must apply the
        // snapshot pin to the metadata-table scan (legacy createTableScan -> scan.useSnapshot). $files is the
        // time-travel-observable table: it lists the data files LIVE in the pinned snapshot. S1 has one file;
        // after S2 the latest $files lists two. Pinned to S1 the connector must read only S1's view (one file
        // row), proving the pin flows into buildScan. MUTATION: bypassing buildScan / dropping the pin (reading
        // latest) -> two rows -> red. (NB: $snapshots ignores useSnapshot — it always lists all snapshots from
        // current metadata — so it is NOT observable here; legacy has the identical no-op, both apply the pin.)
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 1024, null, null)).commit();
        long s1 = table.currentSnapshot().snapshotId();
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db/t1/f2.parquet", 1024, null, null)).commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        long latestRows = countSerializedSplitRows(provider.planScan(
                null, IcebergTableHandle.forSystemTable("db1", "t1", "files", -1L, null, -1L),
                Collections.emptyList(), Optional.empty()));
        long pinnedRows = countSerializedSplitRows(provider.planScan(
                null, IcebergTableHandle.forSystemTable("db1", "t1", "files", s1, null, -1L),
                Collections.emptyList(), Optional.empty()));

        Assertions.assertEquals(2L, latestRows, "latest $files should list both data files");
        Assertions.assertEquals(1L, pinnedRows, "pinned-to-S1 $files should list only S1's file");
    }

    @Test
    public void planScanForSystemTableLoadsMetadataInsideTheAuthScope() {
        // The base-table load + metadata-table build run inside ONE context.executeAuthenticated, so the
        // FE-injected Kerberos UGI covers the remote base load (mirrors IcebergConnectorMetadata.loadSysTable /
        // legacy IcebergSysExternalTable.getSysIcebergTable). The base table is loaded by its BARE name, never a
        // "$snapshots" suffix. MUTATION: resolving the metadata table OUTSIDE the auth wrap -> authCount 0 -> red.
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 1024, null, null)).commit();
        RecordingIcebergCatalogOps ops = opsReturning(table);
        RecordingConnectorContext context = new RecordingConnectorContext();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), ops, context);

        provider.planScan(null, IcebergTableHandle.forSystemTable("db1", "t1", "snapshots", -1L, null, -1L),
                Collections.emptyList(), Optional.empty());

        Assertions.assertEquals(1, context.authCount);
        Assertions.assertEquals("db1", ops.lastLoadDb);
        Assertions.assertEquals("t1", ops.lastLoadTable);
    }

    @Test
    public void planScanForSystemTableCarriesPredicateAsResidualForBe() throws Exception {
        // Predicate pushdown for a SYS table is FE-reachable as the RESIDUAL carried on the serialized
        // FileScanTask: planSystemTableScan -> buildScan -> scan.filter(record_count==10) records the
        // converted predicate as the metadata scan's residual, which BE's IcebergSysTableJniScanner applies
        // when reading $files rows. NB: a metadata-COLUMN predicate is a residual, NOT a manifest prune, so
        // the FE-visible row count is unchanged (verified: 2 vs 2) — the row-level prune happens at BE read
        // time; the FE plan-time prune is the SNAPSHOT pin (see planScanForSystemTableHonorsTheSnapshotPin).
        // MUTATION: dropping the `filter` arg on the sys path (planSystemTableScan ignores it) -> the
        // residual stays alwaysTrue even with a predicate -> red.
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 1000, null, null))
                .appendFile(dataFile(table.spec(), "s3://b/db/t1/f2.parquet", 100, null, null))
                .commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        // record_count is an iceberg LONG field of the $files metadata schema; the converter resolves it by
        // name and pushes a BIGINT equality.
        ConnectorExpression recordCountEq10 = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("record_count", ConnectorType.of("BIGINT")),
                new ConnectorLiteral(ConnectorType.of("BIGINT"), 10L));

        String unfilteredResidual = firstSysSplitResidual(provider.planScan(
                null, IcebergTableHandle.forSystemTable("db1", "t1", "files", -1L, null, -1L),
                Collections.emptyList(), Optional.empty()));
        String filteredResidual = firstSysSplitResidual(provider.planScan(
                null, IcebergTableHandle.forSystemTable("db1", "t1", "files", -1L, null, -1L),
                Collections.emptyList(), Optional.of(recordCountEq10)));

        // No predicate -> the metadata scan carries no residual (alwaysTrue); a pushable predicate -> the
        // residual references record_count, proving the converted filter reached scan.filter.
        Assertions.assertTrue(unfilteredResidual.equalsIgnoreCase("true"),
                "with no predicate the sys metadata scan must carry no residual; got: " + unfilteredResidual);
        Assertions.assertTrue(filteredResidual.contains("record_count"),
                "a pushable $files predicate must be carried as the scan residual for BE; got: " + filteredResidual);
    }

    private static String firstSysSplitResidual(List<ConnectorScanRange> ranges) throws Exception {
        Assertions.assertFalse(ranges.isEmpty(), "the metadata table must plan at least one split");
        FileScanTask task =
                SerializationUtil.deserializeFromBase64(((IcebergScanRange) ranges.get(0)).getSerializedSplit());
        return task.residual().toString();
    }

    @Test
    public void planScanForSystemTableSetsDummyPathOnEverySplit() {
        // Every sys split's path is the sentinel "/dummyPath" (IcebergScanPlanProvider.SYS_TABLE_DUMMY_PATH):
        // a metadata-table split carries its payload in serialized_split and BE never opens a real file path
        // for it (mirrors legacy doGetSystemTableSplits, which sets a dummy path). The earlier T05 tests only
        // assert path on data ranges they supply themselves; this pins the provider-built sys-range path.
        // MUTATION: building the sys range with the real data-file path instead of SYS_TABLE_DUMMY_PATH ->
        // path != "/dummyPath" -> red.
        Table table = createTable("t1", SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db/t1/f1.parquet", 1024, null, null)).commit();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        List<ConnectorScanRange> ranges = provider.planScan(
                null, IcebergTableHandle.forSystemTable("db1", "t1", "snapshots", -1L, null, -1L),
                Collections.emptyList(), Optional.empty());

        Assertions.assertFalse(ranges.isEmpty(), "the $snapshots metadata table must plan at least one split");
        for (ConnectorScanRange range : ranges) {
            Assertions.assertEquals("/dummyPath", range.getPath().get(),
                    "every iceberg sys split must carry the sentinel dummy path");
        }
    }

    private static long countSerializedSplitRows(List<ConnectorScanRange> ranges) throws Exception {
        long rows = 0;
        for (ConnectorScanRange range : ranges) {
            FileScanTask task =
                    SerializationUtil.deserializeFromBase64(((IcebergScanRange) range).getSerializedSplit());
            try (CloseableIterable<StructLike> closeable = task.asDataTask().rows()) {
                Iterator<StructLike> it = closeable.iterator();
                while (it.hasNext()) {
                    it.next();
                    rows++;
                }
            }
        }
        return rows;
    }

    /** A fake FileIO carrying only its own properties (no server-vended StorageCredentials). */
    private static final class PropsOnlyFileIO implements FileIO {
        private final Map<String, String> props;

        PropsOnlyFileIO(Map<String, String> props) {
            this.props = props;
        }

        @Override
        public Map<String, String> properties() {
            return props;
        }

        @Override
        public InputFile newInputFile(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public OutputFile newOutputFile(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteFile(String path) {
            throw new UnsupportedOperationException();
        }
    }

    /** A fake FileIO that ALSO vends StorageCredentials (a REST catalog's delegated creds). */
    private static final class VendedFileIO implements FileIO, SupportsStorageCredentials {
        private final Map<String, String> props;
        private final List<StorageCredential> creds;

        VendedFileIO(Map<String, String> props, List<StorageCredential> creds) {
            this.props = props;
            this.creds = creds;
        }

        @Override
        public Map<String, String> properties() {
            return props;
        }

        @Override
        public List<StorageCredential> credentials() {
            return creds;
        }

        @Override
        public void setCredentials(List<StorageCredential> credentials) {
            throw new UnsupportedOperationException();
        }

        @Override
        public InputFile newInputFile(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public OutputFile newOutputFile(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteFile(String path) {
            throw new UnsupportedOperationException();
        }
    }
}
