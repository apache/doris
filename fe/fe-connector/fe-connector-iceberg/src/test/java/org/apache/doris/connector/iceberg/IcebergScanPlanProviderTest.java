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
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
    public void getScanNodePropertiesEmitsPathPartitionKeysAndJniFormat() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "P", Types.IntegerType.get()),
                Types.NestedField.required(3, "region", Types.StringType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("P").identity("region").build();
        Table table = createTable("pt", schema, spec);
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table));

        Map<String, String> props = provider.getScanNodeProperties(
                null, new IcebergTableHandle("db1", "pt"), Collections.emptyList(), Optional.empty());

        // path_partition_keys = lowercased, comma-joined identity columns (the CI #968880 double-fill guard);
        // file_format_type=jni makes the parent default to FORMAT_JNI, overridden per native range.
        // MUTATION: omitting path_partition_keys -> BE double-fills partition columns -> DCHECK -> this red.
        Assertions.assertEquals("p,region", props.get("path_partition_keys"));
        Assertions.assertEquals("jni", props.get("file_format_type"));
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
        TIcebergDeleteFileDesc d = provider().convertDelete(delete).toThrift();

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
        TIcebergDeleteFileDesc d = provider().convertDelete(delete).toThrift();

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
        TIcebergDeleteFileDesc d = provider().convertDelete(delete).toThrift();

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
        TIcebergDeleteFileDesc d = provider().convertDelete(delete).toThrift();

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

        TIcebergDeleteFileDesc d = provider.convertDelete(delete).toThrift();

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
}
