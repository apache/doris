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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
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
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        return catalog.createTable(TableIdentifier.of("db1", name), schema, spec);
    }

    private static DataFile dataFile(PartitionSpec spec, String path, long sizeBytes, List<Long> splitOffsets,
            String partitionPath) {
        DataFiles.Builder builder = DataFiles.builder(spec)
                .withPath(path)
                .withFileSizeInBytes(sizeBytes)
                .withRecordCount(Math.max(1, sizeBytes / 100))
                .withFormat(FileFormat.PARQUET);
        if (splitOffsets != null) {
            builder.withSplitOffsets(splitOffsets);
        }
        if (partitionPath != null) {
            builder.withPartitionPath(partitionPath);
        }
        return builder.build();
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
}
