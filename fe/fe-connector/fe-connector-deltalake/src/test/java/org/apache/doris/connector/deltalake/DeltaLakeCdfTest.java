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

package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.mvcc.ConnectorTimeTravelSpec;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.thrift.TDeltaLakeCdfRowFilterMode;
import org.apache.doris.thrift.TDeltaLakeFileDesc;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class DeltaLakeCdfTest {
    @TempDir
    Path warehouse;
    private static final StructType BASE = new StructType().add("id", IntegerType.INTEGER);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Map<String, Object> PROTOCOL = Map.of("minReaderVersion", 1, "minWriterVersion", 4);
    private Connector connector;
    private ConnectorMetadata metadata;
    private Path table;

    @BeforeEach
    void createTable() throws Exception {
        table = warehouse.resolve("db/events");
        definition(0, BASE, true);
        connector = new DeltaLakeConnectorProvider().create(Map.of("warehouse", warehouse.toString(),
                "enable.mapping.varbinary", "true"),
                new RecordingConnectorContext());
        metadata = connector.getMetadata(null);
    }

    @AfterEach
    void close() throws Exception {
        metadata.close();
        connector.close();
    }

    private void definition(long version, StructType schema, boolean enabled) throws Exception {
        DeltaLakeTestTables.commit(table, version, "cdf-table", schema, List.of(), PROTOCOL,
                Map.of("delta.enableChangeDataFeed", Boolean.toString(enabled)), null, 1000L + version);
    }

    private void commit(long version, String... actions) throws Exception {
        Path log = table.resolve(String.format("_delta_log/%020d.json", version));
        Files.writeString(log, String.join("\n", actions) + "\n");
        Files.setLastModifiedTime(log, FileTime.fromMillis(1000));
    }

    private static String file(String kind, String path, boolean changes, int cardinality) throws Exception {
        Map<String, Object> action = new LinkedHashMap<>();
        action.put("path", path);
        action.put("dataChange", changes);
        action.put("partitionValues", Map.of());
        action.put("size", 50);
        action.put("modificationTime", 1000);
        if (cardinality > 0) {
            action.put("deletionVector", Map.of("storageType", "p", "pathOrInlineDv", "file:///dv-" + cardinality,
                    "sizeInBytes", 20, "cardinality", cardinality));
        }
        return JSON.writeValueAsString(Map.of(kind, action));
    }

    private ConnectorTableHandle base() {
        return metadata.getTableHandle(null, "db", "events").orElseThrow();
    }

    private ConnectorMvccSnapshot pin(ConnectorTableHandle base, Map<String, String> args) {
        return metadata.resolveTimeTravel(null, base, ConnectorTimeTravelSpec.incremental(args)).orElseThrow();
    }

    private ConnectorTableHandle range(long start, long end) {
        ConnectorTableHandle base = base();
        ConnectorTableHandle changes = metadata.getSysTableHandle(null, base, "changes").orElseThrow();
        ConnectorMvccSnapshot pin = pin(base, Map.of("start_version", String.valueOf(start),
                "end_version", String.valueOf(end)));
        return metadata.applySnapshot(null, changes, pin);
    }

    private List<TDeltaLakeFileDesc> scan(ConnectorTableHandle handle, ConnectorExpression filter) {
        ConnectorScanRequest.Builder request = ConnectorScanRequest.builder(handle, List.of());
        if (filter != null) {
            request.filter(Optional.of(filter));
        }
        return connector.getScanPlanProvider(handle).planScan(null, request.build()).stream().map(range -> {
            Assertions.assertEquals("deltalake_cdf", range.getTableFormatType());
            return descriptor(range);
        }).toList();
    }

    private static TDeltaLakeFileDesc descriptor(ConnectorScanRange range) {
        TTableFormatFileDesc desc = new TTableFormatFileDesc();
        range.populateRangeParams(desc, new TFileRangeDesc());
        return desc.getDeltalakeParams();
    }

    @Test
    void prioritizesCdcPairsBothVectorsAndPreservesRemovedVector() throws Exception {
        commit(1, file("add", "part%20a.parquet", true, 1));
        commit(2, file("remove", "part%20a.parquet", true, 1), file("add", "part%20a.parquet", true, 2));
        commit(3, file("remove", "part%20a.parquet", true, 2), file("add", "part%20a.parquet", true, 2));
        commit(4, file("remove", "part%20a.parquet", true, 2), file("add", "new.parquet", true, 0),
                file("cdc", "physical.parquet", false, 0));
        commit(5, file("add", "optimized.parquet", false, 0), file("remove", "new.parquet", false, 0));
        commit(6, file("remove", "new.parquet", true, 2));
        List<TDeltaLakeFileDesc> result = scan(range(1, 6), null);
        Assertions.assertEquals(List.of(1L, 2L, 2L, 4L, 6L),
                result.stream().map(x -> x.getCdf().getCommitVersion()).toList());
        Assertions.assertEquals(List.of(1001L, 1002L, 1002L, 1004L, 1006L),
                result.stream().map(x -> x.getCdf().getCommitTimestampMillis()).toList());
        Assertions.assertEquals(2, result.get(1).getDeletionVector().getCardinality());
        Assertions.assertEquals(1, result.get(1).getCdf().getBaseDeletionVector().getCardinality());
        Assertions.assertEquals("delete", result.get(1).getCdf().getConstantChangeType());
        Assertions.assertEquals(TDeltaLakeCdfRowFilterMode.KEEP_DELETION_VECTOR_DIFF,
                result.get(2).getCdf().getRowFilterMode());
        Assertions.assertEquals("insert", result.get(2).getCdf().getConstantChangeType());
        Assertions.assertEquals(2, result.get(2).getCdf().getBaseDeletionVector().getCardinality());
        Assertions.assertFalse(result.get(3).getCdf().isSetConstantChangeType());
        Assertions.assertEquals(2, result.get(4).getDeletionVector().getCardinality());
        ConnectorComparison preimage = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("_change_type", ConnectorType.of("STRING")),
                ConnectorLiteral.ofString("update_preimage"));
        Assertions.assertEquals(1, scan(range(1, 6), preimage).size());
    }

    @ParameterizedTest(name = "paired remove has partitionValues={0}, size={1}")
    @CsvSource({"false, false", "true, false", "false, true"})
    void restoresRowsFromANonExtendedRemoveUsingPairedAddMetadata(boolean hasPartitions, boolean hasSize)
            throws Exception {
        Map<String, Object> protocol = Map.of("minReaderVersion", 3, "minWriterVersion", 7,
                "readerFeatures", List.of("deletionVectors"),
                "writerFeatures", List.of("deletionVectors", "changeDataFeed"));
        DeltaLakeTestTables.commit(table, 0, "cdf-table", BASE.add("p", StringType.STRING), List.of("p"), protocol,
                Map.of("delta.enableChangeDataFeed", "true", "delta.enableDeletionVectors", "true"), null, 1000L);
        String path = "p=west/restored.parquet";
        Map<String, String> partitions = Map.of("p", "west");
        Map<String, Object> add = Map.of("path", path, "dataChange", true, "size", 73,
                "partitionValues", partitions, "modificationTime", 1000);
        Map<String, Object> dv = Map.of("storageType", "p", "pathOrInlineDv", "file:///old-dv",
                "sizeInBytes", 20, "cardinality", 2);
        Map<String, Object> previous = new LinkedHashMap<>(add);
        previous.put("deletionVector", dv);
        commit(1, JSON.writeValueAsString(Map.of("add", previous)));
        Map<String, Object> remove = new LinkedHashMap<>(Map.of("path", path, "dataChange", true,
                "extendedFileMetadata", false, "deletionVector", dv));
        if (hasPartitions) {
            remove.put("partitionValues", partitions);
        }
        if (hasSize) {
            remove.put("size", add.get("size"));
        }
        commit(2, JSON.writeValueAsString(Map.of("remove", remove)), JSON.writeValueAsString(Map.of("add", add)));

        // No data file is created, so an unnecessary file-status lookup would fail.
        ConnectorTableHandle handle = range(2, 2);
        var ranges = connector.getScanPlanProvider(handle).planScan(null,
                ConnectorScanRequest.builder(handle, List.of()).build());
        Assertions.assertEquals(1, ranges.size());
        ConnectorScanRange restored = ranges.get(0);
        Assertions.assertEquals(partitions, restored.getPartitionValues());
        Assertions.assertEquals(73, restored.getFileSize());
        TDeltaLakeFileDesc desc = descriptor(restored);
        Assertions.assertEquals("insert", desc.getCdf().getConstantChangeType());
        Assertions.assertEquals(TDeltaLakeCdfRowFilterMode.KEEP_DELETION_VECTOR_DIFF, desc.getCdf().getRowFilterMode());
        Assertions.assertEquals(2, desc.getDeletionVector().getCardinality());
        Assertions.assertFalse(desc.getCdf().isSetBaseDeletionVector());
    }

    @Test
    void decodesPathsAndBinaryPartitionsAndResolvesMissingRemoveSize() throws Exception {
        StructType schema = BASE.add("p", BinaryType.BINARY);
        DeltaLakeTestTables.commit(table, 0, "cdf-table", schema, List.of("p"), PROTOCOL,
                Map.of("delta.enableChangeDataFeed", "true"), null, 1000L);
        Path data = table.resolve("a b.parquet");
        Files.write(data, new byte[23]);
        Map<String, Object> remove = new LinkedHashMap<>();
        remove.put("path", "a%20b.parquet");
        remove.put("partitionValues", Map.of("p", "A/B"));
        remove.put("dataChange", true);
        commit(1, JSON.writeValueAsString(Map.of("remove", remove)));
        ConnectorTableHandle handle = range(1, 1);
        var ranges = connector.getScanPlanProvider(handle).planScan(null,
                ConnectorScanRequest.builder(handle, List.of()).build());
        Assertions.assertEquals(1, ranges.size());
        Assertions.assertEquals(23, ranges.get(0).getFileSize());
        Assertions.assertTrue(ranges.get(0).getPath().orElseThrow().endsWith("a b.parquet"));
        TFileRangeDesc wire = new TFileRangeDesc();
        ranges.get(0).populateRangeParams(new TTableFormatFileDesc(), wire);
        Assertions.assertEquals(List.of("0x412F42"), wire.getColumnsFromPath());
        remove.remove("partitionValues");
        commit(2, JSON.writeValueAsString(Map.of("remove", remove)));
        Assertions.assertTrue(Assertions.assertThrows(RuntimeException.class, () -> scan(range(2, 2), null))
                .getMessage().contains("Missing CDF partition value"));
    }

    @Test
    void resolvesMixedTimestampBoundsAcrossIctEnablementAndReturnsEmptyGaps() throws Exception {
        commit(1, file("add", "before.parquet", true, 0));
        long ict = 1_700_000_000_000L;
        Map<String, Object> protocol = Map.of("minReaderVersion", 1, "minWriterVersion", 7,
                "writerFeatures", List.of("inCommitTimestamp", "changeDataFeed"));
        Map<String, String> config = Map.of("delta.enableChangeDataFeed", "true",
                "delta.enableInCommitTimestamps", "true", "delta.inCommitTimestampEnablementVersion", "2",
                "delta.inCommitTimestampEnablementTimestamp", Long.toString(ict));
        DeltaLakeTestTables.commit(table, 2, "cdf-table", BASE, List.of(), protocol, config, ict, 1000L);
        commit(3, "{\"commitInfo\":{\"inCommitTimestamp\":" + (ict + 200) + "}}",
                file("add", "after.parquet", true, 0));
        ConnectorTableHandle base = base();
        var both = pin(base, Map.of("start_version", "1", "end_timestamp", Instant.ofEpochMilli(ict).toString()));
        Assertions.assertEquals(new DeltaLakeCdfRange(1, 2, 2), DeltaLakeCdfRange.fromPin(both));
        String gap = Instant.ofEpochMilli(ict + 10).toString();
        var empty = pin(base, Map.of("start_timestamp", gap, "end_timestamp", gap));
        Assertions.assertEquals(new DeltaLakeCdfRange(3, 2, 2), DeltaLakeCdfRange.fromPin(empty));
        Assertions.assertEquals(3, pin(base, Map.of("start_timestamp", gap, "end_version", "3")).getSnapshotId());
        Assertions.assertThrows(RuntimeException.class, () -> pin(base, Map.of("start_version", "0",
                "end_timestamp", Instant.ofEpochMilli(999).toString())));
        Assertions.assertThrows(RuntimeException.class, () -> pin(base, Map.of("start_timestamp",
                Instant.ofEpochMilli(ict + 201).toString())));
    }

    @Test
    void emptyTimestampRangesValidateTheEndSnapshot() throws Exception {
        Map<String, String> enabled = Map.of("delta.enableChangeDataFeed", "true");
        Map<String, Object> geospatial = Map.of("minReaderVersion", 3, "minWriterVersion", 7,
                "readerFeatures", List.of("geospatial"), "writerFeatures", List.of("geospatial", "changeDataFeed"));
        DeltaLakeTestTables.commit(table, 1, "cdf-table", BASE, List.of(), PROTOCOL,
                Map.of("delta.enableChangeDataFeed", "false"), null, 3000L);
        DeltaLakeTestTables.commit(table, 2, "cdf-table", BASE, List.of(), geospatial, enabled, null, 6000L);
        DeltaLakeTestTables.commit(table, 3, "cdf-table", BASE, List.of(), PROTOCOL, enabled, null, 9000L);
        ConnectorTableHandle base = base();

        Assertions.assertEquals(new DeltaLakeCdfRange(1, 0, 0),
                DeltaLakeCdfRange.fromPin(pin(base, timestampBounds(1500, 2500))));
        var unsupported = Assertions.assertThrows(DorisConnectorException.class,
                () -> pin(base, timestampBounds(7000, 8000)));
        Assertions.assertTrue(unsupported.getMessage().contains("Unsupported Delta Lake reader features [geospatial]"),
                unsupported.getMessage());
        var disabled = Assertions.assertThrows(DorisConnectorException.class,
                () -> pin(base, timestampBounds(4000, 5000)));
        Assertions.assertEquals("Change data feed was not enabled at version 1", disabled.getMessage());
    }

    private static Map<String, String> timestampBounds(long startMillis, long endMillis) {
        return Map.of("start_timestamp", Instant.ofEpochMilli(startMillis).toString(),
                "end_timestamp", Instant.ofEpochMilli(endMillis).toString());
    }

    @Test
    void pinsTheHistoricalSchemaAndRejectsDisabledMissingAndIncompatibleIntervals() throws Exception {
        commit(1, file("add", "old.parquet", true, 0));
        StructType added = BASE.add("label", StringType.STRING);
        definition(2, added, true);
        definition(3, added, false);
        definition(4, added, true);
        definition(5, new StructType().add("id", LongType.LONG).add("label", StringType.STRING), true);
        Assertions.assertEquals(List.of("id", "_change_type", "_commit_version", "_commit_timestamp"),
                List.copyOf(metadata.getColumnHandles(null, range(0, 1)).keySet()));
        Assertions.assertTrue(metadata.getColumnHandles(null, range(1, 2)).containsKey("label"));
        Assertions.assertThrows(RuntimeException.class, () -> range(2, 4));
        Assertions.assertThrows(RuntimeException.class, () -> range(4, 5));
        Assertions.assertEquals(5, ((DeltaLakeTableHandle) range(5, 99)).getVersion());
        Assertions.assertThrows(RuntimeException.class, () -> range(5, 2));
        Assertions.assertThrows(RuntimeException.class, () -> range(6, 8));
        Assertions.assertThrows(RuntimeException.class, () -> metadata.applySnapshot(null, base(),
                pin(base(), Map.of("start_version", "5"))));
        Files.delete(table.resolve("_delta_log/00000000000000000001.json"));
        Assertions.assertThrows(RuntimeException.class, () -> range(0, 2));
    }
}
