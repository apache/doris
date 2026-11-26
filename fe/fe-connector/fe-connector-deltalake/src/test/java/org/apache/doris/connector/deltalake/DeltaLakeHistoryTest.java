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
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.NamedColumnHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.thrift.TDeltaLakeSysTableBatch;
import org.apache.doris.thrift.TDeltaLakeSysTableColumn;
import org.apache.doris.thrift.TDeltaLakeSysTableField;
import org.apache.doris.thrift.TDeltaLakeSysTableRow;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class DeltaLakeHistoryTest {
    @TempDir
    Path warehouse;

    private static final long FIRST_MTIME = 1_700_000_000_000L;

    @Test
    void readsTypedActionsAndRetainsVersionsWithoutCommitInfo() throws Exception {
        Path table = warehouse.resolve("db/table");
        DeltaLakeTestTables.commit(table, 0, "table-history", new StructType().add("id", LongType.LONG));
        Files.writeString(log(0), """
                {"commitInfo":{"timestamp":1,"userId":"u1","userName":"reader","operation":"CREATE TABLE",
                "operationParameters":{"mode":"Append","missing":null},"job":{"jobId":"j1","runId":"r1"},
                "notebook":{"notebookId":"n1"},"clusterId":"c1","readVersion":0,"isolationLevel":"Serializable",
                "isBlindAppend":false,"operationMetrics":{"numFiles":"0"},"userMetadata":"meta","engineInfo":"spark"}}
                """.replace(",\n", ","), StandardOpenOption.APPEND);
        Files.writeString(log(1), "{\"txn\":{\"appId\":\"app\",\"version\":7}}\n");
        Files.setLastModifiedTime(log(0), FileTime.fromMillis(FIRST_MTIME));
        Files.setLastModifiedTime(log(1), FileTime.fromMillis(FIRST_MTIME - 500));

        try (Connector connector = connector(); ConnectorMetadata metadata = connector.getMetadata(null)) {
            ConnectorTableHandle base = metadata.getTableHandle(null, "db", "table").orElseThrow();
            Assertions.assertEquals(List.of("history"), metadata.listSupportedSysTables(null, base));
            Assertions.assertTrue(metadata.getSysTableHandle(null, base, "files").isEmpty());
            ConnectorTableHandle history = metadata.getSysTableHandle(null, base, "HISTORY").orElseThrow();
            Assertions.assertEquals(List.of("version", "timestamp", "userId", "userName", "operation",
                    "operationParameters", "job", "notebook", "clusterId", "readVersion", "isolationLevel",
                    "isBlindAppend", "operationMetrics", "userMetadata", "engineInfo"),
                    List.copyOf(metadata.getColumnHandles(null, history).keySet()));
            Assertions.assertTrue(metadata.listPartitions(null, history, Optional.empty()).isEmpty());
            Assertions.assertTrue(metadata.getTableStatistics(null, history).isEmpty());
            ConnectorScanPlanProvider provider = connector.getScanPlanProvider(history);
            Assertions.assertFalse(provider.supportsSystemTableTimeTravel());
            Assertions.assertEquals(-1, provider.streamingSplitEstimate(null, history, Optional.empty(), false));

            ConnectorScanRequest.Builder all = ConnectorScanRequest.builder(history,
                    List.copyOf(metadata.getColumnHandles(null, history).values()));
            Assertions.assertThrows(UnsupportedOperationException.class,
                    () -> provider.streamSplits(null, all.build()));
            List<TDeltaLakeSysTableRow> rows = rows(provider.planScan(null, all.build()));
            Assertions.assertEquals(List.of(1L, 0L),
                    rows.stream().map(row -> row.getCells().get(0).getInt64Val()).toList());
            TDeltaLakeSysTableRow absent = rows.get(0);
            Assertions.assertEquals(FIRST_MTIME + 1, absent.getCells().get(1).getTimestampMillis());
            Assertions.assertTrue(absent.getCells().subList(2, 15).stream()
                    .allMatch(new TDeltaLakeSysTableField()::equals));
            List<TDeltaLakeSysTableField> cells = rows.get(1).getCells();
            Assertions.assertEquals(15, cells.size());
            Assertions.assertEquals(FIRST_MTIME, cells.get(1).getTimestampMillis());
            Assertions.assertEquals("u1", cells.get(2).getStringVal());
            Assertions.assertEquals("reader", cells.get(3).getStringVal());
            Assertions.assertEquals("CREATE TABLE", cells.get(4).getStringVal());
            Assertions.assertEquals("j1", cells.get(6).getStructVal().get(0).getStringVal());
            Assertions.assertEquals("r1", cells.get(6).getStructVal().get(3).getStringVal());
            Assertions.assertEquals(new TDeltaLakeSysTableField(), cells.get(6).getStructVal().get(4));
            Assertions.assertEquals("n1", cells.get(7).getStructVal().get(0).getStringVal());
            Assertions.assertEquals("c1", cells.get(8).getStringVal());
            Assertions.assertEquals(0, cells.get(9).getInt64Val());
            Assertions.assertEquals("Serializable", cells.get(10).getStringVal());
            Assertions.assertTrue(cells.get(11).isSetBoolVal());
            Assertions.assertFalse(cells.get(11).isBoolVal());
            Assertions.assertEquals("0", cells.get(12).getMapVal().get(0).getValue().getStringVal());
            Assertions.assertEquals("meta", cells.get(13).getStringVal());
            Assertions.assertEquals("spark", cells.get(14).getStringVal());
            Assertions.assertEquals("Append", cells.get(5).getMapVal().stream()
                    .filter(entry -> entry.getKey().equals("mode")).findFirst().orElseThrow()
                    .getValue().getStringVal());
            Assertions.assertEquals(new TDeltaLakeSysTableField(), cells.get(5).getMapVal().stream()
                    .filter(entry -> entry.getKey().equals("missing")).findFirst().orElseThrow().getValue());

            ConnectorScanRequest subset = ConnectorScanRequest.builder(history, List.of(
                    new NamedColumnHandle("OPERATION"), new NamedColumnHandle("operation")))
                    .filter(Optional.of(versionFilter(ConnectorComparison.Operator.EQ, 0))).build();
            TDeltaLakeSysTableBatch batch = batch(provider.planScan(null, subset).get(0));
            Assertions.assertEquals(List.of("version", "operation"),
                    batch.getColumns().stream().map(TDeltaLakeSysTableColumn::getName).toList());
            Assertions.assertEquals(List.of(0, 4),
                    batch.getColumns().stream().map(TDeltaLakeSysTableColumn::getSchemaOrdinal).toList());
            Assertions.assertEquals(List.of(new TDeltaLakeSysTableField().setInt64Val(0),
                    new TDeltaLakeSysTableField().setStringVal("CREATE TABLE")), batch.getRows().get(0).getCells());

            Assertions.assertEquals(List.of(absent), rows(provider.planScan(null, all.filter(
                    Optional.of(versionFilter(ConnectorComparison.Operator.GE, 1))).build())));
            Assertions.assertTrue(provider.planScan(null, all.filter(
                    Optional.of(versionFilter(ConnectorComparison.Operator.EQ, 9))).build()).isEmpty());

            // With the already pinned snapshot, a version-only request must not open the JSON.
            Files.writeString(log(1), "not a JSON commit body");
            List<TDeltaLakeSysTableRow> versions = rows(provider.planScan(null, ConnectorScanRequest.builder(history,
                    List.of(new NamedColumnHandle("version"))).build()));
            Assertions.assertEquals(List.of(1L, 0L),
                    versions.stream().map(row -> row.getCells().get(0).getInt64Val()).toList());
        }
    }

    @Test
    void readsIctOnlyFromItsEnablementVersion() throws Exception {
        Path table = warehouse.resolve("db/table");
        DeltaLakeTestTables.commit(table, 0, "table-history", new StructType().add("id", LongType.LONG));
        Files.writeString(log(0), "{\"commitInfo\":{\"inCommitTimestamp\":1}}\n", StandardOpenOption.APPEND);
        String metadata = Files.readAllLines(log(0)).get(1).replace("\"configuration\":{}", """
                "configuration":{"delta.enableInCommitTimestamps":"true",
                "delta.inCommitTimestampEnablementVersion":"1",
                "delta.inCommitTimestampEnablementTimestamp":"1700000001000"}
                """.replace("\n", ""));
        Files.writeString(log(1), "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":7,"
                + "\"writerFeatures\":[\"inCommitTimestamp\"]}}\n" + metadata + "\n"
                + "{\"commitInfo\":{\"timestamp\":2,\"inCommitTimestamp\":1700000001000}}\n");
        Files.setLastModifiedTime(log(0), FileTime.fromMillis(FIRST_MTIME));
        Files.setLastModifiedTime(log(1), FileTime.fromMillis(FIRST_MTIME + 70_000));

        try (Connector connector = connector(); ConnectorMetadata meta = connector.getMetadata(null)) {
            ConnectorTableHandle base = meta.getTableHandle(null, "db", "table").orElseThrow();
            ConnectorTableHandle history = meta.getSysTableHandle(null, base, "history").orElseThrow();
            List<TDeltaLakeSysTableRow> result = rows(connector.getScanPlanProvider().planScan(null,
                    ConnectorScanRequest.builder(history, List.of(new NamedColumnHandle("timestamp"))).build()));
            Assertions.assertEquals(List.of(FIRST_MTIME + 1000, FIRST_MTIME),
                    result.stream().map(row -> row.getCells().get(0).getTimestampMillis()).toList());
        }
    }

    @Test
    void startsAtTheEarliestRetainedCommit() throws Exception {
        Path source = Path.of(getClass().getResource("/deltalake/dv-partitioned-with-checkpoint/_delta_log").toURI());
        Files.createDirectories(log(10).getParent());
        try (var entries = Files.list(source)) {
            for (Path file : entries.toList()) {
                if (file.getFileName().toString().compareTo(log(10).getFileName().toString()) >= 0
                        || file.getFileName().toString().contains(".checkpoint.")) {
                    Files.copy(file, log(10).getParent().resolve(file.getFileName()));
                }
            }
        }
        try (Connector connector = connector(); ConnectorMetadata meta = connector.getMetadata(null)) {
            ConnectorTableHandle base = meta.getTableHandle(null, "db", "table").orElseThrow();
            ConnectorTableHandle history = meta.getSysTableHandle(null, base, "history").orElseThrow();
            List<TDeltaLakeSysTableRow> result = rows(connector.getScanPlanProvider().planScan(null,
                    ConnectorScanRequest.builder(history, List.of(new NamedColumnHandle("version"))).build()));
            Assertions.assertEquals(List.of(15L, 14L, 13L, 12L, 11L, 10L),
                    result.stream().map(row -> row.getCells().get(0).getInt64Val()).toList());
        }
    }

    @Test
    void batchesCountOnlyRowsWithoutCommitDetails() throws Exception {
        Path table = warehouse.resolve("db/table");
        DeltaLakeTestTables.commit(table, 0, "table-history", new StructType().add("id", LongType.LONG));
        for (int i = 1; i <= 1000; i++) {
            Files.writeString(log(i), "{}");
        }
        try (Connector connector = connector(); ConnectorMetadata meta = connector.getMetadata(null)) {
            ConnectorTableHandle base = meta.getTableHandle(null, "db", "table").orElseThrow();
            ConnectorTableHandle history = meta.getSysTableHandle(null, base, "history").orElseThrow();
            List<TDeltaLakeSysTableBatch> batches = connector.getScanPlanProvider(history).planScan(null,
                    ConnectorScanRequest.builder(history, List.of(new NamedColumnHandle("version")))
                            .countPushdown(true).build()).stream().map(DeltaLakeHistoryTest::batch).toList();
            Assertions.assertEquals(List.of(1000, 1),
                    batches.stream().map(TDeltaLakeSysTableBatch::getRowsSize).toList());
            for (TDeltaLakeSysTableBatch batch : batches) {
                Assertions.assertEquals(List.of(), batch.getColumns());
                for (TDeltaLakeSysTableRow row : batch.getRows()) {
                    Assertions.assertEquals(List.of(), row.getCells());
                }
            }
        }
    }

    private Connector connector() {
        return new DeltaLakeConnectorProvider().create(Map.of("warehouse", warehouse.toString()),
                new RecordingConnectorContext());
    }

    private Path log(long version) {
        return warehouse.resolve("db/table/_delta_log").resolve(String.format("%020d.json", version));
    }

    private static ConnectorComparison versionFilter(ConnectorComparison.Operator operator, long version) {
        return new ConnectorComparison(operator, new ConnectorColumnRef("version", ConnectorType.of("BIGINT")),
                ConnectorLiteral.ofLong(version));
    }

    private static List<TDeltaLakeSysTableRow> rows(List<ConnectorScanRange> ranges) {
        return ranges.stream().map(DeltaLakeHistoryTest::batch).flatMap(batch -> batch.getRows().stream()).toList();
    }

    private static TDeltaLakeSysTableBatch batch(ConnectorScanRange range) {
        TTableFormatFileDesc format = new TTableFormatFileDesc();
        range.populateRangeParams(format, new TFileRangeDesc());
        return format.getDeltalakeParams().getSysTableBatch();
    }
}
