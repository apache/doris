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
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.NamedColumnHandle;
import org.apache.doris.connector.spi.mvcc.ConnectorTimeTravelSpec;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.scan.ConnectorScanContext;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.thrift.TDeltaLakeSysTableBatch;
import org.apache.doris.thrift.TDeltaLakeSysTableColumn;
import org.apache.doris.thrift.TDeltaLakeSysTableField;
import org.apache.doris.thrift.TDeltaLakeSysTableRow;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;

class DeltaLakeSystemTablesTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Map<String, Object> PROTOCOL = Map.of("minReaderVersion", 1, "minWriterVersion", 2);
    private static final StructType SCHEMA = new StructType().add("id", LongType.LONG).add("p", StringType.STRING);

    @TempDir
    Path warehouse;

    private Path commit(long version, Map<String, String> properties, List<String> partitions) throws IOException {
        return DeltaLakeTestTables.commit(warehouse.resolve("db/table"), version, "table-id", SCHEMA,
                partitions, PROTOCOL, properties, null, null);
    }

    private static String add(String file, String partition, int size, String stats) throws IOException {
        Map<String, Object> values = new HashMap<>();
        values.put("path", file);
        values.put("partitionValues", Collections.singletonMap("p", partition));
        values.put("size", size);
        values.put("modificationTime", 1234L);
        values.put("dataChange", true);
        if (stats != null) {
            values.put("stats", stats);
        }
        return JSON.writeValueAsString(Map.of("add", values)) + "\n";
    }

    private DeltaLakeRuntime runtime() {
        return new DeltaLakeRuntime(DeltaLakeCatalogProperties.of(Map.of("warehouse", warehouse.toString())),
                new RecordingConnectorContext());
    }

    private DeltaLakeTableHandle handle(DeltaLakeRuntime runtime, Probe probe, DeltaLakeTableKind kind) {
        Snapshot snapshot = TableManager.loadSnapshot(warehouse.resolve("db/table").toString()).build(runtime.engine);
        return new DeltaLakeTableHandle("db", "table", probe.observe(snapshot)).asSystemTable(kind);
    }

    private static ConnectorScanRequest.Builder request(DeltaLakeTableHandle handle, String... names) {
        return ConnectorScanRequest.builder(handle,
                Arrays.stream(names).<ConnectorColumnHandle>map(NamedColumnHandle::new).toList());
    }

    private static TDeltaLakeSysTableBatch batch(ConnectorScanRange range) {
        TTableFormatFileDesc desc = new TTableFormatFileDesc();
        range.populateRangeParams(desc, new TFileRangeDesc());
        return desc.getDeltalakeParams().getSysTableBatch();
    }

    private static List<TDeltaLakeSysTableRow> scan(DeltaLakeRuntime runtime, DeltaLakeTableHandle handle,
            String... names) {
        return handle.kind().systemTable.planScan(null, runtime, handle, request(handle, names).build()).stream()
                .map(DeltaLakeSystemTablesTest::batch).flatMap(b -> b.getRows().stream()).toList();
    }

    @Test
    void detailAndPropertiesReadOnlyTheRequestedMetadata() throws Exception {
        Path log = commit(0, Map.of("z", "last", "a", ""), List.of("p"));
        Files.writeString(log, add("a.parquet", "x", 11, null) + add("b.parquet", "y", 13, null),
                StandardOpenOption.APPEND);
        try (DeltaLakeRuntime runtime = runtime()) {
            Probe probe = new Probe();
            DeltaLakeTableHandle detail = handle(runtime, probe, DeltaLakeTableKind.DETAIL);
            Assertions.assertTrue(DeltaKernelCompat.fileTotals(detail.snapshot(runtime)).isEmpty());
            var metadata = scan(runtime, detail, "format", "id", "name", "description", "createdAt",
                    "partitionColumns", "clusteringColumns", "minReaderVersion", "minWriterVersion", "tableFeatures")
                    .get(0).getCells();
            Assertions.assertEquals("delta", metadata.get(0).getStringVal());
            Assertions.assertEquals("table-id", metadata.get(1).getStringVal());
            Assertions.assertEquals(new TDeltaLakeSysTableField(), metadata.get(2));
            Assertions.assertEquals(new TDeltaLakeSysTableField(), metadata.get(3));
            Assertions.assertEquals(0, metadata.get(4).getTimestampMillis());
            Assertions.assertEquals("p", metadata.get(5).getArrayVal().get(0).getStringVal());
            Assertions.assertEquals(List.of(), metadata.get(6).getArrayVal());
            Assertions.assertEquals(1, metadata.get(7).getInt64Val());
            Assertions.assertEquals(2, metadata.get(8).getInt64Val());
            Assertions.assertEquals(List.of(), metadata.get(9).getArrayVal());
            var props = scan(runtime, detail.asSystemTable(DeltaLakeTableKind.PROPERTIES), "key", "value");
            Assertions.assertEquals(List.of("a", "z"),
                    props.stream().map(row -> row.getCells().get(0).getStringVal()).toList());
            Assertions.assertEquals("", props.get(0).getCells().get(1).getStringVal());
            Assertions.assertEquals(List.of(), probe.includeStats);
            var totals = scan(runtime, detail, "numFiles", "sizeInBytes").get(0).getCells();
            Assertions.assertEquals(List.of(2L, 24L),
                    totals.stream().map(TDeltaLakeSysTableField::getInt64Val).toList());
            Assertions.assertEquals(List.of(false), probe.includeStats);
            Assertions.assertEquals(1, probe.closed);
        }
    }

    @Test
    void checksumServesBothFileTotalsWithoutReplay() throws Exception {
        Path log = commit(0, Map.of(), List.of("p"));
        Files.writeString(log, add("first.parquet", "x", 200, null) + add("second.parquet", "x", 220, null),
                StandardOpenOption.APPEND);
        DeltaLakeTestTables.checksum(warehouse.resolve("db/table"), 0, 2, 420);
        try (DeltaLakeRuntime runtime = runtime()) {
            Probe probe = new Probe();
            DeltaLakeTableHandle detail = handle(runtime, probe, DeltaLakeTableKind.DETAIL);
            var totals = scan(runtime, detail, "sizeInBytes", "numFiles").get(0).getCells();
            Assertions.assertEquals(2, totals.get(0).getInt64Val());
            Assertions.assertEquals(420, totals.get(1).getInt64Val());
            Assertions.assertTrue(probe.includeStats.isEmpty());
            Assertions.assertTrue(scan(runtime, detail.asSystemTable(DeltaLakeTableKind.PROPERTIES), "key").isEmpty());
        }
    }

    @Test
    void filesAndPartitionsOpenStatsOnlyForRecordCounts() throws Exception {
        Path log = commit(0, Map.of(), List.of("p"));
        Files.writeString(log, add("a.parquet", "x", 11, "{\"numRecords\":2}")
                + add("b.parquet", "x", 13, null) + add("c.parquet", null, 5, "{\"numRecords\":4}"),
                StandardOpenOption.APPEND);
        try (DeltaLakeRuntime runtime = runtime()) {
            Probe probe = new Probe();
            DeltaLakeTableHandle files = handle(runtime, probe, DeltaLakeTableKind.FILES);
            var select = request(files, "PATH", "path").filter(Optional.of(new ConnectorComparison(
                    ConnectorComparison.Operator.GT, new ConnectorColumnRef("size", ConnectorType.of("BIGINT")),
                    ConnectorLiteral.ofLong(1)))).build();
            var one = batch(files.kind().systemTable.planScan(null, runtime, files, select).get(0));
            Assertions.assertEquals(List.of("path", "size"),
                    one.getColumns().stream().map(TDeltaLakeSysTableColumn::getName).toList());
            Assertions.assertEquals(3, one.getRowsSize());
            var rows = scan(runtime, files, "num_records", "has_deletion_vector", "deletion_vector_cardinality",
                    "deletion_vector_path", "deletion_vector_storage_type");
            Assertions.assertEquals(1, rows.stream().filter(row -> !row.getCells().get(0).isSetInt64Val()).count());
            for (var row : rows) {
                Assertions.assertFalse(row.getCells().get(1).isBoolVal());
                Assertions.assertEquals(0, row.getCells().get(2).getInt64Val());
                Assertions.assertEquals(new TDeltaLakeSysTableField(), row.getCells().get(3));
                Assertions.assertEquals(new TDeltaLakeSysTableField(), row.getCells().get(4));
            }
            DeltaLakeTableHandle parts = files.asSystemTable(DeltaLakeTableKind.PARTITIONS);
            Assertions.assertEquals(2, scan(runtime, parts, "file_count").size());
            var partitions = scan(runtime, parts, "partition", "file_count", "total_size", "record_count",
                    "deleted_record_count");
            for (var row : partitions) {
                var fields = row.getCells();
                var key = fields.get(0).getMapVal().get(0);
                Assertions.assertEquals("p", key.getKey());
                if (key.getValue().isSetStringVal()) {
                    Assertions.assertEquals("x", key.getValue().getStringVal());
                    Assertions.assertEquals(2, fields.get(1).getInt64Val());
                    Assertions.assertEquals(24, fields.get(2).getInt64Val());
                    Assertions.assertEquals(new TDeltaLakeSysTableField(), fields.get(3));
                } else {
                    Assertions.assertEquals(1, fields.get(1).getInt64Val());
                    Assertions.assertEquals(4, fields.get(3).getInt64Val());
                }
                Assertions.assertEquals(0, fields.get(4).getInt64Val());
            }
            Assertions.assertEquals(List.of(false, true, false, true), probe.includeStats);
            Assertions.assertEquals(4, probe.closed);
        }
    }

    @Test
    void exposesInlineAndAbsoluteDeletionVectorMetadata() throws Exception {
        Map<String, Object> protocol = Map.of("minReaderVersion", 3, "minWriterVersion", 7,
                "readerFeatures", List.of("deletionVectors"), "writerFeatures", List.of("deletionVectors"));
        Path log = DeltaLakeTestTables.commit(warehouse.resolve("db/table"), 0, "table-id", SCHEMA,
                List.of("p"), protocol);
        String absolute = warehouse.resolve("vector.bin").toUri().toString();
        // The inline descriptor is the Base85 encoding of a one-row Spark deletion vector.
        for (String type : List.of("i", "p")) {
            var action = JSON.readTree(add(type + ".parquet", "x", 10, null));
            var file = (ObjectNode) action.get("add");
            var dv = file.putObject("deletionVector");
            dv.put("storageType", type);
            dv.put("pathOrInlineDv", type.equals("i") ? "^Bg9^0rr910000000000iXQKl0rr91000005c8Xg00000" : absolute);
            dv.put("sizeInBytes", 34);
            dv.put("cardinality", 1);
            if (type.equals("p")) {
                dv.put("offset", 1);
            }
            Files.writeString(log, JSON.writeValueAsString(action) + "\n", StandardOpenOption.APPEND);
        }
        try (DeltaLakeRuntime runtime = runtime()) {
            DeltaLakeTableHandle handle = handle(runtime, new Probe(), DeltaLakeTableKind.FILES);
            var rows = scan(runtime, handle, "has_deletion_vector", "deletion_vector_cardinality",
                    "deletion_vector_path", "deletion_vector_storage_type");
            Assertions.assertEquals(2, rows.size());
            for (var row : rows) {
                var cells = row.getCells();
                Assertions.assertTrue(cells.get(0).isBoolVal());
                Assertions.assertEquals(1, cells.get(1).getInt64Val());
                if ("i".equals(cells.get(3).getStringVal())) {
                    Assertions.assertFalse(cells.get(2).isSetStringVal());
                } else {
                    Assertions.assertEquals("p", cells.get(3).getStringVal());
                    Assertions.assertEquals(Path.of(URI.create(absolute)),
                            Path.of(URI.create(cells.get(2).getStringVal())));
                }
            }
        }
    }

    @Test
    void historicalViewsRetainTheirOwnKindInTheSameStatement() throws Exception {
        long time = System.currentTimeMillis() - 30000;
        Path first = commit(0, Map.of("stage", "one"), List.of());
        Path second = commit(1, Map.of("stage", "two"), List.of());
        Files.setLastModifiedTime(first, FileTime.fromMillis(time));
        Files.setLastModifiedTime(second, FileTime.fromMillis(time - 500));
        try (Connector connector = new DeltaLakeConnectorProvider().create(Map.of("warehouse", warehouse.toString()),
                new RecordingConnectorContext()); ConnectorMetadata meta = connector.getMetadata(null)) {
            var session = new TestingConnectorSession("system-views");
            var base = meta.getTableHandle(session, "db", "table").orElseThrow();
            var basePin = meta.resolveTimeTravel(session, base, ConnectorTimeTravelSpec.snapshotId("0")).orElseThrow();
            Assertions.assertEquals(DeltaLakeTableKind.DATA,
                    ((DeltaLakeTableHandle) meta.applySnapshot(session, base, basePin)).kind());
            for (DeltaLakeTableKind k : DeltaLakeTableKind.values()) {
                if (k.systemTable == null) {
                    continue;
                }
                var view = meta.getSysTableHandle(session, base, k.systemTable.name()).orElseThrow();
                var provider = connector.getScanPlanProvider(view);
                Assertions.assertEquals(k != DeltaLakeTableKind.HISTORY, provider.supportsSystemTableTimeTravel());
                Assertions.assertFalse(provider.canServeMetadataOnlyCount(session, view, Optional.empty()));
                var old = (DeltaLakeTableHandle) meta.applySnapshot(session, view, basePin);
                Assertions.assertEquals(k, old.kind());
                Assertions.assertEquals(0, old.getVersion());
                Assertions.assertSame(old, meta.applySnapshot(session, view, basePin));
                if (k == DeltaLakeTableKind.DETAIL) {
                    var current = (DeltaLakeTableHandle) view;
                    Assertions.assertEquals(time + 1, batch(provider.planScan(session, request(current, "lastModified")
                            .build()).get(0)).getRows().get(0).getCells().get(0).getTimestampMillis());
                } else if (k == DeltaLakeTableKind.PROPERTIES) {
                    Assertions.assertEquals("one", batch(provider.planScan(session, request(old, "value").build())
                            .get(0)).getRows().get(0).getCells().get(0).getStringVal());
                } else if (k == DeltaLakeTableKind.PARTITIONS) {
                    Assertions.assertTrue(provider.planScan(session, request(old, "partition").build()).isEmpty());
                }
            }
        }
    }

    @Test
    void explicitViewReadsRespectTheLatestRetentionPolicy() throws Exception {
        long oldTime = System.currentTimeMillis() - 60000;
        Files.setLastModifiedTime(commit(0, Map.of(), List.of()), FileTime.fromMillis(oldTime));
        commit(1, Map.of("delta.deletedFileRetentionDuration", "interval 2 seconds"), List.of());
        try (Connector connector = new DeltaLakeConnectorProvider().create(Map.of("warehouse", warehouse.toString()),
                new RecordingConnectorContext()); ConnectorMetadata meta = connector.getMetadata(null)) {
            var session = new TestingConnectorSession("retention");
            var base = meta.getTableHandle(session, "db", "table").orElseThrow();
            for (DeltaLakeTableKind kind : DeltaLakeTableKind.values()) {
                if (kind.systemTable == null || !kind.systemTable.supportsTimeTravel()) {
                    continue;
                }
                var view = meta.getSysTableHandle(session, base, kind.systemTable.name()).orElseThrow();
                for (var spec : List.of(ConnectorTimeTravelSpec.snapshotId("0"),
                        ConnectorTimeTravelSpec.timestamp(Long.toString(oldTime), true))) {
                    var error = Assertions.assertThrows(DorisConnectorException.class,
                            () -> meta.resolveTimeTravel(session, view, spec));
                    Assertions.assertTrue(error.getMessage().contains("delta.deletedFileRetentionDuration"));
                }
            }
        }
    }

    @Test
    void streamingStopsAndClosesOnLimitCancellationAndFailure() throws Exception {
        Path log = commit(0, Map.of(), List.of("p"));
        StringBuilder adds = new StringBuilder();
        for (int i = 0; i < 2100; i++) {
            adds.append(add(i + ".parquet", "x", 10, null));
        }
        Files.writeString(log, adds, StandardOpenOption.APPEND);
        try (DeltaLakeRuntime runtime = runtime()) {
            Probe probe = new Probe();
            DeltaLakeTableHandle files = handle(runtime, probe, DeltaLakeTableKind.FILES);
            DeltaLakeSystemTable view = files.kind().systemTable;
            Assertions.assertTrue(view.streamingSplitEstimate(null) >= 0);
            Assertions.assertEquals(-1, view.streamingSplitEstimate(new TestingConnectorSession("sync",
                    Map.of("enable_external_table_batch_mode", "false"))));
            ConnectorScanContext context = new ConnectorScanContext();
            var request = request(files, "path").scanContext(context).build();
            try (var source = view.streamSplits(null, runtime, files, request, true)) {
                Assertions.assertTrue(probe.includeStats.isEmpty());
                var batch = batch(source.next());
                Assertions.assertTrue(batch.getColumns().isEmpty());
                Assertions.assertEquals(1000, batch.getRowsSize());
                Assertions.assertTrue(batch.getRows().stream().allMatch(row -> row.getCells().isEmpty()));
                Assertions.assertTrue(context.describeProgress().contains("files_enumerated=1000"));
            }
            Assertions.assertEquals(1, probe.closed);
            try (var source = view.streamSplits(null, runtime, files, request, false)) {
                source.next();
                context.cancel();
                Assertions.assertThrows(CancellationException.class, source::hasNext);
            }
            Assertions.assertEquals(2, probe.closed);
            probe.fail = true;
            try (var source = view.streamSplits(null, runtime, files, request(files, "path").build(), false)) {
                Assertions.assertThrows(IllegalStateException.class, source::hasNext);
            }
            Assertions.assertEquals(3, probe.closed);
        }
    }

    @Test
    void byteBoundRetainsOneOversizedRowAndContinues() {
        var col = new TDeltaLakeSysTableColumn().setName("path").setSchemaOrdinal(0);
        String key = "文".repeat(100000);
        List<TDeltaLakeSysTableRow> rows = List.of(key, key, key + key, "tail").stream()
                .map(s -> new TDeltaLakeSysTableRow().setCells(List.of(DeltaLakeSystemTableRows.text(s)))).toList();
        var ranges = DeltaLakeSystemTableRows.pack("files", List.of(col), rows.iterator(), new ConnectorScanContext());
        Assertions.assertEquals(List.of(1, 1, 1, 1), ranges.stream().map(r -> batch(r).getRowsSize()).toList());
        Assertions.assertEquals(rows, ranges.stream().flatMap(r -> batch(r).getRows().stream()).toList());
    }

    /** Observe Kernel's actual getScanFiles(includeStats) call and iterator lifetime, leaving replay to Kernel. */
    private static final class Probe {
        final List<Boolean> includeStats = new ArrayList<>();
        int closed;
        boolean fail;

        Snapshot observe(Snapshot snapshot) {
            return observe(Snapshot.class, snapshot, (method, args, result) -> method.equals("getScanBuilder")
                    ? observe(ScanBuilder.class, (ScanBuilder) result, (builderMethod, unused, built) ->
                            builderMethod.equals("build") ? observe(Scan.class, (Scan) built, this::scanCall) : built)
                    : result);
        }

        @SuppressWarnings("unchecked")
        private Object scanCall(String method, Object[] args, Object result) {
            if (!method.equals("getScanFiles")) {
                return result;
            }
            includeStats.add((Boolean) args[1]);
            var delegate = (CloseableIterator<FilteredColumnarBatch>) result;
            return new CloseableIterator<FilteredColumnarBatch>() {
                @Override
                public boolean hasNext() {
                    if (fail) {
                        throw new IllegalStateException("test scan failure");
                    }
                    return delegate.hasNext();
                }

                @Override
                public FilteredColumnarBatch next() {
                    return delegate.next();
                }

                @Override
                public void close() throws IOException {
                    closed++;
                    delegate.close();
                }
            };
        }

        private interface Observer {
            Object after(String method, Object[] args, Object result);
        }

        private static <T> T observe(Class<T> api, T target, Observer observer) {
            InvocationHandler handler = (proxy, method, args) -> {
                try {
                    return observer.after(method.getName(), args, method.invoke(target, args));
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            };
            return api.cast(Proxy.newProxyInstance(api.getClassLoader(), new Class<?>[] {api}, handler));
        }
    }
}
