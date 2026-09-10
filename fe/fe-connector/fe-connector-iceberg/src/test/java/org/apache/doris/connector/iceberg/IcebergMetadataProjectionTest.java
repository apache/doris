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

import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class IcebergMetadataProjectionTest {
    private static final String FILE_PATH = "s3://bucket/table/data.parquet";
    private static final List<Long> OFFSETS = List.of(0L, 512L);

    @ParameterizedTest
    @ValueSource(strings = {"files", "data_files", "all_files", "all_data_files"})
    void splitOffsetsRetainTheirInternalSizeDependency(String systemTable) throws Exception {
        try (Fixture fixture = new Fixture()) {
            FileScanTask task = fixture.plan(systemTable, List.of("split_offsets", "file_path"));
            Assertions.assertEquals(List.of("split_offsets", "file_path", "file_size_in_bytes"),
                    names(task.schema()));
            Assertions.assertNull(task.schema().findField("readable_metrics"));
            try (CloseableIterable<StructLike> rows = task.asDataTask().rows()) {
                List<StructLike> materialized = new ArrayList<>();
                rows.forEach(materialized::add);
                Assertions.assertEquals(1, materialized.size());
                Assertions.assertEquals(OFFSETS, materialized.get(0).get(0, Object.class));
                Assertions.assertEquals(FILE_PATH, materialized.get(0).get(1, Object.class).toString());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"files", "data_files", "all_files", "all_data_files"})
    void explicitlyRequestedSizeKeepsItsPositionWithoutDuplication(String systemTable) throws Exception {
        try (Fixture fixture = new Fixture()) {
            List<String> requested = List.of("file_size_in_bytes", "split_offsets", "file_path");
            FileScanTask task = fixture.plan(systemTable, requested);
            Assertions.assertEquals(requested, names(task.schema()));
            try (CloseableIterable<StructLike> rows = task.asDataTask().rows()) {
                StructLike row = rows.iterator().next();
                Assertions.assertEquals(1024L, row.get(0, Object.class));
                Assertions.assertEquals(OFFSETS, row.get(1, Object.class));
                Assertions.assertEquals(FILE_PATH, row.get(2, Object.class).toString());
            }
        }
    }

    @Test
    void unrelatedProjectionDoesNotAddSizeOrMetrics() throws Exception {
        try (Fixture fixture = new Fixture()) {
            Assertions.assertEquals(List.of("file_path"),
                    names(fixture.plan("files", List.of("file_path")).schema()));
        }
    }

    private static List<String> names(Schema schema) {
        return schema.columns().stream().map(Types.NestedField::name).collect(Collectors.toList());
    }

    private static final class Fixture implements AutoCloseable {
        private final InMemoryCatalog catalog = new InMemoryCatalog();
        private final IcebergScanPlanProvider provider;

        private Fixture() {
            catalog.initialize("projection-test", Map.of());
            catalog.createNamespace(Namespace.of("db"));
            Table table = catalog.createTable(TableIdentifier.of("db", "t"),
                    new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
                    PartitionSpec.unpartitioned());
            table.newAppend().appendFile(DataFiles.builder(table.spec()).withPath(FILE_PATH)
                    .withFileSizeInBytes(1024).withRecordCount(2).withSplitOffsets(OFFSETS).build()).commit();
            provider = new IcebergScanPlanProvider(IcebergCatalogProperties.of(Map.of()),
                    new IcebergCatalogOps.CatalogBackedIcebergCatalogOps(catalog));
        }

        private FileScanTask plan(String systemTable, List<String> requested) {
            List<ConnectorColumnHandle> columns = requested.stream()
                    .map(name -> new IcebergColumnHandle(name,
                            DataFile.getType(PartitionSpec.unpartitioned().partitionType()).field(name).fieldId()))
                    .collect(Collectors.toList());
            List<ConnectorScanRange> ranges = provider.planScan(null, ConnectorScanRequest.builder(
                    IcebergTableHandle.forSystemTable("db", "t", systemTable, -1L, null, -1L), columns).build());
            Assertions.assertEquals(1, ranges.size());
            return SerializationUtil.deserializeFromBase64(((IcebergScanRange) ranges.get(0)).getSerializedSplit());
        }

        @Override
        public void close() throws Exception {
            catalog.close();
        }
    }
}
