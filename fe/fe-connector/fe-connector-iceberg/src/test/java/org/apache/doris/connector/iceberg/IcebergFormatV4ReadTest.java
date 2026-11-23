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

import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ConnectorSplitSource;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class IcebergFormatV4ReadTest {
    @TempDir
    Path tempDir;

    @Test
    void v4ParquetManifestsAreReadable() throws Exception {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("v4", Collections.singletonMap(
                CatalogProperties.WAREHOUSE_LOCATION, tempDir.toUri().toString()));
        catalog.createNamespace(Namespace.of("db"));
        Table table = catalog.createTable(TableIdentifier.of("db", "v4_table"), schema,
                PartitionSpec.unpartitioned(), null, Collections.singletonMap(TableProperties.FORMAT_VERSION, "4"));
        DataFile data = DataFiles.builder(table.spec()).withPath(table.location() + "/data/first.parquet")
                .withFormat(FileFormat.PARQUET).withFileSizeInBytes(100).withRecordCount(10).build();
        table.newFastAppend().appendFile(data).commit();

        List<ManifestFile> manifests = table.currentSnapshot().dataManifests(table.io());
        Assertions.assertEquals(1, manifests.size());
        Assertions.assertEquals(FileFormat.PARQUET, FileFormat.fromFileName(manifests.get(0).path()));
        try (ManifestReader<DataFile> reader = ManifestFiles.read(manifests.get(0), table.io(), table.specs())) {
            DataFile actual = reader.iterator().next();
            Assertions.assertEquals(data.location(), actual.location());
            Assertions.assertEquals(10, actual.recordCount());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void relativeDataAndVectorPathsKeepLogicalIdentity(boolean cached) throws Exception {
        Table table = createTable(4);
        DataFile first = data(table, "/data/first.parquet");
        table.newFastAppend().appendFile(first).appendFile(data(table, "data/second.parquet")).commit();
        table.newRowDelta().addDeletes(FileMetadata.deleteFileBuilder(table.spec()).ofPositionDeletes()
                .withPath("/data/deletes.puffin").withFormat(FileFormat.PUFFIN)
                .withReferencedDataFile(first.location()).withContentOffset(16).withContentSizeInBytes(64)
                .withFileSizeInBytes(128).withRecordCount(2).build()).commit();
        // The list itself is relative too, so a range-only resolver cannot satisfy this test.
        Table relative = relativeMetadata(table);
        IcebergScanPlanProvider provider = provider(relative, cached);
        ConnectorScanRequest request = request();
        for (int attempt = 0; attempt < 2; attempt++) {
            assertVectorRanges(provider.planScan(null, request), table.location());
            List<ConnectorScanRange> streamed = new ArrayList<>();
            try (ConnectorSplitSource source = provider.streamSplits(null, request)) {
                while (source.hasNext()) {
                    streamed.add(source.next());
                }
            }
            assertVectorRanges(streamed, table.location());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void upgradedTablesRetainPositionAndEqualityDeletes(int originalVersion) throws Exception {
        Table table = createTable(2);
        DataFile data = data(table, originalVersion == 2
                ? "/legacy/data/old.parquet" : "s3://example-bucket/table/data/old.parquet");
        table.newAppend().appendFile(data).commit();
        table.newRowDelta()
                .addDeletes(FileMetadata.deleteFileBuilder(table.spec()).ofPositionDeletes()
                        .withPath("s3://example-bucket/table/data/position.parquet")
                        .withFileSizeInBytes(100).withRecordCount(1).build())
                .addDeletes(FileMetadata.deleteFileBuilder(table.spec()).ofEqualityDeletes(1)
                        .withPath("s3://example-bucket/table/data/equality.parquet")
                        .withFileSizeInBytes(100).withRecordCount(1).build()).commit();
        if (originalVersion == 3) {
            table.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit();
        }
        table.updateProperties().set(TableProperties.FORMAT_VERSION, "4").commit();
        table.newFastAppend().appendFile(data(table, "data/new.parquet")).commit();
        Map<String, Long> expectedFirstRowIds = new HashMap<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                expectedFirstRowIds.put(task.file().location(), task.file().firstRowId());
            }
        }
        for (boolean cached : new boolean[] {false, true}) {
            List<ConnectorScanRange> ranges = provider(table, cached).planScan(null, request());
            Assertions.assertEquals(2, ranges.size());
            for (ConnectorScanRange range : ranges) {
                TIcebergFileDesc file = descriptor(range);
                Assertions.assertEquals(4, file.getFormatVersion());
                if (file.getOriginalFilePath().equals(data.location())) {
                    Assertions.assertEquals(data.location(), range.getPath().orElseThrow());
                    Assertions.assertEquals(expectedFirstRowIds.get(data.location()).longValue(), file.getFirstRowId());
                    Assertions.assertEquals(2, file.getDeleteFilesSize());
                    Assertions.assertTrue(file.getDeleteFiles().stream().anyMatch(delete -> delete.getContent() == 1));
                    Assertions.assertTrue(file.getDeleteFiles().stream().anyMatch(delete -> delete.getContent() == 2
                            && delete.getFieldIds().equals(Collections.singletonList(1))));
                } else {
                    Assertions.assertEquals(table.location() + "/data/new.parquet", range.getPath().orElseThrow());
                    Assertions.assertTrue(file.getFirstRowId() >= 0);
                    Assertions.assertTrue(file.getDeleteFiles().isEmpty());
                }
            }
        }
    }

    @Test
    void relativeManifestNamesAreIsolatedByTableLocation() throws Exception {
        IcebergManifestCache cache = new IcebergManifestCache();
        List<String> names = List.of("left", "right");
        List<Table> views = new ArrayList<>();
        List<ManifestFile> manifests = new ArrayList<>();
        for (String name : names) {
            Table table = createTable(4);
            table.updateLocation().setLocation("s3://example-bucket/" + name).commit();
            table.newFastAppend().appendFile(data(table, "data/" + name + ".parquet")).commit();
            ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
            String relativePath = "metadata/shared.parquet";
            try (org.apache.iceberg.io.SeekableInputStream input = table.io().newInputFile(manifest).newStream();
                    OutputStream output = table.io().newOutputFile(table.location() + "/" + relativePath).create()) {
                input.transferTo(output);
            }
            GenericManifestFile relative = (GenericManifestFile) manifest.copy();
            relative.put(0, relativePath);
            manifests.add(relative);
            views.add(provider(table, true).wrapTableForScan(table));
        }
        for (int attempt = 0; attempt < 2; attempt++) {
            for (int i = 0; i < names.size(); i++) {
                ManifestCacheValue value = cache.getManifestCacheValue(manifests.get(i), views.get(i));
                Assertions.assertEquals("data/" + names.get(i) + ".parquet", value.getDataFiles().get(0).location());
                Assertions.assertSame(value, cache.getCachedManifestValue(manifests.get(i), views.get(i), null));
            }
        }
    }

    @Test
    void partitionMetadataResolvesRelativeManifestLists() throws Exception {
        Table table = createTable(4);
        table.updateSpec().addField("id").commit();
        table.newFastAppend().appendFile(DataFiles.builder(table.spec()).withPath("data/partitioned.parquet")
                .withPartitionPath("id=7").withFileSizeInBytes(100).withRecordCount(10).build()).commit();
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = relativeMetadata(table);
        IcebergConnectorMetadata metadata = new IcebergConnectorMetadata(ops,
                IcebergCatalogProperties.of(Collections.emptyMap()), new RecordingConnectorContext());
        Assertions.assertEquals(Collections.singletonList("id=7"),
                metadata.listPartitionNames(null, new IcebergTableHandle("db", "table")));
    }

    @Test
    void positionDeleteMetadataKeepsLegacyPathsAfterUpgrade() {
        Table table = createTable(2);
        table.newFastAppend().appendFile(data(table, "s3://example-bucket/data/old.parquet")).commit();
        table.newRowDelta().addDeletes(FileMetadata.deleteFileBuilder(table.spec()).ofPositionDeletes()
                .withPath("/legacy/position.parquet").withFileSizeInBytes(100).withRecordCount(1).build()).commit();
        table.updateProperties().set(TableProperties.FORMAT_VERSION, "4").commit();
        List<ConnectorScanRange> ranges = provider(table, false).planScan(null,
                ConnectorScanRequest.builder(IcebergTableHandle.forSystemTable(
                        "db", "table", "position_deletes", -1L, null, -1L), Collections.emptyList()).build());
        Assertions.assertEquals(1, ranges.size());
        Assertions.assertEquals("/legacy/position.parquet", ranges.get(0).getPath().orElseThrow());
    }

    @Test
    void avroV4ManifestUsesV4PathSemantics() throws Exception {
        Table table = createTable(4);
        String manifestPath = table.location() + "/metadata/explicit.avro";
        ManifestWriter<DataFile> writer = ManifestFiles.write(4, table.spec(),
                table.io().newOutputFile(manifestPath), null);
        try (ManifestWriter<DataFile> closeable = writer) {
            closeable.add(data(table, "/data/avro-v4.parquet"));
        }
        table.newFastAppend().appendManifest(writer.toManifestFile()).commit();
        Assertions.assertEquals(FileFormat.AVRO,
                FileFormat.fromFileName(table.currentSnapshot().dataManifests(table.io()).get(0).path()));
        List<ConnectorScanRange> ranges = provider(table, false).planScan(null, request());
        Assertions.assertEquals(1, ranges.size());
        Assertions.assertEquals(table.location() + "//data/avro-v4.parquet", ranges.get(0).getPath().orElseThrow());
    }

    @Test
    void absoluteEntriesDoNotRequireTableLocation() throws Exception {
        Table table = createTable(4);
        String path = "s3://example-bucket/data/absolute.parquet";
        table.newFastAppend().appendFile(data(table, path)).commit();
        ObjectNode json = (ObjectNode) JsonUtil.mapper().readTree(
                TableMetadataParser.toJson(((BaseTable) table).operations().current()));
        json.remove("location");
        Table withoutLocation = new BaseTable(new StaticTableOperations(TableMetadataParser.fromJson(json), table.io()),
                table.name());
        for (boolean cached : new boolean[] {false, true}) {
            List<ConnectorScanRange> ranges = provider(withoutLocation, cached).planScan(null, request());
            Assertions.assertEquals(1, ranges.size());
            Assertions.assertEquals(path, ranges.get(0).getPath().orElseThrow());
        }
    }

    @Test
    void relativePathsRequireABaseWhileAbsolutePathsDoNot() {
        Assertions.assertEquals("s3://example-bucket/data.parquet",
                IcebergV4FileIO.resolve(null, "s3://example-bucket/data.parquet"));
        Assertions.assertEquals("s3://example-bucket/table//data.parquet",
                IcebergV4FileIO.resolve("s3://example-bucket/table", "/data.parquet"));
        Assertions.assertEquals("s3://example-bucket/table//data.parquet",
                IcebergV4FileIO.resolve("s3://example-bucket/table/", "data.parquet"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> IcebergV4FileIO.resolve(null, "/data.parquet"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> IcebergV4FileIO.resolve(null, "data.parquet"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> IcebergV4FileIO.resolve("", "data.parquet"));
    }

    private Table createTable(int version) {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("v4", Collections.singletonMap(CatalogProperties.WAREHOUSE_LOCATION,
                tempDir.toUri().toString()));
        catalog.createNamespace(Namespace.of("db"));
        return catalog.createTable(TableIdentifier.of("db", "table"),
                new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
                PartitionSpec.unpartitioned(), null,
                Collections.singletonMap(TableProperties.FORMAT_VERSION, Integer.toString(version)));
    }

    private static Table relativeMetadata(Table table) throws Exception {
        TableMetadata metadata = ((BaseTable) table).operations().current();
        String json = TableMetadataParser.toJson(metadata).replace(table.location() + "/metadata/", "metadata/");
        // Metadata data tasks carry the metadata JSON itself as their synthetic input file.
        String location = table.location() + "/metadata/read-view.metadata.json";
        try (OutputStream output = table.io().newOutputFile(location).createOrOverwrite()) {
            output.write(json.getBytes(StandardCharsets.UTF_8));
        }
        return new BaseTable(new StaticTableOperations(TableMetadataParser.fromJson(location, json), table.io()),
                table.name());
    }

    private static DataFile data(Table table, String path) {
        return DataFiles.builder(table.spec()).withPath(path).withFileSizeInBytes(100).withRecordCount(10).build();
    }

    private static IcebergScanPlanProvider provider(Table table, boolean cached) {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = table;
        return new IcebergScanPlanProvider(IcebergCatalogProperties.of(Collections.singletonMap(
                "meta.cache.iceberg.manifest.enable", Boolean.toString(cached))), ops, null,
                cached ? new IcebergManifestCache() : null);
    }

    private static ConnectorScanRequest request() {
        return ConnectorScanRequest.builder(new IcebergTableHandle("db", "table"), Collections.emptyList()).build();
    }

    private static TIcebergFileDesc descriptor(ConnectorScanRange range) {
        TTableFormatFileDesc format = new TTableFormatFileDesc();
        range.populateRangeParams(format, new TFileRangeDesc());
        return format.getIcebergParams();
    }

    private static void assertVectorRanges(List<ConnectorScanRange> ranges, String base) {
        Assertions.assertEquals(2, ranges.size());
        int vectors = 0;
        for (ConnectorScanRange range : ranges) {
            TIcebergFileDesc file = descriptor(range);
            Assertions.assertEquals(4, file.getFormatVersion());
            Assertions.assertTrue(file.getFirstRowId() >= 0);
            Assertions.assertEquals(base + "/" + file.getOriginalFilePath(), range.getPath().orElseThrow());
            if (file.getOriginalFilePath().equals("/data/first.parquet")) {
                Assertions.assertEquals(1, file.getDeleteFilesSize());
                TIcebergDeleteFileDesc vector = file.getDeleteFiles().get(0);
                Assertions.assertEquals(base + "//data/deletes.puffin", vector.getPath());
                Assertions.assertEquals(3, vector.getContent());
                Assertions.assertEquals(16, vector.getContentOffset());
                Assertions.assertEquals(64, vector.getContentSizeInBytes());
                Assertions.assertEquals(128, vector.getFileSize());
                vectors++;
            } else {
                Assertions.assertTrue(file.getDeleteFiles().isEmpty());
            }
        }
        Assertions.assertEquals(1, vectors);
    }

}
