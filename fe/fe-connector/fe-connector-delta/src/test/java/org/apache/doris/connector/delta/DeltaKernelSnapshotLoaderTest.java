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

package org.apache.doris.connector.delta;

import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;
import org.apache.doris.thrift.schema.external.TField;
import org.apache.doris.thrift.schema.external.TSchema;

import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.internal.util.VectorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TDeserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class DeltaKernelSnapshotLoaderTest {
    @TempDir
    Path tempDirectory;

    @ParameterizedTest
    @ValueSource(strings = {"GMT+9", "space name", "literal%2B", "hash#question?"})
    public void testEscapedTableLocations(String directoryName) throws Exception {
        Path source = Paths.get(Objects.requireNonNull(getClass().getClassLoader()
                .getResource("delta/path_table/_delta_log")).toURI());
        Path log = Files.createDirectories(tempDirectory.resolve(directoryName).resolve("_delta_log"));
        for (int version = 0; version < 2; version++) {
            Path file = log.resolve(String.format("%020d.json", version));
            Files.copy(source.resolve(file.getFileName()), file);
            Files.setLastModifiedTime(file, FileTime.fromMillis(1_700_000_000_000L + version * 2000L));
        }
        String location = log.getParent().toUri().toASCIIString().replace("+", "%2B");
        DeltaKernelSnapshotLoader loader = new DeltaKernelSnapshotLoader(DefaultEngine.create(new Configuration()));

        Assertions.assertEquals(1, loader.loadLatest(location).getVersion());
        Assertions.assertEquals(0, loader.loadVersion(location, 0).getVersion());
        Assertions.assertEquals(0, loader.loadTimestamp(location, 1_700_000_001_000L).getVersion());
    }

    @Test
    public void testLoadLatestSnapshotAndActiveFiles() throws Exception {
        URL fixture = Objects.requireNonNull(
                getClass().getClassLoader().getResource("delta/path_table"));
        String tablePath = Paths.get(fixture.toURI()).toUri().toString();
        DeltaKernelSnapshotLoader loader = new DeltaKernelSnapshotLoader(
                DefaultEngine.create(new Configuration()));

        DeltaKernelSnapshot snapshot = loader.load(tablePath);

        Assertions.assertEquals(1, snapshot.getVersion());
        Assertions.assertEquals(
                Paths.get(fixture.toURI()).toAbsolutePath().normalize().toString(),
                Paths.get(java.net.URI.create(snapshot.getTablePath())).toAbsolutePath().normalize()
                        .toString());
        Assertions.assertEquals(List.of("id", "name"), snapshot.getSchema().fieldNames());
        Assertions.assertEquals(List.of(), snapshot.getPartitionColumnNames());
        Assertions.assertEquals(
                List.of("part-00001.parquet", "part-00002.parquet"),
                snapshot.getActiveFiles().stream()
                        .map(file -> Paths.get(java.net.URI.create(file.getPath())).getFileName().toString())
                        .collect(Collectors.toList()));

        DeltaKernelSnapshot versionZero = loader.loadVersion(tablePath, 0);
        Assertions.assertEquals(0, versionZero.getVersion());
        Assertions.assertEquals(
                List.of("part-00000.parquet", "part-00001.parquet"),
                versionZero.getActiveFiles().stream()
                        .map(file -> Paths.get(java.net.URI.create(file.getPath())).getFileName().toString())
                        .collect(Collectors.toList()));
    }

    @Test
    public void testLoadSnapshotAsOfTimestamp() throws Exception {
        URL fixture = Objects.requireNonNull(
                getClass().getClassLoader().getResource("delta/path_table/_delta_log"));
        Path targetLog = tempDirectory.resolve("timestamp-table/_delta_log");
        Files.createDirectories(targetLog);
        Path versionZero = targetLog.resolve("00000000000000000000.json");
        Path versionOne = targetLog.resolve("00000000000000000001.json");
        Files.copy(Paths.get(fixture.toURI()).resolve(versionZero.getFileName()), versionZero,
                StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(fixture.toURI()).resolve(versionOne.getFileName()), versionOne,
                StandardCopyOption.REPLACE_EXISTING);
        Files.setLastModifiedTime(versionZero, FileTime.fromMillis(1_700_000_000_000L));
        Files.setLastModifiedTime(versionOne, FileTime.fromMillis(1_700_000_002_000L));

        DeltaKernelSnapshotLoader loader = new DeltaKernelSnapshotLoader(
                DefaultEngine.create(new Configuration()));
        DeltaKernelSnapshot snapshot = loader.loadTimestamp(
                targetLog.getParent().toUri().toString(), 1_700_000_001_000L);

        Assertions.assertEquals(0, snapshot.getVersion());
    }

    @Test
    public void testSerializesColumnMappingToPhysicalSchemaAliases() throws Exception {
        URL fixture = Objects.requireNonNull(
                getClass().getClassLoader().getResource("delta/column_mapping_table"));
        DeltaKernelSnapshotLoader loader = new DeltaKernelSnapshotLoader(
                DefaultEngine.create(new Configuration()));

        DeltaKernelSnapshot snapshot = loader.loadLatest(Paths.get(fixture.toURI()).toUri().toString());
        String encoded = DeltaSchemaInfo.serializeIfMapped(snapshot);
        Assertions.assertNotNull(encoded);

        TSchema schema = new TSchema();
        new TDeserializer().deserialize(schema, Base64.getDecoder().decode(encoded));
        Assertions.assertEquals(snapshot.getVersion(), schema.getSchemaId());
        TField field = schema.getRootField().getFields().get(0).getFieldPtr();
        Assertions.assertEquals("id", field.getName());
        Assertions.assertEquals(List.of("col-91e40a2f-1b63-42a0-a044-35764a3b259a"),
                field.getNameMapping());
        Assertions.assertTrue(field.isNameMappingIsAuthoritative());
        Assertions.assertEquals(1, field.getId());
    }

    @ParameterizedTest
    @ValueSource(strings = {"name", "id"})
    public void testMappedPartitionValuesAndHistoricalNames(String mode) throws Exception {
        Path fixture = Paths.get(Objects.requireNonNull(getClass().getClassLoader()
                .getResource("delta/mapped_partition_table/_delta_log")).toURI());
        DeltaKernelSnapshotLoader loader = new DeltaKernelSnapshotLoader(
                DefaultEngine.create(new Configuration()));
        Path log = Files.createDirectories(tempDirectory.resolve(mode).resolve("_delta_log"));
        for (String version : List.of("00000000000000000000.json", "00000000000000000001.json")) {
            Files.writeString(log.resolve(version), Files.readString(fixture.resolve(version))
                    .replace("\"delta.columnMapping.mode\":\"name\"",
                            "\"delta.columnMapping.mode\":\"" + mode + "\""));
        }
        String tablePath = log.getParent().toUri().toString();
        assertMappedPartitionValues(loader.loadLatest(tablePath), "area");
        assertMappedPartitionValues(loader.loadVersion(tablePath, 0), "region");
    }

    private static void assertMappedPartitionValues(DeltaKernelSnapshot snapshot, String partitionName) {
        Assertions.assertEquals(List.of("day", partitionName), snapshot.getPartitionColumnNames());
        Assertions.assertEquals(1, snapshot.getActiveFiles().size());
        DeltaScanFile file = snapshot.getActiveFiles().get(0);
        Assertions.assertEquals(List.of("day", partitionName), List.copyOf(file.getPartitionValues().keySet()));
        Assertions.assertTrue(file.getPartitionValues().containsKey("day"));
        Assertions.assertNull(file.getPartitionValues().get("day"));
        Assertions.assertEquals("west", file.getPartitionValues().get(partitionName));

        TFileRangeDesc range = new TFileRangeDesc();
        new DeltaScanRange(file).populateRangeParams(new TTableFormatFileDesc(), range);
        Assertions.assertEquals(List.of("day", partitionName), range.getColumnsFromPathKeys());
        Assertions.assertEquals(List.of("\\N", "west"), range.getColumnsFromPath());
        Assertions.assertEquals(List.of(true, false), range.getColumnsFromPathIsNull());

        Row action = snapshot.getActiveRemoveFiles().get(0).toSingleAction(1);
        RemoveFile remove = new RemoveFile(action.getStruct(SingleAction.FULL_SCHEMA.indexOf("remove")));
        Map<String, String> physicalValues = VectorUtils.toJavaMap(remove.getPartitionValues().orElseThrow());
        Assertions.assertEquals(2, physicalValues.size());
        Assertions.assertTrue(physicalValues.containsKey("col-day"));
        Assertions.assertNull(physicalValues.get("col-day"));
        Assertions.assertEquals("west", physicalValues.get("col-region"));
    }

    @Test
    public void testRejectReaderFeaturesThatNeedPhysicalTransforms() throws Exception {
        URL fixture = Objects.requireNonNull(getClass().getClassLoader()
                .getResource("delta/unsupported_reader_feature_table"));
        DeltaKernelSnapshotLoader loader = new DeltaKernelSnapshotLoader(
                DefaultEngine.create(new Configuration()));

        UnsupportedOperationException exception = Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> loader.loadLatest(Paths.get(fixture.toURI()).toUri().toString()));

        Assertions.assertTrue(exception.getMessage().contains("typeWidening"));
        Assertions.assertTrue(exception.getMessage().contains("physical-row transforms"));
    }

    @Test
    public void testRejectsUnknownReaderFeature() throws Exception {
        URL fixture = Objects.requireNonNull(getClass().getClassLoader()
                .getResource("delta/unknown_reader_feature_table"));
        DeltaKernelSnapshotLoader loader = new DeltaKernelSnapshotLoader(
                DefaultEngine.create(new Configuration()));

        RuntimeException exception = Assertions.assertThrows(
                RuntimeException.class,
                () -> loader.loadLatest(Paths.get(fixture.toURI()).toUri().toString()));

        Assertions.assertTrue(exception.getMessage().contains("futureReaderFeature"));
    }

    @Test
    public void testRejectCatalogManagedTableFromPathAdapter() throws Exception {
        URL fixture = Objects.requireNonNull(
                getClass().getClassLoader().getResource("delta/catalog_managed_table"));
        DeltaKernelSnapshotLoader loader = new DeltaKernelSnapshotLoader(
                DefaultEngine.create(new Configuration()));

        UnsupportedOperationException exception = Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> loader.loadLatest(Paths.get(fixture.toURI()).toUri().toString()));

        Assertions.assertTrue(exception.getMessage().contains("catalog-aware adapter"));
    }

    @Test
    public void testCarriesDeltaDeletionVectorDescriptorToScanRange() throws Exception {
        URL fixture = Objects.requireNonNull(
                getClass().getClassLoader().getResource("delta/deletion_vector_table"));
        DeltaKernelSnapshotLoader loader = new DeltaKernelSnapshotLoader(
                DefaultEngine.create(new Configuration()));

        DeltaKernelSnapshot snapshot = loader.loadLatest(Paths.get(fixture.toURI()).toUri().toString());

        DeltaDeletionVector deletionVector = snapshot.getActiveFiles().get(0).getDeletionVector();
        Assertions.assertNotNull(deletionVector);
        Assertions.assertEquals("i", deletionVector.getStorageType());
        Assertions.assertEquals("00000", deletionVector.getPathOrInlineDv());
        Assertions.assertEquals(0, deletionVector.getSizeInBytes());
        Assertions.assertEquals(0, deletionVector.getCardinality());

        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        new DeltaScanRange(snapshot.getActiveFiles().get(0)).populateRangeParams(
                formatDesc, rangeDesc);
        Assertions.assertTrue(formatDesc.isSetDeltaParams());
        Assertions.assertEquals("i", formatDesc.getDeltaParams().getStorageType());
        Assertions.assertEquals("00000", formatDesc.getDeltaParams().getPathOrInlineDv());
        Assertions.assertEquals(0, formatDesc.getDeltaParams().getSizeInBytes());
        Assertions.assertEquals(0, formatDesc.getDeltaParams().getCardinality());
        Assertions.assertEquals(snapshot.getTablePath(), formatDesc.getDeltaParams().getTablePath());
    }

    @Test
    public void testRejectsNonParquetDeltaProvider() throws Exception {
        URL fixture = Objects.requireNonNull(getClass().getClassLoader()
                .getResource("delta/non_parquet_table"));
        DeltaKernelSnapshotLoader loader = new DeltaKernelSnapshotLoader(
                DefaultEngine.create(new Configuration()));

        UnsupportedOperationException exception = Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> loader.loadLatest(Paths.get(fixture.toURI()).toUri().toString()));

        Assertions.assertTrue(exception.getMessage().contains("only supports Parquet"));
    }
}
