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

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ConnectorSplitSource;
import org.apache.doris.kerberos.HadoopAuthenticator;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
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
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.NativeGcsResolvingFileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

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
    @CsvSource({"2,false,false,false", "2,true,false,false", "3,false,false,false", "3,true,false,false",
            "2,false,true,false", "2,true,true,false", "3,false,true,false", "3,true,true,false",
            "2,false,false,true", "2,true,false,true", "3,false,false,true", "3,true,false,true",
            "2,false,true,true", "2,true,true,true", "3,false,true,true", "3,true,true,true"})
    void deletionVectorsUseTheProducingManifestIdentity(int originalVersion, boolean cached, boolean legacyTarget,
            boolean resolving) throws Exception {
        Table table = createTable(originalVersion);
        String path = "/data/identity.parquet";
        table.newFastAppend().appendFile(data(table, path)).commit();
        table.updateProperties().set(TableProperties.FORMAT_VERSION, "4").commit();
        table.newFastAppend().appendFile(data(table, path)).commit();
        table.newRowDelta().addDeletes(FileMetadata.deleteFileBuilder(table.spec()).ofPositionDeletes()
                .withPath("data/vector.puffin").withFormat(FileFormat.PUFFIN)
                .withReferencedDataFile(legacyTarget ? "file:/data/identity.parquet" : path)
                .withContentOffset(16).withContentSizeInBytes(64).withFileSizeInBytes(128)
                .withRecordCount(2).build()).commit();
        FileIO delegate = table.io();
        Configuration filesystem = new Configuration(false);
        filesystem.set("fs.defaultFS", "file:///");
        filesystem.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        RecordingAuthenticator auth = new RecordingAuthenticator();
        FileIO filesystemIdentity = new HadoopFileIO(filesystem) {
            @Override
            public InputFile newInputFile(String location) {
                // Metadata remains in memory; Hadoop supplies the filesystem identity of rooted data paths.
                Assertions.assertTrue(auth.active.get(), "Identity qualification must preserve doAs");
                return location.endsWith(path) ? HadoopInputFile.fromLocation(location, filesystem)
                        : delegate.newInputFile(location);
            }

            @Override
            public InputFile newInputFile(String location, long length) {
                return newInputFile(location);
            }

            @Override
            public OutputFile newOutputFile(String location) {
                return delegate.newOutputFile(location);
            }

            @Override
            public void deleteFile(String location) {
                delegate.deleteFile(location);
            }
        };
        FileIO storage = resolving ? new NativeGcsResolvingFileIO() {
            @Override
            public Map<String, String> properties() {
                return delegate.properties();
            }

            @Override
            public InputFile newInputFile(String location) {
                return filesystemIdentity.newInputFile(location);
            }

            @Override
            public InputFile newInputFile(String location, long length) {
                return newInputFile(location);
            }
        } : filesystemIdentity;
        if (resolving) {
            storage = new IcebergRestEncryptionFileIO(storage,
                    new AtomicReference<>(((BaseTable) table).operations().current()), false);
        }
        FileIO authenticated = new IcebergAuthenticatedFileIO(storage, auth);
        Table view = new BaseTable(new StaticTableOperations(((BaseTable) table).operations().current(),
                authenticated), table.name());
        IcebergScanPlanProvider scans = provider(view, cached);
        List<List<ConnectorScanRange>> plans = new ArrayList<>();
        plans.add(scans.planScan(null, request()));
        List<ConnectorScanRange> streamed = new ArrayList<>();
        try (ConnectorSplitSource source = scans.streamSplits(null, request())) {
            while (source.hasNext()) {
                streamed.add(source.next());
            }
        }
        plans.add(streamed);
        for (List<ConnectorScanRange> ranges : plans) {
            Assertions.assertEquals(2, ranges.size());
            for (ConnectorScanRange range : ranges) {
                boolean legacy = range.getPath().orElseThrow().equals(path);
                Assertions.assertEquals(path, descriptor(range).getOriginalFilePath());
                Assertions.assertEquals(legacy == legacyTarget ? 1 : 0,
                        descriptor(range).getDeleteFilesSize(), range.getPath().orElseThrow());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void expiredLegacyManifestRejectsWrongRelativeCandidate(boolean ambiguous) throws Exception {
        Table table = createTable(2);
        table.updateProperties().set(TableProperties.WRITE_METADATA_LOCATION,
                "/metadata/" + tempDir.getFileName()).commit();
        table.newFastAppend().appendFile(data(table, "/data/correct.parquet")).commit();
        ManifestFile retained = table.currentSnapshot().dataManifests(table.io()).get(0);
        long producer = table.currentSnapshot().snapshotId();
        table.updateProperties().set(TableProperties.FORMAT_VERSION, "4").commit();
        table.newFastAppend().appendFile(data(table, "data/current.parquet")).commit();
        String substitute = table.location() + "/" + retained.path();
        if (ambiguous) {
            try (org.apache.iceberg.io.SeekableInputStream input = table.io().newInputFile(retained.path()).newStream();
                    OutputStream output = table.io().newOutputFile(substitute).create()) {
                input.transferTo(output);
            }
        } else {
            try (ManifestWriter<DataFile> writer = ManifestFiles.write(2, table.spec(),
                    table.io().newOutputFile(substitute), null)) {
                writer.add(DataFiles.builder(table.spec()).withPath("/data/unrelated-long-name.parquet")
                        .withFileSizeInBytes(100).withRecordCount(77).build());
            }
            Assertions.assertNotEquals(retained.length(), table.io().newInputFile(substitute).getLength());
        }
        table.expireSnapshots().expireSnapshotId(producer).cleanExpiredFiles(false).commit();
        for (boolean cached : new boolean[] {false, true}) {
            IcebergScanPlanProvider scans = provider(coldMetadata(table), cached);
            ConnectorSession session = new IcebergStatementScopeTest.ScopeSession(1, "manifest-identity",
                    org.apache.doris.connector.spi.ConnectorStatementScope.NONE);
            if (ambiguous) {
                RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                        () -> scans.planScan(session, request()));
                Throwable cause = failure;
                while (cause.getCause() != null) {
                    cause = cause.getCause();
                }
                Assertions.assertTrue(cause.getMessage().contains("Ambiguous"), cause.toString());
            } else {
                List<ConnectorScanRange> ranges = scans.planScan(session, request());
                Assertions.assertEquals(2, ranges.size());
                Assertions.assertEquals(Set.of("/data/correct.parquet", table.location() + "/data/current.parquet"),
                        ranges.stream().map(range -> range.getPath().orElseThrow()).collect(Collectors.toSet()));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void rootedManifestAliasesRemainReadableAfterProducerExpiration(boolean cached) throws Exception {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("aliases", Map.of());
        catalog.createNamespace(Namespace.of("db"));
        Table table = catalog.createTable(TableIdentifier.of("db", "aliases"),
                new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
                PartitionSpec.unpartitioned(), "testfs://namespace/",
                Map.of(TableProperties.FORMAT_VERSION, "3", TableProperties.WRITE_METADATA_LOCATION,
                        tempDir.resolve("metadata").toString()));
        table.newFastAppend().appendFile(data(table, "testfs://namespace/data/first.parquet")).commit();
        long producer = table.currentSnapshot().snapshotId();
        table.newFastAppend().appendFile(data(table, "testfs://namespace/data/second.parquet")).commit();
        table.updateProperties().set(TableProperties.FORMAT_VERSION, "4").commit();
        table.expireSnapshots().expireSnapshotId(producer).cleanExpiredFiles(false).commit();
        Assertions.assertNull(table.snapshot(producer));
        Assertions.assertEquals("testfs://namespace", table.location());
        List<String> metadataFiles = new ArrayList<>();
        metadataFiles.add(table.currentSnapshot().manifestListLocation());
        metadataFiles.add(((BaseTable) table).operations().current().metadataFileLocation());
        for (ManifestFile manifest : table.currentSnapshot().allManifests(table.io())) {
            Assertions.assertEquals(FileFormat.AVRO, FileFormat.fromFileName(manifest.path()));
            metadataFiles.add(manifest.path());
        }
        for (String location : metadataFiles) {
            Path target = Path.of(location);
            Assertions.assertTrue(target.startsWith(tempDir));
            Files.createDirectories(target.getParent());
            try (org.apache.iceberg.io.SeekableInputStream input = table.io().newInputFile(location).newStream();
                    OutputStream output = Files.newOutputStream(target)) {
                input.transferTo(output);
            }
        }
        Configuration conf = new Configuration(false);
        conf.set("fs.defaultFS", "testfs://namespace");
        conf.set("fs.testfs.impl", RootedLocalFileSystem.class.getName());
        conf.setBoolean("fs.testfs.impl.disable.cache", true);
        RecordingAuthenticator auth = new RecordingAuthenticator();
        FileIO storage = new HadoopFileIO(conf) {
            @Override
            public InputFile newInputFile(String location) {
                Assertions.assertTrue(auth.active.get(), "Alias resolution must preserve authenticated IO");
                return super.newInputFile(location);
            }

            @Override
            public InputFile newInputFile(String location, long length) {
                Assertions.assertTrue(auth.active.get(), "Alias resolution must preserve authenticated IO");
                return super.newInputFile(location, length);
            }
        };
        Table cold = new BaseTable(new StaticTableOperations(((BaseTable) table).operations().current(),
                new IcebergAuthenticatedFileIO(storage, auth)), table.name());
        IcebergScanPlanProvider scans = provider(cold, cached);
        ConnectorSession session = new IcebergStatementScopeTest.ScopeSession(1, "manifest-alias",
                org.apache.doris.connector.spi.ConnectorStatementScope.NONE);
        Set<String> expected = Set.of("testfs://namespace/data/first.parquet",
                "testfs://namespace/data/second.parquet");
        List<ConnectorScanRange> eager = Assertions.assertDoesNotThrow(() -> scans.planScan(session, request()));
        Assertions.assertEquals(expected, eager.stream()
                .map(range -> range.getPath().orElseThrow()).collect(Collectors.toSet()));
        Set<String> streamed = new java.util.HashSet<>();
        try (ConnectorSplitSource source = scans.streamSplits(session, request())) {
            while (source.hasNext()) {
                streamed.add(source.next().getPath().orElseThrow());
            }
        }
        Assertions.assertEquals(expected, streamed);
    }

    /** Real Hadoop path/qualification semantics with all file bytes kept in the test's temporary directory. */
    public static class RootedLocalFileSystem extends RawLocalFileSystem {
        @Override
        public URI getUri() {
            return URI.create("testfs://namespace");
        }

        @Override
        public String getScheme() {
            return "testfs";
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void relativeManifestDoesNotHideStorageFailures(boolean permissionFailure) throws Exception {
        Table table = createTable(4);
        String path = "/metadata/candidate.avro";
        ManifestWriter<DataFile> writer = ManifestFiles.write(4, table.spec(),
                table.io().newOutputFile(table.location() + "/" + path), 1L);
        try (ManifestWriter<DataFile> closeable = writer) {
            closeable.add(data(table, "data/correct.parquet"));
        }
        GenericManifestFile descriptor = (GenericManifestFile) writer.toManifestFile().copy();
        descriptor.put(0, path);
        FileIO storage = new FileIO() {
            @Override
            public InputFile newInputFile(String location) {
                if (location.startsWith("/")) {
                    if (permissionFailure) {
                        throw new UncheckedIOException(new IOException("Permission denied"));
                    }
                    throw new IllegalArgumentException("A URI scheme is required");
                }
                return table.io().newInputFile(location);
            }

            @Override
            public OutputFile newOutputFile(String location) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void deleteFile(String location) {
                throw new UnsupportedOperationException();
            }
        };
        IcebergV4FileIO io = new IcebergV4FileIO(storage, table.location());
        if (permissionFailure) {
            Assertions.assertThrows(UncheckedIOException.class, () -> io.newInputFile(descriptor));
        } else {
            try (ManifestReader<DataFile> reader = ManifestFiles.read(descriptor, io, table.specs())) {
                Assertions.assertEquals("data/correct.parquet", reader.iterator().next().location());
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"refresh,false,4", "refresh,true,4", "commit,false,4", "commit,true,4",
            "commit,false,3", "commit,true,3"})
    void procedureRejectsConcurrentFormatUpgrade(String phase, boolean authenticated, int targetVersion) throws Exception {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("race", Map.of(CatalogProperties.WAREHOUSE_LOCATION, tempDir.toUri().toString()));
        catalog.createNamespace(Namespace.of("db"));
        TableIdentifier identifier = TableIdentifier.of("db", "race");
        Table table = catalog.createTable(identifier,
                new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
                PartitionSpec.unpartitioned(), null, Map.of(TableProperties.FORMAT_VERSION, "3"));
        table.newFastAppend().appendFile(data(table, "/data/first.parquet")).commit();
        table.newFastAppend().appendFile(data(table, "/data/second.parquet")).commit();
        long snapshot = table.currentSnapshot().snapshotId();
        Table concurrent = catalog.loadTable(identifier);
        TableOperations delegate = ((BaseTable) table).operations();
        AtomicBoolean upgraded = new AtomicBoolean();
        TableOperations racing = new TableOperations() {
            private void upgrade(String at) {
                if (phase.equals(at) && upgraded.compareAndSet(false, true)) {
                    concurrent.updateProperties().set(TableProperties.FORMAT_VERSION, Integer.toString(targetVersion))
                            .set("test.concurrent-commit", "true").commit();
                }
            }

            @Override
            public TableMetadata current() {
                return delegate.current();
            }

            @Override
            public TableMetadata refresh() {
                upgrade("refresh");
                return delegate.refresh();
            }

            @Override
            public void commit(TableMetadata base, TableMetadata metadata) {
                upgrade("commit");
                delegate.commit(base, metadata);
            }

            @Override
            public FileIO io() {
                return delegate.io();
            }

            @Override
            public String metadataFileLocation(String name) {
                return delegate.metadataFileLocation(name);
            }

            @Override
            public org.apache.iceberg.io.LocationProvider locationProvider() {
                return delegate.locationProvider();
            }
        };
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = new BaseTable(racing, table.name());
        IcebergProcedureOps procedures = new IcebergProcedureOps(Map.of(), ops,
                authenticated ? new RecordingConnectorContext() : null);
        if (targetVersion == 3) {
            procedures.execute(null, new IcebergTableHandle("db", "race"), "rewrite_manifests",
                    Map.of(), null, List.of());
            Table committed = catalog.loadTable(identifier);
            Assertions.assertTrue(upgraded.get());
            Assertions.assertNotEquals(snapshot, committed.currentSnapshot().snapshotId());
            Assertions.assertEquals("true", committed.properties().get("test.concurrent-commit"));
            Assertions.assertEquals(1, committed.currentSnapshot().dataManifests(committed.io()).size());
            return;
        }
        RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                () -> procedures.execute(null, new IcebergTableHandle("db", "race"), "rewrite_manifests",
                        Map.of(), null, List.of()));
        Assertions.assertTrue(failure.getMessage().contains("format"));
        Assertions.assertTrue(upgraded.get());
        table.refresh();
        Assertions.assertEquals(4, ((BaseTable) table).operations().current().formatVersion());
        Assertions.assertEquals(snapshot, table.currentSnapshot().snapshotId());
        Assertions.assertEquals(Set.of("/data/first.parquet", "/data/second.parquet"),
                provider(coldMetadata(table), false).planScan(null, request()).stream()
                        .map(range -> range.getPath().orElseThrow()).collect(Collectors.toSet()));
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

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void upgradedTablesKeepAbsoluteFilesystemMetadataLocations(int originalVersion) throws Exception {
        Table table = createTable(originalVersion);
        table.updateLocation().setLocation("hdfs://namenode.example/warehouse/table").commit();
        String metadataPath = "/metadata/" + tempDir.getFileName();
        table.updateProperties().set(TableProperties.WRITE_METADATA_LOCATION, metadataPath).commit();
        String legacyData = "/data/legacy.parquet";
        table.newFastAppend().appendFile(data(table, legacyData)).commit();
        long legacySnapshot = table.currentSnapshot().snapshotId();
        Assertions.assertTrue(table.currentSnapshot().manifestListLocation().startsWith(metadataPath + "/"));
        table.updateProperties().set(TableProperties.FORMAT_VERSION, "4").commit();

        // Discard SDK snapshot caches so the upgraded view must reopen its inherited manifest list.
        Table cold = coldMetadata(table);
        Table view = IcebergSnapshotTableOperations.v4ReadTable(cold);
        List<ManifestFile> manifests = Assertions.assertDoesNotThrow(
                () -> view.currentSnapshot().allManifests(view.io()));
        Assertions.assertEquals(1, manifests.size());
        FileIO transported = SerializationUtil.deserializeFromBase64(SerializationUtil.serializeToBase64(view.io()));
        Assertions.assertEquals(1, coldMetadata(table).currentSnapshot().allManifests(transported).size());
        Table inline = new BaseTable(new StaticTableOperations(TableMetadataParser.fromJson(
                TableMetadataParser.toJson(((BaseTable) cold).operations().current())), table.io()), table.name());
        Assertions.assertNull(((BaseTable) inline).operations().current().metadataFileLocation());
        FileIO inlineIO = SerializationUtil.deserializeFromBase64(SerializationUtil.serializeToBase64(
                IcebergSnapshotTableOperations.v4ReadTable(inline).io()));
        ManifestFile inlineManifest = coldMetadata(table).currentSnapshot().allManifests(inlineIO).get(0);
        try (ManifestReader<DataFile> reader = ManifestFiles.read(inlineManifest, inlineIO, table.specs())) {
            Assertions.assertEquals(legacyData,
                    ((IcebergV4FileIO) inlineIO).resolveContentFile(reader.iterator().next()));
        }
        Assertions.assertTrue(manifests.stream().allMatch(manifest -> manifest.path().startsWith(metadataPath + "/")));
        for (ManifestFile manifest : manifests) {
            try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, view.io(), view.specs())) {
                DataFile file = reader.iterator().next();
                Assertions.assertEquals(legacyData, ((IcebergV4FileIO) view.io()).resolveContentFile(file));
            }
        }
        for (boolean cached : new boolean[] {false, true}) {
            IcebergScanPlanProvider provider = provider(cold, cached);
            for (int attempt = 0; attempt < 2; attempt++) {
                List<ConnectorScanRange> ranges = provider.planScan(null, request());
                Assertions.assertEquals(1, ranges.size());
                Assertions.assertEquals(legacyData, ranges.get(0).getPath().orElseThrow());
            }
        }

        // New V4 entries share the same absolute metadata directory but keep table-relative data semantics.
        table.newFastAppend().appendFile(data(table, "/data/new.parquet")).commit();
        Table latest = coldMetadata(table);
        Table latestView = IcebergSnapshotTableOperations.v4ReadTable(latest);
        Assertions.assertDoesNotThrow(() -> latestView.snapshot(legacySnapshot).allManifests(latestView.io()));
        for (boolean cached : new boolean[] {false, true}) {
            List<ConnectorScanRange> ranges = provider(latest, cached).planScan(null, request());
            Assertions.assertEquals(2, ranges.size());
            Assertions.assertEquals(Set.of(legacyData, table.location() + "//data/new.parquet"),
                    ranges.stream().map(range -> range.getPath().orElseThrow()).collect(Collectors.toSet()));
        }

        // Expiration removes the source list, while later snapshots still retain its manifests.
        table.expireSnapshots().expireSnapshotId(legacySnapshot).cleanExpiredFiles(false).commit();
        long firstV4Snapshot = table.currentSnapshot().snapshotId();
        table.newFastAppend().appendFile(data(table, "data/latest.parquet")).commit();
        table.expireSnapshots().expireSnapshotId(firstV4Snapshot).cleanExpiredFiles(false).commit();
        Table expired = coldMetadata(table);
        Assertions.assertNull(expired.snapshot(legacySnapshot));
        Assertions.assertNull(expired.snapshot(firstV4Snapshot));
        Table expiredView = IcebergSnapshotTableOperations.v4ReadTable(expired);
        FileIO restored = SerializationUtil.deserializeFromBase64(
                SerializationUtil.serializeToBase64(expiredView.io()));
        for (ManifestFile manifest : expired.currentSnapshot().allManifests(restored)) {
            try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, restored, expired.specs())) {
                Assertions.assertTrue(reader.iterator().hasNext());
            }
        }
        for (boolean cached : new boolean[] {false, true}) {
            List<ConnectorScanRange> ranges = provider(expired, cached).planScan(null, request());
            Assertions.assertEquals(Set.of(legacyData, table.location() + "//data/new.parquet",
                            table.location() + "/data/latest.parquet"),
                    ranges.stream().map(range -> range.getPath().orElseThrow()).collect(Collectors.toSet()));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void kerberosUpgradesKeepMetadataProbesAuthenticated(int originalVersion) throws Exception {
        Table table = createTable(originalVersion);
        table.updateLocation().setLocation("hdfs://namenode.example/warehouse/table").commit();
        table.updateProperties().set(TableProperties.WRITE_METADATA_LOCATION,
                "/metadata/" + tempDir.getFileName()).commit();
        table.newFastAppend().appendFile(data(table, "/data/legacy.parquet")).commit();
        table.updateProperties().set(TableProperties.FORMAT_VERSION, "4").commit();
        for (boolean cached : new boolean[] {false, true}) {
            RecordingAuthenticator auth = new RecordingAuthenticator();
            Table cold = coldMetadata(table);
            TableMetadata metadata = ((BaseTable) cold).operations().current();
            AtomicInteger reads = new AtomicInteger();
            FileIO guarded = new FileIO() {
                @Override
                public InputFile newInputFile(String path) {
                    Assertions.assertTrue(auth.active.get(), "manifest factories and probes must retain doAs");
                    reads.incrementAndGet();
                    return cold.io().newInputFile(path);
                }

                @Override
                public OutputFile newOutputFile(String path) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void deleteFile(String path) {
                    throw new UnsupportedOperationException();
                }
            };
            Table secured = new BaseTable(new StaticTableOperations(metadata, guarded), table.name());
            RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
            ops.table = secured;
            TcclPinningConnectorContext context = new TcclPinningConnectorContext(new RecordingConnectorContext(),
                    getClass().getClassLoader(), () -> auth);
            IcebergScanPlanProvider provider = new IcebergScanPlanProvider(IcebergCatalogProperties.of(
                    Collections.singletonMap("meta.cache.iceberg.manifest.enable", Boolean.toString(cached))),
                    ops, context, cached ? new IcebergManifestCache() : null);
            for (int attempt = 0; attempt < 2; attempt++) {
                List<ConnectorScanRange> ranges = provider.planScan(null, request());
                Assertions.assertEquals(1, ranges.size());
                Assertions.assertEquals("/data/legacy.parquet", ranges.get(0).getPath().orElseThrow());
                try (ConnectorSplitSource source = provider.streamSplits(null, request())) {
                    Assertions.assertTrue(source.hasNext());
                    Assertions.assertEquals("/data/legacy.parquet", source.next().getPath().orElseThrow());
                    Assertions.assertFalse(source.hasNext());
                }
            }
            Assertions.assertTrue(reads.get() > 0);
            Assertions.assertFalse(auth.active.get());
        }
    }

    @ParameterizedTest
    @CsvSource({"2,false,false,direct", "2,false,false,cache", "2,false,false,tracked",
            "2,false,true,direct", "2,false,true,cache", "2,false,true,tracked",
            "2,true,false,direct", "2,true,false,cache", "2,true,false,tracked",
            "2,true,true,direct", "2,true,true,cache", "2,true,true,tracked",
            "3,false,false,direct", "3,false,false,cache", "3,false,false,tracked",
            "3,false,true,direct", "3,false,true,cache", "3,false,true,tracked",
            "3,true,false,direct", "3,true,false,cache", "3,true,false,tracked",
            "3,true,true,direct", "3,true,true,cache", "3,true,true,tracked"})
    void sharedStatementReaderKeepsV4ProvenanceForEveryScan(int originalVersion, boolean authenticated,
            boolean warmCache, String tableSource) throws Exception {
        Table table = createTable(originalVersion);
        table.updateLocation().setLocation("hdfs://namenode.example/warehouse/table").commit();
        table.updateProperties().set(TableProperties.WRITE_METADATA_LOCATION,
                "/metadata/" + tempDir.getFileName()).commit();
        table.newFastAppend().appendFile(data(table, "/data/legacy.parquet")).commit();
        table.updateProperties().set(TableProperties.FORMAT_VERSION, "4").commit();
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = coldMetadata(table);
        RecordingAuthenticator auth = new RecordingAuthenticator();
        TcclPinningConnectorContext context = authenticated
                ? new TcclPinningConnectorContext(new RecordingConnectorContext(), getClass().getClassLoader(), () -> auth)
                : null;
        IcebergTableCache tableCache = tableSource.equals("cache") ? new IcebergTableCache(60, 16) : null;
        IcebergCatalogResourceTracker tracker = tableSource.equals("tracked") ? new IcebergCatalogResourceTracker() : null;
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(IcebergCatalogProperties.of(
                Map.of("meta.cache.iceberg.manifest.enable", "true")), ignored -> ops, context,
                new IcebergManifestCache(), tableCache, tracker);
        if (warmCache) {
            Assertions.assertEquals(1, provider.planScan(null, request()).size());
        }
        TestStatementScope scope = new TestStatementScope();
        ConnectorSession session = new IcebergStatementScopeTest.ScopeSession(1, "v4-shared-reader", scope);
        try {
            for (int value : new int[] {1, 2, 1}) {
                ConnectorScanRequest request = ConnectorScanRequest.builder(new IcebergTableHandle("db", "table"),
                        Collections.emptyList()).filter(Optional.of(new ConnectorComparison(
                                ConnectorComparison.Operator.EQ, new ConnectorColumnRef("id", ConnectorType.of("INT")),
                                ConnectorLiteral.ofInt(value)))).build();
                List<ConnectorScanRange> ranges = provider.planScan(session, request);
                Assertions.assertEquals(1, ranges.size());
                Assertions.assertEquals("/data/legacy.parquet", ranges.get(0).getPath().orElseThrow());
            }
        } finally {
            scope.closeAll();
            if (tableCache != null) {
                tableCache.close();
            }
            if (tracker != null) {
                tracker.close(() -> { });
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void metadataDeleteWithRelativeLocationsRejectsUnsupportedDml(boolean withDeletes) throws Exception {
        Table table = createTable(4);
        DataFile file = data(table, "data/first.parquet");
        table.newFastAppend().appendFile(file).commit();
        if (withDeletes) {
            table.newRowDelta().addDeletes(FileMetadata.deleteFileBuilder(table.spec()).ofPositionDeletes()
                    .withPath("data/deletes.puffin").withFormat(FileFormat.PUFFIN)
                    .withReferencedDataFile(file.location()).withContentOffset(16).withContentSizeInBytes(64)
                    .withFileSizeInBytes(128).withRecordCount(2).build()).commit();
        }
        Table relative = relativeMetadata(table);
        long snapshot = relative.currentSnapshot().snapshotId();
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = relative;
        IcebergConnectorMetadata metadata = new IcebergConnectorMetadata(ops,
                IcebergCatalogProperties.of(Collections.emptyMap()), new RecordingConnectorContext());
        RuntimeException failure = Assertions.assertThrows(RuntimeException.class, () -> metadata.deleteUsingMetadata(null,
                new IcebergTableHandle("db", "table"), Optional.empty(), null, () -> false));
        Assertions.assertTrue(failure.getMessage().contains("format v4"));
        Assertions.assertEquals(snapshot, relative.currentSnapshot().snapshotId());
        Assertions.assertEquals(1, provider(relative, false).planScan(null, request()).size());
    }

    @ParameterizedTest
    @ValueSource(strings = {"INSERT", "OVERWRITE", "DELETE", "UPDATE", "MERGE", "REWRITE"})
    void v4DataWriteRefusesBeforeOpeningTheRawTransaction(String operation) throws Exception {
        Table table = createTable(4);
        table.newFastAppend().appendFile(data(table, "data/first.parquet")).commit();
        long snapshot = table.currentSnapshot().snapshotId();
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = relativeMetadata(table);
        IcebergConnectorTransaction transaction = new IcebergConnectorTransaction(1, ops, new RecordingConnectorContext());
        TestStatementScope scope = new TestStatementScope();
        ConnectorSession session = new IcebergStatementScopeTest.ScopeSession(1, "v4-write", scope);
        WriteOperation op = WriteOperation.valueOf(operation);
        try {
            DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                    () -> transaction.beginWrite(session, "db", "table", new IcebergWriteContext(op,
                            op == WriteOperation.OVERWRITE, Collections.emptyMap(), Optional.empty())));
            Assertions.assertTrue(error.getMessage().contains("format v4"));
            Assertions.assertEquals(snapshot, table.currentSnapshot().snapshotId());
        } finally {
            transaction.rollback();
            scope.closeAll();
        }
    }

    @ParameterizedTest
    @CsvSource({"2,false", "2,true", "3,false", "3,true"})
    void upgradeThenRewriteRefusesBeforeChangingLegacyPathSemantics(int originalVersion, boolean authenticated)
            throws Exception {
        Table table = createTable(originalVersion);
        table.newFastAppend().appendFile(data(table, "/data/legacy.parquet")).commit();
        table.updateProperties().set(TableProperties.FORMAT_VERSION, "4").commit();
        long snapshot = table.currentSnapshot().snapshotId();
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = table;
        IcebergProcedureOps procedures = new IcebergProcedureOps(Collections.emptyMap(), ops,
                authenticated ? new RecordingConnectorContext() : null);
        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                () -> procedures.execute(null, new IcebergTableHandle("db", "table"), "rewrite_manifests",
                        Collections.emptyMap(), null, Collections.emptyList()));
        Assertions.assertTrue(error.getMessage().contains("format v4"));
        Assertions.assertEquals(snapshot, table.currentSnapshot().snapshotId());
        Assertions.assertEquals("/data/legacy.parquet", provider(table, false).planScan(null, request()).get(0)
                .getPath().orElseThrow());
    }

    private static final class RecordingAuthenticator implements HadoopAuthenticator {
        private final ThreadLocal<Boolean> active = ThreadLocal.withInitial(() -> false);

        @Override
        public UserGroupInformation getUGI() {
            throw new UnsupportedOperationException("doAs is supplied by the test");
        }

        @Override
        public <T> T doAs(PrivilegedExceptionAction<T> action) throws IOException {
            boolean previous = active.get();
            active.set(true);
            try {
                return action.run();
            } catch (IOException | RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            } finally {
                active.set(previous);
            }
        }
    }

    private static Table coldMetadata(Table table) {
        TableMetadata metadata = ((BaseTable) table).operations().current();
        String json = TableMetadataParser.toJson(metadata);
        return new BaseTable(new StaticTableOperations(
                TableMetadataParser.fromJson(metadata.metadataFileLocation(), json), table.io()), table.name());
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void slashLeadingV4MetadataReferencesAreRelative(boolean relativeList) throws Exception {
        Table table = createTable(4);
        String path = "/metadata/leading.avro";
        ManifestWriter<DataFile> writer = ManifestFiles.write(4, table.spec(),
                table.io().newOutputFile(table.location() + "/" + path), null);
        try (ManifestWriter<DataFile> closeable = writer) {
            closeable.add(data(table, "data/leading.parquet"));
        }
        GenericManifestFile manifest = (GenericManifestFile) writer.toManifestFile().copy();
        manifest.put(0, path);
        table.newFastAppend().appendManifest(manifest).commit();
        // An unrelated filesystem-rooted object must not change the meaning of the V4 reference.
        try (ManifestWriter<DataFile> unrelated = ManifestFiles.write(3, table.spec(),
                table.io().newOutputFile(path), table.currentSnapshot().snapshotId())) {
            unrelated.add(data(table, "s3://example-bucket/unrelated.parquet"));
        }
        Table cold = coldMetadata(table);
        if (relativeList) {
            String original = table.currentSnapshot().manifestListLocation();
            String relative = "/metadata/leading-list.avro";
            try (org.apache.iceberg.io.SeekableInputStream input = table.io().newInputFile(original).newStream();
                    OutputStream output = table.io().newOutputFile(table.location() + "/" + relative).create()) {
                input.transferTo(output);
            }
            TableMetadata metadata = ((BaseTable) table).operations().current();
            String json = TableMetadataParser.toJson(metadata).replace(original, relative);
            String metadataLocation = table.location() + "/metadata/leading.metadata.json";
            try (OutputStream output = table.io().newOutputFile(metadataLocation).create()) {
                output.write(json.getBytes(StandardCharsets.UTF_8));
            }
            cold = new BaseTable(new StaticTableOperations(
                    TableMetadataParser.fromJson(metadataLocation, json), table.io()), table.name());
        }
        for (boolean cached : new boolean[] {false, true}) {
            IcebergScanPlanProvider provider = provider(cold, cached);
            for (int attempt = 0; attempt < 2; attempt++) {
                List<ConnectorScanRange> ranges = provider.planScan(null, request());
                Assertions.assertEquals(1, ranges.size());
                Assertions.assertEquals(table.location() + "/data/leading.parquet",
                        ranges.get(0).getPath().orElseThrow());
            }
        }
        Table view = IcebergSnapshotTableOperations.v4ReadTable(cold);
        ManifestFile descriptor = view.currentSnapshot().allManifests(view.io()).get(0);
        FileIO noReads = new FileIO() {
            @Override
            public InputFile newInputFile(String location) {
                throw new AssertionError("Cached-only probes must not perform storage IO");
            }

            @Override
            public OutputFile newOutputFile(String location) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void deleteFile(String location) {
                throw new UnsupportedOperationException();
            }
        };
        IcebergManifestCache cache = new IcebergManifestCache();
        ManifestCacheValue cachedValue = cache.getManifestCacheValue(descriptor, view);
        Table probeView = new BaseTable(new StaticTableOperations(((BaseTable) view).operations().current(),
                ((IcebergV4FileIO) view.io()).withDelegate(noReads)), view.name());
        Assertions.assertSame(cachedValue, cache.getCachedManifestValue(descriptor, probeView, null));
        Table detached = new BaseTable(new StaticTableOperations(((BaseTable) view).operations().current(),
                new IcebergV4FileIO(noReads, view.location(), ((BaseTable) view).operations().current())), view.name());
        Assertions.assertNull(cache.getCachedManifestValue(descriptor, detached, null));
        FileIO restored = SerializationUtil.deserializeFromBase64(SerializationUtil.serializeToBase64(view.io()));
        ManifestFile loaded = coldMetadata(cold).currentSnapshot().allManifests(restored).get(0);
        try (ManifestReader<DataFile> reader = ManifestFiles.read(loaded, restored, view.specs())) {
            DataFile file = reader.iterator().next();
            Assertions.assertEquals(table.location() + "/data/leading.parquet",
                    ((IcebergV4FileIO) restored).resolveContentFile(file));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void sharedManifestAliasesKeepTheirOwnProvenance(boolean reverse) throws Exception {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("aliases", Collections.singletonMap(CatalogProperties.WAREHOUSE_LOCATION,
                tempDir.toUri().toString()));
        catalog.createNamespace(Namespace.of("db"));
        List<Table> tables = new ArrayList<>();
        for (String name : List.of("left", "right")) {
            Table table = catalog.createTable(TableIdentifier.of("db", name),
                    new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
                    PartitionSpec.unpartitioned(), null,
                    Collections.singletonMap(TableProperties.FORMAT_VERSION, "4"));
            tables.add(table);
        }
        String physical = tables.get(0).location() + "/metadata/shared.avro";
        ManifestWriter<DataFile> writer = ManifestFiles.write(4, tables.get(0).spec(),
                tables.get(0).io().newOutputFile(physical), null);
        try (ManifestWriter<DataFile> closeable = writer) {
            closeable.add(data(tables.get(0), "data/shared.parquet"));
        }
        GenericManifestFile relative = (GenericManifestFile) writer.toManifestFile().copy();
        relative.put(0, "metadata/shared.avro");
        tables.get(0).newFastAppend().appendManifest(relative).commit();
        tables.get(1).newFastAppend().appendManifest(writer.toManifestFile()).commit();
        if (reverse) {
            Collections.reverse(tables);
        }
        IcebergManifestCache cache = new IcebergManifestCache();
        for (int attempt = 0; attempt < 2; attempt++) {
            for (Table table : tables) {
                Table view = IcebergSnapshotTableOperations.v4ReadTable(coldMetadata(table));
                ManifestFile manifest = view.currentSnapshot().allManifests(view.io()).get(0);
                ManifestCacheValue value = cache.getManifestCacheValue(manifest, view);
                DataFile file = value.getDataFiles().get(0);
                Assertions.assertEquals(manifest.path(), file.manifestLocation());
                Assertions.assertEquals(table.location() + "/data/shared.parquet",
                        ((IcebergV4FileIO) view.io()).resolveContentFile(file));
                Assertions.assertSame(value, cache.getCachedManifestValue(manifest, view, null));
            }
        }
    }

    @Test
    void readViewKeepsSnapshotHistoryLazyUntilItIsNeeded() throws IOException {
        Table table = createTable(4);
        String metadataLocation = table.location() + "/metadata/lazy.metadata.json";
        String json = TableMetadataParser.toJson(((BaseTable) table).operations().current());
        try (OutputStream output = table.io().newOutputFile(metadataLocation).create()) {
            output.write(json.getBytes(StandardCharsets.UTF_8));
        }
        TableMetadata original = TableMetadataParser.fromJson(metadataLocation, json);
        AtomicInteger loads = new AtomicInteger();
        TableMetadata lazy = TableMetadata.buildFrom(original).setSnapshotsSupplier(() -> {
            loads.incrementAndGet();
            return original.snapshots();
        }).withMetadataLocation(metadataLocation).build();
        Table view = IcebergSnapshotTableOperations.v4ReadTable(new BaseTable(
                new StaticTableOperations(lazy, table.io()), table.name()));
        view.io().properties();
        Assertions.assertEquals(0, loads.get());
        SerializationUtil.serializeToBase64(view.io());
        Assertions.assertEquals(0, loads.get());
    }

    @Test
    void sharedManifestBytesKeepDescriptorInheritanceSeparate() throws Exception {
        Table table = createTable(4);
        ManifestWriter<DataFile> writer = ManifestFiles.write(4, table.spec(),
                table.io().newOutputFile(table.location() + "/metadata/inherited.avro"), null);
        try (ManifestWriter<DataFile> closeable = writer) {
            closeable.add(data(table, "data/inherited.parquet"));
        }
        Table view = IcebergSnapshotTableOperations.v4ReadTable(table);
        IcebergManifestCache cache = new IcebergManifestCache();
        for (long sequence : new long[] {1, 2, 1}) {
            GenericManifestFile manifest = (GenericManifestFile) writer.toManifestFile().copy();
            manifest.put(4, sequence);
            manifest.put(6, sequence * 10);
            manifest.put(15, sequence * 100);
            DataFile file = cache.getManifestCacheValue(manifest, view).getDataFiles().get(0);
            Assertions.assertEquals(sequence, file.dataSequenceNumber());
            Assertions.assertEquals(sequence, file.fileSequenceNumber());
            Assertions.assertEquals(sequence * 100, file.firstRowId());
        }
    }

    @Test
    void overlappingStatementReadsKeepV4DescriptorInheritanceSeparate() throws Exception {
        Table table = createTable(4);
        ManifestWriter<DataFile> writer = ManifestFiles.write(4, table.spec(),
                table.io().newOutputFile(table.location() + "/metadata/overlap.avro"), null);
        try (ManifestWriter<DataFile> closeable = writer) {
            closeable.add(data(table, "data/inherited.parquet"));
        }
        GenericManifestFile first = (GenericManifestFile) writer.toManifestFile().copy();
        GenericManifestFile second = (GenericManifestFile) writer.toManifestFile().copy();
        first.put(4, 1L);
        first.put(6, 10L);
        first.put(15, 100L);
        second.put(4, 2L);
        second.put(6, 20L);
        second.put(15, 200L);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger opens = new AtomicInteger();
        FileIO observed = new FileIO() {
            @Override
            public Map<String, String> properties() {
                return table.io().properties();
            }

            @Override
            public InputFile newInputFile(String path) {
                if (opens.incrementAndGet() == 1) {
                    entered.countDown();
                    try {
                        Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }
                return table.io().newInputFile(path);
            }

            @Override
            public OutputFile newOutputFile(String path) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void deleteFile(String path) {
                throw new UnsupportedOperationException();
            }
        };
        Table view = IcebergSnapshotTableOperations.v4ReadTable(new BaseTable(new StaticTableOperations(
                ((BaseTable) table).operations().current(), observed), table.name()));
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try (IcebergManifestReader reader = new IcebergManifestReader(
                new IcebergManifestCache(), view, "overlap", () -> { })) {
            Future<ManifestCacheValue> initial = workers.submit(() -> reader.read(first));
            try {
                Assertions.assertTrue(entered.await(10, TimeUnit.SECONDS));
                Future<CompletionStage<ManifestCacheValue>> next = workers.submit(() -> reader.readFuture(second));
                CompletionStage<ManifestCacheValue> pending = next.get(10, TimeUnit.SECONDS);
                release.countDown();
                DataFile one = initial.get(10, TimeUnit.SECONDS).getDataFiles().get(0);
                DataFile two = pending.toCompletableFuture().get(10, TimeUnit.SECONDS).getDataFiles().get(0);
                Assertions.assertEquals(1L, one.dataSequenceNumber());
                Assertions.assertEquals(100L, one.firstRowId());
                Assertions.assertEquals(2L, two.dataSequenceNumber());
                Assertions.assertEquals(2L, two.fileSequenceNumber());
                Assertions.assertEquals(200L, two.firstRowId());
            } finally {
                release.countDown();
            }
        } finally {
            workers.shutdownNow();
            Assertions.assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void slashLeadingMetadataJsonPrefersTheTableRelativeObject(boolean rejectSchemeLess) throws Exception {
        Table table = createTable(4);
        String reference = "/metadata/current.json";
        String json = TableMetadataParser.toJson(((BaseTable) table).operations().current());
        ObjectNode unrelated = (ObjectNode) JsonUtil.mapper().readTree(json);
        unrelated.put("table-uuid", "00000000-0000-0000-0000-000000000001");
        try (OutputStream output = table.io().newOutputFile(reference).createOrOverwrite()) {
            output.write(unrelated.toString().getBytes(StandardCharsets.UTF_8));
        }
        try (OutputStream output = table.io().newOutputFile(table.location() + "/" + reference).createOrOverwrite()) {
            output.write(json.getBytes(StandardCharsets.UTF_8));
        }
        FileIO delegate = new FileIO() {
            @Override
            public Map<String, String> properties() {
                return table.io().properties();
            }

            @Override
            public InputFile newInputFile(String path) {
                if (rejectSchemeLess && path.startsWith("/")) {
                    throw new IllegalArgumentException("A URI scheme is required");
                }
                return table.io().newInputFile(path);
            }

            @Override
            public OutputFile newOutputFile(String path) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void deleteFile(String path) {
                throw new UnsupportedOperationException();
            }
        };
        Table view = IcebergSnapshotTableOperations.v4ReadTable(new BaseTable(new StaticTableOperations(
                TableMetadataParser.fromJson(reference, json), delegate), table.name()));
        try (org.apache.iceberg.io.SeekableInputStream input = view.io().newInputFile(reference).newStream()) {
            Assertions.assertArrayEquals(json.getBytes(StandardCharsets.UTF_8), input.readAllBytes());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void rootedMetadataJsonFallbackRequiresTheFrozenGeneration(boolean matching) throws Exception {
        Table table = createTable(4);
        String reference = "/metadata/rooted.metadata.json";
        String json = TableMetadataParser.toJson(((BaseTable) table).operations().current());
        ObjectNode candidate = (ObjectNode) JsonUtil.mapper().readTree(json);
        if (!matching) {
            ((ObjectNode) candidate.get("properties")).put("fixture", "different");
        }
        try (OutputStream output = table.io().newOutputFile(reference).createOrOverwrite()) {
            output.write(candidate.toString().getBytes(StandardCharsets.UTF_8));
        }
        Table view = IcebergSnapshotTableOperations.v4ReadTable(new BaseTable(new StaticTableOperations(
                TableMetadataParser.fromJson(reference, json), table.io()), table.name()));
        if (matching) {
            try (org.apache.iceberg.io.SeekableInputStream input = view.io().newInputFile(reference).newStream()) {
                Assertions.assertArrayEquals(candidate.toString().getBytes(StandardCharsets.UTF_8), input.readAllBytes());
            }
        } else {
            Assertions.assertThrows(org.apache.iceberg.exceptions.NotFoundException.class,
                    () -> view.io().newInputFile(reference));
        }
    }

    @ParameterizedTest
    @CsvSource({"2,avro", "2,parquet", "3,avro", "3,parquet"})
    void qualifiedLegacyEntriesRemainReadableAfterV4Rewrite(int originalVersion, String encoding) throws Exception {
        Table table = createTable(originalVersion);
        table.newFastAppend().appendFile(data(table, "/data/legacy.parquet")).commit();
        long snapshot = table.currentSnapshot().snapshotId();
        ManifestFile original = table.currentSnapshot().dataManifests(table.io()).get(0);
        DataFile legacy;
        try (ManifestReader<DataFile> reader = ManifestFiles.read(original, table.io(), table.specs())) {
            legacy = reader.iterator().next().copy();
        }
        table.updateProperties().set(TableProperties.FORMAT_VERSION, "4").commit();
        // V4 writers must qualify inherited scheme-less paths before copying them to a V4 manifest.
        DataFile qualified = DataFiles.builder(table.spec()).copy(legacy)
                .withPath("file:///data/legacy.parquet").build();
        ManifestWriter<DataFile> writer = ManifestFiles.write(4, table.spec(), table.io().newOutputFile(
                table.location() + "/metadata/qualified." + encoding), null);
        try (ManifestWriter<DataFile> closeable = writer) {
            closeable.existing(qualified, snapshot, legacy.dataSequenceNumber(), legacy.fileSequenceNumber());
        }
        table.rewriteManifests().deleteManifest(original).addManifest(writer.toManifestFile()).commit();
        for (boolean cached : new boolean[] {false, true}) {
            List<ConnectorScanRange> ranges = provider(coldMetadata(table), cached).planScan(null, request());
            Assertions.assertEquals(1, ranges.size());
            Assertions.assertEquals("file:///data/legacy.parquet", ranges.get(0).getPath().orElseThrow());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void rootedV4RewriteRemainsReadableAfterSnapshotExpiration(boolean inlineMetadata) throws Exception {
        Table table = createTable(4);
        table.updateProperties().set(TableProperties.WRITE_METADATA_LOCATION,
                "/metadata/" + tempDir.getFileName()).commit();
        for (String name : List.of("first", "second")) {
            table.newFastAppend().appendFile(data(table, "data/" + name + ".parquet")).commit();
        }
        table.rewriteManifests().clusterBy(file -> 0).rewriteIf(manifest -> true).commit();
        ManifestFile rewritten = table.currentSnapshot().dataManifests(table.io()).get(0);
        Assertions.assertEquals(0, rewritten.addedFilesCount());
        Assertions.assertEquals(2, rewritten.existingFilesCount());
        table.newFastAppend().appendFile(data(table, "data/third.parquet")).commit();
        org.apache.iceberg.ExpireSnapshots expiration = table.expireSnapshots().cleanExpiredFiles(false);
        for (org.apache.iceberg.Snapshot snapshot : table.snapshots()) {
            if (snapshot.snapshotId() != table.currentSnapshot().snapshotId()) {
                expiration.expireSnapshotId(snapshot.snapshotId());
            }
        }
        expiration.commit();
        Table cold = coldMetadata(table);
        if (inlineMetadata) {
            cold = new BaseTable(new StaticTableOperations(TableMetadataParser.fromJson(TableMetadataParser.toJson(
                    ((BaseTable) cold).operations().current())), table.io()), table.name());
        }
        Assertions.assertNull(cold.snapshot(rewritten.snapshotId()));
        Table view = IcebergSnapshotTableOperations.v4ReadTable(cold);
        FileIO restored = SerializationUtil.deserializeFromBase64(SerializationUtil.serializeToBase64(view.io()));
        Set<String> expected = Set.of(table.location() + "/data/first.parquet",
                table.location() + "/data/second.parquet", table.location() + "/data/third.parquet");
        Set<String> actual = new java.util.HashSet<>();
        for (ManifestFile manifest : coldMetadata(cold).currentSnapshot().allManifests(restored)) {
            try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, restored, cold.specs())) {
                for (DataFile file : reader) {
                    actual.add(((IcebergV4FileIO) restored).resolveContentFile(file));
                }
            }
        }
        Assertions.assertEquals(expected, actual);
        for (boolean cached : new boolean[] {false, true}) {
            IcebergScanPlanProvider provider = provider(cold, cached);
            for (int attempt = 0; attempt < 2; attempt++) {
                Assertions.assertEquals(expected, provider.planScan(null, request()).stream()
                        .map(range -> range.getPath().orElseThrow()).collect(Collectors.toSet()));
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"2,false", "2,true", "3,false", "3,true", "4,false", "4,true"})
    void retainedRootedManifestsSurviveDirectoryMovesAndMissingBase(int originalVersion, boolean missingBase)
            throws Exception {
        Table table = createTable(originalVersion);
        table.updateProperties().set(TableProperties.WRITE_METADATA_LOCATION,
                "/metadata/old-" + tempDir.getFileName()).commit();
        String retainedData = originalVersion < 4 ? "/data/" : "s3://example-bucket/data/";
        for (String name : List.of("first", "second")) {
            table.newFastAppend().appendFile(data(table, retainedData + name + ".parquet")).commit();
        }
        table.rewriteManifests().clusterBy(file -> 0).commit();
        ManifestFile retained = table.currentSnapshot().dataManifests(table.io()).get(0);
        Assertions.assertEquals(0, retained.addedFilesCount());
        Assertions.assertEquals(2, retained.existingFilesCount());
        table.updateProperties().set(TableProperties.FORMAT_VERSION, "4")
                .set(TableProperties.WRITE_METADATA_LOCATION, table.location() + "/new-metadata").commit();
        table.newFastAppend().appendFile(data(table, "s3://example-bucket/data/third.parquet")).commit();
        org.apache.iceberg.ExpireSnapshots expiration = table.expireSnapshots().cleanExpiredFiles(false);
        for (org.apache.iceberg.Snapshot snapshot : table.snapshots()) {
            if (snapshot.snapshotId() != table.currentSnapshot().snapshotId()) {
                expiration.expireSnapshotId(snapshot.snapshotId());
            }
        }
        expiration.commit();
        TableMetadata metadata = ((BaseTable) table).operations().current();
        ObjectNode json = (ObjectNode) JsonUtil.mapper().readTree(TableMetadataParser.toJson(metadata));
        if (missingBase) {
            json.remove("location");
        }
        Table cold = new BaseTable(new StaticTableOperations(TableMetadataParser.fromJson(
                metadata.metadataFileLocation(), json.toString()), table.io()), table.name());
        Assertions.assertNull(cold.snapshot(retained.snapshotId()));
        Set<String> expected = Set.of(retainedData + "first.parquet", retainedData + "second.parquet",
                "s3://example-bucket/data/third.parquet");
        Table view = IcebergSnapshotTableOperations.v4ReadTable(cold);
        FileIO restored = SerializationUtil.deserializeFromBase64(SerializationUtil.serializeToBase64(view.io()));
        Set<String> actual = new java.util.HashSet<>();
        for (ManifestFile manifest : cold.currentSnapshot().allManifests(restored)) {
            try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, restored, cold.specs())) {
                for (DataFile file : reader) {
                    actual.add(((IcebergV4FileIO) restored).resolveContentFile(file));
                }
            }
        }
        Assertions.assertEquals(expected, actual);
        for (boolean cached : new boolean[] {false, true}) {
            IcebergScanPlanProvider provider = provider(cold, cached);
            for (int attempt = 0; attempt < 2; attempt++) {
                Assertions.assertEquals(expected, provider.planScan(null, request()).stream()
                        .map(range -> range.getPath().orElseThrow()).collect(Collectors.toSet()));
            }
        }
    }

    @Test
    void retainedRootedManifestRequiresItsDescriptorLength() throws Exception {
        Table table = createTable(4);
        table.updateProperties().set(TableProperties.WRITE_METADATA_LOCATION,
                "/metadata/" + tempDir.getFileName()).commit();
        table.newFastAppend().appendFile(data(table, "data/first.parquet")).commit();
        ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
        IcebergV4FileIO io = new IcebergV4FileIO(table.io(), table.location());
        Assertions.assertTrue(io.newInputFile(manifest).exists());
        GenericManifestFile invalid = (GenericManifestFile) manifest.copy();
        invalid.put(1, manifest.length() + 1);
        IcebergV4FileIO fresh = new IcebergV4FileIO(table.io(), table.location());
        IllegalArgumentException failure = Assertions.assertThrows(IllegalArgumentException.class,
                () -> fresh.newInputFile(invalid));
        Assertions.assertTrue(failure.getMessage().contains("length"));
    }

    @Test
    void missingRelativeV4ManifestDoesNotReadAnUnrelatedRootedFile() throws Exception {
        Table table = createTable(4);
        String path = "/metadata/leading.parquet";
        ManifestWriter<DataFile> writer = ManifestFiles.write(4, table.spec(),
                table.io().newOutputFile(table.location() + "/" + path), null);
        try (ManifestWriter<DataFile> closeable = writer) {
            closeable.add(data(table, "data/first.parquet"));
        }
        GenericManifestFile manifest = (GenericManifestFile) writer.toManifestFile().copy();
        manifest.put(0, path);
        table.newFastAppend().appendManifest(manifest).commit();
        try (org.apache.iceberg.io.SeekableInputStream input =
                        table.io().newInputFile(table.location() + "/" + path).newStream();
                OutputStream output = table.io().newOutputFile(path).create()) {
            input.transferTo(output);
        }
        Table readable = IcebergSnapshotTableOperations.v4ReadTable(coldMetadata(table));
        ManifestFile valid = readable.currentSnapshot().dataManifests(readable.io()).get(0);
        try (ManifestReader<DataFile> reader = ManifestFiles.read(valid, readable.io(), readable.specs())) {
            Assertions.assertEquals(10L, reader.iterator().next().recordCount());
        }
        table.io().deleteFile(table.location() + "/" + path);
        Table cold = coldMetadata(table);
        IcebergV4FileIO io = (IcebergV4FileIO) IcebergSnapshotTableOperations.v4ReadTable(cold).io();
        ManifestFile descriptor = cold.currentSnapshot().dataManifests(io).get(0);
        Assertions.assertThrows(org.apache.iceberg.exceptions.NotFoundException.class,
                () -> io.newInputFile(descriptor));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void serializedMetadataReferencesDoNotReopenAnUnresolvedJsonPointer(boolean leadingSlash) throws Exception {
        Table table = createTable(4);
        table.updateProperties().set(TableProperties.WRITE_METADATA_LOCATION,
                "/metadata/root-" + tempDir.getFileName()).commit();
        table.newFastAppend().appendFile(data(table, "s3://example-bucket/data/first.parquet")).commit();
        String reference = (leadingSlash ? "/" : "") + "metadata/relative.metadata.json";
        String json = TableMetadataParser.toJson(((BaseTable) table).operations().current());
        try (OutputStream output = table.io().newOutputFile(table.location() + "/" + reference).create()) {
            output.write(json.getBytes(StandardCharsets.UTF_8));
        }
        Table cold = new BaseTable(new StaticTableOperations(
                TableMetadataParser.fromJson(reference, json), table.io()), table.name());
        Table view = IcebergSnapshotTableOperations.v4ReadTable(cold);
        FileIO restored = SerializationUtil.deserializeFromBase64(SerializationUtil.serializeToBase64(view.io()));
        try (org.apache.iceberg.io.SeekableInputStream input = restored.newInputFile(reference).newStream()) {
            Assertions.assertArrayEquals(json.getBytes(StandardCharsets.UTF_8), input.readAllBytes());
        }
        ManifestFile manifest = cold.currentSnapshot().allManifests(restored).get(0);
        try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, restored, cold.specs())) {
            Assertions.assertEquals("s3://example-bucket/data/first.parquet", reader.iterator().next().location());
        }
    }

    @Test
    void serializedInlineReadViewDoesNotGrowWithSnapshotHistory() throws Exception {
        Table table = createTable(4);
        table.newFastAppend().appendFile(data(table, "s3://example-bucket/data/first.parquet")).commit();
        ObjectNode json = (ObjectNode) JsonUtil.mapper().readTree(
                TableMetadataParser.toJson(((BaseTable) table).operations().current()));
        int compact = inlineViewSize(json, table.name());
        int compactTask = inlineTaskSize(json, table);
        com.fasterxml.jackson.databind.node.ArrayNode snapshots =
                (com.fasterxml.jackson.databind.node.ArrayNode) json.get("snapshots");
        ObjectNode template = (ObjectNode) snapshots.get(0);
        for (int i = 1; i <= 2000; i++) {
            ObjectNode snapshot = template.deepCopy();
            snapshot.put("snapshot-id", i);
            snapshot.put("manifest-list", "/metadata/history/snapshot-" + i + ".avro");
            snapshots.add(snapshot);
        }
        int large = inlineViewSize(json, table.name());
        int largeTask = inlineTaskSize(json, table);
        Assertions.assertTrue(large <= compact + 1024,
                "Each JNI task must carry its read context, not the table's entire snapshot history: "
                        + compact + " -> " + large);
        Assertions.assertTrue(largeTask <= compactTask + 1024,
                "The complete metadata task must not amplify inline snapshot history: "
                        + compactTask + " -> " + largeTask);
    }

    private static int inlineViewSize(ObjectNode json, String name) {
        TableMetadata metadata = TableMetadataParser.fromJson(json.toString());
        Table table = new BaseTable(new StaticTableOperations(metadata,
                new org.apache.iceberg.inmemory.InMemoryFileIO()), name);
        return SerializationUtil.serializeToBase64(IcebergSnapshotTableOperations.v4ReadTable(table).io()).length();
    }

    private static int inlineTaskSize(ObjectNode json, Table source) throws IOException {
        Table table = IcebergSnapshotTableOperations.v4ReadTable(new BaseTable(new StaticTableOperations(
                TableMetadataParser.fromJson(json.toString()), source.io()), source.name()));
        Table files = org.apache.iceberg.MetadataTableUtils.createMetadataTableInstance(
                table, org.apache.iceberg.MetadataTableType.FILES);
        int length = 0;
        int count = 0;
        try (CloseableIterable<FileScanTask> tasks = files.newScan().select("record_count").planFiles()) {
            for (FileScanTask task : tasks) {
                String serialized = IcebergSystemTableSerialization.serializeToBase64(task);
                FileScanTask restored = IcebergSystemTableSerialization.deserializeFromBase64(serialized);
                try (CloseableIterable<org.apache.iceberg.StructLike> rows = restored.asDataTask().rows()) {
                    Assertions.assertEquals(10L, rows.iterator().next().get(0, Long.class));
                }
                length += serialized.length();
                count++;
            }
        }
        Assertions.assertEquals(1, count);
        return length;
    }

    @ParameterizedTest
    @ValueSource(strings = {"FILES", "ENTRIES", "ALL_MANIFESTS", "MANIFESTS", "SNAPSHOTS", "HISTORY", "REFS",
            "METADATA_LOG_ENTRIES", "PARTITIONS"})
    void serializedTasksPinTheirConcreteMetadataReference(String type) throws Exception {
        Table table = createTable(4);
        table.updateProperties().set(TableProperties.WRITE_METADATA_LOCATION,
                "/metadata/" + tempDir.getFileName()).commit();
        table.newFastAppend().appendFile(data(table, "s3://example-bucket/data/first.parquet")).commit();
        Table view = IcebergSnapshotTableOperations.v4ReadTable(coldMetadata(table));
        Table metadataTable = org.apache.iceberg.MetadataTableUtils.createMetadataTableInstance(
                view, org.apache.iceberg.MetadataTableType.valueOf(type));
        String column = type.equals("FILES") ? "record_count" : type.equals("ENTRIES") ? "data_file.record_count"
                : metadataTable.schema().columns().get(0).name();
        int count = 0;
        try (CloseableIterable<FileScanTask> tasks = metadataTable.newScan().select(column).planFiles()) {
            for (FileScanTask task : tasks) {
                FileScanTask restored = IcebergSystemTableSerialization.deserializeFromBase64(
                        IcebergSystemTableSerialization.serializeToBase64(task));
                try (CloseableIterable<org.apache.iceberg.StructLike> rows = restored.asDataTask().rows()) {
                    Assertions.assertTrue(rows.iterator().hasNext());
                    count++;
                }
            }
        }
        Assertions.assertEquals(1, count);
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
