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

package org.apache.doris.connector;

import org.apache.doris.connector.iceberg.IcebergCatalogOps;
import org.apache.doris.connector.iceberg.IcebergCatalogProperties;
import org.apache.doris.connector.iceberg.IcebergColumnHandle;
import org.apache.doris.connector.iceberg.IcebergScanPlanProvider;
import org.apache.doris.connector.iceberg.IcebergTableHandle;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;
import org.apache.doris.kerberos.ExecutionAuthenticator;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Real FE planning to packaged BE Java consumption, without native Azure credentials beside the task.
 * Enable with -Ddoris.iceberg.packaged-fileio-test=true after building the Java extension packages.
 * This is an offline Java boundary test, not execution of the C++ JNI reader or live Azure authentication.
 */
@EnabledIfSystemProperty(named = "doris.iceberg.packaged-fileio-test", matches = "true")
class IcebergMetadataScannerPackagedIntegrationTest {
    private static final String ACCOUNT_HOST = "account.dfs.core.windows.net";
    private static final String TABLE_ROOT = "abfss://container@" + ACCOUNT_HOST + "/table";
    private static final String TOKEN = "sp=r&se=2100-01-01T00:00:00Z&sig=planned-task-signature";
    private static final String RESOLVING_FILE_IO = "org.apache.iceberg.io.ResolvingFileIO";
    private static final String SCANNER_CLASS = "org.apache.doris.iceberg.IcebergSysTableJniScanner";
    private static final Map<String, String> CATALOG_PROPERTIES = Map.of(
            "iceberg.catalog.type", "rest", "iceberg.rest.vended-credentials-enabled", "true");
    private static final TableIdentifier TABLE_ID = TableIdentifier.of("db", "t");
    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

    @Test
    @Timeout(60)
    void restScopedSasTravelsInTheColdTaskToThePackagedScanner() throws Exception {
        ConnectorStatementScopeImpl scope = new ConnectorStatementScopeImpl();
        try (InMemoryCatalog catalog = new InMemoryCatalog()) {
            catalog.initialize("rest-task-test", Map.of());
            catalog.createNamespace(Namespace.of("db"));
            Table source = catalog.createTable(TABLE_ID, SCHEMA, PartitionSpec.unpartitioned(), TABLE_ROOT, Map.of());
            source.newFastAppend().appendFile(DataFiles.builder(source.spec())
                    .withPath(TABLE_ROOT + "/data/one.parquet")
                    .withFileSizeInBytes(16).withRecordCount(3).build()).commit();
            Snapshot snapshot = source.currentSnapshot();
            ManifestFile manifest = snapshot.dataManifests(source.io()).get(0);
            String manifestListPath = Fixture.azureRequestPath(snapshot.manifestListLocation());
            try (IcebergAzureFileIOIntegrationTest.RestFixture rest =
                    new IcebergAzureFileIOIntegrationTest.RestFixture(Map.of("io-impl", RESOLVING_FILE_IO),
                            ((BaseTable) source).operations().current(),
                            Map.of(manifestListPath, Fixture.read(source.io(), snapshot.manifestListLocation())))) {
                rest.tableCredentials(List.of(ImmutableCredential.builder().prefix(TABLE_ROOT + "/")
                        .putConfig("adls.sas-token." + ACCOUNT_HOST, TOKEN).build()));
                PlannedScan planned = plan(rest.scanProvider(), new TestSession(scope), "all_manifests", List.of(
                        new IcebergColumnHandle("reference_snapshot_id", 18),
                        new IcebergColumnHandle("path", 1),
                        new IcebergColumnHandle("added_data_files_count", 5)));
                Assertions.assertTrue(rest.tableLoadRequestCount() > 0, "planning must load the actual REST table");
                Assertions.assertTrue(rest.storageQueries().isEmpty(), "the serialized FileIO must remain cold");

                Object[][] result = scanPackaged(planned,
                        "reference_snapshot_id,path,added_data_files_count", "bigint#string#int");

                Assertions.assertArrayEquals(new Object[] {snapshot.snapshotId()}, result[0]);
                Assertions.assertArrayEquals(new Object[] {manifest.path()}, result[1]);
                Assertions.assertArrayEquals(new Object[] {1}, result[2]);
                Assertions.assertTrue(rest.rangeReadPaths().contains(manifestListPath));
                Assertions.assertTrue(rest.storageQueries().stream().allMatch(query -> query != null
                        && query.contains("sig=planned-task-signature")));
            }
        } finally {
            scope.closeAll();
        }
    }

    @Test
    @Timeout(60)
    void allManifestsPlannedWithColdResolvingFileIoReadsThroughPackagedScanner() throws Exception {
        try (Fixture fixture = new Fixture()) {
            // Request a non-schema order: FE's projection must match the BE reader's positional columns.
            List<ConnectorColumnHandle> columns = List.of(
                    new IcebergColumnHandle("reference_snapshot_id", 18),
                    new IcebergColumnHandle("path", 1),
                    new IcebergColumnHandle("added_data_files_count", 5));
            PlannedScan planned = fixture.plan("all_manifests", columns);
            Assertions.assertEquals(0, fixture.requests.get(),
                    "FE planScan and serialization must not open or read the cold FileIO");

            Object[][] result = scanPackaged(planned,
                    "reference_snapshot_id,path,added_data_files_count", "bigint#string#int");

            Assertions.assertArrayEquals(new Object[] {fixture.snapshot.snapshotId()}, result[0]);
            Assertions.assertArrayEquals(new Object[] {fixture.manifest.path()}, result[1]);
            Assertions.assertArrayEquals(new Object[] {1}, result[2]);
            Assertions.assertTrue(fixture.rangeReads.get() > 0,
                    "the deserialized task must read real Avro bytes through Azure range GET");
            Assertions.assertEquals(0, fixture.unsignedRequests.get(), "all storage requests must use the task SAS");
        }
    }

    @Test
    @Timeout(60)
    void filesPlannedWithColdResolvingFileIoReadManifestThroughPackagedScanner() throws Exception {
        try (Fixture fixture = new Fixture()) {
            List<ConnectorColumnHandle> columns = List.of(
                    new IcebergColumnHandle("record_count", 103),
                    new IcebergColumnHandle("file_path", 100),
                    new IcebergColumnHandle("file_size_in_bytes", 104));
            PlannedScan planned = fixture.plan("files", columns);
            Assertions.assertEquals(0, fixture.requests.get(),
                    "planning from cached manifest descriptors must leave the task FileIO cold");

            Object[][] result = scanPackaged(planned,
                    "record_count,file_path,file_size_in_bytes", "bigint#string#bigint");

            Assertions.assertArrayEquals(new Object[] {3L}, result[0]);
            Assertions.assertArrayEquals(new Object[] {TABLE_ROOT + "/data/one.parquet"}, result[1]);
            Assertions.assertArrayEquals(new Object[] {16L}, result[2]);
            Assertions.assertTrue(fixture.rangeReadPaths.contains(Fixture.azureRequestPath(fixture.manifest.path())),
                    "$files must range-read the actual manifest Avro, not only its manifest list");
            Assertions.assertEquals(0, fixture.unsignedRequests.get(), "all storage requests must use the task SAS");
        }
    }

    @Test
    @Timeout(60)
    void fileMetricsAndOffsetsSurviveMultiplePackagedScannerBatches() throws Exception {
        try (Fixture fixture = new Fixture(3)) {
            PlannedScan planned = fixture.plan("files", List.of(
                    new IcebergColumnHandle("file_path", DataFile.FILE_PATH.fieldId()),
                    new IcebergColumnHandle("column_sizes", DataFile.COLUMN_SIZES.fieldId()),
                    new IcebergColumnHandle("split_offsets", DataFile.SPLIT_OFFSETS.fieldId())));
            Assertions.assertEquals(0, fixture.requests.get(), "the planned task must keep FileIO cold");

            List<Object[][]> batches = scanPackagedBatches(planned,
                    "file_path,column_sizes,split_offsets", "string#map<int,bigint>#array<bigint>", 2, 2, 1);

            Map<String, List<Object>> rows = new LinkedHashMap<>();
            for (Object[][] batch : batches) {
                for (int row = 0; row < batch[0].length; row++) {
                    Assertions.assertNotNull(batch[1][row], "Missing column_sizes for " + batch[0][row]);
                    Assertions.assertNotNull(batch[2][row], "Missing split_offsets for " + batch[0][row]);
                    Assertions.assertNull(rows.put((String) batch[0][row],
                            List.of(batch[1][row], batch[2][row])), "resetTable must not duplicate rows");
                }
            }
            Assertions.assertEquals(Map.of(
                    TABLE_ROOT + "/data/one.parquet", List.of(Map.of(1, 16L), List.of(0L, 8L)),
                    TABLE_ROOT + "/data/part-1.parquet", List.of(Map.of(1, 17L), List.of(0L, 9L)),
                    TABLE_ROOT + "/data/part-2.parquet", List.of(Map.of(1, 18L), List.of(0L, 10L))), rows);
            Assertions.assertTrue(fixture.rangeReadPaths.contains(Fixture.azureRequestPath(fixture.manifest.path())));
            Assertions.assertEquals(0, fixture.unsignedRequests.get());
        }
    }

    @Test
    @Timeout(60)
    void entriesPlannedWithColdResolvingFileIoReadManifestThroughPackagedScanner() throws Exception {
        try (Fixture fixture = new Fixture()) {
            List<ConnectorColumnHandle> columns = List.of(
                    new IcebergColumnHandle("sequence_number", 3),
                    new IcebergColumnHandle("status", 0),
                    new IcebergColumnHandle("snapshot_id", 1));
            PlannedScan planned = fixture.plan("entries", columns);
            Assertions.assertEquals(0, fixture.requests.get(),
                    "planning from cached manifest descriptors must leave the task FileIO cold");

            Object[][] result = scanPackaged(planned, "sequence_number,status,snapshot_id", "bigint#int#bigint");

            Assertions.assertArrayEquals(new Object[] {fixture.snapshot.sequenceNumber()}, result[0]);
            Assertions.assertArrayEquals(new Object[] {1}, result[1]);
            Assertions.assertArrayEquals(new Object[] {fixture.snapshot.snapshotId()}, result[2]);
            Assertions.assertTrue(fixture.rangeReadPaths.contains(Fixture.azureRequestPath(fixture.manifest.path())),
                    "$entries must decode the actual manifest entries through Azure range GET");
            Assertions.assertEquals(0, fixture.unsignedRequests.get(), "all storage requests must use the task SAS");
        }
    }

    @Test
    @Timeout(60)
    void manifestsPlannedAsStaticRowsNeedNoStorageWhenConsumedByPackagedScanner() throws Exception {
        try (Fixture fixture = new Fixture()) {
            List<ConnectorColumnHandle> columns = List.of(
                    new IcebergColumnHandle("added_snapshot_id", 4),
                    new IcebergColumnHandle("path", 1),
                    new IcebergColumnHandle("length", 2));
            PlannedScan planned = fixture.plan("manifests", columns);
            // Unlike all_manifests, this table materializes manifest descriptors in FE. Planning
            // may HEAD the manifest list to populate the StaticDataTask's DataFile size.
            int planningRequests = fixture.requests.get();

            Object[][] result = scanPackaged(planned, "added_snapshot_id,path,length", "bigint#string#bigint");

            Assertions.assertArrayEquals(new Object[] {fixture.snapshot.snapshotId()}, result[0]);
            Assertions.assertArrayEquals(new Object[] {fixture.manifest.path()}, result[1]);
            Assertions.assertArrayEquals(new Object[] {fixture.manifest.length()}, result[2]);
            Assertions.assertEquals(planningRequests, fixture.requests.get(),
                    "BE Java must consume the materialized manifest rows without reopening their manifest list");
            Assertions.assertEquals(0, fixture.unsignedRequests.get());
        }
    }

    @Test
    @Timeout(60)
    void snapshotsPlannedAsStaticRowsNeedNoStorageWhenConsumedByPackagedScanner() throws Exception {
        // A separate fixture keeps the preceding all-manifests case cold: planning a StaticDataTask
        // may resolve FileIO and HEAD its metadata file to populate DataFile.fileSizeInBytes.
        try (Fixture fixture = new Fixture()) {
            List<ConnectorColumnHandle> columns = List.of(
                    new IcebergColumnHandle("operation", 4), new IcebergColumnHandle("snapshot_id", 2));
            PlannedScan planned = fixture.plan("snapshots", columns);
            int planningRequests = fixture.requests.get();
            Assertions.assertEquals(0, fixture.reads.get(), "planning static snapshot rows does not GET a manifest");

            Object[][] result = scanPackaged(planned, "operation,snapshot_id", "string#bigint");

            Assertions.assertArrayEquals(new Object[] {"append"}, result[0]);
            Assertions.assertArrayEquals(new Object[] {fixture.snapshot.snapshotId()}, result[1]);
            Assertions.assertEquals(planningRequests, fixture.requests.get(),
                    "BE Java must consume the materialized StaticDataTask rows without opening storage");
            Assertions.assertEquals(0, fixture.unsignedRequests.get());
        }
    }

    private static Object[][] scanPackaged(PlannedScan planned, String fields, String types) throws Exception {
        return scanPackagedBatches(planned, fields, types, 8, 1).get(0);
    }

    private static List<Object[][]> scanPackagedBatches(PlannedScan planned, String fields, String types,
            int batchSize, int... expectedBatchRows) throws Exception {
        Path worktree = worktreeRoot();
        List<URL> scannerUrls = List.of(requiredJar(worktree.resolve("fe/be-java-extensions/"
                + "iceberg-metadata-scanner/target/iceberg-metadata-scanner-jar-with-dependencies.jar")));
        ClassLoader previousLoader = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader parent = new URLClassLoader(parentUrls(worktree), ClassLoader.getPlatformClassLoader());
                URLClassLoader loader = scannerLoader(parent, scannerUrls)) {
            Thread.currentThread().setContextClassLoader(loader);
            assertPackagedTypes(parent, loader);
            Class<?> offHeapClass = loader.loadClass("org.apache.doris.common.jni.utils.OffHeap");
            Assertions.assertSame(parent, offHeapClass.getClassLoader());
            offHeapClass.getMethod("setTesting").invoke(null);

            Class<?> scannerClass = loader.loadClass(SCANNER_CLASS);
            Assertions.assertSame(loader, scannerClass.getClassLoader());
            // Follow PluginDrivenScanNode's location.* extraction and the C++ reader's hadoop.
            // prefixing, without filtering credentials or replacing the serialized task in test code.
            String serialized = planned.descriptor.getTableFormatParams().getIcebergParams().getSerializedSplit();
            Map<String, String> params = new LinkedHashMap<>(Map.of(
                    "serialized_split", serialized,
                    "required_fields", fields, "required_types", types, "time_zone", "UTC"));
            if (planned.descriptor.getTableFormatParams().getIcebergParams().isSetFileIoExpiryMs()) {
                params.put("file_io_expiry_ms", Long.toString(
                        planned.descriptor.getTableFormatParams().getIcebergParams().getFileIoExpiryMs()));
            }
            planned.nodeProperties.forEach((key, value) -> {
                if (key.startsWith(ScanNodePropertyKeys.LOCATION_PREFIX)) {
                    params.put("hadoop." + key.substring(ScanNodePropertyKeys.LOCATION_PREFIX.length()), value);
                }
            });
            Assertions.assertFalse(params.containsKey("hadoop.provider"),
                    "FE metadata node properties must not send a native storage provider to JNI");
            Assertions.assertTrue(params.keySet().stream().noneMatch(key -> key.startsWith("hadoop.AZURE_")),
                    "FE metadata node properties must not copy native Azure credentials into the Hadoop channel");
            Object scanner = scannerClass.getConstructor(int.class, Map.class).newInstance(batchSize, params);
            try {
                scannerClass.getMethod("open").invoke(scanner);
                Class<?> vectorClass = loader.loadClass("org.apache.doris.common.jni.vec.VectorTable");
                Assertions.assertSame(parent, vectorClass.getClassLoader());
                // Inspect the scanner's own output vector. createReadableTable accepts the opposite
                // C++ -> Java layout, which has a const flag absent from Java -> BE scanner output.
                List<Object[][]> batches = new ArrayList<>();
                for (int expectedRows : expectedBatchRows) {
                    long address = (long) scannerClass.getMethod("getNextBatchMeta").invoke(scanner);
                    Assertions.assertNotEquals(0, address);
                    Object writable = scannerClass.getMethod("getTable").invoke(scanner);
                    Assertions.assertEquals(expectedRows, vectorClass.getMethod("getNumRows").invoke(writable));
                    batches.add((Object[][]) vectorClass.getMethod("getMaterializedData").invoke(writable));
                    scannerClass.getMethod("resetTable").invoke(scanner);
                }
                Assertions.assertEquals(0L, scannerClass.getMethod("getNextBatchMeta").invoke(scanner));
                return batches;
            } finally {
                try {
                    scannerClass.getMethod("releaseTable").invoke(scanner);
                } finally {
                    scannerClass.getMethod("close").invoke(scanner);
                }
            }
        } finally {
            Thread.currentThread().setContextClassLoader(previousLoader);
        }
    }

    private static void assertPackagedTypes(ClassLoader parent, ClassLoader scanner) throws ClassNotFoundException {
        for (String name : List.of("org.apache.iceberg.CatalogUtil", RESOLVING_FILE_IO,
                "org.apache.iceberg.azure.adlsv2.ADLSFileIO")) {
            Assertions.assertSame(scanner, scanner.loadClass(name).getClassLoader(), name);
        }
        Assertions.assertNotSame(ResolvingFileIO.class, scanner.loadClass(RESOLVING_FILE_IO),
                "sender and receiver must not share Maven's Iceberg classes");
        Assertions.assertNotSame(FileIO.class, parent.loadClass("org.apache.iceberg.io.FileIO"));
        for (String name : List.of("org.apache.iceberg.io.FileIO", "org.apache.iceberg.io.DelegateFileIO",
                "org.apache.iceberg.FileScanTask", "org.apache.iceberg.DataTask", "org.apache.iceberg.StaticDataTask",
                "org.apache.iceberg.AllManifestsTable$ManifestListReadTask",
                "org.apache.doris.common.jni.JniScanner")) {
            Class<?> type = parent.loadClass(name);
            Assertions.assertSame(parent, type.getClassLoader(), name);
            Assertions.assertSame(type, scanner.loadClass(name), name);
        }
    }

    private static URLClassLoader scannerLoader(ClassLoader parent, List<URL> urls) throws Exception {
        Class<?> loaderClass = parent.loadClass("org.apache.doris.common.classloader.JniScannerClassLoader");
        return (URLClassLoader) loaderClass.getConstructor(String.class, List.class, ClassLoader.class)
                .newInstance("iceberg-metadata-scanner", urls, parent);
    }

    private static URL[] parentUrls(Path worktree) throws IOException {
        // Same BE layout as IcebergColdResolvingFileIOPackagedClasspathTest: FE-only builds refresh
        // target Java packages, not the installed output/be copies. Hadoop stays in output/be.
        Path extensions = worktree.resolve("fe/be-java-extensions");
        List<URL> urls = new ArrayList<>();
        Path projectPreload = extensions.resolve("preload-extensions/target/preload-extensions-project.jar");
        if (Files.isRegularFile(projectPreload)) {
            urls.add(requiredJar(projectPreload));
        }
        urls.add(requiredJar(extensions.resolve(
                "preload-extensions/target/preload-extensions-jar-with-dependencies.jar")));
        urls.add(requiredJar(extensions.resolve("java-udf/target/java-udf-jar-with-dependencies.jar")));
        urls.addAll(jarUrls(worktree.resolve("output/be/lib/hadoop_hdfs")));
        urls.addAll(jarUrls(worktree.resolve("output/be/lib/hadoop_hdfs/lib")));
        return urls.toArray(new URL[0]);
    }

    private static URL requiredJar(Path jar) throws IOException {
        Assertions.assertTrue(Files.isRegularFile(jar),
                "Missing BE Java package; build it with the standard script before enabling this test: " + jar);
        return jar.toUri().toURL();
    }

    private static List<URL> jarUrls(Path directory) throws IOException {
        Assertions.assertTrue(Files.isDirectory(directory), "Missing BE Hadoop directory: " + directory);
        List<Path> jars;
        try (Stream<Path> entries = Files.list(directory)) {
            jars = entries.filter(path -> path.getFileName().toString().endsWith(".jar"))
                    .sorted().collect(Collectors.toList());
        }
        List<URL> urls = new ArrayList<>();
        for (Path jar : jars) {
            urls.add(jar.toUri().toURL());
        }
        return urls;
    }

    private static Path worktreeRoot() {
        Path path = Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
        while (path != null && !Files.isRegularFile(path.resolve("run-fe-ut.sh"))) {
            path = path.getParent();
        }
        Assertions.assertNotNull(path, "Run this artifact test from the Doris worktree");
        return path;
    }

    private static final class PlannedScan {
        private final TFileRangeDesc descriptor;
        private final Map<String, String> nodeProperties;

        private PlannedScan(TFileRangeDesc descriptor, Map<String, String> nodeProperties) {
            this.descriptor = descriptor;
            this.nodeProperties = Map.copyOf(nodeProperties);
        }
    }

    private static PlannedScan plan(ConnectorScanPlanProvider provider, ConnectorSession session,
            String systemTable, List<ConnectorColumnHandle> columns) {
        IcebergTableHandle handle = IcebergTableHandle.forSystemTable("db", "t", systemTable, -1L, null, -1L);
        Map<String, String> nodeProperties = provider.getScanNodeProperties(session, handle, columns, Optional.empty());
        Assertions.assertEquals("jni", nodeProperties.get(ScanNodePropertyKeys.FILE_FORMAT_TYPE));
        List<ConnectorScanRange> ranges = provider.planScan(
                session, ConnectorScanRequest.builder(handle, columns).build());
        Assertions.assertEquals(1, ranges.size());
        ConnectorScanRange range = ranges.get(0);
        TTableFormatFileDesc format = new TTableFormatFileDesc();
        format.setTableFormatType(range.getTableFormatType());
        TFileRangeDesc descriptor = new TFileRangeDesc();
        descriptor.setPath(range.getPath().orElseThrow());
        range.populateRangeParams(format, descriptor);
        descriptor.setTableFormatParams(format);
        Assertions.assertEquals(TFileFormatType.FORMAT_JNI, descriptor.getFormatType());
        Assertions.assertTrue(format.getIcebergParams().isSetSerializedSplit());
        Assertions.assertFalse(format.getIcebergParams().getSerializedSplit().isEmpty());
        if (List.of("all_manifests", "files", "entries").contains(systemTable)) {
            Assertions.assertTrue(format.getIcebergParams().isSetFileIoExpiryMs());
            Assertions.assertEquals(4102444800000L, format.getIcebergParams().getFileIoExpiryMs());
        } else {
            Assertions.assertFalse(format.getIcebergParams().isSetFileIoExpiryMs(),
                    "materialized tasks do not need a credential at execution");
        }
        return new PlannedScan(descriptor, nodeProperties);
    }

    private static final class Fixture implements AutoCloseable {
        private final InMemoryCatalog catalog = new InMemoryCatalog();
        private final ResolvingFileIO fileIO = new ResolvingFileIO();
        private final ConnectorStatementScopeImpl statementScope = new ConnectorStatementScopeImpl();
        private final ConnectorSession session = new TestSession(statementScope);
        private final DefaultConnectorContext context;
        private final IcebergScanPlanProvider provider;
        private final HttpServer server;
        private final Snapshot snapshot;
        private final ManifestFile manifest;
        private final AtomicInteger requests = new AtomicInteger();
        private final AtomicInteger reads = new AtomicInteger();
        private final AtomicInteger rangeReads = new AtomicInteger();
        private final List<String> rangeReadPaths = new CopyOnWriteArrayList<>();
        private final AtomicInteger unsignedRequests = new AtomicInteger();

        private Fixture() throws IOException {
            this(1);
        }

        private Fixture(int fileCount) throws IOException {
            catalog.initialize("metadata-scanner-test", Collections.emptyMap());
            catalog.createNamespace(Namespace.of("db"));
            Table source = catalog.createTable(TABLE_ID, SCHEMA, PartitionSpec.unpartitioned(), TABLE_ROOT,
                    Map.of(TableProperties.FORMAT_VERSION, "2"));
            AppendFiles append = source.newFastAppend();
            for (int index = 0; index < fileCount; index++) {
                String fileName = index == 0 ? "one.parquet" : "part-" + index + ".parquet";
                append.appendFile(DataFiles.builder(source.spec()).withPath(TABLE_ROOT + "/data/" + fileName)
                        .withFileSizeInBytes(16 + index)
                        .withMetrics(new Metrics(3L, Map.of(1, 16L + index), Map.of(1, 3L), Map.of(1, 0L), Map.of()))
                        .withSplitOffsets(List.of(0L, 8L + index)).build());
            }
            append.commit();
            snapshot = source.currentSnapshot();
            manifest = snapshot.dataManifests(source.io()).get(0);
            // Positive control: the in-memory fixture must write both complex fields to Avro,
            // before any FE projection, task serialization or isolated scanner is involved.
            try (ManifestReader<DataFile> files = ManifestFiles.read(manifest, source.io())) {
                for (DataFile file : files) {
                    Assertions.assertEquals(Map.of(1, file.fileSizeInBytes()), file.columnSizes());
                    Assertions.assertEquals(List.of(0L, file.fileSizeInBytes() - 8), file.splitOffsets());
                }
            }
            TableMetadata metadata = ((BaseTable) source).operations().current();
            Map<String, byte[]> objects = Map.of(
                    azureRequestPath(snapshot.manifestListLocation()),
                    read(source.io(), snapshot.manifestListLocation()),
                    azureRequestPath(manifest.path()), read(source.io(), manifest.path()),
                    azureRequestPath(metadata.metadataFileLocation()),
                    read(source.io(), metadata.metadataFileLocation()));
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", exchange -> serve(exchange, objects));
            fileIO.initialize(Map.of(
                    "adls.sas-token." + ACCOUNT_HOST, TOKEN,
                    "adls.connection-string." + ACCOUNT_HOST, "http://127.0.0.1:" + server.getAddress().getPort()));
            // Never inspect ioClass/newInputFile here: the task must carry a genuinely cold resolver.
            Table authorized = new BaseTable(new StaticTableOperations(metadata, fileIO), source.name());
            Catalog remoteCatalog = Mockito.mock(Catalog.class);
            Mockito.when(remoteCatalog.loadTable(TABLE_ID)).thenReturn(authorized);
            context = new DefaultConnectorContext("azure_metadata_scanner_test", 1L,
                    () -> new ExecutionAuthenticator() {}, Collections::emptyMap, () -> CATALOG_PROPERTIES);
            provider = new IcebergScanPlanProvider(IcebergCatalogProperties.of(CATALOG_PROPERTIES),
                    new IcebergCatalogOps.CatalogBackedIcebergCatalogOps(remoteCatalog), context);
            server.start();
        }

        private PlannedScan plan(String systemTable, List<ConnectorColumnHandle> columns) {
            return IcebergMetadataScannerPackagedIntegrationTest.plan(provider, session, systemTable, columns);
        }

        private static byte[] read(FileIO fileIO, String location) throws IOException {
            try (SeekableInputStream input = fileIO.newInputFile(location).newStream()) {
                return input.readAllBytes();
            }
        }

        private static String azureRequestPath(String location) {
            return "/container" + URI.create(location).getPath();
        }

        private void serve(HttpExchange exchange, Map<String, byte[]> objects) throws IOException {
            try {
                requests.incrementAndGet();
                String query = exchange.getRequestURI().getRawQuery();
                if (query == null || !query.contains("sig=planned-task-signature")) {
                    unsignedRequests.incrementAndGet();
                    exchange.sendResponseHeaders(403, -1);
                    return;
                }
                byte[] bytes = objects.get(exchange.getRequestURI().getPath());
                if (bytes == null) {
                    exchange.sendResponseHeaders(404, -1);
                    return;
                }
                exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
                exchange.getResponseHeaders().set("Content-Length", Integer.toString(bytes.length));
                exchange.getResponseHeaders().set("ETag", "\"planned-task-etag\"");
                exchange.getResponseHeaders().set("Last-Modified", "Wed, 09 Sep 2026 00:00:00 GMT");
                exchange.getResponseHeaders().set("x-ms-blob-type", "BlockBlob");
                if ("HEAD".equals(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(200, -1);
                    return;
                }
                if (!"GET".equals(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }
                reads.incrementAndGet();
                String range = exchange.getRequestHeaders().getFirst("Range");
                if (range == null) {
                    range = exchange.getRequestHeaders().getFirst("x-ms-range");
                }
                int start = 0;
                int end = bytes.length - 1;
                if (range != null) {
                    rangeReads.incrementAndGet();
                    rangeReadPaths.add(exchange.getRequestURI().getPath());
                    String[] bounds = range.substring("bytes=".length()).split("-", 2);
                    start = Integer.parseInt(bounds[0]);
                    if (!bounds[1].isEmpty()) {
                        end = Math.min(end, Integer.parseInt(bounds[1]));
                    }
                    exchange.getResponseHeaders().set("Content-Range",
                            "bytes " + start + "-" + end + "/" + bytes.length);
                }
                int length = end - start + 1;
                exchange.getResponseHeaders().set("Content-Length", Integer.toString(length));
                exchange.sendResponseHeaders(range == null ? 200 : 206, length);
                exchange.getResponseBody().write(bytes, start, length);
            } finally {
                exchange.close();
            }
        }

        @Override
        public void close() throws IOException {
            try {
                statementScope.closeAll();
                context.close();
            } finally {
                try {
                    fileIO.close();
                    catalog.close();
                } finally {
                    server.stop(0);
                }
            }
        }
    }

    private static final class TestSession implements ConnectorSession {
        private final ConnectorStatementScope statementScope;

        private TestSession(ConnectorStatementScope statementScope) {
            this.statementScope = statementScope;
        }

        @Override
        public String getQueryId() {
            return "packaged-metadata-scan";
        }

        @Override
        public String getUser() {
            return "test-user";
        }

        @Override
        public String getTimeZone() {
            return "UTC";
        }

        @Override
        public String getLocale() {
            return "en_US";
        }

        @Override
        public long getCatalogId() {
            return 1L;
        }

        @Override
        public String getCatalogName() {
            return "azure_metadata_scanner_test";
        }

        @Override
        public <T> T getProperty(String name, Class<T> type) {
            return null;
        }

        @Override
        public Map<String, String> getCatalogProperties() {
            return CATALOG_PROPERTIES;
        }

        @Override
        public ConnectorStatementScope getStatementScope() {
            return statementScope;
        }
    }
}
