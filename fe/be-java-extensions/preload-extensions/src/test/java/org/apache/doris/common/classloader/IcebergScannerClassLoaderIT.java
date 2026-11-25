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

package org.apache.doris.common.classloader;

import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.JarFile;

/** Run with Maven verify so both deployment assemblies exist before constructing their real loaders. */
class IcebergScannerClassLoaderIT {
    private static final String RESOLVER = "org.apache.iceberg.io.NativeGcsResolvingFileIO";
    private static final String GCS_IO = "org.apache.iceberg.gcp.gcs.GCSFileIO";
    private static final String TASK_OWNER = "org.apache.iceberg.DorisMetadataTaskFileIO";

    @Test
    void preloadContainsTheFileIoClosureWithoutReplacingPatchedHadoop() throws Exception {
        try (JarFile jar = new JarFile(System.getProperty("preload.jar"))) {
            for (String name : Arrays.asList(RESOLVER, GCS_IO, TASK_OWNER, "org.apache.iceberg.io.ResolvingFileIO",
                    "org.apache.iceberg.util.SerializationUtil", "com.google.cloud.storage.StorageOptions",
                    "com.google.auth.oauth2.GoogleCredentials")) {
                Assertions.assertNotNull(jar.getEntry(name.replace('.', '/') + ".class"), name);
            }
            Assertions.assertNull(jar.getEntry("org/apache/hadoop/fs/FileSystem.class"));
            Assertions.assertNull(jar.getEntry("org/apache/hadoop/conf/Configuration.class"));
        }
    }

    @Test
    void parentDeserializationAndPackagePrivateRoutingUseTheSameLoader() throws Exception {
        try (DeploymentLoaders loaders = new DeploymentLoaders()) {
            Class<?> resolver = Class.forName(RESOLVER, true, loaders.child);
            Class<?> sdkResolver = Class.forName("org.apache.iceberg.io.ResolvingFileIO", true, loaders.parent);
            Object io = resolver.getConstructor().newInstance();
            try {
                Class<?> confType = Class.forName("org.apache.hadoop.conf.Configuration", true, loaders.parent);
                resolver.getMethod("setConf", confType)
                        .invoke(io, confType.getConstructor(boolean.class).newInstance(false));
                resolver.getMethod("initialize", Map.class).invoke(io, Collections.emptyMap());
                Class<?> serialization = Class.forName(
                        "org.apache.iceberg.util.SerializationUtil", true, loaders.parent);
                String serialized = (String) serialization.getMethod("serializeToBase64", Object.class)
                        .invoke(null, io);
                Object restored = Assertions.assertDoesNotThrow(() -> serialization
                        .getMethod("deserializeFromBase64", String.class).invoke(null, serialized));
                Assertions.assertSame(loaders.parent, restored.getClass().getClassLoader());
                Assertions.assertSame(sdkResolver.getClassLoader(), resolver.getClassLoader());
                // Runtime packages include the defining loader; a child-defined adapter cannot override this method.
                Method route = sdkResolver.getDeclaredMethod("implFromLocation", String.class);
                route.setAccessible(true);
                Assertions.assertEquals("org.apache.iceberg.hadoop.HadoopFileIO",
                        route.invoke(io, "gs://test-bucket/metadata.json"));
                restored.getClass().getMethod("close").invoke(restored);
            } finally {
                resolver.getMethod("close").invoke(io);
            }
            Class<?> scanner = Class.forName(
                    "org.apache.doris.iceberg.IcebergSysTableJniScanner", false, loaders.child);
            Assertions.assertSame(loaders.child, scanner.getClassLoader());
            Assertions.assertThrows(ClassNotFoundException.class,
                    () -> Class.forName(scanner.getName(), false, loaders.parent));
        }
    }

    @Test
    void readsVendedTokenThroughParentFileIoAndChildScannerLoader() throws Exception {
        byte[] content = "metadata-content".getBytes(StandardCharsets.UTF_8);
        AtomicReference<String> authorization = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try {
                authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
                exchange.sendResponseHeaders(200, content.length);
                exchange.getResponseBody().write(content);
            } finally {
                exchange.close();
            }
        });
        server.start();
        try (DeploymentLoaders loaders = new DeploymentLoaders()) {
            Map<String, String> properties = new HashMap<>();
            properties.put("gcs.project-id", "test-project");
            properties.put("gcs.service.host", "http://127.0.0.1:" + server.getAddress().getPort());
            properties.put("gcs.oauth2.token", "synthetic-vended-token");
            properties.put("gcs.oauth2.token-expires-at", String.valueOf(System.currentTimeMillis() + 3600000));
            properties.put("uri", "http://127.0.0.1:" + server.getAddress().getPort());
            properties.put("gcs.oauth2.refresh-credentials-endpoint", properties.get("uri") + "/refresh");
            Class<?> type = Assertions.assertDoesNotThrow(() -> Class.forName(GCS_IO, true, loaders.parent));
            Object io = type.getConstructor().newInstance();
            Object restored = null;
            try {
                type.getMethod("initialize", Map.class).invoke(io, properties);
                Class<?> credentialType = Class.forName("org.apache.iceberg.io.StorageCredential", true, loaders.parent);
                Object credential = credentialType.getMethod("create", String.class, Map.class).invoke(null,
                        "gs://test-bucket", new HashMap<>(properties));
                // Flat token properties alone never schedule the SDK's storage-credential refresh job.
                type.getMethod("setCredentials", List.class).invoke(io, Collections.singletonList(credential));
                Class<?> serialization = Class.forName(
                        "org.apache.iceberg.util.SerializationUtil", true, loaders.parent);
                String encoded = (String) serialization.getMethod("serializeToBase64", Object.class).invoke(null, io);
                restored = serialization.getMethod("deserializeFromBase64", String.class).invoke(null, encoded);
                Assertions.assertSame(loaders.parent, restored.getClass().getClassLoader());
                Class<?> fileIo = Class.forName("org.apache.iceberg.io.FileIO", true, loaders.parent);
                Object file = fileIo.getMethod("newInputFile", String.class, long.class)
                        .invoke(restored, "gs://test-bucket/metadata.json", (long) content.length);
                Class<?> inputFile = Class.forName("org.apache.iceberg.io.InputFile", true, loaders.parent);
                try (InputStream stream = (InputStream) inputFile.getMethod("newStream").invoke(file)) {
                    byte[] actual = new byte[content.length];
                    new DataInputStream(stream).readFully(actual);
                    Assertions.assertArrayEquals(content, actual);
                    Assertions.assertEquals("Bearer synthetic-vended-token", authorization.get());
                }
                for (String name : Arrays.asList(GCS_IO, "com.google.cloud.storage.StorageOptions",
                        "com.google.auth.oauth2.GoogleCredentials")) {
                    Class<?> loaded = Class.forName(name, false, loaders.child);
                    Assertions.assertSame(loaders.parent, loaded.getClassLoader());
                    Assertions.assertEquals(new File(System.getProperty("preload.jar")).getCanonicalFile(),
                            new File(loaded.getProtectionDomain().getCodeSource().getLocation().toURI())
                                    .getCanonicalFile());
                }
                Field futureField = type.getDeclaredField("refreshFuture");
                futureField.setAccessible(true);
                Future<?> refresh = (Future<?>) futureField.get(restored);
                Assertions.assertNotNull(refresh);
                Assertions.assertFalse(refresh.isDone());
                type.getMethod("close").invoke(restored);
                Assertions.assertTrue(refresh.isCancelled());
                Field closedField = type.getDeclaredField("isResourceClosed");
                closedField.setAccessible(true);
                Assertions.assertTrue(((AtomicBoolean) closedField.get(restored)).get());
            } finally {
                if (restored != null) {
                    type.getMethod("close").invoke(restored);
                }
                type.getMethod("close").invoke(io);
            }
        } finally {
            server.stop(0);
        }
    }

    @Test
    void scansFilesTaskThroughTheDeployedParentAndChild(@TempDir Path warehouse) throws Exception {
        String serialized = createFilesTask(warehouse);
        try (DeploymentLoaders loaders = new DeploymentLoaders()) {
            Map<String, String> parameters = new HashMap<>();
            parameters.put("serialized_split", serialized);
            parameters.put("required_fields", "record_count");
            parameters.put("required_types", "bigint");
            parameters.put("time_zone", "UTC");
            Class.forName("org.apache.doris.common.jni.utils.OffHeap", true, loaders.parent)
                    .getMethod("setTesting").invoke(null);
            Class<?> type = Class.forName("org.apache.doris.iceberg.IcebergSysTableJniScanner", true, loaders.child);
            Object scanner = Assertions.assertDoesNotThrow(
                    () -> type.getConstructor(int.class, Map.class).newInstance(16, parameters));
            Field taskField = type.getDeclaredField("scanTask");
            taskField.setAccessible(true);
            Class<?> ownerBridge = Class.forName(TASK_OWNER, true, loaders.parent);
            Assertions.assertSame(loaders.parent, ownerBridge.getClassLoader());
            Assertions.assertEquals(new File(System.getProperty("preload.jar")).getCanonicalFile(),
                    new File(ownerBridge.getProtectionDomain().getCodeSource().getLocation().toURI()).getCanonicalFile());
            Object owner = ownerBridge.getMethod("fileIO", Class.forName(
                    "org.apache.iceberg.FileScanTask", true, loaders.parent)).invoke(null, taskField.get(scanner));
            Field closedField = Class.forName("org.apache.iceberg.io.ResolvingFileIO", true, loaders.parent)
                    .getDeclaredField("isClosed");
            closedField.setAccessible(true);
            Assertions.assertFalse(((AtomicBoolean) closedField.get(owner)).get());
            try {
                type.getMethod("open").invoke(scanner);
                Assertions.assertNotEquals(0L, type.getMethod("getNextBatchMeta").invoke(scanner));
                Object vectorTable = type.getMethod("getTable").invoke(scanner);
                Assertions.assertEquals(1, vectorTable.getClass().getMethod("getNumRows").invoke(vectorTable));
                type.getMethod("releaseTable").invoke(scanner);
                Assertions.assertEquals(0L, type.getMethod("getNextBatchMeta").invoke(scanner));
            } finally {
                type.getMethod("releaseTable").invoke(scanner);
                type.getMethod("close").invoke(scanner);
            }
            Assertions.assertTrue(((AtomicBoolean) closedField.get(owner)).get());
        }
    }

    private static String createFilesTask(Path warehouse) throws Exception {
        // Produce the split with the FE-side SDK before switching to the isolated BE loader topology.
        try (HadoopCatalog catalog = new HadoopCatalog()) {
            catalog.setConf(new Configuration());
            Map<String, String> properties = new HashMap<>();
            properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse.toUri().toString());
            properties.put(CatalogProperties.FILE_IO_IMPL, RESOLVER);
            catalog.initialize("test", properties);
            Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
            Table table = catalog.createTable(TableIdentifier.of("db", "test"), schema, PartitionSpec.unpartitioned());
            table.newAppend().appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath(warehouse.resolve("data.parquet").toString())
                    .withFileSizeInBytes(10).withRecordCount(1).build()).commit();
            Table metadata = MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.FILES);
            try (CloseableIterable<FileScanTask> tasks = metadata.newScan()
                    .project(new Schema(metadata.schema().findField("record_count"))).planFiles()) {
                return SerializationUtil.serializeToBase64(tasks.iterator().next());
            }
        }
    }

    private static final class DeploymentLoaders implements AutoCloseable {
        private final URLClassLoader parent;
        private final URLClassLoader child;
        private final ClassLoader previousContextLoader;

        private DeploymentLoaders() throws Exception {
            List<URL> parentUrls = new ArrayList<>();
            parentUrls.add(new File(System.getProperty("preload.jar")).toURI().toURL());
            String provided = new String(Files.readAllBytes(Paths.get(System.getProperty("provided.classpath"))),
                    StandardCharsets.UTF_8).trim();
            for (String path : provided.split(File.pathSeparator)) {
                parentUrls.add(new File(path).toURI().toURL());
            }
            // Keep the application test loader outside the ancestry so it cannot hide missing deployment classes.
            parent = new URLClassLoader(parentUrls.toArray(new URL[0]),
                    ClassLoader.getSystemClassLoader().getParent());
            Class<?> loader = Class.forName(
                    "org.apache.doris.common.classloader.JniScannerClassLoader", true, parent);
            child = (URLClassLoader) loader.getConstructor(String.class, List.class, ClassLoader.class).newInstance(
                    "iceberg-metadata-scanner",
                    Collections.singletonList(new File(System.getProperty("scanner.jar")).toURI().toURL()), parent);
            previousContextLoader = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(child);
        }

        @Override
        public void close() throws Exception {
            Thread.currentThread().setContextClassLoader(previousContextLoader);
            try {
                child.close();
            } finally {
                parent.close();
            }
        }
    }
}
