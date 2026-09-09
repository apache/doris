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

package org.apache.doris.iceberg;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Opt in with -Ddoris.iceberg.packaged-fileio-test=true after building the Java extension packages.
 * Uses fresh target jars in the BE parent/child layout and installed output/be Hadoop dependencies;
 * a successful FE-only build does not deploy those Java packages into output/be. Missing artifacts
 * fail an enabled run. Maven's flat test classpath cannot establish this FileIO loading behavior.
 */
@EnabledIfSystemProperty(named = "doris.iceberg.packaged-fileio-test", matches = "true")
class IcebergColdResolvingFileIOPackagedClasspathTest {
    private static final String RESOLVING_FILE_IO = "org.apache.iceberg.io.ResolvingFileIO";
    private static final String ADLS_FILE_IO = "org.apache.iceberg.azure.adlsv2.ADLSFileIO";
    private static final String S3_FILE_IO = "org.apache.iceberg.aws.s3.S3FileIO";
    private static final String HADOOP_FILE_IO = "org.apache.iceberg.hadoop.HadoopFileIO";
    private static final String LOCATION = "abfs://container@127.0.0.1/metadata/cold-file.bin";
    private static final String TOKEN = "sp=r&se=2100-01-01T00:00:00Z&sig=packaged-test-signature";
    private static final byte[] CONTENT = "cold serialized Azure FileIO".getBytes(StandardCharsets.UTF_8);
    // Hadoop registers a JVM hook with no public unregister API. Its isolated loaders must remain
    // open until this forked test JVM exits, even after its filesystem resources and hooks are cleared.
    private static final List<URLClassLoader> HADOOP_JVM_LOADERS = new CopyOnWriteArrayList<>();

    @Test
    @Timeout(30)
    void coldSerializedResolvingFileIOReadsAzureWithPackagedScannerClassLoader() throws Exception {
        Path worktree = worktreeRoot();
        List<URL> scannerUrls = List.of(requiredJar(worktree.resolve("fe/be-java-extensions/"
                + "iceberg-metadata-scanner/target/iceberg-metadata-scanner-jar-with-dependencies.jar")));
        URL[] receiverParentUrls = parentUrls(worktree);
        List<URL> senderUrls = new ArrayList<>(Arrays.asList(receiverParentUrls));
        senderUrls.addAll(scannerUrls);

        AtomicInteger requests = new AtomicInteger();
        AtomicInteger reads = new AtomicInteger();
        AtomicInteger unsignedRequests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/container/metadata/cold-file.bin",
                exchange -> serve(exchange, requests, reads, unsignedRequests));
        server.start();
        ClassLoader previousLoader = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader senderLoader = new URLClassLoader(
                senderUrls.toArray(new URL[0]), ClassLoader.getPlatformClassLoader());
                URLClassLoader parent = new URLClassLoader(receiverParentUrls, ClassLoader.getPlatformClassLoader());
                URLClassLoader scanner = scannerLoader(parent, scannerUrls)) {
            Thread.currentThread().setContextClassLoader(scanner);
            Class<?> resolvingClass = scanner.loadClass(RESOLVING_FILE_IO);
            Class<?> catalogUtil = scanner.loadClass("org.apache.iceberg.CatalogUtil");
            Class<?> adlsClass = scanner.loadClass(ADLS_FILE_IO);
            Assertions.assertSame(scanner, resolvingClass.getClassLoader(), "The resolving factory is scanner-local");
            Assertions.assertSame(scanner, catalogUtil.getClassLoader(), "The FileIO loader can see scanner providers");
            Assertions.assertSame(parent, scanner.getParent());
            Assertions.assertNotSame(senderLoader.loadClass(RESOLVING_FILE_IO), resolvingClass,
                    "The sender and receiver must not share the same Iceberg classes");
            Assertions.assertNotSame(getClass().getClassLoader(), resolvingClass.getClassLoader(),
                    "The receiving FileIO must not be loaded from Maven's test classpath");
            Assertions.assertSame(scanner, adlsClass.getClassLoader(), "ADLS remains in the scanner package");
            Class<?> fileIOClass = scanner.loadClass("org.apache.iceberg.io.FileIO");
            Assertions.assertSame(parent, fileIOClass.getClassLoader(), "FileIO retains its shared type identity");
            Assertions.assertTrue(fileIOClass.isAssignableFrom(resolvingClass));
            Assertions.assertTrue(fileIOClass.isAssignableFrom(adlsClass));
            assertSharedIcebergTypes(parent, scanner);

            Map<String, String> properties = Map.of(
                    "adls.connection-string.127.0.0.1", "http://127.0.0.1:" + server.getAddress().getPort(),
                    "adls.sas-token.127.0.0.1", TOKEN);
            String serialized = serializeColdSender(senderLoader, properties);
            Assertions.assertEquals(0, requests.get(), "Serializing a cold FileIO must not access storage");

            // Positive control uses the same packaged SDK, service descriptors, endpoint and token.
            // If it fails, the cold-reader failure is not yet evidence of the ResolvingFileIO bug.
            Class<?> httpClientClass = scanner.loadClass("com.azure.core.http.HttpClient");
            Object transport = httpClientClass.getMethod("createDefault").invoke(null);
            Assertions.assertEquals("com.azure.core.http.jdk.httpclient.JdkHttpClient",
                    transport.getClass().getName(), "Discover the packaged HTTP service, not merely its class");
            Assertions.assertSame(adlsClass.getClassLoader(), transport.getClass().getClassLoader(),
                    "Azure HTTP transport and ADLS must share their packaged dependency closure");
            try (Closeable direct = (Closeable) adlsClass.getConstructor().newInstance()) {
                fileIOClass.getMethod("initialize", Map.class).invoke(direct, properties);
                Assertions.assertArrayEquals(CONTENT, readBytes(direct, scanner));
            }
            int controlReads = reads.get();
            Assertions.assertTrue(controlReads > 0, "The positive control must actually read the loopback service");

            try (Closeable restored = (Closeable) deserialize(serialized, scanner)) {
                Assertions.assertSame(resolvingClass, restored.getClass());
                Assertions.assertEquals(properties, fileIOClass.getMethod("properties").invoke(restored));
                Assertions.assertArrayEquals(CONTENT, readBytes(restored, scanner));
            }
            Assertions.assertTrue(reads.get() > controlReads,
                    "The deserialized cold FileIO must issue its own Azure read");
            Assertions.assertEquals(0, unsignedRequests.get(), "Every Azure read must carry the supplied SAS");
        } finally {
            Thread.currentThread().setContextClassLoader(previousLoader);
            server.stop(0);
        }
    }

    @Test
    @Timeout(30)
    void coldSerializedResolvingFileIOReadsS3ThroughTheParentSdk() throws Exception {
        Path worktree = worktreeRoot();
        List<URL> scannerUrls = List.of(requiredJar(worktree.resolve("fe/be-java-extensions/"
                + "iceberg-metadata-scanner/target/iceberg-metadata-scanner-jar-with-dependencies.jar")));
        URL[] receiverParentUrls = parentUrls(worktree);
        List<URL> senderUrls = new ArrayList<>(Arrays.asList(receiverParentUrls));
        senderUrls.addAll(scannerUrls);

        AtomicInteger requests = new AtomicInteger();
        AtomicInteger reads = new AtomicInteger();
        AtomicInteger unsignedRequests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/bucket/metadata/cold-file.bin",
                exchange -> serveS3(exchange, requests, reads, unsignedRequests));
        server.start();
        ClassLoader previousLoader = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader senderLoader = new URLClassLoader(
                senderUrls.toArray(new URL[0]), ClassLoader.getPlatformClassLoader());
                URLClassLoader parent = new URLClassLoader(receiverParentUrls, ClassLoader.getPlatformClassLoader());
                URLClassLoader scanner = scannerLoader(parent, scannerUrls)) {
            Thread.currentThread().setContextClassLoader(scanner);
            assertSharedIcebergTypes(parent, scanner);
            Assertions.assertSame(scanner, scanner.loadClass(RESOLVING_FILE_IO).getClassLoader());
            Assertions.assertSame(parent, scanner.loadClass(S3_FILE_IO).getClassLoader());
            Assertions.assertSame(parent,
                    scanner.loadClass("software.amazon.awssdk.services.s3.S3Client").getClassLoader());
            Map<String, String> properties = Map.of(
                    "s3.endpoint", "http://127.0.0.1:" + server.getAddress().getPort(),
                    "s3.path-style-access", "true",
                    "s3.access-key-id", "packaged-test-access",
                    "s3.secret-access-key", "packaged-test-secret",
                    "client.region", "us-east-1",
                    "http-client.type", "urlconnection",
                    "http-client.urlconnection.connection-timeout-ms", "2000",
                    "http-client.urlconnection.socket-timeout-ms", "2000",
                    "s3.retry.num-retries", "0");
            String serialized = serializeColdSender(senderLoader, properties);
            Assertions.assertEquals(0, requests.get(), "Serializing a cold S3 FileIO must not issue HEAD or GET");
            try (Closeable restored = (Closeable) deserialize(serialized, scanner)) {
                Assertions.assertSame(scanner.loadClass(RESOLVING_FILE_IO), restored.getClass());
                Assertions.assertArrayEquals(CONTENT, readBytes(restored, scanner,
                        "s3://bucket/metadata/cold-file.bin", "org.apache.iceberg.aws.s3.S3InputFile"));
            }
            Assertions.assertTrue(reads.get() > 0, "The parent S3 SDK must read actual loopback object bytes");
            Assertions.assertEquals(0, unsignedRequests.get(), "Every S3 request must use the supplied AWS identity");
        } finally {
            Thread.currentThread().setContextClassLoader(previousLoader);
            server.stop(0);
        }
    }

    @Test
    @Timeout(30)
    void serializedParentHadoopFileIOKeepsOneLakeConfigurationAndReadsLocalFile(@TempDir Path directory)
            throws Exception {
        Path data = directory.resolve("metadata.bin");
        Files.write(data, CONTENT);
        Path worktree = worktreeRoot();
        List<URL> scannerUrls = List.of(requiredJar(worktree.resolve("fe/be-java-extensions/"
                + "iceberg-metadata-scanner/target/iceberg-metadata-scanner-jar-with-dependencies.jar")));
        URL[] receiverParentUrls = parentUrls(worktree);
        List<URL> senderUrls = new ArrayList<>(Arrays.asList(receiverParentUrls));
        senderUrls.addAll(scannerUrls);
        String account = ".onelake.dfs.fabric.microsoft.com";
        Map<String, String> configuration = Map.of(
                "fs.defaultFS", "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Files",
                "fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem",
                "fs.azure.account.auth.type" + account, "OAuth",
                "fs.azure.account.oauth.provider.type" + account,
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "fs.azure.account.oauth2.client.id" + account, "packaged-test-client",
                "fs.azure.account.oauth2.client.secret" + account, "packaged-test-secret",
                "fs.azure.account.oauth2.client.endpoint" + account, "http://127.0.0.1:1/token",
                "doris.fs.cache.key.abfss", "packaged-test-onelake-cache-key");
        ClassLoader previousLoader = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader senderLoader = new URLClassLoader(
                senderUrls.toArray(new URL[0]), ClassLoader.getPlatformClassLoader())) {
            URLClassLoader parent = keepHadoopLoaderForTestJvm(
                    new URLClassLoader(receiverParentUrls, ClassLoader.getPlatformClassLoader()));
            URLClassLoader scanner = keepHadoopLoaderForTestJvm(scannerLoader(parent, scannerUrls));
            String serialized = serializeConfiguredSender(senderLoader, HADOOP_FILE_IO, configuration);
            Thread.currentThread().setContextClassLoader(scanner);
            assertSharedIcebergTypes(parent, scanner);
            Class<?> hadoopClass = scanner.loadClass(HADOOP_FILE_IO);
            Assertions.assertSame(parent, hadoopClass.getClassLoader());
            Class<?> configurationClass = scanner.loadClass("org.apache.hadoop.conf.Configuration");
            Assertions.assertSame(parent, configurationClass.getClassLoader());
            try (Closeable restored = (Closeable) deserialize(serialized, scanner)) {
                Assertions.assertSame(hadoopClass, restored.getClass());
                Object restoredConfiguration = hadoopClass.getMethod("getConf").invoke(restored);
                Assertions.assertSame(configurationClass, restoredConfiguration.getClass());
                for (Map.Entry<String, String> entry : configuration.entrySet()) {
                    Assertions.assertEquals(entry.getValue(),
                            configurationClass.getMethod("get", String.class)
                                    .invoke(restoredConfiguration, entry.getKey()), entry.getKey());
                }
                // This verifies preserved Hadoop configuration and the real local-file reader,
                // not live OneLake OAuth authentication; no OneLake or token endpoint is contacted.
                Assertions.assertArrayEquals(CONTENT, readBytes(restored, scanner,
                        data.toUri().toString(), "org.apache.iceberg.hadoop.HadoopInputFile"));
            } finally {
                closeHadoopResources(parent);
            }
        } finally {
            Thread.currentThread().setContextClassLoader(previousLoader);
        }
    }

    @Test
    @Timeout(30)
    void coldSerializedResolvingFileIOConfiguresTheParentHadoopReader(@TempDir Path directory) throws Exception {
        Path data = directory.resolve("cold-hadoop-metadata.bin");
        Files.write(data, CONTENT);
        Path worktree = worktreeRoot();
        List<URL> scannerUrls = List.of(requiredJar(worktree.resolve("fe/be-java-extensions/"
                + "iceberg-metadata-scanner/target/iceberg-metadata-scanner-jar-with-dependencies.jar")));
        URL[] receiverParentUrls = parentUrls(worktree);
        List<URL> senderUrls = new ArrayList<>(Arrays.asList(receiverParentUrls));
        senderUrls.addAll(scannerUrls);
        Map<String, String> configuration = Map.of(
                "fs.defaultFS", "file:///",
                "fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem",
                "doris.test.hadoop.marker", "cold-resolving-configuration");
        ClassLoader previousLoader = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader senderLoader = new URLClassLoader(
                senderUrls.toArray(new URL[0]), ClassLoader.getPlatformClassLoader())) {
            URLClassLoader parent = keepHadoopLoaderForTestJvm(
                    new URLClassLoader(receiverParentUrls, ClassLoader.getPlatformClassLoader()));
            URLClassLoader scanner = keepHadoopLoaderForTestJvm(scannerLoader(parent, scannerUrls));
            String serialized = serializeConfiguredSender(senderLoader, RESOLVING_FILE_IO, configuration);
            Thread.currentThread().setContextClassLoader(scanner);
            assertSharedIcebergTypes(parent, scanner);
            Assertions.assertSame(scanner, scanner.loadClass(RESOLVING_FILE_IO).getClassLoader());
            Assertions.assertSame(scanner, scanner.loadClass("org.apache.iceberg.CatalogUtil").getClassLoader());
            Class<?> fileIOClass = scanner.loadClass("org.apache.iceberg.io.FileIO");
            Class<?> hadoopInputClass = scanner.loadClass("org.apache.iceberg.hadoop.HadoopInputFile");
            Assertions.assertSame(parent, hadoopInputClass.getClassLoader());
            Class<?> configurationClass = scanner.loadClass("org.apache.hadoop.conf.Configuration");
            Assertions.assertSame(parent, configurationClass.getClassLoader());
            try (Closeable restored = (Closeable) deserialize(serialized, scanner)) {
                Assertions.assertSame(scanner.loadClass(RESOLVING_FILE_IO), restored.getClass());
                // The first input creation must load the parent HadoopFileIO via scanner-local
                // CatalogUtil and apply the serialized configuration to that new delegate.
                Object input = fileIOClass.getMethod("newInputFile", String.class)
                        .invoke(restored, data.toUri().toString());
                Assertions.assertSame(hadoopInputClass, input.getClass());
                Object inputConfiguration = hadoopInputClass.getMethod("getConf").invoke(input);
                Assertions.assertSame(configurationClass, inputConfiguration.getClass());
                for (Map.Entry<String, String> entry : configuration.entrySet()) {
                    Assertions.assertEquals(entry.getValue(),
                            configurationClass.getMethod("get", String.class)
                                    .invoke(inputConfiguration, entry.getKey()), entry.getKey());
                }
                Object fileSystem = hadoopInputClass.getMethod("getFileSystem").invoke(input);
                Assertions.assertSame(scanner.loadClass("org.apache.hadoop.fs.RawLocalFileSystem"),
                        fileSystem.getClass());
                Class<?> inputFileClass = scanner.loadClass("org.apache.iceberg.io.InputFile");
                try (InputStream stream = (InputStream) inputFileClass.getMethod("newStream").invoke(input)) {
                    Assertions.assertArrayEquals(CONTENT, stream.readAllBytes());
                }
            } finally {
                closeHadoopResources(parent);
            }
        } finally {
            Thread.currentThread().setContextClassLoader(previousLoader);
        }
    }

    @Test
    void otherScannerNamesKeepParentFirstIcebergFactoriesAndTypes() throws Exception {
        Path worktree = worktreeRoot();
        List<URL> scannerUrls = List.of(requiredJar(worktree.resolve("fe/be-java-extensions/"
                + "iceberg-metadata-scanner/target/iceberg-metadata-scanner-jar-with-dependencies.jar")));
        try (URLClassLoader parent = new URLClassLoader(parentUrls(worktree), ClassLoader.getPlatformClassLoader())) {
            for (String name : List.of("paimon-scanner", "hudi-scanner", "iceberg-metadata-scanner-other")) {
                try (URLClassLoader scanner = scannerLoader(parent, scannerUrls, name)) {
                    assertSharedIcebergTypes(parent, scanner);
                    Assertions.assertSame(parent.loadClass(RESOLVING_FILE_IO), scanner.loadClass(RESOLVING_FILE_IO));
                    Assertions.assertSame(parent.loadClass("org.apache.iceberg.CatalogUtil"),
                            scanner.loadClass("org.apache.iceberg.CatalogUtil"));
                }
            }
        }
    }

    private static void assertSharedIcebergTypes(ClassLoader parent, ClassLoader scanner)
            throws ClassNotFoundException {
        for (String name : List.of("org.apache.iceberg.io.FileIO", "org.apache.iceberg.io.DelegateFileIO",
                "org.apache.iceberg.io.SupportsStorageCredentials", "org.apache.iceberg.hadoop.HadoopConfigurable",
                "org.apache.iceberg.FileScanTask", "org.apache.iceberg.DataTask", "org.apache.iceberg.StaticDataTask",
                "org.apache.iceberg.AllManifestsTable$ManifestListReadTask", S3_FILE_IO, HADOOP_FILE_IO)) {
            Class<?> type = parent.loadClass(name);
            Assertions.assertSame(parent, type.getClassLoader(), name);
            Assertions.assertSame(type, scanner.loadClass(name), name);
        }
    }

    private static String serializeColdSender(ClassLoader loader, Map<String, String> properties) throws Exception {
        ClassLoader previousLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(loader);
        try {
            Class<?> resolvingClass = loader.loadClass(RESOLVING_FILE_IO);
            Assertions.assertSame(loader, resolvingClass.getClassLoader());
            try (Closeable sender = (Closeable) resolvingClass.getConstructor().newInstance();
                    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                    ObjectOutputStream output = new ObjectOutputStream(bytes)) {
                resolvingClass.getMethod("initialize", Map.class).invoke(sender, properties);
                // No newInputFile/ioClass call: the sender must have no cached ADLS delegate.
                // Java serialization is also the protocol used for a FileIO nested in an Iceberg
                // task; calling SerializationUtil on a top-level FileIO additionally rewrites its
                // Hadoop configuration, which is not part of this metadata-task scenario.
                output.writeObject(sender);
                output.flush();
                return Base64.getEncoder().encodeToString(bytes.toByteArray());
            }
        } finally {
            Thread.currentThread().setContextClassLoader(previousLoader);
        }
    }

    private static byte[] readBytes(Object fileIO, ClassLoader loader) throws Exception {
        return readBytes(fileIO, loader, LOCATION, "org.apache.iceberg.azure.adlsv2.ADLSInputFile");
    }

    private static byte[] readBytes(Object fileIO, ClassLoader loader, String location, String expectedInputClass)
            throws Exception {
        Class<?> fileIOClass = loader.loadClass("org.apache.iceberg.io.FileIO");
        Object input = fileIOClass.getMethod("newInputFile", String.class).invoke(fileIO, location);
        Assertions.assertEquals(expectedInputClass, input.getClass().getName(),
                "The packaged FileIO must select the expected storage reader");
        Class<?> inputFileClass = loader.loadClass("org.apache.iceberg.io.InputFile");
        try (InputStream stream = (InputStream) inputFileClass.getMethod("newStream").invoke(input)) {
            return stream.readAllBytes();
        }
    }

    private static Object deserialize(String serialized, ClassLoader loader) throws Exception {
        byte[] bytes = Base64.getMimeDecoder().decode(serialized);
        try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes)) {
            @Override
            protected Class<?> resolveClass(ObjectStreamClass descriptor) throws ClassNotFoundException {
                return Class.forName(descriptor.getName(), false, loader);
            }
        }) {
            return input.readObject();
        }
    }

    private static URLClassLoader scannerLoader(ClassLoader parent, List<URL> urls) throws Exception {
        return scannerLoader(parent, urls, "iceberg-metadata-scanner");
    }

    private static URLClassLoader scannerLoader(ClassLoader parent, List<URL> urls, String scannerName)
            throws Exception {
        Class<?> loaderClass = parent.loadClass("org.apache.doris.common.classloader.JniScannerClassLoader");
        return (URLClassLoader) loaderClass.getConstructor(String.class, List.class, ClassLoader.class)
                .newInstance(scannerName, urls, parent);
    }

    private static String serializeConfiguredSender(ClassLoader loader, String fileIOType,
            Map<String, String> properties) throws Exception {
        ClassLoader previousLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(loader);
        try {
            Class<?> fileIOClass = loader.loadClass(fileIOType);
            Class<?> configurationClass = loader.loadClass("org.apache.hadoop.conf.Configuration");
            Object configuration = configurationClass.getConstructor(boolean.class).newInstance(false);
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                configurationClass.getMethod("set", String.class, String.class)
                        .invoke(configuration, entry.getKey(), entry.getValue());
            }
            try (Closeable sender = (Closeable) fileIOClass.getConstructor().newInstance();
                    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                    ObjectOutputStream output = new ObjectOutputStream(bytes)) {
                fileIOClass.getMethod("initialize", Map.class).invoke(sender, Map.of());
                fileIOClass.getMethod("setConf", configurationClass).invoke(sender, configuration);
                // Keep ResolvingFileIO cold: configuration serialization must not create a delegate.
                output.writeObject(sender);
                output.flush();
                return Base64.getEncoder().encodeToString(bytes.toByteArray());
            }
        } finally {
            Thread.currentThread().setContextClassLoader(previousLoader);
        }
    }

    private static URLClassLoader keepHadoopLoaderForTestJvm(URLClassLoader loader) {
        HADOOP_JVM_LOADERS.add(loader);
        return loader;
    }

    private static void closeHadoopResources(ClassLoader loader) throws Exception {
        loader.loadClass("org.apache.hadoop.fs.FileSystem").getMethod("closeAll").invoke(null);
        Class<?> hooksClass = loader.loadClass("org.apache.hadoop.util.ShutdownHookManager");
        Object hooks = hooksClass.getMethod("get").invoke(null);
        hooksClass.getMethod("clearShutdownHooks").invoke(hooks);
        // clearShutdownHooks removes registered work, not Hadoop's own JVM hook. That hook still
        // loads ShutdownHookManager$2 at JVM exit, so closing its loader here is not safe.
    }

    private static URL[] parentUrls(Path worktree) throws IOException {
        // Reproduce start_be.sh's parent precedence, but use this build's target packages instead
        // of pretending build.sh --fe has refreshed the installed BE Java extensions.
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
                "Missing BE Java package; run the standard build before enabling this artifact test: " + jar);
        return jar.toUri().toURL();
    }

    private static List<URL> jarUrls(Path directory) throws IOException {
        Assertions.assertTrue(Files.isDirectory(directory),
                "Missing packaged BE Java directory (this test must not silently skip): " + directory);
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

    private static void serve(HttpExchange exchange, AtomicInteger requests, AtomicInteger reads,
            AtomicInteger unsignedRequests) throws IOException {
        try {
            requests.incrementAndGet();
            String query = exchange.getRequestURI().getRawQuery();
            if (query == null || !query.contains("sig=packaged-test-signature")) {
                unsignedRequests.incrementAndGet();
                exchange.sendResponseHeaders(403, -1);
                return;
            }
            serveObject(exchange, reads);
        } finally {
            exchange.close();
        }
    }

    private static void serveS3(HttpExchange exchange, AtomicInteger requests, AtomicInteger reads,
            AtomicInteger unsignedRequests) throws IOException {
        try {
            requests.incrementAndGet();
            String authorization = exchange.getRequestHeaders().getFirst("Authorization");
            if (authorization == null
                    || !authorization.startsWith("AWS4-HMAC-SHA256 Credential=packaged-test-access/")) {
                unsignedRequests.incrementAndGet();
                exchange.sendResponseHeaders(403, -1);
                return;
            }
            serveObject(exchange, reads);
        } finally {
            exchange.close();
        }
    }

    private static void serveObject(HttpExchange exchange, AtomicInteger reads) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
        exchange.getResponseHeaders().set("Content-Length", Integer.toString(CONTENT.length));
        exchange.getResponseHeaders().set("ETag", "\"packaged-test-etag\"");
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
        int end = CONTENT.length - 1;
        if (range != null) {
            String[] bounds = range.substring("bytes=".length()).split("-", 2);
            start = Integer.parseInt(bounds[0]);
            if (!bounds[1].isEmpty()) {
                end = Math.min(end, Integer.parseInt(bounds[1]));
            }
            exchange.getResponseHeaders().set("Content-Range",
                    "bytes " + start + "-" + end + "/" + CONTENT.length);
        }
        int length = end - start + 1;
        exchange.getResponseHeaders().set("Content-Length", Integer.toString(length));
        exchange.sendResponseHeaders(range == null ? 200 : 206, length);
        exchange.getResponseBody().write(CONTENT, start, length);
    }
}
