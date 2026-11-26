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

package org.apache.doris.datasource.property.metastore;

import org.apache.doris.common.Config;
import org.apache.doris.datasource.property.storage.StorageProperties;

import com.google.common.collect.ImmutableList;
import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.DataInputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

class IcebergGcsRoutingCompatibilityTest {
    private static final String RESOLVER = "org.apache.iceberg.io.ResolvingFileIO";
    private static final String HADOOP_GCS_RESOLVER = "org.apache.iceberg.io.NativeGcsResolvingFileIO";

    @ParameterizedTest
    @MethodSource("nativeStorageModes")
    void preservesSdkStorageOptionsIndependentlyOfRestAuthentication(String key, String value, boolean nativeStorage) {
        for (boolean rest : new boolean[] {true, false}) {
            String[] ioKeys = rest ? new String[] {"", "io-impl", "iceberg.rest.io-impl"}
                    : new String[] {"io-impl"};
            for (String ioKey : ioKeys) {
                Map<String, String> raw = restProperties();
                if (value != null) {
                    raw.put(key, value);
                }
                if (!ioKey.isEmpty()) {
                    raw.put(ioKey, RESOLVER);
                }
                AbstractIcebergProperties properties;
                if (rest) {
                    properties = new IcebergRestProperties(raw);
                } else {
                    raw.put("uri", "jdbc:sqlite::memory:");
                    raw.put("warehouse", "gs://test-bucket/warehouse");
                    raw.put("iceberg.jdbc.catalog_name", "test");
                    properties = new IcebergJdbcMetaStoreProperties(raw);
                }
                properties.initNormalizeAndCheckProps();
                Map<String, String> options = new HashMap<>(raw);
                if (rest) {
                    options.putAll(((IcebergRestProperties) properties).getIcebergRestCatalogProperties());
                }
                options.put(key, value);
                Configuration conf = new Configuration(false);
                conf.set("fs.gs.impl", "example.GcsFileSystem");
                properties.toFileIOProperties(ImmutableList.of(), options, conf);
                String expected = nativeStorage ? (ioKey.isEmpty() ? null : RESOLVER) : HADOOP_GCS_RESOLVER;
                Assertions.assertEquals(expected, options.get(CatalogProperties.FILE_IO_IMPL), key + ": " + ioKey);
            }
        }
    }

    private static Stream<Arguments> nativeStorageModes() {
        return Stream.of(
                Arguments.of("gcs.oauth2.token", "synthetic-token", true),
                Arguments.of("gcs.oauth2.token", "", true),
                Arguments.of("gcs.oauth2.token", null, false),
                Arguments.of("gcs.no-auth", "true", true),
                Arguments.of("gcs.no-auth", "TRUE", true),
                Arguments.of("gcs.no-auth", " true ", true),
                Arguments.of("gcs.no-auth", "false", true),
                Arguments.of("gcs.no-auth", null, false),
                Arguments.of("gcs.impersonate.service-account", "identity@example.invalid", true),
                Arguments.of("gcs.impersonate.service-account", "", true),
                Arguments.of("gcs.impersonate.service-account", null, false),
                Arguments.of("gcs.oauth2.token-expires-at", "1700000000000", true),
                Arguments.of("gcs.oauth2.refresh-credentials-endpoint", "https://catalog.example.com/refresh", true),
                Arguments.of("gcs.project-id", "test-project", true),
                Arguments.of("gcs.client-lib-token", "client-tag", true),
                Arguments.of("gcp.auth.credentials-json", "{\"type\":\"service_account\"}", false));
    }

    @ParameterizedTest
    @ValueSource(strings = {" true ", " TRUE ", "true"})
    void normalizedVendingPreservesServerCredentials(String enabled) {
        Map<String, String> raw = restProperties();
        raw.put("iceberg.rest.vended-credentials-enabled", enabled);
        raw.put("fs.gs.impl", "example.GcsFileSystem");
        raw.put(CatalogProperties.FILE_IO_IMPL, RESOLVER);
        IcebergRestProperties properties = new IcebergRestProperties(raw);
        properties.initNormalizeAndCheckProps();
        Assertions.assertTrue(properties.isIcebergRestVendedCredentialsEnabled());
        Map<String, String> options = new HashMap<>(raw);
        options.putAll(properties.getIcebergRestCatalogProperties());
        properties.toFileIOProperties(ImmutableList.of(), options, new Configuration(false));
        Assertions.assertEquals(RESOLVER, options.get(CatalogProperties.FILE_IO_IMPL));
    }

    @Test
    void detectsGcsSettingsLoadedFromXml(@TempDir Path directory) throws Exception {
        Configuration resource = new Configuration(false);
        resource.set("fs.gs.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        resource.set("fs.s3a.access.key", "test-access-key");
        resource.set("fs.s3a.secret.key", "test-secret-key");
        try (OutputStream output = Files.newOutputStream(directory.resolve("gcs-site.xml"))) {
            resource.writeXml(output);
        }
        String previousDirectory = Config.hadoop_config_dir;
        try {
            Config.hadoop_config_dir = directory.toString() + "/";
            Map<String, String> raw = restProperties();
            raw.put("fs.hdfs.support", "true");
            raw.put("hadoop.config.resources", "gcs-site.xml");
            IcebergRestProperties properties = new IcebergRestProperties(raw);
            properties.initNormalizeAndCheckProps();
            Map<String, String> options = new HashMap<>(raw);
            options.putAll(properties.getIcebergRestCatalogProperties());
            Configuration conf = new Configuration(false);
            properties.toFileIOProperties(ImmutableList.of(StorageProperties.createPrimary(raw)), options, conf);
            Assertions.assertEquals("test-access-key", conf.get("fs.s3a.access.key"));
            Assertions.assertFalse(raw.containsKey("fs.gs.impl"));
            Assertions.assertEquals(HADOOP_GCS_RESOLVER, options.get(CatalogProperties.FILE_IO_IMPL));
        } finally {
            Config.hadoop_config_dir = previousDirectory;
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"io-impl", "iceberg.rest.io-impl"})
    void explicitNativeBearerWinsOverXmlHadoopIdentity(String ioProperty, @TempDir Path directory) throws Exception {
        byte[] content = "metadata".getBytes(StandardCharsets.UTF_8);
        AtomicReference<String> authorization = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try {
                authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
                exchange.getResponseHeaders().set("ETag", "\"test-etag\"");
                exchange.getResponseHeaders().set("Last-Modified", "Tue, 01 Jan 2019 00:00:00 GMT");
                exchange.getResponseHeaders().set("Content-Length", String.valueOf(content.length));
                if ("HEAD".equals(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(200, -1);
                } else {
                    exchange.sendResponseHeaders(200, content.length);
                    exchange.getResponseBody().write(content);
                }
            } finally {
                exchange.close();
            }
        });
        server.start();
        String previousDirectory = Config.hadoop_config_dir;
        try {
            String endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
            Configuration resource = new Configuration(false);
            resource.set("fs.gs.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            resource.set("fs.s3a.access.key", "test-hadoop-key");
            resource.set("fs.s3a.secret.key", "test-hadoop-secret");
            resource.set("fs.s3a.endpoint", endpoint);
            resource.set("fs.s3a.endpoint.region", "us-east-1");
            resource.setBoolean("fs.s3a.path.style.access", true);
            resource.setBoolean("fs.s3a.connection.ssl.enabled", false);
            resource.setInt("fs.s3a.retry.limit", 0);
            resource.setInt("fs.s3a.attempts.maximum", 1);
            resource.set("fs.s3a.bucket.probe", "0");
            try (OutputStream output = Files.newOutputStream(directory.resolve("gcs-site.xml"))) {
                resource.writeXml(output);
            }
            Config.hadoop_config_dir = directory.toString() + "/";
            Map<String, String> raw = restProperties();
            raw.put(ioProperty, RESOLVER);
            raw.put("fs.hdfs.support", "true");
            raw.put("hadoop.config.resources", "gcs-site.xml");
            raw.put("gcs.project-id", "test-project");
            raw.put("gcs.service.host", endpoint);
            raw.put("gcs.oauth2.token", "synthetic-native-token");
            IcebergRestProperties properties = new IcebergRestProperties(raw);
            properties.initNormalizeAndCheckProps();
            Map<String, String> options = new HashMap<>(raw);
            options.putAll(properties.getIcebergRestCatalogProperties());
            Configuration conf = new Configuration(false);
            properties.toFileIOProperties(ImmutableList.of(StorageProperties.createPrimary(raw)), options, conf);
            Assertions.assertFalse(raw.containsKey("fs.gs.impl"));
            Assertions.assertNotNull(conf.get("fs.gs.impl"));
            try (FileIO io = CatalogUtil.loadFileIO(options.get(CatalogProperties.FILE_IO_IMPL), options, conf);
                    SeekableInputStream input = io.newInputFile("gs://test-bucket-" + (ioProperty.contains(".") ? "alias" : "raw")
                            + "/metadata.json", content.length)
                            .newStream()) {
                byte[] actual = new byte[content.length];
                new DataInputStream(input).readFully(actual);
                Assertions.assertArrayEquals(content, actual);
                Assertions.assertEquals("Bearer synthetic-native-token", authorization.get());
            }
        } finally {
            Config.hadoop_config_dir = previousDirectory;
            server.stop(0);
        }
    }

    @Test
    void jdbcGenericResolverPreservesHmac() throws Exception {
        Map<String, String> raw = hmacProperties();
        raw.put("uri", "jdbc:sqlite::memory:");
        raw.put("warehouse", "gs://test-bucket/warehouse");
        raw.put("iceberg.jdbc.catalog_name", "test");
        raw.put(CatalogProperties.FILE_IO_IMPL, RESOLVER);
        AtomicReference<Map<String, String>> captured = new AtomicReference<>();
        IcebergJdbcMetaStoreProperties properties = new IcebergJdbcMetaStoreProperties(raw) {
            @Override
            protected Catalog buildIcebergCatalog(String name, Map<String, String> options, Configuration conf) {
                captured.set(new HashMap<>(options));
                return Mockito.mock(Catalog.class);
            }
        };
        properties.initNormalizeAndCheckProps();
        properties.initializeCatalog("test", ImmutableList.of(StorageProperties.createPrimary(raw)));
        Assertions.assertEquals(HADOOP_GCS_RESOLVER, captured.get().get(CatalogProperties.FILE_IO_IMPL));
    }

    @ParameterizedTest
    @ValueSource(strings = {"glue", "dlf", "s3tables"})
    void preservesGenericResolverBeforeFactoriesThatDoNotConvertStorageProperties(String catalogType) throws Exception {
        Map<String, String> raw = hmacProperties();
        raw.put(CatalogProperties.FILE_IO_IMPL, RESOLVER);
        AtomicReference<Map<String, String>> captured = new AtomicReference<>();
        AbstractIcebergProperties properties = new AbstractIcebergProperties(raw) {
            @Override
            public String getIcebergCatalogType() {
                return catalogType;
            }

            @Override
            protected Catalog initCatalog(String name, Map<String, String> options, List<StorageProperties> storage) {
                captured.set(new HashMap<>(options));
                return Mockito.mock(Catalog.class);
            }
        };
        properties.initNormalizeAndCheckProps();
        properties.initializeCatalog("test", ImmutableList.of(StorageProperties.createPrimary(raw)));
        Assertions.assertEquals(HADOOP_GCS_RESOLVER, captured.get().get(CatalogProperties.FILE_IO_IMPL));
    }

    @Test
    void hiveGenericResolverPreservesHmac() throws Exception {
        Map<String, String> raw = hmacProperties();
        raw.put("hive.metastore.uris", "thrift://127.0.0.1:1");
        raw.put("warehouse", "gs://test-bucket/warehouse");
        raw.put(CatalogProperties.FILE_IO_IMPL, RESOLVER);
        IcebergHMSMetaStoreProperties properties = new IcebergHMSMetaStoreProperties(raw);
        properties.initNormalizeAndCheckProps();
        try (HiveCatalog catalog = (HiveCatalog) properties.initializeCatalog(
                "test", ImmutableList.of(StorageProperties.createPrimary(raw)))) {
            // Inspect the real Hive initialization path, which does not use toFileIOProperties.
            Field field = HiveCatalog.class.getDeclaredField("fileIO");
            field.setAccessible(true);
            // Older HiveCatalog implementations do not own FileIO close; the fixture must release it too.
            try (FileIO io = (FileIO) field.get(catalog)) {
                Assertions.assertEquals(HADOOP_GCS_RESOLVER, io.getClass().getName());
            }
        }
    }

    private static Map<String, String> restProperties() {
        Map<String, String> raw = new HashMap<>();
        raw.put("iceberg.rest.uri", "https://catalog.example.com");
        return raw;
    }

    private static Map<String, String> hmacProperties() {
        Map<String, String> raw = new HashMap<>();
        raw.put("gs.endpoint", "https://storage.googleapis.com");
        raw.put("gs.access_key", "test-access-key");
        raw.put("gs.secret_key", "test-secret-key");
        return raw;
    }
}
