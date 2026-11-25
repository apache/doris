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

import org.apache.doris.datasource.property.storage.StorageProperties;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.DataInputStream;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

class IcebergRestGcsFileIOTest {
    private static final String GCS_RESOLVER = "org.apache.iceberg.io.NativeGcsResolvingFileIO";

    @Test
    void supportsGoogleCatalogAuthentication() {
        IcebergRestProperties properties = properties(ImmutableMap.of(
                "iceberg.rest.security.type", "google",
                "iceberg.rest.google.user-project", "billing-project",
                "iceberg.rest.io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO"));
        Map<String, String> options = properties.getIcebergRestCatalogProperties();
        Assertions.assertEquals(AuthProperties.AUTH_MANAGER_IMPL_GOOGLE, options.get(AuthProperties.AUTH_TYPE));
        Assertions.assertEquals("billing-project", options.get("header.x-goog-user-project"));
        Assertions.assertEquals("org.apache.iceberg.gcp.gcs.GCSFileIO", options.get(CatalogProperties.FILE_IO_IMPL));
    }

    @Test
    void keepsHmacOnHadoopFileIoWhenGcpIsAvailable() {
        Map<String, String> raw = new HashMap<>(ImmutableMap.of(
                "gs.endpoint", "https://storage.googleapis.com", "gs.access_key", "test-access-key",
                "gs.secret_key", "test-secret-key", "iceberg.rest.vended-credentials-enabled", "false"));
        IcebergRestProperties properties = properties(raw);
        Map<String, String> options = new HashMap<>(properties.getIcebergRestCatalogProperties());
        Configuration configuration = new Configuration(false);
        properties.toFileIOProperties(ImmutableList.of(StorageProperties.createPrimary(raw)), options, configuration);
        // The presence of iceberg-gcp must not replace an explicitly configured HMAC identity with ADC.
        Assertions.assertEquals(GCS_RESOLVER, options.get(CatalogProperties.FILE_IO_IMPL));
        Assertions.assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", configuration.get("fs.gs.impl"));
        Assertions.assertEquals("test-access-key", configuration.get("fs.s3a.access.key"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "io-impl", "iceberg.rest.io-impl"})
    void keepsRawHadoopGcsSettingsOnHadoopFileIo(String ioProperty) {
        Map<String, String> raw = new HashMap<>(ImmutableMap.of("fs.gs.impl", "example.GcsFileSystem"));
        if (!ioProperty.isEmpty()) {
            raw.put(ioProperty, "org.apache.iceberg.io.ResolvingFileIO");
        }
        IcebergRestProperties properties = properties(raw);
        Map<String, String> options = new HashMap<>(raw);
        options.putAll(properties.getIcebergRestCatalogProperties());
        properties.toFileIOProperties(ImmutableList.of(), options, new Configuration(false));
        Assertions.assertEquals(GCS_RESOLVER, options.get(CatalogProperties.FILE_IO_IMPL));
    }

    @ParameterizedTest
    @ValueSource(strings = {"org.apache.iceberg.gcp.gcs.GCSFileIO", "example.CustomFileIO"})
    void preservesExplicitFileIo(String ioImpl) {
        Map<String, String> raw = new HashMap<>(ImmutableMap.of("gs.endpoint", "https://storage.googleapis.com",
                "gs.access_key", "test-access-key", "gs.secret_key", "test-secret-key"));
        IcebergRestProperties properties = properties(raw);
        Map<String, String> options = new HashMap<>(ImmutableMap.of(CatalogProperties.FILE_IO_IMPL,
                ioImpl));
        properties.toFileIOProperties(ImmutableList.of(StorageProperties.createPrimary(raw)), options, new Configuration(false));
        Assertions.assertEquals(ioImpl, options.get(CatalogProperties.FILE_IO_IMPL));
    }

    @Test
    void doesNotChangeNonGcsDefaults() {
        IcebergRestProperties properties = properties(ImmutableMap.of());
        Map<String, String> options = new HashMap<>(properties.getIcebergRestCatalogProperties());
        properties.toFileIOProperties(ImmutableList.of(), options, new Configuration(false));
        Assertions.assertFalse(options.containsKey(CatalogProperties.FILE_IO_IMPL));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "io-impl", "iceberg.rest.io-impl"})
    void readsGcsMetadataWithHmacInsteadOfAdc(String ioProperty) throws Exception {
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
        try {
            Map<String, String> raw = new HashMap<>(ImmutableMap.of(
                    "gs.endpoint", "http://127.0.0.1:" + server.getAddress().getPort(),
                    "gs.access_key", "test-access-key", "gs.secret_key", "test-secret-key",
                    "use_path_style", "true", "iceberg.rest.vended-credentials-enabled", "false"));
            if (!ioProperty.isEmpty()) {
                raw.put(ioProperty, "org.apache.iceberg.io.ResolvingFileIO");
            }
            IcebergRestProperties properties = properties(raw);
            Map<String, String> options = new HashMap<>(raw);
            options.putAll(properties.getIcebergRestCatalogProperties());
            Configuration configuration = new Configuration(false);
            properties.toFileIOProperties(ImmutableList.of(StorageProperties.createPrimary(raw)), options, configuration);
            // Keep even an incorrect JSON-API route on the fixture instead of contacting a real bucket.
            options.put("gcs.service.host", raw.get("gs.endpoint"));
            options.put("gcs.no-auth", "true");
            configuration.set("fs.s3a.endpoint.region", "us-east-1");
            configuration.setBoolean("fs.s3a.connection.ssl.enabled", false);
            configuration.setInt("fs.s3a.retry.limit", 0);
            configuration.setInt("fs.s3a.attempts.maximum", 1);
            configuration.set("fs.s3a.bucket.probe", "0");
            String ioImpl = options.getOrDefault(CatalogProperties.FILE_IO_IMPL,
                    "org.apache.iceberg.io.ResolvingFileIO");
            try (FileIO io = CatalogUtil.loadFileIO(ioImpl, options, configuration);
                    SeekableInputStream input = io.newInputFile("gs://test-bucket/metadata.json", content.length)
                            .newStream()) {
                byte[] actual = new byte[content.length];
                new DataInputStream(input).readFully(actual);
                Assertions.assertArrayEquals(content, actual);
                Assertions.assertNotNull(authorization.get());
                Assertions.assertTrue(authorization.get().contains("Credential=test-access-key/"), authorization.get());
            }
        } finally {
            server.stop(0);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "io-impl", "iceberg.rest.io-impl"})
    void vendedGcsKeepsTheTokenAwareSdkDefault(String ioProperty) {
        Map<String, String> raw = new HashMap<>(ImmutableMap.of(
                "iceberg.rest.vended-credentials-enabled", "true", "fs.gs.impl", "example.GcsFileSystem"));
        if (!ioProperty.isEmpty()) {
            raw.put(ioProperty, "org.apache.iceberg.io.ResolvingFileIO");
        }
        IcebergRestProperties properties = properties(raw);
        Map<String, String> options = new HashMap<>(raw);
        options.putAll(properties.getIcebergRestCatalogProperties());
        properties.toFileIOProperties(ImmutableList.of(), options, new Configuration(false));
        if (ioProperty.isEmpty()) {
            Assertions.assertFalse(options.containsKey(CatalogProperties.FILE_IO_IMPL));
        } else {
            Assertions.assertEquals("org.apache.iceberg.io.ResolvingFileIO", options.get(CatalogProperties.FILE_IO_IMPL));
        }
    }

    @Test
    void preservesExplicitFileIoForNativeGcs() {
        Map<String, String> raw = new HashMap<>(ImmutableMap.of(
                "gs.endpoint", "https://storage.googleapis.com", "gs.credential_provider_type", "DEFAULT"));
        IcebergRestProperties properties = properties(raw);
        Map<String, String> options = new HashMap<>(ImmutableMap.of(CatalogProperties.FILE_IO_IMPL,
                "org.apache.iceberg.gcp.gcs.GCSFileIO"));
        properties.toFileIOProperties(ImmutableList.of(StorageProperties.createPrimary(raw)),
                options, new Configuration(false));
        Assertions.assertEquals("org.apache.iceberg.gcp.gcs.GCSFileIO", options.get(CatalogProperties.FILE_IO_IMPL));
    }

    @ParameterizedTest
    @MethodSource("dorisNativeCredentials")
    void preservesDorisNativeCredentialsWithGenericResolver(String provider, String impersonation) throws Exception {
        for (String ioProperty : new String[] {"io-impl", "iceberg.rest.io-impl"}) {
            Map<String, String> raw = new HashMap<>(ImmutableMap.of(
                    "gs.endpoint", "https://storage.googleapis.com", "gs.credential_provider_type", provider,
                    ioProperty, "org.apache.iceberg.io.ResolvingFileIO"));
            if (!impersonation.isEmpty()) {
                raw.put("gs.impersonation_service_account", impersonation);
            }
            IcebergRestProperties properties = properties(raw);
            Map<String, String> options = new HashMap<>(raw);
            options.putAll(properties.getIcebergRestCatalogProperties());
            Configuration conf = new Configuration(false);
            properties.toFileIOProperties(ImmutableList.of(StorageProperties.createPrimary(raw)), options, conf);
            Assertions.assertEquals(GCS_RESOLVER, options.get(CatalogProperties.FILE_IO_IMPL));
            Assertions.assertEquals("com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem", conf.get("fs.gs.impl"));
            Assertions.assertEquals("DEFAULT".equals(provider) ? "APPLICATION_DEFAULT" : "COMPUTE_ENGINE",
                    conf.get("fs.gs.auth.type"));
            Assertions.assertEquals(impersonation, conf.get("fs.gs.auth.impersonation.service.account"));
            try (FileIO io = CatalogUtil.loadFileIO(GCS_RESOLVER, options, conf)) {
                Method route = Class.forName("org.apache.iceberg.io.ResolvingFileIO")
                        .getDeclaredMethod("implFromLocation", String.class);
                route.setAccessible(true);
                Assertions.assertEquals("org.apache.iceberg.hadoop.HadoopFileIO",
                        route.invoke(io, "gs://test-bucket/metadata.json"));
            }
        }
    }

    private static Stream<Arguments> dorisNativeCredentials() {
        return Stream.of(Arguments.of("DEFAULT", ""), Arguments.of("COMPUTE_ENGINE", ""),
                Arguments.of("DEFAULT", "target@fixture.iam.gserviceaccount.com"),
                Arguments.of("COMPUTE_ENGINE", "target@fixture.iam.gserviceaccount.com"));
    }

    private static IcebergRestProperties properties(Map<String, String> raw) {
        Map<String, String> options = new HashMap<>(raw);
        options.put("iceberg.rest.uri", "https://catalog.example.com");
        IcebergRestProperties properties = new IcebergRestProperties(options);
        properties.initNormalizeAndCheckProps();
        return properties;
    }
}
