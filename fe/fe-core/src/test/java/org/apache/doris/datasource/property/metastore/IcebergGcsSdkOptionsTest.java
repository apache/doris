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

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpServer;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.DataInputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class IcebergGcsSdkOptionsTest {
    private static final String SDK_RESOLVER = ResolvingFileIO.class.getName();
    private static final String COMPAT_RESOLVER = "org.apache.iceberg.io.NativeGcsResolvingFileIO";
    private static final String GCS = "org.apache.iceberg.gcp.gcs.GCSFileIO";
    private static final String HADOOP = "org.apache.iceberg.hadoop.HadoopFileIO";
    private static final String LOCATION = "gs://fixture-bucket/v1.metadata.json";
    private static final String KEY = Base64.getEncoder().encodeToString(new byte[32]);

    @ParameterizedTest
    @ValueSource(strings = {"gcs.decryption-key", "gcs.encryption-key", "gcs.user-project", "gcs.project-id",
            "gcs.client-lib-token", "gcs.service.host", "gcs.channel.read.chunk-size-bytes",
            "gcs.channel.write.chunk-size-bytes", "gcs.delete.batch-size", "gcs.analytics-core.enabled",
            "gcs.future-native-option"})
    void preservesExplicitSdkStorageOptions(String key) {
        for (String alias : new String[] {"", "io-impl", "iceberg.rest.io-impl"}) {
            Map<String, String> raw = new HashMap<>();
            raw.put("iceberg.rest.uri", "https://catalog.example.com");
            raw.put(key, "fixture");
            if (!alias.isEmpty()) {
                raw.put(alias, SDK_RESOLVER);
            }
            IcebergRestProperties properties = new IcebergRestProperties(raw);
            properties.initNormalizeAndCheckProps();
            Map<String, String> options = new HashMap<>(raw);
            options.putAll(properties.getIcebergRestCatalogProperties());
            Configuration conf = new Configuration(false);
            conf.set("fs.gs.impl", "example.GcsFileSystem");
            properties.toFileIOProperties(ImmutableList.of(), options, conf);
            Assertions.assertEquals(alias.isEmpty() ? null : SDK_RESOLVER,
                    options.get(CatalogProperties.FILE_IO_IMPL), key + ": " + alias);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"gcs.decryption-key", "gcs.encryption-key", "gcs.user-project", "gcs.project-id",
            "gcs.client-lib-token", "gcs.service.host", "gcs.channel.read.chunk-size-bytes",
            "gcs.channel.write.chunk-size-bytes", "gcs.delete.batch-size", "gcs.analytics-core.enabled",
            "gcs.future-native-option"})
    void honorsLateSdkPropertiesAfterSerialization(String key) throws Exception {
        // REST configuration and table options may arrive after FE selected the compatibility resolver.
        try (FileIO original = CatalogUtil.loadFileIO(COMPAT_RESOLVER,
                ImmutableMap.of(key, "fixture"), new Configuration(false));
                FileIO restored = SerializationUtil.deserializeFromBytes(SerializationUtil.serializeToBytes(original))) {
            ResolvingFileIO resolver = (ResolvingFileIO) restored;
            Assertions.assertEquals(GCS, resolver.ioClass(LOCATION).getName());
            Assertions.assertEquals("fixture", resolver.properties().get(key));
            Assertions.assertEquals("org.apache.iceberg.aws.s3.S3FileIO",
                    resolver.ioClass("s3://fixture-bucket/object").getName());
            Assertions.assertEquals(HADOOP, resolver.ioClass("file:///fixture").getName());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"gs://fixture-bucket/", "gs://another-bucket/", "s3://fixture-bucket/"})
    void honorsOnlyMatchingStorageCredentialOptions(String prefix) throws Exception {
        try (ResolvingFileIO original = (ResolvingFileIO) CatalogUtil.loadFileIO(COMPAT_RESOLVER,
                ImmutableMap.of(), new Configuration(false))) {
            original.setCredentials(ImmutableList.of(StorageCredential.create(prefix,
                    ImmutableMap.of("gcs.decryption-key", KEY))));
            try (ResolvingFileIO restored = SerializationUtil.deserializeFromBytes(
                    SerializationUtil.serializeToBytes(original))) {
                Assertions.assertEquals(LOCATION.startsWith(prefix) ? GCS : HADOOP,
                        restored.ioClass(LOCATION).getName());
            }
        }
    }

    @Test
    void honorsTheLongestCredentialPrefixRegardlessOfOrder() throws Exception {
        String location = "gs://fixture-bucket/private/object";
        Map<String, String> hadoop = ImmutableMap.of("fs.gs.auth.type", "UNAUTHENTICATED");
        for (boolean narrowSdk : ImmutableList.of(false, true)) {
            StorageCredential broad = StorageCredential.create("gs://fixture-bucket/",
                    narrowSdk ? hadoop : ImmutableMap.of("gcs.oauth2.token", "fixture-token"));
            StorageCredential narrow = StorageCredential.create("gs://fixture-bucket/private/",
                    narrowSdk ? ImmutableMap.of("gcs.oauth2.token", "fixture-token") : hadoop);
            for (List<StorageCredential> credentials : ImmutableList.of(
                    ImmutableList.of(broad, narrow), ImmutableList.of(narrow, broad),
                    ImmutableList.of(StorageCredential.create(narrow.prefix(), broad.config()), narrow))) {
                for (boolean globalSdk : ImmutableList.of(false, true)) {
                    try (ResolvingFileIO original = (ResolvingFileIO) CatalogUtil.loadFileIO(COMPAT_RESOLVER,
                            globalSdk ? ImmutableMap.of("gcs.project-id", "fixture-project") : ImmutableMap.of(),
                            new Configuration(false))) {
                        original.setCredentials(credentials);
                        try (ResolvingFileIO restored = SerializationUtil.deserializeFromBytes(
                                SerializationUtil.serializeToBytes(original))) {
                            Assertions.assertEquals(globalSdk || narrowSdk ? GCS : HADOOP,
                                    restored.ioClass(location).getName());
                        }
                    }
                }
            }
        }
    }

    @Test
    void restAuthenticationOptionsRetainLegacyHadoop() throws Exception {
        try (ResolvingFileIO io = (ResolvingFileIO) CatalogUtil.loadFileIO(COMPAT_RESOLVER,
                ImmutableMap.of("gcp.auth.credentials-json", "{\"type\":\"service_account\"}"),
                new Configuration(false))) {
            Assertions.assertEquals(HADOOP, io.ioClass(LOCATION).getName());
        }
    }

    @Test
    void readsCsekMetadataWithBillingProjectAndOperatorAdc() throws Exception {
        GoogleCredentials operatorCredentials = GoogleCredentials.create(
                new AccessToken("fixture-operator-adc", new Date(System.currentTimeMillis() + 3_600_000)));
        new MockUp<GoogleCredentials>() {
            @Mock
            public GoogleCredentials getApplicationDefault() {
                return operatorCredentials;
            }
        };
        Schema schema = new Schema(Types.NestedField.required(1, "value", Types.StringType.get()));
        TableMetadata metadata = TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(),
                SortOrder.unsorted(), "gs://fixture-bucket/table", ImmutableMap.of());
        byte[] content = TableMetadataParser.toJson(metadata).getBytes(StandardCharsets.UTF_8);
        String hash = Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-256").digest(new byte[32]));
        AtomicInteger accepted = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try {
                String query = exchange.getRequestURI().getRawQuery();
                boolean billing = (query != null && query.contains("userProject=fixture-billing"))
                        || "fixture-billing".equals(exchange.getRequestHeaders().getFirst("X-Goog-User-Project"));
                boolean authorized = "Bearer fixture-operator-adc".equals(
                        exchange.getRequestHeaders().getFirst("Authorization"));
                boolean key = KEY.equals(exchange.getRequestHeaders().getFirst("X-Goog-Encryption-Key"))
                        && hash.equals(exchange.getRequestHeaders().getFirst("X-Goog-Encryption-Key-Sha256"))
                        && "AES256".equals(exchange.getRequestHeaders().getFirst("X-Goog-Encryption-Algorithm"));
                if (!billing || !authorized || !key) {
                    exchange.sendResponseHeaders(403, -1);
                    return;
                }
                accepted.incrementAndGet();
                exchange.getResponseHeaders().set("Content-Type", "application/json");
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
            // No SDK token property: storage authentication must still use the operator's ADC path.
            Map<String, String> options = new HashMap<>();
            options.put("gcs.project-id", "fixture-project");
            options.put("gcs.service.host", "http://127.0.0.1:" + server.getAddress().getPort());
            options.put("gcs.decryption-key", KEY);
            options.put("gcs.user-project", "fixture-billing");
            try (ResolvingFileIO original = (ResolvingFileIO) CatalogUtil.loadFileIO(COMPAT_RESOLVER, options,
                    new Configuration(false));
                    ResolvingFileIO io = SerializationUtil.deserializeFromBytes(
                            SerializationUtil.serializeToBytes(original))) {
                // Fail before any stream/ADC lookup if an incorrect resolver would escape the local fixture.
                Assertions.assertEquals(GCS, io.ioClass(LOCATION).getName());
                Assertions.assertFalse(io.properties().containsKey("gcs.oauth2.token"));
                TableMetadata actual = TableMetadataParser.read(io.newInputFile(LOCATION, content.length));
                Assertions.assertEquals("value", actual.schema().findField(1).name());
                Assertions.assertEquals("gs://fixture-bucket/table", actual.location());
                Assertions.assertTrue(accepted.get() > 0);
            }
        } finally {
            server.stop(0);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "io-impl", "iceberg.rest.io-impl"})
    void restGoogleAuthenticationDoesNotReplaceHmacStorage(String alias) throws Exception {
        byte[] content = "fixture-metadata".getBytes(StandardCharsets.UTF_8);
        AtomicReference<String> authorization = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try {
                authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
                exchange.getResponseHeaders().set("ETag", "\"fixture-etag\"");
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
            Map<String, String> raw = new HashMap<>();
            raw.put("iceberg.rest.uri", "https://catalog.example.com");
            raw.put("iceberg.rest.security.type", "google");
            raw.put("iceberg.rest.google.user-project", "fixture-rest-billing");
            raw.put("gcp.auth.credentials-json", "{\"type\":\"service_account\"}");
            raw.put("gs.endpoint", "http://127.0.0.1:" + server.getAddress().getPort());
            raw.put("gs.access_key", "fixture-storage-key");
            raw.put("gs.secret_key", "fixture-storage-secret");
            raw.put("use_path_style", "true");
            raw.put("iceberg.rest.vended-credentials-enabled", "false");
            if (!alias.isEmpty()) {
                raw.put(alias, SDK_RESOLVER);
            }
            IcebergRestProperties properties = new IcebergRestProperties(raw);
            properties.initNormalizeAndCheckProps();
            Map<String, String> options = new HashMap<>(raw);
            options.putAll(properties.getIcebergRestCatalogProperties());
            Configuration conf = new Configuration(false);
            properties.toFileIOProperties(ImmutableList.of(
                    StorageProperties.createPrimary(raw)), options, conf);
            conf.set("fs.s3a.endpoint.region", "us-east-1");
            conf.setBoolean("fs.s3a.connection.ssl.enabled", false);
            conf.setInt("fs.s3a.retry.limit", 0);
            conf.setInt("fs.s3a.attempts.maximum", 1);
            conf.set("fs.s3a.bucket.probe", "0");
            String bucket = "sdk-config-hmac-" + (alias.isEmpty() ? "default" : alias.contains(".") ? "alias" : "raw");
            String location = "gs://" + bucket + "/metadata.json";
            try (ResolvingFileIO io = (ResolvingFileIO) CatalogUtil.loadFileIO(
                    options.get(CatalogProperties.FILE_IO_IMPL), options, conf)) {
                // Check the delegate before opening: an incorrect SDK route must not contact a real bucket.
                Assertions.assertEquals(HADOOP, io.ioClass(location).getName());
                try (SeekableInputStream input = io.newInputFile(location, content.length).newStream()) {
                    byte[] actual = new byte[content.length];
                    new DataInputStream(input).readFully(actual);
                    Assertions.assertArrayEquals(content, actual);
                }
                Assertions.assertNotNull(authorization.get());
                Assertions.assertTrue(authorization.get().contains("Credential=fixture-storage-key/"));
            }
        } finally {
            server.stop(0);
        }
    }
}
