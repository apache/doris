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

import org.apache.doris.filesystem.gcs.GcsFileSystemProperties;

import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.NativeGcsResolvingFileIO;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class IcebergGcsFileIORoutingTest {
    private static final String GCS = "org.apache.iceberg.gcp.gcs.GCSFileIO";
    private static final String HADOOP = "org.apache.iceberg.hadoop.HadoopFileIO";
    private static final String LOCATION = "gs://fixture-bucket/v1.metadata.json";
    private static final String KEY = Base64.getEncoder().encodeToString(new byte[32]);

    @Test
    void gcsEncryptionKeysAreDeclaredSensitive() {
        List<String> secrets = new IcebergConnectorProvider().securitySurface().sensitiveKeys();
        Assertions.assertTrue(secrets.contains("gcs.encryption-key"));
        Assertions.assertTrue(secrets.contains("gcs.decryption-key"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"rest", "hms", "jdbc", "hadoop", "glue"})
    void preservesExplicitChoicesAndNonRestDefaults(String flavor) {
        Map<String, String> raw = new HashMap<>(Map.of("iceberg.catalog.type", flavor));
        Configuration conf = new Configuration(false);
        conf.set("fs.gs.impl", "example.GcsFileSystem");
        for (String selected : List.of(GCS, HADOOP, "example.CustomFileIO")) {
            Map<String, String> options = new HashMap<>(Map.of("io-impl", selected));
            IcebergCatalogFactory.preserveHadoopGcsFileIO(IcebergCatalogProperties.of(raw), options, conf);
            Assertions.assertEquals(selected, options.get("io-impl"));
        }
        Map<String, String> defaults = new HashMap<>();
        IcebergCatalogFactory.preserveHadoopGcsFileIO(IcebergCatalogProperties.of(raw), defaults, conf);
        Assertions.assertEquals("rest".equals(flavor) ? NativeGcsResolvingFileIO.class.getName() : null,
                defaults.get("io-impl"));

        Map<String, String> sdk = new HashMap<>(Map.of("io-impl", ResolvingFileIO.class.getName(),
                "gcs.decryption-key", KEY));
        IcebergCatalogFactory.preserveHadoopGcsFileIO(IcebergCatalogProperties.of(raw), sdk, conf);
        Assertions.assertEquals(ResolvingFileIO.class.getName(), sdk.get("io-impl"));
        sdk.remove("gcs.decryption-key");
        IcebergCatalogFactory.preserveHadoopGcsFileIO(IcebergCatalogProperties.of(raw), sdk,
                new Configuration(false));
        Assertions.assertEquals(ResolvingFileIO.class.getName(), sdk.get("io-impl"));

        raw.put("iceberg.rest.vended-credentials-enabled", " true ");
        IcebergCatalogFactory.preserveHadoopGcsFileIO(IcebergCatalogProperties.of(raw), sdk, conf);
        Assertions.assertEquals("rest".equals(flavor) ? ResolvingFileIO.class.getName()
                : NativeGcsResolvingFileIO.class.getName(), sdk.get("io-impl"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "io-impl", "iceberg.rest.io-impl"})
    void readsWithHmacWhileRestUsesGoogleAuthentication(String alias) throws Exception {
        byte[] content = "fixture-metadata".getBytes(StandardCharsets.UTF_8);
        AtomicReference<String> authorization = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try (exchange) {
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
            }
        });
        server.start();
        try {
            Map<String, String> raw = hmacProperties();
            raw.put("gs.endpoint", "http://127.0.0.1:" + server.getAddress().getPort());
            raw.put("iceberg.rest.security.type", "google");
            raw.put("gcp.auth.credentials-json", "{\"type\":\"service_account\"}");
            if (!alias.isEmpty()) {
                raw.put(alias, ResolvingFileIO.class.getName());
            }
            GcsFileSystemProperties storage = GcsFileSystemProperties.of(raw);
            Map<String, String> options = options(raw, storage);
            Configuration conf = IcebergCatalogFactory.buildHadoopConfiguration(raw,
                    storage.toHadoopConfigurationMap());
            IcebergCatalogFactory.preserveHadoopGcsFileIO(IcebergCatalogProperties.of(raw), options, conf);
            conf.set("fs.s3a.endpoint.region", "us-east-1");
            conf.setBoolean("fs.s3a.connection.ssl.enabled", false);
            conf.setInt("fs.s3a.retry.limit", 0);
            conf.setInt("fs.s3a.attempts.maximum", 1);
            conf.set("fs.s3a.bucket.probe", "0");
            String bucket = "routing-hmac-" + (alias.isEmpty() ? "default" : alias.contains(".") ? "alias" : "raw");
            String location = "gs://" + bucket + "/metadata.json";
            try (ResolvingFileIO io = (ResolvingFileIO) CatalogUtil.loadFileIO(
                    options.get(CatalogProperties.FILE_IO_IMPL), options, conf)) {
                // Fail before opening if the wrong delegate would consult external credentials or storage.
                Assertions.assertEquals(HADOOP, io.ioClass(location).getName());
                try (SeekableInputStream input = io.newInputFile(location, content.length).newStream()) {
                    Assertions.assertArrayEquals(content, input.readAllBytes());
                }
                Assertions.assertNotNull(authorization.get());
                Assertions.assertTrue(authorization.get().contains("Credential=fixture-storage-key/"));
            }
        } finally {
            server.stop(0);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"gcs.decryption-key", "gcs.encryption-key", "gcs.user-project", "gcs.project-id",
            "gcs.client-lib-token", "gcs.service.host", "gcs.channel.read.chunk-size-bytes",
            "gcs.channel.write.chunk-size-bytes", "gcs.delete.batch-size", "gcs.analytics-core.enabled"})
    void preservesSdkOptionsAtInitialAndSerializedRuntimeSelection(String key) throws Exception {
        Map<String, String> raw = hmacProperties();
        raw.put(key, "fixture");
        Map<String, String> properties = options(raw, GcsFileSystemProperties.of(raw));
        try (ResolvingFileIO io = (ResolvingFileIO) CatalogUtil.loadFileIO(
                properties.get(CatalogProperties.FILE_IO_IMPL), properties, new Configuration(false))) {
            Assertions.assertEquals(GCS, io.ioClass(LOCATION).getName());
        }
        // A table response can add SDK options after the FE has selected the compatibility resolver.
        try (NativeGcsResolvingFileIO original = new NativeGcsResolvingFileIO()) {
            original.setConf(new Configuration(false));
            original.initialize(Map.of(key, "fixture"));
            try (ResolvingFileIO restored = SerializationUtil.deserializeFromBytes(
                    SerializationUtil.serializeToBytes(original))) {
                Assertions.assertEquals(GCS, restored.ioClass(LOCATION).getName());
                Assertions.assertEquals("org.apache.iceberg.aws.s3.S3FileIO",
                        restored.ioClass("s3://fixture-bucket/object").getName());
                Assertions.assertEquals(HADOOP, restored.ioClass("file:///fixture").getName());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"gs://fixture-bucket/", "gs://another-bucket/", "s3://fixture-bucket/"})
    void honorsOnlyMatchingStorageCredentialOptions(String prefix) throws Exception {
        try (NativeGcsResolvingFileIO original = new NativeGcsResolvingFileIO()) {
            original.setConf(new Configuration(false));
            original.initialize(Map.of());
            original.setCredentials(List.of(StorageCredential.create(prefix, Map.of("gcs.decryption-key", KEY))));
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
        Map<String, String> hadoop = Map.of("fs.gs.auth.type", "UNAUTHENTICATED");
        for (boolean narrowSdk : List.of(false, true)) {
            StorageCredential broad = StorageCredential.create("gs://fixture-bucket/",
                    narrowSdk ? hadoop : Map.of("gcs.oauth2.token", "fixture-token"));
            StorageCredential narrow = StorageCredential.create("gs://fixture-bucket/private/",
                    narrowSdk ? Map.of("gcs.oauth2.token", "fixture-token") : hadoop);
            for (List<StorageCredential> credentials : List.of(List.of(broad, narrow), List.of(narrow, broad),
                    List.of(StorageCredential.create(narrow.prefix(), broad.config()), narrow))) {
                for (boolean globalSdk : List.of(false, true)) {
                    try (NativeGcsResolvingFileIO original = new NativeGcsResolvingFileIO()) {
                        original.setConf(new Configuration(false));
                        original.initialize(globalSdk ? Map.of("gcs.project-id", "fixture-project") : Map.of());
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
    void readsCsekMetadataWithBillingAndVendedStorageAuthentication() throws Exception {
        Schema schema = new Schema(Types.NestedField.required(1, "value", Types.StringType.get()));
        TableMetadata metadata = TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(),
                SortOrder.unsorted(), "gs://fixture-bucket/table", Map.of());
        byte[] content = TableMetadataParser.toJson(metadata).getBytes(StandardCharsets.UTF_8);
        String hash = Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-256").digest(new byte[32]));
        AtomicInteger accepted = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try (exchange) {
                String query = exchange.getRequestURI().getRawQuery();
                boolean billing = (query != null && query.contains("userProject=fixture-billing"))
                        || "fixture-billing".equals(exchange.getRequestHeaders().getFirst("X-Goog-User-Project"));
                boolean authorized = "Bearer fixture-storage-token".equals(
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
                exchange.sendResponseHeaders(200, content.length);
                exchange.getResponseBody().write(content);
            }
        });
        server.start();
        try {
            Map<String, String> properties = Map.of("gcs.project-id", "fixture-project",
                    "gcs.service.host", "http://127.0.0.1:" + server.getAddress().getPort(),
                    "gcs.decryption-key", KEY, "gcs.user-project", "fixture-billing");
            try (NativeGcsResolvingFileIO original = new NativeGcsResolvingFileIO()) {
                original.setConf(new Configuration(false));
                original.initialize(properties);
                original.setCredentials(List.of(StorageCredential.create("gs://fixture-bucket/",
                        Map.of("gcs.oauth2.token", "fixture-storage-token"))));
                try (ResolvingFileIO io = SerializationUtil.deserializeFromBytes(
                        SerializationUtil.serializeToBytes(original))) {
                    Assertions.assertEquals(GCS, io.ioClass(LOCATION).getName());
                    TableMetadata actual = TableMetadataParser.read(io.newInputFile(LOCATION, content.length));
                    Assertions.assertEquals("value", actual.schema().findField(1).name());
                    Assertions.assertEquals("gs://fixture-bucket/table", actual.location());
                    Assertions.assertTrue(accepted.get() > 0);
                }
            }
        } finally {
            server.stop(0);
        }
    }

    private static Map<String, String> hmacProperties() {
        return new HashMap<>(Map.of("iceberg.catalog.type", "rest", "iceberg.rest.uri", "https://catalog.example.com",
                "warehouse", "gs://fixture-bucket/warehouse", "provider", "GCP",
                "gs.access_key", "fixture-storage-key", "gs.secret_key", "fixture-storage-secret",
                "use_path_style", "true", "iceberg.rest.vended-credentials-enabled", "false"));
    }

    private static Map<String, String> options(Map<String, String> raw, GcsFileSystemProperties storage) {
        return IcebergCatalogFactory.buildCatalogProperties(IcebergCatalogProperties.of(raw),
                Optional.of(storage), false);
    }
}
