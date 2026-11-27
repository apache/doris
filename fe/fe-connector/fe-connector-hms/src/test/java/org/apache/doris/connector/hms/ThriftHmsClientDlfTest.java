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

package org.apache.doris.connector.hms;

import org.apache.doris.connector.spi.ConnectorTableNotFoundException;

import com.aliyun.datalake20200710.external.okhttp3.OkHttpClient;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import shade.doris.hive.com.aliyun.tea.okhttp.ClientHelper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

class ThriftHmsClientDlfTest {
    private static SSLContext tls;
    private static X509TrustManager trust;

    @BeforeAll
    static void createSyntheticTlsIdentity(@TempDir Path directory) throws Exception {
        Path store = directory.resolve("test.p12");
        Path log = directory.resolve("keytool.log");
        Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
                "-genkeypair", "-alias", "test", "-keyalg", "RSA", "-keysize", "2048", "-validity", "1",
                "-dname", "CN=localhost", "-ext", "SAN=IP:127.0.0.1", "-storetype", "PKCS12",
                "-keystore", store.toString(), "-storepass", "test-password", "-keypass", "test-password",
                "-noprompt").redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            Assertions.assertTrue(process.waitFor(30, TimeUnit.SECONDS), "keytool did not finish");
            Assertions.assertEquals(0, process.exitValue(), Files.readString(log));
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly().waitFor();
            }
        }
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keys.load(input, "test-password".toCharArray());
        }
        KeyManagerFactory km = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        km.init(keys, "test-password".toCharArray());
        TrustManagerFactory tm = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tm.init(keys);
        trust = (X509TrustManager) Arrays.stream(tm.getTrustManagers())
                .filter(X509TrustManager.class::isInstance).findFirst().orElseThrow();
        tls = SSLContext.getInstance("TLS");
        tls.init(km.getKeyManagers(), tm.getTrustManagers(), null);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 2})
    void realDlfClientReadsMetadataAndPartitions(int poolSize) throws Exception {
        try (DlfService service = new DlfService();
                ThriftHmsClient client = service.client("test-catalog", poolSize)) {
            Assertions.assertEquals(List.of("test-catalog_db", "test-catalog_tail"), client.listDatabases());
            HmsTableInfo table = client.getTable("test_db", "hive_table");
            Assertions.assertEquals("test_db", table.getDbName());
            Assertions.assertEquals("hive_table", table.getTableName());
            Assertions.assertEquals("id", table.getColumns().get(0).getName());
            Assertions.assertEquals("p", table.getPartitionKeys().get(0).getName());
            Assertions.assertEquals(List.of("p=one"), client.listPartitionNames("test_db", "hive_table", -1));
            List<HmsPartitionInfo> partitions = client.getPartitions("test_db", "hive_table", List.of("p=one"));
            Assertions.assertEquals(1, partitions.size());
            Assertions.assertEquals(List.of("one"), partitions.get(0).getValues());
            Assertions.assertEquals("oss://example-bucket/data/p=one", partitions.get(0).getLocation());
            Assertions.assertTrue(service.requests.stream().anyMatch(s -> s.contains("NextPageToken=second")));
            Assertions.assertTrue(service.requests.stream().anyMatch(s -> s.contains("/partitions/batchget")));
        }
    }

    @Test
    void preservesHudiMetadataForGatewayDispatch() throws Exception {
        try (DlfService service = new DlfService(); ThriftHmsClient client = service.client("test-catalog", 1)) {
            Assertions.assertEquals("org.apache.hudi.hadoop.HoodieParquetInputFormat",
                    client.getTable("test_db", "hudi_table").getInputFormat());
        }
    }

    @Test
    void missingTableKeepsTypedIdentity() throws Exception {
        try (DlfService service = new DlfService(); ThriftHmsClient client = service.client("test-catalog", 1)) {
            HmsClientException error = Assertions.assertThrows(HmsClientException.class,
                    () -> client.getTable("test_db", "missing_table"));
            Assertions.assertInstanceOf(ConnectorTableNotFoundException.class, error.getCause());
            ConnectorTableNotFoundException missing = (ConnectorTableNotFoundException) error.getCause();
            Assertions.assertEquals("test_db", missing.getDatabaseName());
            Assertions.assertEquals("missing_table", missing.getTableName());
        }
    }

    @Test
    void remoteFailureIsNotConvertedToMissingTable() throws Exception {
        try (DlfService service = new DlfService(); ThriftHmsClient client = service.client("test-catalog", 1)) {
            HmsClientException error = Assertions.assertThrows(HmsClientException.class,
                    () -> client.getTable("test_db", "failed_table"));
            Assertions.assertFalse(error.getCause() instanceof ConnectorTableNotFoundException);
            Assertions.assertTrue(error.getMessage().contains("fixture unavailable"), error.toString());
        }
    }

    @Test
    void catalogClientsKeepTheirOwnConfiguration() throws Exception {
        try (DlfService service = new DlfService();
                ThriftHmsClient first = service.client("first", 1);
                ThriftHmsClient second = service.client("second", 1)) {
            Assertions.assertEquals(List.of("first_db", "first_tail"), first.listDatabases());
            Assertions.assertEquals(List.of("second_db", "second_tail"), second.listDatabases());
            Assertions.assertEquals(List.of("first_db", "first_tail"), first.listDatabases());
        }
    }

    @Test
    void sdkCallRestoresForeignThreadContextLoaderAndRejectsUseAfterClose() throws Exception {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try (DlfService service = new DlfService();
                URLClassLoader foreign = new URLClassLoader(new URL[0], null)) {
            ThriftHmsClient client = service.client("test-catalog", 1);
            try {
                Thread.currentThread().setContextClassLoader(foreign);
                Assertions.assertEquals(2, client.listDatabases().size());
                Assertions.assertSame(foreign, Thread.currentThread().getContextClassLoader());
            } finally {
                Thread.currentThread().setContextClassLoader(original);
                client.close();
            }
            Assertions.assertThrows(HmsClientException.class, client::listDatabases);
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    private static final class DlfService implements AutoCloseable {
        private final HttpsServer server;
        private final OkHttpClient transport;
        private final String transportKey;
        private final List<String> requests = Collections.synchronizedList(new java.util.ArrayList<>());

        private DlfService() throws IOException {
            server = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.setHttpsConfigurator(new HttpsConfigurator(tls));
            server.createContext("/", this::respond);
            transport = new OkHttpClient.Builder().sslSocketFactory(tls.getSocketFactory(), trust).build();
            transportKey = ClientHelper.getClientKey("127.0.0.1", server.getAddress().getPort());
            // Only this random endpoint trusts the generated certificate; JVM trust defaults stay untouched.
            Assertions.assertNull(ClientHelper.clients.putIfAbsent(transportKey, transport));
            server.start();
        }

        private ThriftHmsClient client(String catalogId, int poolSize) {
            Map<String, String> properties = new LinkedHashMap<>();
            properties.put("hive.metastore.type", "dlf");
            properties.put("dlf.catalog.endpoint", "127.0.0.1:" + server.getAddress().getPort());
            properties.put("dlf.catalog.proxyMode", "DLF_ONLY");
            properties.put("dlf.catalog.accessKeyId", "test-key-" + catalogId);
            properties.put("dlf.catalog.accessKeySecret", "test-secret");
            properties.put("dlf.catalog.region", "test-region");
            properties.put("dlf.catalog.id", catalogId);
            properties.put("dlf.catalog.createDefaultDBIfNotExist", "false");
            properties.put("dlf.catalog.client.readTimeout", "1000");
            return new ThriftHmsClient(new HmsClientConfig(properties, poolSize), Callable::call);
        }

        private void respond(HttpExchange exchange) throws IOException {
            String query = exchange.getRequestURI().getRawQuery();
            Map<String, String> values = new LinkedHashMap<>();
            if (query != null) {
                for (String pair : query.split("&")) {
                    String[] kv = pair.split("=", 2);
                    values.put(URLDecoder.decode(kv[0], StandardCharsets.UTF_8), kv.length == 1 ? ""
                            : URLDecoder.decode(kv[1], StandardCharsets.UTF_8));
                }
            }
            String path = exchange.getRequestURI().getPath();
            String requestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            requests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI() + " " + requestBody);
            String body;
            if (path.endsWith("/databases/list")) {
                String catalog = values.getOrDefault("CatalogId", "test-catalog");
                boolean last = "second".equals(values.get("NextPageToken"));
                body = "\"Databases\":[{\"Name\":\"" + catalog + (last ? "_tail" : "_db")
                        + "\"}],\"NextPageToken\":\"" + (last ? "" : "second") + "\"";
            } else if (path.endsWith("/partitions/names")) {
                body = "\"PartitionNames\":[\"p=one\"],\"NextPageToken\":\"\"";
            } else if (path.endsWith("/partitions/batchget")) {
                body = "\"Partitions\":[{\"DatabaseName\":\"test_db\",\"TableName\":\"hive_table\","
                        + "\"Values\":[\"one\"],\"Parameters\":{},\"Sd\":"
                        + storageDescriptor("oss://example-bucket/data/p=one", false) + "}],\"PartitionErrors\":[]";
            } else if (path.endsWith("/databases/tables")) {
                String table = values.getOrDefault("TableName", "hive_table");
                if ("missing_table".equals(table) || "failed_table".equals(table)) {
                    send(exchange, "{\"Success\":false,\"Code\":\""
                            + ("missing_table".equals(table) ? "NoSuchObject" : "InternalError")
                            + "\",\"Message\":\"fixture unavailable\",\"RequestId\":\"test-request\"}");
                    return;
                }
                body = "\"Table\":{\"DatabaseName\":\"test_db\",\"TableName\":\"" + table
                        + "\",\"TableType\":\"EXTERNAL_TABLE\",\"Parameters\":{},"
                        + "\"PartitionKeys\":[{\"Name\":\"p\",\"Type\":\"string\"}],\"Sd\":"
                        + storageDescriptor("oss://example-bucket/data", "hudi_table".equals(table)) + "}";
            } else {
                send(exchange, "{\"Success\":false,\"Code\":\"InvalidOperation\","
                        + "\"Message\":\"unexpected fixture request\",\"RequestId\":\"test-request\"}");
                return;
            }
            send(exchange, "{\"Success\":true,\"Code\":\"OK\",\"RequestId\":\"test-request\"," + body + "}");
        }

        private static String storageDescriptor(String location, boolean hudi) {
            return "{\"Cols\":[{\"Name\":\"id\",\"Type\":\"bigint\"}],\"Location\":\"" + location
                    + "\",\"InputFormat\":\"" + (hudi ? "org.apache.hudi.hadoop.HoodieParquetInputFormat"
                            : "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
                    + "\",\"OutputFormat\":\"org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat\","
                    + "\"Parameters\":{},\"SerDeInfo\":{\"SerializationLib\":"
                    + "\"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe\",\"Parameters\":{}}}";
        }

        private static void send(HttpExchange exchange, String response) throws IOException {
            byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            try (exchange; OutputStream output = exchange.getResponseBody()) {
                output.write(bytes);
            }
        }

        @Override
        public void close() {
            server.stop(0);
            ClientHelper.clients.remove(transportKey, transport);
            transport.connectionPool().evictAll();
            transport.dispatcher().executorService().shutdownNow();
        }
    }
}
