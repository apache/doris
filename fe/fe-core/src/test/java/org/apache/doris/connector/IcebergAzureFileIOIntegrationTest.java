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

import org.apache.doris.connector.iceberg.IcebergConnector;
import org.apache.doris.connector.iceberg.IcebergTableHandle;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStorageContext;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.fs.FileSystemPluginManager;
import org.apache.doris.kerberos.ExecutionAuthenticator;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.azure.adlsv2.ADLSFileIO;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ConfigResponseParser;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadTableResponseParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/** Official Iceberg/Azure SDK reads; only the storage HTTP service is replaced. */
class IcebergAzureFileIOIntegrationTest {
    private static final String TOKEN = "sv=2024-11-04&sp=r&sig=unit-test-signature&se=2100-01-01T00:00:00Z";
    private static final String LOCATION = "abfs://container@127.0.0.1/table";
    private static final String REST_LOCATION = "abfss://container@account.dfs.core.windows.net/table";
    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    @Test
    void restCatalogUsesTheBoundSasForItsActualTableFileIO() throws Exception {
        try (RestFixture fixture = new RestFixture(Map.of(
                "azure.account_name", "account", "azure.sas_token", TOKEN))) {
            Table table = fixture.load();
            // Fail before SDK credential discovery if the production wiring did not install the provider output.
            Assertions.assertEquals(TOKEN, table.io().properties().get("adls.sas-token.account.dfs.core.windows.net"));

            TableMetadata actual = TableMetadataParser.read(table.io(), REST_LOCATION + "/v1.metadata.json");

            Assertions.assertEquals(fixture.metadata.uuid(), actual.uuid());
            Assertions.assertEquals(fixture.metadata.schema().asStruct(), actual.schema().asStruct());
            Assertions.assertFalse(fixture.queries.isEmpty());
            Assertions.assertTrue(fixture.queries.stream().allMatch(query -> query != null
                    && query.contains("sig=unit-test-signature")));
        }
    }

    @Test
    void restVendedSasReplacesTheWholeStaticAuthenticationGroup() throws Exception {
        String fresh = "si=stored-access-policy&sig=rotated-test-signature";
        try (RestFixture fixture = new RestFixture(Map.of(
                "azure.account_name", "account", "azure.sas_token", TOKEN))) {
            fixture.tableConfig.put("adls.sas-token.account.dfs.core.windows.net", fresh);

            Table table = fixture.load();
            Map<String, String> fileIOProperties = table.io().properties();

            Assertions.assertEquals(fresh, fileIOProperties.get("adls.sas-token.account.dfs.core.windows.net"));
            Assertions.assertEquals(fresh, fileIOProperties.get("adls.sas-token.account.blob.core.windows.net"));
            Assertions.assertTrue(fileIOProperties.keySet().stream()
                    .noneMatch(key -> key.startsWith("adls.sas-token-expires-at-ms.")),
                    "unknown new expiry must not inherit the old credential's expiry");
            Map<String, String> nativeProperties = fixture.context.resolveStorageProperties(fileIOProperties).stream()
                    .filter(binding -> binding.providerName().equals("AZURE")).findFirst().orElseThrow()
                    .toBackendProperties().orElseThrow().toMap();
            Assertions.assertEquals(fresh, nativeProperties.get("AZURE_SAS_TOKEN"));
            Assertions.assertFalse(nativeProperties.containsKey("AZURE_SAS_EXPIRY_MS"));

            TableMetadata actual = TableMetadataParser.read(table.io(), REST_LOCATION + "/v1.metadata.json");

            Assertions.assertEquals(fixture.metadata.uuid(), actual.uuid());
            Assertions.assertFalse(fixture.queries.isEmpty());
            Assertions.assertTrue(fixture.queries.stream().allMatch(query -> query != null
                    && query.contains("sig=rotated-test-signature")));
        }
    }

    @Test
    void restCanLoadFreshVendedSasWhenTheStaticSasIsAlreadyExpired() throws Exception {
        try (RestFixture fixture = new RestFixture(Map.of(
                "azure.account_name", "account", "azure.sas_token", "sig=expired-static-test-signature",
                "azure.sas_expiry_ms", "1"))) {
            fixture.tableConfig.put("adls.sas-token.account.dfs.core.windows.net", TOKEN);

            Table table = fixture.load();

            Assertions.assertEquals(TOKEN, table.io().properties().get("adls.sas-token.account.dfs.core.windows.net"));
            Assertions.assertEquals("4102444800000",
                    table.io().properties().get("adls.sas-token-expires-at-ms.account.dfs.core.windows.net"));
            TableMetadata actual = TableMetadataParser.read(table.io(), REST_LOCATION + "/v1.metadata.json");
            Assertions.assertEquals(fixture.metadata.uuid(), actual.uuid());
            Assertions.assertFalse(fixture.queries.isEmpty());
            Assertions.assertTrue(fixture.queries.stream().allMatch(query -> query != null
                    && query.contains("sig=unit-test-signature")));
        }
    }

    @Test
    void restVendedSasDoesNotInheritRawCatalogAdlsCredentials() throws Exception {
        String fresh = "si=stored-access-policy&sig=rotated-test-signature";
        try (RestFixture fixture = new RestFixture(Map.of(
                "adls.sas-token.account.dfs.core.windows.net", "sig=old-test-signature",
                "adls.sas-token.account.blob.core.windows.net", "sig=old-test-signature",
                "adls.sas-token-expires-at-ms.account.dfs.core.windows.net", "1",
                "adls.sas-token-expires-at-ms.account.blob.core.windows.net", "1"))) {
            fixture.tableConfig.put("adls.sas-token.account.dfs.core.windows.net", fresh);

            Table table = fixture.load();

            Assertions.assertEquals(fresh, table.io().properties().get("adls.sas-token.account.dfs.core.windows.net"));
            Assertions.assertEquals(fresh, table.io().properties().get("adls.sas-token.account.blob.core.windows.net"));
            Assertions.assertTrue(table.io().properties().keySet().stream()
                    .noneMatch(key -> key.startsWith("adls.sas-token-expires-at-ms.")),
                    "REST catalog inheritance must not restore the replaced credential's expiry");
            TableMetadata actual = TableMetadataParser.read(table.io(), REST_LOCATION + "/v1.metadata.json");
            Assertions.assertEquals(fixture.metadata.uuid(), actual.uuid());
            Assertions.assertFalse(fixture.queries.isEmpty());
            Assertions.assertTrue(fixture.queries.stream().allMatch(query -> query != null
                    && query.contains("sig=rotated-test-signature")));
        }
    }

    @Test
    void nativeClientCredentialsTakePrecedenceOverServerDefaults() throws Exception {
        ConfigResponse serverConfig = ConfigResponse.builder()
                .withDefault("adls.sas-token.account.dfs.core.windows.net", "sig=server-default-test-signature")
                .build();
        try (RestFixture fixture = new RestFixture(Map.of(
                "azure.account_name", "account", "azure.sas_token", TOKEN), serverConfig)) {
            Table table = fixture.load();

            Assertions.assertEquals(TOKEN, table.io().properties().get("adls.sas-token.account.dfs.core.windows.net"),
                    "typed catalog credentials have the same client priority as raw Iceberg properties");
            TableMetadata actual = TableMetadataParser.read(table.io(), REST_LOCATION + "/v1.metadata.json");
            Assertions.assertEquals(fixture.metadata.uuid(), actual.uuid());
            Assertions.assertFalse(fixture.queries.isEmpty());
            Assertions.assertTrue(fixture.queries.stream().allMatch(query -> query != null
                    && query.contains("sig=unit-test-signature")));
        }
    }

    @Test
    void staticSasAuthenticatesAnOfficialFileIOMetadataRead() throws Exception {
        TableMetadata expected = TableMetadata.newTableMetadata(
                SCHEMA, PartitionSpec.unpartitioned(), LOCATION, Collections.emptyMap());
        byte[] bytes = TableMetadataParser.toJson(expected).getBytes(StandardCharsets.UTF_8);
        CopyOnWriteArrayList<String> queries = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/container/table/v1.metadata.json", exchange -> serve(exchange, bytes, queries));
        server.start();
        try (ADLSFileIO fileIO = new ADLSFileIO()) {
            FileSystemProperties storage = StorageAdapter.ofProvider("AZURE", Map.of(
                    "azure.account_name", "account",
                    "azure.endpoint", "http://127.0.0.1:" + server.getAddress().getPort(),
                    "azure.sas_token", TOKEN)).getSpiProperties();
            Map<String, String> fileIOProperties = storage.toIcebergFileIOProperties();
            // Fail before opening an unconfigured SDK client: no ambient credential discovery is allowed.
            Assertions.assertEquals(TOKEN, fileIOProperties.get("adls.sas-token.127.0.0.1"));
            fileIO.initialize(fileIOProperties);

            TableMetadata actual = TableMetadataParser.read(fileIO, LOCATION + "/v1.metadata.json");

            Assertions.assertEquals(expected.uuid(), actual.uuid());
            Assertions.assertEquals(expected.schema().asStruct(), actual.schema().asStruct());
            Assertions.assertFalse(queries.isEmpty(), "metadata must be read from the storage service");
            Assertions.assertTrue(queries.stream().allMatch(query -> query != null
                    && query.contains("sig=unit-test-signature")), "every storage request must carry SAS");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void sharedKeyAuthenticatesAnOfficialFileIOMetadataRead() throws Exception {
        String key = "dW5pdC10ZXN0LXNoYXJlZC1rZXk=";
        TableMetadata expected = TableMetadata.newTableMetadata(
                SCHEMA, PartitionSpec.unpartitioned(), LOCATION, Collections.emptyMap());
        byte[] bytes = TableMetadataParser.toJson(expected).getBytes(StandardCharsets.UTF_8);
        CopyOnWriteArrayList<String> queries = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<String> authorizations = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/container/table/v1.metadata.json", exchange -> {
            authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
            serve(exchange, bytes, queries);
        });
        server.start();
        try (ADLSFileIO fileIO = new ADLSFileIO()) {
            FileSystemProperties storage = StorageAdapter.ofProvider("AZURE", Map.of(
                    "azure.account_name", "account",
                    "azure.endpoint", "http://127.0.0.1:" + server.getAddress().getPort(),
                    "azure.account_key", key)).getSpiProperties();
            Map<String, String> fileIOProperties = storage.toIcebergFileIOProperties();
            Assertions.assertEquals(key, fileIOProperties.get("adls.auth.shared-key.account.key"));
            fileIO.initialize(fileIOProperties);

            TableMetadata actual = TableMetadataParser.read(fileIO, LOCATION + "/v1.metadata.json");

            Assertions.assertEquals(expected.uuid(), actual.uuid());
            Assertions.assertEquals(expected.schema().asStruct(), actual.schema().asStruct());
            Assertions.assertFalse(authorizations.isEmpty());
            Assertions.assertTrue(authorizations.stream().allMatch(authorization -> authorization != null
                    && authorization.startsWith("SharedKey account:")), "every request must be signed with SharedKey");
            Assertions.assertTrue(queries.stream().allMatch(query -> query == null || !query.contains("sig=")));
        } finally {
            server.stop(0);
        }
    }

    private static void serve(HttpExchange exchange, byte[] bytes, CopyOnWriteArrayList<String> queries)
            throws IOException {
        try {
            queries.add(exchange.getRequestURI().getRawQuery());
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.getResponseHeaders().set("Content-Length", Integer.toString(bytes.length));
            exchange.getResponseHeaders().set("ETag", "\"unit-test-etag\"");
            exchange.getResponseHeaders().set("Last-Modified", "Wed, 09 Sep 2026 00:00:00 GMT");
            exchange.getResponseHeaders().set("x-ms-blob-type", "BlockBlob");
            if ("HEAD".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(200, -1);
            } else {
                String range = exchange.getRequestHeaders().getFirst("Range");
                if (range == null) {
                    range = exchange.getRequestHeaders().getFirst("x-ms-range");
                }
                int start = 0;
                int end = bytes.length - 1;
                if (range != null) {
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
            }
        } finally {
            exchange.close();
        }
    }

    private static void serveJson(HttpExchange exchange, String json) throws IOException {
        try {
            byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
        } finally {
            exchange.close();
        }
    }

    static final class RestFixture implements AutoCloseable {
        private final FileSystemPluginManager previousManager = FileSystemFactory.getPluginManager();
        private final HttpServer server;
        private final TableMetadata metadata;
        private final CopyOnWriteArrayList<String> queries = new CopyOnWriteArrayList<>();
        private final Map<String, String> tableConfig = new ConcurrentHashMap<>();
        private final AtomicInteger tableLoadRequests = new AtomicInteger();
        private final CapturingContext context;
        private final IcebergConnector connector;

        RestFixture(Map<String, String> storageProperties) throws IOException {
            this(storageProperties, ConfigResponse.builder().build());
        }

        RestFixture(Map<String, String> storageProperties, ConfigResponse serverConfig) throws IOException {
            this(storageProperties, serverConfig, REST_LOCATION);
        }

        RestFixture(Map<String, String> storageProperties, ConfigResponse serverConfig, String metadataLocation)
                throws IOException {
            this(storageProperties, serverConfig, metadataLocation, metadataLocation);
        }

        RestFixture(Map<String, String> storageProperties, ConfigResponse serverConfig, String metadataLocation,
                String dataLocation)
                throws IOException {
            metadata = TableMetadata.newTableMetadata(
                    SCHEMA, PartitionSpec.unpartitioned(), dataLocation, Collections.emptyMap());
            String json = TableMetadataParser.toJson(metadata);
            TableMetadata located = TableMetadataParser.fromJson(metadataLocation + "/v1.metadata.json", json);
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            String endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
            tableConfig.put("adls.connection-string.account.dfs.core.windows.net", endpoint);
            server.createContext("/v1/config", exchange -> serveJson(exchange,
                    ConfigResponseParser.toJson(serverConfig)));
            server.createContext("/v1/namespaces/db/tables/t", exchange -> {
                if ("GET".equals(exchange.getRequestMethod())) {
                    tableLoadRequests.incrementAndGet();
                }
                serveJson(exchange, LoadTableResponseParser.toJson(LoadTableResponse.builder()
                        .withTableMetadata(located).addAllConfig(new HashMap<>(tableConfig)).build()));
            });
            server.createContext("/container/table/v1.metadata.json", exchange -> serve(exchange,
                    json.getBytes(StandardCharsets.UTF_8), queries));
            server.createContext("/bucket/table/v1.metadata.json", exchange -> serve(exchange,
                    json.getBytes(StandardCharsets.UTF_8), queries));
            Map<String, String> properties = new HashMap<>(Map.of(
                    "iceberg.catalog.type", "rest", "uri", endpoint, "rest.auth.type", "none",
                    "iceberg.rest.vended-credentials-enabled", "true"));
            properties.putAll(storageProperties);
            // Match FE startup: both the static and request-local bindings use the same typed
            // provider registry, rather than the two distinct pre-startup fallback paths.
            FileSystemPluginManager manager = new FileSystemPluginManager();
            manager.loadBuiltins();
            FileSystemFactory.initPluginManager(manager);
            context = new CapturingContext(properties);
            connector = new IcebergConnector(properties, context);
            server.start();
        }

        Table load() {
            // getMetadata-funnel-exempt: exercise the real connector/REST/engine/provider boundary offline.
            connector.getMetadata(new TestSession()).getTableSchema(
                    new TestSession(), new IcebergTableHandle("db", "t"));
            Assertions.assertNotNull(context.table, "capture the actual authenticated REST table load");
            return context.table;
        }

        Table loadForRefresh() {
            load();
            Assertions.assertNotNull(context.catalog, "capture the connector's actual SDK catalog");
            // Load from the SDK catalog, not Doris' immutable statement-snapshot table.
            return context.catalog.loadTable(TableIdentifier.of("db", "t"));
        }

        int tableLoadRequestCount() {
            return tableLoadRequests.get();
        }

        ConnectorStorageContext storageContext() {
            return context;
        }

        String endpoint() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        void tableConfig(Map<String, String> properties) {
            tableConfig.putAll(properties);
        }

        void removeTableConfig(String key) {
            tableConfig.remove(key);
        }

        @Override
        public void close() throws Exception {
            try {
                connector.close();
            } finally {
                try {
                    context.close();
                } finally {
                    server.stop(0);
                    FileSystemFactory.initPluginManager(previousManager);
                }
            }
        }
    }

    private static final class CapturingContext extends DefaultConnectorContext {
        private Table table;
        private Catalog catalog;

        private CapturingContext(Map<String, String> properties) {
            super("azure_rest_test", 1L, () -> new ExecutionAuthenticator() {}, Collections::emptyMap, () -> properties);
        }

        @Override
        public <T> T executeAuthenticated(Callable<T> task) throws Exception {
            T result = super.executeAuthenticated(task);
            if (result instanceof Table) {
                table = (Table) result;
            }
            if (result instanceof Catalog) {
                catalog = (Catalog) result;
            }
            return result;
        }
    }

    private static final class TestSession implements ConnectorSession {
        @Override
        public String getQueryId() {
            return "azure-rest-test";
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
            return "azure_rest_test";
        }

        @Override
        public <T> T getProperty(String name, Class<T> type) {
            return null;
        }

        @Override
        public Map<String, String> getCatalogProperties() {
            return Collections.emptyMap();
        }
    }
}
