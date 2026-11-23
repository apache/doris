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


package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.metastore.deltalake.uc.UnityCatalogProperties;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.mvcc.ConnectorTimeTravelSpec;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.delta.kernel.defaults.engine.fileio.SeekableInputStream;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

class DeltaLakeUnityCatalogTest {
    private HttpServer server;
    private String uri;
    private static final String EXTERNAL = "{\"name\":\"t\",\"data_source_format\":\"DELTA\","
            + "\"table_type\":\"EXTERNAL\",\"storage_location\":\"s3://warehouse/t\",\"table_id\":\"table-1\"}";

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        uri = "http://127.0.0.1:" + server.getAddress().getPort();
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    @Test
    void paginatesEmptyPagesAndFiltersForeignFormats() throws Exception {
        route("schemas", exchange -> reply(exchange, 200, exchange.getRequestURI().getQuery().contains("page_token=")
                ? "{\"schemas\":[{\"name\":\"db\"}]}" : "{\"schemas\":[],\"next_page_token\":\"next+page\"}"));
        route("tables", exchange -> reply(exchange, 200, "{\"tables\":[" + EXTERNAL + ","
                + EXTERNAL.replace("DELTA", "PARQUET") + "," + EXTERNAL.replace("EXTERNAL", "VIEW") + "]}"));
        try (DeltaLakeUnityCatalog uc = catalog("configured")) {
            Assertions.assertEquals(List.of("db"), uc.listDatabases());
            Assertions.assertEquals(List.of("t"), uc.listTables("db"));
        }
    }

    @Test
    void configuredExternalStorageRequiresReadGrantEvenWhenTheTableIsVisible() throws Exception {
        route("tables/main.db.t", exchange -> reply(exchange, 200, EXTERNAL));
        route("tables/main.db.missing", exchange -> reply(exchange, 404, "{}"));
        route("temporary-table-credentials", exchange -> reply(exchange, 403, "{\"message\":\"private body\"}"));
        try (DeltaLakeUnityCatalog uc = catalog("configured")) {
            Assertions.assertTrue(uc.tableLocation("db", "missing").isEmpty());
            DorisConnectorException denied = Assertions.assertThrows(DorisConnectorException.class,
                    () -> uc.tableLocation("db", "t"));
            Assertions.assertTrue(denied.getMessage().contains("403"));
            Assertions.assertFalse(denied.getMessage().contains("private body"));
        }
    }

    @Test
    void rejectsGovernedTablesBeforeTouchingStorage() throws Exception {
        route("tables/main.db.masked", exchange -> reply(exchange, 200,
                EXTERNAL.replace("{\"name\"", "{\"columns\":[{\"mask\":{}}],\"name\"")));
        route("tables/main.db.filtered", exchange -> reply(exchange, 200,
                EXTERNAL.replace("{\"name\"", "{\"row_filter\":{},\"name\"")));
        try (DeltaLakeUnityCatalog uc = catalog("configured")) {
            Assertions.assertThrows(DorisConnectorException.class, () -> uc.tableLocation("db", "masked"));
            Assertions.assertThrows(DorisConnectorException.class, () -> uc.tableLocation("db", "filtered"));
        }
    }

    @Test
    void sendsOnlyTheFreshTableReadGrantToTheBackend() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        String location = "s3://warehouse/my folder/t";
        route("tables/main.db.t", exchange -> reply(exchange, 200, EXTERNAL.replace("s3://warehouse/t", location)));
        route("temporary-table-credentials", exchange -> {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            if (!body.contains("\"operation\":\"READ\"") || !body.contains("\"table_id\":\"table-1\"")) {
                reply(exchange, 400, "{}");
                return;
            }
            int n = calls.incrementAndGet();
            String expiry = n == 1 ? "null" : String.valueOf(System.currentTimeMillis() + 3600000);
            reply(exchange, 200, "{\"expiration_time\":" + expiry
                    + ",\"aws_temp_credentials\":{\"access_key_id\":\"id\",\"secret_access_key\":\"test\","
                    + "\"session_token\":\"grant-" + n + "\"}}");
        });
        try (DeltaLakeUnityCatalog uc = catalog("vended")) {
            DeltaLakeTableReference table = uc.tableLocation("db", "t").orElseThrow();
            Assertions.assertEquals(location, table.path);
            Map<String, String> be = uc.backendProperties(table, Map.of("AWS_REGION", "us-east-1",
                    "AWS_MAX_CONNECTIONS", "97", "AWS_REQUEST_TIMEOUT_MS", "45000",
                    "AWS_CONNECTION_TIMEOUT_MS", "19000",
                    "AWS_ACCESS_KEY", "catalog-id", "AWS_SECRET_KEY", "catalog-secret", "AWS_TOKEN", "catalog-token",
                    "AWS_ROLE_ARN", "unrelated-role"));
            Assertions.assertEquals("grant-2", be.get("AWS_TOKEN"));
            Assertions.assertEquals("id", be.get("AWS_ACCESS_KEY"));
            Assertions.assertEquals("test", be.get("AWS_SECRET_KEY"));
            Assertions.assertFalse(be.containsKey(UnityCatalogProperties.TOKEN));
            Assertions.assertFalse(be.containsKey("AWS_ROLE_ARN"));
            Assertions.assertEquals("us-east-1", be.get("AWS_REGION"));
            Assertions.assertEquals("97", be.get("AWS_MAX_CONNECTIONS"));
            Assertions.assertEquals("45000", be.get("AWS_REQUEST_TIMEOUT_MS"));
            Assertions.assertEquals("19000", be.get("AWS_CONNECTION_TIMEOUT_MS"));
        }
    }

    @Test
    void configuredAzureUsesCatalogStorageAndRejectsManagedCloudTables() throws Exception {
        String table = EXTERNAL.replace("s3://warehouse/t", "abfss://lake@account.dfs.core.windows.net/t");
        route("tables/main.db.t", exchange -> reply(exchange, 200, table));
        route("tables/main.db.managed", exchange -> reply(exchange, 200, table.replace("EXTERNAL", "MANAGED")));
        route("temporary-table-credentials", exchange -> reply(exchange, 200, "{}"));
        try (DeltaLakeUnityCatalog uc = catalog("configured")) {
            DeltaLakeTableReference reference = uc.tableLocation("db", "t").orElseThrow();
            Map<String, String> own = Map.of("test-key", "configured");
            Assertions.assertSame(own, uc.backendProperties(reference, own));
            Assertions.assertThrows(DorisConnectorException.class, () -> uc.tableLocation("db", "managed"));
        }
        try (DeltaLakeUnityCatalog uc = catalog("vended")) {
            Assertions.assertThrows(DorisConnectorException.class, () -> uc.tableLocation("db", "t"));
        }
    }

    @Test
    void refusesConfiguredStorageWhenReadAccessIsRevokedAfterLookup() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        route("tables/main.db.t", exchange -> reply(exchange, 200, EXTERNAL));
        route("temporary-table-credentials", exchange -> reply(exchange,
                requests.incrementAndGet() == 1 ? 200 : 403, "{}"));
        try (DeltaLakeUnityCatalog uc = catalog("configured")) {
            DeltaLakeTableReference table = uc.tableLocation("db", "t").orElseThrow();
            Assertions.assertThrows(DorisConnectorException.class, () -> uc.backendProperties(table, Map.of()));
        }
    }

    @Test
    void refreshesOAuthMachineTokens() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        server.createContext("/oidc/v1/token", exchange -> {
            int n = calls.incrementAndGet();
            reply(exchange, 200, "{\"access_token\":\"oauth-" + n + "\",\"expires_in\":0}");
        });
        server.createContext("/api/2.1/unity-catalog/schemas", exchange -> {
            String authorization = exchange.getRequestHeaders().getFirst("Authorization");
            reply(exchange, ("Bearer oauth-" + calls.get()).equals(authorization) ? 200 : 403, "{}");
        });
        Map<String, String> raw = Map.of("deltalake.catalog.type", "uc", UnityCatalogProperties.URI_KEY, uri,
                UnityCatalogProperties.NAMESPACE, "main", UnityCatalogProperties.CLIENT_ID, "test-id",
                UnityCatalogProperties.SECRET, "test-secret", UnityCatalogProperties.TOKEN_URI, uri + "/oidc/v1/token");
        try (DeltaLakeUnityCatalog uc = new DeltaLakeUnityCatalog(
                (UnityCatalogProperties) DeltaLakeCatalogProperties.of(raw).getMetastore(), new Configuration(false),
                        new RecordingConnectorContext())) {
            uc.listDatabases();
            uc.listDatabases();
            Assertions.assertEquals(2, calls.get());
        }
    }

    @Test
    void renewsExpiredGrantAndKeepsAnOpenStreamAliveOnCatalogClose(@TempDir Path dir) throws Exception {
        Path file = Files.write(dir.resolve("log"), new byte[] {5, 9});
        Map<String, String> aws = Map.of("AWS_ACCESS_KEY", "ak", "AWS_SECRET_KEY", "test", "AWS_TOKEN", "initial");
        AtomicInteger renewals = new AtomicInteger();
        Configuration storage = new Configuration();
        storage.set("fs.s3a.endpoint.region", "us-east-1");
        storage.set("fs.s3a.bucket.warehouse.aws.credentials.provider", "another-identity");
        storage.set("hadoop.security.credential.provider.path", "irrelevant-store");
        DeltaLakeVendedFileIO.Clients clients = new DeltaLakeVendedFileIO.Clients(storage,
                new RecordingConnectorContext());
        try (clients; DeltaLakeVendedFileIO io = new DeltaLakeVendedFileIO(clients,
                new DeltaLakeVendedFileIO.Grant("table-1", aws, 1), () -> {
                    renewals.incrementAndGet();
                    return new DeltaLakeVendedFileIO.Grant("table-1", aws, System.currentTimeMillis() + 3_600_000);
                })) {
            Assertions.assertEquals("us-east-1", io.getConf("fs.s3a.endpoint.region").orElseThrow());
            Assertions.assertTrue(io.getConf("fs.s3a.bucket.warehouse.aws.credentials.provider").isEmpty());
            Assertions.assertNotEquals("irrelevant-store",
                    io.getConf("hadoop.security.credential.provider.path").orElse(""));
            String path = file.toUri().toString();
            Assertions.assertEquals(2, io.newInputFile(path, -1).length());
            try (SeekableInputStream input = io.newInputFile(path, 2).newStream()) {
                clients.close();
                Assertions.assertEquals(5, input.read());
                input.seek(1);
                Assertions.assertEquals(9, input.read());
            }
            Assertions.assertEquals(1, renewals.get());
        }
    }

    @Test
    void resolvesHistoricalVersionThroughUnityCatalog(@TempDir Path dir) throws Exception {
        Path table = dir.resolve("t");
        DeltaLakeTestTables.commit(table, 0, "delta-uuid", new StructType().add("old_name", LongType.LONG));
        DeltaLakeTestTables.commit(table, 1, "delta-uuid", new StructType().add("new_name", StringType.STRING));
        route("tables/main.db.t", exchange -> reply(exchange, 200,
                EXTERNAL.replace("s3://warehouse/t", table.toUri().toString())));
        route("temporary-table-credentials", exchange -> reply(exchange, 200, "{}"));
        Map<String, String> raw = Map.of("deltalake.catalog.type", "uc",
                UnityCatalogProperties.URI_KEY, uri, UnityCatalogProperties.NAMESPACE, "main",
                UnityCatalogProperties.TOKEN, "metadata-only");
        TestingConnectorSession session = new TestingConnectorSession("uc-versioned-read");
        try (Connector connector = new DeltaLakeConnectorProvider().create(raw, new RecordingConnectorContext());
                ConnectorMetadata metadata = connector.getMetadata(session)) {
            ConnectorTableHandle latest = metadata.getTableHandle(session, "db", "t").orElseThrow();
            ConnectorMvccSnapshot v0 = metadata.resolveTimeTravel(session, latest,
                    ConnectorTimeTravelSpec.snapshotId("0")).orElseThrow();
            Assertions.assertEquals(0, v0.getSnapshotId());
            Assertions.assertEquals(Set.of("old_name"), metadata.getColumnHandles(session, latest, v0).keySet());
            DeltaLakeTableHandle pinned = (DeltaLakeTableHandle) metadata.applySnapshot(session, latest, v0);
            Assertions.assertEquals("table-1", pinned.reference().registrationId);
            Assertions.assertSame(pinned, metadata.applySnapshot(session, latest, v0));
        }
    }

    private DeltaLakeUnityCatalog catalog(String mode) {
        Map<String, String> raw = Map.of("deltalake.catalog.type", "uc",
                UnityCatalogProperties.URI_KEY, uri, UnityCatalogProperties.NAMESPACE, "main",
                UnityCatalogProperties.TOKEN, "metadata-only", "deltalake.uc.storage.mode", mode);
        return new DeltaLakeUnityCatalog((UnityCatalogProperties) DeltaLakeCatalogProperties.of(raw).getMetastore(),
                new Configuration(false), new RecordingConnectorContext());
    }

    private void route(String relative, HttpHandler handler) {
        server.createContext("/api/2.1/unity-catalog/" + relative, exchange -> {
            if (!"Bearer metadata-only".equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                reply(exchange, 401, "{}");
                return;
            }
            handler.handle(exchange);
        });
    }

    private static void reply(HttpExchange exchange, int status, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream response = exchange.getResponseBody()) {
            response.write(bytes);
        }
    }
}
