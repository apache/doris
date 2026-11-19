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

import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.metastore.deltalake.uc.UnityCatalogProperties;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.delta.kernel.defaults.engine.fileio.SeekableInputStream;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StructType;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
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
        route("tables/main.db.t", exchange -> reply(exchange, 200, EXTERNAL));
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
            Map<String, String> be = uc.backendProperties(table, Map.of("AWS_REGION", "us-east-1",
                    "AWS_ROLE_ARN", "unrelated-role"));
            Assertions.assertEquals("grant-2", be.get("AWS_TOKEN"));
            Assertions.assertEquals("id", be.get("AWS_ACCESS_KEY"));
            Assertions.assertEquals("test", be.get("AWS_SECRET_KEY"));
            Assertions.assertFalse(be.containsKey(UnityCatalogProperties.TOKEN));
            Assertions.assertFalse(be.containsKey("AWS_ROLE_ARN"));
            Assertions.assertEquals("us-east-1", be.get("AWS_REGION"));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void countsLookupStatisticsAndDispatchGrants(boolean statisticsEnabled, @TempDir Path table) throws Exception {
        serveTableLog(table);
        route("tables/main.db.t", exchange -> reply(exchange, 200, EXTERNAL));
        AtomicInteger grants = new AtomicInteger();
        AtomicBoolean revoked = new AtomicBoolean();
        long expiry = System.currentTimeMillis() + 3_600_000;
        route("temporary-table-credentials", exchange -> {
            int n = grants.incrementAndGet();
            if (revoked.get()) {
                reply(exchange, 403, "{}");
            } else {
                reply(exchange, 200, "{\"expiration_time\":" + expiry
                        + ",\"aws_temp_credentials\":{\"access_key_id\":\"test\","
                        + "\"secret_access_key\":\"test\",\"session_token\":\"grant-" + n + "\"}}");
            }
        });
        Map<String, String> raw = Map.of("deltalake.catalog.type", "uc", UnityCatalogProperties.URI_KEY, uri,
                UnityCatalogProperties.NAMESPACE, "main", UnityCatalogProperties.TOKEN, "metadata-only",
                "fs.s3a.endpoint", uri, "fs.s3a.endpoint.region", "us-east-1",
                "fs.s3a.connection.ssl.enabled", "false", "fs.s3a.path.style.access", "true",
                "meta.cache.deltalake.statistics.enable", Boolean.toString(statisticsEnabled));
        DeltaLakeCatalogProperties properties = DeltaLakeCatalogProperties.of(raw);
        Queue<Runnable> pending = new ArrayDeque<>();
        try (DeltaLakeRuntime runtime = new DeltaLakeRuntime(properties, new RecordingConnectorContext());
                CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            DeltaLakeStatisticsCache stats = new DeltaLakeStatisticsCache(owner, raw, pending::add);
            DeltaLakeConnectorMetadata metadata = new DeltaLakeConnectorMetadata(runtime, properties,
                    new DeltaLakeSnapshotCache(owner, raw), stats);
            for (String query : List.of("cold", "warm", "revoked")) {
                TestingConnectorSession session = new TestingConnectorSession(query);
                int before = grants.get();
                ConnectorTableHandle handle = metadata.getTableHandle(session, "db", "t").orElseThrow();
                Assertions.assertEquals(before + 1, grants.get(), "each new statement authorizes the table");
                Assertions.assertSame(handle, metadata.getTableHandle(session, "db", "t").orElseThrow());
                metadata.getTableSchema(session, handle);
                metadata.getTableStatistics(session, handle);
                Assertions.assertEquals(before + 1, grants.get(), "schema and queuing use the lookup grant");
                boolean coldLoad = statisticsEnabled && query.equals("cold");
                Assertions.assertEquals(coldLoad ? 1 : 0, pending.size());
                if (coldLoad) {
                    pending.remove().run();
                }
                if (statisticsEnabled) {
                    Assertions.assertEquals(1,
                            metadata.getTableStatistics(session, handle).orElseThrow().getRowCount());
                }
                int beforeDispatch = before + (coldLoad ? 2 : 1);
                Assertions.assertEquals(beforeDispatch, grants.get(), "only a running cold stats load reauthorizes");
                DeltaLakeScanPlanProvider scan = new DeltaLakeScanPlanProvider(runtime, stats, false);
                if (query.equals("revoked")) {
                    revoked.set(true);
                    DorisConnectorException denial = Assertions.assertThrows(DorisConnectorException.class,
                            () -> scan.getScanNodeProperties(session, handle, List.of(), Optional.empty()));
                    Assertions.assertTrue(denial.getMessage().contains("403"));
                } else {
                    Map<String, String> be = scan.getScanNodeProperties(session, handle, List.of(), Optional.empty());
                    Assertions.assertEquals("grant-" + (beforeDispatch + 1),
                            be.get(ScanNodePropertyKeys.LOCATION_PREFIX + "AWS_TOKEN"));
                    Assertions.assertEquals(1, scan.planScan(session,
                            ConnectorScanRequest.builder(handle, List.of()).build()).size());
                }
                Assertions.assertEquals(beforeDispatch + 1, grants.get(), "one fresh READ at dispatch");
            }
        }
    }

    // A one-commit S3 table lets the real vended FileIO and detached statistics loader read the log.
    // File counts alone need no Parquet object.
    private void serveTableLog(Path table) throws IOException {
        String key = "t/_delta_log/00000000000000000000.json";
        DeltaLakeTestTables.commit(table, 0, "table-id", new StructType().add("id", LongType.LONG));
        String add = "{\"add\":{\"path\":\"part.parquet\",\"size\":25,\"partitionValues\":{},"
                + "\"modificationTime\":1,\"dataChange\":true,\"stats\":\"{\\\"numRecords\\\":1}\"}}\n";
        byte[] log = (Files.readString(table.resolve("_delta_log/00000000000000000000.json")) + add)
                .getBytes(StandardCharsets.UTF_8);
        server.createContext("/warehouse", exchange -> {
            String path = exchange.getRequestURI().getPath();
            if (path.equals("/warehouse/" + key)) {
                exchange.getResponseHeaders().set("ETag", "\"test-etag\"");
                exchange.getResponseHeaders().set("Last-Modified", "Wed, 01 Jan 2025 00:00:00 GMT");
                if ("HEAD".equals(exchange.getRequestMethod())) {
                    exchange.getResponseHeaders().set("Content-Length", Integer.toString(log.length));
                    exchange.sendResponseHeaders(200, -1);
                    exchange.close();
                    return;
                }
                String range = exchange.getRequestHeaders().getFirst("Range");
                int start = range == null ? 0 : Integer.parseInt(range.substring(6, range.indexOf('-')));
                String last = range == null ? "" : range.substring(range.indexOf('-') + 1);
                int end = last.isEmpty() ? log.length - 1 : Math.min(Integer.parseInt(last), log.length - 1);
                if (range != null) {
                    exchange.getResponseHeaders().set("Content-Range", "bytes " + start + "-" + end + "/" + log.length);
                }
                exchange.sendResponseHeaders(range == null ? 200 : 206, end - start + 1);
                try (OutputStream response = exchange.getResponseBody()) {
                    response.write(log, start, end - start + 1);
                }
            } else if ((path.equals("/warehouse") || path.equals("/warehouse/"))
                    && exchange.getRequestURI().getQuery() != null) {
                String prefix = Arrays.stream(exchange.getRequestURI().getQuery().split("&"))
                        .filter(param -> param.startsWith("prefix=")).map(param -> param.substring(7))
                        .findFirst().orElse("");
                String entry = key.startsWith(prefix) ? "<Contents><Key>" + key + "</Key><Size>" + log.length
                        + "</Size><LastModified>2025-01-01T00:00:00Z</LastModified></Contents>" : "";
                exchange.getResponseHeaders().set("Content-Type", "application/xml");
                reply(exchange, 200, "<ListBucketResult><Name>warehouse</Name><IsTruncated>false</IsTruncated>"
                        + entry + "</ListBucketResult>");
            } else {
                exchange.sendResponseHeaders(404, -1);
                exchange.close();
            }
        });
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
            Assertions.assertEquals(1, requests.get());
            Assertions.assertThrows(DorisConnectorException.class, () -> uc.backendProperties(table, Map.of()));
            Assertions.assertEquals(2, requests.get());
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

    private static void reply(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream response = exchange.getResponseBody()) {
            response.write(bytes);
        }
    }
}
