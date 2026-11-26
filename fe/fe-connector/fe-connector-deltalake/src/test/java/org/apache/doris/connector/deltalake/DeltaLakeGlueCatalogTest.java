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

import org.apache.doris.connector.metastore.deltalake.glue.GlueCatalogProperties;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.NamedColumnHandle;
import org.apache.doris.connector.spi.mvcc.ConnectorTimeTravelSpec;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.utils.AttributeMap;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

class DeltaLakeGlueCatalogTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String CATALOG = "123456789012";
    private static final String ROLE = "arn:aws:iam::123456789012:role/delta-test";

    @TempDir
    Path warehouse;
    private HttpServer server;
    private HttpServer stsServer;
    private final List<Call> calls = new CopyOnWriteArrayList<>();

    private record Call(String operation, JsonNode json, String form, String signature, String token) {
    }

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/glue", this::glueRequest);
        stsServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        stsServer.createContext("/", this::stsRequest);
        server.start();
        stsServer.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
        stsServer.stop(0);
    }

    private URI stsEndpoint() {
        return URI.create("http://127.0.0.1:" + stsServer.getAddress().getPort());
    }

    private Map<String, String> properties() {
        return new HashMap<>(Map.of("deltalake.catalog.type", "glue",
                "glue.endpoint", "http://127.0.0.1:" + server.getAddress().getPort() + "/glue",
                "glue.region", "us-west-2", "glue.access_key", "example-key", "glue.secret_key", "example-secret"));
    }

    private static GlueCatalogProperties bind(Map<String, String> raw) {
        return (GlueCatalogProperties) DeltaLakeCatalogProperties.of(raw).getMetastore();
    }

    private void record(HttpExchange exchange, String op, JsonNode json, String form) {
        calls.add(new Call(op, json, form, exchange.getRequestHeaders().getFirst("Authorization"),
                exchange.getRequestHeaders().getFirst("X-Amz-Security-Token")));
    }

    private static void send(HttpExchange exchange, int status, String type, byte[] bytes) throws IOException {
        try (exchange) {
            exchange.getResponseHeaders().set("Content-Type", type);
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
        }
    }

    private void glueRequest(HttpExchange exchange) throws IOException {
        String target = exchange.getRequestHeaders().getFirst("X-Amz-Target");
        String op = target.substring(target.lastIndexOf('.') + 1);
        JsonNode request = JSON.readTree(exchange.getRequestBody());
        record(exchange, op, request, null);
        boolean next = request.has("NextToken");
        Map<String, ?> response = switch (op) {
            case "GetDatabases" -> next ? Map.of("DatabaseList", List.of(Map.of("Name", "archive")))
                    : Map.of("DatabaseList", List.of(Map.of("Name", "db")), "NextToken", "database-page-2");
            case "GetTables" -> !request.path("DatabaseName").asText().equals("db") ? null
                    : next ? Map.of("TableList", List.of(table("etl"), table("crawler"), table("iceberg")))
                    : Map.of("TableList", List.of(table("native"), table("hive")), "NextToken", "table-page-2");
            case "GetTable" -> {
                var entry = table(request.path("Name").asText());
                yield entry == null ? null : Map.of("Table", entry);
            }
            default -> throw new IllegalArgumentException(op);
        };
        int status = response == null ? 400 : 200;
        if (response == null) {
            String error = "denied".equals(request.path("Name").asText()) ? "AccessDeniedException"
                    : "EntityNotFoundException";
            exchange.getResponseHeaders().set("X-Amzn-Errortype", error);
            response = Map.of("__type", error, "Message", error);
        }
        send(exchange, status, "application/x-amz-json-1.1", JSON.writeValueAsBytes(response));
    }

    private Map<String, ?> table(String name) {
        String root = warehouse.resolve("db/table").toUri().toString();
        Map<String, Object> storage = new HashMap<>(Map.of("Location", root));
        Map<String, String> markers = Map.of("tAbLe_TyPe", "DeLtA");
        switch (name) {
            case "native" -> storage.put("SerdeInfo", Map.of("Parameters", Map.of("path", "s3://ignored/path")));
            case "etl", "crawler" -> {
                markers = name.equals("etl") ? Map.of("SPARK.SQL.SOURCES.PROVIDER", "DELTA")
                        : Map.of("Classification", "dElTa");
                storage.put("Location", name.equals("etl") ? "s3://fallback/db/etl-__PLACEHOLDER__/" : "");
                storage.put("SerdeInfo", Map.of("Parameters", Map.of("path", root)));
            }
            case "hive" -> markers = Map.of("classification", "parquet");
            case "iceberg" -> markers = Map.of("table_type", "ICEBERG");
            case "manifest" -> {
                storage.put("Location", "s3://bucket/table/_symlink_format_manifest/part=1/");
                storage.put("SerdeInfo", Map.of("Parameters", Map.of("path", root)));
            }
            case "serde_manifest" -> {
                storage.remove("Location");
                storage.put("SerdeInfo", Map.of("Parameters",
                        Map.of("path", "s3://bucket/t/_symlink_format_manifest")));
            }
            case "symlink_input" -> storage.put("InputFormat", "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat");
            case "placeholder" -> storage.put("Location", "-__PLACEHOLDER__");
            case "missing_location" -> {
                return Map.of("Name", name, "Parameters", markers);
            }
            case "relative" -> storage.put("Location", "not-an-absolute-location");
            case "malformed" -> storage.put("Location", "s3://[");
            case "no_bucket" -> storage.put("Location", "s3:///no-bucket");
            default -> {
                return null;
            }
        }
        return Map.of("Name", name, "DatabaseName", "db", "Parameters", markers, "StorageDescriptor", storage);
    }

    private void stsRequest(HttpExchange exchange) throws IOException {
        String form = URLDecoder.decode(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8),
                StandardCharsets.UTF_8);
        record(exchange, "AssumeRole", null, form);
        String xml = """
                <AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleResult>
                <Credentials><AccessKeyId>assumed-key</AccessKeyId><SecretAccessKey>assumed-secret</SecretAccessKey>
                <SessionToken>assumed-token</SessionToken><Expiration>%s</Expiration></Credentials>
                <AssumedRoleUser><Arn>arn:aws:sts::123456789012:assumed-role/test/session</Arn>
                <AssumedRoleId>id:session</AssumedRoleId></AssumedRoleUser></AssumeRoleResult>
                <ResponseMetadata><RequestId>local</RequestId></ResponseMetadata></AssumeRoleResponse>
                """.formatted(Instant.now().plusSeconds(3600));
        send(exchange, 200, "text/xml", xml.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void paginatesAndResolvesOnlyNativeDeltaRegistrations() throws Exception {
        Map<String, String> raw = properties();
        raw.put("glue.catalog_id", CATALOG);
        try (DeltaLakeGlueCatalog glue = new DeltaLakeGlueCatalog(bind(raw))) {
            Assertions.assertTrue(calls.isEmpty());
            Assertions.assertEquals(List.of("db", "archive"), glue.listDatabases());
            Assertions.assertEquals(List.of("native", "etl", "crawler"), glue.listTables("db"));
            for (String name : List.of("native", "etl", "crawler")) {
                var ref = glue.tableLocation("db", name).orElseThrow();
                Assertions.assertEquals(warehouse.resolve("db/table").toUri().toString(), ref.path);
                Assertions.assertNull(ref.registrationId);
                Assertions.assertFalse(ref.managed);
            }
            Assertions.assertEquals(List.of("GetDatabases", "GetDatabases", "GetTables", "GetTables",
                    "GetTable", "GetTable", "GetTable"), calls.stream().map(Call::operation).toList());
            Assertions.assertFalse(calls.get(0).json.has("NextToken"));
            Assertions.assertEquals("database-page-2", calls.get(1).json.get("NextToken").asText());
            Assertions.assertFalse(calls.get(2).json.has("NextToken"));
            Assertions.assertEquals("table-page-2", calls.get(3).json.get("NextToken").asText());
            Assertions.assertTrue(glue.listTables("absent").isEmpty());
            for (String name : List.of("absent", "hive", "iceberg")) {
                Assertions.assertTrue(glue.tableLocation("db", name).isEmpty(), name);
            }
            var denied = Assertions.assertThrows(GlueException.class, () -> glue.tableLocation("db", "denied"));
            Assertions.assertEquals("AccessDeniedException", denied.awsErrorDetails().errorCode());
            for (Call call : calls) {
                Assertions.assertEquals(CATALOG, call.json.get("CatalogId").asText());
                Assertions.assertTrue(call.signature.startsWith("AWS4-HMAC-SHA256 Credential=example-key/"));
                Assertions.assertTrue(call.signature.contains("/us-west-2/glue/aws4_request"));
                Assertions.assertNull(call.token);
            }
            Assertions.assertEquals(CATALOG + ".db.native", glue.registeredTableName("db", "native"));
        }
    }

    @Test
    void rejectsManifestAndUnusableRoots() throws Exception {
        try (DeltaLakeGlueCatalog glue = new DeltaLakeGlueCatalog(bind(properties()))) {
            for (String name : List.of("manifest", "serde_manifest", "symlink_input")) {
                var error = Assertions.assertThrows(DorisConnectorException.class,
                        () -> glue.tableLocation("db", name));
                Assertions.assertTrue(error.getMessage().contains("Symlink manifest"));
            }
            for (String name : List.of("missing_location", "placeholder", "relative", "malformed", "no_bucket")) {
                var error = Assertions.assertThrows(DorisConnectorException.class,
                        () -> glue.tableLocation("db", name));
                Assertions.assertTrue(error.getMessage().contains("storage location"));
                Assertions.assertTrue(error.getMessage().contains("db." + name));
            }
            Assertions.assertTrue(calls.stream().noneMatch(call -> call.json.has("CatalogId")));
        }
    }

    @Test
    void signsWithSessionCredentialsAndAssumesRoleOverSts() throws Exception {
        var raw = properties();
        raw.put("glue.session_token", "source-token");
        try (var glue = new DeltaLakeGlueCatalog(bind(raw))) {
            glue.listDatabases();
            Assertions.assertEquals("source-token", calls.get(0).token);
        }
        calls.clear();
        raw.put("glue.role_arn", ROLE);
        raw.put("glue.external_id", "external-test-id");
        var glueHttp = new TrackingHttpBuilder();
        var stsHttp = new TrackingHttpBuilder();
        var glue = new DeltaLakeGlueCatalog(bind(raw), GlueClient.builder().httpClientBuilder(glueHttp),
                StsClient.builder().endpointOverride(stsEndpoint()).httpClientBuilder(stsHttp));
        try (glue) {
            Assertions.assertTrue(calls.isEmpty());
            glue.listDatabases();
            Assertions.assertEquals(List.of("AssumeRole", "GetDatabases", "GetDatabases"),
                    calls.stream().map(Call::operation).toList());
            Call sts = calls.get(0);
            Assertions.assertTrue(sts.signature.contains("Credential=example-key/"));
            Assertions.assertTrue(sts.signature.contains("/us-west-2/sts/aws4_request"));
            Assertions.assertEquals("source-token", sts.token);
            Assertions.assertTrue(sts.form.contains("Action=AssumeRole"));
            Assertions.assertTrue(sts.form.contains("RoleArn=" + ROLE));
            Assertions.assertTrue(sts.form.contains("ExternalId=external-test-id"));
            Assertions.assertTrue(calls.get(1).signature.contains("Credential=assumed-key/"));
            Assertions.assertEquals("assumed-token", calls.get(1).token);
        }
        glue.close();
        Assertions.assertEquals(1, glueHttp.closed.get());
        Assertions.assertEquals(1, stsHttp.closed.get());
    }

    // Decorate real SDK URLConnection transports to count their close calls. Every request still goes over HTTP.
    private static final class TrackingHttpBuilder implements SdkHttpClient.Builder<TrackingHttpBuilder> {
        final AtomicInteger closed = new AtomicInteger();

        @Override
        public SdkHttpClient buildWithDefaults(AttributeMap defaults) {
            SdkHttpClient transport = UrlConnectionHttpClient.builder().buildWithDefaults(defaults);
            return new SdkHttpClient() {
                @Override
                public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
                    return transport.prepareRequest(request);
                }

                @Override
                public void close() {
                    closed.incrementAndGet();
                    transport.close();
                }
            };
        }
    }

    @Test
    @ResourceLock(Resources.SYSTEM_PROPERTIES)
    void signsWithTheDefaultCredentialChain() throws Exception {
        Map<String, String> saved = new HashMap<>();
        var settings = Map.of("aws.accessKeyId", "default-key", "aws.secretAccessKey", "default-secret",
                "aws.sessionToken", "default-token");
        settings.forEach((key, value) -> saved.put(key, System.setProperty(key, value)));
        try {
            var raw = properties();
            raw.remove("glue.access_key");
            raw.remove("glue.secret_key");
            try (var glue = new DeltaLakeGlueCatalog(bind(raw))) {
                glue.listDatabases();
            }
            Assertions.assertTrue(calls.get(0).signature.contains("Credential=default-key/"));
            Assertions.assertEquals("default-token", calls.get(0).token);
            calls.clear();
            raw.put("glue.role_arn", ROLE);
            try (var assumed = new DeltaLakeGlueCatalog(bind(raw),
                    GlueClient.builder().httpClientBuilder(UrlConnectionHttpClient.builder()),
                    StsClient.builder().endpointOverride(stsEndpoint())
                            .httpClientBuilder(UrlConnectionHttpClient.builder()))) {
                assumed.listDatabases();
            }
            Assertions.assertEquals("AssumeRole", calls.get(0).operation);
            Assertions.assertTrue(calls.get(0).signature.contains("Credential=default-key/"));
            Assertions.assertFalse(calls.get(0).form.contains("ExternalId="));
            Assertions.assertEquals("assumed-token", calls.get(1).token);
        } finally {
            saved.forEach((key, value) -> {
                if (value == null) {
                    System.clearProperty(key);
                } else {
                    System.setProperty(key, value);
                }
            });
        }
    }

    @Test
    void plansVersionedDataAndSystemTablesThroughGlue() throws Exception {
        var schema = new StructType().add("id", LongType.LONG);
        Path table = warehouse.resolve("db/table");
        long first = System.currentTimeMillis() - 30000;
        for (int v = 0; v < 2; v++) {
            Path log = DeltaLakeTestTables.commit(table, v, "glue-id", schema);
            Files.writeString(log, """
                    {"add":{"path":"%s.parquet","partitionValues":{},"size":41,"modificationTime":1,"dataChange":true}}
                    """.formatted(v == 0 ? "one" : "two"), StandardOpenOption.APPEND);
            Files.setLastModifiedTime(log, FileTime.fromMillis(first + v * 1000L));
        }
        var provider = new DeltaLakeConnectorProvider();
        var raw = properties();
        provider.validateProperties(raw);
        try (Connector connector = provider.create(raw, new RecordingConnectorContext());
                ConnectorMetadata metadata = connector.getMetadata(null)) {
            Assertions.assertTrue(calls.isEmpty());
            var session = new TestingConnectorSession("glue-snapshots");
            var base = metadata.getTableHandle(session, "db", "etl").orElseThrow();
            Assertions.assertEquals(2, ranges(connector, session, base, "id").size());
            for (var spec : List.of(ConnectorTimeTravelSpec.snapshotId("0"),
                    ConnectorTimeTravelSpec.timestamp(Long.toString(first), true))) {
                var pin = metadata.resolveTimeTravel(session, base, spec).orElseThrow();
                var pinned = metadata.applySnapshot(session, base, pin);
                var files = ranges(connector, session, pinned, "id");
                Assertions.assertEquals(1, files.size());
                Assertions.assertTrue(files.get(0).getPath().orElseThrow().endsWith("/one.parquet"));
                for (String view : List.of("detail", "files")) {
                    var handle = metadata.getSysTableHandle(session, base, view).orElseThrow();
                    var old = metadata.applySnapshot(session, handle, pin);
                    var sys = rows(ranges(connector, session, old, view.equals("detail") ? "name" : "path"));
                    Assertions.assertEquals(1, sys.size());
                    Assertions.assertTrue(view.equals("detail") ? sys.get(0).equals("db.etl")
                            : sys.get(0).endsWith("/one.parquet"));
                }
            }
            var history = metadata.getSysTableHandle(session, base, "history").orElseThrow();
            var format = new TTableFormatFileDesc();
            ranges(connector, session, history, "version").get(0).populateRangeParams(format, new TFileRangeDesc());
            Assertions.assertEquals(List.of(1L, 0L), format.getDeltalakeParams().getSysTableBatch().getRows()
                    .stream().map(row -> row.getCells().get(0).getInt64Val()).toList());
        }
    }

    private static List<ConnectorScanRange> ranges(Connector connector, TestingConnectorSession session,
            ConnectorTableHandle table, String column) {
        return connector.getScanPlanProvider(table).planScan(session, ConnectorScanRequest.builder(table,
                List.<ConnectorColumnHandle>of(new NamedColumnHandle(column))).build());
    }

    private static List<String> rows(List<ConnectorScanRange> ranges) {
        return ranges.stream().flatMap(range -> {
            var format = new TTableFormatFileDesc();
            range.populateRangeParams(format, new TFileRangeDesc());
            return format.getDeltalakeParams().getSysTableBatch().getRows().stream()
                    .map(row -> row.getCells().get(0).getStringVal());
        }).toList();
    }
}
