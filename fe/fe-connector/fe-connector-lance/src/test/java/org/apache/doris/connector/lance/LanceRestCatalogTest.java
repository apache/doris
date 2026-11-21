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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorTestResult;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A REST catalog against a REST namespace server stubbed in this JVM: how each security type reaches the
 * server, what the server's errors become, and what a table description is read for.
 *
 * <p>Skips, loudly, where the native libraries cannot load; see {@link LanceNativeTestSupport}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LanceRestCatalogTest {
    private static final String BEARER_TOKEN = "test-bearer-token";
    private static final String HEADER_SECRET = "test-header-secret";
    private static final String EXISTING_TABLE = "existing_table";
    private static final String MISSING_TABLE = "missing_table";
    private static final String MISSING_NAMESPACE = "missing_namespace";

    private final List<Request> requests = new CopyOnWriteArrayList<>();
    private HttpServer server;
    private String restUri;
    /** The dataset the stub describes {@code all_types} at, as a file URI. */
    private String datasetUri;

    @TempDir
    Path warehouse;

    @BeforeAll
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.start();
        restUri = "http://127.0.0.1:" + server.getAddress().getPort() + "/";
    }

    @AfterAll
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @BeforeEach
    void requireNativeLibraries() {
        LanceNativeTestSupport.requireNativeLibraries();
        requests.clear();
    }

    @Test
    void bearerTokenAndExtraHeadersGoWithEveryRequest() {
        Map<String, String> properties = rest(restUri);
        properties.put("lance.rest.security.type", "bearer");
        properties.put("lance.rest.bearer-token", BEARER_TOKEN);
        properties.put("lance.rest.header.x-tenant", "doris-test");
        assertConnects(properties);
        Assertions.assertTrue(requests.stream().allMatch(request ->
                ("Bearer " + BEARER_TOKEN).equals(request.authorization) && request.apiKey == null
                        && List.of("doris-test").equals(request.tenants)), requests.toString());
    }

    @Test
    void legalTabsInHeadersAndCredentialsReachTheNamespace() {
        for (String mode : List.of("bearer", "api_key")) {
            requests.clear();
            Map<String, String> properties = rest(restUri);
            properties.put("lance.rest.security.type", mode);
            properties.put(mode.equals("bearer") ? "lance.rest.bearer-token" : "lance.rest.api-key", "token\tvalue");
            properties.put("lance.rest.header.x-tenant", "tenant\tname");
            assertConnects(properties);
            // The JDK HTTP server folds HTAB to a space while parsing a header value.
            for (Request request : requests) {
                Assertions.assertEquals(List.of("tenant name"), request.tenants);
                Assertions.assertEquals(mode.equals("bearer") ? "Bearer token value" : null, request.authorization);
                Assertions.assertEquals(mode.equals("api_key") ? "token value" : null, request.apiKey);
            }
        }
    }

    @Test
    void headerCaseVariantsSendOneValueAcrossAuthenticationTransitions() {
        Map<String, String> properties = rest(restUri);
        properties.put("lance.rest.header.X-Tenant", "old");
        assertConnects(properties);
        // Reopen the client after each of the six directed authentication transitions.
        for (String mode : List.of("bearer", "api_key", "none", "api_key", "bearer", "none")) {
            Map<String, String> updates = Map.of(
                    "lance.rest.security.type", mode,
                    "lance.rest.bearer-token", mode.equals("bearer") ? BEARER_TOKEN : "",
                    "lance.rest.api-key", mode.equals("api_key") ? "test-api-key" : "",
                    "lance.rest.header.X-Tenant", "tenant-" + mode,
                    "lance.rest.header.x-tenant", "tenant-" + mode);
            new LanceConnectorProvider().validatePropertiesForUpdate(properties, updates);
            properties.putAll(updates);
            requests.clear();
            assertConnects(properties);
            for (Request request : requests) {
                Assertions.assertEquals(List.of("tenant-" + mode), request.tenants);
                Assertions.assertEquals(mode.equals("bearer") ? "Bearer " + BEARER_TOKEN : null,
                        request.authorization);
                Assertions.assertEquals(mode.equals("api_key") ? "test-api-key" : null, request.apiKey);
            }
        }
    }

    @Test
    void noneSendsNoCredentialAndApiKeySendsOnlyItsHeader() {
        assertConnects(rest(restUri));
        Assertions.assertTrue(requests.stream().allMatch(request ->
                request.authorization == null && request.apiKey == null), requests.toString());

        requests.clear();
        Map<String, String> apiKey = rest(restUri);
        apiKey.put("lance.rest.security.type", "api_key");
        apiKey.put("lance.rest.api-key", "test-api-key");
        assertConnects(apiKey);
        Assertions.assertTrue(requests.stream().allMatch(request ->
                request.authorization == null && "test-api-key".equals(request.apiKey)), requests.toString());
    }

    /** The server echoes the credentials in its error; neither reaches the user nor the FE log. */
    @Test
    void failuresNeverEchoTheCredentials() {
        Map<String, String> properties = rest(restUri + "fail");
        properties.put("lance.rest.security.type", "bearer");
        properties.put("lance.rest.bearer-token", BEARER_TOKEN);
        properties.put("lance.rest.header.X-Session-Token", HEADER_SECRET);
        LanceConnector connector = open(properties);
        try {
            LanceTestSession session = new LanceTestSession();
            ConnectorTestResult result = connector.testConnection(session);
            Assertions.assertFalse(result.isSuccess());
            assertNoSecret(result.getMessage());
            DorisConnectorException thrown = Assertions.assertThrows(DorisConnectorException.class,
                    () -> connector.getMetadata(session).listDatabaseNames(session));
            for (Throwable current = thrown; current != null; current = current.getCause()) {
                assertNoSecret(current.getMessage());
            }
            Assertions.assertFalse(requests.isEmpty(), "the server was never asked");
        } finally {
            connector.close();
        }
    }

    @Test
    void tableExistenceIsTheNamespacesAnswer() {
        LanceConnector connector = open(rest(restUri));
        try {
            LanceTestSession session = new LanceTestSession();
            ConnectorMetadata metadata = connector.getMetadata(session);
            Assertions.assertTrue(metadata.getTableHandle(session, "default", EXISTING_TABLE).isPresent());
            Assertions.assertFalse(metadata.getTableHandle(session, "default", MISSING_TABLE).isPresent());
            Assertions.assertFalse(metadata.getTableHandle(session, MISSING_NAMESPACE, MISSING_TABLE).isPresent());
        } finally {
            connector.close();
        }
    }

    /** A table reported missing over an authentication or service failure would drop out of the name cache. */
    @Test
    void serviceErrorsAreNotTakenForAMissingTable() {
        for (String prefix : new String[] {"fail/", "service-error/"}) {
            LanceConnector connector = open(rest(restUri + prefix));
            try {
                LanceTestSession session = new LanceTestSession();
                Assertions.assertThrows(DorisConnectorException.class, () ->
                        connector.getMetadata(session).getTableHandle(session, "default", EXISTING_TABLE), prefix);
            } finally {
                connector.close();
            }
        }
    }

    /** A REST catalog asks for each table's credentials, then opens the dataset where the namespace says. */
    @Test
    void describeAsksForVendedCredentialsAndOpensTheDescribedDataset() throws IOException {
        datasetUri = LanceNativeTestSupport.copyDataset(LanceNativeTestSupport.VERSIONED_ALL_TYPES, warehouse)
                .toUri().toString();
        LanceConnector connector = open(rest(restUri));
        try {
            LanceTestSession session = new LanceTestSession();
            ConnectorMetadata metadata = connector.getMetadata(session);
            ConnectorTableHandle table = metadata.getTableHandle(session, "default", "all_types").get();
            Assertions.assertEquals(3, metadata.beginQuerySnapshot(session, table).get().getSnapshotId());
            Request describe = requests.stream().filter(request -> request.path.endsWith("/describe"))
                    .findFirst().get();
            Assertions.assertTrue(describe.body.replace(" ", "").contains("\"vend_credentials\":true"),
                    describe.body);
        } finally {
            connector.close();
        }
    }

    /** U4: until Doris reads the versions a namespace records, such a table is refused, not read at random. */
    @Test
    void tablesWhoseVersionsTheNamespaceManagesAreRefused() {
        LanceConnector connector = open(rest(restUri));
        try {
            LanceTestSession session = new LanceTestSession();
            ConnectorMetadata metadata = connector.getMetadata(session);
            ConnectorTableHandle managed = metadata.getTableHandle(session, "default", "managed").get();
            Assertions.assertEquals("Lance table default.managed has its versions managed by the namespace"
                            + " (managed_versioning), which is not supported yet",
                    Assertions.assertThrows(DorisConnectorException.class,
                            () -> metadata.beginQuerySnapshot(session, managed)).getMessage());
            ConnectorTableHandle declared = metadata.getTableHandle(session, "default", "declared").get();
            Assertions.assertEquals("Lance table is declared in the namespace but has no data yet",
                    Assertions.assertThrows(DorisConnectorException.class,
                            () -> metadata.beginQuerySnapshot(session, declared)).getMessage());
        } finally {
            connector.close();
        }
    }

    private static Map<String, String> rest(String uri) {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put("lance.catalog.type", "rest");
        properties.put("lance.rest.uri", uri);
        return properties;
    }

    private static LanceConnector open(Map<String, String> properties) {
        return (LanceConnector) new LanceConnectorProvider().create(properties, new LanceTestContext());
    }

    private void assertConnects(Map<String, String> properties) {
        LanceConnector connector = open(properties);
        try {
            ConnectorTestResult result = connector.testConnection(new LanceTestSession());
            Assertions.assertTrue(result.isSuccess(), result.getMessage());
            Assertions.assertFalse(requests.isEmpty(), "the server was never asked");
        } finally {
            connector.close();
        }
    }

    private static void assertNoSecret(String message) {
        String text = String.valueOf(message);
        Assertions.assertFalse(text.contains(BEARER_TOKEN), text);
        Assertions.assertFalse(text.contains(HEADER_SECRET), text);
    }

    private void handle(HttpExchange exchange) throws IOException {
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        String path = exchange.getRequestURI().getPath();
        requests.add(new Request(path, body, exchange.getRequestHeaders().getFirst("Authorization"),
                exchange.getRequestHeaders().getFirst("x-api-key"),
                exchange.getRequestHeaders().getOrDefault("x-tenant", List.of())));

        int status = 200;
        String response;
        if (path.startsWith("/fail/")) {
            status = 401;
            response = "{\"error\":\"invalid token " + BEARER_TOKEN + " with session " + HEADER_SECRET
                    + "\",\"code\":16}";
        } else if (path.startsWith("/service-error/")) {
            status = 503;
            response = "{\"error\":\"service unavailable\",\"code\":17}";
        } else if (path.matches("/v1/table/[^/]+/exists")) {
            String tableId = path.substring("/v1/table/".length(), path.length() - "/exists".length());
            if (!tableId.startsWith(MISSING_NAMESPACE + "$") && !MISSING_TABLE.equals(tableId)) {
                exchange.sendResponseHeaders(200, -1);
                exchange.close();
                return;
            }
            status = 404;
            response = tableId.startsWith(MISSING_NAMESPACE + "$")
                    ? "{\"error\":\"namespace not found\",\"code\":1}"
                    : "{\"error\":\"table not found\",\"code\":4}";
        } else if ("/v1/table/all_types/describe".equals(path)) {
            response = describe(datasetUri, false, false);
        } else if ("/v1/table/managed/describe".equals(path)) {
            response = describe("file:///nowhere/managed.lance", true, false);
        } else if ("/v1/table/declared/describe".equals(path)) {
            response = describe("file:///nowhere/declared.lance", false, true);
        } else if (path.endsWith("/table/list")) {
            response = "{\"tables\":[]}";
        } else if (path.endsWith("/list")) {
            response = "{\"namespaces\":[]}";
        } else {
            status = 404;
            response = "{\"error\":\"not found\",\"code\":4}";
        }
        byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    private static String describe(String uri, boolean managedVersioning, boolean onlyDeclared) {
        return "{\"location\":\"" + uri + "\",\"table_uri\":\"" + uri + "\",\"storage_options\":{},"
                + "\"managed_versioning\":" + managedVersioning + ",\"is_only_declared\":" + onlyDeclared + "}";
    }

    private static final class Request {
        private final String path;
        private final String body;
        private final String authorization;
        private final String apiKey;
        private final List<String> tenants;

        private Request(String path, String body, String authorization, String apiKey, List<String> tenants) {
            this.path = path;
            this.body = body;
            this.authorization = authorization;
            this.apiKey = apiKey;
            this.tenants = tenants;
        }

        @Override
        public String toString() {
            return path + " Authorization=" + authorization + " x-api-key=" + apiKey + " x-tenant=" + tenants;
        }
    }
}
