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

package org.apache.doris.datasource.lance;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.datasource.CatalogMgr;

import com.google.common.io.ByteStreams;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class LanceRestCatalogTest {
    private static final String BEARER_TOKEN = "test-bearer-token";

    private final List<RequestRecord> requests = new CopyOnWriteArrayList<>();
    private HttpServer server;
    private String restUri;

    @BeforeAll
    public void setUp() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handleRequest);
        server.start();
        restUri = "http://127.0.0.1:" + server.getAddress().getPort() + "/";
    }

    @AfterAll
    public void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    public void testBearerConnectivityAndSecretMasking() throws Exception {
        requests.clear();
        Map<String, String> properties = restProperties();
        properties.put(LanceExternalCatalog.REST_SECURITY_TYPE, "bearer");
        properties.put(LanceExternalCatalog.REST_BEARER_TOKEN, BEARER_TOKEN);
        properties.put(LanceExternalCatalog.REST_HEADER_PREFIX + "x-tenant", "doris-test");
        LanceExternalCatalog catalog = new LanceExternalCatalog(
                100, "lance_rest_bearer", null, properties, "");
        catalog.checkWhenCreating();

        Assertions.assertFalse(requests.isEmpty());
        Assertions.assertTrue(requests.stream().allMatch(request ->
                ("Bearer " + BEARER_TOKEN).equals(request.authorization)
                        && "doris-test".equals(request.tenant)));

        Map<String, String> printable = CatalogMgr.getCatalogPropertiesWithPrintable(catalog);
        Assertions.assertEquals(PrintableMap.PASSWORD_MASK,
                printable.get(LanceExternalCatalog.REST_BEARER_TOKEN));
    }

    @Test
    public void testNoneAndApiKeyConnectivity() throws Exception {
        requests.clear();
        LanceExternalCatalog noneCatalog = new LanceExternalCatalog(101, "lance_rest_none", null,
                restProperties(), "");
        noneCatalog.checkWhenCreating();
        Assertions.assertFalse(requests.isEmpty());
        Assertions.assertTrue(requests.stream().allMatch(request ->
                request.authorization == null && request.apiKey == null));

        requests.clear();
        Map<String, String> apiKeyProperties = restProperties();
        apiKeyProperties.put(LanceExternalCatalog.REST_SECURITY_TYPE, "api_key");
        apiKeyProperties.put(LanceExternalCatalog.REST_API_KEY, "test-api-key");
        LanceExternalCatalog apiKeyCatalog = new LanceExternalCatalog(102, "lance_rest_api_key", null,
                apiKeyProperties, "");
        apiKeyCatalog.checkWhenCreating();
        Assertions.assertFalse(requests.isEmpty());
        Assertions.assertTrue(requests.stream().allMatch(request ->
                request.authorization == null && "test-api-key".equals(request.apiKey)));

        Map<String, String> failingProperties = restProperties();
        failingProperties.put(LanceExternalCatalog.REST_URI, restUri + "fail");
        failingProperties.put(LanceExternalCatalog.REST_SECURITY_TYPE, "bearer");
        failingProperties.put(LanceExternalCatalog.REST_BEARER_TOKEN, BEARER_TOKEN);
        LanceExternalCatalog failingCatalog = new LanceExternalCatalog(103, "lance_rest_failure", null,
                failingProperties, "");
        DdlException exception = Assertions.assertThrows(DdlException.class, failingCatalog::checkWhenCreating);
        Throwable current = exception;
        while (current != null) {
            Assertions.assertFalse(String.valueOf(current.getMessage()).contains(BEARER_TOKEN),
                    String.valueOf(current.getMessage()));
            current = current.getCause();
        }
    }

    @Test
    public void testRestPropertyValidation() {
        assertInvalid(restPropertiesWithoutUri(), LanceExternalCatalog.REST_URI);

        Map<String, String> warehouseProperties = restProperties();
        warehouseProperties.put(LanceExternalCatalog.WAREHOUSE, "/unused/lance-warehouse");
        assertInvalid(warehouseProperties, "warehouse");

        Map<String, String> bearerWithoutToken = restProperties();
        bearerWithoutToken.put(LanceExternalCatalog.REST_SECURITY_TYPE, "bearer");
        assertInvalid(bearerWithoutToken, LanceExternalCatalog.REST_BEARER_TOKEN);

        Map<String, String> noneWithApiKey = restProperties();
        noneWithApiKey.put(LanceExternalCatalog.REST_API_KEY, "unexpected");
        assertInvalid(noneWithApiKey, "security type 'none'");

        Map<String, String> customAuthHeader = restProperties();
        customAuthHeader.put(LanceExternalCatalog.REST_HEADER_PREFIX + "Authorization", "secret");
        assertInvalid(customAuthHeader, "must be configured through");

        Map<String, String> injectedBearerToken = restProperties();
        injectedBearerToken.put(LanceExternalCatalog.REST_SECURITY_TYPE, "bearer");
        injectedBearerToken.put(LanceExternalCatalog.REST_BEARER_TOKEN, "secret\nX-Injected: true");
        assertInvalid(injectedBearerToken, "Invalid HTTP credential value");

        Map<String, String> unsupportedProperty = restProperties();
        unsupportedProperty.put("lance.rest.unknown", "value");
        assertInvalid(unsupportedProperty, "Unsupported Lance REST property");

        Map<String, String> filesystemWithRest = new HashMap<>();
        filesystemWithRest.put("type", "lance");
        filesystemWithRest.put(LanceExternalCatalog.LANCE_CATALOG_TYPE,
                LanceExternalCatalog.LANCE_FILESYSTEM);
        filesystemWithRest.put(LanceExternalCatalog.WAREHOUSE, "/unused/lance-warehouse");
        filesystemWithRest.put(LanceExternalCatalog.REST_URI, restUri);
        assertInvalid(filesystemWithRest, "not valid for Lance filesystem catalog");
    }

    private Map<String, String> restProperties() {
        Map<String, String> properties = restPropertiesWithoutUri();
        properties.put(LanceExternalCatalog.REST_URI, restUri);
        return properties;
    }

    private Map<String, String> restPropertiesWithoutUri() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put(LanceExternalCatalog.LANCE_CATALOG_TYPE, LanceExternalCatalog.LANCE_REST);
        properties.put("test_connection", "true");
        return properties;
    }

    private static void assertInvalid(Map<String, String> properties, String expectedMessage) {
        LanceExternalCatalog catalog = new LanceExternalCatalog(200, "invalid_lance_rest", null,
                properties, "");
        DdlException exception = Assertions.assertThrows(DdlException.class, catalog::checkProperties);
        Assertions.assertTrue(exception.getMessage().contains(expectedMessage), exception.getMessage());
    }

    private void handleRequest(HttpExchange exchange) throws IOException {
        ByteStreams.toByteArray(exchange.getRequestBody());
        String path = exchange.getRequestURI().getPath();
        requests.add(new RequestRecord(exchange.getRequestHeaders().getFirst("Authorization"),
                exchange.getRequestHeaders().getFirst("x-api-key"),
                exchange.getRequestHeaders().getFirst("x-tenant")));

        String response;
        int status = 200;
        if (path.startsWith("/fail/")) {
            status = 401;
            response = "{\"error\":\"invalid token " + BEARER_TOKEN + "\",\"code\":16}";
        } else if (path.endsWith("/table/list")) {
            response = "{\"tables\":[]}";
        } else if (path.endsWith("/list")) {
            response = "{\"namespaces\":[]}";
        } else {
            status = 404;
            response = "{\"error\":\"not found\",\"code\":4}";
        }
        byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, responseBytes.length);
        exchange.getResponseBody().write(responseBytes);
        exchange.close();
    }

    private static class RequestRecord {
        private final String authorization;
        private final String apiKey;
        private final String tenant;

        private RequestRecord(String authorization, String apiKey, String tenant) {
            this.authorization = authorization;
            this.apiKey = apiKey;
            this.tenant = tenant;
        }
    }
}
