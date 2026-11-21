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

package org.apache.doris.connector.iceberg.auth;


import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpServer;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.gcp.gcs.OAuth2RefreshCredentialsHandler;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class GcsVendedCredentialsTest {
    private HttpServer server;
    private final AtomicInteger requests = new AtomicInteger();
    private final AtomicReference<String> authorization = new AtomicReference<>();
    private final AtomicReference<String> requestPath = new AtomicReference<>();
    private volatile String response;
    private volatile int responseStatus = 200;
    private long refreshedExpiration;
    private final List<VendedRESTCatalog> catalogs = new ArrayList<>();
    private final AtomicInteger oauthRequests = new AtomicInteger();
    private final CountDownLatch catalogRenewed = new CountDownLatch(1);
    private volatile boolean rotateCatalogAuthentication;
    private String renewedCatalogToken;

    @BeforeEach
    public void startCatalog() throws IOException {
        refreshedExpiration = System.currentTimeMillis() + 3_600_000;
        response = credentialsResponse("refreshed-token", refreshedExpiration);
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        renewedCatalogToken = jwt(System.currentTimeMillis() / 1000 + 3600);
        server.createContext("/", exchange -> {
            if (exchange.getRequestURI().getPath().endsWith("/v1/oauth/tokens")) {
                oauthRequests.incrementAndGet();
                byte[] body = ("{\"access_token\":\"" + renewedCatalogToken
                        + "\",\"token_type\":\"Bearer\",\"issued_token_type\":"
                        + "\"urn:ietf:params:oauth:token-type:access_token\",\"expires_in\":3600}")
                        .getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                try (java.io.OutputStream output = exchange.getResponseBody()) {
                    output.write(body);
                }
                catalogRenewed.countDown();
                return;
            }
            if (exchange.getRequestURI().getPath().endsWith("/v1/config")) {
                byte[] body = "{\"defaults\":{},\"overrides\":{}}".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                try (java.io.OutputStream output = exchange.getResponseBody()) {
                    output.write(body);
                }
                return;
            }
            requests.incrementAndGet();
            authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            requestPath.set(exchange.getRequestURI().getPath());
            boolean rejected = rotateCatalogAuthentication && oauthRequests.get() > 0
                    && !("Bearer " + renewedCatalogToken).equals(authorization.get());
            byte[] body = (rejected
                    ? "{\"error\":{\"message\":\"expired catalog token\",\"type\":\"NotAuthorizedException\",\"code\":401}}"
                    : response).getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(rejected ? 401 : responseStatus, body.length);
            try (java.io.OutputStream output = exchange.getResponseBody()) {
                output.write(body);
            }
        });
        server.start();
    }

    @AfterEach
    public void stopCatalog() throws Exception {
        for (VendedRESTCatalog catalog : catalogs) {
            catalog.close();
        }
        server.stop(0);
    }

    @Test
    public void testRefreshUsesCatalogAuthenticationAndCachesCurrentToken() {
        Map<String, String> properties = properties(1);
        Map<String, String> first = GcsVendedCredentials.currentProperties(properties);
        Map<String, String> second = GcsVendedCredentials.currentProperties(properties);

        Assertions.assertEquals("refreshed-token", first.get(GCPProperties.GCS_OAUTH2_TOKEN));
        Assertions.assertEquals(Long.toString(refreshedExpiration),
                first.get(GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT));
        Assertions.assertEquals(first, second);
        Assertions.assertEquals(1, requests.get());
        Assertions.assertEquals("Bearer catalog-token", authorization.get());
        Assertions.assertEquals("/catalog/v1/credentials", requestPath.get());
        Assertions.assertEquals("initial-token", properties.get(GCPProperties.GCS_OAUTH2_TOKEN));
    }

    @Test
    public void testRefreshesBeforeExpiration() {
        Map<String, String> current = GcsVendedCredentials.currentProperties(
                properties(System.currentTimeMillis() + 30_000));
        Assertions.assertEquals("refreshed-token", current.get(GCPProperties.GCS_OAUTH2_TOKEN));
        Assertions.assertEquals(1, requests.get());
    }

    @Test
    public void testValidTokenDoesNotContactCatalog() {
        Map<String, String> current = GcsVendedCredentials.currentProperties(
                properties(System.currentTimeMillis() + 3_600_000));
        Assertions.assertEquals("initial-token", current.get(GCPProperties.GCS_OAUTH2_TOKEN));
        Assertions.assertEquals(0, requests.get());
    }

    @Test
    public void testDisabledOrMissingEndpointKeepsSuppliedToken() {
        Map<String, String> properties = properties(1);
        properties.put(GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENABLED, "false");
        Assertions.assertSame(properties, GcsVendedCredentials.currentProperties(properties));
        properties.remove(GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENABLED);
        properties.remove(GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT);
        Assertions.assertSame(properties, GcsVendedCredentials.currentProperties(properties));
        Assertions.assertEquals(0, requests.get());
    }

    @Test
    public void testConcurrentScansShareOneRefresh() throws Exception {
        Map<String, String> properties = properties(1);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Future<Map<String, String>>> results = new ArrayList<>();
            for (int i = 0; i < 16; i++) {
                results.add(executor.submit(() -> GcsVendedCredentials.currentProperties(properties)));
            }
            for (Future<Map<String, String>> result : results) {
                Assertions.assertEquals("refreshed-token", result.get().get(GCPProperties.GCS_OAUTH2_TOKEN));
            }
            Assertions.assertEquals(1, requests.get());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testReplacingCatalogCredentialsInvalidatesCachedToken() {
        Map<String, String> properties = properties(1);
        GcsVendedCredentials.currentProperties(properties);
        properties.put(GCPProperties.GCS_OAUTH2_TOKEN, "replacement-token");
        properties.put(GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT,
                Long.toString(System.currentTimeMillis() + 3_600_000));
        Assertions.assertEquals("replacement-token",
                GcsVendedCredentials.currentProperties(properties).get(GCPProperties.GCS_OAUTH2_TOKEN));
        Assertions.assertEquals(1, requests.get());
    }

    @Test
    public void testRefreshFailureCanRetryWithoutChangingIdentity() {
        Map<String, String> properties = properties(1);
        responseStatus = 403;
        response = "{\"error\":{\"message\":\"denied\",\"type\":\"ForbiddenException\",\"code\":403}}";
        Assertions.assertThrows(VendedCredentialsException.class,
                () -> GcsVendedCredentials.currentProperties(properties));
        responseStatus = 200;
        response = credentialsResponse("retry-token", refreshedExpiration);
        Assertions.assertEquals("retry-token",
                GcsVendedCredentials.currentProperties(properties).get(GCPProperties.GCS_OAUTH2_TOKEN));
        Assertions.assertEquals(2, requests.get());
    }

    @Test
    public void testExpiredOrMalformedReplacementIsRejected() {
        response = credentialsResponse("expired-token", 1);
        Assertions.assertThrows(VendedCredentialsException.class,
                () -> GcsVendedCredentials.currentProperties(properties(1)));
        for (String token : ImmutableList.of("", " ", "\t", "\u2003", "invalid token")) {
            response = credentialsResponse(token, refreshedExpiration);
            Assertions.assertThrows(VendedCredentialsException.class,
                    () -> GcsVendedCredentials.currentProperties(properties(1)));
        }
    }

    @Test
    public void testRefreshableFeCredentialsSendNewBearerToken() throws IOException {
        GoogleCredentials credentials = createCredentials(
                new AccessToken("initial-token", new Date(1)), properties(1));
        Assertions.assertEquals(ImmutableList.of("Bearer refreshed-token"),
                credentials.getRequestMetadata().get("Authorization"));
        Assertions.assertEquals(1, requests.get());
    }

    @Test
    public void testCatalogTokenRotatesBeforeFirstStorageRefreshAndSurvivesLaterRefreshes() throws Exception {
        rotateCatalogAuthentication = true;
        Map<String, String> properties = properties(System.currentTimeMillis() + 3_600_000,
                ImmutableMap.of("token-refresh-enabled", "true", "token-expires-in-ms", "200"));
        Assertions.assertEquals(0, requests.get());
        Assertions.assertTrue(catalogRenewed.await(5, TimeUnit.SECONDS));
        // First create storage refresh state AFTER the catalog has rotated its authentication.
        GoogleCredentials credentials = createCredentials(new AccessToken("initial-token",
                new Date(System.currentTimeMillis() + 3_600_000)), properties);
        // The HTTP response can arrive just before Iceberg installs it in the live auth session.
        boolean refreshed = false;
        for (int i = 0; i < 50; i++) {
            try {
                credentials.refresh();
                refreshed = true;
                break;
            } catch (IOException e) {
                Thread.sleep(10);
            }
        }
        Assertions.assertTrue(refreshed, () -> "Storage refresh failed after catalog token rotation");
        Assertions.assertEquals("Bearer " + renewedCatalogToken, authorization.get());
        int before = requests.get();
        credentials.refresh();
        Assertions.assertEquals(before + 1, requests.get());
        Assertions.assertEquals("Bearer " + renewedCatalogToken, authorization.get());
        Assertions.assertEquals(1, oauthRequests.get());
        Assertions.assertEquals("catalog-token", properties.get("token"));
    }

    @Test
    public void testCatalogCloseImmediatelyRejectsFurtherRenewalWithoutGc() throws Exception {
        GoogleCredentials credentials = createCredentials(new AccessToken("initial-token", new Date(1)), properties(1));
        credentials.refresh();
        catalogs.get(0).close();
        catalogs.get(0).close();
        Assertions.assertThrows(IOException.class, credentials::refresh);
        Assertions.assertEquals(1, requests.get());
    }

    @Test
    public void testCloseCancelsCatalogTokenRenewal() throws Exception {
        Map<String, String> properties = properties(System.currentTimeMillis() + 3_600_000,
                ImmutableMap.of("token-refresh-enabled", "true", "token-expires-in-ms", "300"));
        createCredentials(new AccessToken("initial-token",
                new Date(System.currentTimeMillis() + 3_600_000)), properties);
        catalogs.get(0).close();
        Assertions.assertFalse(catalogRenewed.await(600, TimeUnit.MILLISECONDS));
        Assertions.assertEquals(0, oauthRequests.get());
        Assertions.assertEquals(0, requests.get());
    }

    @Test
    public void testFeClientAndScansShareCredentialsAcrossAliases() throws Exception {
        Map<String, String> properties = properties(1);
        Map<String, String> aliases = new HashMap<>(properties);
        aliases.put("gs.oauth2.token", "initial-token");
        aliases.put("gs.oauth2.token-expires-at", "1");
        aliases.put("gs.credential_provider_type", "ACCESS_TOKEN");
        GoogleCredentials fe = createCredentials(new AccessToken("initial-token", new Date(1)), aliases);
        Assertions.assertSame(fe, createCredentials(new AccessToken("initial-token", new Date(1)), properties));
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Future<?>> results = new ArrayList<>();
            for (int i = 0; i < 16; i++) {
                results.add(executor.submit(() -> {
                    Assertions.assertEquals("refreshed-token", GcsVendedCredentials.currentProperties(properties)
                            .get(GCPProperties.GCS_OAUTH2_TOKEN));
                }));
                results.add(executor.submit(() -> {
                    Assertions.assertEquals(ImmutableList.of("Bearer refreshed-token"), fe.getRequestMetadata().get("Authorization"));
                    return null;
                }));
            }
            for (Future<?> result : results) {
                result.get();
            }
            Assertions.assertEquals(1, requests.get());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testSeparateTablesAndCatalogsDoNotShareStorageTokens() {
        Map<String, String> first = properties(1);
        Map<String, String> second = new HashMap<>(first);
        second.put(GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT, "v1/other-table/credentials");
        AccessToken token = new AccessToken("initial-token", new Date(1));
        Assertions.assertNotSame(createCredentials(token, first), createCredentials(token, second));
        Assertions.assertNotSame(createCredentials(token, first), createCredentials(token, properties(1)));
    }

    @Test
    public void testMissingOrClosedCatalogNeverRecreatesAuthentication() throws Exception {
        Map<String, String> properties = properties(1);
        catalogs.get(0).close();
        Assertions.assertThrows(VendedCredentialsException.class,
                () -> GcsVendedCredentials.currentProperties(properties));
        properties.remove(IcebergRestVendedAuthManager.CONTEXT_ID);
        Assertions.assertThrows(VendedCredentialsException.class,
                () -> GcsVendedCredentials.currentProperties(properties));
        Assertions.assertEquals(0, requests.get());
    }

    @Test
    public void testNativeFileIoBorrowsCatalogSessionWithoutClosingIt() throws Exception {
        Map<String, String> properties = properties(1);
        try (OAuth2RefreshCredentialsHandler handler = OAuth2RefreshCredentialsHandler.create(properties)) {
            Assertions.assertEquals("refreshed-token", handler.refreshAccessToken().getTokenValue());
        }
        Assertions.assertEquals("refreshed-token", GcsVendedCredentials.currentProperties(properties)
                .get(GCPProperties.GCS_OAUTH2_TOKEN));
        Assertions.assertEquals(2, requests.get());
    }

    @Test
    public void testTableAuthenticationOverrideIsPreserved() throws Exception {
        Map<String, String> properties = properties(1);
        properties.put("token", "table-scoped-token");
        GcsVendedCredentials.currentProperties(properties);
        Assertions.assertEquals("Bearer table-scoped-token", authorization.get());
    }

    @Test
    public void testTableOverridesPreserveExpiryAndScopeOptions() {
        Map<String, String> base = ImmutableMap.of("token", "catalog-token", "token-expires-in-ms", "200", "scope", "read");
        Map<String, String> table = new HashMap<>(base);
        table.put("token", "table-token");
        AtomicReference<Map<String, String>> supplied = new AtomicReference<>();
        AuthManager manager = new AuthManager() {
            @Override
            public AuthSession catalogSession(RESTClient client, Map<String, String> properties) {
                return AuthSession.EMPTY;
            }

            @Override
            public AuthSession tableSession(TableIdentifier name, Map<String, String> properties, AuthSession parent) {
                supplied.set(properties);
                return parent;
            }

            @Override
            public void close() {
            }
        };
        try (GcsVendedCredentials.CatalogCredentials context =
                new GcsVendedCredentials.CatalogCredentials(null, manager, AuthSession.EMPTY, base)) {
            context.session(table);
            Assertions.assertEquals("table-token", supplied.get().get("token"));
            Assertions.assertEquals("200", supplied.get().get("token-expires-in-ms"));
            Assertions.assertEquals("read", supplied.get().get("scope"));
            context.session(base);
            Assertions.assertFalse(supplied.get().containsKey("token"));
        }
    }

    @Test
    public void testActiveFeCredentialsRemainSharedAfterManyTables() throws Exception {
        rotateCatalogAuthentication = true;
        Map<String, String> properties = properties(1,
                ImmutableMap.of("token-refresh-enabled", "true", "token-expires-in-ms", "200"));
        GoogleCredentials original = createCredentials(new AccessToken("initial-token", new Date(1)), properties);
        for (int i = 0; i < 1100; i++) {
            Map<String, String> other = new HashMap<>(properties);
            other.put(GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT, "v1/table-" + i + "/credentials");
            createCredentials(new AccessToken("initial-token", new Date(1)), other);
        }
        Assertions.assertTrue(catalogRenewed.await(5, TimeUnit.SECONDS));
        GoogleCredentials current = createCredentials(new AccessToken("initial-token", new Date(1)), properties);
        Assertions.assertSame(original, current);
        // Wait until the SDK has installed the OAuth response in the catalog session.
        for (int i = 0; ; i++) {
            try {
                current.refresh();
                break;
            } catch (IOException e) {
                if (i == 49) {
                    throw e;
                }
                Thread.sleep(10);
            }
        }
        Assertions.assertEquals("Bearer " + renewedCatalogToken, authorization.get());
        Assertions.assertEquals(1, oauthRequests.get());
    }

    @Test
    public void testInitializationFailureClosesRegisteredContext() {
        VendedRESTCatalog failed = new VendedRESTCatalog();
        Assertions.assertThrows(RuntimeException.class, () -> failed.initialize("failed", ImmutableMap.of(
                "uri", "http://127.0.0.1:" + server.getAddress().getPort() + "/catalog",
                "rest.auth.type", "oauth2", "token", "catalog-token", "token-refresh-enabled", "false",
                "io-impl", FailingFileIO.class.getName())));
        Assertions.assertNotNull(FailingFileIO.capturedProperties);
        Assertions.assertThrows(VendedCredentialsException.class,
                () -> IcebergRestVendedAuthManager.credentials(FailingFileIO.capturedProperties));
    }

    public static class FailingFileIO extends TestFileIO {
        private static Map<String, String> capturedProperties;

        @Override
        public void initialize(Map<String, String> properties) {
            capturedProperties = ImmutableMap.copyOf(properties);
            throw new IllegalStateException("FileIO initialization failure");
        }
    }

    public static class TestFileIO implements FileIO {
        @Override
        public InputFile newInputFile(String location) {
            throw new UnsupportedOperationException();
        }

        @Override
        public OutputFile newOutputFile(String location) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteFile(String location) {
            throw new UnsupportedOperationException();
        }
    }

    private GoogleCredentials createCredentials(AccessToken token, Map<String, String> properties) {
        return GcsVendedCredentials.createCredentials(token, properties);
    }

    private static String jwt(long expirationSeconds) {
        return "eyJhbGciOiJub25lIn0." + Base64.getUrlEncoder().withoutPadding().encodeToString(
                ("{\"exp\":" + expirationSeconds + "}").getBytes(StandardCharsets.UTF_8)) + ".signature";
    }

    private Map<String, String> properties(long expiresAt) {
        return properties(expiresAt, ImmutableMap.of());
    }

    private Map<String, String> properties(long expiresAt, Map<String, String> overrides) {
        Map<String, String> properties = new HashMap<>();
        properties.put("uri", "http://127.0.0.1:" + server.getAddress().getPort() + "/catalog");
        properties.put("rest.auth.type", "oauth2");
        properties.put("token", "catalog-token");
        properties.put("token-refresh-enabled", "false");
        properties.put("io-impl", TestFileIO.class.getName());
        properties.putAll(overrides);
        VendedRESTCatalog catalog = new VendedRESTCatalog();
        catalog.initialize("test", properties);
        catalogs.add(catalog);
        properties = new HashMap<>(catalog.properties());
        properties.put(GCPProperties.GCS_OAUTH2_TOKEN, "initial-token");
        properties.put(GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT, Long.toString(expiresAt));
        properties.put(GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT, "v1/credentials");
        return properties;
    }

    @Test
    public void testBackendSnapshotDropsCatalogAndDelegatedAuthentication() {
        Map<String, String> properties = properties(1);
        properties.put("doris.vended-credentials.session.id", "alice");
        properties.put("doris.vended-credentials.session.credential.token", "user-secret");
        // Use the catalog context here; delegated contexts are checked separately below.
        properties.remove("doris.vended-credentials.session.id");
        Map<String, String> snapshot = GcsVendedCredentials.snapshotProperties(properties);
        Assertions.assertEquals("refreshed-token", snapshot.get(GCPProperties.GCS_OAUTH2_TOKEN));
        Assertions.assertEquals("false", snapshot.get(GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENABLED));
        Assertions.assertFalse(snapshot.containsKey("token"));
        Assertions.assertFalse(snapshot.containsKey("rest.auth.type"));
        Assertions.assertFalse(snapshot.keySet().stream().anyMatch(key -> key.startsWith("doris.vended-credentials.")));
        Assertions.assertFalse(snapshot.containsKey(GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT));
    }

    @Test
    public void testDelegatedSessionIsBorrowedWithoutChangingTableAuthentication() {
        Map<String, String> properties = properties(System.currentTimeMillis() + 3_600_000);
        properties.put("token", "table-specific-token");
        org.apache.iceberg.catalog.SessionCatalog.SessionContext alice =
                new org.apache.iceberg.catalog.SessionCatalog.SessionContext("alice", "alice-identity",
                        ImmutableMap.of("token", "alice-token"), ImmutableMap.of("scope", "alice-scope"));
        Map<String, String> scoped = IcebergRestVendedAuthManager.withSession(properties, alice);
        Assertions.assertEquals("table-specific-token", scoped.get("token"));
        AtomicReference<org.apache.iceberg.catalog.SessionCatalog.SessionContext> captured = new AtomicReference<>();
        AuthSession catalog = noOpSession();
        AuthSession delegated = noOpSession();
        AuthManager manager = new AuthManager() {
            @Override
            public AuthSession initSession(RESTClient client, Map<String, String> props) {
                return catalog;
            }

            @Override
            public AuthSession catalogSession(RESTClient client, Map<String, String> props) {
                return catalog;
            }

            @Override
            public AuthSession contextualSession(org.apache.iceberg.catalog.SessionCatalog.SessionContext ctx,
                    AuthSession parent) {
                Assertions.assertSame(catalog, parent);
                captured.set(ctx);
                return delegated;
            }

            @Override
            public AuthSession tableSession(TableIdentifier table, Map<String, String> props, AuthSession parent) {
                return parent;
            }

            @Override
            public void close() {
            }
        };
        Assertions.assertSame(delegated, IcebergRestVendedAuthManager.contextualSession(manager, catalog, scoped));
        Assertions.assertEquals("alice", captured.get().sessionId());
        Assertions.assertEquals("alice-token", captured.get().credentials().get("token"));
        Assertions.assertEquals("alice-scope", captured.get().properties().get("scope"));
        Assertions.assertSame(catalog, IcebergRestVendedAuthManager.contextualSession(manager, catalog, properties));
    }

    private static AuthSession noOpSession() {
        return new AuthSession() {
            @Override
            public org.apache.iceberg.rest.HTTPRequest authenticate(org.apache.iceberg.rest.HTTPRequest request) {
                return request;
            }

            @Override
            public void close() {
            }
        };
    }

    private static String credentialsResponse(String token, long expiresAt) {
        return "{\"storage-credentials\":[{\"prefix\":\"gs://bucket/\",\"config\":{"
                + "\"gcs.oauth2.token\":\"" + token + "\",\"gcs.oauth2.token-expires-at\":\"" + expiresAt + "\"}}]}";
    }
}
