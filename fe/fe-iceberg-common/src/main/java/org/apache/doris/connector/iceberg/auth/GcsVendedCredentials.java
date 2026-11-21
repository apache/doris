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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/** Shares table credentials between FE clients and scan snapshots, using catalog-owned authentication. */
public final class GcsVendedCredentials {
    private GcsVendedCredentials() {
    }

    public static boolean isRefreshEnabled(Map<String, String> properties) {
        return properties.containsKey(GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT)
                && Boolean.parseBoolean(properties.getOrDefault(
                        GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENABLED, "true"));
    }

    public static GoogleCredentials createCredentials(AccessToken initialToken, Map<String, String> properties) {
        return IcebergRestVendedAuthManager.credentials(properties).credentials(initialToken, properties);
    }

    public static Map<String, String> currentProperties(Map<String, String> properties) {
        if (!properties.containsKey(GCPProperties.GCS_OAUTH2_TOKEN) || !isRefreshEnabled(properties)) {
            return properties;
        }
        try {
            AccessToken initial = new AccessToken(properties.get(GCPProperties.GCS_OAUTH2_TOKEN),
                    new Date(Long.parseLong(properties.getOrDefault(GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT, "0"))));
            GoogleCredentials credentials = createCredentials(initial, properties);
            // The Google auth library serializes concurrent FE and scan refreshes.
            credentials.refreshIfExpired();
            AccessToken token = credentials.getAccessToken();
            Map<String, String> current = new HashMap<>(properties);
            current.put(GCPProperties.GCS_OAUTH2_TOKEN, token.getTokenValue());
            current.put(GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT,
                    Long.toString(token.getExpirationTime().getTime()));
            return current;
        } catch (Exception e) {
            throw new VendedCredentialsException("Failed to refresh catalog-vended GCS credentials", e);
        }
    }

    /** A BE query may use only the current storage token, never catalog authentication or FE context. */
    public static Map<String, String> snapshotProperties(Map<String, String> properties) {
        Map<String, String> snapshot = new HashMap<>(currentProperties(properties));
        for (String key : ImmutableList.of(OAuth2Properties.TOKEN, OAuth2Properties.CREDENTIAL,
                OAuth2Properties.ACCESS_TOKEN_TYPE, OAuth2Properties.REFRESH_TOKEN_TYPE,
                OAuth2Properties.ID_TOKEN_TYPE, OAuth2Properties.JWT_TOKEN_TYPE,
                OAuth2Properties.SAML1_TOKEN_TYPE, OAuth2Properties.SAML2_TOKEN_TYPE,
                "rest.auth.type", GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT)) {
            snapshot.remove(key);
        }
        snapshot.keySet().removeIf(key -> key.startsWith("doris.vended-credentials.")
                || key.equalsIgnoreCase("header.Authorization"));
        snapshot.put(GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENABLED, "false");
        return snapshot;
    }

    /** No per-table HTTP clients, auth managers, renewal tasks or GC-dependent resource cleanup. */
    public static final class CatalogCredentials implements AutoCloseable {
        private final RESTClient client;
        private final AuthManager manager;
        private final AuthSession catalogSession;
        private final Map<String, String> catalogProperties;
        // These entries contain only storage-token state. Eviction can safely reconstruct it using
        // the live catalog session, unlike eviction of independent authentication sessions.
        private final Cache<Map<String, String>, GoogleCredentials> recentTokens =
                CacheBuilder.newBuilder().maximumSize(1024).build();
        // A live FE client keeps its credential canonical even after the recent-token cache evicts it.
        // Weak values contain no independently owned connections or authentication renewal tasks.
        private final Cache<Map<String, String>, GoogleCredentials> liveTokens =
                CacheBuilder.newBuilder().weakValues().build();
        private final ReentrantReadWriteLock lifecycle = new ReentrantReadWriteLock();
        private volatile boolean closed;

        public CatalogCredentials(RESTClient client, AuthManager manager, AuthSession catalogSession,
                Map<String, String> properties) {
            this.client = client;
            this.manager = manager;
            this.catalogSession = catalogSession;
            this.catalogProperties = ImmutableMap.copyOf(properties);
        }

        private void ensureOpen() {
            if (closed) {
                throw new IllegalStateException("REST catalog credential context is closed");
            }
        }

        public AuthSession session(Map<String, String> properties) {
            ensureOpen();
            Map<String, String> overrides = new HashMap<>(properties);
            // FileIO properties include the catalog's original token. Do not reinterpret that
            // stale copy as a table-specific override of the catalog's live auth session.
            for (String key : ImmutableList.of(OAuth2Properties.TOKEN, OAuth2Properties.CREDENTIAL,
                    OAuth2Properties.ACCESS_TOKEN_TYPE, OAuth2Properties.REFRESH_TOKEN_TYPE,
                    OAuth2Properties.ID_TOKEN_TYPE, OAuth2Properties.JWT_TOKEN_TYPE,
                    OAuth2Properties.SAML1_TOKEN_TYPE, OAuth2Properties.SAML2_TOKEN_TYPE)) {
                overrides.remove(key, catalogProperties.get(key));
            }
            // Keep expiry, scope and exchange options: a table token may need the same values
            // as the catalog token, but still owns a distinct authentication session.
            String endpoint = properties.getOrDefault(GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT, "gcs");
            TableIdentifier scope = TableIdentifier.of("gcs-credentials",
                    UUID.nameUUIDFromBytes((endpoint + "\0"
                            + properties.getOrDefault("doris.vended-credentials.session.id", ""))
                            .getBytes(StandardCharsets.UTF_8)).toString());
            return manager.tableSession(scope, overrides,
                    IcebergRestVendedAuthManager.contextualSession(manager, catalogSession, properties));
        }

        private GoogleCredentials credentials(AccessToken initial, Map<String, String> properties) {
            lifecycle.readLock().lock();
            try {
                ensureOpen();
                if (initial.getExpirationTime() == null || initial.getExpirationTime().getTime() <= 0) {
                    throw new IllegalArgumentException("Refreshable GCS credentials require a token expiration");
                }
                Map<String, String> identity = new HashMap<>(properties);
                identity.put(GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT,
                        new GCPProperties(properties).oauth2RefreshCredentialsEndpoint().orElseThrow(
                                () -> new IllegalArgumentException("GCS credentials refresh endpoint is required")));
                identity.put(GCPProperties.GCS_OAUTH2_TOKEN, initial.getTokenValue());
                identity.put(GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT,
                        Long.toString(initial.getExpirationTime().getTime()));
                // The S3-compatible adapter adds these aliases; they must not fork refresh state.
                identity.remove("gs.oauth2.token");
                identity.remove("gs.oauth2.token-expires-at");
                identity.remove("gs.credential_provider_type");
                identity.remove("gs.impersonation_service_account");
                Map<String, String> key = ImmutableMap.copyOf(identity);
                GoogleCredentials credentials = liveTokens.get(key,
                        () -> new RefreshableCredentials(initial, this, key));
                recentTokens.put(key, credentials);
                return credentials;
            } catch (ExecutionException e) {
                throw new VendedCredentialsException("Failed to create catalog-vended GCS credentials", e.getCause());
            } finally {
                lifecycle.readLock().unlock();
            }
        }

        private AccessToken refresh(Map<String, String> properties) throws IOException {
            lifecycle.readLock().lock();
            try {
                ensureOpen();
                String endpoint = properties.get(GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT);
                LoadCredentialsResponse response = client.withAuthSession(session(properties)).get(
                        endpoint, null, LoadCredentialsResponse.class, ImmutableMap.of(),
                        ErrorHandlers.defaultErrorHandler());
                List<Credential> credentials = response.credentials().stream()
                        .filter(credential -> credential.prefix().startsWith("gs"))
                        .collect(Collectors.toList());
                if (credentials.size() != 1) {
                    throw new IOException("Catalog must return exactly one GCS credential");
                }
                Map<String, String> config = credentials.get(0).config();
                String token = config.get(GCPProperties.GCS_OAUTH2_TOKEN);
                long expiration = Long.parseLong(config.get(GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT));
                if (token == null || token.isEmpty() || token.chars().anyMatch(Character::isWhitespace)
                        || expiration <= System.currentTimeMillis()) {
                    throw new IOException("Catalog returned an invalid or expired GCS access token");
                }
                return new AccessToken(token, new Date(expiration));
            } catch (RuntimeException e) {
                throw new IOException("Failed to refresh GCS credentials from the catalog", e);
            } finally {
                lifecycle.readLock().unlock();
            }
        }

        @Override
        public void close() {
            lifecycle.writeLock().lock();
            try {
                closed = true;
                recentTokens.invalidateAll();
                liveTokens.invalidateAll();
                // The enclosing REST catalog closes the shared client, session and auth manager.
            } finally {
                lifecycle.writeLock().unlock();
            }
        }
    }

    private static final class RefreshableCredentials extends GoogleCredentials {
        private final CatalogCredentials owner;
        private final Map<String, String> properties;

        private RefreshableCredentials(AccessToken initial, CatalogCredentials owner, Map<String, String> properties) {
            super(initial);
            this.owner = owner;
            this.properties = properties;
        }

        @Override
        public AccessToken refreshAccessToken() throws IOException {
            return owner.refresh(properties);
        }

        @Override
        public Map<String, List<String>> getRequestMetadata(URI uri) throws IOException {
            owner.ensureOpen();
            return super.getRequestMetadata(uri);
        }
    }
}
