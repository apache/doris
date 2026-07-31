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

import org.apache.doris.connector.api.ConnectorDelegatedCredential;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.iceberg.IcebergSessionCatalogAdapter.DelegatedTokenMode;

import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * Verifies the neutral-credential → Iceberg-REST bridge in {@link IcebergSessionCatalogAdapter}: how a user's
 * {@link ConnectorDelegatedCredential} is turned into the Iceberg {@code SessionCatalog.SessionContext} credential
 * map (per {@code delegated-token-mode}), that the stable session id rides through as the {@code AuthSession} key,
 * and that the delegated (per-user) catalog accessors fail closed when no credential is present — the security
 * contract re-migrated from #63068. The credential-mapping cases run entirely offline through the
 * {@code @VisibleForTesting} static {@code toIcebergSessionContext}; the fail-closed cases only need the
 * credential-presence guard, which precedes any real catalog use, so a {@code null} session catalog suffices.
 */
public class IcebergSessionCatalogAdapterTest {

    // ── delegated-token-mode = access_token: the token is the OAuth2 bearer verbatim, whatever its kind ──

    @Test
    public void accessTokenModePassesTokenVerbatimAsBearerRegardlessOfType() {
        // Even a JWT-kind credential is attached under the plain OAuth2 TOKEN (bearer) key in access_token mode —
        // the REST server treats it as an already-minted access token (no token exchange).
        SessionCatalog.SessionContext ctx = IcebergSessionCatalogAdapter.toIcebergSessionContext(
                session(ConnectorDelegatedCredential.Type.JWT, "raw-token", "sess-1"),
                DelegatedTokenMode.ACCESS_TOKEN);

        Assertions.assertEquals(ImmutableMap.of(OAuth2Properties.TOKEN, "raw-token"), ctx.credentials());
    }

    // ── delegated-token-mode = token_exchange: each credential kind maps to its typed OAuth2 token-type key ──

    @Test
    public void tokenExchangeModeMapsEachTypeToItsTokenTypeKey() {
        assertExchangeKey(ConnectorDelegatedCredential.Type.ACCESS_TOKEN, OAuth2Properties.TOKEN);
        assertExchangeKey(ConnectorDelegatedCredential.Type.ID_TOKEN, OAuth2Properties.ID_TOKEN_TYPE);
        assertExchangeKey(ConnectorDelegatedCredential.Type.JWT, OAuth2Properties.JWT_TOKEN_TYPE);
        assertExchangeKey(ConnectorDelegatedCredential.Type.SAML, OAuth2Properties.SAML2_TOKEN_TYPE);
    }

    private static void assertExchangeKey(ConnectorDelegatedCredential.Type type, String expectedKey) {
        SessionCatalog.SessionContext ctx = IcebergSessionCatalogAdapter.toIcebergSessionContext(
                session(type, "tok-" + type, "s"), DelegatedTokenMode.TOKEN_EXCHANGE);
        Assertions.assertEquals(ImmutableMap.of(expectedKey, "tok-" + type), ctx.credentials(),
                "token_exchange must key the token by its OAuth2 token-type for " + type);
    }

    @Test
    public void sessionIdIsCarriedAsTheAuthSessionKey() {
        // The stable, FE-forward-preserved session id must become the SessionContext.sessionId() so a user's
        // queries reuse one minted AuthSession (not re-authenticate per query).
        SessionCatalog.SessionContext ctx = IcebergSessionCatalogAdapter.toIcebergSessionContext(
                session(ConnectorDelegatedCredential.Type.ACCESS_TOKEN, "t", "stable-session-99"),
                DelegatedTokenMode.ACCESS_TOKEN);
        Assertions.assertEquals("stable-session-99", ctx.sessionId());
    }

    @Test
    public void noCredentialYieldsEmptyCredentialMap() {
        SessionCatalog.SessionContext ctx = IcebergSessionCatalogAdapter.toIcebergSessionContext(
                session(null, null, "s"), DelegatedTokenMode.ACCESS_TOKEN);
        Assertions.assertTrue(ctx.credentials().isEmpty(),
                "a credential-less session must not attach any bearer/token to the REST request");
    }

    // ── fail-closed: the per-user accessors reject a session that carries no delegated credential ──

    @Test
    public void delegatedCatalogFailsClosedWithoutCredential() {
        IcebergSessionCatalogAdapter adapter =
                new IcebergSessionCatalogAdapter(null, null, DelegatedTokenMode.ACCESS_TOKEN);
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> adapter.delegatedCatalog(session(null, null, "s")));
        Assertions.assertTrue(e.getMessage().contains("delegated credential"));
    }

    @Test
    public void delegatedViewCatalogFailsClosedWithoutCredential() {
        IcebergSessionCatalogAdapter adapter =
                new IcebergSessionCatalogAdapter(null, null, DelegatedTokenMode.ACCESS_TOKEN);
        Assertions.assertThrows(DorisConnectorException.class,
                () -> adapter.delegatedViewCatalog(session(null, null, "s")));
    }

    // ── delegated-token-mode parsing ──

    @Test
    public void delegatedTokenModeFromStringParsesKnownAndRejectsUnknown() {
        Assertions.assertEquals(DelegatedTokenMode.ACCESS_TOKEN, DelegatedTokenMode.fromString("access_token"));
        Assertions.assertEquals(DelegatedTokenMode.TOKEN_EXCHANGE, DelegatedTokenMode.fromString("token_exchange"));
        // Case-insensitive (mirrors the connector-property parsing).
        Assertions.assertEquals(DelegatedTokenMode.ACCESS_TOKEN, DelegatedTokenMode.fromString("ACCESS_TOKEN"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> DelegatedTokenMode.fromString("bogus"));
    }

    /**
     * A minimal {@link ConnectorSession} carrying an optional delegated credential and a fixed session id (as the
     * query id, which {@link ConnectorSession#getSessionId()} falls back to). A {@code null} type yields a
     * credential-less session.
     */
    private static ConnectorSession session(ConnectorDelegatedCredential.Type type, String token, String sessionId) {
        ConnectorDelegatedCredential credential =
                type == null ? null : new ConnectorDelegatedCredential(type, token);
        return new ConnectorSession() {
            @Override
            public Optional<ConnectorDelegatedCredential> getDelegatedCredential() {
                return Optional.ofNullable(credential);
            }

            @Override
            public String getQueryId() {
                return sessionId;
            }

            @Override
            public String getUser() {
                return "u";
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
                return "ice";
            }

            @Override
            public <T> T getProperty(String name, Class<T> clazz) {
                return null;
            }

            @Override
            public Map<String, String> getCatalogProperties() {
                return Collections.emptyMap();
            }
        };
    }
}
