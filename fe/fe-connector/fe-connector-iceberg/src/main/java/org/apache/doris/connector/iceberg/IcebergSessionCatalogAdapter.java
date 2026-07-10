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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;

import java.util.Map;
import java.util.Optional;

/**
 * Bridges the querying user's neutral {@link ConnectorDelegatedCredential} (carried on the
 * {@link ConnectorSession}) to Iceberg REST {@code SessionCatalog} calls, re-migrated from the pre-P6 fe-core
 * {@code IcebergSessionCatalogAdapter} and retargeted off the neutral SPI (no fe-core imports).
 *
 * <p>When {@code iceberg.rest.session=user}, the connector holds a SINGLE shared {@link RESTSessionCatalog}
 * (never one-per-user, exactly as Trino / #63068) and this adapter mints a per-request session-bound
 * {@link Catalog}/{@link ViewCatalog} from it via {@code asCatalog(ctx)} / {@code asViewCatalog(ctx)}, carrying
 * the user's OAuth2/delegated credential in the {@link SessionCatalog.SessionContext}. A request without a
 * credential either falls back to the plain shared catalog ({@link #catalog}/{@link #viewCatalog}) or fails
 * closed ({@link #delegatedCatalog}/{@link #delegatedViewCatalog}) — the connector uses the fail-closed variants
 * under {@code session=user}, so a tokenless request is rejected rather than served a shared identity.
 */
class IcebergSessionCatalogAdapter {

    // The plain (non-delegated) catalog: the shared default asCatalog(SessionContext.createEmpty()). Returned for
    // credential-less requests on the graceful path; the connector never routes session=user through it.
    private final Catalog catalog;
    // The session-aware REST catalog (empty for a non-REST / non-session catalog). asCatalog(ctx)/asViewCatalog(ctx)
    // attach the per-user delegated credential. A SINGLE shared instance — never one-per-user.
    private final Optional<RESTSessionCatalog> sessionCatalog;
    private final DelegatedTokenMode delegatedTokenMode;

    IcebergSessionCatalogAdapter(Catalog catalog, RESTSessionCatalog sessionCatalog) {
        this(catalog, sessionCatalog, DelegatedTokenMode.ACCESS_TOKEN);
    }

    IcebergSessionCatalogAdapter(Catalog catalog, RESTSessionCatalog sessionCatalog,
            DelegatedTokenMode delegatedTokenMode) {
        this.catalog = catalog;
        this.sessionCatalog = Optional.ofNullable(sessionCatalog);
        this.delegatedTokenMode = delegatedTokenMode;
    }

    /** Graceful table/namespace catalog: the plain shared catalog when no credential, else the per-user one. */
    Catalog catalog(ConnectorSession session) {
        if (!hasDelegatedCredential(session)) {
            return catalog;
        }
        return requireSessionCatalog().asCatalog(toIcebergSessionContext(session, delegatedTokenMode));
    }

    /** Fail-closed table/namespace catalog: requires a credential (rejects a tokenless session=user request). */
    Catalog delegatedCatalog(ConnectorSession session) {
        requireDelegatedCredential(session);
        return requireSessionCatalog().asCatalog(toIcebergSessionContext(session, delegatedTokenMode));
    }

    /** Graceful view catalog: the plain catalog's view facet when no credential (may be null), else per-user. */
    ViewCatalog viewCatalog(ConnectorSession session) {
        if (!hasDelegatedCredential(session)) {
            return catalog instanceof ViewCatalog ? (ViewCatalog) catalog : null;
        }
        return requireSessionCatalog().asViewCatalog(toIcebergSessionContext(session, delegatedTokenMode));
    }

    /** Fail-closed view catalog: requires a credential. asCatalog(ctx) is NOT a ViewCatalog, so views need this. */
    ViewCatalog delegatedViewCatalog(ConnectorSession session) {
        requireDelegatedCredential(session);
        return requireSessionCatalog().asViewCatalog(toIcebergSessionContext(session, delegatedTokenMode));
    }

    /**
     * Builds the Iceberg {@link SessionCatalog.SessionContext} from a Doris {@link ConnectorSession}: the stable
     * {@code sessionId} (the OAuth2 AuthSession cache key, preserved across FE forwarding) plus the credential map
     * for the current {@code delegatedTokenMode}. A credential-less session yields an empty credential map.
     */
    @VisibleForTesting
    static SessionCatalog.SessionContext toIcebergSessionContext(ConnectorSession session,
            DelegatedTokenMode delegatedTokenMode) {
        Map<String, String> credentials = ImmutableMap.of();
        if (session.getDelegatedCredential().isPresent()) {
            credentials = toIcebergCredentials(session.getDelegatedCredential().get(), delegatedTokenMode);
        }
        return new SessionCatalog.SessionContext(session.getSessionId(), null, credentials, ImmutableMap.of());
    }

    private RESTSessionCatalog requireSessionCatalog() {
        if (!sessionCatalog.isPresent()) {
            throw new DorisConnectorException("Iceberg REST user session requires a session-aware Iceberg catalog");
        }
        return sessionCatalog.get();
    }

    private static void requireDelegatedCredential(ConnectorSession session) {
        if (!hasDelegatedCredential(session)) {
            // Fail closed: a user-session catalog has no shared identity to borrow, so a request that carries no
            // delegated credential is rejected rather than served another request's (or a shared) credential.
            throw new DorisConnectorException("Iceberg REST user session requires a delegated credential");
        }
    }

    private static Map<String, String> toIcebergCredentials(ConnectorDelegatedCredential credential,
            DelegatedTokenMode delegatedTokenMode) {
        if (delegatedTokenMode == DelegatedTokenMode.ACCESS_TOKEN) {
            // access_token: pass the token verbatim as the OAuth2 bearer.
            return ImmutableMap.of(OAuth2Properties.TOKEN, credential.getToken());
        }
        // token_exchange: pass the original token under its typed key so the REST server performs the exchange.
        return ImmutableMap.of(IcebergDelegatedCredentialUtils.credentialKey(credential.getType()),
                credential.getToken());
    }

    private static boolean hasDelegatedCredential(ConnectorSession session) {
        return session != null && session.getDelegatedCredential().isPresent();
    }

    /**
     * How the delegated credential is attached to the Iceberg REST session (re-migrated from the pre-P6
     * {@code IcebergRestProperties.DelegatedTokenMode}). {@code access_token} = verbatim OAuth2 bearer;
     * {@code token_exchange} = the typed token key so the REST server exchanges it (RFC-8693).
     */
    enum DelegatedTokenMode {
        ACCESS_TOKEN("access_token"),
        TOKEN_EXCHANGE("token_exchange");

        private final String value;

        DelegatedTokenMode(String value) {
            this.value = value;
        }

        static DelegatedTokenMode fromString(String value) {
            for (DelegatedTokenMode mode : values()) {
                if (mode.value.equalsIgnoreCase(value)) {
                    return mode;
                }
            }
            throw new IllegalArgumentException("Invalid delegated token mode: " + value
                    + ". Supported values are: access_token, token_exchange");
        }
    }
}
