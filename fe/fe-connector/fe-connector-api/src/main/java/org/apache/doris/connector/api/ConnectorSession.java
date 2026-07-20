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

package org.apache.doris.connector.api;

import org.apache.doris.connector.api.handle.ConnectorTransaction;

import java.util.Map;
import java.util.Optional;

/**
 * Session context passed to every connector operation.
 */
public interface ConnectorSession {

    /** Returns the unique query identifier. */
    String getQueryId();

    /**
     * Returns a stable per-connection session identifier, preserved across FE observer&rarr;master forwarding.
     *
     * <p>Used as the Iceberg {@code SessionCatalog.SessionContext.sessionId()} — the OAuth2 {@code AuthSession}
     * cache key — for {@link ConnectorCapability#SUPPORTS_USER_SESSION} connectors, so a session's queries reuse
     * one minted auth session rather than re-authenticating per query. The default falls back to
     * {@link #getQueryId()} for sessions/tests that carry no session id; the engine session implementation
     * overrides it with the captured (and FE-forward-preserved) session id.</p>
     */
    default String getSessionId() {
        return getQueryId();
    }

    /**
     * Returns the user's per-connection delegated credential (OIDC/JWT/SAML), when one was captured at
     * authentication and this session targets a connector that consumes it.
     *
     * <p>Populated ONLY when the connector declares {@link ConnectorCapability#SUPPORTS_USER_SESSION}
     * (least-privilege: a connector that would never use the token never receives it). The credential is a
     * neutral SPI DTO — the connector reads it here instead of any fe-core type. Empty by default (no
     * credential, or a connector that does not opt in).</p>
     */
    default Optional<ConnectorDelegatedCredential> getDelegatedCredential() {
        return Optional.empty();
    }

    /** Returns the authenticated user name. */
    String getUser();

    /** Returns the session time zone identifier (e.g. "Asia/Shanghai"). */
    String getTimeZone();

    /** Returns the session locale (e.g. "en_US"). */
    String getLocale();

    /** Returns the catalog id. */
    long getCatalogId();

    /** Returns the catalog name this session is bound to. */
    String getCatalogName();

    /** Retrieves a typed session/catalog property. */
    <T> T getProperty(String name, Class<T> type);

    /** Returns all catalog-level configuration properties. */
    Map<String, String> getCatalogProperties();

    /**
     * Returns session-level variable overrides relevant to connector operations.
     *
     * <p>These are per-query settings from the user session (e.g., SET statements)
     * that connectors may need for planning decisions. Keys are the variable names
     * as defined in the FE session variable registry.</p>
     *
     * @return unmodifiable map of session variable name → string value; never null
     */
    default Map<String, String> getSessionProperties() {
        return java.util.Collections.emptyMap();
    }

    /**
     * Returns the transaction this session is currently bound to, if any.
     *
     * <p>Used by connectors whose {@code begin*} write operations need to
     * attach work to an outer transaction opened by
     * {@link ConnectorWriteOps#beginTransaction(ConnectorSession)}.
     * Connectors with statement-scoped writes (e.g. JDBC auto-commit) can
     * ignore this and the default empty value.</p>
     */
    default Optional<ConnectorTransaction> getCurrentTransaction() {
        return Optional.empty();
    }

    /**
     * Binds a transaction to this session so that connector {@code begin*} /
     * {@code planWrite} operations can attach their work to it. Mutable session
     * implementations (e.g. the engine's {@code ConnectorSessionImpl}) override
     * this; the default rejects binding, matching the empty default of
     * {@link #getCurrentTransaction()}.
     */
    default void setCurrentTransaction(ConnectorTransaction txn) {
        throw new UnsupportedOperationException("setCurrentTransaction is not supported by this session");
    }

    /**
     * Allocates a globally-unique engine (Doris) transaction id for a connector
     * transaction opened via {@link ConnectorWriteOps#beginTransaction(ConnectorSession)}.
     *
     * <p>The id is the engine-side transaction id: it is registered in the engine
     * transaction registry and stamped into the connector's data sink, so a
     * connector must obtain it from the engine rather than mint its own. The
     * default throws; the engine session implementation overrides it.</p>
     *
     * @return a fresh engine transaction id
     */
    default long allocateTransactionId() {
        throw new UnsupportedOperationException("transaction id allocation not supported");
    }

    /**
     * Returns the per-statement scope for this session — a memoization arena that lives exactly as long
     * as the current SQL statement, letting a connector load a table (and derive per-statement state)
     * once and share that single object across every read + write resolver in the statement.
     *
     * <p>The default is {@link ConnectorStatementScope#NONE} (no memoization = load every time), so
     * every existing implementation is unaffected. The engine session implementation overrides it with a
     * scope hung on the statement, captured at session construction (the request thread, where the
     * statement context is reachable) so off-thread scan pumps that reuse the session still reach it.</p>
     */
    default ConnectorStatementScope getStatementScope() {
        return ConnectorStatementScope.NONE;
    }
}
