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

package org.apache.doris.connector;

import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.connector.api.ConnectorDelegatedCredential;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.datasource.DelegatedCredential;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.qe.VariableMgr;

import java.util.Collections;
import java.util.Map;

/**
 * Builder for {@link ConnectorSession} instances.
 *
 * <p>Use {@link #from(ConnectContext)} to bridge from Doris' internal session context,
 * or {@link #create()} for test scenarios without a live {@link ConnectContext}.
 */
public final class ConnectorSessionBuilder {

    private String queryId;
    private String user;
    private String timeZone;
    private String locale;
    private long catalogId;
    private String catalogName;
    private Map<String, String> catalogProperties = Collections.emptyMap();
    private Map<String, String> sessionProperties = Collections.emptyMap();
    // The originating ConnectContext (from #from), read at build() time to pull the retained per-connection
    // delegated credential + session id off SessionContext — but ONLY when the target connector declares
    // SUPPORTS_USER_SESSION (userSessionCapable). Kept as a reference (not eagerly extracted) so a non-opt-in
    // connector never even touches the credential (least-privilege).
    private ConnectContext connectContext;
    private boolean userSessionCapable;
    // Explicit overrides for tests / callers without a live ConnectContext; when set (and capable) they win
    // over the ConnectContext extraction.
    private String sessionId;
    private ConnectorDelegatedCredential delegatedCredential;

    private ConnectorSessionBuilder() {}

    /**
     * Creates a builder pre-populated from the given {@link ConnectContext}.
     *
     * @param ctx the active connection context
     * @return a builder with queryId, user, timeZone, and session properties set
     */
    public static ConnectorSessionBuilder from(ConnectContext ctx) {
        ConnectorSessionBuilder b = new ConnectorSessionBuilder();
        b.queryId = ctx.queryId() != null ? DebugUtil.printId(ctx.queryId()) : "";
        b.user = ctx.getQualifiedUser();
        b.timeZone = ctx.getSessionVariable().getTimeZone();
        b.locale = "en_US";  // Doris doesn't have per-session locale yet
        b.sessionProperties = extractSessionProperties(ctx);
        b.connectContext = ctx;  // read for the delegated credential at build() time, gated by capability
        return b;
    }

    /** Creates a builder without ConnectContext (for testing). */
    public static ConnectorSessionBuilder create() {
        return new ConnectorSessionBuilder();
    }

    public ConnectorSessionBuilder withCatalogId(long catalogId) {
        this.catalogId = catalogId;
        return this;
    }

    public ConnectorSessionBuilder withCatalogName(String catalogName) {
        this.catalogName = catalogName;
        return this;
    }

    public ConnectorSessionBuilder withCatalogProperties(Map<String, String> props) {
        this.catalogProperties = props != null ? props : Collections.emptyMap();
        return this;
    }

    public ConnectorSessionBuilder withSessionProperties(Map<String, String> props) {
        this.sessionProperties = props != null ? props : Collections.emptyMap();
        return this;
    }

    public ConnectorSessionBuilder withQueryId(String queryId) {
        this.queryId = queryId;
        return this;
    }

    public ConnectorSessionBuilder withUser(String user) {
        this.user = user;
        return this;
    }

    public ConnectorSessionBuilder withTimeZone(String timeZone) {
        this.timeZone = timeZone;
        return this;
    }

    /**
     * Declares whether the target connector consumes the user's delegated credential
     * ({@link org.apache.doris.connector.api.ConnectorCapability#SUPPORTS_USER_SESSION}). When {@code false}
     * (the default), {@link #build()} carries neither the session id nor the credential onto the session, so a
     * connector that would never use the OIDC token never receives it (least-privilege).
     */
    public ConnectorSessionBuilder withUserSessionCapability(boolean capable) {
        this.userSessionCapable = capable;
        return this;
    }

    /** Sets the session id explicitly (for callers without a live {@link ConnectContext}, e.g. tests). */
    public ConnectorSessionBuilder withSessionId(String sessionId) {
        this.sessionId = sessionId;
        return this;
    }

    /** Sets the delegated credential explicitly (for callers without a live {@link ConnectContext}, e.g. tests). */
    public ConnectorSessionBuilder withDelegatedCredential(ConnectorDelegatedCredential credential) {
        this.delegatedCredential = credential;
        return this;
    }

    /** Builds an immutable {@link ConnectorSession} instance. */
    public ConnectorSession build() {
        String sid = null;
        ConnectorDelegatedCredential cred = null;
        // Only a SUPPORTS_USER_SESSION connector receives the credential. An explicit override (tests) wins;
        // otherwise pull the retained per-connection SessionContext off the originating ConnectContext (the
        // #63068 generic base re-materializes it on the executing FE, incl. after observer->master forwarding).
        if (userSessionCapable) {
            if (delegatedCredential != null) {
                sid = sessionId;
                cred = delegatedCredential;
            } else if (connectContext != null) {
                SessionContext sc = connectContext.getSessionContext();
                if (sc != null && sc.hasDelegatedCredential()) {
                    sid = sc.getSessionId();
                    cred = toConnectorCredential(sc.getDelegatedCredential().get());
                }
            }
        }
        return new ConnectorSessionImpl(queryId, user, timeZone, locale,
                catalogId, catalogName, catalogProperties, sessionProperties, sid, cred);
    }

    /**
     * Maps the fe-core {@link DelegatedCredential} to the neutral SPI {@link ConnectorDelegatedCredential} so
     * the connector never imports a fe-core type. The {@code Type} is bridged by enum name — the two enums are
     * kept constant-for-constant identical ({@code ACCESS_TOKEN/ID_TOKEN/JWT/SAML}), so an added-but-unmapped
     * type fails loud here rather than being silently dropped.
     */
    private static ConnectorDelegatedCredential toConnectorCredential(DelegatedCredential credential) {
        return new ConnectorDelegatedCredential(
                ConnectorDelegatedCredential.Type.valueOf(credential.getType().name()),
                credential.getToken(), credential.getExpiresAtMillis());
    }

    /**
     * Extracts all visible session variables from the connect context.
     * Uses {@link VariableMgr#toMap} to avoid maintaining a hard-coded whitelist.
     * Server-level globals (e.g., lower_case_table_names) are also included.
     */
    private static Map<String, String> extractSessionProperties(ConnectContext ctx) {
        Map<String, String> props = VariableMgr.toMap(ctx.getSessionVariable());
        // Server-level lower_case_table_names for identifier mapping
        props.put("lower_case_table_names",
                String.valueOf(GlobalVariable.lowerCaseTableNames));
        // MaxCompute write block-id cap: the connector cannot import fe-core Config, so the tunable
        // Config.max_compute_write_max_block_count is surfaced through this channel (same as
        // lower_case_table_names above) and read back via ConnectorSession.getSessionProperties().
        // Key must stay byte-identical to MaxComputeConnectorMetadata.MAX_COMPUTE_WRITE_MAX_BLOCK_COUNT.
        props.put("max_compute_write_max_block_count",
                String.valueOf(Config.max_compute_write_max_block_count));
        return props;
    }
}
