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

import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.connector.api.ConnectorSession;
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

    /** Builds an immutable {@link ConnectorSession} instance. */
    public ConnectorSession build() {
        return new ConnectorSessionImpl(queryId, user, timeZone, locale,
                catalogId, catalogName, catalogProperties, sessionProperties);
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
        return props;
    }
}
