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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorStatementScope;

import java.util.Collections;
import java.util.Map;

/**
 * Minimal {@link ConnectorSession} for hive-connector unit tests, carrying a catalog id and a per-statement
 * scope. The heterogeneous gateway keys its per-statement sibling-metadata funnel on
 * {@code "metadata:" + getCatalogId() + ":" + ownerLabel} and reads the scope via {@link #getStatementScope()},
 * so a test wires a live {@link TestStatementScope} to prove sharing (or {@link ConnectorStatementScope#NONE} to
 * prove the pre-funnel load-every-time behavior). Mirrors the iceberg connector's test session.
 */
final class ScopeSession implements ConnectorSession {

    private final long catalogId;
    private final String queryId;
    private final ConnectorStatementScope scope;

    ScopeSession(long catalogId, String queryId, ConnectorStatementScope scope) {
        this.catalogId = catalogId;
        this.queryId = queryId;
        this.scope = scope;
    }

    @Override
    public long getCatalogId() {
        return catalogId;
    }

    @Override
    public String getQueryId() {
        return queryId;
    }

    @Override
    public ConnectorStatementScope getStatementScope() {
        return scope;
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
    public String getCatalogName() {
        return "c";
    }

    @Override
    public <T> T getProperty(String name, Class<T> type) {
        return null;
    }

    @Override
    public Map<String, String> getCatalogProperties() {
        return Collections.emptyMap();
    }
}
