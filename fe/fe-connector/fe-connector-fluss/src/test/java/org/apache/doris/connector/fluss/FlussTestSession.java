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

package org.apache.doris.connector.fluss;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * A session with a real per-statement scope, for tests that assert what a statement shares.
 *
 * <p>The scope is a plain map keyed exactly as the engine's is, so the sharing (and the isolation
 * between two query ids) is the connector's own, not something this class arranges.
 */
final class FlussTestSession implements ConnectorSession {

    private final long catalogId;
    private final String queryId;
    private final Map<String, Object> values = new ConcurrentHashMap<>();
    private final ConnectorStatementScope scope = new ConnectorStatementScope() {
        @SuppressWarnings("unchecked")
        @Override
        public <T> T computeIfAbsent(String key, Supplier<T> loader) {
            return (T) values.computeIfAbsent(key, ignored -> loader.get());
        }
    };

    FlussTestSession(long catalogId, String queryId) {
        this.catalogId = catalogId;
        this.queryId = queryId;
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
    public String getCatalogName() {
        return "fluss_catalog";
    }

    @Override
    public String getUser() {
        return "test_user";
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
    public <T> T getProperty(String name, Class<T> type) {
        return null;
    }

    @Override
    public Map<String, String> getCatalogProperties() {
        return Collections.emptyMap();
    }
}
