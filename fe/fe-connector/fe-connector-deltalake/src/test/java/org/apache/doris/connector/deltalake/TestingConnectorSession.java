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


package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

final class TestingConnectorSession implements ConnectorSession {
    private final String queryId;
    private final Map<String, String> properties;
    private final ConnectorStatementScope scope = new ConnectorStatementScope() {
        private final Map<String, Object> values = new HashMap<>();

        @Override
        @SuppressWarnings("unchecked")
        public <T> T computeIfAbsent(String key, Supplier<T> loader) {
            return (T) values.computeIfAbsent(key, ignored -> loader.get());
        }
    };

    TestingConnectorSession(String queryId) {
        this(queryId, Collections.emptyMap());
    }

    TestingConnectorSession(String queryId, Map<String, String> properties) {
        this.queryId = queryId;
        this.properties = Map.copyOf(properties);
    }

    @Override
    public String getQueryId() {
        return queryId;
    }

    @Override
    public String getUser() {
        return "delta_user";
    }

    @Override
    public String getTimeZone() {
        return "UTC";
    }

    @Override
    public String getLocale() {
        return "en-US";
    }

    @Override
    public long getCatalogId() {
        return 42;
    }

    @Override
    public String getCatalogName() {
        return "delta_test";
    }

    @Override
    public <T> T getProperty(String name, Class<T> type) {
        return type.cast(properties.get(name));
    }

    @Override
    public Map<String, String> getCatalogProperties() {
        return Collections.emptyMap();
    }

    @Override
    public ConnectorStatementScope getStatementScope() {
        return scope;
    }
}
