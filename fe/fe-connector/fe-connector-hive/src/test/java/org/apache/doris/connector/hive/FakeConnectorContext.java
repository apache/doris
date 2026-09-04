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

import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorStorageContext;

import java.util.Collections;
import java.util.Map;

/**
 * Minimal {@link ConnectorContext} test double: carries a fixed catalog identity plus the two maps a
 * deployment-level setting can arrive in — this plugin's own {@code hms.conf}
 * ({@link #getConnectorConfig()}) and fe.conf as forwarded by the engine ({@link #getEnvironment()}).
 * Everything else uses the interface defaults.
 */
public class FakeConnectorContext implements ConnectorContext, ConnectorStorageContext {

    // This double is both halves of the context, so a subclass that overrides a storage method (several
    // tests do, anonymously) still has the connector reach that override.
    @Override
    public ConnectorStorageContext getStorageContext() {
        return this;
    }

    private final String catalogName;
    private final long catalogId;
    private final Map<String, String> environment;
    private final Map<String, String> connectorConfig;

    public FakeConnectorContext() {
        this("test_catalog", 0L, Collections.emptyMap());
    }

    public FakeConnectorContext(Map<String, String> environment) {
        this("test_catalog", 0L, environment);
    }

    public FakeConnectorContext(String catalogName, long catalogId, Map<String, String> environment) {
        this(catalogName, catalogId, environment, Collections.emptyMap());
    }

    public FakeConnectorContext(String catalogName, long catalogId, Map<String, String> environment,
            Map<String, String> connectorConfig) {
        this.catalogName = catalogName;
        this.catalogId = catalogId;
        this.environment = environment == null ? Collections.emptyMap() : environment;
        this.connectorConfig = connectorConfig == null ? Collections.emptyMap() : connectorConfig;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public long getCatalogId() {
        return catalogId;
    }

    @Override
    public Map<String, String> getEnvironment() {
        return environment;
    }

    @Override
    public Map<String, String> getConnectorConfig() {
        return connectorConfig;
    }
}
