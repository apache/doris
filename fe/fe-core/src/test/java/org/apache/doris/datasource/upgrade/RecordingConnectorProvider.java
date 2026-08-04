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

package org.apache.doris.datasource.upgrade;

import org.apache.doris.connector.ConnectorFactory;
import org.apache.doris.connector.ConnectorPluginManager;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorProvider;
import org.apache.doris.connector.spi.ConnectorSession;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A connector provider that records what it was asked to build.
 *
 * <p>fe-core depends only on {@code fe-connector-spi}, so no real connector is
 * on the unit-test classpath and every catalog would otherwise land in the degraded arm of
 * {@code CatalogFactory}. Registering a recorder instead buys the strongest statement a fe-core test can
 * make about a migrated catalog: the right provider was chosen, and the properties that reach it are
 * exactly the ones the old FE persisted. A real connector could not be used for this even if it were on
 * the classpath -- there is no way to observe the map it received.
 */
public class RecordingConnectorProvider implements ConnectorProvider {

    /** One {@code create()} invocation. */
    public static final class Call {
        public final Map<String, String> properties;
        public final String catalogName;
        public final long catalogId;

        Call(Map<String, String> properties, ConnectorContext context) {
            // Defensive snapshot: the caller may keep mutating the map it handed us.
            this.properties = new TreeMap<>(properties);
            this.catalogName = context.getCatalogName();
            this.catalogId = context.getCatalogId();
        }
    }

    /** The providers installed for one test, by catalog type. */
    public static final class Registry {
        private final Map<String, RecordingConnectorProvider> byType = new HashMap<>();

        public RecordingConnectorProvider get(String type) {
            RecordingConnectorProvider provider = byType.get(type);
            if (provider == null) {
                throw new IllegalArgumentException("no recording provider installed for type '" + type
                        + "'; installed: " + byType.keySet());
            }
            return provider;
        }
    }

    public final List<Call> calls = new ArrayList<>();

    private final String type;
    private final boolean failOnCreate;

    private RecordingConnectorProvider(String type, boolean failOnCreate) {
        this.type = type;
        this.failOnCreate = failOnCreate;
    }

    /** Installs one recording provider per type, replacing whatever the previous test class registered. */
    public static Registry installFor(List<String> types) {
        Registry registry = new Registry();
        ConnectorPluginManager manager = new ConnectorPluginManager();
        for (String type : types) {
            RecordingConnectorProvider provider = new RecordingConnectorProvider(type, false);
            registry.byType.put(type, provider);
            manager.registerProvider(provider);
        }
        ConnectorFactory.initPluginManager(manager);
        return registry;
    }

    /**
     * Installs a provider whose {@code create()} throws, standing in for a connector that rejects a
     * property an older FE stored without validating, or a half-installed plugin.
     */
    public static Registry installThrowingFor(String type) {
        Registry registry = new Registry();
        ConnectorPluginManager manager = new ConnectorPluginManager();
        RecordingConnectorProvider provider = new RecordingConnectorProvider(type, true);
        registry.byType.put(type, provider);
        manager.registerProvider(provider);
        ConnectorFactory.initPluginManager(manager);
        return registry;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public boolean isStandaloneCatalogType() {
        return true;
    }

    @Override
    public Connector create(Map<String, String> properties, ConnectorContext context) {
        calls.add(new Call(properties, context));
        if (failOnCreate) {
            throw new IllegalArgumentException(
                    "simulated connector rejection of a property persisted by an older FE");
        }
        RecordingConnector connector = new RecordingConnector();
        created.add(connector);
        return connector;
    }

    /** Every connector this provider handed out, in creation order. */
    public final List<RecordingConnector> created = new ArrayList<>();

    /** The properties handed to the connector that actually ends up serving the catalog. */
    public Map<String, String> lastProperties() {
        if (calls.isEmpty()) {
            throw new IllegalStateException("the provider was never asked to build a connector");
        }
        return calls.get(calls.size() - 1).properties;
    }

    /** A connector that remembers whether it was closed, so leaked instances are observable. */
    public static final class RecordingConnector implements Connector {
        private boolean closed;

        public boolean isClosed() {
            return closed;
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return null;
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
