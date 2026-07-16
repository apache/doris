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

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests the fe-core override of the cross-plugin sibling-connector seam
 * ({@link DefaultConnectorContext#createSiblingConnector}). The override must build the sibling through the shared
 * {@link ConnectorFactory}/{@link ConnectorPluginManager} (so the sibling loads in the requested type's own plugin
 * classloader) and pass THIS context through unchanged (so the sibling reuses the caller catalog's id/auth/storage).
 *
 * <p>Dormant: no production code calls {@code createSiblingConnector} yet (the hive gateway substep does), so this
 * only exercises the seam in isolation with a recording fake provider registered on the shared manager.
 */
public class DefaultConnectorContextSiblingTest {

    @AfterEach
    void tearDown() {
        // The plugin manager is a process-wide static; reset it so this test does not leak the recording fake
        // provider into any other test that shares the ConnectorFactory singleton.
        ConnectorFactory.clearPluginManager();
    }

    @Test
    void createSiblingConnector_buildsViaFactory_passesPropsAndThisContext() {
        RecordingProvider provider = new RecordingProvider("iceberg");
        ConnectorPluginManager manager = new ConnectorPluginManager();
        manager.registerProvider(provider);
        ConnectorFactory.initPluginManager(manager);

        DefaultConnectorContext ctx = new DefaultConnectorContext("hms_catalog", 42L);
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.catalog.type", "hms");
        props.put("hive.metastore.uris", "thrift://host:9083");

        Connector sibling = ctx.createSiblingConnector("iceberg", props);

        Assertions.assertNotNull(sibling, "sibling must be built when a provider matches the type");
        Assertions.assertSame(provider.lastConnector, sibling,
                "the sibling must be exactly the connector the matching provider produced");
        Assertions.assertSame(props, provider.lastProperties,
                "the caller-synthesized properties must be forwarded to the sibling provider unchanged");
        Assertions.assertSame(ctx, provider.lastContext,
                "the gateway's own context (this) must be passed to the sibling so it shares id/auth/storage");
    }

    @Test
    void createSiblingConnector_returnsNull_whenNoProviderMatches() {
        ConnectorPluginManager manager = new ConnectorPluginManager();
        manager.registerProvider(new RecordingProvider("iceberg"));
        ConnectorFactory.initPluginManager(manager);

        DefaultConnectorContext ctx = new DefaultConnectorContext("hms_catalog", 42L);
        // A gateway asking for a type no registered provider serves: fe-core returns null (connector-agnostic);
        // the gateway caller is the one that fails loud. Here the only provider serves "iceberg", not "paimon".
        Connector sibling = ctx.createSiblingConnector("paimon", new HashMap<>());

        Assertions.assertNull(sibling, "no matching provider must yield null, not an exception");
    }

    @Test
    void createSiblingConnector_returnsNull_whenPluginManagerUninitialized() {
        // No initPluginManager -> ConnectorFactory has no manager -> null (never throws). Mirrors the pre-flip
        // reality where a connector context may exist before/without the plugin manager being wired.
        ConnectorFactory.clearPluginManager();

        DefaultConnectorContext ctx = new DefaultConnectorContext("hms_catalog", 42L);
        Connector sibling = ctx.createSiblingConnector("iceberg", new HashMap<>());

        Assertions.assertNull(sibling, "uninitialized plugin manager must yield null, not an exception");
    }

    /** A ConnectorProvider that records the exact args of its last {@code create} call and the connector it made. */
    private static final class RecordingProvider implements ConnectorProvider {
        private final String type;
        private Map<String, String> lastProperties;
        private ConnectorContext lastContext;
        private Connector lastConnector;

        RecordingProvider(String type) {
            this.type = type;
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public Connector create(Map<String, String> properties, ConnectorContext context) {
            this.lastProperties = properties;
            this.lastContext = context;
            this.lastConnector = new RecordingConnector();
            return lastConnector;
        }
    }

    /** A minimal Connector — every method uses its SPI default; identity is all the test asserts on. */
    private static final class RecordingConnector implements Connector {
        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return null;
        }
    }
}
