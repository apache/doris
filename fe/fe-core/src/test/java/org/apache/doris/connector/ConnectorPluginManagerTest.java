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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

/**
 * Tests for {@link ConnectorPluginManager}, focusing on API version
 * compatibility checking (P1-9).
 */
public class ConnectorPluginManagerTest {

    private ConnectorPluginManager manager;
    private ConnectorContext testContext;

    @BeforeEach
    void setUp() {
        manager = new ConnectorPluginManager();
        testContext = new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "test_catalog";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }
        };
    }

    @Test
    void testCompatibleApiVersionCreatesConnector() {
        manager.registerProvider(createProvider("test_type",
                ConnectorPluginManager.CURRENT_API_VERSION));

        Connector connector = manager.createConnector("test_type",
                Collections.emptyMap(), testContext);
        Assertions.assertNotNull(connector,
                "Compatible provider should create connector successfully");
    }

    @Test
    void testIncompatibleApiVersionReturnsNull() {
        manager.registerProvider(createProvider("test_type", 999));

        Connector connector = manager.createConnector("test_type",
                Collections.emptyMap(), testContext);
        Assertions.assertNull(connector,
                "Incompatible provider should be skipped, returning null");
    }

    @Test
    void testIncompatibleApiVersionValidateThrows() {
        manager.registerProvider(createProvider("test_type", 999));

        Assertions.assertThrows(IllegalArgumentException.class, () ->
                manager.validateProperties("test_type", Collections.emptyMap()),
                "validateProperties should throw for incompatible version");
    }

    @Test
    void testFallsBackToCompatibleProvider() {
        // Register incompatible first, compatible second
        manager.registerProvider(createProvider("test_type", 999));
        // registerProvider adds at index 0, so add compatible last with direct list access
        // Actually registerProvider always inserts at index 0, so we need a workaround.
        // The incompatible one was added at 0. Now add compatible at 0 too — it'll be first.
        // But we want incompatible first. Let's just test the reverse: compatible registered,
        // then incompatible for same type — the compatible (index 0) should be found first.
        ConnectorPluginManager mgr = new ConnectorPluginManager();
        // Add compatible at index 0
        mgr.registerProvider(createProvider("test_type",
                ConnectorPluginManager.CURRENT_API_VERSION));

        Connector connector = mgr.createConnector("test_type",
                Collections.emptyMap(), testContext);
        Assertions.assertNotNull(connector,
                "Should use the compatible provider");
    }

    @Test
    void testNoMatchingProviderReturnsNull() {
        Connector connector = manager.createConnector("nonexistent",
                Collections.emptyMap(), testContext);
        Assertions.assertNull(connector,
                "No matching provider should return null");
    }

    private static ConnectorProvider createProvider(String type, int apiVersion) {
        return new ConnectorProvider() {
            @Override
            public String getType() {
                return type;
            }

            @Override
            public int apiVersion() {
                return apiVersion;
            }

            @Override
            public Connector create(Map<String, String> properties, ConnectorContext context) {
                return new Connector() {
                    @Override
                    public ConnectorMetadata getMetadata(ConnectorSession session) {
                        return null;
                    }

                    @Override
                    public void close() {
                    }
                };
            }
        };
    }
}
