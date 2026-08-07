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

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Guards the fluss connector's identity and its discovery wiring.
 */
public class FlussConnectorProviderTest {

    private static ConnectorContext context() {
        return new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "fluss_catalog";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }
        };
    }

    @Test
    public void typeIsFluss() {
        // The type name is what CREATE CATALOG routes on and what every statement-scope namespace is
        // prefixed with, so it is API: renaming it breaks existing catalogs.
        Assertions.assertEquals("fluss", new FlussConnectorProvider().getType());
        Assertions.assertTrue(new FlussConnectorProvider().isStandaloneCatalogType());
    }

    @Test
    public void providerIsDiscoverableThroughServiceLoader() {
        // The META-INF/services line is what makes the plugin loadable at all; a typo in it is invisible
        // until FE starts and the catalog type simply does not exist.
        boolean found = false;
        for (ConnectorProvider provider : ServiceLoader.load(ConnectorProvider.class,
                FlussConnectorProviderTest.class.getClassLoader())) {
            if (provider instanceof FlussConnectorProvider) {
                found = true;
            }
        }
        Assertions.assertTrue(found, "FlussConnectorProvider must be registered in META-INF/services");
    }

    @Test
    public void createValidatesPropertiesBeforeHandingBackACatalog() {
        // Validation must also happen on the create path, not only in validateProperties: a catalog
        // restored from the FE image at startup goes straight through create.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new FlussConnectorProvider().create(new HashMap<>(), context()));
    }

    @Test
    public void createBuildsAFlussConnectorWithoutTouchingTheCluster() throws IOException {
        // Constructing a catalog must stay offline — FE creates connectors while replaying its image,
        // where an unreachable cluster must not block startup. The fluss connection is therefore lazy,
        // and closing a connector that never connected must be a no-op rather than an NPE.
        Map<String, String> properties = new HashMap<>();
        properties.put(FlussCatalogProperties.BOOTSTRAP_SERVERS, "localhost:9123");

        Connector connector = new FlussConnectorProvider().create(properties, context());
        Assertions.assertTrue(connector instanceof FlussConnector);
        connector.close();
    }
}
