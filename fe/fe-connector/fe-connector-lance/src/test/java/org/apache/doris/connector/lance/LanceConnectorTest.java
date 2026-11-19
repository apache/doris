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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.ConnectorCapability;
import org.apache.doris.connector.spi.ConnectorStorageContext;
import org.apache.doris.connector.spi.DorisConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/** The connector's lifecycle, without reaching storage or loading the native library. */
class LanceConnectorTest {

    private static Map<String, String> filesystem() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put("warehouse", "/tmp/lance");
        return properties;
    }

    private static Map<String, String> rest() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put("lance.catalog.type", "rest");
        properties.put("lance.rest.uri", "http://127.0.0.1:19102");
        return properties;
    }

    /** The MVCC pin is what keeps every step of a scan on one version, and 4.x images replay into it. */
    @Test
    void declaresStatementSnapshots() {
        LanceConnector connector = new LanceConnector(filesystem(), new LanceTestContext());
        Assertions.assertEquals(EnumSet.of(ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT), connector.getCapabilities());
        connector.close();
    }

    /** An FE replaying the edit log builds every catalog: building one must not reach storage or lance.conf. */
    @Test
    void buildingTheConnectorReadsNothing() {
        LanceTestContext unreachable = new LanceTestContext() {
            @Override
            public ConnectorStorageContext getStorageContext() {
                throw new AssertionError("building the connector must not bind storage properties");
            }

            @Override
            public Map<String, String> getConnectorConfig() {
                throw new AssertionError("building the connector must not read lance.conf");
            }
        };
        new LanceConnector(filesystem(), unreachable).close();
    }

    /**
     * A REST catalog from an earlier version still builds, so it replays and can be dropped or altered;
     * CREATE refuses a new one and reading through one says why.
     */
    @Test
    void restCatalogBuildsButCannotBeCreatedOrRead() {
        LanceConnector connector = new LanceConnector(rest(), new LanceTestContext());
        String expected = "Lance rest catalogs are not supported yet; only 'lance.catalog.type'='filesystem'"
                + " catalogs can be read";
        Assertions.assertEquals(expected, Assertions.assertThrows(DorisConnectorException.class,
                () -> connector.preCreateValidation(null)).getMessage());
        LanceTestSession session = new LanceTestSession();
        Assertions.assertEquals(expected, Assertions.assertThrows(DorisConnectorException.class,
                () -> connector.getMetadata(session).listDatabaseNames(session)).getMessage());
        Assertions.assertFalse(connector.testConnection(session).isSuccess());
        connector.close();
    }

    @Test
    void closedConnectorRefusesNewWork() {
        LanceConnector connector = new LanceConnector(filesystem(), new LanceTestContext());
        connector.close();
        LanceTestSession session = new LanceTestSession();
        Assertions.assertEquals("Lance catalog resources have been closed", Assertions.assertThrows(
                DorisConnectorException.class, () -> connector.getMetadata(session).listDatabaseNames(session))
                .getMessage());
        // Closing twice is harmless.
        connector.close();
    }

    @Test
    void filesystemCatalogPassesCreateValidation() {
        LanceConnector connector = new LanceConnector(filesystem(), new LanceTestContext());
        connector.preCreateValidation(null);
        Assertions.assertTrue(Collections.unmodifiableSet(connector.getCapabilities()).size() == 1);
        connector.close();
    }
}
