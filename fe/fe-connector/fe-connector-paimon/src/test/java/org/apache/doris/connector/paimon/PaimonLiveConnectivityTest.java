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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.ConnectorMetadata;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Live Paimon connectivity smoke (warehouse required; <b>user-run</b>).
 *
 * <p>Complements the offline {@link PaimonConnectorMetadataTest}: this one confirms a real
 * {@link org.apache.paimon.catalog.Catalog} built from {@link PaimonConnector} can actually
 * be reached and listed through the production seam. It is <b>skipped</b> unless
 * {@code PAIMON_WAREHOUSE} is set, so it is inert in CI and never hard-codes a warehouse.
 *
 * <pre>
 *   PAIMON_WAREHOUSE=/path/to/warehouse [PAIMON_CATALOG_TYPE=filesystem] \
 *   mvn -pl :fe-connector-paimon test -Dtest=PaimonLiveConnectivityTest
 * </pre>
 */
public class PaimonLiveConnectivityTest {

    @Test
    public void liveMetadataRoundTrip() {
        String warehouse = System.getenv("PAIMON_WAREHOUSE");
        Assumptions.assumeTrue(warehouse != null && !warehouse.isEmpty(),
                "skipped: set PAIMON_WAREHOUSE (and optionally PAIMON_CATALOG_TYPE) to run live");

        String catalogType = System.getenv("PAIMON_CATALOG_TYPE");

        Map<String, String> props = new HashMap<>();
        props.put(PaimonConnectorProperties.WAREHOUSE, warehouse);
        if (catalogType != null && !catalogType.isEmpty()) {
            props.put(PaimonConnectorProperties.PAIMON_CATALOG_TYPE, catalogType);
        }

        // Exercise the full production path: PaimonConnector lazily builds a real Catalog and
        // wires the CatalogBackedPaimonCatalogOps seam into the metadata. One listDatabaseNames
        // round-trip confirms the catalog is reachable end to end.
        try (PaimonConnector connector = new PaimonConnector(props)) {
            ConnectorMetadata metadata = connector.getMetadata(null);
            Assertions.assertNotNull(metadata.listDatabaseNames(null),
                    "a reachable Paimon catalog must return a (possibly empty) database list");
        } catch (Exception e) {
            throw new AssertionError("live Paimon round-trip failed for warehouse " + warehouse, e);
        }
    }
}
