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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Live Iceberg connectivity smoke (<b>user-run</b>), mirroring {@code PaimonLiveConnectivityTest}.
 *
 * <p>Complements the offline {@link IcebergConnectorMetadataTest}: this one confirms a real
 * {@link org.apache.iceberg.catalog.Catalog} built from {@link IcebergConnector} can actually be reached
 * and listed through the production seam. It is <b>skipped</b> unless {@code ICEBERG_REST_URI} is set, so
 * it is inert in CI and never hard-codes an endpoint.
 *
 * <pre>
 *   ICEBERG_REST_URI=http://host:8181 [ICEBERG_WAREHOUSE=s3://bucket/wh] \
 *   mvn -pl :fe-connector-iceberg test -Dtest=IcebergLiveConnectivityTest
 * </pre>
 */
public class IcebergLiveConnectivityTest {

    /** Minimal context: simple auth (default executeAuthenticated) and an empty environment. */
    private static ConnectorContext testContext() {
        return new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "iceberg_live";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }

            @Override
            public Map<String, String> getEnvironment() {
                return Collections.emptyMap();
            }
        };
    }

    @Test
    public void liveMetadataRoundTrip() {
        String restUri = System.getenv("ICEBERG_REST_URI");
        Assumptions.assumeTrue(restUri != null && !restUri.isEmpty(),
                "skipped: set ICEBERG_REST_URI (and optionally ICEBERG_WAREHOUSE) to run live");

        String warehouse = System.getenv("ICEBERG_WAREHOUSE");

        Map<String, String> props = new HashMap<>();
        props.put(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE, IcebergConnectorProperties.TYPE_REST);
        props.put("iceberg.rest.uri", restUri);
        if (warehouse != null && !warehouse.isEmpty()) {
            props.put("warehouse", warehouse);
        }

        // Exercise the full production path: IcebergConnector lazily builds a real Catalog and wires the
        // CatalogBackedIcebergCatalogOps seam into the metadata. One listDatabaseNames round-trip confirms
        // the catalog is reachable end to end.
        try (IcebergConnector connector = new IcebergConnector(props, testContext())) {
            ConnectorMetadata metadata = connector.getMetadata(null);
            Assertions.assertNotNull(metadata.listDatabaseNames(null),
                    "a reachable Iceberg REST catalog must return a (possibly empty) database list");
        } catch (Exception e) {
            throw new AssertionError("live Iceberg round-trip failed for REST uri " + restUri, e);
        }
    }
}
