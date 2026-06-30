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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * CREATE-CATALOG property validation through the production entry point
 * {@link IcebergConnectorProvider#validateProperties(Map)} (called by fe-core
 * {@code PluginDrivenExternalCatalog.checkProperties}). Exercises the full path
 * resolveFlavor → {@code MetaStoreProviders.bindForType(flavor)} → {@code validate()} on the iceberg
 * connector's own classpath (only the iceberg metastore providers are discoverable here). The per-flavor
 * verbatim messages/fire-order are pinned in the metastore-iceberg module's tests; this pins the wiring.
 */
public class IcebergConnectorValidatePropertiesTest {

    private static final IcebergConnectorProvider PROVIDER = new IcebergConnectorProvider();

    private static Map<String, String> props(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static String rejectMessage(Map<String, String> props) {
        return Assertions.assertThrows(IllegalArgumentException.class,
                () -> PROVIDER.validateProperties(props)).getMessage();
    }

    @Test
    public void restFlavorRulesReachableThroughProvider() {
        Assertions.assertEquals("Invalid security type: bogus. Supported values are: none, oauth2",
                rejectMessage(props("iceberg.catalog.type", "rest", "iceberg.rest.security.type", "bogus")));
        // valid REST (default none security) accepted.
        PROVIDER.validateProperties(props("iceberg.catalog.type", "rest", "iceberg.rest.uri", "http://r"));
    }

    @Test
    public void glueFlavorRulesReachableThroughProvider() {
        Assertions.assertEquals("At least one of glue.access_key or glue.role_arn must be set",
                rejectMessage(props("iceberg.catalog.type", "glue",
                        "glue.endpoint", "https://glue.us-east-1.amazonaws.com")));
    }

    @Test
    public void jdbcFlavorRulesReachableThroughProvider() {
        Assertions.assertEquals("Property uri is required.",
                rejectMessage(props("iceberg.catalog.type", "jdbc")));
    }

    @Test
    public void hmsAcceptedWithoutWarehouse() {
        // iceberg HMS does not require warehouse (unlike paimon); a bare uri is accepted through the provider.
        PROVIDER.validateProperties(props("iceberg.catalog.type", "hms", "hive.metastore.uris", "thrift://h"));
    }

    @Test
    public void hadoopAndS3TablesAcceptedAsNoOp() {
        PROVIDER.validateProperties(props("iceberg.catalog.type", "hadoop", "warehouse", "s3://b/wh"));
        PROVIDER.validateProperties(props("iceberg.catalog.type", "s3tables", "warehouse", "arn:aws:s3tables:::bucket"));
    }

    @Test
    public void hadoopRejectedWithoutWarehouse() {
        // M-1/L-1: restore the legacy IcebergHadoopExternalCatalog warehouse-required check at CREATE, with
        // the verbatim message. MUTATION: neuter the isEmpty(warehouse) check -> accepted -> red.
        Assertions.assertEquals(
                "Cannot initialize Iceberg HadoopCatalog because 'warehouse' must not be null or empty",
                rejectMessage(props("iceberg.catalog.type", "hadoop")));
    }

    @Test
    public void s3TablesAcceptedWithoutWarehouse() {
        // s3tables shares the no-op metastore class, but the warehouse gate is HADOOP-only, so a missing
        // warehouse is still accepted. MUTATION: drop the "HADOOP".equals(providerName) gate -> s3tables
        // starts throwing here -> red.
        PROVIDER.validateProperties(props("iceberg.catalog.type", "s3tables"));
    }

    @Test
    public void unknownFlavorRejected() {
        Assertions.assertTrue(rejectMessage(props("iceberg.catalog.type", "nessie"))
                .startsWith("No MetaStoreProvider supports"));
    }

    @Test
    public void missingCatalogTypeRejected() {
        // resolveFlavor returns null for a missing iceberg.catalog.type; bindForType(null) fails loudly
        // (no iceberg provider claims null), parity with the connector's createCatalog "Missing" guard.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PROVIDER.validateProperties(props("warehouse", "s3://b/wh")));
    }
}
