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

import org.apache.doris.connector.spi.ConnectorSecuritySurface;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 * Pins the Paimon connector's driver_url declaration: the engine's operator gate checks these keys, but
 * only on a jdbc-flavored catalog ({@code paimon.catalog.type = jdbc}); on any other flavor they are dead
 * config the gate must leave alone.
 */
public class PaimonConnectorProviderSecuritySurfaceTest {

    @Test
    public void declaresJdbcDriverUrlKeysGatedByFlavor() {
        ConnectorSecuritySurface surface = new PaimonConnectorProvider().securitySurface();
        Assertions.assertEquals(Arrays.asList("paimon.jdbc.driver_url", "jdbc.driver_url"),
                surface.jdbcDriverUrlKeys());
        Assertions.assertEquals("paimon.catalog.type", surface.jdbcDriverUrlFlavorKey());
        Assertions.assertTrue(surface.ssrfEndpointKeys().isEmpty());
    }

    @Test
    public void declaresTheSecretsOfEveryMetastoreFlavor() {
        // Before these were declared, SHOW CREATE CATALOG printed the paimon jdbc password and the REST DLF
        // key pair in clear: fe-core's own list knows neither spelling.
        ConnectorSecuritySurface surface = new PaimonConnectorProvider().securitySurface();
        for (String key : new String[] {"paimon.jdbc.password", "jdbc.password", "paimon.rest.dlf.access-key-id",
                "paimon.rest.dlf.access-key-secret", "dlf.secret_key", "dlf.catalog.securityToken"}) {
            Assertions.assertTrue(surface.sensitiveKeys().contains(key), key + " missing from "
                    + surface.sensitiveKeys());
        }
        Assertions.assertFalse(surface.sensitiveKeys().contains("hive.metastore.client.keytab"),
                "a keytab is a path, not a secret");
        Assertions.assertFalse(surface.sensitiveKeys().contains("paimon.rest.token.provider"));
    }
}
