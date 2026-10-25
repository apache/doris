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

import org.apache.doris.connector.spi.ConnectorSecuritySurface;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 * Pins the HMS connector's SSRF endpoint declaration: the engine's CREATE / ALTER CATALOG gate probes
 * exactly these user-supplied thrift-endpoint property keys (see
 * {@code PluginDrivenExternalCatalog#checkEndpointsAgainstSsrf}).
 */
public class HiveConnectorProviderSecuritySurfaceTest {

    @Test
    public void declaresMetastoreUriAsSsrfEndpoint() {
        ConnectorSecuritySurface surface = new HiveConnectorProvider().securitySurface();
        Assertions.assertEquals(Arrays.asList("hive.metastore.uris", "uri"), surface.ssrfEndpointKeys());
        // An HMS catalog loads no driver jar.
        Assertions.assertTrue(surface.jdbcDriverUrlKeys().isEmpty());
        Assertions.assertNull(surface.jdbcDriverUrlFlavorKey());
    }
}
