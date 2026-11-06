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

package org.apache.doris.connector.spi;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

public class ConnectorSecuritySurfaceTest {

    @Test
    public void gateOnlySurfacesDeclareNoSecrets() {
        Assertions.assertEquals(Collections.emptyList(), ConnectorSecuritySurface.EMPTY.sensitiveKeys());
        Assertions.assertEquals(Collections.emptyList(),
                ConnectorSecuritySurface.ofSsrfEndpoints("x.uri").sensitiveKeys());
        Assertions.assertEquals(Collections.emptyList(),
                ConnectorSecuritySurface.ofJdbcDriverUrls(null, "x.driver_url").sensitiveKeys());
    }

    @Test
    public void ofSensitiveKeysDeclaresOnlySecrets() {
        ConnectorSecuritySurface surface = ConnectorSecuritySurface.ofSensitiveKeys("x.token", "x.api-key");

        Assertions.assertEquals(Arrays.asList("x.token", "x.api-key"), surface.sensitiveKeys());
        Assertions.assertEquals(Collections.emptyList(), surface.ssrfEndpointKeys());
        Assertions.assertEquals(Collections.emptyList(), surface.jdbcDriverUrlKeys());
        Assertions.assertNull(surface.jdbcDriverUrlFlavorKey());
    }

    @Test
    public void withSensitiveKeysKeepsTheGateDeclarationsAndAppends() {
        // A connector with a REST endpoint and a token needs both declarations on one surface.
        ConnectorSecuritySurface ssrf = ConnectorSecuritySurface.ofSsrfEndpoints("x.uri");
        ConnectorSecuritySurface surface = ssrf.withSensitiveKeys("x.token").withSensitiveKeys("x.api-key");

        Assertions.assertEquals(Collections.singletonList("x.uri"), surface.ssrfEndpointKeys());
        Assertions.assertEquals(Arrays.asList("x.token", "x.api-key"), surface.sensitiveKeys());

        ConnectorSecuritySurface jdbc = ConnectorSecuritySurface.ofJdbcDriverUrls("x.type", "x.driver_url")
                .withSensitiveKeys("x.password");
        Assertions.assertEquals("x.type", jdbc.jdbcDriverUrlFlavorKey());
        Assertions.assertEquals(Collections.singletonList("x.driver_url"), jdbc.jdbcDriverUrlKeys());
        Assertions.assertEquals(Collections.singletonList("x.password"), jdbc.sensitiveKeys());
    }

    @Test
    public void declaringSecretsNeverChangesAnExistingSurface() {
        // EMPTY is shared by every connector that declares nothing; a mutable surface would leak one
        // connector's declaration into all of them.
        ConnectorSecuritySurface ssrf = ConnectorSecuritySurface.ofSsrfEndpoints("x.uri");
        ConnectorSecuritySurface withSecret = ssrf.withSensitiveKeys("x.token");
        ConnectorSecuritySurface.ofSensitiveKeys("x.api-key");

        Assertions.assertEquals(Collections.emptyList(), ssrf.sensitiveKeys());
        Assertions.assertEquals(Collections.emptyList(), ConnectorSecuritySurface.EMPTY.sensitiveKeys());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> withSecret.sensitiveKeys().add("x.other"));
    }
}
