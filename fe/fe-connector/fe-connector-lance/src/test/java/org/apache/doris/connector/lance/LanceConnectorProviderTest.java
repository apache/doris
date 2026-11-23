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

import org.apache.doris.connector.spi.ConnectorSecuritySurface;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

class LanceConnectorProviderTest {

    /**
     * 4.x images persist Lance catalogs with type "lance", and the engine routes a replayed catalog to the
     * provider of that name, so the name is a compatibility contract.
     */
    @Test
    void typeIsLance() {
        Assertions.assertEquals("lance", new LanceConnectorProvider().getType());
        Assertions.assertEquals("lance", new LanceConnectorProvider().name());
    }

    /**
     * The engine's SSRF gate probes the REST server at CREATE / ALTER CATALOG, and SHOW CREATE CATALOG masks the
     * REST credentials and every extra header that carries one.
     */
    @Test
    void securitySurfaceDeclaresTheRestServerAndTheSecrets() {
        ConnectorSecuritySurface surface = new LanceConnectorProvider().securitySurface();
        Assertions.assertTrue(surface.ssrfEndpointKeys().contains("lance.rest.uri"));
        Assertions.assertEquals(new HashSet<>(Arrays.asList("lance.rest.bearer-token", "lance.rest.api-key")),
                new HashSet<>(surface.sensitiveKeys()));
        Assertions.assertTrue(surface.jdbcDriverUrlKeys().isEmpty());
        Assertions.assertTrue(masked(surface, "lance.rest.header.X-Auth-Token"));
        Assertions.assertTrue(masked(surface, "lance.rest.header.Proxy-Authorization"));
        Assertions.assertTrue(masked(surface, "lance.rest.header.x-goog-api-key"));
        Assertions.assertTrue(masked(surface, "lance.rest.header.Cookie"));
        Assertions.assertFalse(masked(surface, "lance.rest.header.x-tenant"));
        Assertions.assertFalse(masked(surface, "lance.rest.uri"));
        Assertions.assertFalse(masked(surface, "lance.rest.security.type"));
    }

    private static boolean masked(ConnectorSecuritySurface surface, String key) {
        return surface.sensitiveKeys().contains(key)
                || surface.sensitiveKeyPrefixes().stream().anyMatch(prefix -> prefix.covers(key));
    }

    /** Every endpoint spelling the filesystem binding can select must reach the CREATE / ALTER gate. */
    @Test
    void s3CompatibleEndpointsAreDeclared() {
        Assertions.assertEquals(Arrays.asList(
                "lance.rest.uri", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT", "aws.endpoint",
                "glue.endpoint", "aws.glue.endpoint", "oss.endpoint", "OSS_ENDPOINT",
                "dlf.endpoint", "dlf.catalog.endpoint", "fs.oss.endpoint", "cos.endpoint",
                "COS_ENDPOINT", "obs.endpoint", "OBS_ENDPOINT", "uri"),
                new LanceConnectorProvider().securitySurface().ssrfEndpointKeys());
    }

    @Test
    void validationRejectsWhatTheConnectorWouldRefuse() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceConnectorProvider().validateProperties(properties));
        properties.put("warehouse", "/tmp/lance");
        new LanceConnectorProvider().validateProperties(properties);
        // ALTER validates the merged candidate.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceConnectorProvider().validatePropertiesForUpdate(properties,
                        Collections.singletonMap("lance.rest.uri", "http://127.0.0.1:19102")));
    }
}
