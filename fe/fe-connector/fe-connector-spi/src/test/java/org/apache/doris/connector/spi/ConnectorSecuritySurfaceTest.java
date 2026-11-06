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

import org.apache.doris.connector.spi.ConnectorSecuritySurface.SensitiveKeyPrefix;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

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

    @Test
    public void withSensitiveKeysTakesTheNamesAConnectorDerivesFromItsAnnotations() {
        // getSensitiveKeys returns a Set; the collection form saves every connector an array copy.
        ConnectorSecuritySurface surface = ConnectorSecuritySurface.ofSensitiveKeys("x.token")
                .withSensitiveKeys(new LinkedHashSet<>(Arrays.asList("x.password", "x.secret")));

        Assertions.assertEquals(Arrays.asList("x.token", "x.password", "x.secret"), surface.sensitiveKeys());
        Assertions.assertEquals(Collections.emptyList(), surface.sensitiveKeyPrefixes());
    }

    @Test
    public void sensitiveKeyPrefixesKeepEveryOtherDeclarationAndAppend() {
        ConnectorSecuritySurface surface = ConnectorSecuritySurface.ofSsrfEndpoints("x.uri")
                .withSensitiveKeys("x.token")
                .withSensitiveKeyPrefix("x.lake.", "key", "secret")
                .withSensitiveKeyPrefix("x.raw.");

        Assertions.assertEquals(Collections.singletonList("x.uri"), surface.ssrfEndpointKeys());
        Assertions.assertEquals(Collections.singletonList("x.token"), surface.sensitiveKeys());
        List<SensitiveKeyPrefix> prefixes = surface.sensitiveKeyPrefixes();
        Assertions.assertEquals(2, prefixes.size());
        Assertions.assertEquals("x.lake.", prefixes.get(0).prefix());
        Assertions.assertEquals(Arrays.asList("key", "secret"), prefixes.get(0).markers());
        Assertions.assertEquals("x.raw.", prefixes.get(1).prefix());
        Assertions.assertEquals(Collections.emptyList(), prefixes.get(1).markers());

        ConnectorSecuritySurface only = ConnectorSecuritySurface.ofSensitiveKeyPrefix("x.", "password");
        Assertions.assertEquals(Collections.emptyList(), only.sensitiveKeys());
        Assertions.assertEquals(Collections.emptyList(), only.ssrfEndpointKeys());
        Assertions.assertEquals("x.", only.sensitiveKeyPrefixes().get(0).prefix());
    }

    @Test
    public void keyPrefixCoversTheCredentialLikeKeysUnderIt() {
        SensitiveKeyPrefix lake = ConnectorSecuritySurface.ofSensitiveKeyPrefix("x.Lake.", "Secret", "token")
                .sensitiveKeyPrefixes().get(0);
        Assertions.assertTrue(lake.covers("x.lake.s3.secret-key"));
        Assertions.assertTrue(lake.covers("X.LAKE.REST.TOKEN"), "prefix and markers match case-insensitively");
        Assertions.assertFalse(lake.covers("x.lake.s3.endpoint"), "a key naming no marker stays visible");
        Assertions.assertFalse(lake.covers("x.lakes.secret"), "only keys under the prefix");
        Assertions.assertFalse(lake.covers("y.x.lake.secret"));

        // Only the rest is searched, so a namespace whose own name holds a marker does not cover all of it.
        SensitiveKeyPrefix tokens = ConnectorSecuritySurface.ofSensitiveKeyPrefix("x.token.", "token")
                .sensitiveKeyPrefixes().get(0);
        Assertions.assertFalse(tokens.covers("x.token.endpoint"));
        Assertions.assertTrue(tokens.covers("x.token.refresh-token"));

        SensitiveKeyPrefix all = ConnectorSecuritySurface.ofSensitiveKeyPrefix("x.raw.").sensitiveKeyPrefixes()
                .get(0);
        Assertions.assertTrue(all.covers("x.raw.endpoint"), "with no marker every key under it is a secret");
        Assertions.assertFalse(all.covers("x.ra"));
    }

    @Test
    public void declaringAPrefixNeverChangesAnExistingSurfaceOrTheCallersArray() {
        String[] markers = {"token"};
        ConnectorSecuritySurface keys = ConnectorSecuritySurface.ofSensitiveKeys("x.token");
        ConnectorSecuritySurface withPrefix = keys.withSensitiveKeyPrefix("x.", markers);
        markers[0] = "changed";

        Assertions.assertEquals(Collections.emptyList(), keys.sensitiveKeyPrefixes());
        Assertions.assertEquals(Collections.emptyList(), ConnectorSecuritySurface.EMPTY.sensitiveKeyPrefixes());
        Assertions.assertEquals(Collections.singletonList("token"),
                withPrefix.sensitiveKeyPrefixes().get(0).markers());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> withPrefix.sensitiveKeyPrefixes().clear());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> withPrefix.sensitiveKeyPrefixes().get(0).markers().add("key"));
    }
}
