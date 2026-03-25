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

package org.apache.doris.authentication.plugin.oidc;

import org.apache.doris.foundation.property.ConnectorPropertiesUtils;

import com.nimbusds.jose.JWSAlgorithm;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@DisplayName("OIDC Integration Config Tests")
class OidcIntegrationConfigTest {

    @Test
    @DisplayName("UT-OIDC-CONFIG-001: validate rejects missing issuer")
    void testValidateRejectsMissingIssuer() {
        Map<String, String> props = createMinimalProperties();
        props.remove("oidc.issuer");

        OidcIntegrationConfig config = OidcIntegrationConfig.fromProperties(props);

        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, config::validate);
        Assertions.assertTrue(exception.getMessage().contains("oidc.issuer"));
    }

    @Test
    @DisplayName("UT-OIDC-CONFIG-002: validate rejects missing jwks uri")
    void testValidateRejectsMissingJwksUri() {
        Map<String, String> props = createMinimalProperties();
        props.remove("oidc.jwks_uri");

        OidcIntegrationConfig config = OidcIntegrationConfig.fromProperties(props);

        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, config::validate);
        Assertions.assertTrue(exception.getMessage().contains("oidc.jwks_uri"));
    }

    @Test
    @DisplayName("UT-OIDC-CONFIG-003: validate rejects missing allowed audiences")
    void testValidateRejectsMissingAllowedAudiences() {
        Map<String, String> props = createMinimalProperties();
        props.remove("oidc.allowed_audiences");

        OidcIntegrationConfig config = OidcIntegrationConfig.fromProperties(props);

        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, config::validate);
        Assertions.assertTrue(exception.getMessage().contains("oidc.allowed_audiences"));
    }

    @Test
    @DisplayName("UT-OIDC-CONFIG-004: defaults and parsed sets are applied")
    void testDefaultsAndParsedSets() {
        Map<String, String> props = createMinimalProperties();
        props.put("oidc.allowed_audiences", "doris, grafana ,doris");
        props.put("oidc.extra_claims", "email, tenant, email");

        OidcIntegrationConfig config = OidcIntegrationConfig.fromProperties(props);

        Assertions.assertDoesNotThrow(config::validate);
        Assertions.assertEquals("preferred_username", config.getUsernameClaim());
        Assertions.assertEquals("sub", config.getSubjectClaim());
        Assertions.assertEquals("groups", config.getGroupsClaim());
        Assertions.assertEquals(60, config.getClockSkewSeconds());
        Assertions.assertEquals(Set.of("doris", "grafana"), config.getAllowedAudiences());
        Assertions.assertEquals(Set.of("email", "tenant"), config.getExtraClaims());
        Assertions.assertEquals(Set.of(JWSAlgorithm.RS256), config.getAllowedAlgorithms());
    }

    @Test
    @DisplayName("UT-OIDC-CONFIG-005: validate rejects negative clock skew")
    void testValidateRejectsNegativeClockSkew() {
        Map<String, String> props = createMinimalProperties();
        props.put("oidc.clock_skew_seconds", "-1");

        OidcIntegrationConfig config = OidcIntegrationConfig.fromProperties(props);

        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, config::validate);
        Assertions.assertTrue(exception.getMessage().contains("clock_skew_seconds"));
    }

    @Test
    @DisplayName("UT-OIDC-CONFIG-006: all sensitive keys must use secret prefix")
    void testSensitiveKeysMustUseSecretPrefix() {
        Set<String> sensitiveKeys = ConnectorPropertiesUtils.getSensitiveKeys(OidcIntegrationConfig.class);

        Assertions.assertTrue(sensitiveKeys.stream().allMatch(key -> key.startsWith("secret.")));
    }

    private Map<String, String> createMinimalProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("oidc.issuer", "https://issuer.example.com");
        props.put("oidc.jwks_uri", "https://issuer.example.com/keys");
        props.put("oidc.allowed_audiences", "doris");
        return props;
    }
}
