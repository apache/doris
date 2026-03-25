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
import org.apache.doris.foundation.property.ConnectorProperty;
import org.apache.doris.foundation.property.ParamRules;

import com.nimbusds.jose.JWSAlgorithm;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class OidcIntegrationConfig {

    private static final Set<String> SUPPORTED_ALGORITHM_NAMES = Set.of(
            JWSAlgorithm.RS256.getName(),
            JWSAlgorithm.RS384.getName(),
            JWSAlgorithm.RS512.getName(),
            JWSAlgorithm.PS256.getName(),
            JWSAlgorithm.PS384.getName(),
            JWSAlgorithm.PS512.getName(),
            JWSAlgorithm.ES256.getName(),
            JWSAlgorithm.ES384.getName(),
            JWSAlgorithm.ES512.getName()
    );

    @ConnectorProperty(names = {"oidc.issuer"}, description = "Expected issuer")
    private String issuer = "";

    @ConnectorProperty(names = {"oidc.jwks_uri"}, description = "JWKS endpoint")
    private String jwksUri = "";

    @ConnectorProperty(names = {"oidc.allowed_audiences"}, description = "Allowed audiences")
    private String allowedAudiences = "";

    @ConnectorProperty(names = {"oidc.username_claim"}, description = "Username claim")
    private String usernameClaim = "preferred_username";

    @ConnectorProperty(names = {"oidc.subject_claim"}, description = "Subject claim")
    private String subjectClaim = "sub";

    @ConnectorProperty(names = {"oidc.groups_claim"}, description = "Groups claim")
    private String groupsClaim = "groups";

    @ConnectorProperty(names = {"oidc.extra_claims"}, description = "Extra claims allowlist")
    private String extraClaims = "";

    @ConnectorProperty(names = {"oidc.allowed_algorithms"}, description = "Allowed JWS algorithms")
    private String allowedAlgorithms = JWSAlgorithm.RS256.getName();

    @ConnectorProperty(names = {"oidc.clock_skew_seconds"}, description = "Clock skew in seconds")
    private int clockSkewSeconds = 60;

    public static OidcIntegrationConfig fromProperties(Map<String, String> props) {
        OidcIntegrationConfig config = new OidcIntegrationConfig();
        ConnectorPropertiesUtils.bindConnectorProperties(config, props);
        return config;
    }

    public void validate() {
        buildRules().validate();
    }

    public ParamRules buildRules() {
        return new ParamRules()
                .require(issuer, "oidc.issuer is required")
                .require(jwksUri, "oidc.jwks_uri is required")
                .require(allowedAudiences, "oidc.allowed_audiences is required")
                .check(() -> clockSkewSeconds < 0, "oidc.clock_skew_seconds must be non-negative")
                .check(() -> getAllowedAudiences().isEmpty(), "oidc.allowed_audiences must not be empty")
                .check(() -> getAllowedAlgorithms().isEmpty(), "oidc.allowed_algorithms must not be empty");
    }

    public String getIssuer() {
        return issuer;
    }

    public String getJwksUri() {
        return jwksUri;
    }

    public String getUsernameClaim() {
        return usernameClaim;
    }

    public String getSubjectClaim() {
        return subjectClaim;
    }

    public String getGroupsClaim() {
        return groupsClaim;
    }

    public int getClockSkewSeconds() {
        return clockSkewSeconds;
    }

    public Set<String> getAllowedAudiences() {
        return splitToSet(allowedAudiences);
    }

    public Set<String> getExtraClaims() {
        return splitToSet(extraClaims);
    }

    public Set<JWSAlgorithm> getAllowedAlgorithms() {
        Set<String> names = splitToSet(allowedAlgorithms);
        if (names.isEmpty()) {
            return Collections.emptySet();
        }
        return names.stream()
                .map(this::toSupportedAlgorithm)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private JWSAlgorithm toSupportedAlgorithm(String algorithmName) {
        if (!SUPPORTED_ALGORITHM_NAMES.contains(algorithmName)) {
            throw new IllegalArgumentException("Unsupported OIDC algorithm: " + algorithmName);
        }
        return JWSAlgorithm.parse(algorithmName);
    }

    private Set<String> splitToSet(String rawValue) {
        if (rawValue == null || rawValue.trim().isEmpty()) {
            return Collections.emptySet();
        }
        return Arrays.stream(rawValue.split(","))
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }
}
