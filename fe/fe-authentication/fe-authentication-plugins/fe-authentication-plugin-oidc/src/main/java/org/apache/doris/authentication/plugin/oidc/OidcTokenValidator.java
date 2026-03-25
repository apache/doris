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

import org.apache.doris.authentication.AuthenticationException;
import org.apache.doris.authentication.AuthenticationFailureType;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.RemoteJWKSet;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class OidcTokenValidator {

    private final OidcIntegrationConfig config;
    private final ConfigurableJWTProcessor<SecurityContext> jwtProcessor;

    public OidcTokenValidator(OidcIntegrationConfig config) throws AuthenticationException {
        this(config, createRemoteJwkSource(config));
    }

    OidcTokenValidator(OidcIntegrationConfig config, JWKSource<SecurityContext> jwkSource) {
        this.config = Objects.requireNonNull(config, "config is required");
        this.jwtProcessor = createJwtProcessor(config, jwkSource);
    }

    public OidcIdentity validate(String rawToken, String expectedUsername) throws AuthenticationException {
        SignedJWT signedJwt = parseSignedToken(rawToken);
        validateAlgorithm(signedJwt.getHeader().getAlgorithm());
        JWTClaimsSet claimsSet = processClaims(rawToken);
        validateClaims(claimsSet, expectedUsername);
        return buildIdentity(claimsSet);
    }

    private static JWKSource<SecurityContext> createRemoteJwkSource(OidcIntegrationConfig config)
            throws AuthenticationException {
        try {
            return new RemoteJWKSet<>(new URL(config.getJwksUri()));
        } catch (MalformedURLException e) {
            throw new AuthenticationException(
                    "Invalid oidc.jwks_uri: " + config.getJwksUri(),
                    e,
                    AuthenticationFailureType.MISCONFIGURED
            );
        }
    }

    private static ConfigurableJWTProcessor<SecurityContext> createJwtProcessor(
            OidcIntegrationConfig config,
            JWKSource<SecurityContext> jwkSource
    ) {
        DefaultJWTProcessor<SecurityContext> processor = new DefaultJWTProcessor<>();
        processor.setJWSKeySelector(new JWSVerificationKeySelector<>(config.getAllowedAlgorithms(), jwkSource));
        processor.setJWTClaimsSetVerifier((claimsSet, context) -> {
        });
        return processor;
    }

    private SignedJWT parseSignedToken(String rawToken) throws AuthenticationException {
        if (rawToken == null || rawToken.trim().isEmpty()) {
            throw badCredential("OIDC token is required");
        }
        try {
            return SignedJWT.parse(rawToken);
        } catch (ParseException e) {
            throw badCredential("OIDC token is not a valid signed JWT");
        }
    }

    private void validateAlgorithm(JWSAlgorithm actualAlgorithm) throws AuthenticationException {
        if (actualAlgorithm == null || !config.getAllowedAlgorithms().contains(actualAlgorithm)) {
            throw badCredential("OIDC token uses a disallowed algorithm: " + actualAlgorithm);
        }
    }

    private JWTClaimsSet processClaims(String rawToken) throws AuthenticationException {
        try {
            return jwtProcessor.process(rawToken, null);
        } catch (BadJOSEException e) {
            throw badCredential("OIDC token signature validation failed: " + e.getMessage());
        } catch (JOSEException e) {
            throw new AuthenticationException(
                    "Failed to load or process OIDC JWKS: " + e.getMessage(),
                    e,
                    AuthenticationFailureType.SOURCE_UNAVAILABLE
            );
        } catch (ParseException e) {
            throw badCredential("OIDC token is not a valid signed JWT");
        }
    }

    private void validateClaims(JWTClaimsSet claimsSet, String expectedUsername) throws AuthenticationException {
        if (!config.getIssuer().equals(claimsSet.getIssuer())) {
            throw badCredential("OIDC token issuer does not match expected issuer");
        }

        List<String> audiences = claimsSet.getAudience();
        if (audiences == null || Collections.disjoint(new LinkedHashSet<>(audiences), config.getAllowedAudiences())) {
            throw badCredential("OIDC token audience is not allowed");
        }

        Date expirationTime = claimsSet.getExpirationTime();
        if (expirationTime == null) {
            throw badCredential("OIDC token expiration time is missing");
        }
        if (Instant.now().minusSeconds(config.getClockSkewSeconds()).isAfter(expirationTime.toInstant())) {
            throw badCredential("OIDC token has expired");
        }

        Date notBeforeTime = claimsSet.getNotBeforeTime();
        if (notBeforeTime != null
                && Instant.now().plusSeconds(config.getClockSkewSeconds()).isBefore(notBeforeTime.toInstant())) {
            throw badCredential("OIDC token is not valid yet");
        }

        String username = getRequiredStringClaim(claimsSet, config.getUsernameClaim(), "username");
        if (!Objects.equals(expectedUsername, username)) {
            throw badCredential("Authentication request username does not match OIDC token username");
        }

        getRequiredStringClaim(claimsSet, config.getSubjectClaim(), "subject");
    }

    private OidcIdentity buildIdentity(JWTClaimsSet claimsSet) throws AuthenticationException {
        String username = getRequiredStringClaim(claimsSet, config.getUsernameClaim(), "username");
        String subject = getRequiredStringClaim(claimsSet, config.getSubjectClaim(), "subject");
        Set<String> groups = extractGroups(claimsSet);
        Map<String, String> attributes = extractAttributes(claimsSet);
        return new OidcIdentity(username, subject, groups, attributes);
    }

    private Set<String> extractGroups(JWTClaimsSet claimsSet) throws AuthenticationException {
        Object groupsValue = getClaimValue(claimsSet, config.getGroupsClaim());
        if (groupsValue == null) {
            return Collections.emptySet();
        }
        if (groupsValue instanceof String) {
            String group = ((String) groupsValue).trim();
            return group.isEmpty() ? Collections.emptySet() : Set.of(group);
        }
        if (groupsValue instanceof Collection<?>) {
            return ((Collection<?>) groupsValue).stream()
                    .map(String::valueOf)
                    .map(String::trim)
                    .filter(value -> !value.isEmpty())
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }
        throw badCredential("OIDC groups claim must be a string or an array");
    }

    private Map<String, String> extractAttributes(JWTClaimsSet claimsSet) {
        Map<String, String> attributes = new LinkedHashMap<>();
        for (String claimName : config.getExtraClaims()) {
            Object value = getClaimValue(claimsSet, claimName);
            if (value == null) {
                continue;
            }
            if (value instanceof Collection<?>) {
                String joinedValue = ((Collection<?>) value).stream()
                        .map(String::valueOf)
                        .collect(Collectors.joining(","));
                attributes.put(claimName, joinedValue);
                continue;
            }
            attributes.put(claimName, String.valueOf(value));
        }
        return attributes;
    }

    private String getRequiredStringClaim(JWTClaimsSet claimsSet, String claimName, String logicalName)
            throws AuthenticationException {
        Object value = getClaimValue(claimsSet, claimName);
        if (!(value instanceof String) || ((String) value).trim().isEmpty()) {
            throw badCredential("OIDC token " + logicalName + " claim is missing");
        }
        return ((String) value).trim();
    }

    private Object getClaimValue(JWTClaimsSet claimsSet, String claimName) {
        if ("sub".equals(claimName)) {
            return claimsSet.getSubject();
        }
        if ("iss".equals(claimName)) {
            return claimsSet.getIssuer();
        }
        return claimsSet.getClaim(claimName);
    }

    private AuthenticationException badCredential(String message) {
        return new AuthenticationException(message, AuthenticationFailureType.BAD_CREDENTIAL);
    }
}
