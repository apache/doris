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
import org.apache.doris.authentication.AuthenticationIntegration;
import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.authentication.AuthenticationResult;
import org.apache.doris.authentication.BasicPrincipal;
import org.apache.doris.authentication.CredentialType;
import org.apache.doris.authentication.spi.AuthenticationPlugin;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class OidcAuthenticationPlugin implements AuthenticationPlugin {

    public static final String PLUGIN_NAME = "oidc";

    private volatile OidcIntegrationConfig config;
    private volatile OidcTokenValidator validator;
    private volatile String integrationName;

    @Override
    public String name() {
        return PLUGIN_NAME;
    }

    @Override
    public String description() {
        return "OIDC authentication plugin - validates signed OIDC/JWT bearer tokens";
    }

    @Override
    public boolean supports(AuthenticationRequest request) {
        String credentialType = request.getCredentialType();
        return CredentialType.OIDC_ID_TOKEN.equalsIgnoreCase(credentialType)
                || CredentialType.JWT_TOKEN.equalsIgnoreCase(credentialType);
    }

    @Override
    public AuthenticationResult authenticate(AuthenticationRequest request, AuthenticationIntegration integration)
            throws AuthenticationException {
        String username = request.getUsername();
        if (username.isEmpty()) {
            return AuthenticationResult.failure(
                    AuthenticationFailureType.BAD_CREDENTIAL,
                    "Username is required for OIDC authentication");
        }

        String rawToken = extractToken(request);
        if (rawToken == null) {
            return AuthenticationResult.failure(
                    AuthenticationFailureType.BAD_CREDENTIAL,
                    "OIDC token is required");
        }

        try {
            OidcIdentity identity = getOrCreateValidator(integration).validate(rawToken, username);
            return AuthenticationResult.success(toPrincipal(integration, identity));
        } catch (AuthenticationException e) {
            if (e.getFailureType() == AuthenticationFailureType.BAD_CREDENTIAL) {
                return AuthenticationResult.failure(e);
            }
            throw e;
        }
    }

    @Override
    public void validate(AuthenticationIntegration integration) throws AuthenticationException {
        loadAndValidateConfig(integration);
    }

    @Override
    public void initialize(AuthenticationIntegration integration) throws AuthenticationException {
        replaceValidator(integration);
    }

    @Override
    public void reload(AuthenticationIntegration integration) throws AuthenticationException {
        replaceValidator(integration);
    }

    @Override
    public void close() {
        config = null;
        validator = null;
        integrationName = null;
    }

    protected OidcIntegrationConfig loadConfig(AuthenticationIntegration integration) {
        return OidcIntegrationConfig.fromProperties(integration.getProperties());
    }

    protected OidcTokenValidator createValidator(OidcIntegrationConfig config) throws AuthenticationException {
        return new OidcTokenValidator(config);
    }

    private OidcTokenValidator getOrCreateValidator(AuthenticationIntegration integration)
            throws AuthenticationException {
        OidcTokenValidator localValidator = validator;
        if (localValidator != null && Objects.equals(integrationName, integration.getName())) {
            return localValidator;
        }
        synchronized (this) {
            localValidator = validator;
            if (localValidator == null || !Objects.equals(integrationName, integration.getName())) {
                replaceValidator(integration);
                localValidator = validator;
            }
            return localValidator;
        }
    }

    private void replaceValidator(AuthenticationIntegration integration) throws AuthenticationException {
        OidcIntegrationConfig newConfig = loadAndValidateConfig(integration);
        OidcTokenValidator newValidator = createValidator(newConfig);
        config = newConfig;
        validator = newValidator;
        integrationName = integration.getName();
    }

    private OidcIntegrationConfig loadAndValidateConfig(AuthenticationIntegration integration)
            throws AuthenticationException {
        try {
            OidcIntegrationConfig newConfig = loadConfig(integration);
            newConfig.validate();
            return newConfig;
        } catch (RuntimeException e) {
            throw new AuthenticationException(
                    "Invalid OIDC configuration: " + e.getMessage(),
                    e,
                    AuthenticationFailureType.MISCONFIGURED
            );
        }
    }

    private String extractToken(AuthenticationRequest request) {
        byte[] credential = request.getCredential();
        if (credential == null || credential.length == 0) {
            return null;
        }
        String rawToken = new String(credential, StandardCharsets.UTF_8).trim();
        return rawToken.isEmpty() ? null : rawToken;
    }

    private BasicPrincipal toPrincipal(AuthenticationIntegration integration, OidcIdentity identity) {
        return BasicPrincipal.builder()
                .name(identity.getUsername())
                .authenticator(integration.getName())
                .externalPrincipal(identity.getSubject())
                .externalGroups(identity.getGroups())
                .attributes(identity.getAttributes())
                .build();
    }
}
