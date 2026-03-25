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

package org.apache.doris.mysql.authenticate.integration;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authentication.AuthenticationException;
import org.apache.doris.authentication.AuthenticationFailureType;
import org.apache.doris.authentication.AuthenticationIntegration;
import org.apache.doris.authentication.AuthenticationIntegrationMeta;
import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.authentication.handler.AuthenticationOutcome;
import org.apache.doris.catalog.Env;
import org.apache.doris.mysql.authenticate.AuthenticateRequest;
import org.apache.doris.mysql.authenticate.AuthenticateResponse;
import org.apache.doris.mysql.authenticate.Authenticator;
import org.apache.doris.mysql.authenticate.password.ClearPassword;
import org.apache.doris.mysql.authenticate.password.ClearPasswordResolver;
import org.apache.doris.mysql.authenticate.password.Password;
import org.apache.doris.mysql.authenticate.password.PasswordResolver;
import org.apache.doris.mysql.privilege.Auth;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Authenticator that executes a configured authentication integration chain.
 */
public class AuthenticationIntegrationAuthenticator implements Authenticator {
    private static final Logger LOG = LogManager.getLogger(AuthenticationIntegrationAuthenticator.class);

    private final PasswordResolver passwordResolver;
    private final String chainConfig;
    private final String chainConfigName;

    public AuthenticationIntegrationAuthenticator(String chainConfig, String chainConfigName) {
        this.chainConfig = chainConfig;
        this.chainConfigName = chainConfigName;
        validateChainConfig(chainConfig, chainConfigName);
        this.passwordResolver = new ClearPasswordResolver();
    }

    @Override
    public AuthenticateResponse authenticate(AuthenticateRequest request) throws IOException {
        AuthenticationRequest integrationRequest = toIntegrationRequest(request);
        if (integrationRequest == null) {
            return AuthenticateResponse.failedResponse;
        }

        AuthenticationOutcome outcome;
        try {
            outcome = Env.getCurrentEnv().getAuthenticationIntegrationRuntime()
                    .authenticate(resolveAuthenticationChain(), integrationRequest);
        } catch (AuthenticationException e) {
            LOG.warn("Authentication integration chain failed for user '{}': {}", request.getUserName(),
                    e.getMessage());
            return AuthenticateResponse.failedResponse;
        }

        if (outcome.isContinue()) {
            LOG.warn("Authentication integration '{}' returned CONTINUE for user '{}', which is not supported",
                    outcome.getIntegration().getName(), request.getUserName());
            return AuthenticateResponse.failedResponse;
        }
        if (!outcome.isSuccess()) {
            if (outcome.getAuthResult().getException() != null) {
                LOG.info("Authentication integration '{}' rejected user '{}': {}",
                        outcome.getIntegration().getName(),
                        request.getUserName(),
                        outcome.getAuthResult().getException().getMessage());
            }
            return AuthenticateResponse.failedResponse;
        }

        return mapSuccessfulAuthentication(request.getUserName(), request.getRemoteIp(), outcome.getIntegration());
    }

    @Override
    public boolean canDeal(String qualifiedUser) {
        return !Auth.ROOT_USER.equals(qualifiedUser) && !Auth.ADMIN_USER.equals(qualifiedUser);
    }

    @Override
    public PasswordResolver getPasswordResolver() {
        return passwordResolver;
    }

    private AuthenticationRequest toIntegrationRequest(AuthenticateRequest request) {
        AuthenticationRequest.Builder builder = AuthenticationRequest.builder()
                .username(request.getUserName())
                .remoteHost(request.getRemoteHost())
                .remotePort(request.getRemotePort())
                .clientType(request.getClientType() == null ? "mysql" : request.getClientType());
        if (!request.getProperties().isEmpty()) {
            builder.properties(request.getProperties());
        }
        if (request.getCredentialType() != null) {
            return builder.credentialType(request.getCredentialType())
                    .credential(request.getCredential())
                    .build();
        }

        // TODO(authentication): drop password fallback once protocol adapters emit
        // generic credentials for all plugin-based authenticators.
        Password password = request.getPassword();
        if (!(password instanceof ClearPassword)) {
            return null;
        }
        ClearPassword clearPassword = (ClearPassword) password;
        return builder.credentialType(org.apache.doris.authentication.CredentialType.CLEAR_TEXT_PASSWORD)
                .credential(clearPassword.getPassword().getBytes(StandardCharsets.UTF_8))
                .build();
    }

    public static List<String> parseAuthenticationChain(String chainConfig) {
        if (Strings.isNullOrEmpty(chainConfig)) {
            return Collections.emptyList();
        }
        return Splitter.on(',')
                .trimResults()
                .omitEmptyStrings()
                .splitToList(chainConfig);
    }

    private AuthenticateResponse mapSuccessfulAuthentication(String qualifiedUser, String remoteIp,
            AuthenticationIntegration integration) {
        List<UserIdentity> userIdentities =
                Env.getCurrentEnv().getAuth().getUserIdentityForExternalAuth(qualifiedUser, remoteIp);
        if (!userIdentities.isEmpty()) {
            return new AuthenticateResponse(true, userIdentities.get(0), false);
        }
        if (!Boolean.parseBoolean(integration.getProperty("enable_jit_user", "false"))) {
            LOG.info("Authentication integration '{}' authenticated user '{}' but JIT is disabled",
                    integration.getName(), qualifiedUser);
            return AuthenticateResponse.failedResponse;
        }
        UserIdentity tempUserIdentity = UserIdentity.createAnalyzedUserIdentWithIp(qualifiedUser, remoteIp);
        return new AuthenticateResponse(true, tempUserIdentity, true);
    }

    private List<AuthenticationIntegrationMeta> resolveAuthenticationChain() throws AuthenticationException {
        List<String> chainNames = parseAuthenticationChain(chainConfig);
        if (chainNames.isEmpty()) {
            throw new AuthenticationException(
                    chainConfigName + " is empty",
                    AuthenticationFailureType.MISCONFIGURED);
        }

        List<AuthenticationIntegrationMeta> chain = new ArrayList<>(chainNames.size());
        for (String integrationName : chainNames) {
            AuthenticationIntegrationMeta meta =
                    Env.getCurrentEnv().getAuthenticationIntegrationMgr().getAuthenticationIntegration(integrationName);
            if (meta == null) {
                throw new AuthenticationException(
                        "Authentication integration does not exist in " + chainConfigName + ": "
                                + integrationName,
                        AuthenticationFailureType.MISCONFIGURED);
            }
            chain.add(meta);
        }
        return chain;
    }

    private static void validateChainConfig(String chainConfig, String chainConfigName) {
        if (parseAuthenticationChain(chainConfig).isEmpty()) {
            throw new IllegalStateException(chainConfigName + " must not be empty");
        }
    }
}
