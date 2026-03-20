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

package org.apache.doris.mysql.authenticate.plugin;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authentication.AuthenticationException;
import org.apache.doris.authentication.AuthenticationFailureType;
import org.apache.doris.authentication.AuthenticationIntegration;
import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.authentication.AuthenticationResult;
import org.apache.doris.authentication.CredentialType;
import org.apache.doris.authentication.handler.AuthenticationPluginManager;
import org.apache.doris.authentication.spi.AuthenticationPlugin;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.mysql.authenticate.AuthenticateRequest;
import org.apache.doris.mysql.authenticate.AuthenticateResponse;
import org.apache.doris.mysql.authenticate.Authenticator;
import org.apache.doris.mysql.authenticate.password.ClearPassword;
import org.apache.doris.mysql.authenticate.password.ClearPasswordResolver;
import org.apache.doris.mysql.authenticate.password.NativePassword;
import org.apache.doris.mysql.authenticate.password.NativePasswordResolver;
import org.apache.doris.mysql.authenticate.password.Password;
import org.apache.doris.mysql.authenticate.password.PasswordResolver;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.plugin.PropertiesUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Bridge authenticator that adapts an {@link org.apache.doris.authentication.spi.AuthenticationPluginFactory}
 * to the legacy MySQL {@link Authenticator} contract.
 */
public class AuthenticationPluginAuthenticator implements Authenticator {
    private static final Logger LOG = LogManager.getLogger(AuthenticationPluginAuthenticator.class);
    private static final String CONFIG_INTEGRATION_NAME_PREFIX = "__config_auth_type__:";

    private final AuthenticationIntegration integration;
    private final AuthenticationPlugin plugin;
    private final PasswordResolver passwordResolver;

    public AuthenticationPluginAuthenticator(String pluginType, Properties initProps) throws AuthenticationException {
        this(pluginType, PropertiesUtils.propertiesToMap(initProps), new AuthenticationPluginManager());
    }

    AuthenticationPluginAuthenticator(String pluginType, Map<String, String> initProps,
            AuthenticationPluginManager pluginManager) throws AuthenticationException {
        Objects.requireNonNull(pluginType, "pluginType");
        AuthenticationPluginManager resolvedPluginManager = Objects.requireNonNull(pluginManager, "pluginManager");
        ensurePluginFactoryLoaded(resolvedPluginManager, pluginType);
        integration = AuthenticationIntegration.builder()
                .name(CONFIG_INTEGRATION_NAME_PREFIX + pluginType)
                .type(pluginType)
                .properties(initProps == null ? Collections.emptyMap() : initProps)
                .build();
        plugin = resolvedPluginManager.createPlugin(integration);
        passwordResolver = plugin.requiresClearPassword() ? new ClearPasswordResolver() : new NativePasswordResolver();
    }

    @Override
    public AuthenticateResponse authenticate(AuthenticateRequest request) throws IOException {
        AuthenticationRequest pluginRequest = toPluginRequest(request);
        if (!plugin.supports(pluginRequest)) {
            return AuthenticateResponse.failedResponse;
        }

        AuthenticationResult result;
        try {
            result = plugin.authenticate(pluginRequest, integration);
        } catch (AuthenticationException e) {
            LOG.warn("Authentication plugin '{}' failed for user '{}': {}", integration.getType(),
                    request.getUserName(), e.getMessage(), e);
            return AuthenticateResponse.failedResponse;
        }

        if (result.isContinue()) {
            LOG.warn("Authentication plugin '{}' returned CONTINUE for user '{}', which is not supported",
                    integration.getType(), request.getUserName());
            return AuthenticateResponse.failedResponse;
        }
        if (!result.isSuccess()) {
            if (result.getException() != null) {
                LOG.info("Authentication plugin '{}' rejected user '{}': {}", integration.getType(),
                        request.getUserName(), result.getException().getMessage());
            }
            return AuthenticateResponse.failedResponse;
        }

        return mapSuccessfulAuthentication(request.getUserName(), request.getRemoteIp());
    }

    @Override
    public boolean canDeal(String qualifiedUser) {
        return !Auth.ROOT_USER.equals(qualifiedUser) && !Auth.ADMIN_USER.equals(qualifiedUser);
    }

    @Override
    public PasswordResolver getPasswordResolver() {
        return passwordResolver;
    }

    private AuthenticateResponse mapSuccessfulAuthentication(String qualifiedUser, String remoteIp) {
        List<UserIdentity> userIdentities =
                Env.getCurrentEnv().getAuth().getUserIdentityForExternalAuth(qualifiedUser, remoteIp);
        if (!userIdentities.isEmpty()) {
            return new AuthenticateResponse(true, userIdentities.get(0), false);
        }
        if (!Boolean.parseBoolean(integration.getProperty("enable_jit_user", "false"))) {
            LOG.info("Authentication plugin '{}' authenticated user '{}' but JIT is disabled",
                    integration.getType(), qualifiedUser);
            return AuthenticateResponse.failedResponse;
        }
        UserIdentity tempUserIdentity = UserIdentity.createAnalyzedUserIdentWithIp(qualifiedUser, remoteIp);
        return new AuthenticateResponse(true, tempUserIdentity, true);
    }

    private AuthenticationRequest toPluginRequest(AuthenticateRequest request) {
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
        if (password instanceof ClearPassword) {
            builder.credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                    .credential(((ClearPassword) password).getPassword().getBytes(StandardCharsets.UTF_8));
        } else if (password instanceof NativePassword) {
            NativePassword nativePassword = (NativePassword) password;
            builder.credentialType(CredentialType.MYSQL_NATIVE_PASSWORD)
                    .credential(nativePassword.getRemotePasswd())
                    .property(NativePasswordResolver.MYSQL_RANDOM_STRING_PROPERTY, nativePassword.getRandomString());
        } else {
            throw new IllegalArgumentException("Unsupported password type: "
                    + (password == null ? "null" : password.getClass().getName()));
        }

        return builder.build();
    }

    private void ensurePluginFactoryLoaded(AuthenticationPluginManager pluginManager, String pluginType)
            throws AuthenticationException {
        if (pluginManager.hasFactory(pluginType)) {
            return;
        }
        try {
            Path pluginRoot = Paths.get(Config.authentication_plugins_dir);
            pluginManager.loadAll(Collections.singletonList(pluginRoot), getClass().getClassLoader());
        } catch (AuthenticationException e) {
            throw new AuthenticationException(
                    "Failed to load authentication plugin for type '" + pluginType + "': " + e.getMessage(),
                    e,
                    AuthenticationFailureType.MISCONFIGURED);
        }
        if (!pluginManager.hasFactory(pluginType)) {
            throw new AuthenticationException(
                    "No AuthenticationPluginFactory found for plugin: " + pluginType,
                    AuthenticationFailureType.MISCONFIGURED);
        }
    }
}
