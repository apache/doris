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

package org.apache.doris.mysql.authenticate;

import org.apache.doris.authentication.CredentialType;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.util.ClassLoaderUtils;
import org.apache.doris.mysql.MysqlAuthPacket;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.authenticate.integration.AuthenticationIntegrationAuthenticator;
import org.apache.doris.mysql.authenticate.password.ClearPassword;
import org.apache.doris.mysql.authenticate.password.Password;
import org.apache.doris.mysql.authenticate.plugin.AuthenticationPluginAuthenticator;
import org.apache.doris.plugin.PropertiesUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;

/**
 * MySQL authenticator entry that keeps legacy auth-type compatibility while supporting plugin-based authenticators.
 *
 * <p>The compatibility rules are:
 * <ul>
 *     <li>{@code password} remains an alias of {@code default}</li>
 *     <li>built-in and customer legacy {@link AuthenticatorFactory} implementations are resolved first</li>
 *     <li>authentication plugins are used as the fallback when no legacy factory matches</li>
 * </ul>
 */
public class AuthenticatorManager {
    private static final Logger LOG = LogManager.getLogger(AuthenticatorManager.class);

    private static volatile Authenticator defaultAuthenticator = null;
    private static volatile Authenticator authTypeAuthenticator = null;
    private static volatile String authTypeIdentifier = null;

    public AuthenticatorManager(String type) {
        String normalizedType = normalizeAuthTypeIdentifier(type);
        LOG.info("Authenticate type: {}", normalizedType);
        defaultAuthenticator = new DefaultAuthenticator();
        if (authTypeAuthenticator == null || !normalizedType.equalsIgnoreCase(authTypeIdentifier)) {
            synchronized (AuthenticatorManager.class) {
                if (authTypeAuthenticator == null || !normalizedType.equalsIgnoreCase(authTypeIdentifier)) {
                    try {
                        authTypeAuthenticator = loadFactoriesByName(normalizedType);
                        authTypeIdentifier = normalizedType;
                    } catch (Exception e) {
                        if (!AuthenticateType.DEFAULT.name().equalsIgnoreCase(normalizedType)) {
                            throw new IllegalStateException("Failed to load authenticator by name: "
                                    + normalizedType, e);
                        }
                        LOG.warn("Failed to load authenticator by name: {}, using default authenticator",
                                normalizedType, e);
                        authTypeAuthenticator = defaultAuthenticator;
                        authTypeIdentifier = AuthenticateType.DEFAULT.name();
                    }
                }
            }
        }
    }

    /**
     * Normalize auth-type names so old config values keep their historical meaning.
     */
    private String normalizeAuthTypeIdentifier(String type) {
        if (type == null) {
            return AuthenticateType.DEFAULT.name();
        }
        if ("password".equalsIgnoreCase(type) || AuthenticateType.DEFAULT.name().equalsIgnoreCase(type)) {
            return AuthenticateType.DEFAULT.name();
        }
        if (AuthenticateType.LDAP.name().equalsIgnoreCase(type)) {
            return AuthenticateType.LDAP.name();
        }
        return type.toLowerCase();
    }


    /**
     * Preserve the old resolution order: legacy factories first, plugin authenticator second.
     */
    private Authenticator loadFactoriesByName(String identifier) throws Exception {
        Authenticator authenticator = loadLegacyFactoryByName(identifier);
        if (authenticator != null) {
            return authenticator;
        }
        return loadPluginFactoryByName(identifier);
    }

    private Authenticator loadLegacyFactoryByName(String identifier) throws Exception {
        ServiceLoader<AuthenticatorFactory> loader = ServiceLoader.load(AuthenticatorFactory.class);
        for (AuthenticatorFactory factory : loader) {
            LOG.info("Found Authenticator Plugin Factory: {}", factory.factoryIdentifier());
            if (factory.factoryIdentifier().equalsIgnoreCase(identifier)) {
                Properties properties = PropertiesUtils.loadAuthenticationConfigFile();
                return factory.create(properties);
            }
        }
        return loadCustomerFactories(identifier);
    }

    private Authenticator loadCustomerFactories(String identifier) throws Exception {
        List<AuthenticatorFactory> factories = ClassLoaderUtils.loadServicesFromDirectory(AuthenticatorFactory.class);
        if (factories.isEmpty()) {
            return null;
        }
        for (AuthenticatorFactory factory : factories) {
            LOG.info("Found Customer Authenticator Plugin Factory: {}", factory.factoryIdentifier());
            if (factory.factoryIdentifier().equalsIgnoreCase(identifier)) {
                Properties properties = PropertiesUtils.loadAuthenticationConfigFile();
                return factory.create(properties);
            }
        }
        return null;
    }

    private Authenticator loadPluginFactoryByName(String identifier) throws Exception {
        Properties properties = PropertiesUtils.loadAuthenticationConfigFile();
        return new AuthenticationPluginAuthenticator(identifier, properties);
    }

    public boolean authenticate(ConnectContext context,
                                String userName,
                                MysqlChannel channel,
                                MysqlSerializer serializer,
                                MysqlAuthPacket authPacket,
                                MysqlHandshakePacket handshakePacket) throws IOException {
        String remoteIp = context.getMysqlChannel().getRemoteIp();
        Authenticator primaryAuthenticator = chooseAuthenticator(userName, remoteIp);
        Optional<AuthenticateRequest> primaryRequest = resolveAuthenticateRequest(primaryAuthenticator, userName,
                context, channel, serializer, authPacket, handshakePacket);
        if (!primaryRequest.isPresent()) {
            return false;
        }

        AuthenticateRequest request = primaryRequest.get();
        remoteIp = request.getRemoteIp();
        AuthenticateResponse primaryResponse = authenticateWith(primaryAuthenticator, request);
        if (primaryResponse.isSuccess()) {
            return finishSuccessfulAuthentication(context, remoteIp, primaryResponse, false);
        }

        AuthenticateResponse chainResponse = tryAuthenticationChainFallback(context, userName, remoteIp,
                channel, serializer, authPacket, handshakePacket, request);
        if (chainResponse != null && chainResponse.isSuccess()) {
            return finishSuccessfulAuthentication(context, remoteIp, chainResponse, true);
        }

        return reportAuthenticationFailure(context, userName, remoteIp, request.getPassword());
    }

    Authenticator chooseAuthenticator(String userName, String remoteIp) {
        Authenticator primaryAuthenticator = authTypeAuthenticator;
        return primaryAuthenticator.canDeal(userName) ? primaryAuthenticator : defaultAuthenticator;
    }

    Authenticator getAuthenticationChainAuthenticator() {
        return new AuthenticationIntegrationAuthenticator(Config.authentication_chain, "authentication_chain");
    }

    private void applyAuthenticateResponse(ConnectContext context, String remoteIp, AuthenticateResponse response) {
        context.setCurrentUserIdentity(response.getUserIdentity());
        context.setRemoteIP(remoteIp);
        context.setIsTempUser(response.isTemp());
    }

    private Optional<AuthenticateRequest> resolveAuthenticateRequest(Authenticator authenticator,
            String userName,
            ConnectContext context,
            MysqlChannel channel,
            MysqlSerializer serializer,
            MysqlAuthPacket authPacket,
            MysqlHandshakePacket handshakePacket) throws IOException {
        return authenticator.getPasswordResolver().resolveAuthenticateRequest(userName, context, channel, serializer,
                authPacket, handshakePacket);
    }

    private AuthenticateResponse authenticateWith(Authenticator authenticator,
            AuthenticateRequest request) throws IOException {
        return authenticator.authenticate(request);
    }

    private boolean finishSuccessfulAuthentication(ConnectContext context, String remoteIp,
            AuthenticateResponse response, boolean setOkState) {
        if (setOkState) {
            context.getState().setOk();
        }
        applyAuthenticateResponse(context, remoteIp, response);
        return true;
    }

    private AuthenticateResponse tryAuthenticationChainFallback(ConnectContext context,
            String userName,
            String remoteIp,
            MysqlChannel channel,
            MysqlSerializer serializer,
            MysqlAuthPacket authPacket,
            MysqlHandshakePacket handshakePacket,
            AuthenticateRequest primaryRequest) throws IOException {
        if (!hasAuthenticationChain()) {
            return null;
        }

        Authenticator chainAuthenticator;
        try {
            chainAuthenticator = getAuthenticationChainAuthenticator();
        } catch (RuntimeException e) {
            LOG.warn("Failed to initialize authentication_chain fallback authenticator: {}", e.getMessage(), e);
            return null;
        }
        if (!chainAuthenticator.canDeal(userName)) {
            return null;
        }

        AuthenticateRequest chainRequest = primaryRequest;
        if (!canReuseRequestForAuthenticationChain(chainRequest)) {
            Optional<AuthenticateRequest> fallbackRequest = resolveAuthenticateRequest(chainAuthenticator, userName,
                    context, channel, serializer, authPacket, handshakePacket);
            if (!fallbackRequest.isPresent()) {
                return null;
            }
            chainRequest = fallbackRequest.get();
        }

        LOG.info("Try authentication_chain fallback for user '{}'", userName);
        return authenticateWith(chainAuthenticator, chainRequest);
    }

    private boolean hasAuthenticationChain() {
        return !AuthenticationIntegrationAuthenticator.parseAuthenticationChain(Config.authentication_chain).isEmpty();
    }

    private boolean canReuseRequestForAuthenticationChain(AuthenticateRequest request) {
        if (CredentialType.CLEAR_TEXT_PASSWORD.equals(request.getCredentialType())) {
            return true;
        }
        return request.getPassword() instanceof ClearPassword;
    }

    private boolean reportAuthenticationFailure(ConnectContext context, String userName, String remoteIp,
            Password password) throws IOException {
        ensureAuthenticationErrorReported(context, userName, remoteIp, password);
        if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
            MysqlProto.sendResponsePacket(context);
        }
        return false;
    }

    private void ensureAuthenticationErrorReported(ConnectContext context, String userName, String remoteIp,
            Password password) {
        if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
            return;
        }
        context.getState().setError(ErrorCode.ERR_ACCESS_DENIED_ERROR,
                ErrorCode.ERR_ACCESS_DENIED_ERROR.formatErrorMsg(userName, remoteIp,
                        hasPassword(password) ? "YES" : "NO"));
    }

    private boolean hasPassword(Password password) {
        if (password == null) {
            return false;
        }
        if (password instanceof ClearPassword) {
            return !Strings.isNullOrEmpty(((ClearPassword) password).getPassword());
        }
        return true;
    }
}
