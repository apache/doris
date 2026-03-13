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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.util.ClassLoaderUtils;
import org.apache.doris.mysql.MysqlAuthPacket;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.authenticate.integration.AuthenticationIntegrationAuthenticator;
import org.apache.doris.mysql.authenticate.ldap.LdapAuthenticator;
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
        Optional<Password> password = primaryAuthenticator.getPasswordResolver()
                .resolvePassword(context, channel, serializer, authPacket, handshakePacket);
        if (!password.isPresent()) {
            return false;
        }
        AuthenticateResponse primaryResponse =
                primaryAuthenticator.authenticate(new AuthenticateRequest(userName, password.get(), remoteIp));
        if (primaryResponse.isSuccess()) {
            applyAuthenticateResponse(context, remoteIp, primaryResponse);
            return true;
        }

        Optional<AuthenticateResponse> jitChainResponse = tryJitUserAuthenticationChainFallback(context, userName,
                remoteIp, channel, serializer, authPacket, handshakePacket, password.get());
        if (jitChainResponse.isPresent()) {
            AuthenticateResponse response = jitChainResponse.get();
            if (response.isSuccess()) {
                context.getState().setOk();
                applyAuthenticateResponse(context, remoteIp, response);
                return true;
            }
            ensureAuthenticationErrorReported(context, userName, remoteIp, password.get());
            if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                MysqlProto.sendResponsePacket(context);
            }
            return false;
        }

        AuthenticateResponse chainResponse = tryAuthenticationChainFallback(context, userName, remoteIp,
                channel, serializer, authPacket, handshakePacket, primaryAuthenticator, password.get());
        if (chainResponse != null && chainResponse.isSuccess()) {
            context.getState().setOk();
            applyAuthenticateResponse(context, remoteIp, chainResponse);
            return true;
        }

        ensureAuthenticationErrorReported(context, userName, remoteIp, password.get());
        if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
            MysqlProto.sendResponsePacket(context);
        }
        return false;
    }

    Authenticator chooseAuthenticator(String userName, String remoteIp) {
        return authTypeAuthenticator.canDeal(userName) ? authTypeAuthenticator : defaultAuthenticator;
    }

    Authenticator getAuthenticationChainAuthenticator() {
        return new AuthenticationIntegrationAuthenticator(Config.authentication_chain, "authentication_chain");
    }

    Optional<AuthenticateResponse> tryJitUserAuthenticationChainFallback(ConnectContext context,
            String userName,
            String remoteIp,
            MysqlChannel channel,
            MysqlSerializer serializer,
            MysqlAuthPacket authPacket,
            MysqlHandshakePacket handshakePacket,
            Password primaryPassword) throws IOException {
        if (!shouldTryJitUserAuthenticationChain(userName, remoteIp)) {
            return Optional.empty();
        }

        Authenticator chainAuthenticator;
        try {
            chainAuthenticator = getAuthenticationChainAuthenticator();
        } catch (RuntimeException e) {
            LOG.warn("Failed to initialize JIT authentication_chain authenticator: {}", e.getMessage(), e);
            return Optional.of(AuthenticateResponse.failedResponse);
        }
        if (!chainAuthenticator.canDeal(userName)) {
            return Optional.of(AuthenticateResponse.failedResponse);
        }

        Password chainPassword = primaryPassword;
        if (!(chainPassword instanceof ClearPassword)) {
            Optional<Password> fallbackPassword = chainAuthenticator.getPasswordResolver()
                    .resolvePassword(context, channel, serializer, authPacket, handshakePacket);
            if (!fallbackPassword.isPresent()) {
                return Optional.of(AuthenticateResponse.failedResponse);
            }
            chainPassword = fallbackPassword.get();
        }

        LOG.info("Try JIT authentication_chain fallback for user '{}'", userName);
        return Optional.of(chainAuthenticator.authenticate(new AuthenticateRequest(userName, chainPassword, remoteIp)));
    }

    private void applyAuthenticateResponse(ConnectContext context, String remoteIp, AuthenticateResponse response) {
        context.setCurrentUserIdentity(response.getUserIdentity());
        context.setRemoteIP(remoteIp);
        context.setIsTempUser(response.isTemp());
    }

    private AuthenticateResponse tryAuthenticationChainFallback(ConnectContext context,
            String userName,
            String remoteIp,
            MysqlChannel channel,
            MysqlSerializer serializer,
            MysqlAuthPacket authPacket,
            MysqlHandshakePacket handshakePacket,
            Authenticator primaryAuthenticator,
            Password primaryPassword) throws IOException {
        if (!shouldTryAuthenticationChain(primaryAuthenticator, userName, remoteIp)) {
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

        Password chainPassword = primaryPassword;
        if (!(chainPassword instanceof ClearPassword)) {
            Optional<Password> fallbackPassword = chainAuthenticator.getPasswordResolver()
                    .resolvePassword(context, channel, serializer, authPacket, handshakePacket);
            if (!fallbackPassword.isPresent()) {
                return null;
            }
            chainPassword = fallbackPassword.get();
        }

        LOG.info("Try authentication_chain fallback for user '{}' with policy '{}'",
                userName, Config.authentication_chain_fallback_policy);
        return chainAuthenticator.authenticate(new AuthenticateRequest(userName, chainPassword, remoteIp));
    }

    private boolean shouldTryJitUserAuthenticationChain(String userName, String remoteIp) {
        if (!Config.enable_jit_user_authentication_chain) {
            return false;
        }
        if (AuthenticationIntegrationAuthenticator.parseAuthenticationChain(Config.authentication_chain).isEmpty()) {
            return false;
        }
        return !Env.getCurrentEnv().getAuth().doesUserExist(userName, remoteIp);
    }

    private boolean shouldTryAuthenticationChain(Authenticator primaryAuthenticator, String userName, String remoteIp) {
        if (!Config.enable_authentication_chain) {
            return false;
        }
        if (AuthenticationIntegrationAuthenticator.parseAuthenticationChain(Config.authentication_chain).isEmpty()) {
            return false;
        }

        AuthenticationChainFallbackPolicy policy =
                AuthenticationChainFallbackPolicy.fromConfig(Config.authentication_chain_fallback_policy);
        switch (policy) {
            case ANY_FAILURE:
                return true;
            case USER_NOT_FOUND:
                return isPrimaryUserNotFound(primaryAuthenticator, userName, remoteIp);
            case DISABLED:
            default:
                return false;
        }
    }

    private boolean isPrimaryUserNotFound(Authenticator primaryAuthenticator, String userName, String remoteIp) {
        if (primaryAuthenticator instanceof LdapAuthenticator
                || AuthenticateType.LDAP.name().equalsIgnoreCase(authTypeIdentifier)) {
            return !Env.getCurrentEnv().getAuth().getLdapManager().doesUserExist(userName);
        }
        return !Env.getCurrentEnv().getAuth().doesUserExist(userName, remoteIp);
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
