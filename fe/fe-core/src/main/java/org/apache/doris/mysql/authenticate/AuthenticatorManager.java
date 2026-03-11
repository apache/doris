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
import org.apache.doris.common.util.ClassLoaderUtils;
import org.apache.doris.mysql.MysqlAuthPacket;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.authenticate.password.Password;
import org.apache.doris.plugin.PropertiesUtils;
import org.apache.doris.qe.ConnectContext;

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
        LOG.info("Authenticate type: {}", type);
        defaultAuthenticator = new DefaultAuthenticator();
        if (authTypeAuthenticator == null || !type.equalsIgnoreCase(authTypeIdentifier)) {
            synchronized (AuthenticatorManager.class) {
                if (authTypeAuthenticator == null || !type.equalsIgnoreCase(authTypeIdentifier)) {
                    try {
                        authTypeAuthenticator = loadFactoriesByName(type);
                        authTypeIdentifier = type;
                    } catch (Exception e) {
                        if (!AuthenticateType.DEFAULT.name().equalsIgnoreCase(type)) {
                            throw new IllegalStateException("Failed to load authenticator by name: " + type, e);
                        }
                        LOG.warn("Failed to load authenticator by name: {}, using default authenticator", type, e);
                        authTypeAuthenticator = defaultAuthenticator;
                        authTypeIdentifier = AuthenticateType.DEFAULT.name();
                    }
                }
            }
        }
    }


    private Authenticator loadFactoriesByName(String identifier) throws Exception {
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
            LOG.info("No customer authenticator found, using default authenticator");
            return defaultAuthenticator;
        }
        for (AuthenticatorFactory factory : factories) {
            LOG.info("Found Customer Authenticator Plugin Factory: {}", factory.factoryIdentifier());
            if (factory.factoryIdentifier().equalsIgnoreCase(identifier)) {
                Properties properties = PropertiesUtils.loadAuthenticationConfigFile();
                return factory.create(properties);
            }
        }

        throw new RuntimeException("No AuthenticatorFactory found for identifier: " + identifier);
    }

    public boolean authenticate(ConnectContext context,
                                String userName,
                                MysqlChannel channel,
                                MysqlSerializer serializer,
                                MysqlAuthPacket authPacket,
                                MysqlHandshakePacket handshakePacket) throws IOException {
        String remoteIp = context.getMysqlChannel().getRemoteIp();
        Authenticator authenticator = chooseAuthenticator(userName, remoteIp);
        Optional<Password> password = authenticator.getPasswordResolver()
                .resolvePassword(context, channel, serializer, authPacket, handshakePacket);
        if (!password.isPresent()) {
            return false;
        }
        AuthenticateRequest request = new AuthenticateRequest(userName, password.get(), remoteIp);
        AuthenticateResponse response = authenticator.authenticate(request);
        if (!response.isSuccess()) {
            MysqlProto.sendResponsePacket(context);
            return false;
        }
        context.setCurrentUserIdentity(response.getUserIdentity());
        context.setRemoteIP(remoteIp);
        context.setIsTempUser(response.isTemp());
        return true;
    }

    Authenticator chooseAuthenticator(String userName, String remoteIp) {
        if (AuthenticateType.INTEGRATION.name().equalsIgnoreCase(authTypeIdentifier)
                && Env.getCurrentEnv().getAuth().doesUserExist(userName, remoteIp)) {
            return defaultAuthenticator;
        }
        return authTypeAuthenticator.canDeal(userName) ? authTypeAuthenticator : defaultAuthenticator;
    }
}
