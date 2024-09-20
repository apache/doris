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

import org.apache.doris.common.EnvUtils;
import org.apache.doris.mysql.MysqlAuthPacket;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.authenticate.password.Password;
import org.apache.doris.qe.ConnectContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;

public class AuthenticatorManager {
    private static final Logger LOG = LogManager.getLogger(AuthenticatorManager.class);

    private static volatile Authenticator defaultAuthenticator = null;
    private static volatile Authenticator authTypeAuthenticator = null;

    public AuthenticatorManager(String type) {
        LOG.info("Authenticate type: {}", type);

        if (authTypeAuthenticator == null) {
            synchronized (AuthenticatorManager.class) {
                if (authTypeAuthenticator == null) {
                    try {
                        authTypeAuthenticator = loadFactoriesByName(type);
                    } catch (Exception e) {
                        LOG.warn("Failed to load authenticator by name: {}, using default authenticator", type, e);
                        authTypeAuthenticator = defaultAuthenticator;
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
                return factory.create(loadConfigFile());
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
        Authenticator authenticator = chooseAuthenticator(userName);
        Optional<Password> password = authenticator.getPasswordResolver()
                .resolvePassword(context, channel, serializer, authPacket, handshakePacket);
        if (!password.isPresent()) {
            return false;
        }
        String remoteIp = context.getMysqlChannel().getRemoteIp();
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

    private Authenticator chooseAuthenticator(String userName) {
        return authTypeAuthenticator.canDeal(userName) ? authTypeAuthenticator : defaultAuthenticator;
    }

    private static Properties loadConfigFile() throws Exception {
        String configFilePath = EnvUtils.getDorisHome() + "/conf/authenticate.conf";
        if (new File(configFilePath).exists()) {
            LOG.info("Loading authenticate configuration file: {}", configFilePath);
            Properties properties = new Properties();
            properties.load(Files.newInputStream(Paths.get(configFilePath)));
            return properties;
        }
        return new Properties();
    }
}
