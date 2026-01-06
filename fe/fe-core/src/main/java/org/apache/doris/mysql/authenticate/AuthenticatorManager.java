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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.util.ClassLoaderUtils;
import org.apache.doris.mysql.MysqlAuthPacket;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.authenticate.password.Password;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.plugin.PropertiesUtils;
import org.apache.doris.qe.ConnectContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;

public class AuthenticatorManager {
    private static final Logger LOG = LogManager.getLogger(AuthenticatorManager.class);

    private static volatile Authenticator defaultAuthenticator = null;
    private static volatile Authenticator authTypeAuthenticator = null;

    public AuthenticatorManager(String type) {
        LOG.info("Authenticate type: {}", type);
        defaultAuthenticator = new DefaultAuthenticator();
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

        // Step 1: Try TLS certificate-based authentication first
        CertificateAuthVerifier certVerifier = CertificateAuthVerifierFactory.getInstance();
        if (certVerifier.isEnabled()) {
            CertificateAuthResult certResult = tryCertificateAuth(
                    userName, remoteIp, channel, certVerifier);
            if (certResult.shouldReject) {
                // Certificate verification failed - reject the connection
                context.getState().setError(ErrorCode.ERR_ACCESS_DENIED_ERROR,
                        certResult.errorMessage == null ? "TLS certificate verification failed"
                                : certResult.errorMessage);
                MysqlProto.sendResponsePacket(context);
                return false;
            }
            if (certResult.authenticated) {
                // Certificate verification succeeded and password can be skipped
                context.setCurrentUserIdentity(certResult.userIdentity);
                context.setRemoteIP(remoteIp);
                context.setIsTempUser(false);
                LOG.info("User {} authenticated via TLS certificate (skip password)", certResult.userIdentity);
                return true;
            }
            // If certResult.userIdentity is set but not fully authenticated,
            // proceed with password verification using the matched UserIdentity
        }

        // Step 2: Normal password-based authentication
        Authenticator authenticator = chooseAuthenticator(userName);
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

    /**
     * Result of certificate-based authentication attempt.
     */
    private static class CertificateAuthResult {
        /** If true, the connection should be rejected immediately */
        boolean shouldReject = false;
        /** If true, authentication is complete (certificate verified, password skipped) */
        boolean authenticated = false;
        /** The matched UserIdentity (may be set even if not fully authenticated) */
        UserIdentity userIdentity = null;
        /** Error message for certificate verification failure */
        String errorMessage = null;
    }

    /**
     * Attempts TLS certificate-based authentication.
     *
     * @return CertificateAuthResult indicating the outcome
     */
    private CertificateAuthResult tryCertificateAuth(String userName, String remoteIp,
            MysqlChannel channel, CertificateAuthVerifier certVerifier) {
        CertificateAuthResult result = new CertificateAuthResult();

        // Get matching UserIdentity without checking password
        Auth auth = Env.getCurrentEnv().getAuth();
        List<UserIdentity> matchedUsers = auth.getUserIdentityUncheckPasswd(userName, remoteIp);

        if (matchedUsers.isEmpty()) {
            // No matching user found - let normal auth handle the error
            return result;
        }

        // Find the first user that matches and has TLS requirements
        X509Certificate clientCert = channel.getClientCertificate();

        for (UserIdentity userIdentity : matchedUsers) {
            if (!userIdentity.hasTlsRequirements()) {
                // This user doesn't have TLS requirements, skip certificate check
                continue;
            }

            // User has TLS requirements - must verify certificate
            CertificateAuthVerifier.VerificationResult verifyResult =
                    certVerifier.verify(userIdentity, clientCert);

            if (!verifyResult.isSuccess()) {
                // Certificate verification failed - reject immediately
                LOG.warn("TLS certificate verification failed for user {}: {}",
                        userIdentity, verifyResult.getErrorMessage());
                result.shouldReject = true;
                result.errorMessage = verifyResult.getErrorMessage();
                return result;
            }

            // Certificate verification succeeded
            result.userIdentity = userIdentity;
            if (certVerifier.shouldSkipPasswordVerification()) {
                // Skip password verification - authentication complete
                result.authenticated = true;
            }
            return result;
        }

        // No user with TLS requirements found - proceed with normal auth
        return result;
    }

    private Authenticator chooseAuthenticator(String userName) {
        return authTypeAuthenticator.canDeal(userName) ? authTypeAuthenticator : defaultAuthenticator;
    }
}
