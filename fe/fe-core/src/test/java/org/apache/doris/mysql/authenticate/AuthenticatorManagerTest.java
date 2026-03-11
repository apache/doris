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
import org.apache.doris.mysql.authenticate.integration.AuthenticationIntegrationAuthenticator;
import org.apache.doris.mysql.privilege.Auth;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;

class AuthenticatorManagerTest {
    private static final String USER_NAME = "alice";
    private static final String REMOTE_IP = "127.0.0.1";

    private Env env;
    private Auth auth;
    private MockedStatic<Env> envMockedStatic;
    private String originalChain;

    @BeforeEach
    void setUp() throws Exception {
        resetAuthenticatorManagerState();
        originalChain = Config.default_authentication_chain;
        Config.default_authentication_chain = "corp_ldap";

        env = Mockito.mock(Env.class);
        auth = Mockito.mock(Auth.class);

        envMockedStatic = Mockito.mockStatic(Env.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getAuth()).thenReturn(auth);
    }

    @AfterEach
    void tearDown() throws Exception {
        Config.default_authentication_chain = originalChain;
        if (envMockedStatic != null) {
            envMockedStatic.close();
        }
        resetAuthenticatorManagerState();
    }

    @Test
    void testChooseAuthenticatorUsesDefaultForExistingLocalUserInIntegrationMode() {
        Mockito.when(auth.doesUserExist(USER_NAME, REMOTE_IP)).thenReturn(true);

        AuthenticatorManager manager = new AuthenticatorManager(AuthenticateType.INTEGRATION.name());

        Authenticator authenticator = manager.chooseAuthenticator(USER_NAME, REMOTE_IP);
        Assertions.assertTrue(authenticator instanceof DefaultAuthenticator);
    }

    @Test
    void testChooseAuthenticatorUsesIntegrationForNonExistingLocalUserInIntegrationMode() {
        Mockito.when(auth.doesUserExist(USER_NAME, REMOTE_IP)).thenReturn(false);

        AuthenticatorManager manager = new AuthenticatorManager(AuthenticateType.INTEGRATION.name());

        Authenticator authenticator = manager.chooseAuthenticator(USER_NAME, REMOTE_IP);
        Assertions.assertTrue(authenticator instanceof AuthenticationIntegrationAuthenticator);
    }

    private static void resetAuthenticatorManagerState() throws Exception {
        setStaticField("defaultAuthenticator", null);
        setStaticField("authTypeAuthenticator", null);
        setStaticField("authTypeIdentifier", null);
    }

    private static void setStaticField(String fieldName, Object value) throws Exception {
        Field field = AuthenticatorManager.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(null, value);
    }
}
