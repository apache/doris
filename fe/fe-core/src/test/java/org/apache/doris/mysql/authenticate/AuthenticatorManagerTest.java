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
import org.apache.doris.mysql.MysqlAuthPacket;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.authenticate.ldap.LdapManager;
import org.apache.doris.mysql.authenticate.password.ClearPassword;
import org.apache.doris.mysql.authenticate.password.NativePassword;
import org.apache.doris.mysql.authenticate.password.PasswordResolver;
import org.apache.doris.mysql.authenticate.plugin.AuthenticationPluginAuthenticator;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Optional;

class AuthenticatorManagerTest {
    private static final String USER_NAME = "alice";
    private static final String REMOTE_IP = "127.0.0.1";

    private Env env;
    private Auth auth;
    private MockedStatic<Env> envMockedStatic;
    private String originalFallbackChain;
    private boolean originalEnableAuthenticationChain;
    private String originalFallbackPolicy;
    private boolean originalEnableJitUserAuthenticationChain;

    @BeforeEach
    void setUp() throws Exception {
        resetAuthenticatorManagerState();
        originalFallbackChain = Config.authentication_chain;
        originalEnableAuthenticationChain = Config.enable_authentication_chain;
        originalFallbackPolicy = Config.authentication_chain_fallback_policy;
        originalEnableJitUserAuthenticationChain = Config.enable_jit_user_authentication_chain;
        Config.authentication_chain = "";
        Config.enable_authentication_chain = false;
        Config.authentication_chain_fallback_policy = "disabled";
        Config.enable_jit_user_authentication_chain = false;

        env = Mockito.mock(Env.class);
        auth = Mockito.mock(Auth.class);

        envMockedStatic = Mockito.mockStatic(Env.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getAuth()).thenReturn(auth);
    }

    @AfterEach
    void tearDown() throws Exception {
        Config.authentication_chain = originalFallbackChain;
        Config.enable_authentication_chain = originalEnableAuthenticationChain;
        Config.authentication_chain_fallback_policy = originalFallbackPolicy;
        Config.enable_jit_user_authentication_chain = originalEnableJitUserAuthenticationChain;
        if (envMockedStatic != null) {
            envMockedStatic.close();
        }
        resetAuthenticatorManagerState();
    }

    @Test
    void testChooseAuthenticatorUsesConfiguredPluginForExistingLocalUser() {
        Mockito.when(auth.doesUserExist(USER_NAME, REMOTE_IP)).thenReturn(true);

        AuthenticatorManager manager = new AuthenticatorManager("test_plugin");

        Authenticator authenticator = manager.chooseAuthenticator(USER_NAME, REMOTE_IP);
        Assertions.assertTrue(authenticator instanceof AuthenticationPluginAuthenticator);
    }

    @Test
    void testChooseAuthenticatorUsesPluginFactoryWhenLegacyAuthenticatorMissing() {
        AuthenticatorManager manager = new AuthenticatorManager("test_plugin");

        Authenticator authenticator = manager.chooseAuthenticator(USER_NAME, REMOTE_IP);
        Assertions.assertTrue(authenticator instanceof AuthenticationPluginAuthenticator);
    }

    @Test
    void testPasswordAliasUsesDefaultAuthenticator() {
        AuthenticatorManager manager = new AuthenticatorManager("password");

        Authenticator authenticator = manager.chooseAuthenticator(USER_NAME, REMOTE_IP);
        Assertions.assertTrue(authenticator instanceof DefaultAuthenticator);
    }

    @Test
    void testAuthenticateFallsBackToAuthenticationChainOnAnyFailure() throws Exception {
        Config.enable_authentication_chain = true;
        Config.authentication_chain = "corp_ldap";
        Config.authentication_chain_fallback_policy = "any_failure";

        Authenticator primaryAuthenticator = Mockito.mock(Authenticator.class);
        PasswordResolver primaryResolver = Mockito.mock(PasswordResolver.class);
        Mockito.when(primaryAuthenticator.canDeal(USER_NAME)).thenReturn(true);
        Mockito.when(primaryAuthenticator.getPasswordResolver()).thenReturn(primaryResolver);
        Mockito.when(primaryResolver.resolvePassword(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any())).thenReturn(Optional.of(new ClearPassword("secret")));
        Mockito.when(primaryAuthenticator.authenticate(Mockito.any()))
                .thenReturn(AuthenticateResponse.failedResponse);

        Authenticator chainAuthenticator = Mockito.mock(Authenticator.class);
        Mockito.when(chainAuthenticator.canDeal(USER_NAME)).thenReturn(true);
        Mockito.when(chainAuthenticator.authenticate(Mockito.any()))
                .thenReturn(new AuthenticateResponse(true,
                        org.apache.doris.analysis.UserIdentity.createAnalyzedUserIdentWithIp(USER_NAME, REMOTE_IP),
                        true));

        AuthenticatorManager manager = Mockito.spy(new AuthenticatorManager(AuthenticateType.DEFAULT.name()));
        setStaticField("authTypeAuthenticator", primaryAuthenticator);
        setStaticField("authTypeIdentifier", AuthenticateType.DEFAULT.name());
        Mockito.doReturn(chainAuthenticator).when(manager).getAuthenticationChainAuthenticator();

        QueryState state = new QueryState();
        ConnectContext context = mockContext(state);

        boolean result = manager.authenticate(context, USER_NAME, context.getMysqlChannel(),
                Mockito.mock(MysqlSerializer.class), Mockito.mock(MysqlAuthPacket.class),
                Mockito.mock(MysqlHandshakePacket.class));

        Assertions.assertTrue(result);
        Assertions.assertEquals(QueryState.MysqlStateType.OK, state.getStateType());
        Mockito.verify(primaryAuthenticator).authenticate(Mockito.any());
        Mockito.verify(chainAuthenticator).authenticate(Mockito.any());
        Mockito.verify(context).setIsTempUser(true);
    }

    @Test
    void testAuthenticateFallsBackToJitUserChainWhenPrimaryFailsAndLocalUserMissing() throws Exception {
        Config.enable_jit_user_authentication_chain = true;
        Config.authentication_chain = "corp_ldap";
        Mockito.when(auth.doesUserExist(USER_NAME, REMOTE_IP)).thenReturn(false);

        Authenticator primaryAuthenticator = Mockito.mock(Authenticator.class);
        PasswordResolver primaryResolver = Mockito.mock(PasswordResolver.class);
        Mockito.when(primaryAuthenticator.canDeal(USER_NAME)).thenReturn(true);
        Mockito.when(primaryAuthenticator.getPasswordResolver()).thenReturn(primaryResolver);
        Mockito.when(primaryResolver.resolvePassword(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any())).thenReturn(Optional.of(new ClearPassword("secret")));
        Mockito.when(primaryAuthenticator.authenticate(Mockito.any()))
                .thenReturn(AuthenticateResponse.failedResponse);

        Authenticator chainAuthenticator = Mockito.mock(Authenticator.class);
        PasswordResolver chainResolver = Mockito.mock(PasswordResolver.class);
        Mockito.when(chainAuthenticator.canDeal(USER_NAME)).thenReturn(true);
        Mockito.when(chainAuthenticator.getPasswordResolver()).thenReturn(chainResolver);
        Mockito.when(chainResolver.resolvePassword(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any())).thenReturn(Optional.of(new ClearPassword("secret")));
        Mockito.when(chainAuthenticator.authenticate(Mockito.any()))
                .thenReturn(new AuthenticateResponse(true,
                        org.apache.doris.analysis.UserIdentity.createAnalyzedUserIdentWithIp(USER_NAME, REMOTE_IP),
                        true));

        AuthenticatorManager manager = Mockito.spy(new AuthenticatorManager(AuthenticateType.DEFAULT.name()));
        setStaticField("authTypeAuthenticator", primaryAuthenticator);
        setStaticField("authTypeIdentifier", AuthenticateType.DEFAULT.name());
        Mockito.doReturn(chainAuthenticator).when(manager).getAuthenticationChainAuthenticator();

        QueryState state = new QueryState();
        ConnectContext context = mockContext(state);

        boolean result = manager.authenticate(context, USER_NAME, context.getMysqlChannel(),
                Mockito.mock(MysqlSerializer.class), Mockito.mock(MysqlAuthPacket.class),
                Mockito.mock(MysqlHandshakePacket.class));

        Assertions.assertTrue(result);
        Mockito.verify(chainAuthenticator).authenticate(Mockito.any());
        Mockito.verify(context).setIsTempUser(true);
    }

    @Test
    void testAuthenticateDoesNotFallbackToJitUserChainWhenLocalUserExists() throws Exception {
        Config.enable_jit_user_authentication_chain = true;
        Config.authentication_chain = "corp_ldap";
        Mockito.when(auth.doesUserExist(USER_NAME, REMOTE_IP)).thenReturn(true);

        Authenticator primaryAuthenticator = Mockito.mock(Authenticator.class);
        PasswordResolver primaryResolver = Mockito.mock(PasswordResolver.class);
        Mockito.when(primaryAuthenticator.canDeal(USER_NAME)).thenReturn(true);
        Mockito.when(primaryAuthenticator.getPasswordResolver()).thenReturn(primaryResolver);
        Mockito.when(primaryResolver.resolvePassword(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any())).thenReturn(Optional.of(new ClearPassword("secret")));
        Mockito.when(primaryAuthenticator.authenticate(Mockito.any()))
                .thenReturn(AuthenticateResponse.failedResponse);

        Authenticator chainAuthenticator = Mockito.mock(Authenticator.class);

        AuthenticatorManager manager = Mockito.spy(new AuthenticatorManager(AuthenticateType.DEFAULT.name()));
        setStaticField("authTypeAuthenticator", primaryAuthenticator);
        setStaticField("authTypeIdentifier", AuthenticateType.DEFAULT.name());
        Mockito.doReturn(chainAuthenticator).when(manager).getAuthenticationChainAuthenticator();

        QueryState state = Mockito.mock(QueryState.class);
        Mockito.when(state.getStateType()).thenReturn(QueryState.MysqlStateType.ERR);
        Mockito.when(state.toResponsePacket()).thenReturn(Mockito.mock(org.apache.doris.mysql.MysqlPacket.class));
        ConnectContext context = mockContext(state);

        boolean result = manager.authenticate(context, USER_NAME, context.getMysqlChannel(),
                Mockito.mock(MysqlSerializer.class), Mockito.mock(MysqlAuthPacket.class),
                Mockito.mock(MysqlHandshakePacket.class));

        Assertions.assertFalse(result);
        Mockito.verify(chainAuthenticator, Mockito.never()).authenticate(Mockito.any());
    }

    @Test
    void testAuthenticateFallsBackToAuthenticationChainWithResolverSwitch() throws Exception {
        Config.enable_authentication_chain = true;
        Config.authentication_chain = "corp_ldap";
        Config.authentication_chain_fallback_policy = "any_failure";

        Authenticator primaryAuthenticator = Mockito.mock(Authenticator.class);
        PasswordResolver primaryResolver = Mockito.mock(PasswordResolver.class);
        Mockito.when(primaryAuthenticator.canDeal(USER_NAME)).thenReturn(true);
        Mockito.when(primaryAuthenticator.getPasswordResolver()).thenReturn(primaryResolver);
        Mockito.when(primaryResolver.resolvePassword(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any())).thenReturn(Optional.of(new NativePassword(new byte[] {1}, new byte[] {2})));
        Mockito.when(primaryAuthenticator.authenticate(Mockito.any()))
                .thenReturn(AuthenticateResponse.failedResponse);

        Authenticator chainAuthenticator = Mockito.mock(Authenticator.class);
        PasswordResolver chainResolver = Mockito.mock(PasswordResolver.class);
        Mockito.when(chainAuthenticator.canDeal(USER_NAME)).thenReturn(true);
        Mockito.when(chainAuthenticator.getPasswordResolver()).thenReturn(chainResolver);
        Mockito.when(chainResolver.resolvePassword(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any())).thenReturn(Optional.of(new ClearPassword("secret")));
        Mockito.when(chainAuthenticator.authenticate(Mockito.any()))
                .thenReturn(new AuthenticateResponse(true,
                        org.apache.doris.analysis.UserIdentity.createAnalyzedUserIdentWithIp(USER_NAME, REMOTE_IP),
                        false));

        AuthenticatorManager manager = Mockito.spy(new AuthenticatorManager(AuthenticateType.DEFAULT.name()));
        setStaticField("authTypeAuthenticator", primaryAuthenticator);
        setStaticField("authTypeIdentifier", AuthenticateType.DEFAULT.name());
        Mockito.doReturn(chainAuthenticator).when(manager).getAuthenticationChainAuthenticator();

        QueryState state = new QueryState();
        ConnectContext context = mockContext(state);

        boolean result = manager.authenticate(context, USER_NAME, context.getMysqlChannel(),
                Mockito.mock(MysqlSerializer.class), Mockito.mock(MysqlAuthPacket.class),
                Mockito.mock(MysqlHandshakePacket.class));

        Assertions.assertTrue(result);
        Mockito.verify(chainResolver).resolvePassword(Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any(), Mockito.any());
        Mockito.verify(chainAuthenticator).authenticate(Mockito.any());
    }

    @Test
    void testAuthenticateFallsBackToAuthenticationChainOnUserNotFound() throws Exception {
        Config.enable_authentication_chain = true;
        Config.authentication_chain = "corp_ldap";
        Config.authentication_chain_fallback_policy = "user_not_found";
        Mockito.when(auth.doesUserExist(USER_NAME, REMOTE_IP)).thenReturn(false);

        Authenticator primaryAuthenticator = Mockito.mock(Authenticator.class);
        PasswordResolver primaryResolver = Mockito.mock(PasswordResolver.class);
        Mockito.when(primaryAuthenticator.canDeal(USER_NAME)).thenReturn(true);
        Mockito.when(primaryAuthenticator.getPasswordResolver()).thenReturn(primaryResolver);
        Mockito.when(primaryResolver.resolvePassword(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any())).thenReturn(Optional.of(new ClearPassword("secret")));
        Mockito.when(primaryAuthenticator.authenticate(Mockito.any()))
                .thenReturn(AuthenticateResponse.failedResponse);

        Authenticator chainAuthenticator = Mockito.mock(Authenticator.class);
        Mockito.when(chainAuthenticator.canDeal(USER_NAME)).thenReturn(true);
        Mockito.when(chainAuthenticator.authenticate(Mockito.any()))
                .thenReturn(new AuthenticateResponse(true,
                        org.apache.doris.analysis.UserIdentity.createAnalyzedUserIdentWithIp(USER_NAME, REMOTE_IP),
                        false));

        AuthenticatorManager manager = Mockito.spy(new AuthenticatorManager(AuthenticateType.DEFAULT.name()));
        setStaticField("authTypeAuthenticator", primaryAuthenticator);
        setStaticField("authTypeIdentifier", AuthenticateType.DEFAULT.name());
        Mockito.doReturn(chainAuthenticator).when(manager).getAuthenticationChainAuthenticator();

        QueryState state = new QueryState();
        ConnectContext context = mockContext(state);

        boolean result = manager.authenticate(context, USER_NAME, context.getMysqlChannel(),
                Mockito.mock(MysqlSerializer.class), Mockito.mock(MysqlAuthPacket.class),
                Mockito.mock(MysqlHandshakePacket.class));

        Assertions.assertTrue(result);
        Mockito.verify(chainAuthenticator).authenticate(Mockito.any());
        Mockito.verify(context).setIsTempUser(false);
    }

    @Test
    void testAuthenticateFallsBackToAuthenticationChainOnLdapUserNotFound() throws Exception {
        Config.enable_authentication_chain = true;
        Config.authentication_chain = "corp_ldap";
        Config.authentication_chain_fallback_policy = "user_not_found";

        LdapManager ldapManager = Mockito.mock(LdapManager.class);
        Mockito.when(auth.getLdapManager()).thenReturn(ldapManager);
        Mockito.when(ldapManager.doesUserExist(USER_NAME)).thenReturn(false);

        Authenticator primaryAuthenticator = Mockito.mock(Authenticator.class);
        PasswordResolver primaryResolver = Mockito.mock(PasswordResolver.class);
        Mockito.when(primaryAuthenticator.canDeal(USER_NAME)).thenReturn(true);
        Mockito.when(primaryAuthenticator.getPasswordResolver()).thenReturn(primaryResolver);
        Mockito.when(primaryResolver.resolvePassword(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any())).thenReturn(Optional.of(new ClearPassword("secret")));
        Mockito.when(primaryAuthenticator.authenticate(Mockito.any()))
                .thenReturn(AuthenticateResponse.failedResponse);

        Authenticator chainAuthenticator = Mockito.mock(Authenticator.class);
        Mockito.when(chainAuthenticator.canDeal(USER_NAME)).thenReturn(true);
        Mockito.when(chainAuthenticator.authenticate(Mockito.any()))
                .thenReturn(new AuthenticateResponse(true,
                        org.apache.doris.analysis.UserIdentity.createAnalyzedUserIdentWithIp(USER_NAME, REMOTE_IP),
                        false));

        AuthenticatorManager manager = Mockito.spy(new AuthenticatorManager(AuthenticateType.LDAP.name()));
        setStaticField("authTypeAuthenticator", primaryAuthenticator);
        setStaticField("authTypeIdentifier", AuthenticateType.LDAP.name());
        Mockito.doReturn(chainAuthenticator).when(manager).getAuthenticationChainAuthenticator();

        QueryState state = new QueryState();
        ConnectContext context = mockContext(state);

        boolean result = manager.authenticate(context, USER_NAME, context.getMysqlChannel(),
                Mockito.mock(MysqlSerializer.class), Mockito.mock(MysqlAuthPacket.class),
                Mockito.mock(MysqlHandshakePacket.class));

        Assertions.assertTrue(result);
        Mockito.verify(ldapManager).doesUserExist(USER_NAME);
        Mockito.verify(chainAuthenticator).authenticate(Mockito.any());
    }

    @Test
    void testAuthenticateFallsBackToAuthenticationChainOnUserNotFoundForCustomLegacyAuthenticator() throws Exception {
        Config.enable_authentication_chain = true;
        Config.authentication_chain = "corp_ldap";
        Config.authentication_chain_fallback_policy = "user_not_found";
        Mockito.when(auth.doesUserExist(USER_NAME, REMOTE_IP)).thenReturn(false);

        Authenticator primaryAuthenticator = Mockito.mock(Authenticator.class);
        PasswordResolver primaryResolver = Mockito.mock(PasswordResolver.class);
        Mockito.when(primaryAuthenticator.canDeal(USER_NAME)).thenReturn(true);
        Mockito.when(primaryAuthenticator.getPasswordResolver()).thenReturn(primaryResolver);
        Mockito.when(primaryResolver.resolvePassword(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any())).thenReturn(Optional.of(new ClearPassword("secret")));
        Mockito.when(primaryAuthenticator.authenticate(Mockito.any()))
                .thenReturn(AuthenticateResponse.failedResponse);

        Authenticator chainAuthenticator = Mockito.mock(Authenticator.class);
        Mockito.when(chainAuthenticator.canDeal(USER_NAME)).thenReturn(true);
        Mockito.when(chainAuthenticator.authenticate(Mockito.any()))
                .thenReturn(new AuthenticateResponse(true,
                        org.apache.doris.analysis.UserIdentity.createAnalyzedUserIdentWithIp(USER_NAME, REMOTE_IP),
                        false));

        AuthenticatorManager manager = Mockito.spy(new AuthenticatorManager(AuthenticateType.DEFAULT.name()));
        setStaticField("authTypeAuthenticator", primaryAuthenticator);
        setStaticField("authTypeIdentifier", "mihayou--oidc");
        Mockito.doReturn(chainAuthenticator).when(manager).getAuthenticationChainAuthenticator();

        QueryState state = new QueryState();
        ConnectContext context = mockContext(state);

        boolean result = manager.authenticate(context, USER_NAME, context.getMysqlChannel(),
                Mockito.mock(MysqlSerializer.class), Mockito.mock(MysqlAuthPacket.class),
                Mockito.mock(MysqlHandshakePacket.class));

        Assertions.assertTrue(result);
        Mockito.verify(auth).doesUserExist(USER_NAME, REMOTE_IP);
        Mockito.verify(chainAuthenticator).authenticate(Mockito.any());
    }

    @Test
    void testAuthenticateDoesNotFallbackWhenUserExistsUnderUserNotFoundPolicy() throws Exception {
        Config.enable_authentication_chain = true;
        Config.authentication_chain = "corp_ldap";
        Config.authentication_chain_fallback_policy = "user_not_found";
        Mockito.when(auth.doesUserExist(USER_NAME, REMOTE_IP)).thenReturn(true);

        Authenticator primaryAuthenticator = Mockito.mock(Authenticator.class);
        PasswordResolver primaryResolver = Mockito.mock(PasswordResolver.class);
        Mockito.when(primaryAuthenticator.canDeal(USER_NAME)).thenReturn(true);
        Mockito.when(primaryAuthenticator.getPasswordResolver()).thenReturn(primaryResolver);
        Mockito.when(primaryResolver.resolvePassword(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any())).thenReturn(Optional.of(new ClearPassword("secret")));
        Mockito.when(primaryAuthenticator.authenticate(Mockito.any()))
                .thenReturn(AuthenticateResponse.failedResponse);

        Authenticator chainAuthenticator = Mockito.mock(Authenticator.class);

        AuthenticatorManager manager = Mockito.spy(new AuthenticatorManager(AuthenticateType.DEFAULT.name()));
        setStaticField("authTypeAuthenticator", primaryAuthenticator);
        setStaticField("authTypeIdentifier", AuthenticateType.DEFAULT.name());
        Mockito.doReturn(chainAuthenticator).when(manager).getAuthenticationChainAuthenticator();

        QueryState state = Mockito.mock(QueryState.class);
        Mockito.when(state.getStateType()).thenReturn(QueryState.MysqlStateType.ERR);
        Mockito.when(state.toResponsePacket()).thenReturn(Mockito.mock(org.apache.doris.mysql.MysqlPacket.class));
        ConnectContext context = mockContext(state);

        boolean result = manager.authenticate(context, USER_NAME, context.getMysqlChannel(),
                Mockito.mock(MysqlSerializer.class), Mockito.mock(MysqlAuthPacket.class),
                Mockito.mock(MysqlHandshakePacket.class));

        Assertions.assertFalse(result);
        Mockito.verify(chainAuthenticator, Mockito.never()).authenticate(Mockito.any());
    }

    private static void resetAuthenticatorManagerState() throws Exception {
        setStaticField("defaultAuthenticator", null);
        setStaticField("authTypeAuthenticator", null);
        setStaticField("authTypeIdentifier", null);
    }

    private ConnectContext mockContext(QueryState state) {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        MysqlChannel channel = Mockito.mock(MysqlChannel.class);
        MysqlSerializer serializer = Mockito.mock(MysqlSerializer.class);
        Mockito.when(context.getMysqlChannel()).thenReturn(channel);
        Mockito.when(context.getState()).thenReturn(state);
        Mockito.when(channel.getRemoteIp()).thenReturn(REMOTE_IP);
        Mockito.when(channel.getSerializer()).thenReturn(serializer);
        return context;
    }

    private static void setStaticField(String fieldName, Object value) throws Exception {
        Field field = AuthenticatorManager.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(null, value);
    }
}
