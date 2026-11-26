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

package org.apache.doris.mysql.authenticate.ldap;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.mysql.authenticate.AuthenticateRequest;
import org.apache.doris.mysql.authenticate.AuthenticateResponse;
import org.apache.doris.mysql.authenticate.password.ClearPassword;
import org.apache.doris.mysql.authenticate.password.ClearPasswordResolver;
import org.apache.doris.mysql.privilege.Auth;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;

public class LdapAuthenticatorTest {
    private static final String USER_NAME = "user";
    private static final String IP = "192.168.1.1";

    private LdapManager ldapManager = Mockito.mock(LdapManager.class);
    private Auth auth = Mockito.mock(Auth.class);
    private Env env = Mockito.mock(Env.class);
    private MockedStatic<Env> mockedEnvStatic;

    private LdapAuthenticator ldapAuthenticator = new LdapAuthenticator();
    private AuthenticateRequest request = new AuthenticateRequest(USER_NAME, new ClearPassword("123"), IP);

    @BeforeEach
    public void setUp() {
        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getAuth()).thenReturn(auth);
        Mockito.when(auth.getLdapManager()).thenReturn(ldapManager);
    }

    @AfterEach
    public void tearDown() {
        mockedEnvStatic.close();
    }

    private void setCheckPassword(boolean res) {
        Mockito.when(ldapManager.checkUserPasswd(Mockito.anyString(), Mockito.anyString())).thenReturn(res);
    }

    private void setCheckPasswordException() {
        Mockito.when(ldapManager.checkUserPasswd(Mockito.anyString(), Mockito.anyString()))
                .thenThrow(new RuntimeException("exception"));
    }

    private void setGetUserInDoris(boolean res) {
        if (res) {
            List<UserIdentity> list = Lists.newArrayList(new UserIdentity(USER_NAME, IP));
            Mockito.when(auth.getUserIdentityForLdap(Mockito.anyString(), Mockito.anyString())).thenReturn(list);
        } else {
            Mockito.when(auth.getUserIdentityForLdap(Mockito.anyString(), Mockito.anyString()))
                    .thenReturn(Lists.newArrayList());
            Mockito.when(auth.getCurrentUserIdentity(Mockito.any(UserIdentity.class))).thenReturn(null);
        }
    }

    private void setLdapUserExist(boolean res) {
        Mockito.when(ldapManager.doesUserExist(Mockito.anyString())).thenReturn(res);
    }

    @Test
    public void testAuthenticate() throws IOException {
        setCheckPassword(true);
        setGetUserInDoris(true);
        AuthenticateResponse response = ldapAuthenticator.authenticate(request);
        Assertions.assertTrue(response.isSuccess());
        Assertions.assertFalse(response.isTemp());
        Assertions.assertEquals("'user'@'192.168.1.1'", response.getUserIdentity().toString());
    }

    @Test
    public void testAuthenticateWithWrongPassword() throws IOException {
        setCheckPassword(false);
        setGetUserInDoris(true);
        AuthenticateResponse response = ldapAuthenticator.authenticate(request);
        Assertions.assertFalse(response.isSuccess());
    }

    @Test
    public void testAuthenticateWithCheckPasswordException() throws IOException {
        setCheckPasswordException();
        setGetUserInDoris(true);
        AuthenticateResponse response = ldapAuthenticator.authenticate(request);
        Assertions.assertFalse(response.isSuccess());
    }

    @Test
    public void testAuthenticateUserNotExistInDoris() throws IOException {
        setCheckPassword(true);
        setGetUserInDoris(false);
        AuthenticateResponse response = ldapAuthenticator.authenticate(request);
        Assertions.assertTrue(response.isSuccess());
        Assertions.assertTrue(response.isTemp());
        Assertions.assertEquals("'user'@'192.168.1.1'", response.getUserIdentity().toString());
    }

    @Test
    public void testCanDeal() {
        setLdapUserExist(true);
        Assertions.assertFalse(ldapAuthenticator.canDeal(Auth.ROOT_USER));
        Assertions.assertFalse(ldapAuthenticator.canDeal(Auth.ADMIN_USER));
        Assertions.assertTrue(ldapAuthenticator.canDeal("ss"));
        setLdapUserExist(false);
        Assertions.assertFalse(ldapAuthenticator.canDeal("ss"));
    }

    @Test
    public void testGetPasswordResolver() {
        Assertions.assertTrue(ldapAuthenticator.getPasswordResolver() instanceof ClearPasswordResolver);
    }
}
