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
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.DdlException;
import org.apache.doris.mysql.authenticate.password.NativePassword;
import org.apache.doris.mysql.authenticate.password.NativePasswordResolver;
import org.apache.doris.mysql.privilege.Auth;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;

public class DefaultAuthenticatorTest {
    private static final String USER_NAME = "user";
    private static final String IP = "192.168.1.1";

    private Auth auth = Mockito.mock(Auth.class);
    private Env env = Mockito.mock(Env.class);
    private MockedStatic<Env> mockedEnvStatic;

    private DefaultAuthenticator defaultAuthenticator = new DefaultAuthenticator();
    private AuthenticateRequest request = new AuthenticateRequest(USER_NAME,
            new NativePassword(new byte[2], new byte[2]), IP);

    @Before
    public void setUp() throws DdlException, AuthenticationException, IOException {
        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getAuth()).thenReturn(auth);

        Mockito.doAnswer(inv -> {
            List<UserIdentity> currentUser = inv.getArgument(4);
            UserIdentity userIdentity = new UserIdentity(USER_NAME, IP);
            currentUser.add(userIdentity);
            return null;
        }).when(auth).checkPassword(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.any(byte[].class), ArgumentMatchers.any(byte[].class), ArgumentMatchers.any(List.class));
    }

    @After
    public void tearDown() {
        mockedEnvStatic.close();
    }

    @Test
    public void testAuthenticate() throws IOException {
        AuthenticateResponse response = defaultAuthenticator.authenticate(request);
        Assert.assertTrue(response.isSuccess());
        Assert.assertFalse(response.isTemp());
        Assert.assertEquals("'user'@'192.168.1.1'", response.getUserIdentity().toString());
    }

    @Test
    public void testAuthenticateFailed() throws IOException, AuthenticationException {
        Mockito.doThrow(new AuthenticationException("exception"))
                .when(auth).checkPassword(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.any(byte[].class), ArgumentMatchers.any(byte[].class), ArgumentMatchers.any(List.class));
        AuthenticateResponse response = defaultAuthenticator.authenticate(request);
        Assert.assertFalse(response.isSuccess());
    }

    @Test
    public void testCanDeal() {
        Assert.assertTrue(defaultAuthenticator.canDeal("ss"));
    }

    @Test
    public void testGetPasswordResolver() {
        Assert.assertTrue(defaultAuthenticator.getPasswordResolver() instanceof NativePasswordResolver);
    }
}
