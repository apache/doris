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
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.DdlException;
import org.apache.doris.mysql.authenticate.password.NativePassword;
import org.apache.doris.mysql.authenticate.password.NativePasswordResolver;
import org.apache.doris.mysql.privilege.Auth;

import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class DefaultAuthenticatorTest {
    private static final String USER_NAME = "user";
    private static final String IP = "192.168.1.1";

    @Mocked
    private Auth auth;

    private DefaultAuthenticator defaultAuthenticator = new DefaultAuthenticator();
    private AuthenticateRequest request = new AuthenticateRequest(USER_NAME,
            new NativePassword(new byte[2], new byte[2]), IP);


    @Before
    public void setUp() throws DdlException, AuthenticationException, IOException {

        // mock auth
        new Expectations() {
            {
                auth.checkPassword(anyString, anyString, (byte[]) any, (byte[]) any, (List<UserIdentity>) any);
                minTimes = 0;
                result = new Delegate() {
                    void fakeCheckPassword(String remoteUser, String remoteHost, byte[] remotePasswd,
                            byte[] randomString, List<UserIdentity> currentUser) {
                        UserIdentity userIdentity = new UserIdentity(USER_NAME, IP);
                        currentUser.add(userIdentity);
                    }
                };
            }
        };
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
        new Expectations() {
            {
                auth.checkPassword(anyString, anyString, (byte[]) any, (byte[]) any, (List<UserIdentity>) any);
                minTimes = 0;
                result = new AuthenticationException("exception");
            }
        };
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
