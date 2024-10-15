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
import org.apache.doris.mysql.authenticate.AuthenticateRequest;
import org.apache.doris.mysql.authenticate.AuthenticateResponse;
import org.apache.doris.mysql.authenticate.password.ClearPassword;
import org.apache.doris.mysql.authenticate.password.ClearPasswordResolver;
import org.apache.doris.mysql.privilege.Auth;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class LdapAuthenticatorTest {
    private static final String USER_NAME = "user";
    private static final String IP = "192.168.1.1";

    @Mocked
    private LdapManager ldapManager;

    @Mocked
    private Auth auth;

    private LdapAuthenticator ldapAuthenticator = new LdapAuthenticator();
    private AuthenticateRequest request = new AuthenticateRequest(USER_NAME, new ClearPassword("123"), IP);

    private void setCheckPassword(boolean res) {
        new Expectations() {
            {
                ldapManager.checkUserPasswd(anyString, anyString);
                minTimes = 0;
                result = res;
            }
        };
    }

    private void setCheckPasswordException() {
        new Expectations() {
            {
                ldapManager.checkUserPasswd(anyString, anyString);
                minTimes = 0;
                result = new RuntimeException("exception");
            }
        };
    }

    private void setGetUserInDoris(boolean res) {
        new Expectations() {
            {
                if (res) {
                    List<UserIdentity> list = Lists.newArrayList(new UserIdentity(USER_NAME, IP));
                    auth.getUserIdentityForLdap(anyString, anyString);
                    minTimes = 0;
                    result = list;
                } else {
                    auth.getCurrentUserIdentity((UserIdentity) any);
                    minTimes = 0;
                    result = null;
                }
            }
        };
    }

    private void setLdapUserExist(boolean res) {
        new Expectations() {
            {
                ldapManager.doesUserExist(anyString);
                minTimes = 0;
                result = res;
            }
        };
    }

    @Test
    public void testAuthenticate() throws IOException {
        setCheckPassword(true);
        setGetUserInDoris(true);
        AuthenticateResponse response = ldapAuthenticator.authenticate(request);
        Assert.assertTrue(response.isSuccess());
        Assert.assertFalse(response.isTemp());
        Assert.assertEquals("'user'@'192.168.1.1'", response.getUserIdentity().toString());
    }

    @Test
    public void testAuthenticateWithWrongPassword() throws IOException {
        setCheckPassword(false);
        setGetUserInDoris(true);
        AuthenticateResponse response = ldapAuthenticator.authenticate(request);
        Assert.assertFalse(response.isSuccess());
    }

    @Test
    public void testAuthenticateWithCheckPasswordException() throws IOException {
        setCheckPasswordException();
        setGetUserInDoris(true);
        AuthenticateResponse response = ldapAuthenticator.authenticate(request);
        Assert.assertFalse(response.isSuccess());
    }

    @Test
    public void testAuthenticateUserNotExistInDoris() throws IOException {
        setCheckPassword(true);
        setGetUserInDoris(false);
        AuthenticateResponse response = ldapAuthenticator.authenticate(request);
        Assert.assertTrue(response.isSuccess());
        Assert.assertTrue(response.isTemp());
        Assert.assertEquals("'user'@'192.168.1.1'", response.getUserIdentity().toString());
    }

    @Test
    public void testCanDeal() {
        setLdapUserExist(true);
        Assert.assertFalse(ldapAuthenticator.canDeal(Auth.ROOT_USER));
        Assert.assertFalse(ldapAuthenticator.canDeal(Auth.ADMIN_USER));
        Assert.assertTrue(ldapAuthenticator.canDeal("ss"));
        setLdapUserExist(false);
        Assert.assertFalse(ldapAuthenticator.canDeal("ss"));
    }

    @Test
    public void testGetPasswordResolver() {
        Assert.assertTrue(ldapAuthenticator.getPasswordResolver() instanceof ClearPasswordResolver);
    }
}
