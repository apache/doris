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

package org.apache.doris.ldap;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.Role;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Sets;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class LdapAuthenticateTest {
    private static final String USER_NAME = "user";
    private static final String IP = "192.168.1.1";
    private static final String TABLE_RD = "palo_rd";

    private Role ldapGroupsPrivs;

    @Mocked
    private LdapManager ldapManager;
    @Mocked
    private Env env;
    @Mocked
    private Auth auth;
    @Mocked
    private AccessControllerManager accessManager;

    @Before
    public void setUp() throws DdlException {
        new Expectations() {
            {
                auth.doesRoleExist(anyString);
                minTimes = 0;
                result = true;

                auth.mergeRolesNoCheckName((List<String>) any, (Role) any);
                minTimes = 0;
                result = new Delegate() {
                    void fakeMergeRolesNoCheckName(List<String> roles, Role savedRole) {
                        ldapGroupsPrivs = savedRole;
                    }
                };

                env.getAccessManager();
                minTimes = 0;
                result = accessManager;

                env.getAuth();
                minTimes = 0;
                result = auth;

                Env.getCurrentEnv();
                minTimes = 0;
                result = env;
            }
        };
    }

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

    private void setGetUserInfo(boolean res) {
        new Expectations() {
            {
                if (res) {
                    ldapManager.getUserInfo(anyString);
                    minTimes = 0;
                    result = new Delegate() {
                        LdapUserInfo fakeGetGroups(String user) {
                            return new LdapUserInfo(anyString, false, "", Sets.newHashSet(new Role(anyString)));
                        }
                    };
                } else {
                    ldapManager.getUserInfo(anyString);
                    minTimes = 0;
                    result = null;
                }
            }
        };
    }

    private void setGetCurrentUserIdentity(boolean res) {
        new Expectations() {
            {
                if (res) {
                    auth.getCurrentUserIdentity((UserIdentity) any);
                    minTimes = 0;
                    result = new UserIdentity(USER_NAME, IP);
                } else {
                    auth.getCurrentUserIdentity((UserIdentity) any);
                    minTimes = 0;
                    result = null;
                }
            }
        };
    }

    private ConnectContext getContext() {
        ConnectContext context = new ConnectContext();
        context.setEnv(env);
        context.setThreadLocalInfo();
        return context;
    }


    @Test
    public void testAuthenticate() {
        ConnectContext context = getContext();
        setCheckPassword(true);
        setGetUserInfo(true);
        setGetCurrentUserIdentity(true);
        String qualifiedUser = USER_NAME;
        Assert.assertTrue(LdapAuthenticate.authenticate(context, "123", qualifiedUser));
        Assert.assertTrue(context.getIsTempUser());
    }

    @Test
    public void testAuthenticateWithWrongPassword() {
        ConnectContext context = getContext();
        setCheckPassword(false);
        setGetUserInfo(true);
        setGetCurrentUserIdentity(true);
        String qualifiedUser = USER_NAME;
        Assert.assertFalse(LdapAuthenticate.authenticate(context, "123", qualifiedUser));
        Assert.assertFalse(context.getIsTempUser());
    }

    @Test
    public void testAuthenticateWithCheckPasswordException() {
        ConnectContext context = getContext();
        setCheckPasswordException();
        setGetUserInfo(true);
        setGetCurrentUserIdentity(true);
        String qualifiedUser = USER_NAME;
        Assert.assertFalse(LdapAuthenticate.authenticate(context, "123", qualifiedUser));
        Assert.assertFalse(context.getIsTempUser());
    }

    @Test
    public void testAuthenticateGetGroupsNull() {
        ConnectContext context = getContext();
        setCheckPassword(true);
        setGetUserInfo(false);
        setGetCurrentUserIdentity(true);
        String qualifiedUser = USER_NAME;
        Assert.assertTrue(LdapAuthenticate.authenticate(context, "123", qualifiedUser));
        Assert.assertTrue(context.getIsTempUser());
    }

    @Test
    public void testAuthenticateUserNotExistInDoris() {
        ConnectContext context = getContext();
        setCheckPassword(true);
        setGetUserInfo(true);
        setGetCurrentUserIdentity(false);
        String qualifiedUser = USER_NAME;
        Assert.assertTrue(LdapAuthenticate.authenticate(context, "123", qualifiedUser));
        Assert.assertTrue(context.getIsTempUser());
    }
}
