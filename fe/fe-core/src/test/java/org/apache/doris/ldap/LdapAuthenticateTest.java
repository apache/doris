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

import com.google.common.collect.Lists;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PaloRole;
import org.apache.doris.qe.ConnectContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class LdapAuthenticateTest {
    private static final String DEFAULT_CLUSTER = "default_cluster";
    private static final String USER_NAME = "user";
    private static final String IP = "192.168.1.1";
    private static final String TABLE_RD = "palo_rd";

    private PaloRole ldapGroupsPrivs;

    @Mocked
    private LdapClient ldapClient;
    @Mocked
    private LdapPrivsChecker ldapPrivsChecker;
    @Mocked
    private Catalog catalog;
    @Mocked
    private PaloAuth auth;

    @Before
    public void setUp() throws DdlException {
        new Expectations() {
            {
                auth.doesRoleExist(anyString);
                minTimes = 0;
                result = true;

                auth.mergeRolesNoCheckName((List<String>) any, (PaloRole) any);
                minTimes = 0;
                result = new Delegate() {
                    void fakeMergeRolesNoCheckName(List<String> roles, PaloRole savedRole) {
                        ldapGroupsPrivs = savedRole;
                    }
                };

                catalog.getAuth();
                minTimes = 0;
                result = auth;

                Catalog.getCurrentCatalog();
                minTimes = 0;
                result = catalog;
            }
        };
    }

    private void setCheckPassword(boolean res) {
        new Expectations() {
            {
                LdapClient.checkPassword(anyString, anyString);
                minTimes = 0;
                result = res;
            }
        };
    }

    private void setCheckPasswordException() {
        new Expectations() {
            {
                LdapClient.checkPassword(anyString, anyString);
                minTimes = 0;
                result = new RuntimeException("exception");
            }
        };
    }

    private void setGetGroups(boolean res) {
        new Expectations() {
            {
                if (res) {
                    LdapClient.getGroups(anyString);
                    minTimes = 0;
                    result = new Delegate() {
                        List<String> fakeGetGroups(String user) {
                            List<String> list = new ArrayList<>();
                            list.add(TABLE_RD);
                            return list;
                        }
                    };
                } else {
                    LdapClient.getGroups(anyString);
                    minTimes = 0;
                    result = Lists.newArrayList();
                }
            }
        };
    }

    private void setGetGroupsException() {
        new Expectations() {
            {
                LdapClient.getGroups(anyString);
                minTimes = 0;
                result = new RuntimeException("exception");
            }
        };
    }

    private void setGetCurrentUserIdentity(boolean res) {
        new Expectations() {
            {
                if (res) {
                    auth.getCurrentUserIdentity((UserIdentity) any);
                    minTimes = 0;
                    result = new UserIdentity(ClusterNamespace.getFullName(DEFAULT_CLUSTER, USER_NAME), IP);
                } else {
                    auth.getCurrentUserIdentity((UserIdentity) any);
                    minTimes = 0;
                    result = null;
                }
            }
        };
    }

    private ConnectContext getContext() {
        ConnectContext context = new ConnectContext(null);
        context.setCatalog(catalog);
        context.setThreadLocalInfo();
        return context;
    }


    @Test
    public void testAuthenticate() {
        ConnectContext context = getContext();
        setCheckPassword(true);
        setGetGroups(true);
        setGetCurrentUserIdentity(true);
        String qualifiedUser = ClusterNamespace.getFullName(DEFAULT_CLUSTER, USER_NAME);
        Assert.assertTrue(LdapAuthenticate.authenticate(context, "123", qualifiedUser));
        Assert.assertFalse(context.getIsTempUser());
        Assert.assertSame(ldapGroupsPrivs, context.getLdapGroupsPrivs());
    }

    @Test
    public void testAuthenticateWithWrongPassword() {
        ConnectContext context = getContext();
        setCheckPassword(false);
        setGetGroups(true);
        setGetCurrentUserIdentity(true);
        String qualifiedUser = ClusterNamespace.getFullName(DEFAULT_CLUSTER, USER_NAME);
        Assert.assertFalse(LdapAuthenticate.authenticate(context, "123", qualifiedUser));
        Assert.assertFalse(context.getIsTempUser());
        Assert.assertNull(context.getLdapGroupsPrivs());
    }

    @Test
    public void testAuthenticateWithCheckPasswordException() {
        ConnectContext context = getContext();
        setCheckPasswordException();
        setGetGroups(true);
        setGetCurrentUserIdentity(true);
        String qualifiedUser = ClusterNamespace.getFullName(DEFAULT_CLUSTER, USER_NAME);
        Assert.assertFalse(LdapAuthenticate.authenticate(context, "123", qualifiedUser));
        Assert.assertFalse(context.getIsTempUser());
        Assert.assertNull(context.getLdapGroupsPrivs());
    }

    @Test
    public void testAuthenticateGetGroupsNull() {
        ConnectContext context = getContext();
        setCheckPassword(true);
        setGetGroups(false);
        setGetCurrentUserIdentity(true);
        String qualifiedUser = ClusterNamespace.getFullName(DEFAULT_CLUSTER, USER_NAME);
        Assert.assertTrue(LdapAuthenticate.authenticate(context, "123", qualifiedUser));
        Assert.assertFalse(context.getIsTempUser());
        Assert.assertNull(context.getLdapGroupsPrivs());
    }

    @Test
    public void testAuthenticateGetGroupsException() {
        ConnectContext context = getContext();
        setCheckPassword(true);
        setGetGroupsException();
        setGetCurrentUserIdentity(true);
        String qualifiedUser = ClusterNamespace.getFullName(DEFAULT_CLUSTER, USER_NAME);
        Assert.assertFalse(LdapAuthenticate.authenticate(context, "123", qualifiedUser));
        Assert.assertFalse(context.getIsTempUser());
        Assert.assertNull(context.getLdapGroupsPrivs());
    }

    @Test
    public void testAuthenticateUserNotExistInDoris() {
        ConnectContext context = getContext();
        setCheckPassword(true);
        setGetGroups(true);
        setGetCurrentUserIdentity(false);
        String qualifiedUser = ClusterNamespace.getFullName(DEFAULT_CLUSTER, USER_NAME);
        Assert.assertTrue(LdapAuthenticate.authenticate(context, "123", qualifiedUser));
        Assert.assertTrue(context.getIsTempUser());
        Assert.assertSame(ldapGroupsPrivs, context.getLdapGroupsPrivs());
    }
}
