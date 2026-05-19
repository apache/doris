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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.LdapConfig;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.Role;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;

public class LdapManagerTest {

    private static final String USER1 = "user1";
    private static final String USER2 = "user2";
    private static final String LDAP_GROUP_ROLE = "ldap_group_role";
    private static final String LDAP_DEFAULT_ROLE = "ldap_default_role";
    private static final String MISSING_LDAP_DEFAULT_ROLE = "missing_ldap_default_role";

    private LdapClient ldapClient = Mockito.mock(LdapClient.class);

    @Before
    public void setUp() {
        Config.authentication_type = "ldap";
        LdapConfig.ldap_default_roles = new String[0];
    }

    private void mockClient(boolean userExist, boolean passwd) {
        mockClient(userExist, passwd, new ArrayList<>());
    }

    private void mockClient(boolean userExist, boolean passwd, ArrayList<String> groups) {
        Mockito.when(ldapClient.doesUserExist(Mockito.anyString())).thenReturn(userExist);
        Mockito.when(ldapClient.checkPassword(Mockito.anyString(), Mockito.anyString())).thenReturn(passwd);
        Mockito.when(ldapClient.getGroups(Mockito.anyString())).thenReturn(groups);
    }

    private void mockAuth(MockedStatic<Env> envMockedStatic, Role ldapGroupRole, Role ldapDefaultRole) {
        Env env = Mockito.mock(Env.class);
        Auth auth = Mockito.mock(Auth.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getAuth()).thenReturn(auth);
        Mockito.when(auth.doesRoleExist(LDAP_GROUP_ROLE)).thenReturn(true);
        Mockito.when(auth.getRoleByName(LDAP_GROUP_ROLE)).thenReturn(ldapGroupRole);
        Mockito.when(auth.doesRoleExist(LDAP_DEFAULT_ROLE)).thenReturn(true);
        Mockito.when(auth.getRoleByName(LDAP_DEFAULT_ROLE)).thenReturn(ldapDefaultRole);
        Mockito.when(auth.doesRoleExist(MISSING_LDAP_DEFAULT_ROLE)).thenReturn(false);
    }

    @Test
    public void testGetUserInfo() {
        LdapManager ldapManager = new LdapManager();
        Deencapsulation.setField(ldapManager, "ldapClient", ldapClient);
        mockClient(true, true);
        LdapUserInfo ldapUserInfo = ldapManager.getUserInfo(USER1);
        Assert.assertNotNull(ldapUserInfo);
        String paloRoleString = ldapUserInfo.getRoles().toString();
        Assert.assertTrue(paloRoleString.contains("information_schema"));
        Assert.assertTrue(paloRoleString.contains("Select_priv"));

        mockClient(false, false);
        Assert.assertNull(ldapManager.getUserInfo(USER2));
    }

    @Test
    public void testCheckUserPasswd() {
        LdapManager ldapManager = new LdapManager();
        Deencapsulation.setField(ldapManager, "ldapClient", ldapClient);
        mockClient(true, true);
        Assert.assertTrue(ldapManager.checkUserPasswd(USER1, "123"));
        LdapUserInfo ldapUserInfo = ldapManager.getUserInfo(USER1);
        Assert.assertNotNull(ldapUserInfo);
        Assert.assertTrue(ldapUserInfo.isSetPasswd());
        Assert.assertEquals("123", ldapUserInfo.getPasswd());

        mockClient(true, false);
        Assert.assertFalse(ldapManager.checkUserPasswd(USER2, "123"));
    }

    @Test
    public void testGetUserInfoWithLdapDefaultRoles() {
        LdapManager ldapManager = new LdapManager();
        Deencapsulation.setField(ldapManager, "ldapClient", ldapClient);
        LdapConfig.ldap_default_roles = new String[] {LDAP_DEFAULT_ROLE, MISSING_LDAP_DEFAULT_ROLE};
        Role ldapGroupRole = new Role(LDAP_GROUP_ROLE);
        Role ldapDefaultRole = new Role(LDAP_DEFAULT_ROLE);
        mockClient(true, true, new ArrayList<>(Arrays.asList(LDAP_GROUP_ROLE)));
        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            mockAuth(envMockedStatic, ldapGroupRole, ldapDefaultRole);

            LdapUserInfo ldapUserInfo = ldapManager.getUserInfo(USER1);
            Assert.assertNotNull(ldapUserInfo);
            Assert.assertTrue(ldapUserInfo.getRoles().contains(ldapGroupRole));
            Assert.assertTrue(ldapUserInfo.getRoles().contains(ldapDefaultRole));
            Assert.assertEquals(3, ldapUserInfo.getRoles().size());
        }
    }
}
