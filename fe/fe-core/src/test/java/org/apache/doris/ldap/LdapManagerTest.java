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

import org.apache.doris.common.LdapConfig;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

public class LdapManagerTest {

    private static final String USER1 = "user1";
    private static final String USER2 = "user2";

    @Mocked
    private LdapClient ldapClient;

    @Before
    public void setUp() {
        LdapConfig.ldap_authentication_enabled = true;
    }

    private void mockClient(boolean userExist, boolean passwd) {
        new Expectations() {
            {
                ldapClient.doesUserExist(anyString);
                minTimes = 0;
                result = userExist;

                ldapClient.checkPassword(anyString, anyString);
                minTimes = 0;
                result = passwd;

                ldapClient.getGroups(anyString);
                minTimes = 0;
                result = new ArrayList<>();
            }
        };
    }

    @Test
    public void testGetUserInfo() {
        LdapManager ldapManager = new LdapManager();
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
        mockClient(true, true);
        Assert.assertTrue(ldapManager.checkUserPasswd(USER1, "123"));
        LdapUserInfo ldapUserInfo = ldapManager.getUserInfo(USER1);
        Assert.assertNotNull(ldapUserInfo);
        Assert.assertTrue(ldapUserInfo.isSetPasswd());
        Assert.assertEquals("123", ldapUserInfo.getPasswd());

        mockClient(true, false);
        Assert.assertFalse(ldapManager.checkUserPasswd(USER2, "123"));
    }
}
