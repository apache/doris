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

import org.apache.doris.common.Config;
import org.apache.doris.common.LdapConfig;

import mockit.Expectations;
import mockit.Tested;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.ldap.query.LdapQuery;

import java.util.Arrays;
import java.util.List;

public class LdapClientTest {
    @Tested
    private LdapClient ldapClient;

    @Before
    public void setUp() {
        Config.authentication_type = "ldap";
        LdapConfig.ldap_host = "127.0.0.1";
        LdapConfig.ldap_port = 389;
        LdapConfig.ldap_admin_name = "cn=admin,dc=baidu,dc=com";
        LdapConfig.ldap_user_basedn = "dc=baidu,dc=com";
        LdapConfig.ldap_group_basedn = "ou=group,dc=baidu,dc=com";
        LdapConfig.ldap_user_filter = "(&(uid={login}))";
    }

    @Test
    public void testDoesUserExist() {
        List<String> list = Arrays.asList("zhangsan");

        new Expectations(ldapClient) {
            {
                ldapClient.getDn((LdapQuery) any);
                result = list;
            }
        };

        boolean result = ldapClient.doesUserExist("zhangsan");
        Assert.assertTrue(result);
    }

    @Test
    public void testDoesUserExistFail() {
        new Expectations(ldapClient) {
            {
                ldapClient.getDn((LdapQuery) any);
                result = null;
            }
        };
        Assert.assertFalse(ldapClient.doesUserExist("zhangsan"));
    }

    @Test(expected = RuntimeException.class)
    public void testDoesUserExistException() {
        List<String> list = Arrays.asList("zhangsan", "zhangsan");
        new Expectations(ldapClient) {
            {
                ldapClient.getDn((LdapQuery) any);
                result = list;
            }
        };
        Assert.assertTrue(ldapClient.doesUserExist("zhangsan"));
        Assert.fail("No Exception throws.");
    }

    @Test
    public void testGetGroups() {
        List<String> list = Arrays.asList("cn=groupName,ou=groups,dc=example,dc=com");
        new Expectations(ldapClient) {
            {
                ldapClient.getDn((LdapQuery) any);
                result = list;
            }
        };
        Assert.assertEquals(1, ldapClient.getGroups("zhangsan").size());
    }

    @Test
    public void testSecuredProtocolIsUsed() {
        //testing default case with not specified property ldap_use_ssl or it is specified as false
        String insecureUrl = ldapClient.getURL();
        Assert.assertNotNull("connection URL should not be null", insecureUrl);
        Assert.assertTrue("with ldap_use_ssl connection = false or not specified URL should start with ldap, but received: " + insecureUrl,
                          insecureUrl.startsWith("ldap://"));

        //testing new case with specified property ldap_use_ssl as true
        LdapConfig.ldap_use_ssl = true;
        String secureUrl = ldapClient.getURL();
        Assert.assertNotNull("connection URL should not be null", secureUrl);
        Assert.assertTrue("with ldap_use_ssl = true URL connection should start with ldaps, but received: " + secureUrl,
                          secureUrl.startsWith("ldaps://"));
    }

    @After
    public void tearDown() {
        LdapConfig.ldap_use_ssl = false; // restoring default value for other tests
    }
}
