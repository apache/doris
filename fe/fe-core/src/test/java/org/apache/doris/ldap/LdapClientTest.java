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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.LdapConfig;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.persist.LdapInfo;

import com.clearspring.analytics.util.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.core.support.AbstractContextMapper;
import org.springframework.ldap.query.LdapQuery;

import java.util.List;

import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;

public class LdapClientTest {
    private static final String ADMIN_PASSWORD = "admin";

    @Mocked
    private LdapTemplate ldapTemplate;

    @Mocked
    private Catalog catalog;

    @Mocked
    private PaloAuth auth;

    private LdapInfo ldapInfo = new LdapInfo(ADMIN_PASSWORD);

    @Before
    public void setUp() {
        new Expectations() {
            {
                Catalog.getCurrentCatalog();
                minTimes = 0;
                result = catalog;

                catalog.getAuth();
                minTimes = 0;
                result = auth;

                auth.getLdapInfo();
                minTimes = 0;
                result = ldapInfo;
            }
        };

        LdapConfig.ldap_authentication_enabled = true;
        LdapConfig.ldap_host = "127.0.0.1";
        LdapConfig.ldap_port = 389;
        LdapConfig.ldap_admin_name = "cn=admin,dc=baidu,dc=com";
        LdapConfig.ldap_user_basedn = "dc=baidu,dc=com";
        LdapConfig.ldap_group_basedn = "ou=group,dc=baidu,dc=com";
        LdapConfig.ldap_user_filter = "(&(uid={login}))";
    }
    
    private void mockLdapTemplateSearch(List list) {
        new Expectations() {
            {
                ldapTemplate.search((LdapQuery) any, (AbstractContextMapper) any);
                minTimes = 0;
                result = list;
            }
        };
    }

    private void mockLdapTemplateAuthenticate(String password) {
        new Expectations() {
            {
                ldapTemplate.authenticate((LdapQuery) any, anyString);
                minTimes = 0;
                result = new Delegate() {
                    void fakeAuthenticate(LdapQuery query, String passwd) {
                        if (passwd.equals(password)) {
                            return;
                        } else {
                            throw new RuntimeException("exception");
                        }
                    }
                };
            }
        };
    }

    @Test
    public void testDoesUserExist() {
        List<String> list = Lists.newArrayList();
        list.add("zhangsan");
        mockLdapTemplateSearch(list);
        Assert.assertTrue(LdapClient.doesUserExist("zhangsan"));
    }

    @Test
    public void testDoesUserExistFail() {
        mockLdapTemplateSearch(null);
        Assert.assertFalse(LdapClient.doesUserExist("zhangsan"));
    }

    @Test(expected = RuntimeException.class)
    public void testDoesUserExistException() {
        List<String> list = Lists.newArrayList();
        list.add("zhangsan");
        list.add("zhangsan");
        mockLdapTemplateSearch(list);
        Assert.assertTrue(LdapClient.doesUserExist("zhangsan"));
        Assert.fail("No Exception throws.");
    }

    @Test
    public void testCheckPassword() {
        mockLdapTemplateAuthenticate(ADMIN_PASSWORD);
        Assert.assertTrue(LdapClient.checkPassword("zhangsan", ADMIN_PASSWORD));
        Assert.assertFalse(LdapClient.checkPassword("zhangsan", "123"));
    }

    @Test
    public void testGetGroups() {
        List<String> list = Lists.newArrayList();
        list.add("cn=groupName,ou=groups,dc=example,dc=com");
        mockLdapTemplateSearch(list);
        Assert.assertEquals(1, LdapClient.getGroups("zhangsan").size());
    }
}
