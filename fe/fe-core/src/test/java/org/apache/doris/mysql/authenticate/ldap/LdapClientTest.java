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
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.persist.LdapInfo;

import com.google.common.collect.Lists;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.core.support.AbstractContextMapper;
import org.springframework.ldap.query.LdapQuery;

import java.util.List;

public class LdapClientTest {
    private static final String ADMIN_PASSWORD = "admin";

    @Mocked
    private LdapTemplate ldapTemplate;

    @Mocked
    private Env env;

    @Mocked
    private Auth auth;

    private LdapInfo ldapInfo = new LdapInfo(ADMIN_PASSWORD);

    private LdapClient ldapClient = new LdapClient();

    @Before
    public void setUp() {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAuth();
                minTimes = 0;
                result = auth;

                auth.getLdapInfo();
                minTimes = 0;
                result = ldapInfo;
            }
        };

        Config.authentication_type = "ldap";
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
                            throw new org.springframework.ldap.AuthenticationException();
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
        Assert.assertTrue(ldapClient.doesUserExist("zhangsan"));
    }

    @Test
    public void testDoesUserExistFail() {
        mockLdapTemplateSearch(null);
        Assert.assertFalse(ldapClient.doesUserExist("zhangsan"));
    }

    @Test(expected = RuntimeException.class)
    public void testDoesUserExistException() {
        List<String> list = Lists.newArrayList();
        list.add("zhangsan");
        list.add("zhangsan");
        mockLdapTemplateSearch(list);
        Assert.assertTrue(ldapClient.doesUserExist("zhangsan"));
        Assert.fail("No Exception throws.");
    }

    @Test
    public void testCheckPassword() {
        mockLdapTemplateAuthenticate(ADMIN_PASSWORD);
        Assert.assertTrue(ldapClient.checkPassword("zhangsan", ADMIN_PASSWORD));
        Assert.assertFalse(ldapClient.checkPassword("zhangsan", "123"));
    }

    @Test
    public void testGetGroups() {
        List<String> list = Lists.newArrayList();
        list.add("cn=groupName,ou=groups,dc=example,dc=com");
        mockLdapTemplateSearch(list);
        Assert.assertEquals(1, ldapClient.getGroups("zhangsan").size());
    }
}
