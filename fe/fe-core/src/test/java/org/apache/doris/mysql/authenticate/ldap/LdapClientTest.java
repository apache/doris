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
import org.apache.doris.common.util.NetUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.ldap.query.LdapQuery;
import org.springframework.ldap.support.LdapEncoder;

import java.util.Arrays;
import java.util.List;

public class LdapClientTest {
    private LdapClient ldapClient = Mockito.spy(new LdapClient());

    @BeforeEach
    public void setUp() {
        Config.authentication_type = "ldap";
        LdapConfig.ldap_host = "127.0.0.1";
        LdapConfig.ldap_port = 389;
        LdapConfig.ldap_admin_name = "cn=admin,dc=baidu,dc=com";
        LdapConfig.ldap_user_basedn = "dc=baidu,dc=com";
        LdapConfig.ldap_group_basedn = "ou=group,dc=baidu,dc=com";
        LdapConfig.ldap_user_filter = "(&(uid={login}))";
        LdapConfig.ldap_use_ssl = false;
    }

    @Test
    public void testDoesUserExist() {
        List<String> list = Arrays.asList("zhangsan");
        Mockito.doReturn(list).when(ldapClient).getDn(Mockito.any(LdapQuery.class));

        boolean result = ldapClient.doesUserExist("zhangsan");
        Assertions.assertTrue(result);
    }

    @Test
    public void testDoesUserExistFail() {
        Mockito.doReturn(null).when(ldapClient).getDn(Mockito.any(LdapQuery.class));
        Assertions.assertFalse(ldapClient.doesUserExist("zhangsan"));
    }

    @Test
    public void testDoesUserExistException() {
        Assertions.assertThrows(RuntimeException.class, () -> {
            List<String> list = Arrays.asList("zhangsan", "zhangsan");
            Mockito.doReturn(list).when(ldapClient).getDn(Mockito.any(LdapQuery.class));
            Assertions.assertTrue(ldapClient.doesUserExist("zhangsan"));
            Assertions.fail("No Exception throws.");
        });
    }

    @Test
    public void testGetGroups() {
        List<String> list = Arrays.asList("cn=groupName,ou=groups,dc=example,dc=com");
        Mockito.doReturn(list).when(ldapClient).getDn(Mockito.any(LdapQuery.class));
        Assertions.assertEquals(1, ldapClient.getGroups("zhangsan").size());
    }

    @Test
    public void testSecuredProtocolIsUsed() {
        String insecureUrl = LdapConfig.getConnectionURL(
                NetUtils.getHostPortInAccessibleFormat(LdapConfig.ldap_host, LdapConfig.ldap_port));

        Assertions.assertNotNull(insecureUrl, "connection URL should not be null");
        Assertions.assertTrue(insecureUrl.startsWith("ldap://"), "with ldap_use_ssl = false or not specified URL should start with ldap, but received: " + insecureUrl);

        LdapConfig.ldap_use_ssl = true;
        String secureUrl = LdapConfig.getConnectionURL(
                NetUtils.getHostPortInAccessibleFormat(LdapConfig.ldap_host, LdapConfig.ldap_port));
        Assertions.assertNotNull(secureUrl, "connection URL should not be null");
        Assertions.assertTrue(secureUrl.startsWith("ldaps://"), "with ldap_use_ssl = true URL should start with ldaps, but received: " + secureUrl);
    }

    @Test
    public void testLdapFilterEncoding() {
        // Combined special characters
        String input = "test*()\\\u0000";
        String expected = "test\\2a\\28\\29\\5c\\00";
        Assertions.assertEquals(expected, LdapEncoder.filterEncode(input));

        // Null input
        Assertions.assertNull(LdapEncoder.filterEncode(null));

        // Normal username should not be altered
        Assertions.assertEquals("zhangsan", LdapEncoder.filterEncode("zhangsan"));
        Assertions.assertEquals("user.name@example.com", LdapEncoder.filterEncode("user.name@example.com"));

        // Empty string
        Assertions.assertEquals("", LdapEncoder.filterEncode(""));

        // Each special character individually
        Assertions.assertEquals("\\2a", LdapEncoder.filterEncode("*"));
        Assertions.assertEquals("\\28", LdapEncoder.filterEncode("("));
        Assertions.assertEquals("\\29", LdapEncoder.filterEncode(")"));
        Assertions.assertEquals("\\5c", LdapEncoder.filterEncode("\\"));
        Assertions.assertEquals("\\00", LdapEncoder.filterEncode("\u0000"));

        // Injection payload: dorisuser6)(mail=testp*
        Assertions.assertEquals("dorisuser6\\29\\28mail=testp\\2a",
                LdapEncoder.filterEncode("dorisuser6)(mail=testp*"));
    }

    @AfterEach
    public void tearDown() {
        LdapConfig.ldap_use_ssl = false;
    }
}
