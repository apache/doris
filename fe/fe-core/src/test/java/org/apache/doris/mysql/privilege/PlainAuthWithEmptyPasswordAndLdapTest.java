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

package org.apache.doris.mysql.privilege;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.LdapConfig;
import org.apache.doris.mysql.authenticate.ldap.LdapManager;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;


public class PlainAuthWithEmptyPasswordAndLdapTest extends TestWithFeService {
    private static final String IP = "192.168.1.1";

    private LdapManager ldapManager = Mockito.mock(LdapManager.class);
    private MockedStatic<Env> mockedEnv;

    @Test
    public void testPlainPasswordAuthWithAllowEmptyPassDefault() throws Exception {
        //running test with non-specified value - ldap_allow_empty_pass should be true
        //non empty pass - success
        Assertions.assertTrue(LdapConfig.ldap_allow_empty_pass);
        Env.getCurrentEnv().getAuth().checkPlainPassword("user1.2", IP, "testPass", null);
        //empty pass - success
        Assertions.assertTrue(LdapConfig.ldap_allow_empty_pass);
        Env.getCurrentEnv().getAuth().checkPlainPassword("user1.1", IP, "", null);
    }


    @Test
    public void testPlainPasswordAuthWithAllowEmptyPassTrue() throws Exception {
        //running test with specified value - ldap_allow_empty_pass is be true
        LdapConfig.ldap_allow_empty_pass = true;

        //non empty pass - success
        Assertions.assertTrue(LdapConfig.ldap_allow_empty_pass);
        Env.getCurrentEnv().getAuth().checkPlainPassword("user2.2", IP, "testPass", null);
        //empty pass - success
        Assertions.assertTrue(LdapConfig.ldap_allow_empty_pass);
        Env.getCurrentEnv().getAuth().checkPlainPassword("user2.1", IP, "", null);
    }

    @Test
    public void testPlainPasswordAuthWithAllowEmptyPassFalse() throws Exception {
        //running test with specified value - ldap_allow_empty_pass is false
        LdapConfig.ldap_allow_empty_pass = false;

        //empty pass - failure
        Assertions.assertFalse(LdapConfig.ldap_allow_empty_pass);
        Assertions.assertThrows(AuthenticationException.class, () -> {
            Env.getCurrentEnv().getAuth().checkPlainPassword("user3.1", IP, "", null);
        });

        //non empty pass - success
        Assertions.assertFalse(LdapConfig.ldap_allow_empty_pass);
        Env.getCurrentEnv().getAuth().checkPlainPassword("user3.2", IP, "testPass", null);
    }

    @AfterEach
    public void tearDown() {
        LdapConfig.ldap_allow_empty_pass = true; // restoring default value for other tests
        mockedEnv.close();
    }

    @BeforeEach
    public void setUp() {
        LdapConfig.ldap_allow_empty_pass = true; // restoring default value for other tests
        Auth realAuth = Env.getCurrentEnv().getAuth();

        mockedEnv = Mockito.mockStatic(Env.class);
        Env mockedEnvInstance = Mockito.mock(Env.class);

        Auth authSpy = Mockito.spy(realAuth);

        Mockito.when(ldapManager.doesUserExist(Mockito.anyString())).thenReturn(true);
        Mockito.when(ldapManager.checkUserPasswd(
            Mockito.anyString(), Mockito.anyString(),
            Mockito.anyString(), Mockito.any())).thenReturn(true);

        Mockito.when(authSpy.getLdapManager()).thenReturn(ldapManager);

        Mockito.when(mockedEnvInstance.getAuth()).thenReturn(authSpy);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(mockedEnvInstance);
    }
}
