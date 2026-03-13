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

package org.apache.doris.mysql.authenticate;

import org.apache.doris.common.Config;
import org.apache.doris.common.LdapConfig;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AuthenticateTypeTest {
    private String originalAuthenticationType;
    private boolean originalLegacyLdapEnabled;

    @BeforeEach
    void setUp() {
        originalAuthenticationType = Config.authentication_type;
        originalLegacyLdapEnabled = LdapConfig.ldap_authentication_enabled;
        LdapConfig.ldap_authentication_enabled = false;
    }

    @AfterEach
    void tearDown() {
        Config.authentication_type = originalAuthenticationType;
        LdapConfig.ldap_authentication_enabled = originalLegacyLdapEnabled;
    }

    @Test
    void testPasswordAliasMapsToDefaultAuthenticator() {
        Config.authentication_type = "password";

        Assertions.assertEquals(AuthenticateType.DEFAULT, AuthenticateType.getAuthTypeConfig());
        Assertions.assertEquals(AuthenticateType.DEFAULT.name(), AuthenticateType.getAuthTypeConfigString());
    }

    @Test
    void testUnknownAuthTypeStringIsPreservedForPluginLookup() {
        Config.authentication_type = "test_plugin";

        Assertions.assertEquals(AuthenticateType.DEFAULT, AuthenticateType.getAuthTypeConfig());
        Assertions.assertEquals("test_plugin", AuthenticateType.getAuthTypeConfigString());
    }

    @Test
    void testIntegrationKeywordIsNotReservedAnymore() {
        Config.authentication_type = "integration";

        Assertions.assertEquals(AuthenticateType.DEFAULT, AuthenticateType.getAuthTypeConfig());
        Assertions.assertEquals("integration", AuthenticateType.getAuthTypeConfigString());
    }
}
