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

package org.apache.doris.stack.component;

import static org.mockito.Mockito.when;

import org.apache.doris.stack.model.ldap.LdapConnectionInfo;
import org.apache.doris.stack.model.request.config.InitStudioReq;
import org.apache.doris.stack.model.request.config.LdapSettingReq;
import org.apache.doris.stack.model.response.config.LdapSettingResp;
import org.apache.doris.stack.connector.LdapClient;
import org.apache.doris.stack.entity.SettingEntity;
import org.apache.doris.stack.service.config.ConfigConstant;
import com.google.common.collect.Lists;
import com.unboundid.ldap.sdk.LDAPConnection;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

@RunWith(JUnit4.class)
@Slf4j
public class LdapComponentTest {

    @InjectMocks
    private LdapComponent ldapComponent;

    @Mock
    private SettingComponent settingComponent;

    @Mock
    private LdapClient ldapClient;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Store LDAP configuration
     */
    @Test
    public void testAddLdapConfig() {
        Map<String, String> configCache = new HashMap<>();
        LdapSettingReq ldapSettingReq = new LdapSettingReq();

        ldapSettingReq.setLdapHost("10.23.32.43");
        ldapSettingReq.setLdapBindDn("test");
        ldapSettingReq.setLdapPort(400);
        ldapSettingReq.setLdapAttributeEmail("test");
        ldapSettingReq.setLdapPassword("123456");
        ldapSettingReq.setLdapUserBase(Lists.newArrayList("a", "b"));

        try {
            ldapComponent.addLdapConfig(ldapSettingReq, configCache);
        } catch (Exception e) {
            e.printStackTrace();
        }

        ldapSettingReq.setLdapPort(null);
        ldapSettingReq.setLdapAttributeEmail(null);
        try {
            ldapComponent.addLdapConfig(ldapSettingReq, configCache);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * LDAP enable test
     */
    @Test
    public void testEnable() {
        // Authentication type not configured
        when(settingComponent.readSetting(ConfigConstant.AUTH_TYPE_KEY)).thenReturn(null);
        Assert.assertEquals(ldapComponent.enabled(), false);

        // The authentication type is not LDAP
        SettingEntity settingEntity =
                new SettingEntity(ConfigConstant.AUTH_TYPE_KEY, InitStudioReq.AuthType.studio.name());
        when(settingComponent.readSetting(ConfigConstant.AUTH_TYPE_KEY)).thenReturn(settingEntity);
        Assert.assertEquals(ldapComponent.enabled(), false);

        // LDAP enable
        settingEntity =
                new SettingEntity(ConfigConstant.AUTH_TYPE_KEY, InitStudioReq.AuthType.ldap.name());
        when(settingComponent.readSetting(ConfigConstant.AUTH_TYPE_KEY)).thenReturn(settingEntity);
        Assert.assertEquals(ldapComponent.enabled(), true);
    }

    /**
     * Read LDAP configureation test
     */
    @Test
    public void testReadLdapConfig() {
        // Authentication type not configured
        when(settingComponent.readSetting(ConfigConstant.AUTH_TYPE_KEY)).thenReturn(null);
        LdapSettingResp resp = ldapComponent.readLdapConfig();
        Assert.assertEquals(resp.getLdapEnabled(), false);

        // The authentication type is not LDAP
        SettingEntity settingEntity =
                new SettingEntity(ConfigConstant.AUTH_TYPE_KEY, InitStudioReq.AuthType.studio.name());
        when(settingComponent.readSetting(ConfigConstant.AUTH_TYPE_KEY)).thenReturn(settingEntity);
        resp = ldapComponent.readLdapConfig();
        Assert.assertEquals(resp.getLdapEnabled(), false);

        // LDAP enable
        settingEntity =
                new SettingEntity(ConfigConstant.AUTH_TYPE_KEY, InitStudioReq.AuthType.ldap.name());
        when(settingComponent.readSetting(ConfigConstant.AUTH_TYPE_KEY)).thenReturn(settingEntity);
        resp = ldapComponent.readLdapConfig();
        Assert.assertEquals(resp.getLdapEnabled(), true);

        // mock LDAP configuration
        readConfigMock();

        resp = ldapComponent.readLdapConfig();
        Assert.assertEquals(resp.getLdapEnabled(), true);
        Assert.assertEquals(resp.getLdapHost(), "123.34.434.43");
        Assert.assertEquals(resp.getLdapPort(), new Integer(8080));
        Assert.assertEquals(resp.getLdapSecurity(), "sec");
        Assert.assertEquals(resp.getLdapBindDn(), "bindOn");
        Assert.assertEquals(resp.getLdapPassword(), "passwd");
        Assert.assertEquals(resp.getLdapUserBase(), Lists.newArrayList("1,2", "2,3", "3,4"));
        Assert.assertEquals(resp.getLdapUserFilter(), "filter");
        Assert.assertEquals(resp.getLdapAttributeEmail(), "email");
        Assert.assertEquals(resp.getLdapAttributeFirstName(), "frist");
        Assert.assertEquals(resp.getLdapAttributeLastName(), "last");
    }

    @Test
    public void testCheckLdapConnection() {
        LdapSettingReq ldapSettingReq = new LdapSettingReq();
        LdapConnectionInfo ldapConnection = new LdapConnectionInfo();
        ldapConnection.setPort(ldapSettingReq.getLdapPort());
        ldapConnection.setAttributeEmail(LdapSettingReq.MAIL);
        when(ldapClient.getConnection(ldapConnection)).thenReturn(null);
        Assert.assertEquals(ldapComponent.checkLdapConnection(ldapSettingReq), false);

        LDAPConnection connection = new LDAPConnection();
        when(ldapClient.getConnection(ldapConnection)).thenReturn(connection);
        Assert.assertEquals(ldapComponent.checkLdapConnection(ldapSettingReq), true);
    }

    // mock LDAP configuration
    private void readConfigMock() {
        SettingEntity settingEntity =
                new SettingEntity(ConfigConstant.AUTH_TYPE_KEY, InitStudioReq.AuthType.ldap.name());
        when(settingComponent.readSetting(ConfigConstant.AUTH_TYPE_KEY)).thenReturn(settingEntity);

        SettingEntity hostSettingEntity =
                new SettingEntity(ConfigConstant.LDAP_HOST_KEY, "123.34.434.43");
        when(settingComponent.readSetting(ConfigConstant.LDAP_HOST_KEY)).thenReturn(hostSettingEntity);

        SettingEntity portSettingEntity =
                new SettingEntity(ConfigConstant.LDAP_PORT_KEY, "8080");
        when(settingComponent.readSetting(ConfigConstant.LDAP_PORT_KEY)).thenReturn(portSettingEntity);

        SettingEntity secSettingEntity =
                new SettingEntity(ConfigConstant.LDAP_SECURITY_KEY, "sec");
        when(settingComponent.readSetting(ConfigConstant.LDAP_SECURITY_KEY)).thenReturn(secSettingEntity);

        SettingEntity bindOnSettingEntity =
                new SettingEntity(ConfigConstant.LDAP_BIND_DN_KEY, "bindOn");
        when(settingComponent.readSetting(ConfigConstant.LDAP_BIND_DN_KEY)).thenReturn(bindOnSettingEntity);

        SettingEntity passwdSettingEntity =
                new SettingEntity(ConfigConstant.LDAP_PASSWORD_KEY, "passwd");
        when(settingComponent.readSetting(ConfigConstant.LDAP_PASSWORD_KEY)).thenReturn(passwdSettingEntity);

        SettingEntity baseKeySettingEntity =
                new SettingEntity(ConfigConstant.LDAP_USER_BASE_KEY, "1,2&2,3&3,4");
        when(settingComponent.readSetting(ConfigConstant.LDAP_USER_BASE_KEY)).thenReturn(baseKeySettingEntity);

        SettingEntity filterSettingEntity =
                new SettingEntity(ConfigConstant.LDAP_USER_FILTER_KEY, "filter");
        when(settingComponent.readSetting(ConfigConstant.LDAP_USER_FILTER_KEY)).thenReturn(filterSettingEntity);

        SettingEntity emailSettingEntity =
                new SettingEntity(ConfigConstant.LDAP_ATTRIBUTE_EMAIL_KEY, "email");
        when(settingComponent.readSetting(ConfigConstant.LDAP_ATTRIBUTE_EMAIL_KEY)).thenReturn(emailSettingEntity);

        SettingEntity firstSettingEntity =
                new SettingEntity(ConfigConstant.LDAP_ATTRIBUTE_FIRSTNAME_KEY, "frist");
        when(settingComponent.readSetting(ConfigConstant.LDAP_ATTRIBUTE_FIRSTNAME_KEY)).thenReturn(firstSettingEntity);

        SettingEntity lastSettingEntity =
                new SettingEntity(ConfigConstant.LDAP_ATTRIBUTE_LASTNAME_KEY, "last");
        when(settingComponent.readSetting(ConfigConstant.LDAP_ATTRIBUTE_LASTNAME_KEY)).thenReturn(lastSettingEntity);
    }
}
