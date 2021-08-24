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

package org.apache.doris.stack.service.config;

import static org.mockito.Mockito.when;

import org.apache.doris.stack.model.request.config.LdapSettingReq;
import org.apache.doris.stack.component.LdapComponent;
import org.apache.doris.stack.component.SettingComponent;
import org.apache.doris.stack.exception.LdapConnectionException;
import org.apache.doris.stack.exception.LdapNotExistException;
import org.apache.doris.stack.exception.RequestFieldNullException;
import com.google.common.collect.Lists;
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

@RunWith(JUnit4.class)
@Slf4j
public class LdapServiceTest {

    @InjectMocks
    private LdapService ldapService;

    @Mock
    private SettingComponent settingComponent;

    @Mock
    private LdapComponent ldapComponent;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Test and update LDAP configuration information
     */
    @Test
    public void testUpdate() {
        log.debug("Test update ldap config.");

        // LDAP not enable
        when(ldapComponent.enabled()).thenReturn(false);
        LdapSettingReq ldapSettingReq = new LdapSettingReq();
        try {
            ldapService.update(ldapSettingReq);
        } catch (Exception e) {
            Assert.assertEquals(LdapNotExistException.MESSAGE, e.getMessage());
            log.debug(e.getMessage());
        }

        // request exception
        when(ldapComponent.enabled()).thenReturn(true);
        try {
            ldapService.update(ldapSettingReq);
        } catch (Exception e) {
            Assert.assertEquals(RequestFieldNullException.MESSAGE, e.getMessage());
        }

        ldapSettingReq.setLdapHost("10.23.32.43");
        ldapSettingReq.setLdapBindDn("test");
        try {
            ldapService.update(ldapSettingReq);
        } catch (Exception e) {
            Assert.assertEquals(RequestFieldNullException.MESSAGE, e.getMessage());
        }

        // LDAP connection exception
        ldapSettingReq.setLdapPassword("123456");
        ldapSettingReq.setLdapUserBase(Lists.newArrayList("a", "b"));
        when(ldapComponent.checkLdapConnection(ldapSettingReq)).thenReturn(false);
        try {
            ldapService.update(ldapSettingReq);
        } catch (Exception e) {
            Assert.assertEquals(LdapConnectionException.MESSAGE, e.getMessage());
        }

        // The information configuration is correct and the result is returned
        when(ldapComponent.checkLdapConnection(ldapSettingReq)).thenReturn(true);
        try {
            ldapService.update(ldapSettingReq);
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }

}
