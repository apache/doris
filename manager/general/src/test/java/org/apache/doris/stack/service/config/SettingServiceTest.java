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

import org.apache.doris.stack.model.request.config.ConfigUpdateReq;
import org.apache.doris.stack.model.request.config.InitStudioReq;
import org.apache.doris.stack.model.request.config.LdapSettingReq;
import org.apache.doris.stack.model.response.config.SettingItem;
import org.apache.doris.stack.component.ClusterUserComponent;
import org.apache.doris.stack.component.LdapComponent;
import org.apache.doris.stack.component.SettingComponent;
import org.apache.doris.stack.entity.SettingEntity;
import org.apache.doris.stack.entity.StudioSettingEntity;
import org.apache.doris.stack.exception.LdapConnectionException;
import org.apache.doris.stack.exception.NoPermissionException;
import org.apache.doris.stack.exception.RequestFieldNullException;
import org.apache.doris.stack.exception.StudioInitException;
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
import org.springframework.core.env.Environment;

import java.util.List;

@RunWith(JUnit4.class)
@Slf4j
public class SettingServiceTest {

    @InjectMocks
    private SettingService settingService;

    @Mock
    private ClusterUserComponent clusterUserComponent;

    @Mock
    private SettingComponent settingComponent;

    @Mock
    private Environment environment;

    @Mock
    private LdapComponent ldapComponent;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Initialize the configuration when the test system starts
     */
    @Test
    public void testInitConfig() {
        log.debug("Test init config.");
        // No environment variables are configured,
        // and there is no configuration information in the database
        try {
            settingService.initConfig();
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Test the authentication type of initialization service (studio user authentication type test)
     */
    @Test
    public void testInitStudioUserAuth() {
        log.debug("Test init studio auth type by studio.");
        // Test input request exception
        InitStudioReq req = new InitStudioReq();
        try {
            settingService.initStudio(req);
        } catch (Exception e) {
            Assert.assertEquals(RequestFieldNullException.MESSAGE, e.getMessage());
        }

        // The authentication method has been configured.
        // There is no need to configure it. An exception is returned
        req.setAuthType(InitStudioReq.AuthType.studio);
        String settingKey = ConfigConstant.AUTH_TYPE_KEY;
        SettingEntity authEntity = new SettingEntity(ConfigConstant.AUTH_TYPE_KEY, InitStudioReq.AuthType.studio.name());
        when(settingComponent.readSetting(settingKey)).thenReturn(authEntity);
        try {
            settingService.initStudio(req);
        } catch (Exception e) {
            Assert.assertEquals(StudioInitException.MESSAGE, e.getMessage());
            log.debug(e.getMessage());
        }

        // The authentication method is not configured, and the result is returned
        when(settingComponent.readSetting(settingKey)).thenReturn(null);
        try {
            settingService.initStudio(req);
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Test the authentication tyoe of initialization service (LDAP authentication method test)
     */
    @Test
    public void testInitStudioLdapAuth() {
        log.debug("Test init studio auth type by ldap.");
        // Test input request exception
        InitStudioReq req = new InitStudioReq();
        req.setAuthType(InitStudioReq.AuthType.ldap);
        try {
            settingService.initStudio(req);
        } catch (Exception e) {
            Assert.assertEquals(RequestFieldNullException.MESSAGE, e.getMessage());
        }

        LdapSettingReq ldapSettingReq = new LdapSettingReq();
        req.setLdapSetting(ldapSettingReq);
        try {
            settingService.initStudio(req);
        } catch (Exception e) {
            Assert.assertEquals(RequestFieldNullException.MESSAGE, e.getMessage());
        }

        ldapSettingReq.setLdapHost("10.23.32.43");
        ldapSettingReq.setLdapBindDn("test");
        try {
            settingService.initStudio(req);
        } catch (Exception e) {
            Assert.assertEquals(RequestFieldNullException.MESSAGE, e.getMessage());
        }

        // ldap conncetion Exception
        ldapSettingReq.setLdapPassword("123456");
        ldapSettingReq.setLdapUserBase(Lists.newArrayList("a", "b"));
        req.setLdapSetting(ldapSettingReq);
        when(ldapComponent.checkLdapConnection(ldapSettingReq)).thenReturn(false);
        try {
            settingService.initStudio(req);
        } catch (Exception e) {
            Assert.assertEquals(LdapConnectionException.MESSAGE, e.getMessage());
        }

        when(ldapComponent.checkLdapConnection(ldapSettingReq)).thenReturn(true);
        try {
            settingService.initStudio(req);
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * The test space administrator obtains all public and space configuration item information
     */
    @Test
    public void testGetAllConfig() {
        log.debug("Test get admin user all config.");

        int userId = 1;

        // Return normally and check the number of returned configuration items
        try {
            when(clusterUserComponent.getClusterIdByUserId(userId)).thenReturn(1);
            List<SettingItem> settingItems = settingService.getAllConfig(userId);

            Assert.assertEquals(ConfigConstant.ALL_ADMIN_CONFIGS.size()
                    + ConfigConstant.ALL_PUBLIC_CONFIGS.size(), settingItems.size());
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }

        // User space information does not exist
        try {
            when(clusterUserComponent.getClusterIdByUserId(userId)).thenThrow(new NoPermissionException());
            settingService.getAllConfig(userId);
        } catch (Exception e) {
            Assert.assertEquals(NoPermissionException.MESSAGE, e.getMessage());
        }
    }

    /**
     * The test super administrator obtains all public configuration item information
     */
    @Test
    public void testGetAllPublicConfig() {
        log.debug("Test super user get all configs.");
        List<SettingItem> settingItems = settingService.getAllPublicConfig();
        Assert.assertEquals(ConfigConstant.ALL_PUBLIC_CONFIGS.size(), settingItems.size());
    }

    /**
     * The test super administrator obtains the public configuration item
     * information according to the configuration item name
     */
    @Test
    public void testGetConfigByKey() {
        log.debug("Test super user get config by key.");
        // configuration key error
        String key = "test-key";
        try {
            settingService.getConfigByKey(key);
        } catch (Exception e) {
            Assert.assertEquals("Configuration key does not exist.", e.getMessage());
        }

        // get configuration
        key = ConfigConstant.AUTH_TYPE_KEY;
        try {
            SettingEntity settingEntity = new SettingEntity(key, "value");
            ConfigItem configItem = ConfigConstant.ALL_PUBLIC_CONFIGS.get(key);
            SettingItem item = new SettingItem();
            item.setKey(key);
            item.setValue("value");
            when(settingComponent.readSetting(key)).thenReturn(settingEntity);
            SettingItem result = settingService.getConfigByKey(key);
            Assert.assertEquals(result.getKey(), key);
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * The test space administrator obtains the public space configuration
     * item information according to the configuration item name
     */
    @Test
    public void testGetAdminConfigByKey() {
        log.debug("Test admin user get config by key.");
        int userId = 1;
        // Configuration key error
        String key = "test-key";
        try {
            settingService.getConfigByKey(key, userId);
        } catch (Exception e) {
            Assert.assertEquals("Configuration key does not exist.", e.getMessage());
        }

        // Get public configuration information
        key = ConfigConstant.AUTH_TYPE_KEY;
        try {
            SettingItem item = settingService.getConfigByKey(key, userId);
            Assert.assertEquals(item.getKey(), key);
        } catch (Exception e) {
            log.debug(e.getMessage());
        }

        // Get space configuration information
        key = ConfigConstant.QUERY_CACHING_MIN_TTL;
        try {
            settingService.getConfigByKey(key, userId);
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * The test super administrator modifies the public configuration
     * item information according to the configuration item name
     */
    @Test
    public void testSuperUpdateConfigByKey() {
        log.debug("Test super user update config by key.");
        // request empty
        ConfigUpdateReq updateReq = new ConfigUpdateReq();
        String key = "test-key";
        try {
            settingService.superUpdateConfigByKey(key, updateReq);
        } catch (Exception e) {
            Assert.assertEquals(RequestFieldNullException.MESSAGE, e.getMessage());
        }
        updateReq.setValue("test");

        // Configuration key error
        try {
            settingService.superUpdateConfigByKey(key, updateReq);
        } catch (Exception e) {
            Assert.assertEquals("Configuration key does not exist.", e.getMessage());
        }

        // update configuration
        key = ConfigConstant.CUSTOM_FORMATTING_KEY;
        SettingEntity settingEntity = new SettingEntity(key, "{}");
        when(settingComponent.readSetting(key)).thenReturn(settingEntity);
        try {
            settingService.superUpdateConfigByKey(key, updateReq);
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * The test space administrator obtains the space configuration item
     * information according to the configuration item name
     */
    @Test
    public void testAmdinUpdateConfigByKey() {
        log.debug("Test admin user update config by key.");
        int userId = 1;
        int clusterId = 2;

        try {
            when(clusterUserComponent.getClusterIdByUserId(userId)).thenReturn(clusterId);
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
        // request empty
        ConfigUpdateReq updateReq = new ConfigUpdateReq();
        String key = "test-key";
        try {
            settingService.amdinUpdateConfigByKey(key, userId, updateReq);
        } catch (Exception e) {
            Assert.assertEquals(RequestFieldNullException.MESSAGE, e.getMessage());
        }
        updateReq.setValue("test");

        // Configuration key error
        try {
            settingService.amdinUpdateConfigByKey(key, userId, updateReq);
        } catch (Exception e) {
            Assert.assertEquals("Configuration key does not exist.", e.getMessage());
        }

        key = ConfigConstant.AUTH_TYPE_KEY;
        try {
            settingService.amdinUpdateConfigByKey(key, userId, updateReq);
        } catch (Exception e) {
            Assert.assertEquals("Configuration key does not exist.", e.getMessage());
        }

        // update configuration
        key = ConfigConstant.QUERY_CACHING_MIN_TTL;
        StudioSettingEntity studioSettingEntity = new StudioSettingEntity(key, clusterId, "value");
        when(settingComponent.readAdminSetting(clusterId, key)).thenReturn(studioSettingEntity);
        try {
            settingService.amdinUpdateConfigByKey(key, userId, updateReq);
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
