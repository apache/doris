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

import org.apache.doris.stack.model.response.config.SettingItem;
import org.apache.doris.stack.constant.EnvironmentDefine;
import org.apache.doris.stack.constant.PropertyDefine;
import org.apache.doris.stack.dao.SettingRepository;
import org.apache.doris.stack.dao.StudioSettingRepository;
import org.apache.doris.stack.entity.SettingEntity;
import org.apache.doris.stack.entity.StudioSettingEntity;
import org.apache.doris.stack.entity.StudioSettingEntityPk;
import org.apache.doris.stack.service.config.ConfigCache;
import org.apache.doris.stack.service.config.ConfigConstant;
import org.apache.doris.stack.service.config.ConfigItem;
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

import java.util.List;
import java.util.Optional;

@RunWith(JUnit4.class)
@Slf4j
public class SettingComponentTest {

    @InjectMocks
    private SettingComponent settingComponent;

    @Mock
    private SettingRepository settingRepository;

    @Mock
    private StudioSettingRepository studioSettingRepository;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Test configuration add or update cache
     */
    @Test
    public void testAddNewSetting() {
        log.debug("Test user add or update setting.");
        // clear cache
        ConfigCache.configCache.clear();

        // Configuration key error
        String key = "test-key";
        String value = "value";
        try {
            settingComponent.addNewSetting(key, value, ConfigCache.configCache);
        } catch (Exception e) {
            Assert.assertEquals("Configuration common key does not exist.", e.getMessage());
        }

        // Add success
        key = ConfigConstant.LDAP_HOST_KEY;
        try {
            settingComponent.addNewSetting(key, value, ConfigCache.configCache);
            Assert.assertEquals(ConfigCache.readConfig(key), value);
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Test configuration add or update cache
     */
    @Test
    public void testAddNewSettingBase() {
        log.debug("Test user add or update setting.");
        // clear cache
        ConfigCache.configCache.clear();

        // Configuration key error
        String key = "test-key";
        String value = "value";
        try {
            settingComponent.addNewSetting(key, value);
        } catch (Exception e) {
            Assert.assertEquals("Configuration common key does not exist.", e.getMessage());
        }

        // Add success
        key = ConfigConstant.LDAP_HOST_KEY;
        try {
            settingComponent.addNewSetting(key, value);
            Assert.assertEquals(ConfigCache.readConfig(key), value);
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    public void testReadSetting() {
        log.debug("Test user read setting.");

        ConfigCache.configCache.clear();

        String key = ConfigConstant.EMAIL_SECURITY_KEY;
        String value = "value";

        Optional<SettingEntity> entityOp = Optional.empty();
        when(settingRepository.findById(key)).thenReturn(entityOp);
        SettingEntity result = settingComponent.readSetting(key);
        Assert.assertEquals(result, null);

        SettingEntity entity = new SettingEntity(key, value);
        entityOp = Optional.of(entity);
        when(settingRepository.findById(key)).thenReturn(entityOp);
        result = settingComponent.readSetting(key);
        Assert.assertEquals(result.getKey(), key);
        // Verify that the cache is written
        result = settingComponent.readSetting(key);
        Assert.assertEquals(result.getKey(), key);

        // read cache
        key = ConfigConstant.SITE_URL_KEY;
        ConfigCache.writeConfig(key, value);
        result = settingComponent.readSetting(key);
        Assert.assertEquals(result.getKey(), key);
    }

    /**
     * Test to get mail configuration
     */
    @Test
    public void testGetUserEmailConfig() {
        log.debug("Test user get all email configs.");
        List<SettingItem> settingItems = settingComponent.getUserEmailConfig();
        Assert.assertEquals(ConfigConstant.EMAIL_CONFIGS.size(), settingItems.size());
    }

    /**
     * Test the function of adding space configuration items
     */
    @Test
    public void testAddNewAdminSetting() {

        ConfigCache.adminConfigCache.clear();

        log.debug("Test add new admin setting.");
        int clusterId = 1;
        String key = ConfigConstant.AUTH_TYPE_KEY;
        String value = "value";
        try {
            settingComponent.addNewAdminSetting(clusterId, key, value);
        } catch (Exception e) {
            Assert.assertEquals("Configuration admin key does not exist.", e.getMessage());
        }

        key = ConfigConstant.QUERY_CACHING_MIN_TTL;
        try {
            settingComponent.addNewAdminSetting(clusterId, key, value);
            Assert.assertEquals(ConfigCache.readAdminConfig(clusterId, key), value);

            Assert.assertEquals(ConfigCache.readAdminConfig(2, key), null);
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    public void testReadAdminSetting() {
        ConfigCache.adminConfigCache.clear();

        log.debug("Test read admin setting.");
        int clusterId = 1;
        String key = ConfigConstant.QUERY_CACHING_MIN_TTL;
        String value = "value";

        StudioSettingEntityPk entityKey = new StudioSettingEntityPk(key, clusterId);
        Optional<StudioSettingEntity> entityOp = Optional.empty();
        when(studioSettingRepository.findById(entityKey)).thenReturn(entityOp);
        StudioSettingEntity result = settingComponent.readAdminSetting(clusterId, key);
        Assert.assertEquals(result, null);

        StudioSettingEntity entity = new StudioSettingEntity(key, clusterId, value);
        entityOp = Optional.of(entity);
        when(studioSettingRepository.findById(entityKey)).thenReturn(entityOp);
        result = settingComponent.readAdminSetting(clusterId, key);
        Assert.assertEquals(result.getKey(), key);
        result = settingComponent.readAdminSetting(clusterId, key);
        Assert.assertEquals(result.getKey(), key);

        result = settingComponent.readAdminSetting(2, key);
        Assert.assertEquals(result, null);

        key = ConfigConstant.QUERY_CACHING_TTL_TATIO;
        ConfigCache.writeAdminConfig(clusterId, key, value);
        result = settingComponent.readAdminSetting(clusterId, key);
        Assert.assertEquals(result.getKey(), key);
    }

    @Test
    public void testReadAdminSettingAndDefault() {
        ConfigCache.adminConfigCache.clear();

        log.debug("Test read admin setting or default.");
        int clusterId = 1;
        String key = ConfigConstant.QUERY_CACHING_MIN_TTL;
        StudioSettingEntityPk entityKey = new StudioSettingEntityPk(key, clusterId);
        Optional<StudioSettingEntity> entityOp = Optional.empty();
        when(studioSettingRepository.findById(entityKey)).thenReturn(entityOp);

        String result = settingComponent.readAdminSettingOrDefault(clusterId, key);
        Assert.assertEquals("60", result);

        StudioSettingEntity entity = new StudioSettingEntity(key, clusterId, "100");
        entityOp = Optional.of(entity);
        when(studioSettingRepository.findById(entityKey)).thenReturn(entityOp);
        result = settingComponent.readAdminSettingOrDefault(clusterId, key);
        Assert.assertEquals("100", result);
    }

    @Test
    public void testTransSettingEntityToModel() {
        String key = ConfigConstant.AUTH_TYPE_KEY;
        ConfigItem item = ConfigConstant.ALL_PUBLIC_CONFIGS.get(key);
        SettingItem result = item.transSettingToModel(null);
        Assert.assertEquals(null, result.getValue());

        String value = "studio";
        SettingEntity entity = new SettingEntity(key, value);
        result = item.transSettingToModel(entity);
        Assert.assertEquals(value, result.getValue());

        entity = new SettingEntity(ConfigConstant.CUSTOM_FORMATTING_KEY, "{}");
        item = ConfigConstant.ALL_PUBLIC_CONFIGS.get(ConfigConstant.CUSTOM_FORMATTING_KEY);
        result = item.transSettingToModel(entity);
        Assert.assertEquals(EnvironmentDefine.CUSTOM_FORMATTING_KEY_ENV, result.getEnvName());

        key = ConfigConstant.ENABLE_QUERY_CACHING;
        item = ConfigConstant.ALL_ADMIN_CONFIGS.get(key);
        result = item.transAdminSettingToModel(null);
        Assert.assertEquals(null, result.getValue());

        int clusterId = 1;
        value = "true";
        StudioSettingEntity studioSettingEntity = new StudioSettingEntity(key, clusterId, value);
        result = item.transAdminSettingToModel(studioSettingEntity);
        Assert.assertEquals(value, result.getValue());
    }

    @Test
    public void publicSharingEnableTest() {
        log.debug("public sharing enable test.");
        ConfigCache.configCache.clear();
        Optional<SettingEntity> entityOp = Optional.empty();
        when(settingRepository.findById(ConfigConstant.ENABLE_PUBLIC_KEY)).thenReturn(entityOp);
        boolean result = settingComponent.publicSharingEnable();
        Assert.assertEquals(result, false);

        ConfigCache.configCache.clear();
        SettingEntity entity = new SettingEntity(ConfigConstant.ENABLE_PUBLIC_KEY, "false");
        when(settingRepository.findById(ConfigConstant.ENABLE_PUBLIC_KEY)).thenReturn(Optional.of(entity));
        result = settingComponent.publicSharingEnable();
        Assert.assertEquals(result, false);

        ConfigCache.configCache.clear();
        entity = new SettingEntity(ConfigConstant.ENABLE_PUBLIC_KEY, "true");
        when(settingRepository.findById(ConfigConstant.ENABLE_PUBLIC_KEY)).thenReturn(Optional.of(entity));
        result = settingComponent.publicSharingEnable();
        Assert.assertEquals(result, true);
    }

    @Test
    public void emailEnableTest() {
        log.debug("email enable test.");

        ConfigCache.configCache.clear();
        Optional<SettingEntity> entityOp = Optional.empty();
        when(settingRepository.findById(ConfigConstant.EMAIL_CONFIGURED_KEY)).thenReturn(entityOp);
        boolean result = settingComponent.emailEnable();
        Assert.assertFalse(result);

        ConfigCache.configCache.clear();
        SettingEntity entity = new SettingEntity(ConfigConstant.EMAIL_CONFIGURED_KEY, "false");
        when(settingRepository.findById(ConfigConstant.EMAIL_CONFIGURED_KEY)).thenReturn(Optional.of(entity));
        result = settingComponent.emailEnable();
        Assert.assertFalse(result);

        ConfigCache.configCache.clear();
        entity = new SettingEntity(ConfigConstant.EMAIL_CONFIGURED_KEY, "true");
        when(settingRepository.findById(ConfigConstant.EMAIL_CONFIGURED_KEY)).thenReturn(Optional.of(entity));
        result = settingComponent.emailEnable();
        Assert.assertTrue(result);
    }

    @Test
    public void sampleDataEnableTest() {
        log.debug("sample data enable test.");
        ConfigCache.configCache.clear();
        Optional<SettingEntity> entityOp = Optional.empty();
        when(settingRepository.findById(ConfigConstant.SAMPLE_DATA_ENABLE_KEY)).thenReturn(entityOp);
        boolean result = settingComponent.sampleDataEnable();
        Assert.assertFalse(result);

        ConfigCache.configCache.clear();

        SettingEntity smapleEnable = new SettingEntity(ConfigConstant.SAMPLE_DATA_ENABLE_KEY, "true");
        when(settingRepository.findById(ConfigConstant.SAMPLE_DATA_ENABLE_KEY)).thenReturn(Optional.of(smapleEnable));
        when(settingRepository.findById(ConfigConstant.DATABASE_TYPE_KEY)).thenReturn(entityOp);
        result = settingComponent.sampleDataEnable();
        Assert.assertFalse(result);

        ConfigCache.configCache.clear();
        SettingEntity dbType = new SettingEntity(ConfigConstant.DATABASE_TYPE_KEY, PropertyDefine.JPA_DATABASE_POSTGRESQL);
        when(settingRepository.findById(ConfigConstant.DATABASE_TYPE_KEY)).thenReturn(Optional.of(dbType));
        result = settingComponent.sampleDataEnable();
        Assert.assertFalse(result);

        ConfigCache.configCache.clear();
        dbType = new SettingEntity(ConfigConstant.DATABASE_TYPE_KEY, PropertyDefine.JPA_DATABASE_MYSQL);
        when(settingRepository.findById(ConfigConstant.DATABASE_TYPE_KEY)).thenReturn(Optional.of(dbType));
        result = settingComponent.sampleDataEnable();
        Assert.assertTrue(result);

        ConfigCache.configCache.clear();
        smapleEnable = new SettingEntity(ConfigConstant.SAMPLE_DATA_ENABLE_KEY, "false");
        when(settingRepository.findById(ConfigConstant.SAMPLE_DATA_ENABLE_KEY)).thenReturn(Optional.of(smapleEnable));
        result = settingComponent.sampleDataEnable();
        Assert.assertFalse(result);
    }
}
