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

import com.alibaba.fastjson.JSON;
import org.apache.doris.stack.model.request.config.ConfigUpdateReq;
import org.apache.doris.stack.model.request.config.InitStudioReq;
import org.apache.doris.stack.model.request.config.IdaasSettingReq;
import org.apache.doris.stack.model.request.config.LdapSettingReq;
import org.apache.doris.stack.model.response.config.SettingItem;
import org.apache.doris.stack.constant.PropertyDefine;
import org.apache.doris.stack.component.ClusterUserComponent;
import org.apache.doris.stack.component.IdaasComponent;
import org.apache.doris.stack.component.LdapComponent;
import org.apache.doris.stack.component.SettingComponent;
import org.apache.doris.stack.entity.SettingEntity;
import org.apache.doris.stack.entity.StudioSettingEntity;
import org.apache.doris.stack.exception.BadRequestException;
import org.apache.doris.stack.exception.IdaasConnectionException;
import org.apache.doris.stack.exception.LdapConnectionException;
import org.apache.doris.stack.exception.StudioInitException;
import org.apache.doris.stack.service.BaseService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class SettingService extends BaseService {

    @Autowired
    private ClusterUserComponent clusterUserComponent;

    @Autowired
    private SettingComponent settingComponent;

    @Autowired
    private Environment environment;

    @Autowired
    private LdapComponent ldapComponent;

    @Autowired
    private IdaasComponent idaasComponent;

    /**
     * Initializes the authentication method for the ervice
     * @param initStudioReq
     * @throws Exception
     */
    @Transactional
    public void initStudio(InitStudioReq initStudioReq) throws Exception {
        log.debug("Init studio auth type.");
        checkRequestBody(initStudioReq.getAuthType() == null);

        // Check whether the authentication method has been configured
        SettingEntity entity = settingComponent.readSetting(ConfigConstant.AUTH_TYPE_KEY);

        if (entity != null && !StringUtils.isEmpty(entity.getValue())) {
            log.error("Auth config type already exists.");
            throw new StudioInitException();
        }

        // Store the cache information and update the cache in batches after writing to the database successfully,
        // because the database can be rolled back, but the cache cannot
        Map<String, String> configCache = new HashMap<>();

        try {
            if (initStudioReq.getAuthType() == InitStudioReq.AuthType.ldap) {
                log.debug("init ladp config.");

                // Check whether the LDAP request information is complete
                log.debug("Check LdapSettingReq info.");
                LdapSettingReq ldapSettingReq = initStudioReq.getLdapSetting();
                checkRequestBody(ldapSettingReq == null);
                checkRequestBody(ldapSettingReq.hasEmptyField());

                boolean isConnection = ldapComponent.checkLdapConnection(ldapSettingReq);
                if (!isConnection) {
                    throw new LdapConnectionException();
                }

                log.debug("add new ldap config.");
                settingComponent.addNewSetting(ConfigConstant.AUTH_TYPE_KEY, initStudioReq.getAuthType().name(), configCache);
                ldapComponent.addLdapConfig(ldapSettingReq, configCache);
                log.debug("add new ldap config success.");

            } else if (initStudioReq.getAuthType() == InitStudioReq.AuthType.studio) {
                log.debug("init studio config.");
                settingComponent.addNewSetting(ConfigConstant.AUTH_TYPE_KEY, initStudioReq.getAuthType().name(), configCache);
            } else if (initStudioReq.getAuthType() == InitStudioReq.AuthType.idaas) {
                log.debug("init idaas config.");
                // Check whether the idaas request information is complete
                log.debug("Check IdaasSettingReq info.");
                IdaasSettingReq idaasSettingReq = initStudioReq.getIdaasSetting();
                checkRequestBody(idaasSettingReq == null);
                checkRequestBody(idaasSettingReq.hasEmptyField());

                // Check idaas connectivity
                boolean isConnection = idaasComponent.checkIdaasConnection(idaasSettingReq);
                if (!isConnection) {
                    throw new IdaasConnectionException();
                }

                log.debug("add new idaas config.");
                settingComponent.addNewSetting(ConfigConstant.AUTH_TYPE_KEY, initStudioReq.getAuthType().name(), configCache);
                idaasComponent.addIdaasConfig(idaasSettingReq, configCache);
                log.debug("add new idaas config success.");

            }  else {
                throw new BadRequestException("不支持此认证方式.");
            }
        } catch (Exception e) {
            log.error("Write auth type config error, delete auth type from database.");
            settingComponent.deleteSetting(ConfigConstant.AUTH_TYPE_KEY);
            throw e;
        }

        ConfigCache.writeConfigs(configCache);
    }

    /**
     * Initialize configuration information at service startup
     * If it has been initialized and has not been modified, the configuration will not be changed
     * If the configuration information exists in the database, it is updated to the cache
     */
    @Transactional
    public void initConfig() throws Exception {
        log.debug("init config.");

        // Store cache information
        Map<String, String> configCache = new HashMap<>();

        // Write configuration items that can read environment variables to the database and cache
        for (String key : ConfigConstant.PUBLIC_CONFIGS.keySet()) {
            ConfigItem configItem = ConfigConstant.PUBLIC_CONFIGS.get(key);

            // Read the environment variable and judge whether the user has set new configuration information
            String envValue = System.getenv(configItem.getEnvName());
            if (StringUtils.isEmpty(envValue)) {
                // If the environment variable is not configured, it will be configured according to whether the
                // configuration information already exists. If it does not exist, the default value will be used
                if (key.equals(ConfigConstant.SITE_URL_KEY)) {
                    // The service address information does not need to be configured here
                    continue;
                } else {
                    SettingEntity entity = settingComponent.readSetting(key);
                    if (entity == null) {
                        settingComponent.addNewSetting(key, configItem.getDefaultValue(), configCache);
                    }
                }
            } else {
                // If the environment variable is configured with new information,
                // it indicates that the user wants to modify the new configuration
                settingComponent.addNewSetting(key, envValue, configCache);
            }
        }

        // Write deploy-type to database and cache
        settingComponent.addNewSetting(ConfigConstant.DEPLOY_TYPE,
                environment.getProperty(PropertyDefine.DEPLOY_TYPE_PROPERTY), configCache);

        // TODO:The front-end is not implemented. First, the user system is specified as
        //  the application's own system by default
        settingComponent.addNewSetting(ConfigConstant.AUTH_TYPE_KEY,
                InitStudioReq.AuthType.studio.name(), configCache);

        ConfigCache.writeConfigs(configCache);
        log.debug("init config end.");
    }

    /**
     * The space administrator obtains the configuration items of the current
     * space and the configuration items common to all spaces
     * @param userId
     * @return
     * @throws Exception
     */
    public List<SettingItem> getAllConfig(int userId) throws Exception {
        log.debug("user {} get all config info.", userId);
        int clusterId = clusterUserComponent.getClusterIdByUserId(userId);

        List<SettingItem> settingItems = getAllPublicConfig();

        for (String key : ConfigConstant.ALL_ADMIN_CONFIGS.keySet()) {
            ConfigItem item = ConfigConstant.ALL_ADMIN_CONFIGS.get(key);
            StudioSettingEntity studioSettingEntity = settingComponent.readAdminSetting(clusterId, key);
            SettingItem settingItem = item.transAdminSettingToModel(studioSettingEntity);
            settingItems.add(settingItem);
        }

        log.debug("get all config info.");

        return settingItems;
    }

    /**
     * Super administrator can only view all public configuration items
     * @return
     * @throws Exception
     */
    public List<SettingItem> getAllPublicConfig() {
        log.debug("super admin get all common config info.");
        List<SettingItem> settingItems = new ArrayList<>();

        for (String key : ConfigConstant.ALL_PUBLIC_CONFIGS.keySet()) {
            ConfigItem item = ConfigConstant.ALL_PUBLIC_CONFIGS.get(key);
            SettingEntity entity = settingComponent.readSetting(key);
            SettingItem settingItem = item.transSettingToModel(entity);
            settingItems.add(settingItem);
        }

        return settingItems;
    }

    /**
     * Super administrator can only view all public configuration items
     * @param key
     * @return
     * @throws Exception
     */
    public SettingItem getConfigByKey(String key) throws Exception {
        if (ConfigConstant.ALL_PUBLIC_CONFIGS.keySet().contains(key)) {
            log.debug("public config key {}.", key);
            SettingEntity entity = settingComponent.readSetting(key);
            ConfigItem item = ConfigConstant.ALL_PUBLIC_CONFIGS.get(key);
            SettingItem settingItem = item.transSettingToModel(entity);
            return settingItem;
        }

        log.error("Input key {} error.", key);
        throw new BadRequestException("Configuration key does not exist.");
    }

    /**
     * The space administrator obtains the configuration information common to the current space and all spaces
     * @param userId
     * @param key
     * @return
     * @throws Exception
     */
    public SettingItem getConfigByKey(String key, int userId) throws Exception {
        log.debug("User {} get config by key {}.", userId, key);
        try {
            return getConfigByKey(key);
        } catch (Exception e) {
            log.debug("The config key {} not common config.", key);
            int clusterId = clusterUserComponent.getClusterIdByUserId(userId);
            if (ConfigConstant.ALL_ADMIN_CONFIGS.keySet().contains(key)) {
                StudioSettingEntity entity = settingComponent.readAdminSetting(clusterId, key);
                ConfigItem item = ConfigConstant.ALL_ADMIN_CONFIGS.get(key);
                return item.transAdminSettingToModel(entity);
            }
        }

        log.error("Input key {} error.", key);
        throw new BadRequestException("Configuration key does not exist.");
    }

    /**
     * The space administrator can only modify the configuration items of the current space
     * @param key
     * @param userId
     * @param updateReq
     * @throws Exception
     */
    @Transactional
    public SettingItem amdinUpdateConfigByKey(String key, int userId, ConfigUpdateReq updateReq) throws Exception {
        log.debug("User {} update config by key {}.", userId, key);
        checkRequestBody(updateReq.hasEmptyField());
        int clusterId = clusterUserComponent.getClusterIdByUserId(userId);

        if (ConfigConstant.ALL_ADMIN_CONFIGS.keySet().contains(key)) {
            ConfigItem configItem = ConfigConstant.ALL_ADMIN_CONFIGS.get(key);
            String value = updateReq.getValue().toString();

            StudioSettingEntity oldEntity = settingComponent.readAdminSetting(clusterId, key);
            StudioSettingEntity newEntity = settingComponent.addNewAdminSetting(clusterId, key, value);

            SettingItem item = configItem.transAdminSettingToModel(newEntity);
            if (oldEntity != null) {
                item.setOriginalValue(oldEntity.getValue());
            }
            return item;
        } else {
            log.error("Input key {} error.", key);
            throw new BadRequestException("Configuration key does not exist.");
        }
    }

    /**
     * Super administrator can only modify public configuration items
     * @param key
     * @param updateReq
     * @throws Exception
     */
    @Transactional
    public SettingItem superUpdateConfigByKey(String key, ConfigUpdateReq updateReq) throws Exception {
        log.debug("Super User update config by key {}.", key);
        checkRequestBody(updateReq.hasEmptyField());
        if (ConfigConstant.ALL_PUBLIC_CONFIGS.keySet().contains(key)) {
            log.debug("public config key.");
            ConfigItem configItem = ConfigConstant.ALL_PUBLIC_CONFIGS.get(key);
            String value;
            if (key.equals(ConfigConstant.CUSTOM_FORMATTING_KEY)) {
                value = JSON.toJSONString(updateReq.getValue());
            } else {
                value = updateReq.getValue().toString();
            }
            SettingEntity oldEntity = settingComponent.readSetting(key);
            SettingEntity newEntity = settingComponent.addNewSetting(key, value, ConfigCache.configCache);
            SettingItem item = configItem.transSettingToModel(newEntity);
            if (oldEntity != null) {
                item.setOriginalValue(oldEntity.getValue());
            }
            return item;
        } else {
            log.error("Input key {} error.", key);
            throw new BadRequestException("Configuration key does not exist.");
        }
    }

}
