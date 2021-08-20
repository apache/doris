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

import org.apache.doris.stack.model.response.config.SettingItem;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Component
@Slf4j
public class SettingComponent {

    @Autowired
    private SettingRepository settingRepository;

    @Autowired
    private StudioSettingRepository studioSettingRepository;

    /**
     * Add or update a new global configuration item, store it in the database, and write it to the cache
     * @param key
     * @param value
     * @param configCache
     */
    public SettingEntity addNewSetting(String key, String value, Map<String, String> configCache) throws Exception {
        log.debug("Add a new common setting key {} value {}.", key, value);
        if (ConfigConstant.ALL_PUBLIC_CONFIGS.containsKey(key)) {
            SettingEntity settingItem = new SettingEntity(key, value);
            settingRepository.save(settingItem);
            configCache.put(key, value);
            return settingItem;
        } else {
            log.error("Input key {} error.", key);
            throw new Exception("Configuration common key does not exist.");
        }
    }

    /**
     * Add or update a new global configuration item, store it in the database, and write it to the cache
     * @param key
     * @param value
     */
    public SettingEntity addNewSetting(String key, String value) throws Exception {
        log.debug("Add a new common setting key {} value {}.", key, value);
        if (ConfigConstant.ALL_PUBLIC_CONFIGS.containsKey(key)) {
            SettingEntity settingItem = new SettingEntity(key, value);
            settingRepository.save(settingItem);
            ConfigCache.writeConfig(key, value);
            return settingItem;
        } else {
            log.error("Input key {} error.", key);
            throw new Exception("Configuration common key does not exist.");
        }
    }

    /**
     * Read a global configuration item
     * Read the cache before reading the contents
     * Returns NULL if it does not exist
     *
     * @param key
     * @return
     */
    public SettingEntity readSetting(String key) {
        log.debug("Read a common setting key {}.", key);
        String value = ConfigCache.readConfig(key);

        if (value == null) {
            log.debug("Cache not exist.");
            Optional<SettingEntity> settingEntity = settingRepository.findById(key);
            if (settingEntity.equals(Optional.empty())) {
                log.debug("The setting not exist");
                return null;
            } else {
                log.debug("The setting exist");
                value = settingEntity.get().getValue();
                ConfigCache.writeConfig(key, value);
            }
        }
        return new SettingEntity(key, value);
    }

    /**
     * Delete a global configuration item
     * @param key
     */
    public void deleteSetting(String key) {
        log.debug("Delete a common setting key {}.", key);
        settingRepository.deleteById(key);
        ConfigCache.deleteConfig(key);
    }

    /**
     * Read the mailbox configuration information and return to the configuration list.
     * If the stored configuration item is empty, the default value is returned directly
     * @return
     */
    public List<SettingItem> getUserEmailConfig() {
        List<SettingItem> emailSettings = new ArrayList<>();

        for (String key : ConfigConstant.EMAIL_CONFIGS.keySet()) {
            ConfigItem item = ConfigConstant.EMAIL_CONFIGS.get(key);
            SettingEntity entity = readSetting(key);
            SettingItem settingItem = item.transSettingToModel(entity);
            emailSettings.add(settingItem);
        }

        return emailSettings;
    }

    /**
     * Add a new space configuration item, store it in the database, and write it to the cache
     * @param clusterId
     * @param key
     * @param value
     */
    public StudioSettingEntity addNewAdminSetting(int clusterId, String key, String value) throws Exception {
        log.debug("Add a new admin setting key {} value {} for cluster {}.", key, value, clusterId);
        if (ConfigConstant.ALL_ADMIN_CONFIGS.containsKey(key)) {
            StudioSettingEntity studioSettingEntity = new StudioSettingEntity(key, clusterId, value);
            studioSettingRepository.save(studioSettingEntity);
            ConfigCache.writeAdminConfig(clusterId, key, value);
            return studioSettingEntity;
        } else {
            log.error("Input key {} error.", key);
            throw new Exception("Configuration admin key does not exist.");
        }
    }

    /**
     * Read a space configuration item
     * Read the cache before reading the contents
     * Returns NULL if it does not exist
     *
     * @param key
     * @return
     */
    public StudioSettingEntity readAdminSetting(int clusterId, String key) {
        log.debug("Read a admin setting key {} from cluster {}.", key, clusterId);
        String value = ConfigCache.readAdminConfig(clusterId, key);

        if (value == null) {
            log.debug("Cache not exist.");
            StudioSettingEntityPk entityKey = new StudioSettingEntityPk(key, clusterId);
            Optional<StudioSettingEntity> settingEntity = studioSettingRepository.findById(entityKey);
            if (settingEntity.equals(Optional.empty())) {
                log.debug("The setting not exist");
                return null;
            } else {
                log.debug("The setting exist");
                value = settingEntity.get().getValue();
                ConfigCache.writeAdminConfig(clusterId, key, value);
            }
        }

        return new StudioSettingEntity(key, clusterId, value);
    }

    public void deleteAdminSetting(int clusterId, String key) {
        log.debug("Delete a admin setting key {} form cluster {}.", key, clusterId);
        StudioSettingEntityPk settingEntityPk = new StudioSettingEntityPk(key, clusterId);
        studioSettingRepository.deleteById(settingEntityPk);
        ConfigCache.deleteAdminConfig(clusterId, key);
    }

    public void deleteAdminSetting(int clusterId) {
        log.debug("Delete all admin settings form cluster {}.", clusterId);
        studioSettingRepository.deleteByClusterId(clusterId);
        ConfigCache.deleteAdminConfig(clusterId);
    }

    /**
     * Read a space configuration item
     * Read the cache before reading the contents
     * If it does not exist, the default value of the configuration item is returned
     *
     * @param key
     * @return
     */
    public String readAdminSettingOrDefault(int clusterId, String key) {
        log.debug("Read a admin setting key {} from cluster {}.", key, clusterId);

        StudioSettingEntity valueEntity = readAdminSetting(clusterId, key);
        String value;
        if (valueEntity == null) {
            log.debug("The setting not exist,read default value.");
            ConfigItem item = ConfigConstant.ALL_ADMIN_CONFIGS.get(key);
            value = item.getDefaultValue();
        } else {
            value = valueEntity.getValue();
        }

        return value;
    }

    // enable-public-sharing
    /**
     * Judge whether the current stack service is open for public sharing
     * @return
     */
    public boolean publicSharingEnable() {
        log.debug("Judge whether public sharing enable.");
        SettingEntity enablePublicSharing = readSetting(ConfigConstant.ENABLE_PUBLIC_KEY);
        if (enablePublicSharing == null) {
            return false;
        }
        return Boolean.valueOf(enablePublicSharing.getValue());
    }

    /**
     * Judge whether the mailbox service is enabled for the current stack service
     * @return
     */
    public boolean emailEnable() {
        log.debug("Judge whether email enable.");
        SettingEntity enableEmail = readSetting(ConfigConstant.EMAIL_CONFIGURED_KEY);
        if (enableEmail == null) {
            return false;
        }
        return Boolean.parseBoolean(enableEmail.getValue());
    }

    /**
     * Judge whether the current studio service has its own sample data
     * The engine type must be MySQL and the user has enabled the sample data
     * @return
     */
    public boolean sampleDataEnable() {
        log.debug("Judge whether sample data enable.");
        SettingEntity sampleDataEnable = readSetting(ConfigConstant.SAMPLE_DATA_ENABLE_KEY);
        if (sampleDataEnable == null) {
            return false;
        }
        boolean isEnable = Boolean.parseBoolean(sampleDataEnable.getValue());

        SettingEntity dbType = readSetting(ConfigConstant.DATABASE_TYPE_KEY);
        if (dbType == null) {
            return false;
        }
        return isEnable && dbType.getValue().equals(PropertyDefine.JPA_DATABASE_MYSQL);
    }

}
