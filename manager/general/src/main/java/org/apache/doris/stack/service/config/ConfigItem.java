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

import com.alibaba.fastjson.JSONObject;
import org.apache.doris.stack.model.response.config.SettingItem;
import org.apache.doris.stack.entity.SettingEntity;
import org.apache.doris.stack.entity.StudioSettingEntity;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * @Description：The configuration item attribute information and attributes are statically
 * set and cannot be modified separately
 */
@NoArgsConstructor
@AllArgsConstructor
public class ConfigItem {

    private String key;

    private Type type;

    private String defaultValue;

    private String description;

    private boolean sensitive;

    private Visibility visibility;

    // Whether to configure through environment variables. At present,
    // this part of the configuration cannot be configured through environment variables.
    // It needs to be discussed later
    private boolean isEnvSetting;

    private String envName;

    public String getKey() {
        return key;
    }

    public Type getType() {
        return type;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public String getDescription() {
        return description;
    }

    public boolean isSensitive() {
        return sensitive;
    }

    public Visibility getVisibility() {
        return visibility;
    }

    public boolean isEnvSetting() {
        return isEnvSetting;
    }

    public String getEnvName() {
        return envName;
    }

    public enum Visibility {
        PUBLIC, AUTHENTICATED, ADMIN, INTERNAL;
    }

    public enum Type {
        STRING, BOOLEAN, INTEGER, JSON, DOUBLE, TIMESTAMP;
    }

    public ConfigItem(String key, Type type, String description, boolean sensitive, Visibility visibility) {
        this.key = key;
        this.type = type;
        this.description = description;
        this.sensitive = sensitive;
        this.visibility = visibility;
    }

    public ConfigItem(String key, Type type, String description, boolean sensitive, Visibility visibility,
                      String envName) {
        this.key = key;
        this.type = type;
        this.description = description;
        this.sensitive = sensitive;
        this.visibility = visibility;
        this.envName = envName;
    }

    public ConfigItem(String key, Type type, String description, boolean sensitive, Visibility visibility,
                      String envName, String defaultValue) {
        this.key = key;
        this.type = type;
        this.description = description;
        this.sensitive = sensitive;
        this.visibility = visibility;
        this.envName = envName;
        this.defaultValue = defaultValue;
    }

    /**
     * Front end view configuration information conversion
     * @return
     */
    public SettingItem transAdminSettingToModel(StudioSettingEntity entity) {
        String value = null;
        if (entity != null) {
            value = entity.getValue();
        }
        return transToModel(value);
    }

    /**
     * Front end view configuration information conversion
     * @return
     */
    public SettingItem transSettingToModel(SettingEntity entity) {
        String value = null;
        if (entity != null) {
            value = entity.getValue();
        }
        return transToModel(value);
    }

    private SettingItem transToModel(String value) {
        SettingItem settingItem = new SettingItem(key, description, isEnvSetting, envName, defaultValue);
        if (value != null && !sensitive) {
            if (key.equals(ConfigConstant.CUSTOM_FORMATTING_KEY)) {
                settingItem.setValue(JSONObject.parse(value));
            } else {
                settingItem.setValue(value);
            }
        }

        if (visibility == Visibility.PUBLIC) {
            // Only configuration items that are visible to all spaces can be configured
            // through the environment variable
            settingItem.setEnvName(envName);
            settingItem.setEnvSetting(isEnvSetting);
        }

        return settingItem;
    }
}
