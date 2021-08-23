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

package org.apache.doris.stack.model.response.config;

import com.alibaba.fastjson.annotation.JSONField;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class SettingItem {

    private String key;

    private Object value;

    private String defaultValue;

    private String description;

    private boolean isEnvSetting;

    private String envName;

    private int placeholder;

    private boolean allowValueCollection;

    private String originalValue;

    public SettingItem(String key, String description, boolean isEnvSetting, String envName, String defaultValue) {
        this.key = key;
        this.description = description;
        this.isEnvSetting = isEnvSetting;
        this.envName = envName;
        this.defaultValue = defaultValue;
    }

    @JSONField(name = "default")
    @JsonProperty("default")
    public String getDefaultValue() {
        return defaultValue;
    }

    @JSONField(name = "default")
    @JsonProperty("default")
    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    @JSONField(name = "is_env_setting")
    @JsonProperty("is_env_setting")
    public boolean isEnvSetting() {
        return isEnvSetting;
    }

    @JSONField(name = "is_env_setting")
    @JsonProperty("is_env_setting")
    public void setEnvSetting(boolean envSetting) {
        isEnvSetting = envSetting;
    }

    @JSONField(name = "env_name")
    @JsonProperty("env_name")
    public String getEnvName() {
        return envName;
    }

    @JSONField(name = "env_name")
    @JsonProperty("env_name")
    public void setEnvName(String envName) {
        this.envName = envName;
    }
}
