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

package org.apache.doris.catalog;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The changelog configuration of a table, or the default a database gives to the tables created in it.
 *
 * <p>It is stored as the "changelog.*" properties in the property map of the table or the database and
 * rebuilt from that map, like {@link BinlogConfig}. Only a table with changelog enabled stores them.
 * Changelog can only be enabled when a table is created, and so can the before images
 * ("changelog.need_historical_value"); the retention ("changelog.ttl_seconds") can be altered.
 */
public class ChangelogConfig {
    // Internal persistence property, not a user-settable table property. It is the generation of the
    // retention published to BE and Meta Service, which reject a configuration older than the one they have.
    // ATTN(changelog-rename): formerly "__binlog_config_version"
    public static final String CONFIG_VERSION = "__changelog_config_version";

    public static final long TTL_SECONDS = 86400L; // 1 day

    @SerializedName("enable")
    private boolean enable;

    @SerializedName("ttlSeconds")
    private long ttlSeconds;

    @SerializedName("needHistoricalValue")
    private boolean needHistoricalValue;

    @SerializedName("configVersion")
    private long configVersion;

    public ChangelogConfig(boolean enable, long ttlSeconds, boolean needHistoricalValue) {
        this.enable = enable;
        this.ttlSeconds = ttlSeconds;
        this.needHistoricalValue = needHistoricalValue;
    }

    public ChangelogConfig(ChangelogConfig config) {
        this(config.enable, config.ttlSeconds, config.needHistoricalValue);
        configVersion = config.configVersion;
    }

    public ChangelogConfig() {
        this(false, TTL_SECONDS, false);
    }

    /**
     * Overrides this configuration with the "changelog.*" properties. The properties come from a statement or
     * a database, so the internal generation is never read from them.
     */
    public void mergeFromProperties(Map<String, String> properties) {
        if (properties == null) {
            return;
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_CHANGELOG_ENABLE)) {
            enable = Boolean.parseBoolean(properties.get(PropertyAnalyzer.PROPERTIES_CHANGELOG_ENABLE));
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_CHANGELOG_TTL_SECONDS)) {
            ttlSeconds = Long.parseLong(properties.get(PropertyAnalyzer.PROPERTIES_CHANGELOG_TTL_SECONDS));
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_CHANGELOG_NEED_HISTORICAL_VALUE)) {
            needHistoricalValue = Boolean.parseBoolean(
                    properties.get(PropertyAnalyzer.PROPERTIES_CHANGELOG_NEED_HISTORICAL_VALUE));
        }
    }

    /**
     * Applies the "changelog.*" properties of an ALTER TABLE. Changelog and its before images can only be
     * chosen when the table is created.
     */
    public void alterFromProperties(Map<String, String> properties) throws AnalysisException {
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_CHANGELOG_ENABLE)) {
            boolean newEnable = Boolean.parseBoolean(properties.get(PropertyAnalyzer.PROPERTIES_CHANGELOG_ENABLE));
            if (newEnable != enable) {
                throw new AnalysisException("not support change " + PropertyAnalyzer.PROPERTIES_CHANGELOG_ENABLE
                        + " from " + enable + " to " + newEnable);
            }
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_CHANGELOG_NEED_HISTORICAL_VALUE)) {
            boolean newNeedHistoricalValue = Boolean.parseBoolean(
                    properties.get(PropertyAnalyzer.PROPERTIES_CHANGELOG_NEED_HISTORICAL_VALUE));
            if (newNeedHistoricalValue != needHistoricalValue) {
                throw new AnalysisException("not support change "
                        + PropertyAnalyzer.PROPERTIES_CHANGELOG_NEED_HISTORICAL_VALUE
                        + " from " + needHistoricalValue + " to " + newNeedHistoricalValue);
            }
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_CHANGELOG_TTL_SECONDS)) {
            applyExplicitTtl(Long.parseLong(properties.get(PropertyAnalyzer.PROPERTIES_CHANGELOG_TTL_SECONDS)));
        }
    }

    // A changelog always expires: its retention must be positive once changelog is enabled. A database
    // keeps any retention as a default while it does not enable changelog.
    public void applyExplicitTtl(long ttlSeconds) throws AnalysisException {
        if (enable && ttlSeconds <= 0) {
            throw new AnalysisException(PropertyAnalyzer.PROPERTIES_CHANGELOG_TTL_SECONDS + " must be greater than 0");
        }
        this.ttlSeconds = ttlSeconds;
    }

    public boolean isEnable() {
        return enable;
    }

    public long getTtlSeconds() {
        return ttlSeconds;
    }

    public void setTtlSeconds(long ttlSeconds) {
        this.ttlSeconds = ttlSeconds;
    }

    public boolean isTtlEnabled() {
        return enable && ttlSeconds > 0;
    }

    public boolean getNeedHistoricalValue() {
        return needHistoricalValue;
    }

    public long getConfigVersion() {
        return configVersion;
    }

    public void setConfigVersion(long configVersion) {
        this.configVersion = configVersion;
    }

    /**
     * The properties a table with changelog stores. A table without changelog stores none of them. The
     * internal generation is included once a retention has been published, so a new table does not have it.
     */
    public Map<String, String> toProperties() {
        Map<String, String> properties = new HashMap<>();
        if (configVersion != 0) {
            properties.put(CONFIG_VERSION, String.valueOf(configVersion));
        }
        properties.put(PropertyAnalyzer.PROPERTIES_CHANGELOG_ENABLE, String.valueOf(enable));
        properties.put(PropertyAnalyzer.PROPERTIES_CHANGELOG_TTL_SECONDS, String.valueOf(ttlSeconds));
        properties.put(PropertyAnalyzer.PROPERTIES_CHANGELOG_NEED_HISTORICAL_VALUE,
                String.valueOf(needHistoricalValue));
        return properties;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ChangelogConfig)) {
            return false;
        }
        ChangelogConfig other = (ChangelogConfig) obj;
        return enable == other.enable
                && ttlSeconds == other.ttlSeconds
                && needHistoricalValue == other.needHistoricalValue
                && configVersion == other.configVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(enable, ttlSeconds, needHistoricalValue, configVersion);
    }

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }

    public void appendToShowCreateTable(StringBuilder sb) {
        sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_CHANGELOG_ENABLE).append("\" = \"")
                .append(enable).append("\"");
        sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_CHANGELOG_TTL_SECONDS).append("\" = \"")
                .append(ttlSeconds).append("\"");
        sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_CHANGELOG_NEED_HISTORICAL_VALUE).append("\" = \"")
                .append(needHistoricalValue).append("\"");
    }

    public static ChangelogConfig fromProperties(Map<String, String> properties) {
        ChangelogConfig changelogConfig = new ChangelogConfig();
        changelogConfig.mergeFromProperties(properties);
        return changelogConfig;
    }
}
