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

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description：Configuration item cache management,
 *
 * If the user has no configuration item, this field is empty;
 * If the user adds or modifies a configuration item, the cache content should be modified;
 * If the user reads the cache, read the contents of the cache first;
 * When the system restarts, the cache should be updated again;
 * The cache is theoretically consistent with the contents of the database, but if you manually change the database,
 * there will be inconsistencies. You need to empty the cache
 */
@Slf4j
public class ConfigCache {

    private ConfigCache() {
        throw new UnsupportedOperationException();
    }

    // Data structure stored in global configuration item cache
    public static Map<String, String> configCache = new HashMap<>();

    /**
     * Add a new cache configuration item
     * @param key
     * @param value
     */
    public static void writeConfig(String key, String value) {
        log.debug("Write cache config {} value {}.", key, value);
        configCache.put(key, value);
    }

    /**
     * Batch add new cache configuration items
     * @param configs
     */
    public static void writeConfigs(Map<String, String> configs) {
        log.debug("Write cache configs");
        configCache.putAll(configs);
    }

    /**
     * Read cache configuration item
     * @param key
     * @return
     */
    public static String readConfig(String key) {
        log.debug("Get cache config {}.", key);
        return configCache.get(key);
    }

    /**
     * Delete cache configuration item
     * @param key
     */
    public static void deleteConfig(String key) {
        log.debug("Delete cache config {}.", key);
        configCache.remove(key);
    }

    // The data structure stored in the user space configuration item cache. The key is the space ID
    public static Map<Integer, Map<String, String>> adminConfigCache = new HashMap<>();

    /**
     * Add in space cache configuration item
     * @param clusterId
     * @param key
     * @param value
     */
    public static void writeAdminConfig(int clusterId, String key, String value) {
        log.debug("Write cache config {} value {} for cluster {}.", key, value, clusterId);
        Map<String, String> clusterConfig = adminConfigCache.get(clusterId);
        if (clusterConfig == null) {
            clusterConfig = new HashMap<>();
        }

        clusterConfig.put(key, value);
        adminConfigCache.put(clusterId, clusterConfig);
    }

    /**
     * Read cache configuration items in space
     * @param clusterId
     * @param key
     * @return
     */
    public static String readAdminConfig(int clusterId, String key) {
        log.debug("Get cache config {} from cluster {}.", key, clusterId);
        Map<String, String> clusterConfig = adminConfigCache.get(clusterId);
        if (clusterConfig == null) {
            return null;
        }

        return clusterConfig.get(key);
    }

    /**
     * Delete cache configuration item in space
     * @param clusterId
     * @param key
     */
    public static void deleteAdminConfig(int clusterId, String key) {
        log.debug("Delete cluster {} admin config {}", clusterId, key);
        Map<String, String> clusterConfig = adminConfigCache.get(clusterId);
        if (clusterConfig == null) {
            return;
        }
        clusterConfig.remove(key);
        adminConfigCache.put(clusterId, clusterConfig);
    }

    /**
     * Delete all cache configuration items in the space
     * @param clusterId
     */
    public static void deleteAdminConfig(int clusterId) {
        log.debug("Delete cluster {} all admin config", clusterId);
        adminConfigCache.remove(clusterId);
    }

}
