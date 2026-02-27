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

package org.apache.doris.datasource.metacache;

import org.apache.doris.common.DdlException;

import java.util.Map;
import java.util.Objects;

/**
 * Unified property key wrapper for one cache module:
 * meta.cache.&lt;engine&gt;.&lt;module&gt;.{enable,ttl-second,capacity}.
 */
public final class UnifiedCacheModuleKey {
    private final String engineType;
    private final String moduleName;

    private UnifiedCacheModuleKey(String engineType, String moduleName) {
        this.engineType = Objects.requireNonNull(engineType, "engineType cannot be null");
        this.moduleName = Objects.requireNonNull(moduleName, "moduleName cannot be null");
    }

    public static UnifiedCacheModuleKey of(String engineType, String moduleName) {
        return new UnifiedCacheModuleKey(engineType, moduleName);
    }

    public String getEnableKey() {
        return getKeyPrefix() + "enable";
    }

    public String getTtlSecondKey() {
        return getKeyPrefix() + "ttl-second";
    }

    public String getCapacityKey() {
        return getKeyPrefix() + "capacity";
    }

    public CacheSpec toCacheSpec(Map<String, String> properties,
            boolean defaultEnable, long defaultTtlSecond, long defaultCapacity) {
        return CacheSpec.fromUnifiedProperties(properties, engineType, moduleName,
                defaultEnable, defaultTtlSecond, defaultCapacity);
    }

    public void checkProperties(Map<String, String> properties) throws DdlException {
        String enableKey = getEnableKey();
        String ttlSecondKey = getTtlSecondKey();
        String capacityKey = getCapacityKey();
        CacheSpec.checkBooleanProperty(properties.get(enableKey), enableKey);
        CacheSpec.checkLongProperty(properties.get(ttlSecondKey), -1L, ttlSecondKey);
        CacheSpec.checkLongProperty(properties.get(capacityKey), 0L, capacityKey);
    }

    public boolean hasAnyUpdatedProperty(Map<String, String> updatedProps) {
        return Objects.nonNull(updatedProps.get(getEnableKey()))
                || Objects.nonNull(updatedProps.get(getTtlSecondKey()))
                || Objects.nonNull(updatedProps.get(getCapacityKey()));
    }

    public static void checkProperties(Map<String, String> properties, Iterable<UnifiedCacheModuleKey> moduleKeys)
            throws DdlException {
        for (UnifiedCacheModuleKey moduleKey : moduleKeys) {
            moduleKey.checkProperties(properties);
        }
    }

    public static boolean hasAnyUpdatedProperty(
            Map<String, String> updatedProps, Iterable<UnifiedCacheModuleKey> moduleKeys) {
        for (UnifiedCacheModuleKey moduleKey : moduleKeys) {
            if (moduleKey.hasAnyUpdatedProperty(updatedProps)) {
                return true;
            }
        }
        return false;
    }

    private String getKeyPrefix() {
        return CacheSpec.getUnifiedModuleKeyPrefix(engineType, moduleName);
    }
}
