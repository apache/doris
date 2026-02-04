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

import org.apache.commons.lang3.math.NumberUtils;

import java.util.Map;
import java.util.OptionalLong;

/**
 * Common cache specification for external metadata caches.
 *
 * <p>Semantics:
 * <ul>
 *   <li>enable=false disables cache</li>
 *   <li>ttlSecond=0 disables cache, ttlSecond=-1 means no expiration</li>
 *   <li>capacity=0 disables cache; capacity is count-based</li>
 * </ul>
 */
public final class CacheSpec {
    private final boolean enable;
    private final long ttlSecond;
    private final long capacity;

    private CacheSpec(boolean enable, long ttlSecond, long capacity) {
        this.enable = enable;
        this.ttlSecond = ttlSecond;
        this.capacity = capacity;
    }

    public static CacheSpec fromProperties(Map<String, String> properties,
            String enableKey, boolean defaultEnable,
            String ttlKey, long defaultTtlSecond,
            String capacityKey, long defaultCapacity) {
        boolean enable = getBooleanProperty(properties, enableKey, defaultEnable);
        long ttlSecond = getLongProperty(properties, ttlKey, defaultTtlSecond);
        long capacity = getLongProperty(properties, capacityKey, defaultCapacity);
        if (!isCacheEnabled(enable, ttlSecond, capacity)) {
            capacity = 0;
        }
        return new CacheSpec(enable, ttlSecond, capacity);
    }

    public static void checkBooleanProperty(String value, String key) throws DdlException {
        if (value == null) {
            return;
        }
        if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
            throw new DdlException("The parameter " + key + " is wrong, value is " + value);
        }
    }

    public static void checkLongProperty(String value, long minValue, String key) throws DdlException {
        if (value == null) {
            return;
        }
        long parsed;
        try {
            parsed = Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new DdlException("The parameter " + key + " is wrong, value is " + value);
        }
        if (parsed < minValue) {
            throw new DdlException("The parameter " + key + " is wrong, value is " + value);
        }
    }

    public static boolean isCacheEnabled(boolean enable, long ttlSecond, long capacity) {
        return enable && ttlSecond != 0 && capacity != 0;
    }

    /**
     * Convert ttlSecond to OptionalLong for CacheFactory.
     * ttlSecond=-1 means no expiration; ttlSecond=0 disables cache.
     */
    public static OptionalLong toExpireAfterAccess(long ttlSecond) {
        if (ttlSecond == -1) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(Math.max(ttlSecond, 0));
    }

    private static boolean getBooleanProperty(Map<String, String> properties, String key, boolean defaultValue) {
        String value = properties.get(key);
        if (value == null) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
    }

    private static long getLongProperty(Map<String, String> properties, String key, long defaultValue) {
        String value = properties.get(key);
        if (value == null) {
            return defaultValue;
        }
        return NumberUtils.toLong(value, defaultValue);
    }

    public boolean isEnable() {
        return enable;
    }

    public long getTtlSecond() {
        return ttlSecond;
    }

    public long getCapacity() {
        return capacity;
    }
}
