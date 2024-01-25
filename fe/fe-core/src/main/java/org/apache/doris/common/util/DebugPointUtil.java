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

package org.apache.doris.common.util;

import org.apache.doris.common.Config;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Use for manage debug points.
 *
 * usage example can see DebugPointUtilTest.java
 *
 **/
public class DebugPointUtil {
    private static final Logger LOG = LogManager.getLogger(DebugPointUtil.class);

    private static final Map<String, DebugPoint> debugPoints = new ConcurrentHashMap<>();

    public static class DebugPoint {
        public AtomicInteger executeNum = new AtomicInteger(0);
        public int executeLimit = -1;
        public long expireTime = -1;

        // params
        public Map<String, String> params = Maps.newHashMap();

        public <E> E param(String key, E defaultValue) {
            Preconditions.checkState(defaultValue != null);

            String value = params.get(key);
            if (value == null) {
                return defaultValue;
            }
            if (defaultValue instanceof Boolean) {
                return (E) Boolean.valueOf(value);
            }
            if (defaultValue instanceof Byte) {
                return (E) Byte.valueOf(value);
            }
            if (defaultValue instanceof Character) {
                Preconditions.checkState(value.length() == 1);
                return (E) Character.valueOf(value.charAt(0));
            }
            if (defaultValue instanceof Short) {
                return (E) Short.valueOf(value);
            }
            if (defaultValue instanceof Integer) {
                return (E) Integer.valueOf(value);
            }
            if (defaultValue instanceof Long) {
                return (E) Long.valueOf(value);
            }
            if (defaultValue instanceof Float) {
                return (E) Float.valueOf(value);
            }
            if (defaultValue instanceof Double) {
                return (E) Double.valueOf(value);
            }
            if (defaultValue instanceof String) {
                return (E) value;
            }

            Preconditions.checkState(false, "Can not convert with default value=" + defaultValue);

            return defaultValue;
        }
    }

    public static boolean isEnable(String debugPointName) {
        return getDebugPoint(debugPointName) != null;
    }

    public static DebugPoint getDebugPoint(String debugPointName) {
        if (!Config.enable_debug_points) {
            return null;
        }

        DebugPoint debugPoint = debugPoints.get(debugPointName);
        if (debugPoint == null) {
            return null;
        }

        if ((debugPoint.expireTime > 0 && System.currentTimeMillis() >= debugPoint.expireTime)
                || (debugPoint.executeLimit > 0 && debugPoint.executeNum.incrementAndGet() > debugPoint.executeLimit)) {
            debugPoints.remove(debugPointName);
            return null;
        }

        return debugPoint;
    }

    // if not enable debug point or its params not contains `key`, then return `defaultValue`
    // url: /api/debug_point/add/name?k1=v1&k2=v2&...
    public static <E> E getDebugParamOrDefault(String debugPointName, String key, E defaultValue) {
        DebugPoint debugPoint = getDebugPoint(debugPointName);

        return debugPoint != null ? debugPoint.param(key, defaultValue) : defaultValue;
    }

    // url: /api/debug_point/add/name?value=v
    public static <E> E getDebugParamOrDefault(String debugPointName, E defaultValue) {
        return getDebugParamOrDefault(debugPointName, "value", defaultValue);
    }

    public static void addDebugPoint(String name, DebugPoint debugPoint) {
        debugPoints.put(name, debugPoint);
        LOG.info("add debug point: name={}, params={}", name, debugPoint.params);
    }

    public static void addDebugPoint(String name) {
        addDebugPoint(name, new DebugPoint());
    }

    public static <E> void addDebugPointWithValue(String name, E value) {
        DebugPoint debugPoint = new DebugPoint();
        debugPoint.params.put("value", String.format("%s", value));
        addDebugPoint(name, debugPoint);
    }

    public static void removeDebugPoint(String name) {
        DebugPoint debugPoint = debugPoints.remove(name);
        LOG.info("remove debug point: name={}, exists={}", name, debugPoint != null);
    }

    public static void clearDebugPoints() {
        debugPoints.clear();
        LOG.info("clear debug points");
    }
}
