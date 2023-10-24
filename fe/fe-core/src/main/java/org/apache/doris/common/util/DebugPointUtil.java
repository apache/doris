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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Use for manage debug points.
 **/
public class DebugPointUtil {
    private static final Logger LOG = LogManager.getLogger(DebugPointUtil.class);

    private static final Map<String, DebugPoint> debugPoints = new ConcurrentHashMap<>();

    private static class DebugPoint {
        public AtomicInteger executeNum = new AtomicInteger(0);
        public int executeLimit = -1;
        public long expireTime = -1;
    }

    public static boolean isEnable(String debugPointName) {
        if (!Config.enable_debug_points) {
            return false;
        }

        DebugPoint debugPoint = debugPoints.get(debugPointName);
        if (debugPoint == null) {
            return false;
        }

        if ((debugPoint.expireTime > 0 && System.currentTimeMillis() >= debugPoint.expireTime)
                || (debugPoint.executeLimit > 0 && debugPoint.executeNum.incrementAndGet() > debugPoint.executeLimit)) {
            debugPoints.remove(debugPointName);
            return false;
        }

        return true;
    }

    public static void addDebugPoint(String name, int executeLimit, long timeoutSecond) {
        DebugPoint debugPoint = new DebugPoint();
        debugPoint.executeLimit = executeLimit;
        if (timeoutSecond > 0) {
            debugPoint.expireTime = System.currentTimeMillis() + timeoutSecond * 1000;
        }
        debugPoints.put(name, debugPoint);
        LOG.info("add debug point: name={}, execute={}, timeout seconds={}", name, executeLimit, timeoutSecond);
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
