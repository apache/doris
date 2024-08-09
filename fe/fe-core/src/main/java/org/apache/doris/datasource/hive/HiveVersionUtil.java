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

package org.apache.doris.datasource.hive;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * For getting a compatible version of hive
 * if user specified the version, it will parse it and return the compatible HiveVersion,
 * otherwise, use DEFAULT_HIVE_VERSION
 */
public class HiveVersionUtil {
    private static final Logger LOG = LogManager.getLogger(HiveVersionUtil.class);

    private static final HiveVersion DEFAULT_HIVE_VERSION = HiveVersion.V2_3;

    /**
     * HiveVersion
     */
    public enum HiveVersion {
        V1_0,   // [1.0.0 - 1.2.2]
        V2_0,   // [2.0.0 - 2.2.0]
        V2_3,   // [2.3.0 - 2.3.6]
        V3_0    // [3.0.0 - 3.1.2]
    }

    /**
     * get the compatible HiveVersion
     *
     * @param version the version string
     * @return HiveVersion
     */
    public static HiveVersion getVersion(String version) {
        if (Strings.isNullOrEmpty(version)) {
            return DEFAULT_HIVE_VERSION;
        }
        String[] parts = version.split("\\.");
        if (parts.length < 2) {
            LOG.warn("invalid hive version: " + version);
            return DEFAULT_HIVE_VERSION;
        }
        try {
            int major = Integer.parseInt(parts[0]);
            int minor = Integer.parseInt(parts[1]);
            if (major == 1) {
                return HiveVersion.V1_0;
            } else if (major == 2) {
                if (minor >= 0 && minor <= 2) {
                    return HiveVersion.V1_0;
                } else if (minor >= 3) {
                    return HiveVersion.V2_3;
                } else {
                    LOG.warn("invalid hive version: " + version);
                    return DEFAULT_HIVE_VERSION;
                }
            } else if (major >= 3) {
                return HiveVersion.V3_0;
            } else {
                LOG.warn("invalid hive version: " + version);
                return DEFAULT_HIVE_VERSION;
            }
        } catch (NumberFormatException e) {
            LOG.warn("invalid hive version: " + version);
            return DEFAULT_HIVE_VERSION;
        }
    }
}
