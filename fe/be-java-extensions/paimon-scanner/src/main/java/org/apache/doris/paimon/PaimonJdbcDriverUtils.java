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

package org.apache.doris.paimon;

import org.apache.doris.common.jdbc.JdbcDriverUtils;

import java.util.Map;

final class PaimonJdbcDriverUtils {
    static final String PAIMON_JDBC_DRIVER_URL = "paimon.jdbc.driver_url";
    static final String PAIMON_JDBC_DRIVER_CLASS = "paimon.jdbc.driver_class";
    static final String JDBC_DRIVER_URL = "jdbc.driver_url";
    static final String JDBC_DRIVER_CLASS = "jdbc.driver_class";

    private PaimonJdbcDriverUtils() {
    }

    static void registerDriverIfNeeded(Map<String, String> params, ClassLoader parentClassLoader) {
        String driverUrl = firstNonBlank(params.get(PAIMON_JDBC_DRIVER_URL), params.get(JDBC_DRIVER_URL));
        if (driverUrl == null) {
            return;
        }
        String driverClassName = firstNonBlank(params.get(PAIMON_JDBC_DRIVER_CLASS), params.get(JDBC_DRIVER_CLASS));
        if (driverClassName == null) {
            throw new IllegalArgumentException("paimon.jdbc.driver_class or jdbc.driver_class is required when "
                    + "paimon.jdbc.driver_url or jdbc.driver_url is specified");
        }
        registerDriver(driverUrl, driverClassName, parentClassLoader);
    }

    static void registerDriver(String driverUrl, String driverClassName, ClassLoader parentClassLoader) {
        JdbcDriverUtils.registerDriver(driverUrl, driverClassName, parentClassLoader);
    }

    private static String firstNonBlank(String first, String second) {
        if (first != null && !first.trim().isEmpty()) {
            return first;
        }
        if (second != null && !second.trim().isEmpty()) {
            return second;
        }
        return null;
    }
}
