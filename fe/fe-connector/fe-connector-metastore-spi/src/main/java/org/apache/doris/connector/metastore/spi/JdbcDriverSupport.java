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

package org.apache.doris.connector.metastore.spi;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * Shared JDBC driver-url resolution. Only the PURE resolver lives here (a function of the raw
 * {@code driver_url} + the engine environment map). The live driver REGISTRATION
 * ({@code DriverManager.registerDriver} + the {@code DriverShim} + the class-loader cache) is a JVM
 * side-effect with no caller until the paimon adapter cuts over (P2-T03), so it is intentionally NOT
 * moved here yet (Rule 2: no speculative dead code).
 */
public final class JdbcDriverSupport {

    private JdbcDriverSupport() {
    }

    /**
     * Resolves a JDBC {@code driver_url} to a full, scheme-bearing URL string. A value already
     * carrying a scheme ({@code "://"}) is used as-is; an absolute path (starting with {@code "/"}) is
     * returned unchanged; otherwise it is treated as a bare jar file name and resolved against the
     * engine's configured {@code jdbc_drivers_dir} (defaulting to
     * {@code $DORIS_HOME/plugins/jdbc_drivers}). Mirrors the minimal {@code JdbcResource.getFullDriverUrl}
     * resolution (no file-existence / legacy old-dir / cloud-download handling), so the FE driver
     * registration and the BE-bound options resolve a given {@code driver_url} identically.
     *
     * @param driverUrl the raw driver_url; must be non-null and non-blank (the caller's responsibility)
     * @param env       the engine environment map (e.g. {@code jdbc_drivers_dir}, {@code doris_home}); never null
     */
    public static String resolveDriverUrl(String driverUrl, Map<String, String> env) {
        if (driverUrl.contains("://")) {
            return driverUrl;
        }
        if (driverUrl.startsWith("/")) {
            // Absolute path, no scheme: legacy returns it as-is (no driversDir prepend).
            return driverUrl;
        }
        String driversDir = env.get("jdbc_drivers_dir");
        if (StringUtils.isBlank(driversDir)) {
            String dorisHome = env.getOrDefault("doris_home", ".");
            driversDir = dorisHome + "/plugins/jdbc_drivers";
        }
        return "file://" + driversDir + "/" + driverUrl;
    }
}
