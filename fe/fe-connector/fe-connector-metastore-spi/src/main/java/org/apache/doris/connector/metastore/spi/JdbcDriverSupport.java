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
     * returned unchanged; otherwise it is treated as a bare jar file name and resolved against
     * {@code driversDir} (defaulting to {@code $DORIS_HOME/plugins/jdbc_drivers}). Mirrors the minimal
     * {@code JdbcResource.getFullDriverUrl} resolution (no file-existence / legacy old-dir /
     * cloud-download handling), so the FE driver registration and the BE-bound options resolve a given
     * {@code driver_url} identically.
     *
     * <p>Both directories are passed in rather than read from the engine environment here: which
     * settings file a drivers directory comes from is the calling connector's business (its own
     * {@code <name>.conf} first, then fe.conf's {@code jdbc_drivers_dir}), and this module is shared by
     * connectors whose conf files differ. Same shape as
     * {@code AbstractHmsMetaStoreProperties}, which likewise takes its default as a parameter.
     *
     * @param driverUrl  the raw driver_url; must be non-null and non-blank (the caller's responsibility)
     * @param driversDir directory a bare jar name resolves under; blank falls back to the default below
     * @param dorisHome  the FE install root, used only to build that default; blank means "."
     */
    public static String resolveDriverUrl(String driverUrl, String driversDir, String dorisHome) {
        if (driverUrl.contains("://")) {
            return driverUrl;
        }
        if (driverUrl.startsWith("/")) {
            // Absolute path, no scheme: legacy returns it as-is (no driversDir prepend).
            return driverUrl;
        }
        String resolvedDir = driversDir;
        if (StringUtils.isBlank(resolvedDir)) {
            resolvedDir = (StringUtils.isBlank(dorisHome) ? "." : dorisHome) + "/plugins/jdbc_drivers";
        }
        return "file://" + resolvedDir + "/" + driverUrl;
    }
}
