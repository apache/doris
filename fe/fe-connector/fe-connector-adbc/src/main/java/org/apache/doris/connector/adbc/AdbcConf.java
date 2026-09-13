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

package org.apache.doris.connector.adbc;

import org.apache.doris.connector.spi.ConnectorConf;
import org.apache.doris.connector.spi.ConnectorContext;

/**
 * This plugin's own settings file, {@code adbc.conf}: the keys, their defaults, and the reads.
 *
 * <p>These are one-per-FE settings, unlike the per-catalog properties in {@link AdbcCatalogProperties}.
 * They have no fe.conf half: this connector is newer than the plugin conf channel, so no deployment ever
 * configured them before {@code adbc.conf} existed, and adding a {@code @ConfField} would tie a key name
 * of this plugin into fe-core for nothing. That is why every read below passes a {@code null} legacy
 * environment key.
 */
public final class AdbcConf {

    /**
     * Directory a bare {@code driver_url} file name resolves under. Defaults to
     * {@code <DORIS_HOME>/plugins/adbc_drivers}, which is where build.sh creates the directory.
     */
    public static final String CONF_DRIVERS_DIR = "drivers_dir";

    /**
     * Semicolon-separated directories a driver may be loaded from; {@code *} allows any. Defaults to
     * {@link #DEFAULT_DRIVER_SECURE_PATH}.
     */
    public static final String CONF_DRIVER_SECURE_PATH = "driver_secure_path";

    /** The subdirectory of DORIS_HOME {@link #CONF_DRIVERS_DIR} defaults to. */
    public static final String DEFAULT_DRIVERS_SUBDIR = "/plugins/adbc_drivers";

    /** Allow any directory, matching the jdbc catalog's default. */
    public static final String DEFAULT_DRIVER_SECURE_PATH = "*";

    /** Engine-wide rather than this connector's setting, so it keeps coming from the environment. */
    public static final String ENV_DORIS_HOME = "doris_home";

    private AdbcConf() {
    }

    /**
     * The directory a bare {@code driver_url} file name resolves under: adbc.conf's
     * {@code drivers_dir}, else {@code <DORIS_HOME>/plugins/adbc_drivers}.
     *
     * <p>The default is computed here rather than declared as an fe.conf {@code @ConfField}, because a
     * key in fe-core is an engine change per connector setting. Null when DORIS_HOME is unknown and the
     * conf file says nothing -- {@link AdbcDriverPathResolver#resolve} then reports the bare name as
     * unresolvable instead of quietly resolving it against the process working directory.
     */
    public static String driversDir(ConnectorContext context) {
        String dorisHome = context.getEnvironment().get(ENV_DORIS_HOME);
        String defaultDir = dorisHome == null ? null : dorisHome + DEFAULT_DRIVERS_SUBDIR;
        return ConnectorConf.get(context, CONF_DRIVERS_DIR, null, defaultDir);
    }

    /** The directories a driver library may be loaded from; see {@link #CONF_DRIVER_SECURE_PATH}. */
    public static String driverSecurePath(ConnectorContext context) {
        return ConnectorConf.get(context, CONF_DRIVER_SECURE_PATH, null, DEFAULT_DRIVER_SECURE_PATH);
    }
}
