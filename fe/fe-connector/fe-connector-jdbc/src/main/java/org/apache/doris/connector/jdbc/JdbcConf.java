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

package org.apache.doris.connector.jdbc;

import org.apache.doris.connector.spi.ConnectorConf;
import org.apache.doris.connector.spi.ConnectorContext;

/**
 * The deployment-level settings of this plugin: one per FE, not one per catalog.
 *
 * <p>They are read from the plugin's own {@code jdbc.conf} (named after the provider), each falling
 * back to the {@code fe.conf} key it used to live under, which still works. Per-catalog settings are
 * the other half and live in {@link JdbcCatalogProperties}.
 */
public final class JdbcConf {

    /** Where a bare {@code driver_url} file name is resolved. */
    public static final String CONF_DRIVERS_DIR = "drivers_dir";

    /** The fe.conf name of {@link #CONF_DRIVERS_DIR}, forwarded through the engine environment. */
    public static final String ENV_DRIVERS_DIR = "jdbc_drivers_dir";

    /**
     * Whether a SQL Server URL gets {@code encrypt=false} appended when it names no encryption. It
     * exists because a driver upgrade changed that default, and a deployment may not have TLS on its
     * SQL Server yet.
     */
    public static final String CONF_FORCE_SQLSERVER_ENCRYPT_FALSE = "force_sqlserver_encrypt_false";

    /** The fe.conf name of {@link #CONF_FORCE_SQLSERVER_ENCRYPT_FALSE}. */
    public static final String ENV_FORCE_SQLSERVER_ENCRYPT_FALSE = "force_sqlserver_jdbc_encrypt_false";

    /** Engine-wide, not this connector's: the FE install root. Stays in the engine environment. */
    public static final String ENV_DORIS_HOME = "doris_home";

    private JdbcConf() {
    }

    /** The drivers directory, or null when neither the plugin conf nor fe.conf names one. */
    public static String driversDir(ConnectorContext context) {
        return ConnectorConf.get(context, CONF_DRIVERS_DIR, ENV_DRIVERS_DIR, null);
    }

    public static String dorisHome(ConnectorContext context) {
        return context.getEnvironment().get(ENV_DORIS_HOME);
    }

    public static boolean forceSqlServerEncryptFalse(ConnectorContext context) {
        return Boolean.parseBoolean(ConnectorConf.get(context, CONF_FORCE_SQLSERVER_ENCRYPT_FALSE,
                ENV_FORCE_SQLSERVER_ENCRYPT_FALSE, "false"));
    }
}
