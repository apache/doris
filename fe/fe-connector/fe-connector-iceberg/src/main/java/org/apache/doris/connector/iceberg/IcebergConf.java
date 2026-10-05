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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.spi.ConnectorConf;
import org.apache.doris.connector.spi.ConnectorContext;

/**
 * This connector's deployment-level settings — the "B class" of the connector property convention, as
 * opposed to the per-catalog properties in {@link IcebergCatalogProperties}. These are read from the
 * plugin's own {@code iceberg.conf} (named after {@code ConnectorProvider.name()}), each falling back to
 * the {@code ENV_} name below it, which is the fe.conf key it used to live under and still works.
 *
 * <p>Both conf keys are shared with other connectors at the fe.conf end — one {@code jdbc_drivers_dir}
 * and one {@code hive_metastore_client_timeout_second} serve jdbc, iceberg and paimon. A plugin conf
 * cannot express that, so a deployment that moves to these files sets the value in each plugin's own
 * conf. That is the accepted cost of a per-plugin file; the fe.conf keys stay as the shared fallback.
 */
public final class IcebergConf {

    public static final String CONF_DRIVERS_DIR = "drivers_dir";
    public static final String CONF_METASTORE_CLIENT_TIMEOUT_SECOND = "metastore_client_timeout_second";

    /** The fe.conf name of {@link #CONF_DRIVERS_DIR}, forwarded through the engine environment. */
    public static final String ENV_JDBC_DRIVERS_DIR = "jdbc_drivers_dir";
    /** The fe.conf name of {@link #CONF_METASTORE_CLIENT_TIMEOUT_SECOND}. */
    public static final String ENV_HIVE_METASTORE_CLIENT_TIMEOUT_SECOND =
            "hive_metastore_client_timeout_second";
    /** Engine-wide, not this connector's: the FE install root. Stays in the engine environment. */
    public static final String ENV_DORIS_HOME = "doris_home";
    /** Legacy default when neither channel names a metastore client timeout. */
    public static final String DEFAULT_METASTORE_CLIENT_TIMEOUT_SECOND = "10";

    private IcebergConf() {
    }

    /**
     * The directory a bare driver jar name resolves under, from this plugin's own {@code iceberg.conf}
     * or fe.conf's {@code jdbc_drivers_dir}. Null when neither names one — {@code JdbcDriverSupport}
     * then falls back to {@code <doris_home>/plugins/jdbc_drivers}, as before.
     *
     * <p>A null context is a direct-construction unit test, which has neither channel.
     */
    public static String driversDir(ConnectorContext context) {
        return context == null ? null
                : ConnectorConf.get(context, CONF_DRIVERS_DIR, ENV_JDBC_DRIVERS_DIR, null);
    }

    /** The FE install root. Engine-wide rather than this connector's, so it stays in the environment. */
    public static String dorisHome(ConnectorContext context) {
        return context == null ? null : context.getEnvironment().get(ENV_DORIS_HOME);
    }

    /**
     * The hive metastore client socket timeout, in seconds, handed to the shared HMS parser so it can
     * apply it when the catalog does not set {@code hive.metastore.client.socket.timeout} itself. Falls
     * back to the legacy literal so a deployment with neither file behaves as it did.
     */
    public static String metastoreClientTimeoutSecond(ConnectorContext context) {
        return ConnectorConf.get(context, CONF_METASTORE_CLIENT_TIMEOUT_SECOND,
                ENV_HIVE_METASTORE_CLIENT_TIMEOUT_SECOND, DEFAULT_METASTORE_CLIENT_TIMEOUT_SECOND);
    }
}
