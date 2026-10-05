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

package org.apache.doris.connector.trino;

import org.apache.doris.connector.spi.ConnectorConf;
import org.apache.doris.connector.spi.ConnectorContext;

/**
 * The deployment-level settings of this plugin: one per FE, not one per catalog. Per-catalog settings
 * are the other half and live in {@link TrinoCatalogProperties}.
 *
 * <p>They are read from the plugin's own conf file, which is named after
 * {@link TrinoConnectorProvider#TYPE} ({@code trino-connector.conf}) rather than after the plugin
 * directory, each falling back to the {@code fe.conf} key it used to live under.
 */
public final class TrinoConf {

    /**
     * Directory holding the Trino plugins this connector loads, in {@code trino-connector.conf}.
     * A catalog may override it with {@link TrinoCatalogProperties#PLUGIN_DIR}, which wins over this.
     */
    public static final String CONF_PLUGIN_DIR = "plugin_dir";

    /** The fe.conf name of {@link #CONF_PLUGIN_DIR}, forwarded through the engine environment. */
    public static final String ENV_PLUGIN_DIR = "trino_connector_plugin_dir";

    private TrinoConf() {
    }

    /**
     * The configured Trino plugin directory, or null when the engine delivered neither the plugin conf
     * key nor the fe.conf one. There is no built-in default: guessing a directory would surface as
     * "catalog creates fine but every query fails", so the caller fails where the cause is visible.
     */
    public static String pluginDir(ConnectorContext context) {
        return ConnectorConf.get(context, CONF_PLUGIN_DIR, ENV_PLUGIN_DIR, null);
    }
}
