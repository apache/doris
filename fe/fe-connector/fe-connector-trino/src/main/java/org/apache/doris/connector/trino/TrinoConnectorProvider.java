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

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;

import java.util.Map;

/**
 * SPI entry point for the Trino Connector bridge.
 * Discovered via META-INF/services/org.apache.doris.connector.spi.ConnectorProvider.
 */
public class TrinoConnectorProvider implements ConnectorProvider {

    static final String TRINO_CONNECTOR_NAME = "trino.connector.name";

    /**
     * This connector's type, and therefore its {@code name()} — which is what the engine names its
     * conf file after, so the plugin must ship {@code trino-connector.conf.template}. Note that this is
     * NOT the plugin directory name ({@code plugins/connector/trino}); the directory is the deployer's
     * choice, the conf file name is this string.
     */
    public static final String TYPE = "trino-connector";

    /**
     * Directory holding the Trino plugins this connector loads, in {@code trino-connector.conf}.
     * Falls back to fe.conf's {@code trino_connector_plugin_dir}, which is where it used to live.
     */
    public static final String CONF_PLUGIN_DIR = "plugin_dir";

    /** The fe.conf name of {@link #CONF_PLUGIN_DIR}, forwarded through the engine environment. */
    public static final String ENV_PLUGIN_DIR = "trino_connector_plugin_dir";

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public Connector create(Map<String, String> properties, ConnectorContext context) {
        return new TrinoDorisConnector(properties, context);
    }

    @Override
    public void validateProperties(Map<String, String> properties) {
        String connectorName = properties.get(TRINO_CONNECTOR_NAME);
        if (connectorName == null || connectorName.isEmpty()) {
            throw new IllegalArgumentException(
                    "Required property '" + TRINO_CONNECTOR_NAME + "' is missing");
        }
    }
}
