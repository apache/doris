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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * How this connector's two deployment-level settings are resolved: its own {@code iceberg.conf}
 * first, then the fe.conf key each used to live under.
 *
 * <p>Asserted rather than trusted, because both failure directions are silent. Reading fe.conf first
 * would make an administrator's edit to iceberg.conf do nothing. Dropping the fe.conf fallback would
 * change an untouched deployment's metastore timeout and drivers directory on upgrade, with nothing
 * in either file to show why.
 */
public class IcebergConnectorConfTest {

    @Test
    public void driversDirPrefersThePluginConfThenFeConf() {
        Assertions.assertEquals("/from/plugin/conf", ConnectorConf.get(
                context(Collections.singletonMap(IcebergConnectorProperties.CONF_DRIVERS_DIR,
                                "/from/plugin/conf"),
                        Collections.singletonMap(IcebergConnectorProperties.ENV_JDBC_DRIVERS_DIR,
                                "/from/fe/conf")),
                IcebergConnectorProperties.CONF_DRIVERS_DIR,
                IcebergConnectorProperties.ENV_JDBC_DRIVERS_DIR, null));

        Assertions.assertEquals("/from/fe/conf", ConnectorConf.get(
                context(Collections.emptyMap(),
                        Collections.singletonMap(IcebergConnectorProperties.ENV_JDBC_DRIVERS_DIR,
                                "/from/fe/conf")),
                IcebergConnectorProperties.CONF_DRIVERS_DIR,
                IcebergConnectorProperties.ENV_JDBC_DRIVERS_DIR, null));

        Assertions.assertNull(ConnectorConf.get(context(Collections.emptyMap(), Collections.emptyMap()),
                IcebergConnectorProperties.CONF_DRIVERS_DIR,
                IcebergConnectorProperties.ENV_JDBC_DRIVERS_DIR, null));
    }

    @Test
    public void metastoreTimeoutPrefersThePluginConfThenFeConfThenTen() {
        Assertions.assertEquals("30", ConnectorConf.get(
                context(Collections.singletonMap(
                                IcebergConnectorProperties.CONF_METASTORE_CLIENT_TIMEOUT_SECOND, "30"),
                        Collections.singletonMap(
                                IcebergConnectorProperties.ENV_HIVE_METASTORE_CLIENT_TIMEOUT_SECOND, "20")),
                IcebergConnectorProperties.CONF_METASTORE_CLIENT_TIMEOUT_SECOND,
                IcebergConnectorProperties.ENV_HIVE_METASTORE_CLIENT_TIMEOUT_SECOND,
                IcebergConnectorProperties.DEFAULT_METASTORE_CLIENT_TIMEOUT_SECOND));

        Assertions.assertEquals("20", ConnectorConf.get(
                context(Collections.emptyMap(), Collections.singletonMap(
                        IcebergConnectorProperties.ENV_HIVE_METASTORE_CLIENT_TIMEOUT_SECOND, "20")),
                IcebergConnectorProperties.CONF_METASTORE_CLIENT_TIMEOUT_SECOND,
                IcebergConnectorProperties.ENV_HIVE_METASTORE_CLIENT_TIMEOUT_SECOND,
                IcebergConnectorProperties.DEFAULT_METASTORE_CLIENT_TIMEOUT_SECOND));

        // The literal the call site used before this channel existed; keeping it is what makes a
        // deployment with neither file behave exactly as it did.
        Assertions.assertEquals("10", ConnectorConf.get(
                context(Collections.emptyMap(), Collections.emptyMap()),
                IcebergConnectorProperties.CONF_METASTORE_CLIENT_TIMEOUT_SECOND,
                IcebergConnectorProperties.ENV_HIVE_METASTORE_CLIENT_TIMEOUT_SECOND,
                IcebergConnectorProperties.DEFAULT_METASTORE_CLIENT_TIMEOUT_SECOND));
    }

    @Test
    public void theConfTemplateIsNamedAfterTheProvider() {
        // The engine reads <name>.conf, so a template under any other name deploys a file nothing ever
        // opens -- silently, with every setting in it ignored. Renaming getType() must break here.
        String expected = new IcebergConnectorProvider().name() + ".conf.template";
        Assertions.assertNotNull(getClass().getClassLoader().getResource(expected),
                "the plugin must ship " + expected + " on its classpath");
    }

    private static ConnectorContext context(Map<String, String> conf, Map<String, String> env) {
        Map<String, String> confCopy = new HashMap<>(conf);
        Map<String, String> envCopy = new HashMap<>(env);
        return new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "test_catalog";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }

            @Override
            public Map<String, String> getConnectorConfig() {
                return confCopy;
            }

            @Override
            public Map<String, String> getEnvironment() {
                return envCopy;
            }
        };
    }
}
