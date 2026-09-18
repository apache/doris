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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.ConnectorContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * How this connector's two deployment-level settings are resolved: its own {@code paimon.conf} first,
 * then the fe.conf key each used to live under.
 *
 * <p>Asserted rather than trusted, because both failure directions are silent. Reading fe.conf first
 * would make an administrator's edit to paimon.conf do nothing. Dropping the fe.conf fallback would
 * change an untouched deployment's metastore timeout and drivers directory on upgrade, with nothing
 * in either file to show why.
 */
public class PaimonConnectorConfTest {

    @Test
    public void driversDirPrefersThePluginConfThenFeConf() {
        Assertions.assertEquals("/from/plugin/conf", PaimonConf.driversDir(
                context(Collections.singletonMap(PaimonConf.CONF_DRIVERS_DIR,
                                "/from/plugin/conf"),
                        Collections.singletonMap(PaimonConf.ENV_JDBC_DRIVERS_DIR,
                                "/from/fe/conf"))));

        Assertions.assertEquals("/from/fe/conf", PaimonConf.driversDir(
                context(Collections.emptyMap(),
                        Collections.singletonMap(PaimonConf.ENV_JDBC_DRIVERS_DIR,
                                "/from/fe/conf"))));

        Assertions.assertNull(PaimonConf.driversDir(
                context(Collections.emptyMap(), Collections.emptyMap())));
    }

    @Test
    public void nullContextResolvesToNothingRatherThanThrowing() {
        // Both accessors are reached from direct-construction unit tests that pass no context at all
        // (PaimonConnector.resolveFullDriverUrl / PaimonScanPlanProvider both null-check today).
        Assertions.assertNull(PaimonConf.driversDir(null));
        Assertions.assertNull(PaimonConf.dorisHome(null));
    }

    @Test
    public void metastoreTimeoutPrefersThePluginConfThenFeConfThenTen() {
        // Through the production reader, not a re-spelling of it: the call site used to inline the
        // three-argument ConnectorConf.get, so a test that inlined it too would still pass if the
        // connector started reading a different key.
        Assertions.assertEquals("30", PaimonConf.metastoreClientTimeoutSecond(
                context(Collections.singletonMap(
                                PaimonConf.CONF_METASTORE_CLIENT_TIMEOUT_SECOND, "30"),
                        Collections.singletonMap(
                                PaimonConf.ENV_HIVE_METASTORE_CLIENT_TIMEOUT_SECOND, "20"))));

        Assertions.assertEquals("20", PaimonConf.metastoreClientTimeoutSecond(
                context(Collections.emptyMap(), Collections.singletonMap(
                        PaimonConf.ENV_HIVE_METASTORE_CLIENT_TIMEOUT_SECOND, "20"))));

        // The literal the call site used before this channel existed; keeping it is what makes a
        // deployment with neither file behave exactly as it did.
        Assertions.assertEquals("10", PaimonConf.metastoreClientTimeoutSecond(
                context(Collections.emptyMap(), Collections.emptyMap())));
    }

    @Test
    public void theConfTemplateIsNamedAfterTheProvider() {
        // The engine reads <name>.conf, so a template under any other name deploys a file nothing ever
        // opens -- silently, with every setting in it ignored. Renaming getType() must break here.
        String expected = new PaimonConnectorProvider().name() + ".conf.template";
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
