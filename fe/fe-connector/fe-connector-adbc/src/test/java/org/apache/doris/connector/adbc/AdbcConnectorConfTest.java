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

import org.apache.doris.connector.spi.ConnectorContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * How this connector's two deployment-level settings are resolved: adbc.conf, then the built-in
 * default. There is no fe.conf half -- this connector is newer than the plugin conf file -- so these
 * tests also pin that no fe.conf key is consulted behind the administrator's back.
 *
 * <p>Asserted rather than trusted because every failure direction here is silent: a template under the
 * wrong name deploys a file the engine never opens, and a settings read that ignores adbc.conf makes an
 * administrator's edit do nothing, with neither file showing why.
 */
public class AdbcConnectorConfTest {

    @Test
    public void theConfTemplateIsNamedAfterTheProvider() {
        // The engine reads <name>.conf, so a template under any other name deploys a file nothing ever
        // opens -- silently, with every setting in it ignored. Renaming name() must break here.
        String expected = new AdbcConnectorProvider().name() + ".conf.template";
        Assertions.assertNotNull(getClass().getClassLoader().getResource(expected),
                "the plugin must ship " + expected + " on its classpath");
    }

    @Test
    public void driversDirComesFromThePluginConfThenFromDorisHome() {
        Map<String, String> dorisHome = Collections.singletonMap(
                AdbcConf.ENV_DORIS_HOME, "/opt/doris");

        Assertions.assertEquals("/from/plugin/conf", AdbcConf.driversDir(
                context(Collections.singletonMap(AdbcConf.CONF_DRIVERS_DIR, "/from/plugin/conf"),
                        dorisHome)));

        Assertions.assertEquals("/opt/doris/plugins/adbc_drivers", AdbcConf.driversDir(
                context(Collections.emptyMap(), dorisHome)));
    }

    @Test
    public void driversDirIsNullWhenNothingConfiguresIt() {
        // Null rather than a guess: AdbcDriverPathResolver then says a bare file name cannot be
        // resolved, instead of quietly resolving it against whatever directory FE happens to run in --
        // which would load a driver nobody pointed at, or fail with a path nothing in the catalog
        // explains.
        Assertions.assertNull(AdbcConf.driversDir(
                context(Collections.emptyMap(), Collections.emptyMap())));
    }

    @Test
    public void blankIsTreatedAsUnset() {
        // An operator who writes "drivers_dir=" means "I have not configured this", so the default has
        // to win. Reading it as the empty string would make the bare-name resolution fail with a
        // "not configured" message that the conf file appears to contradict.
        Assertions.assertEquals("/opt/doris/plugins/adbc_drivers", AdbcConf.driversDir(
                context(Collections.singletonMap(AdbcConf.CONF_DRIVERS_DIR, "   "),
                        Collections.singletonMap(AdbcConf.ENV_DORIS_HOME, "/opt/doris"))));
    }

    @Test
    public void noFeConfKeyIsConsultedForTheseSettings() {
        // The pre-plugin-conf shape of this connector read fe.conf's adbc_drivers_dir /
        // adbc_driver_secure_path through the environment. Those @ConfFields are gone; an environment
        // that still carries them must not resurrect a channel fe-core no longer feeds.
        //
        // Asserted through AdbcConf's own readers, not through ConnectorConf with a hand-written null
        // legacy key: passing the null here would only prove that this test passes null, and would stay
        // green if a reader started naming a legacy key.
        Map<String, String> legacyEnv = new HashMap<>();
        legacyEnv.put("adbc_drivers_dir", "/from/fe/conf");
        legacyEnv.put("adbc_driver_secure_path", "/from/fe/conf");

        Assertions.assertNull(AdbcConf.driversDir(context(Collections.emptyMap(), legacyEnv)));
        Assertions.assertEquals(AdbcConf.DEFAULT_DRIVER_SECURE_PATH,
                AdbcConf.driverSecurePath(context(Collections.emptyMap(), legacyEnv)));
    }

    @Test
    public void driverSecurePathComesFromThePluginConf() {
        Assertions.assertEquals("/opt/drivers;/srv/drivers", AdbcConf.driverSecurePath(
                context(Collections.singletonMap(AdbcConf.CONF_DRIVER_SECURE_PATH,
                        "/opt/drivers;/srv/drivers"), Collections.emptyMap())));
    }

    private static ConnectorContext context(Map<String, String> conf, Map<String, String> env) {
        Map<String, String> confCopy = new HashMap<>(conf);
        Map<String, String> envCopy = new HashMap<>(env);
        return new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "adbc_test";
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
