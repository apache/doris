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

package org.apache.doris.datasource.plugin;

import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.connector.ConnectorFactory;
import org.apache.doris.connector.ConnectorPluginManager;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;
import org.apache.doris.connector.spi.ConnectorSession;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ALTER CATALOG must apply the operator's driver-jar gate ({@code jdbc_driver_secure_path} /
 * {@code jdbc_driver_url_white_list}) to a repointed {@code driver_url}.
 *
 * <p>WHY this exists: CREATE applies that gate inside {@code Connector.preCreateValidation}, but ALTER
 * CATALOG never reaches that hook — it validates through {@code validatePropertiesBeforeUpdate} alone, and
 * {@code resetToUninitialized} then makes the new driver_url effective on the next metadata access. So
 * without this gate an operator who restricts {@code jdbc_driver_secure_path} gets the restriction enforced
 * at CREATE and silently bypassed by a follow-up ALTER — i.e. the config would not actually protect
 * anything. The connector names the property (via {@code ConnectorProvider.driverUrlsToValidate}); only the
 * engine knows the fe.conf policy.
 */
public class PluginDrivenExternalCatalogDriverUrlGateTest {

    private static final String ALLOWED_DIR = "/opt/doris/plugins/jdbc_drivers";

    private String savedSecurePath;

    @BeforeEach
    public void setUp() {
        savedSecurePath = Config.jdbc_driver_secure_path;
        // The operator has locked driver jars down to one directory — the posture this gate exists to keep.
        Config.jdbc_driver_secure_path = ALLOWED_DIR;
        ConnectorPluginManager mgr = new ConnectorPluginManager();
        mgr.registerProvider(new DriverLoadingProvider());
        ConnectorFactory.initPluginManager(mgr);
    }

    @AfterEach
    public void tearDown() {
        Config.jdbc_driver_secure_path = savedSecurePath;
        ConnectorFactory.initPluginManager(null);
    }

    @Test
    public void alterRejectsDriverUrlOutsideOperatorAllowList() {
        TestCatalog catalog = new TestCatalog(props("driver_url", ALLOWED_DIR + "/mysql.jar"));

        // MUTATION: drop checkDriverUrlsAgainstOperatorGate from validatePropertiesBeforeUpdate
        // -> the ALTER is accepted and the remote jar is loaded on the next metadata access -> red.
        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> catalog.validatePropertiesBeforeUpdate(
                        props("driver_url", ALLOWED_DIR + "/mysql.jar"),
                        Collections.singletonMap("driver_url", "http://attacker.test/evil.jar")));
        Assertions.assertTrue(e.getMessage().contains("does not match any allowed paths"), e.getMessage());
    }

    @Test
    public void alterAcceptsDriverUrlInsideOperatorAllowList() throws Exception {
        TestCatalog catalog = new TestCatalog(props("driver_url", ALLOWED_DIR + "/mysql.jar"));

        catalog.validatePropertiesBeforeUpdate(
                props("driver_url", ALLOWED_DIR + "/mysql.jar"),
                Collections.singletonMap("driver_url", "file://" + ALLOWED_DIR + "/postgresql.jar"));
    }

    @Test
    public void alterUntouchedByGateWhenConnectorLoadsNoDriver() throws Exception {
        // A connector that declares no driver_url must not be affected by the operator's jdbc policy —
        // the gate is driven by the connector's declaration, not by the property name existing in the map.
        TestCatalog catalog = new TestCatalog(props("some.other.url", "http://elsewhere.test/x"));

        catalog.validatePropertiesBeforeUpdate(
                props("some.other.url", "http://elsewhere.test/x"),
                Collections.singletonMap("some.other.url", "http://another.test/y"));
    }

    private static Map<String, String> props(String key, String value) {
        Map<String, String> props = new HashMap<>();
        props.put("type", DriverLoadingProvider.TYPE);
        props.put(key, value);
        return props;
    }

    /**
     * Stands in for the jdbc / iceberg-jdbc / paimon-jdbc providers: it declares that {@code driver_url}
     * is a jar it will load into the FE JVM, which is all the engine needs to know to apply the policy.
     */
    private static final class DriverLoadingProvider implements ConnectorProvider {
        static final String TYPE = "driver-loading-test";

        @Override
        public String getType() {
            return TYPE;
        }

        @Override
        public Connector create(Map<String, String> properties, ConnectorContext context) {
            return Mockito.mock(Connector.class);
        }

        @Override
        public List<String> driverUrlsToValidate(Map<String, String> properties) {
            String driverUrl = properties.get("driver_url");
            return driverUrl == null ? Collections.emptyList() : Collections.singletonList(driverUrl);
        }
    }

    /** Keeps the real {@code validatePropertiesBeforeUpdate}; stubs out what needs a full FE environment. */
    private static final class TestCatalog extends PluginDrivenExternalCatalog {
        TestCatalog(Map<String, String> props) {
            super(1L, "driver-gate-catalog", null, props, "", Mockito.mock(Connector.class));
            this.initialized = true;
        }

        @Override
        protected Connector createConnectorFromProperties() {
            return null;
        }

        @Override
        protected void initLocalObjectsImpl() {
        }

        @Override
        public ConnectorSession buildConnectorSession() {
            return Mockito.mock(ConnectorSession.class);
        }
    }
}
