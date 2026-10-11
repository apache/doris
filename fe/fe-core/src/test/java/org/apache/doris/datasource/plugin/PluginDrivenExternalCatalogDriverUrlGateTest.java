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
import org.apache.doris.connector.spi.ConnectorSession;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
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
 * anything. The checked keys come from the key table in
 * {@code PluginDrivenExternalCatalog#driverUrlKeysOf}.
 */
public class PluginDrivenExternalCatalogDriverUrlGateTest {

    private static final String ALLOWED_DIR = "/opt/doris/plugins/jdbc_drivers";

    private String savedSecurePath;

    @BeforeEach
    public void setUp() {
        savedSecurePath = Config.jdbc_driver_secure_path;
        // The operator has locked driver jars down to one directory — the posture this gate exists to keep.
        Config.jdbc_driver_secure_path = ALLOWED_DIR;
        // A fresh empty manager so provider-side validation is a no-op and the gate is tested in isolation.
        ConnectorFactory.initPluginManager(new ConnectorPluginManager());
    }

    @AfterEach
    public void tearDown() {
        Config.jdbc_driver_secure_path = savedSecurePath;
        // An empty manager, not null: surefire reuses the fork, and a null singleton would make later
        // tests' ConnectorFactory calls silently no-op instead of fail (same shape as the sibling
        // plugin tests' tearDown).
        ConnectorFactory.initPluginManager(new ConnectorPluginManager());
    }

    @Test
    public void alterRejectsJdbcDriverUrlOutsideOperatorAllowList() {
        TestCatalog catalog = new TestCatalog(
                props("jdbc", "driver_url", "file://" + ALLOWED_DIR + "/mysql.jar"));

        // MUTATION: drop checkDriverUrlsAgainstOperatorGate from validatePropertiesBeforeUpdate
        // -> the ALTER is accepted and the remote jar is loaded on the next metadata access -> red.
        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> catalog.validatePropertiesBeforeUpdate(
                        props("jdbc", "driver_url", "file://" + ALLOWED_DIR + "/mysql.jar"),
                        Collections.singletonMap("driver_url", "http://attacker.test/evil.jar")));
        Assertions.assertTrue(e.getMessage().contains("does not match any allowed paths"), e.getMessage());
    }

    @Test
    public void alterAcceptsJdbcDriverUrlInsideOperatorAllowList() throws Exception {
        TestCatalog catalog = new TestCatalog(
                props("jdbc", "driver_url", "file://" + ALLOWED_DIR + "/mysql.jar"));

        catalog.validatePropertiesBeforeUpdate(
                props("jdbc", "driver_url", "file://" + ALLOWED_DIR + "/mysql.jar"),
                Collections.singletonMap("driver_url", "file://" + ALLOWED_DIR + "/postgresql.jar"));
    }

    @Test
    public void alterRejectsIcebergJdbcFlavorDriverUrl() {
        Map<String, String> stored = props("iceberg", "iceberg.jdbc.driver_url",
                "file://" + ALLOWED_DIR + "/mysql.jar");
        stored.put("iceberg.catalog.type", "jdbc");
        TestCatalog catalog = new TestCatalog(stored);

        Assertions.assertThrows(DdlException.class,
                () -> catalog.validatePropertiesBeforeUpdate(stored,
                        Collections.singletonMap("iceberg.jdbc.driver_url",
                                "http://attacker.test/evil.jar")));
    }

    @Test
    public void alterIgnoresDriverUrlOnNonJdbcIcebergFlavor() throws Exception {
        // On a REST catalog the key is dead config that never reaches a class loader; the gate must not
        // turn an unrelated ALTER into a failure over it.
        Map<String, String> stored = props("iceberg", "iceberg.jdbc.driver_url",
                "http://elsewhere.test/dead-config.jar");
        stored.put("iceberg.catalog.type", "rest");
        TestCatalog catalog = new TestCatalog(stored);

        catalog.validatePropertiesBeforeUpdate(stored,
                Collections.singletonMap("iceberg.jdbc.driver_url", "http://another.test/y.jar"));
    }

    @Test
    public void alterRejectsPaimonDriverUrl() {
        Map<String, String> stored = props("paimon", "paimon.jdbc.driver_url",
                "file://" + ALLOWED_DIR + "/mysql.jar");
        stored.put("paimon.catalog.type", "jdbc");
        TestCatalog catalog = new TestCatalog(stored);

        Assertions.assertThrows(DdlException.class,
                () -> catalog.validatePropertiesBeforeUpdate(stored,
                        Collections.singletonMap("paimon.jdbc.driver_url",
                                "http://attacker.test/evil.jar")));
    }

    @Test
    public void alterRejectsJdbcPrefixedAliasDriverUrl() {
        // The holder strips the "jdbc." prefix, short spelling winning when both are present — so with
        // "driver_url" also stored this update is dead config that never loads. The gate still checks
        // every spelling in the candidate, deliberately fail-closed: which alias wins is the holder's
        // business, and a rejected dead value is repairable in the same ALTER.
        // MUTATION: dropping "jdbc.driver_url" from the key table -> red.
        TestCatalog catalog = new TestCatalog(
                props("jdbc", "driver_url", "file://" + ALLOWED_DIR + "/mysql.jar"));

        Assertions.assertThrows(DdlException.class,
                () -> catalog.validatePropertiesBeforeUpdate(
                        props("jdbc", "driver_url", "file://" + ALLOWED_DIR + "/mysql.jar"),
                        Collections.singletonMap("jdbc.driver_url", "http://attacker.test/evil.jar")));
    }

    @Test
    public void alterRejectsPaimonJdbcAliasDriverUrl() {
        // MUTATION: dropping "jdbc.driver_url" from the paimon row of the key table -> red.
        Map<String, String> stored = props("paimon", "jdbc.driver_url",
                "file://" + ALLOWED_DIR + "/mysql.jar");
        stored.put("paimon.catalog.type", "jdbc");
        TestCatalog catalog = new TestCatalog(stored);

        Assertions.assertThrows(DdlException.class,
                () -> catalog.validatePropertiesBeforeUpdate(stored,
                        Collections.singletonMap("jdbc.driver_url", "http://attacker.test/evil.jar")));
    }

    @Test
    public void alterNotTouchingDriverUrlSkipsTheGate() throws Exception {
        // A stored driver_url was gated at its own CREATE/ALTER time; re-resolving it on every
        // unrelated ALTER would (a) fail the ALTER outright once the operator tightens
        // jdbc_driver_secure_path after the fact, and (b) re-run getFullDriverUrl's file-existence /
        // cloud-download side effects under CatalogMgr's write lock.
        // MUTATION: dropping the touched-keys guard -> the stored URL is re-resolved and rejected
        // -> red.
        TestCatalog catalog = new TestCatalog(
                props("jdbc", "driver_url", "http://legacy.test/pre-tightening.jar"));

        catalog.validatePropertiesBeforeUpdate(
                props("jdbc", "driver_url", "http://legacy.test/pre-tightening.jar"),
                Collections.singletonMap("only_specified_database", "true"));
    }

    @Test
    public void alterFlippingFlavorToJdbcGatesTheStoredDriverUrl() {
        // Flipping iceberg.catalog.type to jdbc brings a previously-dead driver_url to life, so the
        // flavor key must count as a gate trigger even though no driver-url key changed.
        // MUTATION: nulling the flavorKey in the iceberg row of driverUrlKeysOf -> red.
        Map<String, String> stored = props("iceberg", "iceberg.jdbc.driver_url",
                "http://elsewhere.test/dead-config.jar");
        stored.put("iceberg.catalog.type", "rest");
        TestCatalog catalog = new TestCatalog(stored);

        Assertions.assertThrows(DdlException.class,
                () -> catalog.validatePropertiesBeforeUpdate(stored,
                        Collections.singletonMap("iceberg.catalog.type", "jdbc")));
    }

    @Test
    public void alterRejectsTraversalEvenWhenSecurePathIsWildcard() {
        // The mandatory rule is non-configurable: with jdbc_driver_secure_path=* the allow-list gate
        // accepts everything, and on a degraded catalog whose plugin is absent the provider-side rule
        // silently no-ops (this test's empty plugin manager models exactly that), so the engine-side
        // check in the gate is the last line.
        // MUTATION: drop the JdbcDriverUrlSecurity.check call from the gate loop -> red.
        Config.jdbc_driver_secure_path = "*";
        TestCatalog catalog = new TestCatalog(
                props("jdbc", "driver_url", "file://" + ALLOWED_DIR + "/mysql.jar"));

        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> catalog.validatePropertiesBeforeUpdate(
                        props("jdbc", "driver_url", "file://" + ALLOWED_DIR + "/mysql.jar"),
                        Collections.singletonMap("driver_url",
                                "file://" + ALLOWED_DIR + "/../../../etc/evil.jar")));
        Assertions.assertTrue(e.getMessage().contains("path traversal"), e.getMessage());
    }

    private static Map<String, String> props(String type, String key, String value) {
        Map<String, String> props = new HashMap<>();
        props.put("type", type);
        props.put(key, value);
        return props;
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
