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

import org.apache.doris.connector.spi.ConnectorValidationContext;
import org.apache.doris.connector.spi.DriverUrlPolicy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link PaimonConnector#preCreateValidation} (rereview2 B-8b): a JDBC-flavor catalog
 * with a {@code driver_url} must pass the FE's shared driver-jar policy ({@link DriverUrlPolicy}) at
 * CREATE CATALOG, before the jar is ever loaded into the FE JVM. Mirrors
 * {@code JdbcDorisConnector.preCreateValidation}.
 *
 * <p>Offline: the policy reads its settings off {@link RecordingConnectorContext#environment}, so a
 * temporary drivers directory stands in for {@code jdbc_drivers_dir} and a {@code jdbc_driver_secure_path}
 * that excludes the url simulates a rejected one.
 */
public class PaimonConnectorPreCreateValidationTest {

    /**
     * A context whose FE install root is {@code home}, with the default drivers directory
     * ({@code home/plugins/jdbc_drivers}) — the layout in which the policy checks that a bare jar name
     * actually exists.
     */
    private static RecordingConnectorContext contextWithDriversDir(Path home) {
        RecordingConnectorContext context = new RecordingConnectorContext();
        Map<String, String> env = new HashMap<>();
        env.put(DriverUrlPolicy.ENV_DORIS_HOME, home.toString());
        env.put(DriverUrlPolicy.ENV_DRIVERS_DIR, home.resolve("plugins/jdbc_drivers").toString());
        context.environment = env;
        return context;
    }

    private static Path driversDirWith(Path home, String jar) throws IOException {
        Path driversDir = Files.createDirectories(home.resolve("plugins/jdbc_drivers"));
        Files.write(driversDir.resolve(jar), new byte[] {1});
        return home;
    }

    /** Hand-written {@link ConnectorValidationContext} test double (no Mockito); nothing on it is consulted. */
    private static final ConnectorValidationContext VALIDATION_CONTEXT = new ConnectorValidationContext() {
        @Override
        public long getCatalogId() {
            return 0;
        }

        @Override
        public String getProperty(String key) {
            return null;
        }

        @Override
        public void storeProperty(String key, String value) {
        }

        @Override
        public void requestBeConnectivityTest(byte[] serializedDescriptor, int connectionTypeValue,
                String testQuery) {
        }
    };

    @Test
    public void validatesJdbcDriverUrl(@TempDir Path dir) throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.catalog.type", "jdbc");
        props.put("jdbc.driver_url", "mysql.jar");

        // WHY (BLOCKER B-8b): a jdbc driver_url is loaded into the FE JVM (URLClassLoader); CREATE
        // CATALOG must put it through the format / white-list / secure-path policy. A bare name that
        // exists nowhere the policy looks is rejected; the same name present in the drivers directory
        // passes. MUTATION: dropping the preCreateValidation override -> nothing rejected -> red.
        Assertions.assertThrows(RuntimeException.class, () -> new PaimonConnector(props,
                contextWithDriversDir(dir)).preCreateValidation(VALIDATION_CONTEXT));
        new PaimonConnector(props, contextWithDriversDir(driversDirWith(dir, "mysql.jar")))
                .preCreateValidation(VALIDATION_CONTEXT);
    }

    @Test
    public void validatesPaimonJdbcDriverUrlAlias(@TempDir Path dir) throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.catalog.type", "jdbc");
        props.put("paimon.jdbc.driver_url", "mysql.jar");

        Assertions.assertThrows(RuntimeException.class, () -> new PaimonConnector(props,
                contextWithDriversDir(dir)).preCreateValidation(VALIDATION_CONTEXT),
                "the paimon.jdbc.driver_url alias must also be validated");
        new PaimonConnector(props, contextWithDriversDir(driversDirWith(dir, "mysql.jar")))
                .preCreateValidation(VALIDATION_CONTEXT);
    }

    @Test
    public void skipsValidationForNonJdbcFlavor(@TempDir Path dir) throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.catalog.type", "filesystem");
        props.put("jdbc.driver_url", "mysql.jar");

        // The name exists nowhere, which would be rejected for the jdbc flavor.
        new PaimonConnector(props, contextWithDriversDir(dir)).preCreateValidation(VALIDATION_CONTEXT);
    }

    @Test
    public void skipsValidationWhenNoDriverUrl(@TempDir Path dir) throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.catalog.type", "jdbc");

        // a jdbc catalog without a driver_url uses the platform driver -> nothing to validate
        new PaimonConnector(props, contextWithDriversDir(dir)).preCreateValidation(VALIDATION_CONTEXT);
    }

    @Test
    public void propagatesRejectedDriverUrl(@TempDir Path dir) throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.catalog.type", "jdbc");
        props.put("jdbc.driver_url", "http://evil.test/x.jar");
        RecordingConnectorContext context = contextWithDriversDir(dir);
        Map<String, String> env = new HashMap<>(context.environment);
        env.put(DriverUrlPolicy.ENV_DRIVER_SECURE_PATH, "http://good.test/drivers");
        context.environment = env;

        // WHY (BLOCKER B-8b): a disallowed url must FAIL CREATE CATALOG — the policy throws and the
        // connector must let it propagate, not swallow it. MUTATION: catching the exception -> no
        // throw -> red.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new PaimonConnector(props, context).preCreateValidation(VALIDATION_CONTEXT));
    }
}
