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
import org.apache.doris.connector.spi.JdbcDriverUrlSecurity;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The Paimon JDBC catalog loads {@code paimon.jdbc.driver_url} / {@code jdbc.driver_url} into the FE JVM
 * through a {@code URLClassLoader} + {@code Class.forName(name, true, loader)}, exactly like the jdbc
 * catalog does. These tests pin that it reaches the SAME mandatory rule ({@link JdbcDriverUrlSecurity}) on
 * BOTH user-facing paths — the provider hook that fe-core runs on CREATE and ALTER, and the connector's
 * CREATE-only pre-create hook.
 *
 * <p>The rule's own semantics live in {@code JdbcDriverUrlSecurityTest}; here one rejected shape is enough
 * to prove the call is wired.
 */
public class PaimonJdbcDriverUrlSecurityTest {

    private static final PaimonConnectorProvider PROVIDER = new PaimonConnectorProvider();

    private static Map<String, String> jdbcProps(String driverUrlKey, String driverUrl) {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.catalog.type", "jdbc");
        props.put("uri", "jdbc:mysql://127.0.0.1:3306/paimon");
        props.put("warehouse", "s3://bucket/wh");
        props.put(driverUrlKey, driverUrl);
        props.put("jdbc.driver_class", "com.mysql.cj.jdbc.Driver");
        return props;
    }

    // ---------------------------------------------------------------------
    // provider hook — the only one fe-core runs on ALTER CATALOG
    // ---------------------------------------------------------------------

    @Test
    public void providerRejectsTraversalDriverUrl() {
        // MUTATION: drop the JdbcDriverUrlSecurity.check call from PaimonConnectorProvider
        // -> the props are otherwise valid, so validateProperties returns -> red.
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> PROVIDER.validateProperties(
                        jdbcProps("jdbc.driver_url", "file:///opt/drivers/../../etc/evil.jar")));
        Assertions.assertTrue(e.getMessage().contains("path traversal"), e.getMessage());
    }

    @Test
    public void providerRejectsTraversalOnPaimonPrefixedAlias() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PROVIDER.validateProperties(
                        jdbcProps("paimon.jdbc.driver_url", "file:///opt/drivers/../../etc/evil.jar")));
    }

    @Test
    public void providerRejectsSchemelessPathDriverUrl() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PROVIDER.validateProperties(jdbcProps("jdbc.driver_url", "sub/dir/evil.jar")));
    }

    @Test
    public void providerAcceptsBareJarName() {
        PROVIDER.validateProperties(jdbcProps("jdbc.driver_url", "mysql-connector-j-8.4.0.jar"));
    }

    @Test
    public void providerSkipsRuleForNonJdbcFlavor() {
        // A driver_url is dead config on a non-jdbc flavor: it never reaches a class loader, so the rule
        // must not turn a previously-accepted filesystem catalog into a CREATE/ALTER failure.
        Map<String, String> props = new HashMap<>();
        props.put("paimon.catalog.type", "filesystem");
        props.put("warehouse", "s3://bucket/wh");
        props.put("jdbc.driver_url", "../evil.jar");
        PROVIDER.validateProperties(props);
    }

    // ---------------------------------------------------------------------
    // connector pre-create hook — must reject BEFORE the configurable secure-path gate
    // ---------------------------------------------------------------------

    @Test
    public void preCreateValidationRejectsTraversalBeforeEngineGate() {
        RecordingValidationContext ctx = new RecordingValidationContext();
        PaimonConnector connector = new PaimonConnector(
                jdbcProps("jdbc.driver_url", "file:///opt/drivers/../../etc/evil.jar"),
                new RecordingConnectorContext());

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> connector.preCreateValidation(ctx));
        // WHY: the mandatory rule cannot be turned off, so it must fire even when the operator left
        // jdbc_driver_secure_path at its allow-all default (which is what the engine gate applies).
        // MUTATION: run the check after validateAndResolveDriverPath -> the url reaches the gate -> red.
        Assertions.assertTrue(ctx.validatedDriverUrls.isEmpty(),
                "the mandatory rule must reject before the configurable engine gate is consulted");
    }

    @Test
    public void preCreateValidationPassesCleanDriverUrlToEngineGate() throws Exception {
        RecordingValidationContext ctx = new RecordingValidationContext();
        new PaimonConnector(jdbcProps("jdbc.driver_url", "mysql-connector-j-8.4.0.jar"),
                new RecordingConnectorContext()).preCreateValidation(ctx);

        Assertions.assertEquals(List.of("mysql-connector-j-8.4.0.jar"), ctx.validatedDriverUrls);
    }

    /** Hand-written {@link ConnectorValidationContext} test double (no Mockito). */
    private static final class RecordingValidationContext implements ConnectorValidationContext {
        final List<String> validatedDriverUrls = new ArrayList<>();

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
        public String validateAndResolveDriverPath(String driverUrl) {
            validatedDriverUrls.add(driverUrl);
            return "file:///resolved/" + driverUrl;
        }

        @Override
        public String computeDriverChecksum(String driverUrl) {
            return "deadbeef";
        }

        @Override
        public void requestBeConnectivityTest(byte[] serializedDescriptor, int connectionTypeValue,
                String testQuery) {
        }
    }
}
