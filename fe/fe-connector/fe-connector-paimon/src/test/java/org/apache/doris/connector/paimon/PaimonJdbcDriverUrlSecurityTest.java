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

import org.apache.doris.foundation.security.JdbcDriverUrlSecurity;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * The Paimon JDBC catalog loads {@code paimon.jdbc.driver_url} / {@code jdbc.driver_url} into the FE JVM
 * through a {@code URLClassLoader} + {@code Class.forName(name, true, loader)}, exactly like the jdbc
 * catalog does. These tests pin that the provider's CREATE and ALTER hooks both reach the SAME mandatory
 * rule ({@link JdbcDriverUrlSecurity}), which is wired inside
 * {@code PaimonJdbcMetaStoreProperties#validate()} — the statement-time hook that
 * {@code checkCreateTimeOnlyRules} selects for the jdbc flavor.
 *
 * <p>The rule's own semantics live in {@code JdbcDriverUrlSecurityTest} (fe-foundation); here one rejected
 * shape is enough to prove the call is wired.
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

    @Test
    public void createRejectsTraversalDriverUrl() {
        // MUTATION: drop the JdbcDriverUrlSecurity.check call from PaimonJdbcMetaStoreProperties.validate()
        // -> the props are otherwise valid, so validateProperties returns -> red.
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> PROVIDER.validateProperties(
                        jdbcProps("jdbc.driver_url", "file:///opt/drivers/../../etc/evil.jar")));
        Assertions.assertTrue(e.getMessage().contains("path traversal"), e.getMessage());
    }

    @Test
    public void createRejectsTraversalOnPaimonPrefixedAlias() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PROVIDER.validateProperties(
                        jdbcProps("paimon.jdbc.driver_url", "file:///opt/drivers/../../etc/evil.jar")));
    }

    @Test
    public void alterRejectsRepointedDriverUrl() {
        // ALTER goes through validatePropertiesForUpdate, which does not funnel back to
        // validateProperties — it reaches the rule through checkCreateTimeOnlyRules -> bind
        // -> PaimonJdbcMetaStoreProperties.validate() on the merged candidate.
        Map<String, String> stored = jdbcProps("jdbc.driver_url", "mysql-connector-j-8.4.0.jar");
        Map<String, String> update = new HashMap<>();
        update.put("jdbc.driver_url", "file:///opt/drivers/../../etc/evil.jar");
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> PROVIDER.validatePropertiesForUpdate(stored, update));
        Assertions.assertTrue(e.getMessage().contains("path traversal"), e.getMessage());
    }

    @Test
    public void createRejectsSchemelessPathDriverUrl() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PROVIDER.validateProperties(jdbcProps("jdbc.driver_url", "sub/dir/evil.jar")));
    }

    @Test
    public void createAcceptsBareJarName() {
        PROVIDER.validateProperties(jdbcProps("jdbc.driver_url", "mysql-connector-j-8.4.0.jar"));
    }

    @Test
    public void ruleSkippedForNonJdbcFlavor() {
        // A driver_url is dead config on a non-jdbc flavor: bind never selects the jdbc metastore
        // holder, so the rule must not turn a previously-accepted filesystem catalog into a
        // CREATE/ALTER failure.
        Map<String, String> props = new HashMap<>();
        props.put("paimon.catalog.type", "filesystem");
        props.put("warehouse", "s3://bucket/wh");
        props.put("jdbc.driver_url", "../evil.jar");
        PROVIDER.validateProperties(props);
    }
}
