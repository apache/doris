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

package org.apache.doris.datasource.paimon;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * The Paimon JDBC catalog loads {@code paimon.jdbc.driver_url} / {@code jdbc.driver_url} into the FE JVM,
 * exactly like the jdbc catalog does. These tests pin that BOTH statement-time hooks — the CREATE-side
 * {@code checkProperties()} and the detached ALTER-side {@code validatePropertiesBeforeUpdate} — reach the
 * SAME mandatory rule ({@code JdbcDriverUrlSecurity}) through this catalog's shared private validation.
 * Neither hook runs on replay or catalog rebuild. The rule's own semantics are pinned once in
 * {@code JdbcDriverUrlSecurityTest} (fe-foundation).
 */
public class PaimonExternalCatalogDriverUrlSecurityTest {

    @Before
    public void setUp() {
        FeConstants.runningUnitTest = true;
    }

    private static Map<String, String> jdbcProps(String driverUrlKey, String driverUrl) {
        Map<String, String> props = new HashMap<>();
        props.put("type", "paimon");
        props.put("paimon.catalog.type", "jdbc");
        props.put("uri", "jdbc:mysql://127.0.0.1:3306/paimon");
        props.put("warehouse", "s3://bucket/wh");
        props.put(driverUrlKey, driverUrl);
        props.put("jdbc.driver_class", "com.mysql.cj.jdbc.Driver");
        return props;
    }

    private static PaimonExternalCatalog catalogWith(Map<String, String> props) {
        return new PaimonExternalCatalog(1L, "paimon_driver_url_test", null, props, "");
    }

    @Test
    public void createRejectsTraversalDriverUrl() {
        // MUTATION: drop the JdbcDriverUrlSecurity.check loop from PaimonExternalCatalog's private
        // checkProperties -> the traversal URL survives to the metastore build and registration -> red.
        DdlException e = Assert.assertThrows(DdlException.class,
                () -> catalogWith(jdbcProps("jdbc.driver_url", "file:///opt/drivers/../../etc/evil.jar"))
                        .checkProperties());
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("path traversal"));
    }

    @Test
    public void createRejectsTraversalOnPaimonPrefixedAlias() {
        DdlException e = Assert.assertThrows(DdlException.class,
                () -> catalogWith(
                        jdbcProps("paimon.jdbc.driver_url", "file:///opt/drivers/../../etc/evil.jar"))
                        .checkProperties());
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("path traversal"));
    }

    @Test
    public void alterRejectsRepointedDriverUrl() {
        // The detached ALTER hook funnels through the same private validation as CREATE; a repointed
        // driver_url in the merged candidate must be rejected before anything is published.
        PaimonExternalCatalog catalog = catalogWith(
                jdbcProps("jdbc.driver_url", "mysql-connector-j-8.4.0.jar"));
        Map<String, String> update = new HashMap<>();
        update.put("jdbc.driver_url", "file:///opt/drivers/../../etc/evil.jar");

        DdlException e = Assert.assertThrows(DdlException.class,
                () -> catalog.validatePropertiesBeforeUpdate(
                        jdbcProps("jdbc.driver_url", "mysql-connector-j-8.4.0.jar"), update));
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("path traversal"));
    }

    @Test
    public void nonJdbcFlavorSkipsTheRule() {
        // On a filesystem catalog the keys are dead config that never reach a class loader; the rule must
        // not turn such a catalog into a CREATE/ALTER failure. The tail of the validation may fail for
        // unrelated storage reasons in this bare unit-test environment, so only the rule's absence is
        // pinned.
        Map<String, String> props = new HashMap<>();
        props.put("type", "paimon");
        props.put("paimon.catalog.type", "filesystem");
        props.put("warehouse", "s3://bucket/wh");
        props.put("jdbc.driver_url", "../evil.jar");
        try {
            catalogWith(props).checkProperties();
        } catch (Exception e) {
            Assert.assertFalse(e.getMessage(),
                    e.getMessage() != null && e.getMessage().contains("path traversal"));
        }
    }
}
