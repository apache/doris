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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.CatalogProperty;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * The Iceberg JDBC catalog loads {@code iceberg.jdbc.driver_url} into the FE JVM, exactly like the jdbc
 * catalog does. These tests pin that {@code checkProperties()} — the statement-time hook the engine runs
 * on CREATE and on ALTER, and never on replay or catalog rebuild — reaches the SAME mandatory rule
 * ({@code JdbcDriverUrlSecurity}). The rule's own semantics are pinned once in
 * {@code JdbcDriverUrlSecurityTest} (fe-foundation).
 */
public class IcebergExternalCatalogDriverUrlSecurityTest {

    @Before
    public void setUp() {
        FeConstants.runningUnitTest = true;
    }

    private static IcebergExternalCatalog catalogWith(Map<String, String> props) {
        // The base constructor takes no properties; an anonymous subclass may set the protected field.
        return new IcebergExternalCatalog(1L, "iceberg_driver_url_test", "") {
            {
                catalogProperty = new CatalogProperty(null, props);
            }
        };
    }

    private static Map<String, String> jdbcProps(String driverUrl) {
        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "jdbc");
        props.put("uri", "jdbc:mysql://127.0.0.1:3306/iceberg");
        props.put("warehouse", "s3://bucket/wh");
        props.put("iceberg.jdbc.driver_url", driverUrl);
        props.put("iceberg.jdbc.driver_class", "com.mysql.cj.jdbc.Driver");
        return props;
    }

    @Test
    public void checkPropertiesRejectsTraversalDriverUrl() {
        // MUTATION: drop the JdbcDriverUrlSecurity.check call from IcebergExternalCatalog.checkProperties
        // -> the traversal URL survives to the metastore-properties build and driver registration -> red.
        DdlException e = Assert.assertThrows(DdlException.class,
                () -> catalogWith(jdbcProps("file:///opt/drivers/../../etc/evil.jar")).checkProperties());
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("path traversal"));
    }

    @Test
    public void checkPropertiesRejectsSchemelessPathDriverUrl() {
        DdlException e = Assert.assertThrows(DdlException.class,
                () -> catalogWith(jdbcProps("sub/dir/evil.jar")).checkProperties());
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("must match"));
    }

    @Test
    public void nonJdbcFlavorSkipsTheRule() {
        // On a REST catalog the key is dead config that never reaches a class loader; the rule must not
        // turn such a catalog into a CREATE/ALTER failure. The tail of checkProperties may fail for
        // unrelated REST reasons in this bare unit-test environment, so only the rule's absence is pinned.
        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "rest");
        props.put("uri", "http://127.0.0.1:8181");
        props.put("iceberg.jdbc.driver_url", "../evil.jar");
        try {
            catalogWith(props).checkProperties();
        } catch (Exception e) {
            Assert.assertFalse(e.getMessage(),
                    e.getMessage() != null && e.getMessage().contains("path traversal"));
        }
    }
}
