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

package org.apache.doris.connector.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link JdbcConnectorProvider#validateProperties(Map)}.
 */
public class JdbcConnectorProviderValidateTest {

    private final JdbcConnectorProvider provider = new JdbcConnectorProvider();

    private Map<String, String> validProps() {
        Map<String, String> props = new HashMap<>();
        props.put("jdbc_url", "jdbc:mysql://localhost:3306/db");
        props.put("driver_url", "/path/to/driver.jar");
        props.put("driver_class", "com.mysql.cj.jdbc.Driver");
        return props;
    }

    @Test
    public void testValidPropertiesPass() {
        Assertions.assertDoesNotThrow(() -> provider.validateProperties(validProps()));
    }

    @Test
    public void testMissingJdbcUrl() {
        Map<String, String> props = validProps();
        props.remove("jdbc_url");
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains("jdbc_url"));
    }

    @Test
    public void testMissingDriverUrl() {
        Map<String, String> props = validProps();
        props.remove("driver_url");
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains("driver_url"));
    }

    @Test
    public void testMissingDriverClass() {
        Map<String, String> props = validProps();
        props.remove("driver_class");
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains("driver_class"));
    }

    @Test
    public void testPrefixedPropertiesAccepted() {
        Map<String, String> props = new HashMap<>();
        props.put("jdbc.jdbc_url", "jdbc:mysql://localhost:3306/db");
        props.put("jdbc.driver_url", "/path/to/driver.jar");
        props.put("jdbc.driver_class", "com.mysql.cj.jdbc.Driver");
        Assertions.assertDoesNotThrow(() -> provider.validateProperties(props));
    }

    @Test
    public void testInvalidBooleanProperty() {
        Map<String, String> props = validProps();
        props.put("only_specified_database", "yes");
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains("only_specified_database"));
        Assertions.assertTrue(ex.getMessage().contains("true or false"));
    }

    @Test
    public void testInvalidConnectionPoolKeepAlive() {
        Map<String, String> props = validProps();
        props.put("connection_pool_keep_alive", "maybe");
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains("connection_pool_keep_alive"));
    }

    @Test
    public void testInvalidTestConnection() {
        Map<String, String> props = validProps();
        props.put("test_connection", "invalid");
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains("test_connection"));
    }

    @Test
    public void testDatabaseListWithoutOnlySpecified() {
        Map<String, String> props = validProps();
        props.put("include_database_list", "db1,db2");
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains("include_database_list"));
        Assertions.assertTrue(ex.getMessage().contains("only_specified_database"));
    }

    @Test
    public void testDatabaseListWithOnlySpecifiedTrue() {
        Map<String, String> props = validProps();
        props.put("only_specified_database", "true");
        props.put("include_database_list", "db1,db2");
        Assertions.assertDoesNotThrow(() -> provider.validateProperties(props));
    }

    @Test
    public void testExcludeDatabaseListWithoutOnlySpecified() {
        Map<String, String> props = validProps();
        props.put("exclude_database_list", "db1");
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains("exclude_database_list"));
    }

    @Test
    public void testConnectionPoolMinSizeNegative() {
        Map<String, String> props = validProps();
        props.put("connection_pool_min_size", "-1");
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains("connection_pool_min_size"));
    }

    @Test
    public void testConnectionPoolMaxSizeZero() {
        Map<String, String> props = validProps();
        props.put("connection_pool_max_size", "0");
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains("connection_pool_max_size"));
    }

    @Test
    public void testConnectionPoolMaxSizeLessThanMin() {
        Map<String, String> props = validProps();
        props.put("connection_pool_min_size", "10");
        props.put("connection_pool_max_size", "5");
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains("connection_pool_max_size"));
    }

    @Test
    public void testConnectionPoolMaxWaitTimeTooHigh() {
        Map<String, String> props = validProps();
        props.put("connection_pool_max_wait_time", "50000");
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains("connection_pool_max_wait_time"));
    }

    @Test
    public void testConnectionPoolMaxLifeTimeTooLow() {
        Map<String, String> props = validProps();
        props.put("connection_pool_max_life_time", "1000");
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains("connection_pool_max_life_time"));
    }

    @Test
    public void testConnectionPoolInvalidInteger() {
        Map<String, String> props = validProps();
        props.put("connection_pool_min_size", "abc");
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains("valid integer"));
    }

    @Test
    public void testLowerCaseTableNamesRejected() {
        Map<String, String> props = validProps();
        props.put("lower_case_table_names", "true");
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains("lower_case_table_names"));
    }

    @Test
    public void testValidConnectionPoolSettings() {
        Map<String, String> props = validProps();
        props.put("connection_pool_min_size", "5");
        props.put("connection_pool_max_size", "50");
        props.put("connection_pool_max_wait_time", "10000");
        props.put("connection_pool_max_life_time", "300000");
        Assertions.assertDoesNotThrow(() -> provider.validateProperties(props));
    }

    @Test
    public void testMetaNamesMappingCollisionWithLowerCase() {
        Map<String, String> props = validProps();
        props.put("lower_case_meta_names", "true");
        // "DB_A" maps to "db_a" and "Db_A" maps to "Db_A". After lowercasing,
        // both resolve to "db_a" — this must be caught at CREATE CATALOG time.
        props.put("meta_names_mapping",
                "{\"databases\":[{\"remoteDatabase\":\"DB_A\",\"mapping\":\"db_a\"},"
                        + "{\"remoteDatabase\":\"Db_A\",\"mapping\":\"Db_A\"}]}");
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
    }

    @Test
    public void testMetaNamesMappingNoCollisionWithoutLowerCase() {
        Map<String, String> props = validProps();
        // Without lower_case_meta_names, different-case mappings should pass
        props.put("meta_names_mapping",
                "{\"databases\":[{\"remoteDatabase\":\"DB_A\",\"mapping\":\"db_a\"},"
                        + "{\"remoteDatabase\":\"Db_A\",\"mapping\":\"Db_A\"}]}");
        Assertions.assertDoesNotThrow(() -> provider.validateProperties(props));
    }
}
