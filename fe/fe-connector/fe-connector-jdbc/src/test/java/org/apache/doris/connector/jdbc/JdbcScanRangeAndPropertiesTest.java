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

import org.apache.doris.connector.api.scan.ConnectorScanRangeType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class JdbcScanRangeAndPropertiesTest {

    // === JdbcScanRange Builder ===

    @Test
    void testBuildMinimalScanRange() {
        JdbcScanRange range = new JdbcScanRange.Builder()
                .querySql("SELECT 1")
                .build();
        Assertions.assertEquals("SELECT 1", range.getProperties().get("query_sql"));
    }

    @Test
    void testBuildFullScanRange() {
        JdbcScanRange range = new JdbcScanRange.Builder()
                .querySql("SELECT * FROM t")
                .jdbcUrl("jdbc:mysql://host:3306/db")
                .jdbcUser("root")
                .jdbcPassword("secret")
                .driverClass("com.mysql.cj.jdbc.Driver")
                .driverUrl("http://driver.jar")
                .driverChecksum("abc123")
                .catalogId(42L)
                .tableType(JdbcDbType.MYSQL)
                .connectionPoolMinSize(2)
                .connectionPoolMaxSize(20)
                .connectionPoolMaxWaitTime(3000)
                .connectionPoolMaxLifeTime(600000)
                .connectionPoolKeepAlive(true)
                .build();

        Map<String, String> props = range.getProperties();
        Assertions.assertEquals("SELECT * FROM t", props.get("query_sql"));
        Assertions.assertEquals("jdbc:mysql://host:3306/db", props.get("jdbc_url"));
        Assertions.assertEquals("root", props.get("jdbc_user"));
        Assertions.assertEquals("secret", props.get("jdbc_password"));
        Assertions.assertEquals("com.mysql.cj.jdbc.Driver", props.get("jdbc_driver_class"));
        Assertions.assertEquals("http://driver.jar", props.get("jdbc_driver_url"));
        Assertions.assertEquals("abc123", props.get("jdbc_driver_checksum"));
        Assertions.assertEquals("42", props.get("catalog_id"));
        Assertions.assertEquals("MYSQL", props.get("table_type"));
        Assertions.assertEquals("2", props.get("connection_pool_min_size"));
        Assertions.assertEquals("20", props.get("connection_pool_max_size"));
        Assertions.assertEquals("3000", props.get("connection_pool_max_wait_time"));
        Assertions.assertEquals("600000", props.get("connection_pool_max_life_time"));
        Assertions.assertEquals("true", props.get("connection_pool_keep_alive"));
    }

    @Test
    void testScanRangeType() {
        JdbcScanRange range = new JdbcScanRange.Builder().build();
        Assertions.assertEquals(ConnectorScanRangeType.FILE_SCAN, range.getRangeType());
    }

    @Test
    void testScanRangePath() {
        JdbcScanRange range = new JdbcScanRange.Builder().build();
        Assertions.assertTrue(range.getPath().isPresent());
        Assertions.assertEquals("jdbc://virtual", range.getPath().get());
    }

    @Test
    void testScanRangeTableFormatType() {
        JdbcScanRange range = new JdbcScanRange.Builder().build();
        Assertions.assertEquals("jdbc", range.getTableFormatType());
    }

    @Test
    void testScanRangePropertiesAreUnmodifiable() {
        JdbcScanRange range = new JdbcScanRange.Builder()
                .querySql("SELECT 1")
                .build();
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> range.getProperties().put("new_key", "value"));
    }

    @Test
    void testDriverChecksumNullBecomesEmptyString() {
        JdbcScanRange range = new JdbcScanRange.Builder()
                .driverChecksum(null)
                .build();
        Assertions.assertEquals("", range.getProperties().get("jdbc_driver_checksum"));
    }

    @Test
    void testConnectionPoolKeepAliveFalse() {
        JdbcScanRange range = new JdbcScanRange.Builder()
                .connectionPoolKeepAlive(false)
                .build();
        Assertions.assertEquals("false", range.getProperties().get("connection_pool_keep_alive"));
    }

    // === JdbcConnectorProperties.getInt ===

    @Test
    void testGetIntValidValue() {
        Map<String, String> props = new HashMap<>();
        props.put("pool_size", "42");
        Assertions.assertEquals(42, JdbcConnectorProperties.getInt(props, "pool_size", 10));
    }

    @Test
    void testGetIntMissingKeyReturnsDefault() {
        Map<String, String> props = new HashMap<>();
        Assertions.assertEquals(10, JdbcConnectorProperties.getInt(props, "missing", 10));
    }

    @Test
    void testGetIntNullValueReturnsDefault() {
        Map<String, String> props = new HashMap<>();
        props.put("key", null);
        Assertions.assertEquals(5, JdbcConnectorProperties.getInt(props, "key", 5));
    }

    @Test
    void testGetIntEmptyStringReturnsDefault() {
        Map<String, String> props = new HashMap<>();
        props.put("key", "");
        Assertions.assertEquals(5, JdbcConnectorProperties.getInt(props, "key", 5));
    }

    @Test
    void testGetIntNonNumericReturnsDefault() {
        Map<String, String> props = new HashMap<>();
        props.put("key", "not_a_number");
        Assertions.assertEquals(5, JdbcConnectorProperties.getInt(props, "key", 5));
    }

    @Test
    void testGetIntWithWhitespace() {
        Map<String, String> props = new HashMap<>();
        props.put("key", "  100  ");
        Assertions.assertEquals(100, JdbcConnectorProperties.getInt(props, "key", 5));
    }

    @Test
    void testGetIntNegativeValue() {
        Map<String, String> props = new HashMap<>();
        props.put("key", "-1");
        Assertions.assertEquals(-1, JdbcConnectorProperties.getInt(props, "key", 0));
    }

    @Test
    void testGetIntZero() {
        Map<String, String> props = new HashMap<>();
        props.put("key", "0");
        Assertions.assertEquals(0, JdbcConnectorProperties.getInt(props, "key", 99));
    }
}
