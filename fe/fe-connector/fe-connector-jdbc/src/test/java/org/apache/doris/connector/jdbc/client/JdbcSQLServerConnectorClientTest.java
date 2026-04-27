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

package org.apache.doris.connector.jdbc.client;

import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.jdbc.JdbcDbType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

/**
 * Tests for {@link JdbcSQLServerConnectorClient}, focusing on SQL Server
 * IDENTITY column type name handling.
 */
public class JdbcSQLServerConnectorClientTest {

    private JdbcSQLServerConnectorClient createClient() {
        return new JdbcSQLServerConnectorClient(
                "test_catalog",
                JdbcDbType.SQLSERVER,
                "jdbc:sqlserver://localhost:1433;databaseName=test",
                false,
                Collections.emptyMap(),
                Collections.emptyMap(),
                false,
                false);
    }

    @Test
    void testDecimalIdentityTypeMapping() {
        JdbcSQLServerConnectorClient client = createClient();
        JdbcFieldInfo info = new JdbcFieldInfo(
                "id", Optional.of("decimal identity"), 3 /* DECIMAL */,
                Optional.of(18), Optional.of(0), Optional.empty());
        ConnectorType ct = client.jdbcTypeToConnectorType(info);
        Assertions.assertEquals("DECIMALV3", ct.getTypeName(),
                "decimal identity should map to DECIMALV3");
        Assertions.assertEquals(18, ct.getPrecision());
        Assertions.assertEquals(0, ct.getScale());
    }

    @Test
    void testDecimalParenIdentityTypeMapping() {
        // SQL Server JDBC driver 11.x returns "decimal() identity" for IDENTITY decimal columns
        JdbcSQLServerConnectorClient client = createClient();
        JdbcFieldInfo info = new JdbcFieldInfo(
                "id", Optional.of("decimal() identity"), 3 /* DECIMAL */,
                Optional.of(18), Optional.of(0), Optional.empty());
        ConnectorType ct = client.jdbcTypeToConnectorType(info);
        Assertions.assertEquals("DECIMALV3", ct.getTypeName(),
                "decimal() identity should map to DECIMALV3");
        Assertions.assertEquals(18, ct.getPrecision());
        Assertions.assertEquals(0, ct.getScale());
    }

    @Test
    void testNumericParenIdentityTypeMapping() {
        // SQL Server JDBC driver may also return "numeric(18, 0) identity"
        JdbcSQLServerConnectorClient client = createClient();
        JdbcFieldInfo info = new JdbcFieldInfo(
                "id", Optional.of("numeric(18, 0) identity"), 2 /* NUMERIC */,
                Optional.of(18), Optional.of(0), Optional.empty());
        ConnectorType ct = client.jdbcTypeToConnectorType(info);
        Assertions.assertEquals("DECIMALV3", ct.getTypeName(),
                "numeric(18, 0) identity should map to DECIMALV3");
    }

    @Test
    void testIntIdentityTypeMapping() {
        JdbcSQLServerConnectorClient client = createClient();
        JdbcFieldInfo info = new JdbcFieldInfo(
                "id", Optional.of("int identity"), 4 /* INTEGER */,
                Optional.of(10), Optional.of(0), Optional.empty());
        ConnectorType ct = client.jdbcTypeToConnectorType(info);
        Assertions.assertEquals("INT", ct.getTypeName(),
                "int identity should map to INT");
    }

    @Test
    void testBigintIdentityTypeMapping() {
        JdbcSQLServerConnectorClient client = createClient();
        JdbcFieldInfo info = new JdbcFieldInfo(
                "id", Optional.of("bigint identity"), -5 /* BIGINT */,
                Optional.of(19), Optional.of(0), Optional.empty());
        ConnectorType ct = client.jdbcTypeToConnectorType(info);
        Assertions.assertEquals("BIGINT", ct.getTypeName(),
                "bigint identity should map to BIGINT");
    }

    @Test
    void testPlainDecimalTypeMapping() {
        JdbcSQLServerConnectorClient client = createClient();
        JdbcFieldInfo info = new JdbcFieldInfo(
                "amount", Optional.of("decimal"), 3 /* DECIMAL */,
                Optional.of(10), Optional.of(2), Optional.empty());
        ConnectorType ct = client.jdbcTypeToConnectorType(info);
        Assertions.assertEquals("DECIMALV3", ct.getTypeName());
        Assertions.assertEquals(10, ct.getPrecision());
        Assertions.assertEquals(2, ct.getScale());
    }

    @Test
    void testTimestampTypeMapping() {
        JdbcSQLServerConnectorClient client = createClient();
        JdbcFieldInfo info = new JdbcFieldInfo(
                "ts", Optional.of("timestamp"), -2 /* BINARY */,
                Optional.of(8), Optional.of(0), Optional.empty());
        ConnectorType ct = client.jdbcTypeToConnectorType(info);
        Assertions.assertEquals("STRING", ct.getTypeName(),
                "SQL Server timestamp (rowversion) should map to STRING");
    }
}
