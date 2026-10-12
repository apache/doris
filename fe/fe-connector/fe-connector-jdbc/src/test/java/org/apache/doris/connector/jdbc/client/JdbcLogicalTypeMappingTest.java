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

import org.apache.doris.connector.jdbc.JdbcDbType;
import org.apache.doris.connector.spi.ConnectorType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.Collections;
import java.util.Optional;

/**
 * The binary and instant mappings are determined by source semantics; the deprecated
 * {@code enable.mapping.*} flags must not select a different external logical type.
 */
class JdbcLogicalTypeMappingTest {

    @Test
    void binaryTypesKeepBytesAndDeclaredLength() {
        for (boolean flag : new boolean[] {false, true}) {
            assertVarbinary(new JdbcMySQLConnectorClient("test", JdbcDbType.MYSQL, "jdbc:mysql://localhost/test",
                    false, Collections.emptyMap(), Collections.emptyMap(), flag, flag),
                    "BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB");
            assertVarbinary(postgres(flag), "bytea");
            assertVarbinary(new JdbcOracleConnectorClient("test", JdbcDbType.ORACLE, "jdbc:oracle:thin:@localhost",
                    false, Collections.emptyMap(), Collections.emptyMap(), flag, flag), "BLOB");
            assertVarbinary(sqlServer(flag), "binary", "varbinary", "image");
            assertVarbinary(new JdbcDB2ConnectorClient("test", JdbcDbType.DB2, "jdbc:db2://localhost/test",
                    false, Collections.emptyMap(), Collections.emptyMap(), flag, flag), "BLOB", "BINARY", "VARBINARY");
        }
    }

    @Test
    void remoteDorisVarbinaryKeepsBytesAndDeclaredLength() throws ReflectiveOperationException {
        for (boolean flag : new boolean[] {false, true}) {
            JdbcMySQLConnectorClient doris = new JdbcMySQLConnectorClient("test", JdbcDbType.MYSQL,
                    "jdbc:mysql://localhost/test", false, Collections.emptyMap(), Collections.emptyMap(), flag, flag);
            // A remote Doris is detected through the connection, which a unit test does not have.
            Field isDoris = JdbcMySQLConnectorClient.class.getDeclaredField("isDoris");
            isDoris.setAccessible(true);
            isDoris.setBoolean(doris, true);
            assertVarbinary(doris, "VARBINARY");
        }
    }

    @Test
    void instantTimestampsMapToTimestamptzWhileLocalTimestampsStayDatetime() {
        for (boolean flag : new boolean[] {false, true}) {
            JdbcConnectorClient mysql = new JdbcMySQLConnectorClient("test", JdbcDbType.MYSQL,
                    "jdbc:mysql://localhost/test", false, Collections.emptyMap(), Collections.emptyMap(), flag, flag);
            assertTimestamp(mysql, "TIMESTAMP", true);
            assertTimestamp(mysql, "DATETIME", false);
            assertTimestamp(postgres(flag), "timestamptz", true);
            assertTimestamp(postgres(flag), "timestamp", false);
            JdbcConnectorClient oracle = new JdbcOracleConnectorClient("test", JdbcDbType.ORACLE,
                    "jdbc:oracle:thin:@localhost", false, Collections.emptyMap(), Collections.emptyMap(), flag, flag);
            assertTimestamp(oracle, "TIMESTAMP(6) WITH LOCAL TIME ZONE", true);
            assertTimestamp(oracle, "TIMESTAMP(6) WITH TIME ZONE", true);
            assertTimestamp(oracle, "TIMESTAMP(6)", false);
            JdbcConnectorClient trino = new JdbcTrinoConnectorClient("test", JdbcDbType.TRINO,
                    "jdbc:trino://localhost", false, Collections.emptyMap(), Collections.emptyMap(), flag, flag);
            assertTimestamp(trino, "timestamp(6) with time zone", true);
            assertTimestamp(trino, "timestamp(6)", false);
            assertTimestamp(sqlServer(flag), "datetimeoffset", true);
            assertTimestamp(sqlServer(flag), "datetime2", false);
            assertTimestamp(new JdbcClickHouseConnectorClient("test", JdbcDbType.CLICKHOUSE,
                    "jdbc:clickhouse://localhost/test", false, Collections.emptyMap(), Collections.emptyMap(),
                    flag, flag), "DateTime64(6, 'UTC')", true);
        }
    }

    @Test
    void postgresInstantsAllowDecoderCreatedNulls() {
        // Out-of-range and infinite timestamptz values decode to NULL, so a source NOT NULL cannot hold.
        JdbcFieldInfo instant = timestampField("timestamptz");
        instant.setAllowNull(false);
        postgres(false).jdbcTypeToConnectorType(instant);
        Assertions.assertTrue(instant.isAllowNull());

        JdbcFieldInfo local = timestampField("timestamp");
        local.setAllowNull(false);
        postgres(false).jdbcTypeToConnectorType(local);
        Assertions.assertFalse(local.isAllowNull());
    }

    private static JdbcConnectorClient postgres(boolean flag) {
        return new JdbcPostgreSQLConnectorClient("test", JdbcDbType.POSTGRESQL, "jdbc:postgresql://localhost/test",
                false, Collections.emptyMap(), Collections.emptyMap(), flag, flag);
    }

    private static JdbcConnectorClient sqlServer(boolean flag) {
        return new JdbcSQLServerConnectorClient("test", JdbcDbType.SQLSERVER, "jdbc:sqlserver://localhost",
                false, Collections.emptyMap(), Collections.emptyMap(), flag, flag);
    }

    private static JdbcFieldInfo timestampField(String typeName) {
        return new JdbcFieldInfo("ts", Optional.of(typeName), Types.TIMESTAMP,
                Optional.of(26), Optional.of(6), Optional.empty());
    }

    private static void assertVarbinary(JdbcConnectorClient client, String... typeNames) {
        for (String typeName : typeNames) {
            ConnectorType type = client.jdbcTypeToConnectorType(new JdbcFieldInfo("b", Optional.of(typeName),
                    Types.VARBINARY, Optional.of(32), Optional.of(0), Optional.empty()));
            Assertions.assertEquals("VARBINARY", type.getTypeName(), typeName);
            Assertions.assertEquals(32, type.getPrecision(), typeName);
        }
    }

    private static void assertTimestamp(JdbcConnectorClient client, String typeName, boolean instant) {
        ConnectorType type = client.jdbcTypeToConnectorType(timestampField(typeName));
        Assertions.assertEquals(instant ? "TIMESTAMPTZ" : "DATETIMEV2", type.getTypeName(), typeName);
        Assertions.assertEquals(6, type.getPrecision(), typeName);
    }
}
