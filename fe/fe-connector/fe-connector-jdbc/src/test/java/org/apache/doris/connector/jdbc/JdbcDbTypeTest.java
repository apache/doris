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

import org.apache.doris.connector.api.DorisConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for JdbcDbType parsing and OceanBase mode handling.
 */
public class JdbcDbTypeTest {

    @Test
    public void testParseFromUrlMySQL() {
        Assertions.assertEquals(JdbcDbType.MYSQL,
                JdbcDbType.parseFromUrl("jdbc:mysql://host:3306/db"));
    }

    @Test
    public void testParseFromUrlMariaDB() {
        Assertions.assertEquals(JdbcDbType.MYSQL,
                JdbcDbType.parseFromUrl("jdbc:mariadb://host:3306/db"));
    }

    @Test
    public void testParseFromUrlPostgreSQL() {
        Assertions.assertEquals(JdbcDbType.POSTGRESQL,
                JdbcDbType.parseFromUrl("jdbc:postgresql://host:5432/db"));
    }

    @Test
    public void testParseFromUrlOracle() {
        Assertions.assertEquals(JdbcDbType.ORACLE,
                JdbcDbType.parseFromUrl("jdbc:oracle:thin:@host:1521:orcl"));
    }

    @Test
    public void testParseFromUrlSQLServer() {
        Assertions.assertEquals(JdbcDbType.SQLSERVER,
                JdbcDbType.parseFromUrl("jdbc:sqlserver://host:1433;database=db"));
    }

    @Test
    public void testParseFromUrlClickHouse() {
        Assertions.assertEquals(JdbcDbType.CLICKHOUSE,
                JdbcDbType.parseFromUrl("jdbc:clickhouse://host:8123/db"));
    }

    @Test
    public void testParseFromUrlSapHana() {
        Assertions.assertEquals(JdbcDbType.SAP_HANA,
                JdbcDbType.parseFromUrl("jdbc:sap://host:30015"));
    }

    @Test
    public void testParseFromUrlTrino() {
        Assertions.assertEquals(JdbcDbType.TRINO,
                JdbcDbType.parseFromUrl("jdbc:trino://host:8080/catalog"));
    }

    @Test
    public void testParseFromUrlPresto() {
        Assertions.assertEquals(JdbcDbType.PRESTO,
                JdbcDbType.parseFromUrl("jdbc:presto://host:8080/catalog"));
    }

    @Test
    public void testParseFromUrlOceanBase() {
        // OceanBase URLs always parse to OCEANBASE (mode detection is runtime)
        Assertions.assertEquals(JdbcDbType.OCEANBASE,
                JdbcDbType.parseFromUrl("jdbc:oceanbase://host:2881/db"));
    }

    @Test
    public void testParseFromUrlDB2() {
        Assertions.assertEquals(JdbcDbType.DB2,
                JdbcDbType.parseFromUrl("jdbc:db2://host:50000/db"));
    }

    @Test
    public void testParseFromUrlGBase() {
        Assertions.assertEquals(JdbcDbType.GBASE,
                JdbcDbType.parseFromUrl("jdbc:gbase://host:5258/db"));
    }

    @Test
    public void testParseFromUrlNull() {
        Assertions.assertThrows(DorisConnectorException.class,
                () -> JdbcDbType.parseFromUrl(null));
    }

    @Test
    public void testParseFromUrlUnsupported() {
        Assertions.assertThrows(DorisConnectorException.class,
                () -> JdbcDbType.parseFromUrl("jdbc:unknown://host/db"));
    }

    @Test
    public void testOceanBaseOracleEnumExists() {
        // Verify OCEANBASE_ORACLE enum value exists and is distinct from OCEANBASE
        JdbcDbType oracleMode = JdbcDbType.OCEANBASE_ORACLE;
        JdbcDbType mysqlMode = JdbcDbType.OCEANBASE;
        Assertions.assertNotEquals(oracleMode, mysqlMode);
        Assertions.assertEquals("OCEANBASE_ORACLE", oracleMode.getLabel());
        Assertions.assertEquals("OCEANBASE", mysqlMode.getLabel());
    }

    @Test
    public void testParseFromUrlWithWhitespace() {
        Assertions.assertEquals(JdbcDbType.MYSQL,
                JdbcDbType.parseFromUrl("  jdbc:mysql://host:3306/db  "));
    }
}
