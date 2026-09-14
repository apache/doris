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

import java.sql.Types;
import java.util.Collections;
import java.util.Optional;

/**
 * Tests for {@link JdbcSQLServerConnectorClient}, focusing on SQL Server
 * IDENTITY column type name handling and user-defined alias type resolution.
 */
public class JdbcSQLServerConnectorClientTest {

    // ODBC type codes that mssql-jdbc passes through DatabaseMetaData.getColumns() unchanged
    private static final int SQL_VARIANT = -150;
    private static final int SQL_SS_TIMESTAMPOFFSET = -155;

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

    @Test
    void testUseWildcardSchemaPatternForDatabaseNameList() {
        JdbcSQLServerConnectorClient client = createClient();

        Assertions.assertEquals("%", client.getSchemaPatternForDatabaseNameList());
    }

    /**
     * One DatabaseMetaData.getColumns() row as reported by mssql-jdbc. For a user-defined alias type,
     * TYPE_NAME is the alias name while DATA_TYPE, COLUMN_SIZE and DECIMAL_DIGITS describe the base type.
     */
    private static JdbcFieldInfo column(String typeName, int dataType, int columnSize, int decimalDigits) {
        return new JdbcFieldInfo("col", Optional.of(typeName), dataType,
                Optional.of(columnSize), Optional.of(decimalDigits), Optional.empty());
    }

    private static String typeOf(JdbcSQLServerConnectorClient client, JdbcFieldInfo info) {
        return client.jdbcTypeToConnectorType(info).getTypeName();
    }

    @Test
    void testAliasTypeIsResolvedByJdbcTypeCode() {
        JdbcSQLServerConnectorClient client = createClient();

        // CREATE TYPE dbo.customtexttype FROM varchar(50), the case reported in #67793
        Assertions.assertEquals("STRING", typeOf(client, column("customtexttype", Types.VARCHAR, 50, 0)));
        // sysname is a built-in alias over nvarchar(128)
        Assertions.assertEquals("STRING", typeOf(client, column("sysname", Types.NVARCHAR, 128, 0)));
        Assertions.assertEquals("STRING", typeOf(client, column("alias_nchar", Types.NCHAR, 10, 0)));
        Assertions.assertEquals("STRING",
                typeOf(client, column("alias_text", Types.LONGVARCHAR, Integer.MAX_VALUE, 0)));
        Assertions.assertEquals("STRING",
                typeOf(client, column("alias_ntext", Types.LONGNVARCHAR, Integer.MAX_VALUE / 2, 0)));
        Assertions.assertEquals("STRING", typeOf(client, column("alias_time", Types.TIME, 16, 7)));
        // uniqueidentifier is reported as CHAR(36)
        Assertions.assertEquals("STRING", typeOf(client, column("alias_guid", Types.CHAR, 36, 0)));

        Assertions.assertEquals("BOOLEAN", typeOf(client, column("alias_bit", Types.BIT, 1, 0)));
        // SQL Server tinyint is unsigned, so it keeps the SMALLINT mapping of the name based path
        Assertions.assertEquals("SMALLINT", typeOf(client, column("alias_tinyint", Types.TINYINT, 3, 0)));
        Assertions.assertEquals("SMALLINT", typeOf(client, column("alias_smallint", Types.SMALLINT, 5, 0)));
        Assertions.assertEquals("INT", typeOf(client, column("alias_int", Types.INTEGER, 10, 0)));
        Assertions.assertEquals("BIGINT", typeOf(client, column("alias_bigint", Types.BIGINT, 19, 0)));
        Assertions.assertEquals("FLOAT", typeOf(client, column("alias_real", Types.REAL, 24, 0)));
        Assertions.assertEquals("DOUBLE", typeOf(client, column("alias_float", Types.DOUBLE, 53, 0)));

        ConnectorType decimal = client.jdbcTypeToConnectorType(column("alias_decimal", Types.DECIMAL, 10, 2));
        Assertions.assertEquals("DECIMALV3", decimal.getTypeName());
        Assertions.assertEquals(10, decimal.getPrecision());
        Assertions.assertEquals(2, decimal.getScale());
        // money is reported as DECIMAL(19,4), the same as the name based mapping produces
        ConnectorType money = client.jdbcTypeToConnectorType(column("alias_money", Types.DECIMAL, 19, 4));
        Assertions.assertEquals("DECIMALV3", money.getTypeName());
        Assertions.assertEquals(19, money.getPrecision());
        Assertions.assertEquals(4, money.getScale());
        // precision beyond DECIMAL128 falls back to STRING like the name based path
        Assertions.assertEquals("STRING", typeOf(client, column("alias_numeric", Types.NUMERIC, 39, 0)));

        Assertions.assertEquals("DATEV2", typeOf(client, column("alias_date", Types.DATE, 10, 0)));
        ConnectorType datetime = client.jdbcTypeToConnectorType(column("alias_datetime", Types.TIMESTAMP, 23, 3));
        Assertions.assertEquals("DATETIMEV2", datetime.getTypeName());
        Assertions.assertEquals(3, datetime.getPrecision());
        // datetime2 defaults to 7 fractional digits, Doris supports at most 6
        ConnectorType datetime2 = client.jdbcTypeToConnectorType(column("alias_datetime2", Types.TIMESTAMP, 27, 7));
        Assertions.assertEquals("DATETIMEV2", datetime2.getTypeName());
        Assertions.assertEquals(6, datetime2.getPrecision());
    }

    @Test
    void testUnknownTypesStayUnsupported() {
        JdbcSQLServerConnectorClient client = createClient();

        // CLR user-defined types are reported as VARBINARY, exactly like an alias over varbinary,
        // so binary codes must not be resolved by the fallback
        Assertions.assertEquals("UNSUPPORTED",
                typeOf(client, column("geometry", Types.VARBINARY, Integer.MAX_VALUE, 0)));
        Assertions.assertEquals("UNSUPPORTED", typeOf(client, column("my_clr_type", Types.VARBINARY, 8000, 0)));
        Assertions.assertEquals("UNSUPPORTED", typeOf(client, column("alias_binary", Types.BINARY, 20, 0)));
        Assertions.assertEquals("UNSUPPORTED",
                typeOf(client, column("alias_image", Types.LONGVARBINARY, Integer.MAX_VALUE, 0)));
        // vendor specific type codes
        Assertions.assertEquals("UNSUPPORTED", typeOf(client, column("sql_variant", SQL_VARIANT, 8000, 0)));
        Assertions.assertEquals("UNSUPPORTED",
                typeOf(client, column("alias_datetimeoffset", SQL_SS_TIMESTAMPOFFSET, 34, 7)));
        // explicitly unsupported system types keep that behavior whatever type code the driver reports
        Assertions.assertEquals("UNSUPPORTED",
                typeOf(client, column("xml", Types.LONGNVARCHAR, Integer.MAX_VALUE / 2, 0)));
        Assertions.assertEquals("UNSUPPORTED",
                typeOf(client, column("json", Types.LONGNVARCHAR, Integer.MAX_VALUE / 2, 0)));
        Assertions.assertEquals("UNSUPPORTED", typeOf(client, column("hierarchyid", Types.VARBINARY, 892, 0)));
    }

    @Test
    void testAliasNamedLikeASystemTypeIsResolvedByJdbcTypeCode() {
        JdbcSQLServerConnectorClient client = createClient();

        // A delimited alias name may contain spaces and parentheses ([int alias], [decimal(18,0) identity]);
        // it is reported as is, and the base type is still what DATA_TYPE says
        Assertions.assertEquals("STRING", typeOf(client, column("int alias", Types.VARCHAR, 50, 0)));
        Assertions.assertEquals("STRING", typeOf(client, column("decimal(18,0) identity", Types.NVARCHAR, 20, 0)));
        Assertions.assertEquals("STRING", typeOf(client, column("int identity", Types.VARCHAR, 10, 0)));
        Assertions.assertEquals("STRING", typeOf(client, column("bigint identity", Types.NVARCHAR, 20, 0)));
        Assertions.assertEquals("DATETIMEV2", typeOf(client, column("varchar(50) alias", Types.TIMESTAMP, 23, 3)));

        // The IDENTITY decoration of a real system type, in the forms the driver versions report it in
        Assertions.assertEquals("INT", typeOf(client, column("int identity", Types.INTEGER, 10, 0)));
        Assertions.assertEquals("BIGINT", typeOf(client, column("bigint identity", Types.BIGINT, 19, 0)));
        Assertions.assertEquals("SMALLINT", typeOf(client, column("tinyint identity", Types.TINYINT, 3, 0)));
        for (String decorated : new String[] {"decimal identity", "decimal() identity",
                "decimal(18,0) IDENTITY(1,1)", "DECIMAL(18, 0) IDENTITY"}) {
            ConnectorType ct = client.jdbcTypeToConnectorType(column(decorated, Types.DECIMAL, 18, 0));
            Assertions.assertEquals("DECIMALV3", ct.getTypeName(), decorated);
            Assertions.assertEquals(18, ct.getPrecision(), decorated);
            Assertions.assertEquals(0, ct.getScale(), decorated);
        }
        ConnectorType numeric = client.jdbcTypeToConnectorType(column("numeric(18, 0) identity", Types.NUMERIC, 18, 0));
        Assertions.assertEquals("DECIMALV3", numeric.getTypeName());
        Assertions.assertEquals(18, numeric.getPrecision());
    }

    @Test
    void testSystemTypeNamesTakePrecedence() {
        JdbcSQLServerConnectorClient client = createClient();

        // the name based mapping is unchanged, the type code is only consulted for unknown names
        Assertions.assertEquals("SMALLINT", typeOf(client, column("tinyint", Types.TINYINT, 3, 0)));
        Assertions.assertEquals("STRING", typeOf(client, column("varbinary", Types.VARBINARY, 20, 0)));
        Assertions.assertEquals("STRING",
                typeOf(client, column("datetimeoffset", SQL_SS_TIMESTAMPOFFSET, 34, 7)));
    }
}
