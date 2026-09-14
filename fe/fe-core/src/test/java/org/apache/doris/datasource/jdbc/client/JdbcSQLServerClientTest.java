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

package org.apache.doris.datasource.jdbc.client;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.jdbc.util.JdbcFieldSchema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.Mockito;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

public class JdbcSQLServerClientTest {

    // ODBC type codes that mssql-jdbc passes through DatabaseMetaData.getColumns() unchanged
    private static final int SQL_VARIANT = -150;
    private static final int SQL_SS_TIMESTAMPOFFSET = -155;

    private final JdbcSQLServerClient client = Mockito.mock(JdbcSQLServerClient.class, Answers.CALLS_REAL_METHODS);

    /**
     * Builds the schema of one DatabaseMetaData.getColumns() row as reported by mssql-jdbc.
     * For a user-defined alias type, TYPE_NAME is the alias name while DATA_TYPE, COLUMN_SIZE
     * and DECIMAL_DIGITS describe the base type.
     */
    private static JdbcFieldSchema column(String typeName, int dataType, int columnSize, int decimalDigits)
            throws SQLException {
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getString("COLUMN_NAME")).thenReturn("col");
        Mockito.when(rs.getInt("DATA_TYPE")).thenReturn(dataType);
        Mockito.when(rs.getString("TYPE_NAME")).thenReturn(typeName);
        Mockito.when(rs.getInt("COLUMN_SIZE")).thenReturn(columnSize);
        Mockito.when(rs.getInt("DECIMAL_DIGITS")).thenReturn(decimalDigits);
        Mockito.when(rs.getInt("NULLABLE")).thenReturn(DatabaseMetaData.columnNullable);
        return new JdbcFieldSchema(rs);
    }

    @Test
    public void testAliasTypeIsResolvedByJdbcTypeCode() throws SQLException {
        // CREATE TYPE dbo.customtexttype FROM varchar(50), the case reported in #67793
        Assertions.assertEquals(Type.STRING, client.jdbcTypeToDoris(column("customtexttype", Types.VARCHAR, 50, 0)));
        // sysname is a built-in alias over nvarchar(128)
        Assertions.assertEquals(Type.STRING, client.jdbcTypeToDoris(column("sysname", Types.NVARCHAR, 128, 0)));
        Assertions.assertEquals(Type.STRING, client.jdbcTypeToDoris(column("alias_nchar", Types.NCHAR, 10, 0)));
        Assertions.assertEquals(Type.STRING,
                client.jdbcTypeToDoris(column("alias_text", Types.LONGVARCHAR, Integer.MAX_VALUE, 0)));
        Assertions.assertEquals(Type.STRING,
                client.jdbcTypeToDoris(column("alias_ntext", Types.LONGNVARCHAR, Integer.MAX_VALUE / 2, 0)));
        Assertions.assertEquals(Type.STRING, client.jdbcTypeToDoris(column("alias_time", Types.TIME, 16, 7)));
        // uniqueidentifier is reported as CHAR(36)
        Assertions.assertEquals(Type.STRING, client.jdbcTypeToDoris(column("alias_guid", Types.CHAR, 36, 0)));

        Assertions.assertEquals(Type.BOOLEAN, client.jdbcTypeToDoris(column("alias_bit", Types.BIT, 1, 0)));
        // SQL Server tinyint is unsigned, so it keeps the SMALLINT mapping of the name based path
        Assertions.assertEquals(Type.SMALLINT, client.jdbcTypeToDoris(column("alias_tinyint", Types.TINYINT, 3, 0)));
        Assertions.assertEquals(Type.SMALLINT, client.jdbcTypeToDoris(column("alias_smallint", Types.SMALLINT, 5, 0)));
        Assertions.assertEquals(Type.INT, client.jdbcTypeToDoris(column("alias_int", Types.INTEGER, 10, 0)));
        Assertions.assertEquals(Type.BIGINT, client.jdbcTypeToDoris(column("alias_bigint", Types.BIGINT, 19, 0)));
        Assertions.assertEquals(Type.FLOAT, client.jdbcTypeToDoris(column("alias_real", Types.REAL, 24, 0)));
        Assertions.assertEquals(Type.DOUBLE, client.jdbcTypeToDoris(column("alias_float", Types.DOUBLE, 53, 0)));

        Assertions.assertEquals(ScalarType.createDecimalV3Type(10, 2),
                client.jdbcTypeToDoris(column("alias_decimal", Types.DECIMAL, 10, 2)));
        Assertions.assertEquals(ScalarType.createDecimalV3Type(38, 10),
                client.jdbcTypeToDoris(column("alias_numeric", Types.NUMERIC, 38, 10)));
        // money is reported as DECIMAL(19,4), the same as the name based mapping produces
        Assertions.assertEquals(ScalarType.createDecimalV3Type(19, 4),
                client.jdbcTypeToDoris(column("alias_money", Types.DECIMAL, 19, 4)));

        Assertions.assertEquals(Type.DATEV2, client.jdbcTypeToDoris(column("alias_date", Types.DATE, 10, 0)));
        Assertions.assertEquals(ScalarType.createDatetimeV2Type(3),
                client.jdbcTypeToDoris(column("alias_datetime", Types.TIMESTAMP, 23, 3)));
        // datetime2 defaults to 7 fractional digits, Doris supports at most 6
        Assertions.assertEquals(ScalarType.createDatetimeV2Type(6),
                client.jdbcTypeToDoris(column("alias_datetime2", Types.TIMESTAMP, 27, 7)));
        Assertions.assertEquals(ScalarType.createDatetimeV2Type(0),
                client.jdbcTypeToDoris(column("alias_smalldatetime", Types.TIMESTAMP, 16, 0)));
    }

    @Test
    public void testUnknownTypesStayUnsupported() throws SQLException {
        // CLR user-defined types are reported as VARBINARY, exactly like an alias over varbinary,
        // so binary codes must not be resolved by the fallback
        Assertions.assertEquals(Type.UNSUPPORTED,
                client.jdbcTypeToDoris(column("geometry", Types.VARBINARY, Integer.MAX_VALUE, 0)));
        Assertions.assertEquals(Type.UNSUPPORTED,
                client.jdbcTypeToDoris(column("my_clr_type", Types.VARBINARY, 8000, 0)));
        Assertions.assertEquals(Type.UNSUPPORTED, client.jdbcTypeToDoris(column("alias_binary", Types.BINARY, 20, 0)));
        Assertions.assertEquals(Type.UNSUPPORTED,
                client.jdbcTypeToDoris(column("alias_image", Types.LONGVARBINARY, Integer.MAX_VALUE, 0)));
        // vendor specific type codes
        Assertions.assertEquals(Type.UNSUPPORTED, client.jdbcTypeToDoris(column("sql_variant", SQL_VARIANT, 8000, 0)));
        Assertions.assertEquals(Type.UNSUPPORTED,
                client.jdbcTypeToDoris(column("alias_datetimeoffset", SQL_SS_TIMESTAMPOFFSET, 34, 7)));
        // explicitly unsupported system types keep that behavior whatever type code the driver reports
        Assertions.assertEquals(Type.UNSUPPORTED,
                client.jdbcTypeToDoris(column("xml", Types.LONGNVARCHAR, Integer.MAX_VALUE / 2, 0)));
        Assertions.assertEquals(Type.UNSUPPORTED,
                client.jdbcTypeToDoris(column("json", Types.LONGNVARCHAR, Integer.MAX_VALUE / 2, 0)));
        Assertions.assertEquals(Type.UNSUPPORTED,
                client.jdbcTypeToDoris(column("hierarchyid", Types.VARBINARY, 892, 0)));
    }

    @Test
    public void testAliasNamedLikeASystemTypeIsResolvedByJdbcTypeCode() throws SQLException {
        // A delimited alias name may contain spaces and parentheses ([int alias], [decimal(18,0) identity]);
        // it is reported as is, and the base type is still what DATA_TYPE says
        Assertions.assertEquals(Type.STRING, client.jdbcTypeToDoris(column("int alias", Types.VARCHAR, 50, 0)));
        Assertions.assertEquals(Type.STRING,
                client.jdbcTypeToDoris(column("decimal(18,0) identity", Types.NVARCHAR, 20, 0)));
        Assertions.assertEquals(Type.STRING, client.jdbcTypeToDoris(column("int identity", Types.VARCHAR, 10, 0)));
        Assertions.assertEquals(Type.STRING,
                client.jdbcTypeToDoris(column("bigint identity", Types.NVARCHAR, 20, 0)));
        Assertions.assertEquals(ScalarType.createDatetimeV2Type(3),
                client.jdbcTypeToDoris(column("varchar(50) alias", Types.TIMESTAMP, 23, 3)));

        // The IDENTITY decoration of a real system type, in the forms the driver versions report it in
        Assertions.assertEquals(Type.INT, client.jdbcTypeToDoris(column("int identity", Types.INTEGER, 10, 0)));
        Assertions.assertEquals(Type.BIGINT, client.jdbcTypeToDoris(column("bigint identity", Types.BIGINT, 19, 0)));
        Assertions.assertEquals(Type.SMALLINT,
                client.jdbcTypeToDoris(column("tinyint identity", Types.TINYINT, 3, 0)));
        Assertions.assertEquals(ScalarType.createDecimalV3Type(18, 0),
                client.jdbcTypeToDoris(column("decimal identity", Types.DECIMAL, 18, 0)));
        Assertions.assertEquals(ScalarType.createDecimalV3Type(18, 0),
                client.jdbcTypeToDoris(column("decimal() identity", Types.DECIMAL, 18, 0)));
        Assertions.assertEquals(ScalarType.createDecimalV3Type(18, 0),
                client.jdbcTypeToDoris(column("numeric(18, 0) identity", Types.NUMERIC, 18, 0)));
        Assertions.assertEquals(ScalarType.createDecimalV3Type(18, 0),
                client.jdbcTypeToDoris(column("decimal(18,0) IDENTITY(1,1)", Types.DECIMAL, 18, 0)));
    }

    @Test
    public void testSystemTypeNamesTakePrecedence() throws SQLException {
        // the name based mapping is unchanged, the type code is only consulted for unknown names
        Assertions.assertEquals(Type.SMALLINT, client.jdbcTypeToDoris(column("tinyint", Types.TINYINT, 3, 0)));
        Assertions.assertEquals(Type.INT, client.jdbcTypeToDoris(column("int identity", Types.INTEGER, 10, 0)));
        Assertions.assertEquals(ScalarType.createDecimalV3Type(19, 4),
                client.jdbcTypeToDoris(column("money", Types.DECIMAL, 19, 4)));
        Assertions.assertEquals(Type.STRING, client.jdbcTypeToDoris(column("varbinary", Types.VARBINARY, 20, 0)));
        Assertions.assertEquals(Type.STRING, client.jdbcTypeToDoris(column("timestamp", Types.BINARY, 8, 0)));
        Assertions.assertEquals(Type.STRING,
                client.jdbcTypeToDoris(column("datetimeoffset", SQL_SS_TIMESTAMPOFFSET, 34, 7)));
    }
}
