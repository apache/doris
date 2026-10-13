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

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.datasource.jdbc.util.JdbcFieldSchema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * The legacy fe-core clients (still used by CDC schema discovery) map binary and instant types by source
 * semantics; the deprecated mapping flags must not restore the STRING or unzoned DATETIME contract.
 */
public class JdbcClientTest {

    @Test
    public void testBinaryMappingsKeepBytesAndDeclaredLength() throws Exception {
        assertBinaryMapping(JdbcMySQLClient.class, "BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB");
        assertBinaryMapping(JdbcPostgreSQLClient.class, "bytea");
        assertBinaryMapping(JdbcOracleClient.class, "BLOB");
        assertBinaryMapping(JdbcSQLServerClient.class, "binary", "varbinary", "image");
        assertBinaryMapping(JdbcDB2Client.class, "BLOB");
        JdbcClientConfig config = new JdbcClientConfig().setEnableMappingVarbinary(false);
        Assertions.assertTrue(config.isEnableMappingVarbinary());
    }

    private void assertBinaryMapping(Class<? extends JdbcClient> clientClass, String... names) throws Exception {
        JdbcClient client = Mockito.mock(clientClass, Mockito.CALLS_REAL_METHODS);
        for (String name : names) {
            JdbcFieldSchema field = new JdbcFieldSchema(Mockito.mock(ResultSet.class));
            field.setDataTypeName(Optional.of(name));
            field.setColumnSize(Optional.of(32));
            Assertions.assertEquals(ScalarType.createVarbinaryType(32), client.jdbcTypeToDoris(field), name);
        }
    }

    @Test
    public void testTimestampMappingsIgnoreLegacyFlag() throws Exception {
        assertTimestampMapping(JdbcMySQLClient.class, "TIMESTAMP", true);
        assertTimestampMapping(JdbcMySQLClient.class, "DATETIME", false);
        assertTimestampMapping(JdbcPostgreSQLClient.class, "timestamptz", true);
        assertTimestampMapping(JdbcPostgreSQLClient.class, "timestamp", false);
        assertTimestampMapping(JdbcOracleClient.class, "TIMESTAMP(6) WITH LOCAL TIME ZONE", true);
        assertTimestampMapping(JdbcOracleClient.class, "TIMESTAMP(6)", false);
    }

    @Test
    public void testExplicitZonedJdbcTimestampMappings() throws Exception {
        assertTimestampMapping(JdbcTrinoClient.class, "timestamp(6) with time zone", true);
        assertTimestampMapping(JdbcTrinoClient.class, "timestamp(6)", false);
        assertTimestampMapping(JdbcSQLServerClient.class, "datetimeoffset", true);
        assertTimestampMapping(JdbcSQLServerClient.class, "datetime2", false);
        assertTimestampMapping(JdbcOracleClient.class, "TIMESTAMP(6) WITH TIME ZONE", true);
        assertTimestampMapping(JdbcClickHouseClient.class, "DateTime64(6, 'UTC')", true);
    }

    private void assertTimestampMapping(Class<? extends JdbcClient> clientClass, String typeName,
            boolean withZone) throws Exception {
        // Avoid opening remote connections; execute the real metadata conversion with either legacy flag.
        JdbcClient client = Mockito.mock(clientClass, Mockito.CALLS_REAL_METHODS);
        JdbcFieldSchema field = new JdbcFieldSchema(Mockito.mock(ResultSet.class));
        field.setDataTypeName(Optional.of(typeName));
        field.setColumnSize(Optional.of(26));
        field.setDecimalDigits(Optional.of(6));
        for (boolean flag : new boolean[] {false, true}) {
            client.enableMappingTimestampTz = flag;
            Assertions.assertEquals(withZone ? ScalarType.createTimeStampTzType(6)
                    : ScalarType.createDatetimeV2Type(6), client.jdbcTypeToDoris(field), typeName);
        }
    }

    @Test
    public void testPostgresTimestampMappingAllowsDecoderCreatedNulls() throws Exception {
        JdbcPostgreSQLClient client = Mockito.mock(JdbcPostgreSQLClient.class, Mockito.CALLS_REAL_METHODS);
        JdbcFieldSchema instant = new JdbcFieldSchema(Mockito.mock(ResultSet.class));
        instant.setColumnName("event_time");
        instant.setDataTypeName(Optional.of("timestamptz"));
        instant.setDecimalDigits(Optional.of(6));
        instant.setAllowNull(false);
        JdbcFieldSchema local = new JdbcFieldSchema(instant);
        local.setColumnName("local_time");
        local.setDataTypeName(Optional.of("timestamp"));
        JdbcFieldSchema array = new JdbcFieldSchema(instant);
        array.setColumnName("events");
        array.setDataTypeName(Optional.of("_timestamptz"));
        array.setDataType(Types.ARRAY);
        array.setArrayDimensions(Optional.of(2));
        Mockito.doReturn(Arrays.asList(instant, local, array)).when(client)
                .getJdbcColumnsInfo("sample_schema", "sample_table");

        List<Column> columns = client.getColumnsFromJdbc("sample_schema", "sample_table");

        // Source NOT NULL cannot prevent the timestamp range conversion from creating NULL.
        Assertions.assertTrue(columns.get(0).isAllowNull());
        Assertions.assertTrue(columns.get(0).getType().isTimeStampTz());
        Assertions.assertFalse(columns.get(1).isAllowNull());
        Assertions.assertFalse(columns.get(2).isAllowNull());
        ArrayType arrayType = (ArrayType) columns.get(2).getType();
        Assertions.assertTrue(arrayType.getContainsNull());
        Assertions.assertTrue(((ArrayType) arrayType.getItemType()).getContainsNull());
    }
}
