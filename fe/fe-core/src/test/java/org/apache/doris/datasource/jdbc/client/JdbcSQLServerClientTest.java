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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.sql.Types;

public class JdbcSQLServerClientTest {

    private JdbcSQLServerClient client;

    @BeforeEach
    public void setUp() {
        client = Mockito.mock(JdbcSQLServerClient.class);
        Mockito.doCallRealMethod().when(client).jdbcTypeToDoris(Mockito.any());
    }

    @Test
    public void testUserDefinedVarcharAliasTypeMapping() throws Exception {
        JdbcFieldSchema fieldSchema = fieldSchema("customtexttype", Types.VARCHAR, 50, 0);
        Type dorisType = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(ScalarType.createStringType(), dorisType);
    }

    @Test
    public void testUnknownTypeNameWithVarbinaryStaysUnsupported() throws Exception {
        JdbcFieldSchema fieldSchema = fieldSchema("geometry", Types.VARBINARY, 8000, 0);
        Type dorisType = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.UNSUPPORTED, dorisType);
    }

    @Test
    public void testPlainVarcharStillMapsByName() throws Exception {
        JdbcFieldSchema fieldSchema = fieldSchema("varchar", Types.VARCHAR, 50, 0);
        Type dorisType = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(ScalarType.createStringType(), dorisType);
    }

    private static JdbcFieldSchema fieldSchema(String typeName, int dataType, int columnSize, int scale)
            throws Exception {
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getString("COLUMN_NAME")).thenReturn("col");
        Mockito.when(rs.getInt("DATA_TYPE")).thenReturn(dataType);
        Mockito.when(rs.getString("TYPE_NAME")).thenReturn(typeName);
        Mockito.when(rs.getInt("COLUMN_SIZE")).thenReturn(columnSize);
        Mockito.when(rs.getInt("DECIMAL_DIGITS")).thenReturn(scale);
        Mockito.when(rs.wasNull()).thenReturn(false);
        Mockito.when(rs.getInt("NUM_PREC_RADIX")).thenReturn(10);
        Mockito.when(rs.getInt("NULLABLE")).thenReturn(1);
        Mockito.when(rs.getString("REMARKS")).thenReturn(null);
        Mockito.when(rs.getInt("CHAR_OCTET_LENGTH")).thenReturn(columnSize * 3);
        return new JdbcFieldSchema(rs);
    }
}
