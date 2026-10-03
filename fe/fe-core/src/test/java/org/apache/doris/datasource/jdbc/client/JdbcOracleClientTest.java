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

public class JdbcOracleClientTest {
    private final JdbcOracleClient client = Mockito.mock(JdbcOracleClient.class, Answers.CALLS_REAL_METHODS);

    private static JdbcFieldSchema column(String typeName, int dataType) throws SQLException {
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(resultSet.getString("COLUMN_NAME")).thenReturn("col");
        Mockito.when(resultSet.getInt("DATA_TYPE")).thenReturn(dataType);
        Mockito.when(resultSet.getString("TYPE_NAME")).thenReturn(typeName);
        Mockito.when(resultSet.getInt("COLUMN_SIZE")).thenReturn(8);
        Mockito.when(resultSet.getInt("DECIMAL_DIGITS")).thenReturn(0);
        Mockito.when(resultSet.getInt("NULLABLE")).thenReturn(DatabaseMetaData.columnNullable);
        return new JdbcFieldSchema(resultSet);
    }

    @Test
    public void testOracleBinaryFloatingPointTypes() throws SQLException {
        Assertions.assertEquals(Type.FLOAT,
                client.jdbcTypeToDoris(column("BINARY_FLOAT", Types.FLOAT)));
        Assertions.assertEquals(Type.DOUBLE,
                client.jdbcTypeToDoris(column("BINARY_DOUBLE", Types.DOUBLE)));
    }
}
