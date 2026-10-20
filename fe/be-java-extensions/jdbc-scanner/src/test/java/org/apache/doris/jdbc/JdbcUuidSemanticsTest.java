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

package org.apache.doris.jdbc;

import org.apache.doris.common.jni.vec.ColumnType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.UUID;

class JdbcUuidSemanticsTest {
    @Test
    void scalarUuidsUseNativeJniValues() throws Exception {
        UUID value = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
        for (BaseJdbcExecutor executor : Arrays.asList(
                Mockito.mock(PostgreSQLJdbcExecutor.class, Mockito.CALLS_REAL_METHODS),
                Mockito.mock(SQLServerJdbcExecutor.class, Mockito.CALLS_REAL_METHODS),
                Mockito.mock(TrinoJdbcExecutor.class, Mockito.CALLS_REAL_METHODS))) {
            executor.resultSet = Mockito.mock(ResultSet.class);
            Mockito.when(executor.resultSet.getString(1)).thenReturn(value.toString(), null);
            ColumnType type = ColumnType.parseType("id", "uuid");
            Assertions.assertEquals(value, executor.getColumnValue(0, type, new String[0]));
            Assertions.assertNull(executor.getColumnValue(0, type, new String[0]));
        }
    }

    @Test
    void nestedUuidArraysPreserveValuesAndNulls() throws Exception {
        UUID value = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff");
        for (BaseJdbcExecutor executor : Arrays.asList(
                Mockito.mock(PostgreSQLJdbcExecutor.class, Mockito.CALLS_REAL_METHODS),
                Mockito.mock(TrinoJdbcExecutor.class, Mockito.CALLS_REAL_METHODS))) {
            ColumnType type = ColumnType.parseType("ids", "array<uuid>");
            Object[] result = executor.getOutputConverter(type, "")
                    .convert(new Object[] {Arrays.asList(value.toString(), null)});
            Assertions.assertEquals(Arrays.asList(value, null), result[0]);
        }
    }

    @Test
    void driversBindUuidTextAndNull() throws Exception {
        UUID value = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
        for (BaseJdbcExecutor executor : Arrays.asList(
                Mockito.mock(SQLServerJdbcExecutor.class, Mockito.CALLS_REAL_METHODS),
                Mockito.mock(TrinoJdbcExecutor.class, Mockito.CALLS_REAL_METHODS))) {
            executor.preparedStatement = Mockito.mock(java.sql.PreparedStatement.class);
            executor.setUuid(1, value);
            executor.setUuidNull(2);
            Mockito.verify(executor.preparedStatement).setString(1, value.toString());
            Mockito.verify(executor.preparedStatement).setNull(2, java.sql.Types.VARCHAR);
        }
    }

}
