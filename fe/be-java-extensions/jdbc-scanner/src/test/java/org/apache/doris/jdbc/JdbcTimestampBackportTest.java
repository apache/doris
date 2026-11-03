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
import org.apache.doris.thrift.TOdbcTableType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.Arrays;

class JdbcTimestampBackportTest {
    @Test
    void mysqlReadsProjectedTextAndHonorsZeroDates() throws Exception {
        MySQLJdbcExecutor executor = Mockito.mock(MySQLJdbcExecutor.class, Mockito.CALLS_REAL_METHODS);
        executor.config = new JdbcDataSourceConfig().setTableType(TOdbcTableType.MYSQL);
        executor.resultSet = Mockito.mock(ResultSet.class);
        ColumnType type = ColumnType.parseType("ts", "timestamptz(6)");
        Mockito.when(executor.resultSet.getString(1)).thenReturn("2021-11-07 06:30:00.123456");
        Assertions.assertEquals(LocalDateTime.parse("2021-11-07T06:30:00.123456"),
                executor.getColumnValue(0, type, new String[0]));
        Mockito.when(executor.resultSet.getString(1)).thenReturn("0000-00-00 00:00:00");
        Assertions.assertNull(executor.getColumnValue(0, type, new String[0]));
        Mockito.verify(executor.resultSet).getDate(1);
    }

    @Test
    void prestoReadsZonedStringsInNestedArrays() {
        TrinoJdbcExecutor executor = Mockito.mock(TrinoJdbcExecutor.class, Mockito.CALLS_REAL_METHODS);
        executor.config = new JdbcDataSourceConfig().setTableType(TOdbcTableType.PRESTO);
        ColumnType type = ColumnType.parseType("ts", "array<array<timestamptz(6)>>");
        Object input = Arrays.asList(Arrays.asList("1969-12-31 23:59:59.999999 UTC", null));
        Object expected = Arrays.asList(Arrays.asList(LocalDateTime.parse("1969-12-31T23:59:59.999999"), null));
        Assertions.assertEquals(expected, executor.getOutputConverter(type, "").convert(new Object[] {input})[0]);
    }

    @Test
    void legacyDriversBindZonedTimestampsAsText() throws Exception {
        LocalDateTime value = LocalDateTime.parse("2021-11-07T06:30:00.123456");
        OracleJdbcExecutor oracle = Mockito.mock(OracleJdbcExecutor.class, Mockito.CALLS_REAL_METHODS);
        oracle.preparedStatement = Mockito.mock(PreparedStatement.class);
        oracle.setTimestampTz(1, value);
        Mockito.verify(oracle.preparedStatement).setString(1, "2021-11-07 06:30:00.123456 +00:00");
        TrinoJdbcExecutor presto = Mockito.mock(TrinoJdbcExecutor.class, Mockito.CALLS_REAL_METHODS);
        presto.config = new JdbcDataSourceConfig().setTableType(TOdbcTableType.PRESTO);
        presto.preparedStatement = Mockito.mock(PreparedStatement.class);
        presto.setTimestampTz(1, value);
        Mockito.verify(presto.preparedStatement).setString(1, "2021-11-07 06:30:00.123456 UTC");
    }
}
