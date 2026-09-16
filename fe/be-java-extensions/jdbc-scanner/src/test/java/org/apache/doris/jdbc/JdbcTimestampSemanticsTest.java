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
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

class JdbcTimestampSemanticsTest {
    private static final Instant INSTANT = Instant.parse("2020-01-02T04:01:00.111333Z");
    private static final LocalDateTime UTC_VALUE = LocalDateTime.ofInstant(INSTANT, ZoneOffset.UTC);

    @Test
    void testNestedTrinoTimestampRetainsInstant() throws Exception {
        TrinoJdbcExecutor executor = Mockito.mock(TrinoJdbcExecutor.class, Mockito.CALLS_REAL_METHODS);
        java.lang.reflect.Method convert = TrinoJdbcExecutor.class.getDeclaredMethod("convertArray",
                java.util.List.class, ColumnType.class);
        convert.setAccessible(true);
        ColumnType child = ColumnType.parseType("element", "timestamptz(6)");
        Assertions.assertEquals(java.util.Arrays.asList(UTC_VALUE, null),
                convert.invoke(executor, java.util.Arrays.asList(Timestamp.from(INSTANT), null), child));
    }

    @Test
    void testClickHouseTimestampArrayUsesEpochMicros() throws Exception {
        ClickHouseJdbcExecutor executor = Mockito.mock(ClickHouseJdbcExecutor.class, Mockito.CALLS_REAL_METHODS);
        java.lang.reflect.Method convert = ClickHouseJdbcExecutor.class.getDeclaredMethod("convertArray",
                java.util.List.class, ColumnType.class);
        convert.setAccessible(true);
        ColumnType child = ColumnType.parseType("element", "timestamptz(6)");
        long micros = INSTANT.getEpochSecond() * 1_000_000 + INSTANT.getNano() / 1000;
        Assertions.assertEquals(java.util.Arrays.asList(UTC_VALUE, null,
                LocalDateTime.ofInstant(Instant.ofEpochSecond(-1, 999999000), ZoneOffset.UTC)),
                convert.invoke(executor, java.util.Arrays.asList(micros, null, -1L), child));
        Assertions.assertEquals(java.util.Collections.singletonList(java.util.Arrays.asList(UTC_VALUE, null)),
                convert.invoke(executor, java.util.Collections.singletonList(new Long[] {micros, null}),
                        ColumnType.parseType("element", "array<timestamptz(6)>")));
    }

    @Test
    void testTimestampWriteDoesNotUseJvmTimezone() throws Exception {
        BaseJdbcExecutor executor = Mockito.mock(MySQLJdbcExecutor.class, Mockito.CALLS_REAL_METHODS);
        executor.preparedStatement = Mockito.mock(java.sql.PreparedStatement.class);
        org.apache.doris.common.jni.vec.VectorColumn column =
                Mockito.mock(org.apache.doris.common.jni.vec.VectorColumn.class);
        Mockito.when(column.getColumnPrimitiveType()).thenReturn(ColumnType.Type.TIMESTAMPTZ);
        Mockito.when(column.getTimeStampTz(0)).thenReturn(UTC_VALUE);
        java.lang.reflect.Method insert = BaseJdbcExecutor.class.getDeclaredMethod("insertColumn",
                int.class, int.class, org.apache.doris.common.jni.vec.VectorColumn.class);
        insert.setAccessible(true);
        java.util.TimeZone previous = java.util.TimeZone.getDefault();
        try {
            java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("Asia/Shanghai"));
            insert.invoke(executor, 0, 0, column);
            Mockito.verify(executor.preparedStatement).setObject(1, Timestamp.from(INSTANT));
        } finally {
            java.util.TimeZone.setDefault(previous);
        }
    }

    @Test
    void testSqlServerTimestampRetainsInstantAndNull() throws Exception {
        verifyZonedRead(SQLServerJdbcExecutor.class, OffsetDateTime.class,
                INSTANT.atOffset(ZoneOffset.ofHours(8)));
    }

    @Test
    void testTrinoTimestampRetainsInstantAndNull() throws Exception {
        verifyZonedRead(TrinoJdbcExecutor.class, ZonedDateTime.class,
                INSTANT.atZone(ZoneOffset.ofHours(8)));
    }

    @Test
    void testClickHouseTimestampRetainsInstantAndNull() throws Exception {
        verifyZonedRead(ClickHouseJdbcExecutor.class, ZonedDateTime.class,
                INSTANT.atZone(ZoneOffset.ofHours(8)));
    }

    @Test
    void testMySqlTimestampRetainsInstantAndNull() throws Exception {
        BaseJdbcExecutor executor = Mockito.mock(MySQLJdbcExecutor.class, Mockito.CALLS_REAL_METHODS);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        executor.resultSet = resultSet;
        Mockito.when(resultSet.getTimestamp(1)).thenReturn(Timestamp.from(INSTANT)).thenReturn(null);
        ColumnType type = ColumnType.parseType("event_time", "timestamptz(6)");
        Assertions.assertEquals(UTC_VALUE, executor.getColumnValue(0, type, new String[0]));
        Assertions.assertNull(executor.getColumnValue(0, type, new String[0]));
    }

    @Test
    void testOracleLegacyDriverTimestampRetainsInstantAndNull() throws Exception {
        BaseJdbcExecutor executor = Mockito.mock(OracleJdbcExecutor.class, Mockito.CALLS_REAL_METHODS);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        executor.resultSet = resultSet;
        Mockito.when(resultSet.getTimestamp(1)).thenReturn(Timestamp.from(INSTANT)).thenReturn(null);
        ColumnType type = ColumnType.parseType("event_time", "timestamptz(6)");
        Assertions.assertEquals(UTC_VALUE, executor.getColumnValue(0, type, new String[0]));
        Assertions.assertNull(executor.getColumnValue(0, type, new String[0]));
    }

    private <T> void verifyZonedRead(Class<? extends BaseJdbcExecutor> executorClass,
            Class<T> valueClass, T value) throws Exception {
        // Only isolate JDBC I/O; the real executor must normalize the returned instant for JNI.
        BaseJdbcExecutor executor = Mockito.mock(executorClass, Mockito.CALLS_REAL_METHODS);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        executor.resultSet = resultSet;
        Mockito.when(resultSet.getObject(1, valueClass)).thenReturn(value).thenReturn(null);
        ColumnType type = ColumnType.parseType("event_time", "timestamptz(6)");
        Assertions.assertEquals(UTC_VALUE, executor.getColumnValue(0, type, new String[0]));
        Assertions.assertNull(executor.getColumnValue(0, type, new String[0]));
    }
}
