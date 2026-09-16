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
    void testMySqlInitializesUtcBeforePreparingEachStatement() throws Exception {
        MySQLJdbcExecutor executor = Mockito.mock(MySQLJdbcExecutor.class, Mockito.CALLS_REAL_METHODS);
        java.sql.Connection connection = Mockito.mock(java.sql.Connection.class);
        java.sql.Statement timezoneStatement = Mockito.mock(java.sql.Statement.class);
        java.sql.PreparedStatement statement = Mockito.mock(java.sql.PreparedStatement.class);
        Mockito.when(connection.createStatement()).thenReturn(timezoneStatement);
        Mockito.when(connection.prepareStatement(Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt()))
                .thenReturn(statement);
        Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(statement);
        JdbcDataSourceConfig config = new JdbcDataSourceConfig()
                .setTableType(org.apache.doris.thrift.TOdbcTableType.MYSQL);
        executor.initializeStatement(connection, config.setOp(org.apache.doris.thrift.TJdbcOperation.READ), "SELECT 1");
        executor.initializeStatement(connection, config.setOp(org.apache.doris.thrift.TJdbcOperation.WRITE), "INSERT");
        org.mockito.InOrder order = Mockito.inOrder(connection, timezoneStatement);
        order.verify(connection).createStatement();
        order.verify(timezoneStatement).execute("SET SESSION time_zone = '+00:00'");
        order.verify(timezoneStatement).close();
        order.verify(connection).prepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        order.verify(connection).createStatement();
        order.verify(timezoneStatement).execute("SET SESSION time_zone = '+00:00'");
        order.verify(timezoneStatement).close();
        order.verify(connection).prepareStatement("INSERT");
    }

    @Test
    void testMySqlTimestampWriteUsesExplicitUtcCalendar() throws Exception {
        MySQLJdbcExecutor executor = Mockito.mock(MySQLJdbcExecutor.class, Mockito.CALLS_REAL_METHODS);
        executor.config = new JdbcDataSourceConfig().setTableType(org.apache.doris.thrift.TOdbcTableType.MYSQL);
        executor.preparedStatement = Mockito.mock(java.sql.PreparedStatement.class);
        executor.setTimestampTz(1, UTC_VALUE);
        Mockito.verify(executor.preparedStatement).setTimestamp(Mockito.eq(1), Mockito.eq(Timestamp.from(INSTANT)),
                Mockito.argThat(calendar -> calendar.getTimeZone().getID().equals("UTC")));
    }

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
    void testPostgreSqlTimestampArraysRetainInstants() throws Exception {
        PostgreSQLJdbcExecutor executor = Mockito.mock(PostgreSQLJdbcExecutor.class, Mockito.CALLS_REAL_METHODS);
        ColumnType type = ColumnType.parseType("events", "array<array<timestamptz(6)>>");
        java.util.TimeZone previous = java.util.TimeZone.getDefault();
        try {
            java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("America/New_York"));
            // The two instants have identical local fields during the DST overlap.
            Instant first = Instant.parse("2021-11-07T05:30:00.123456Z");
            Instant second = Instant.parse("2021-11-07T06:30:00.123456Z");
            Object[] input = {java.util.Arrays.asList(new Timestamp[] {
                    Timestamp.from(first), Timestamp.from(second), null}, null)};
            Object[] output = executor.getOutputConverter(type, "").convert(input);
            Assertions.assertEquals(java.util.Arrays.asList(java.util.Arrays.asList(
                    LocalDateTime.ofInstant(first, ZoneOffset.UTC),
                    LocalDateTime.ofInstant(second, ZoneOffset.UTC), null), null), output[0]);
        } finally {
            java.util.TimeZone.setDefault(previous);
        }
    }

    @Test
    void testOceanBaseTimestampUsesMySqlUtcContract() throws Exception {
        MySQLJdbcExecutor executor = Mockito.mock(MySQLJdbcExecutor.class, Mockito.CALLS_REAL_METHODS);
        executor.config = new JdbcDataSourceConfig().setTableType(org.apache.doris.thrift.TOdbcTableType.OCEANBASE);
        executor.resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(executor.resultSet.getTimestamp(Mockito.eq(1), Mockito.any(java.util.Calendar.class)))
                .thenReturn(Timestamp.from(INSTANT));
        Assertions.assertEquals(UTC_VALUE, executor.getColumnValue(0,
                ColumnType.parseType("event_time", "timestamptz(6)"), new String[0]));
        executor.preparedStatement = Mockito.mock(java.sql.PreparedStatement.class);
        executor.setTimestampTz(1, UTC_VALUE);
        Mockito.verify(executor.preparedStatement).setTimestamp(Mockito.eq(1), Mockito.eq(Timestamp.from(INSTANT)),
                Mockito.argThat(calendar -> calendar.getTimeZone().getID().equals("UTC")));
        java.sql.Connection connection = Mockito.mock(java.sql.Connection.class);
        java.sql.Statement statement = Mockito.mock(java.sql.Statement.class);
        Mockito.when(connection.createStatement()).thenReturn(statement);
        Mockito.when(connection.prepareStatement(Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt()))
                .thenReturn(executor.preparedStatement);
        executor.initializeStatement(connection, executor.config.setOp(org.apache.doris.thrift.TJdbcOperation.READ),
                "SELECT 1");
        Mockito.verify(statement).execute("SET SESSION time_zone = '+00:00'");
        Mockito.verify(statement).close();
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
        BaseJdbcExecutor executor = Mockito.mock(PostgreSQLJdbcExecutor.class, Mockito.CALLS_REAL_METHODS);
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
        BaseJdbcExecutor executor = Mockito.mock(SQLServerJdbcExecutor.class, Mockito.CALLS_REAL_METHODS);
        executor.resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(executor.resultSet.getTimestamp(1)).thenReturn(Timestamp.from(INSTANT)).thenReturn(null);
        ColumnType type = ColumnType.parseType("event_time", "timestamptz(6)");
        Assertions.assertEquals(UTC_VALUE, executor.getColumnValue(0, type, new String[0]));
        Assertions.assertNull(executor.getColumnValue(0, type, new String[0]));
    }

    @Test
    void testTrinoTimestampRetainsInstantAndNull() throws Exception {
        verifyZonedRead(TrinoJdbcExecutor.class, ZonedDateTime.class,
                INSTANT.atZone(ZoneOffset.ofHours(8)));
    }

    @Test
    void testClickHouseTimestampRetainsInstantAndNull() throws Exception {
        BaseJdbcExecutor executor = Mockito.mock(ClickHouseJdbcExecutor.class, Mockito.CALLS_REAL_METHODS);
        executor.resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(executor.resultSet.getLong(1)).thenReturn(
                INSTANT.getEpochSecond() * 1_000_000 + INSTANT.getNano() / 1000, -1L, 0L);
        Mockito.when(executor.resultSet.wasNull()).thenReturn(false, false, true);
        ColumnType type = ColumnType.parseType("event_time", "timestamptz(6)");
        Assertions.assertEquals(UTC_VALUE, executor.getColumnValue(0, type, new String[0]));
        Assertions.assertEquals(LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999999000),
                executor.getColumnValue(0, type, new String[0]));
        Assertions.assertNull(executor.getColumnValue(0, type, new String[0]));
    }

    @Test
    void testSqlServerLegacyDriverRetainsInstantAndNull() throws Exception {
        BaseJdbcExecutor executor = Mockito.mock(SQLServerJdbcExecutor.class, Mockito.CALLS_REAL_METHODS);
        executor.resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(executor.resultSet.getObject(1, OffsetDateTime.class))
                .thenThrow(new java.sql.SQLException("The conversion to class java.time.OffsetDateTime is unsupported."));
        Mockito.when(executor.resultSet.getTimestamp(1)).thenReturn(Timestamp.from(INSTANT)).thenReturn(null);
        ColumnType type = ColumnType.parseType("event_time", "timestamptz(6)");
        Assertions.assertEquals(UTC_VALUE, executor.getColumnValue(0, type, new String[0]));
        Assertions.assertNull(executor.getColumnValue(0, type, new String[0]));
        Mockito.verify(executor.resultSet, Mockito.never()).getObject(1, OffsetDateTime.class);
    }

    @Test
    void testSqlServerTimestampReadPropagatesDatabaseErrors() throws Exception {
        BaseJdbcExecutor executor = Mockito.mock(SQLServerJdbcExecutor.class, Mockito.CALLS_REAL_METHODS);
        executor.resultSet = Mockito.mock(ResultSet.class);
        java.sql.SQLException failure = new java.sql.SQLException("Connection closed", "08003");
        Mockito.when(executor.resultSet.getTimestamp(1)).thenThrow(failure);
        Assertions.assertSame(failure, Assertions.assertThrows(java.sql.SQLException.class,
                () -> executor.getColumnValue(0, ColumnType.parseType("event_time", "timestamptz(6)"), new String[0])));
    }

    @Test
    void testSqlServerWriteBindsAnExplicitOffset() throws Exception {
        SQLServerJdbcExecutor executor = Mockito.mock(SQLServerJdbcExecutor.class, Mockito.CALLS_REAL_METHODS);
        executor.preparedStatement = Mockito.mock(java.sql.PreparedStatement.class);
        executor.setTimestampTz(1, UTC_VALUE);
        Mockito.verify(executor.preparedStatement).setString(1, "2020-01-02T04:01:00.111333+00:00");
    }

    @Test
    void testMySqlTimestampRetainsInstantAndNull() throws Exception {
        BaseJdbcExecutor executor = Mockito.mock(MySQLJdbcExecutor.class, Mockito.CALLS_REAL_METHODS);
        executor.config = new JdbcDataSourceConfig().setTableType(org.apache.doris.thrift.TOdbcTableType.MYSQL);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        executor.resultSet = resultSet;
        Mockito.when(resultSet.getTimestamp(Mockito.eq(1), Mockito.argThat((java.util.Calendar calendar) ->
                calendar != null && calendar.getTimeZone().getID().equals("UTC"))))
                .thenReturn(Timestamp.from(INSTANT)).thenReturn(null);
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
