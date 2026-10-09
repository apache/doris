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

import org.apache.doris.jni.spi.vec.ColumnType;

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
    void oracleAndTrinoRejectOutOfRangeUtcInstants() throws Exception {
        ResultSet rs = Mockito.mock(ResultSet.class);
        ColumnType scalar = ColumnType.parseType("event_time", "timestamptz(6)");
        ColumnType array = ColumnType.parseType("events", "array<timestamptz(6)>");
        OracleTypeHandler oracle = new OracleTypeHandler();
        TrinoTypeHandler trino = new TrinoTypeHandler();
        for (LocalDateTime value : new LocalDateTime[] {
                LocalDateTime.of(-1, 1, 1, 0, 0), LocalDateTime.of(10000, 1, 1, 0, 0)}) {
            Timestamp timestamp = Timestamp.from(value.toInstant(ZoneOffset.UTC));
            Mockito.when(rs.getTimestamp(1)).thenReturn(timestamp);
            Mockito.when(rs.getObject(1, ZonedDateTime.class)).thenReturn(value.atZone(ZoneOffset.UTC));
            Assertions.assertThrows(IllegalArgumentException.class, () -> oracle.getColumnValue(rs, 1, scalar, null));
            Assertions.assertThrows(IllegalArgumentException.class, () -> trino.getColumnValue(rs, 1, scalar, null));
            Assertions.assertThrows(IllegalArgumentException.class, () -> trino.getOutputConverter(array, "")
                    .convert(new Object[] {java.util.Collections.singletonList(timestamp)}));
        }
    }

    @Test
    void testPostgreSqlTimestampRejectsUnrepresentableCalendarYears() throws Exception {
        PostgreSQLTypeHandler executor = new PostgreSQLTypeHandler();
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        ColumnType type = ColumnType.parseType("event_time", "timestamptz(6)");
        for (OffsetDateTime value : new OffsetDateTime[] {
                OffsetDateTime.MAX, OffsetDateTime.MIN,
                OffsetDateTime.of(294276, 12, 31, 23, 59, 59, 999999000, ZoneOffset.UTC),
                OffsetDateTime.of(-4712, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC),
                OffsetDateTime.of(0, 1, 1, 0, 0, 0, 0, ZoneOffset.ofHours(8)),
                OffsetDateTime.of(9999, 12, 31, 23, 59, 59, 0, ZoneOffset.ofHours(-8))}) {
            Mockito.when(resultSet.getObject(1, OffsetDateTime.class)).thenReturn(value);
            Assertions.assertNull(executor.getColumnValue(resultSet, 1, type, null));
        }
        Mockito.when(resultSet.getObject(1, OffsetDateTime.class))
                .thenReturn(OffsetDateTime.ofInstant(INSTANT, ZoneOffset.ofHours(8)));
        Object value = executor.getColumnValue(resultSet, 1, type, null);
        Assertions.assertEquals(UTC_VALUE, executor.getOutputConverter(type, "").convert(new Object[] {value})[0]);
        for (OffsetDateTime boundary : new OffsetDateTime[] {
                OffsetDateTime.of(0, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC),
                OffsetDateTime.of(10000, 1, 1, 0, 0, 0, 0, ZoneOffset.ofHours(8))}) {
            Mockito.when(resultSet.getObject(1, OffsetDateTime.class)).thenReturn(boundary);
            Assertions.assertEquals(boundary.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime(),
                    executor.getColumnValue(resultSet, 1, type, null));
        }
    }

    @Test
    void testMySqlInitializesUtcBeforePreparingEachStatement() throws Exception {
        MySQLTypeHandler executor = new MySQLTypeHandler("MYSQL");
        java.sql.Connection connection = Mockito.mock(java.sql.Connection.class);
        java.sql.Statement timezoneStatement = Mockito.mock(java.sql.Statement.class);
        java.sql.PreparedStatement statement = Mockito.mock(java.sql.PreparedStatement.class);
        Mockito.when(connection.createStatement()).thenReturn(timezoneStatement);
        Mockito.when(connection.prepareStatement(Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt()))
                .thenReturn(statement);
        Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(statement);
        executor.initializeStatement(connection, "SELECT 1", 100);
        executor.initializeWriteConnection(connection);
        connection.prepareStatement("INSERT");
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
    void testMySqlTimestampWriteUsesUtcSessionFields() throws Exception {
        MySQLTypeHandler executor = new MySQLTypeHandler("MYSQL");
        java.sql.PreparedStatement preparedStatement = Mockito.mock(java.sql.PreparedStatement.class);
        executor.setTimestampTz(preparedStatement, 1, UTC_VALUE);
        Mockito.verify(preparedStatement).setString(1, "2020-01-02 04:01:00.111333");
    }

    @Test
    void testNestedTrinoTimestampRetainsInstant() throws Exception {
        TrinoTypeHandler executor = new TrinoTypeHandler();
        java.lang.reflect.Method convert = TrinoTypeHandler.class.getDeclaredMethod("convertArray",
                java.util.List.class, ColumnType.class);
        convert.setAccessible(true);
        ColumnType child = ColumnType.parseType("element", "timestamptz(6)");
        Assertions.assertEquals(java.util.Arrays.asList(UTC_VALUE, null),
                convert.invoke(executor, java.util.Arrays.asList(Timestamp.from(INSTANT), null), child));
    }

    @Test
    void testTrinoProjectedOverlapInstantsAndNestedArrays() throws Exception {
        TrinoTypeHandler executor = new TrinoTypeHandler();
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        java.util.TimeZone previous = java.util.TimeZone.getDefault();
        try {
            java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("America/Los_Angeles"));
            Instant first = Instant.parse("2023-11-05T08:30:00.123456Z");
            Instant second = Instant.parse("2023-11-05T09:30:00.123456Z");
            Instant negative = Instant.parse("1969-12-31T23:59:59.999999Z");
            // Remote UTC projections keep the two fold instants distinct before JDBC decoding.
            Mockito.when(resultSet.getObject(1, ZonedDateTime.class))
                    .thenReturn(first.atZone(ZoneOffset.UTC), second.atZone(ZoneOffset.UTC), null);
            ColumnType scalar = ColumnType.parseType("event_time", "timestamptz(6)");
            Assertions.assertEquals(LocalDateTime.ofInstant(first, ZoneOffset.UTC),
                    executor.getColumnValue(resultSet, 1, scalar, null));
            Assertions.assertEquals(LocalDateTime.ofInstant(second, ZoneOffset.UTC),
                    executor.getColumnValue(resultSet, 1, scalar, null));
            Assertions.assertNull(executor.getColumnValue(resultSet, 1, scalar, null));
            ColumnType nested = ColumnType.parseType("events", "array<array<timestamptz(6)>>");
            Object input = java.util.Arrays.asList(java.util.Arrays.asList(
                    Timestamp.from(first), Timestamp.from(second), Timestamp.from(negative), null),
                    null, java.util.Collections.emptyList());
            Object expected = java.util.Arrays.asList(java.util.Arrays.asList(
                    LocalDateTime.ofInstant(first, ZoneOffset.UTC), LocalDateTime.ofInstant(second, ZoneOffset.UTC),
                    LocalDateTime.ofInstant(negative, ZoneOffset.UTC), null), null, java.util.Collections.emptyList());
            Assertions.assertEquals(expected, executor.getOutputConverter(nested, "")
                    .convert(new Object[] {input})[0]);
        } finally {
            java.util.TimeZone.setDefault(previous);
        }
    }

    @Test
    void postgresArrayUsesTypedElementsForYearZero() throws Exception {
        PostgreSQLTypeHandler handler = new PostgreSQLTypeHandler();
        ColumnType type = ColumnType.parseType("events", "array<timestamptz(6)>");
        ResultSet result = Mockito.mock(ResultSet.class);
        java.sql.Array array = Mockito.mock(java.sql.Array.class);
        ResultSet elements = Mockito.mock(ResultSet.class);
        Mockito.when(result.getArray(1)).thenReturn(array);
        Mockito.when(array.getResultSet()).thenReturn(elements);
        Mockito.when(elements.next()).thenReturn(true, true, false);
        LocalDateTime minimum = LocalDateTime.of(0, 1, 1, 0, 0);
        Mockito.when(elements.getObject(2, OffsetDateTime.class)).thenReturn(minimum.atOffset(ZoneOffset.UTC), null);
        Object values = handler.getColumnValue(result, 1, type, null);
        Assertions.assertEquals(java.util.Arrays.asList(minimum, null),
                handler.getOutputConverter(type, "").convert(new Object[] {values})[0]);
        Mockito.verify(elements).close();
        Mockito.verify(array).free();
    }

    @Test
    void testPostgreSqlTimestampArraysRetainInstants() throws Exception {
        PostgreSQLTypeHandler executor = new PostgreSQLTypeHandler();
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
        MySQLTypeHandler executor = new MySQLTypeHandler("OCEANBASE");
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(resultSet.getString(1)).thenReturn(UTC_VALUE.toString().replace('T', ' '));
        Assertions.assertEquals(UTC_VALUE, executor.getColumnValue(resultSet, 1,
                ColumnType.parseType("event_time", "timestamptz(6)"), null));
        java.sql.PreparedStatement preparedStatement = Mockito.mock(java.sql.PreparedStatement.class);
        executor.setTimestampTz(preparedStatement, 1, UTC_VALUE);
        Mockito.verify(preparedStatement).setString(1, UTC_VALUE.toString().replace('T', ' '));
        java.sql.Connection connection = Mockito.mock(java.sql.Connection.class);
        java.sql.Statement statement = Mockito.mock(java.sql.Statement.class);
        Mockito.when(connection.createStatement()).thenReturn(statement);
        Mockito.when(connection.prepareStatement(Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt()))
                .thenReturn(preparedStatement);
        executor.initializeStatement(connection, "SELECT 1", 100);
        Mockito.verify(statement).execute("SET SESSION time_zone = '+00:00'");
        Mockito.verify(statement).close();
    }

    @Test
    void testClickHouseTimestampArrayUsesEpochMicros() throws Exception {
        ClickHouseTypeHandler executor = new ClickHouseTypeHandler();
        java.lang.reflect.Method convert = ClickHouseTypeHandler.class.getDeclaredMethod("convertArray",
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
    void testSqlServerTimestampRetainsInstantAndNull() throws Exception {
        SQLServerTypeHandler executor = new SQLServerTypeHandler();
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(resultSet.getTimestamp(1)).thenReturn(Timestamp.from(INSTANT)).thenReturn(null);
        ColumnType type = ColumnType.parseType("event_time", "timestamptz(6)");
        Assertions.assertEquals(UTC_VALUE, executor.getColumnValue(resultSet, 1, type, null));
        Assertions.assertNull(executor.getColumnValue(resultSet, 1, type, null));
    }

    @Test
    void testTrinoTimestampRetainsInstantAndNull() throws Exception {
        verifyZonedRead(TrinoTypeHandler.class, ZonedDateTime.class,
                INSTANT.atZone(ZoneOffset.ofHours(8)));
    }

    @Test
    void testClickHouseTimestampRetainsInstantAndNull() throws Exception {
        ClickHouseTypeHandler executor = new ClickHouseTypeHandler();
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(resultSet.getLong(1)).thenReturn(
                INSTANT.getEpochSecond() * 1_000_000 + INSTANT.getNano() / 1000, -1L, 0L);
        Mockito.when(resultSet.wasNull()).thenReturn(false, false, true);
        ColumnType type = ColumnType.parseType("event_time", "timestamptz(6)");
        Assertions.assertEquals(UTC_VALUE, executor.getColumnValue(resultSet, 1, type, null));
        Assertions.assertEquals(LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999999000),
                executor.getColumnValue(resultSet, 1, type, null));
        Assertions.assertNull(executor.getColumnValue(resultSet, 1, type, null));
    }

    @Test
    void testSqlServerLegacyDriverRetainsInstantAndNull() throws Exception {
        SQLServerTypeHandler executor = new SQLServerTypeHandler();
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(resultSet.getObject(1, OffsetDateTime.class))
                .thenThrow(new java.sql.SQLException("The conversion to class java.time.OffsetDateTime is unsupported."));
        Mockito.when(resultSet.getTimestamp(1)).thenReturn(Timestamp.from(INSTANT)).thenReturn(null);
        ColumnType type = ColumnType.parseType("event_time", "timestamptz(6)");
        Assertions.assertEquals(UTC_VALUE, executor.getColumnValue(resultSet, 1, type, null));
        Assertions.assertNull(executor.getColumnValue(resultSet, 1, type, null));
        Mockito.verify(resultSet, Mockito.never()).getObject(1, OffsetDateTime.class);
    }

    @Test
    void testSqlServerTimestampReadPropagatesDatabaseErrors() throws Exception {
        SQLServerTypeHandler executor = new SQLServerTypeHandler();
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        java.sql.SQLException failure = new java.sql.SQLException("Connection closed", "08003");
        Mockito.when(resultSet.getTimestamp(1)).thenThrow(failure);
        Assertions.assertSame(failure, Assertions.assertThrows(java.sql.SQLException.class,
                () -> executor.getColumnValue(resultSet, 1, ColumnType.parseType("event_time", "timestamptz(6)"), null)));
    }

    @Test
    void testSqlServerWriteBindsAnExplicitOffset() throws Exception {
        SQLServerTypeHandler executor = new SQLServerTypeHandler();
        java.sql.PreparedStatement preparedStatement = Mockito.mock(java.sql.PreparedStatement.class);
        executor.setTimestampTz(preparedStatement, 1, UTC_VALUE);
        Mockito.verify(preparedStatement).setString(1, "2020-01-02T04:01:00.111333+00:00");
    }

    @Test
    void testMySqlZeroTimestampKeepsDriverConversionPolicy() throws Exception {
        MySQLTypeHandler executor = new MySQLTypeHandler("MYSQL");
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(resultSet.getString(1)).thenReturn("0000-00-00 00:00:00.000000");
        ColumnType type = ColumnType.parseType("event_time", "timestamptz(6)");
        LocalDateTime rounded = LocalDateTime.of(1, 1, 1, 0, 0);
        java.sql.SQLException rejected = new java.sql.SQLException("Zero date value prohibited");
        Mockito.when(resultSet.getDate(1))
                .thenReturn(null).thenReturn(java.sql.Date.valueOf(rounded.toLocalDate())).thenThrow(rejected);
        Assertions.assertNull(executor.getColumnValue(resultSet, 1, type, null));
        Assertions.assertEquals(rounded, executor.getColumnValue(resultSet, 1, type, null));
        Assertions.assertSame(rejected, Assertions.assertThrows(java.sql.SQLException.class,
                () -> executor.getColumnValue(resultSet, 1, type, null)));
    }

    @Test
    void testMySqlTimestampRetainsInstantAndNull() throws Exception {
        MySQLTypeHandler executor = new MySQLTypeHandler("MYSQL");
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(resultSet.getString(1)).thenReturn(UTC_VALUE.toString().replace('T', ' ')).thenReturn(null);
        ColumnType type = ColumnType.parseType("event_time", "timestamptz(6)");
        Assertions.assertEquals(UTC_VALUE, executor.getColumnValue(resultSet, 1, type, null));
        Assertions.assertNull(executor.getColumnValue(resultSet, 1, type, null));
    }

    @Test
    void testOracleLegacyDriverTimestampRetainsInstantAndNull() throws Exception {
        OracleTypeHandler executor = new OracleTypeHandler();
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(resultSet.getTimestamp(1)).thenReturn(Timestamp.from(INSTANT)).thenReturn(null);
        ColumnType type = ColumnType.parseType("event_time", "timestamptz(6)");
        Assertions.assertEquals(UTC_VALUE, executor.getColumnValue(resultSet, 1, type, null));
        Assertions.assertNull(executor.getColumnValue(resultSet, 1, type, null));
    }

    private <T> void verifyZonedRead(Class<? extends JdbcTypeHandler> executorClass,
            Class<T> valueClass, T value) throws Exception {
        // Only isolate JDBC I/O; the real executor must normalize the returned instant for JNI.
        JdbcTypeHandler executor = executorClass.getDeclaredConstructor().newInstance();
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(resultSet.getObject(1, valueClass)).thenReturn(value).thenReturn(null);
        ColumnType type = ColumnType.parseType("event_time", "timestamptz(6)");
        Assertions.assertEquals(UTC_VALUE, executor.getColumnValue(resultSet, 1, type, null));
        Assertions.assertNull(executor.getColumnValue(resultSet, 1, type, null));
    }

    @Test
    void prestoDbZonedArrayStringsPreserveOffsetsAndNulls() {
        PrestoTypeHandler handler = new PrestoTypeHandler();
        ColumnType type = ColumnType.parseType("events", "array<array<timestamptz(6)>>");
        Object input = java.util.Arrays.asList(java.util.Arrays.asList(
                "2023-11-05 01:30:00.123 -07:00", "2023-11-05 01:30:00.123 -08:00",
                "1969-12-31 23:59:59.999 UTC", null), null, java.util.Collections.emptyList());
        Object expected = java.util.Arrays.asList(java.util.Arrays.asList(
                LocalDateTime.of(2023, 11, 5, 8, 30, 0, 123000000),
                LocalDateTime.of(2023, 11, 5, 9, 30, 0, 123000000),
                LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999000000), null),
                null, java.util.Collections.emptyList());
        Assertions.assertEquals(expected, handler.getOutputConverter(type, "").convert(new Object[] {input})[0]);
    }

}
