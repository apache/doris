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
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Properties;
import java.util.TimeZone;

class JdbcZonedTimestampIntegrationTest {
    @Test
    @EnabledIfSystemProperty(named = "trino.integration.url", matches = ".+")
    void testTrinoUtcProjectionPreservesNamedZoneOverlap() throws Exception {
        verifyUtcProjectionPreservesNamedZoneOverlap("trino", "io.trino.jdbc.TrinoDriver");
    }

    @Test
    @EnabledIfSystemProperty(named = "presto.integration.url", matches = ".+")
    void testPrestoUtcProjectionPreservesNamedZoneOverlap() throws Exception {
        verifyUtcProjectionPreservesNamedZoneOverlap("presto", "io.prestosql.jdbc.PrestoDriver");
    }

    private void verifyUtcProjectionPreservesNamedZoneOverlap(String dialect, String driver) throws Exception {
        // Both dialects must disambiguate the fold before the driver reconstructs the instant.
        withDriver(dialect, driver, connection -> {
            TrinoTypeHandler executor = new TrinoTypeHandler();
            String values = "ARRAY[first_value, second_value, old_value, NULL]";
            String query = "WITH sample AS (SELECT "
                    + "at_timezone(TIMESTAMP '2023-11-05 08:30:00.123456 UTC', 'America/Los_Angeles') first_value, "
                    + "at_timezone(TIMESTAMP '2023-11-05 09:30:00.123456 UTC', 'America/Los_Angeles') second_value, "
                    + "TIMESTAMP '1969-12-31 23:59:59.999999 UTC' old_value) "
                    + "SELECT at_timezone(first_value, 'UTC'), at_timezone(second_value, 'UTC'), "
                    + "at_timezone(CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE), 'UTC'), "
                    + "transform(" + values + ", t -> at_timezone(t, 'UTC')), "
                    + "transform(ARRAY[" + values + ", CAST(NULL AS ARRAY(TIMESTAMP(6) WITH TIME ZONE)), ARRAY[]], "
                    + "t0 -> transform(t0, t1 -> at_timezone(t1, 'UTC'))) FROM sample";
            try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery(query)) {
                Assertions.assertTrue(rows.next());
                assertInstant(executor, rows, 0, "2023-11-05T08:30:00.123456Z");
                assertInstant(executor, rows, 1, "2023-11-05T09:30:00.123456Z");
                assertInstant(executor, rows, 2, null);
                java.util.List<LocalDateTime> expected = Arrays.asList(
                        LocalDateTime.of(2023, 11, 5, 8, 30, 0, 123456000),
                        LocalDateTime.of(2023, 11, 5, 9, 30, 0, 123456000),
                        LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999999000), null);
                for (int i = 3; i < 5; ++i) {
                    ColumnType type = ColumnType.parseType("events", i == 3
                            ? "array<timestamptz(6)>" : "array<array<timestamptz(6)>>");
                    Object raw = executor.getColumnValue(rows, i + 1, type, null);
                    Assertions.assertEquals(i == 3 ? expected
                                    : Arrays.asList(expected, null, java.util.Collections.emptyList()),
                            executor.getOutputConverter(type, "").convert(new Object[] {raw})[0]);
                }
            }
        });
    }

    @Test
    @EnabledIfSystemProperty(named = "postgresql.integration.url", matches = ".+")
    void testPostgreSqlUnrepresentableTimestampRange() throws Exception {
        withDriver("postgresql", "org.postgresql.Driver", connection -> {
            PostgreSQLTypeHandler executor = new PostgreSQLTypeHandler();
            String values = "'infinity'::timestamptz, '-infinity'::timestamptz, "
                    + "'294276-12-31 23:59:59.999999+00'::timestamptz, "
                    + "'4713-01-01 00:00:00+00 BC'::timestamptz";
            try (Statement statement = connection.createStatement();
                    ResultSet rows = statement.executeQuery("SELECT " + values + ", ARRAY[" + values + "]")) {
                Assertions.assertTrue(rows.next());
                for (int i = 0; i < 4; ++i) {
                    assertInstant(executor, rows, i, null);
                }
                ColumnType array = ColumnType.parseType("events", "array<timestamptz(6)>");
                Object raw = executor.getColumnValue(rows, 5, array, null);
                Assertions.assertEquals(Arrays.asList(null, null, null, null),
                        executor.getOutputConverter(array, "").convert(new Object[] {raw})[0]);
            }
        });
    }

    @Test
    @EnabledIfSystemProperty(named = "postgresql.integration.url", matches = ".+")
    void testPostgreSqlTimestampArraysAndDstOverlap() throws Exception {
        withDriver("postgresql", "org.postgresql.Driver", connection -> {
            PostgreSQLTypeHandler executor = new PostgreSQLTypeHandler();
            for (String zone : Arrays.asList("UTC", "Asia/Shanghai", "America/New_York")) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("SET TIME ZONE '" + zone + "'");
                    String array = "ARRAY['2021-11-07 01:30:00.123456-04'::timestamptz, "
                            + "'2021-11-07 01:30:00.123456-05'::timestamptz, NULL, "
                            + "'1969-12-31 23:59:59.999999+00'::timestamptz]";
                    try (ResultSet rows = statement.executeQuery("SELECT " + array + ", ARRAY[" + array
                            + "], NULL::timestamptz[]")) {
                        Assertions.assertTrue(rows.next());
                        java.util.List<LocalDateTime> expected = Arrays.asList(
                                LocalDateTime.of(2021, 11, 7, 5, 30, 0, 123456000),
                                LocalDateTime.of(2021, 11, 7, 6, 30, 0, 123456000), null,
                                LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999999000));
                        ColumnType flat = ColumnType.parseType("events", "array<timestamptz(6)>");
                        ColumnType nested = ColumnType.parseType("events", "array<array<timestamptz(6)>>");
                        for (int i = 0; i < 3; ++i) {
                            ColumnType type = i == 1 ? nested : flat;
                            Object raw = executor.getColumnValue(rows, i + 1, type, null);
                            Object result = executor.getOutputConverter(type, "").convert(new Object[] {raw})[0];
                            Assertions.assertEquals(i == 2 ? null : i == 1
                                    ? java.util.Collections.singletonList(expected) : expected, result);
                        }
                    }
                }
            }
        });
    }

    @Test
    @EnabledIfSystemProperty(named = "clickhouse.integration.url", matches = ".+")
    void testClickHouseColumnZonesAndDstOverlap() throws Exception {
        withDriver("clickhouse", "com.clickhouse.jdbc.ClickHouseDriver", connection -> {
            ClickHouseTypeHandler executor = new ClickHouseTypeHandler();
            String query = "SELECT toUnixTimestamp64Micro(toDateTime64(toDateTime(1577966460, 'Asia/Shanghai'), 6)), "
                    + "toUnixTimestamp64Micro(toDateTime64('2020-01-02 12:01:00.111333', 6, 'Asia/Shanghai')), "
                    + "toUnixTimestamp64Micro(toDateTime64("
                    + "fromUnixTimestamp64Micro(1636263000123456, 'America/New_York'), 6)), "
                    + "toUnixTimestamp64Micro(toDateTime64("
                    + "fromUnixTimestamp64Micro(1636266600123456, 'America/New_York'), 6)), "
                    + "toUnixTimestamp64Micro(toDateTime64(CAST(NULL AS Nullable(DateTime64(6, 'Asia/Shanghai'))), 6)), "
                    + "arrayMap(t -> toUnixTimestamp64Micro(toDateTime64(t, 6)), "
                    + "[fromUnixTimestamp64Micro(-1, 'Asia/Shanghai'), "
                    + "CAST(NULL AS Nullable(DateTime64(6, 'Asia/Shanghai')))])";
            try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery(query)) {
                Assertions.assertTrue(rows.next());
                assertInstant(executor, rows, 0, "2020-01-02T12:01:00Z");
                assertInstant(executor, rows, 1, "2020-01-02T04:01:00.111333Z");
                assertInstant(executor, rows, 2, "2021-11-07T05:30:00.123456Z");
                assertInstant(executor, rows, 3, "2021-11-07T06:30:00.123456Z");
                assertInstant(executor, rows, 4, null);
                ColumnType array = ColumnType.parseType("events", "array<timestamptz(6)>");
                Object raw = executor.getColumnValue(rows, 6, array, null);
                java.lang.reflect.Method convert = ClickHouseTypeHandler.class.getDeclaredMethod(
                        "convertArray", java.util.List.class, ColumnType.class);
                convert.setAccessible(true);
                Assertions.assertEquals(Arrays.asList(LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999999000), null),
                        convert.invoke(executor, raw, array.getChildTypes().get(0)));
            }
        });
    }

    @Test
    @EnabledIfSystemProperty(named = "sqlserver.integration.url", matches = ".+")
    void testSqlServerExplicitOffsetsAndDstOverlap() throws Exception {
        withDriver("sqlserver", "com.microsoft.sqlserver.jdbc.SQLServerDriver", connection -> {
            SQLServerTypeHandler executor = new SQLServerTypeHandler();
            String query = "SELECT CAST('2020-01-02T12:01:00.111333+08:00' AS datetimeoffset(6)), "
                    + "CAST('2020-01-02T09:46:00.111333+05:45' AS datetimeoffset(6)), "
                    + "CAST('2021-11-07T01:30:00.123456-04:00' AS datetimeoffset(6)), "
                    + "CAST('2021-11-07T01:30:00.123456-05:00' AS datetimeoffset(6)), "
                    + "CAST(NULL AS datetimeoffset(6))";
            try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery(query)) {
                Assertions.assertTrue(rows.next());
                assertInstant(executor, rows, 0, "2020-01-02T04:01:00.111333Z");
                assertInstant(executor, rows, 1, "2020-01-02T04:01:00.111333Z");
                assertInstant(executor, rows, 2, "2021-11-07T05:30:00.123456Z");
                assertInstant(executor, rows, 3, "2021-11-07T06:30:00.123456Z");
                assertInstant(executor, rows, 4, null);
            }
        });
    }

    private void assertInstant(JdbcTypeHandler executor, ResultSet rows, int index, String expected) throws Exception {
        Assertions.assertEquals(expected == null ? null : LocalDateTime.ofInstant(Instant.parse(expected), ZoneOffset.UTC),
                executor.getColumnValue(rows, index + 1, ColumnType.parseType("ts", "timestamptz(6)"), null));
    }

    @Test
    @EnabledIfSystemProperty(named = "sqlserver.integration.url", matches = ".+")
    void testSqlServerTimestampWriteRetainsInstantAndPrecision() throws Exception {
        withDriver("sqlserver", "com.microsoft.sqlserver.jdbc.SQLServerDriver", connection -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE #timestamp_roundtrip (id INT, event_time datetimeoffset(6))");
            }
            SQLServerTypeHandler executor = new SQLServerTypeHandler();
            String[] values = {"2020-01-02T04:01:00.111333Z", "2021-11-07T05:30:00.123456Z",
                "2021-11-07T06:30:00.123456Z"};
            try (java.sql.PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO #timestamp_roundtrip VALUES (?, ?)")) {
                for (int i = 0; i < values.length; ++i) {
                    statement.setInt(1, i);
                    executor.setTimestampTz(statement, 2, LocalDateTime.ofInstant(Instant.parse(values[i]), ZoneOffset.UTC));
                    statement.executeUpdate();
                }
            }
            try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery(
                    "SELECT event_time FROM #timestamp_roundtrip ORDER BY id")) {
                for (String value : values) {
                    Assertions.assertTrue(rows.next());
                    assertInstant(executor, rows, 0, value);
                }
                Assertions.assertFalse(rows.next());
            }
        });
    }

    @Test
    @EnabledIfSystemProperty(named = "postgresql.integration.url", matches = ".+")
    void testPostgresTimestampWriteRoundTrip() throws Exception {
        verifyTimestampWriteRoundTrip("postgresql", "org.postgresql.Driver", PostgreSQLTypeHandler.class,
                "SET TIME ZONE 'America/New_York'", new String[] {"TIMESTAMPTZ(6)"}, "");
    }

    @Test
    @EnabledIfSystemProperty(named = "oracle.integration.url", matches = ".+")
    void testOracleTimestampWriteRoundTrip() throws Exception {
        verifyTimestampWriteRoundTrip("oracle", "oracle.jdbc.OracleDriver", OracleTypeHandler.class,
                "ALTER SESSION SET TIME_ZONE = '-07:00'",
                new String[] {"TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP(6) WITH LOCAL TIME ZONE"}, "");
    }

    @Test
    @EnabledIfSystemProperty(named = "clickhouse.integration.url", matches = ".+")
    void testClickHouseTimestampWriteRoundTrip() throws Exception {
        verifyTimestampWriteRoundTrip("clickhouse", "com.clickhouse.jdbc.ClickHouseDriver", ClickHouseTypeHandler.class,
                null, new String[] {"Nullable(DateTime64(6, 'Asia/Tokyo'))",
                    "Nullable(DateTime64(6, 'America/Los_Angeles'))"}, " ENGINE = Memory");
    }

    @Test
    @EnabledIfSystemProperty(named = "trino.integration.url", matches = ".+")
    void testTrinoTimestampWriteRoundTrip() throws Exception {
        verifyTimestampWriteRoundTrip("trino", "io.trino.jdbc.TrinoDriver", TrinoTypeHandler.class,
                null, new String[] {"TIMESTAMP(6) WITH TIME ZONE"}, "");
    }

    @Test
    @EnabledIfSystemProperty(named = "presto.integration.url", matches = ".+")
    void testPrestoTimestampWriteRoundTrip() throws Exception {
        verifyTimestampWriteRoundTrip("presto", "io.prestosql.jdbc.PrestoDriver", PrestoTypeHandler.class,
                null, new String[] {"TIMESTAMP(6) WITH TIME ZONE"}, "");
    }

    @Test
    @EnabledIfSystemProperty(named = "prestodb.integration.url", matches = ".+")
    void testOfficialPrestoDbTimestampWriteRoundTrip() throws Exception {
        verifyTimestampWriteRoundTrip("prestodb", "com.facebook.presto.jdbc.PrestoDriver", PrestoTypeHandler.class,
                null, new String[] {"TIMESTAMP WITH TIME ZONE"}, "");
    }

    @Test
    @EnabledIfSystemProperty(named = "prestodb.integration.url", matches = ".+")
    void testOfficialPrestoDbNestedTimestampArrays() throws Exception {
        withDriver("prestodb", "com.facebook.presto.jdbc.PrestoDriver", connection -> {
            PrestoTypeHandler handler = new PrestoTypeHandler();
            ColumnType type = ColumnType.parseType("events", "array<array<timestamptz(6)>>");
            try (Statement statement = connection.createStatement();
                    ResultSet result = statement.executeQuery("SELECT ARRAY[ARRAY["
                            + "TIMESTAMP '2023-11-05 01:30:00.123 -07:00',"
                            + "TIMESTAMP '2023-11-05 01:30:00.123 -08:00',"
                            + "TIMESTAMP '1969-12-31 23:59:59.999 UTC',"
                            + "CAST(NULL AS TIMESTAMP WITH TIME ZONE)], NULL, ARRAY[]]")) {
                Assertions.assertTrue(result.next());
                Object raw = handler.getColumnValue(result, 1, type, result.getMetaData());
                Object converted = handler.getOutputConverter(type, "").convert(new Object[] {raw})[0];
                Assertions.assertEquals(java.util.Arrays.asList(java.util.Arrays.asList(
                        LocalDateTime.of(2023, 11, 5, 8, 30, 0, 123000000),
                        LocalDateTime.of(2023, 11, 5, 9, 30, 0, 123000000),
                        LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999000000), null),
                        null, java.util.Collections.emptyList()), converted);
            }
        });
    }

    @Test
    @EnabledIfSystemProperty(named = "oraclelegacy.integration.url", matches = ".+")
    void testOracleLegacyDriverTimestampWriteRoundTrip() throws Exception {
        verifyTimestampWriteRoundTrip("oraclelegacy", "oracle.jdbc.OracleDriver", OracleTypeHandler.class,
                "ALTER SESSION SET TIME_ZONE = '-07:00'",
                new String[] {"TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP(6) WITH LOCAL TIME ZONE"}, "");
    }

    private void verifyTimestampWriteRoundTrip(String prefix, String driver,
            Class<? extends JdbcTypeHandler> dialect, String sessionSql, String[] types, String suffix) throws Exception {
        withDriver(prefix, driver, connection -> {
            String table = "doris_tz_" + java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 12);
            java.util.List<String> definitions = new java.util.ArrayList<>();
            java.util.List<String> projections = new java.util.ArrayList<>();
            for (int i = 0; i < types.length; ++i) {
                String column = "event_time" + i;
                definitions.add(column + " " + types[i]);
                projections.add(dialect == ClickHouseTypeHandler.class ? "toUnixTimestamp64Micro(" + column + ")"
                        : (dialect == TrinoTypeHandler.class || dialect == PrestoTypeHandler.class)
                                ? "at_timezone(" + column + ", 'UTC')" : column);
            }
            if (dialect == TrinoTypeHandler.class || dialect == PrestoTypeHandler.class) {
                // The driver API sets the remote session zone independently of the JVM default.
                connection.getClass().getMethod("setTimeZoneId", String.class).invoke(connection, "America/New_York");
            }
            try (Statement ddl = connection.createStatement()) {
                if (sessionSql != null) {
                    ddl.execute(sessionSql);
                }
                ddl.execute("CREATE TABLE " + table + " (id INT, " + String.join(", ", definitions) + ")" + suffix);
                try {
                    JdbcTypeHandler executor = dialect.getDeclaredConstructor().newInstance();
                    String[] values = {"2020-01-02T04:01:00.111333Z", "1969-12-31T23:59:59.999999Z",
                        "2023-11-05T08:30:00.123456Z", "2023-11-05T09:30:00.123456Z", "2020-01-02T00:00:00Z", null};
                    if ("prestodb".equals(prefix)) {
                        // PrestoDB timestamps have millisecond precision, unlike PrestoSQL's timestamp(6).
                        for (int i = 0; i < values.length; i++) {
                            if (values[i] != null) {
                                values[i] = Instant.parse(values[i]).truncatedTo(java.time.temporal.ChronoUnit.MILLIS)
                                        .toString();
                            }
                        }
                    }
                    String zonedParameter = dialect == OracleTypeHandler.class
                            ? "TO_TIMESTAMP_TZ(?, 'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')"
                            : dialect == PrestoTypeHandler.class ? "CAST(? AS TIMESTAMP(6) WITH TIME ZONE)" : "?";
                    if ("prestodb".equals(prefix)) {
                        zonedParameter = "CAST(? AS TIMESTAMP WITH TIME ZONE)";
                    }
                    String parameters = "?, " + String.join(", ",
                            java.util.Collections.nCopies(types.length, zonedParameter));
                    try (java.sql.PreparedStatement insert = connection.prepareStatement(
                            "INSERT INTO " + table + " VALUES (" + parameters + ")")) {
                        for (int row = 0; row < values.length; ++row) {
                            insert.setInt(1, row);
                            for (int col = 0; col < types.length; ++col) {
                                if (values[row] == null) {
                                    executor.setTimestampTzNull(insert, col + 2);
                                } else {
                                    executor.setTimestampTz(insert, col + 2,
                                            LocalDateTime.ofInstant(Instant.parse(values[row]), ZoneOffset.UTC));
                                }
                            }
                            insert.addBatch();
                        }
                        insert.executeBatch();
                    }
                    // Check the stored instant independently of the JVM, remote session, and declared column zone.
                    try (ResultSet rows = ddl.executeQuery("SELECT " + String.join(", ", projections)
                            + " FROM " + table + " ORDER BY id")) {
                        for (String text : values) {
                            Assertions.assertTrue(rows.next());
                            for (int col = 1; col <= types.length; ++col) {
                                Instant actual;
                                if (dialect == ClickHouseTypeHandler.class) {
                                    long micros = rows.getLong(col);
                                    actual = rows.wasNull() ? null : Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000),
                                            Math.floorMod(micros, 1_000_000) * 1000);
                                } else if (dialect == TrinoTypeHandler.class) {
                                    java.time.ZonedDateTime value = rows.getObject(col, java.time.ZonedDateTime.class);
                                    actual = value == null ? null : value.toInstant();
                                } else if (dialect == OracleTypeHandler.class || dialect == PrestoTypeHandler.class) {
                                    java.sql.Timestamp value = rows.getTimestamp(col);
                                    actual = value == null ? null : value.toInstant();
                                } else {
                                    java.time.OffsetDateTime value = rows.getObject(col, java.time.OffsetDateTime.class);
                                    actual = value == null ? null : value.toInstant();
                                }
                                Assertions.assertEquals(text == null ? null : Instant.parse(text), actual);
                                if (dialect == OracleTypeHandler.class || dialect == PrestoTypeHandler.class) {
                                    Assertions.assertEquals(actual == null ? null
                                                    : LocalDateTime.ofInstant(actual, ZoneOffset.UTC),
                                            executor.getColumnValue(rows, col,
                                                    ColumnType.parseType("event_time", "timestamptz(6)"),
                                                    rows.getMetaData()));
                                }
                            }
                        }
                        Assertions.assertFalse(rows.next());
                    }
                } finally {
                    ddl.execute("DROP TABLE " + table);
                }
            }
        });
    }

    private void withDriver(String prefix, String driverClass, Check check) throws Exception {
        TimeZone original = TimeZone.getDefault();
        URL jar = new File(System.getProperty(prefix + ".integration.driverJar")).toURI().toURL();
        try (URLClassLoader loader = new URLClassLoader(new URL[] {jar}, getClass().getClassLoader())) {
            Driver driver = (Driver) loader.loadClass(driverClass).getDeclaredConstructor().newInstance();
            for (String zone : new String[] {"UTC", "Asia/Shanghai", "America/New_York"}) {
                TimeZone.setDefault(TimeZone.getTimeZone(zone));
                Properties properties = new Properties();
                properties.setProperty("user", System.getProperty(prefix + ".integration.user", ""));
                properties.setProperty("password", System.getProperty(prefix + ".integration.password", ""));
                try (Connection connection = driver.connect(System.getProperty(prefix + ".integration.url"), properties)) {
                    check.run(connection);
                }
            }
        } finally {
            TimeZone.setDefault(original);
        }
    }

    private interface Check {
        void run(Connection connection) throws Exception;
    }
}
