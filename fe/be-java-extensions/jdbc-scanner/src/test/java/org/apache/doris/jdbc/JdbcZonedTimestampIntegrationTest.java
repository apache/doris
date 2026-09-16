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
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockito.Mockito;

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
    @EnabledIfSystemProperty(named = "clickhouse.integration.url", matches = ".+")
    void testClickHouseColumnZonesAndDstOverlap() throws Exception {
        withDriver("clickhouse", "com.clickhouse.jdbc.ClickHouseDriver", connection -> {
            ClickHouseJdbcExecutor executor = Mockito.mock(ClickHouseJdbcExecutor.class, Mockito.CALLS_REAL_METHODS);
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
                executor.resultSet = rows;
                Assertions.assertTrue(rows.next());
                assertInstant(executor, 0, "2020-01-02T12:01:00Z");
                assertInstant(executor, 1, "2020-01-02T04:01:00.111333Z");
                assertInstant(executor, 2, "2021-11-07T05:30:00.123456Z");
                assertInstant(executor, 3, "2021-11-07T06:30:00.123456Z");
                assertInstant(executor, 4, null);
                ColumnType array = ColumnType.parseType("events", "array<timestamptz(6)>");
                Object raw = executor.getColumnValue(5, array, new String[0]);
                java.lang.reflect.Method convert = ClickHouseJdbcExecutor.class.getDeclaredMethod(
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
            SQLServerJdbcExecutor executor = Mockito.mock(SQLServerJdbcExecutor.class, Mockito.CALLS_REAL_METHODS);
            String query = "SELECT CAST('2020-01-02T12:01:00.111333+08:00' AS datetimeoffset(6)), "
                    + "CAST('2020-01-02T09:46:00.111333+05:45' AS datetimeoffset(6)), "
                    + "CAST('2021-11-07T01:30:00.123456-04:00' AS datetimeoffset(6)), "
                    + "CAST('2021-11-07T01:30:00.123456-05:00' AS datetimeoffset(6)), "
                    + "CAST(NULL AS datetimeoffset(6))";
            try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery(query)) {
                executor.resultSet = rows;
                Assertions.assertTrue(rows.next());
                assertInstant(executor, 0, "2020-01-02T04:01:00.111333Z");
                assertInstant(executor, 1, "2020-01-02T04:01:00.111333Z");
                assertInstant(executor, 2, "2021-11-07T05:30:00.123456Z");
                assertInstant(executor, 3, "2021-11-07T06:30:00.123456Z");
                assertInstant(executor, 4, null);
            }
        });
    }

    private void assertInstant(BaseJdbcExecutor executor, int index, String expected) throws Exception {
        Assertions.assertEquals(expected == null ? null : LocalDateTime.ofInstant(Instant.parse(expected), ZoneOffset.UTC),
                executor.getColumnValue(index, ColumnType.parseType("ts", "timestamptz(6)"), new String[0]));
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
