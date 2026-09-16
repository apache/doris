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
import org.apache.doris.common.jni.vec.VectorColumn;
import org.apache.doris.thrift.TJdbcOperation;
import org.apache.doris.thrift.TOdbcTableType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockito.Mockito;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Properties;
import java.util.TimeZone;

/** Real server/driver coverage, enabled with mysql.integration.url and mysql.integration.driverJar. */
@EnabledIfSystemProperty(named = "mysql.integration.url", matches = ".+")
class MySqlTimestampIntegrationTest {
    private static final ColumnType INSTANT_TYPE = ColumnType.parseType("event_time", "timestamptz(6)");
    private static final ColumnType LOCAL_TYPE = ColumnType.parseType("local_time", "datetimev2(6)");
    private static final String[] VALUES = {
        "2020-01-02T12:01:00.111333Z", "2021-11-07T05:30:00.123456Z", "2021-11-07T06:30:00.123456Z"
    };

    @Test
    void testReadAcrossDriverAndSessionTimezones() throws Exception {
        runMatrix(false);
    }

    @Test
    void testWriteAcrossDriverAndSessionTimezones() throws Exception {
        runMatrix(true);
    }

    private void runMatrix(boolean write) throws Exception {
        TimeZone original = TimeZone.getDefault();
        URL driverUrl = new File(System.getProperty("mysql.integration.driverJar")).toURI().toURL();
        try (URLClassLoader loader = new URLClassLoader(new URL[] {driverUrl}, getClass().getClassLoader())) {
            Driver driver = (Driver) loader.loadClass("com.mysql.cj.jdbc.Driver").getDeclaredConstructor().newInstance();
            for (String jvmZone : new String[] {"Asia/Shanghai", "UTC", "America/New_York"}) {
                TimeZone.setDefault(TimeZone.getTimeZone(jvmZone));
                for (String sessionZone : new String[] {"+00:00", "+08:00"}) {
                    for (String driverOptions : new String[] {"", "&preserveInstants=false",
                            "&connectionTimeZone=America/New_York", "&connectionTimeZone=SERVER"}) {
                        for (boolean serverPrepared : new boolean[] {false, true}) {
                            String baseUrl = System.getProperty("mysql.integration.url");
                            String url = baseUrl + (baseUrl.contains("?") ? "&" : "?")
                                    + "useServerPrepStmts=" + serverPrepared + driverOptions;
                            Properties properties = new Properties();
                            properties.setProperty("user", System.getProperty("mysql.integration.user", "root"));
                            properties.setProperty("password", System.getProperty("mysql.integration.password", ""));
                            try (Connection connection = driver.connect(url, properties)) {
                                seed(connection, sessionZone);
                                if (write) {
                                    verifyWrite(connection);
                                } else {
                                    verifyRead(connection);
                                }
                            } catch (AssertionError e) {
                                throw new AssertionError("JVM=" + jvmZone + ", session=" + sessionZone
                                        + ", driver=" + driverOptions + ", serverPrepared=" + serverPrepared, e);
                            }
                        }
                    }
                }
            }
        } finally {
            TimeZone.setDefault(original);
        }
    }

    private void seed(Connection connection, String sessionZone) throws Exception {
        try (Statement statement = connection.createStatement()) {
            statement.execute("SET time_zone = '+00:00'");
            statement.execute("CREATE TEMPORARY TABLE timestamp_roundtrip "
                    + "(id INT, event_time TIMESTAMP(6) NULL, local_time DATETIME(6) NULL)");
            for (int i = 0; i < VALUES.length; i++) {
                String value = VALUES[i].replace('T', ' ').replace("Z", "");
                statement.execute("INSERT INTO timestamp_roundtrip VALUES (" + i + ", '" + value + "', '"
                        + value + "')");
            }
            statement.execute("INSERT INTO timestamp_roundtrip VALUES (3, NULL, NULL)");
            statement.execute("SET time_zone = '" + sessionZone + "'");
        }
    }

    private MySQLJdbcExecutor executor(Connection connection, TJdbcOperation operation, String sql) throws Exception {
        // Bypass only the JNI bootstrap; statements, result sets and timezone decoding use the real driver.
        MySQLJdbcExecutor executor = Mockito.mock(MySQLJdbcExecutor.class, Mockito.CALLS_REAL_METHODS);
        JdbcDataSourceConfig config = new JdbcDataSourceConfig().setOp(operation).setTableType(TOdbcTableType.MYSQL);
        executor.config = config;
        executor.initializeStatement(connection, config, sql);
        return executor;
    }

    private void verifyRead(Connection connection) throws Exception {
        MySQLJdbcExecutor executor = executor(connection, TJdbcOperation.READ,
                "SELECT event_time, local_time FROM timestamp_roundtrip ORDER BY id");
        try (PreparedStatement statement = (PreparedStatement) executor.stmt; ResultSet rows = statement.executeQuery()) {
            executor.resultSet = rows;
            for (String value : VALUES) {
                Assertions.assertTrue(rows.next());
                LocalDateTime expected = LocalDateTime.ofInstant(Instant.parse(value), ZoneOffset.UTC);
                Assertions.assertEquals(expected, executor.getColumnValue(0, INSTANT_TYPE, new String[0]));
                Assertions.assertEquals(expected, executor.getColumnValue(1, LOCAL_TYPE, new String[0]));
            }
            Assertions.assertTrue(rows.next());
            Assertions.assertNull(executor.getColumnValue(0, INSTANT_TYPE, new String[0]));
            Assertions.assertNull(executor.getColumnValue(1, LOCAL_TYPE, new String[0]));
            Assertions.assertFalse(rows.next());
        }
    }

    private void verifyWrite(Connection connection) throws Exception {
        MySQLJdbcExecutor executor = executor(connection, TJdbcOperation.WRITE,
                "INSERT INTO timestamp_roundtrip (id, event_time) VALUES (4, ?)");
        Instant instant = Instant.parse(VALUES[0]);
        VectorColumn column = Mockito.mock(VectorColumn.class);
        Mockito.when(column.getColumnPrimitiveType()).thenReturn(ColumnType.Type.TIMESTAMPTZ);
        Mockito.when(column.getTimeStampTz(0)).thenReturn(LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
        Method insert = BaseJdbcExecutor.class.getDeclaredMethod("insertColumn", int.class, int.class, VectorColumn.class);
        insert.setAccessible(true);
        try (PreparedStatement statement = executor.preparedStatement) {
            insert.invoke(executor, 0, 0, column);
            statement.executeUpdate();
        }
        try (Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery(
                        "SELECT UNIX_TIMESTAMP(event_time) FROM timestamp_roundtrip WHERE id = 4")) {
            Assertions.assertTrue(rows.next());
            Assertions.assertEquals(instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1000,
                    rows.getBigDecimal(1).movePointRight(6).longValueExact());
        }
    }
}
