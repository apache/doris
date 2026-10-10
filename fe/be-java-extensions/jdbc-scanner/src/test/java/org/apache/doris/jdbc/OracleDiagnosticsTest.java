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
import org.apache.doris.jni.spi.vec.ColumnValueConverter;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

class OracleDiagnosticsTest {
    @Test
    void scannerSuppliesSecretsToDriverMetadataAndClobDiagnosticLogs() throws Exception {
        String password = "test-password";
        String url = "jdbc:oracle:thin:alice/url-secret@host:1521:db";
        Map<String, String> params = new HashMap<>();
        params.put("table_type", "ORACLE");
        params.put("jdbc_url", url);
        params.put("jdbc_password", password);
        JdbcJniScanner scanner = new JdbcJniScanner(1, params);
        Field field = JdbcJniScanner.class.getDeclaredField("typeHandler");
        field.setAccessible(true);
        JdbcTypeHandler typeHandler = (JdbcTypeHandler) field.get(scanner);
        SQLException raw = new SQLException("raw=" + password + " url=" + url, "08001", 0);
        PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                PreparedStatement.class.getClassLoader(), new Class<?>[] {PreparedStatement.class},
                (object, method, args) -> null);
        Connection connection = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class}, (object, method, args) -> {
                    switch (method.getName()) {
                        case "getMetaData": throw raw;
                        case "prepareStatement": return statement;
                        default: throw new UnsupportedOperationException(method.getName());
                    }
                });
        Clob clob = (Clob) Proxy.newProxyInstance(Clob.class.getClassLoader(), new Class<?>[] {Clob.class},
                (object, method, args) -> {
                    throw raw;
                });
        List<String> messages = new CopyOnWriteArrayList<>();
        List<Throwable> rawCauses = new CopyOnWriteArrayList<>();
        // Production uses JUL; tests may select the inherited Log4j SLF4J provider instead.
        Handler julHandler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                messages.add(record.getMessage());
                if (record.getThrown() != null) {
                    rawCauses.add(record.getThrown());
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        AbstractAppender appender = new AbstractAppender("oracle-diagnostics-test", null,
                null, true, Property.EMPTY_ARRAY) {
            @Override
            public void append(LogEvent event) {
                messages.add(event.getMessage().getFormattedMessage());
                if (event.getThrown() != null) {
                    rawCauses.add(event.getThrown());
                }
            }
        };
        java.util.logging.Logger jul = java.util.logging.Logger.getLogger(OracleTypeHandler.class.getName());
        java.util.logging.Level previousJulLevel = jul.getLevel();
        Logger log4j = (Logger) LogManager.getLogger(OracleTypeHandler.class);
        Level previousLevel = log4j.getLevel();
        jul.setLevel(java.util.logging.Level.ALL);
        jul.addHandler(julHandler);
        appender.start();
        log4j.addAppender(appender);
        log4j.setLevel(Level.ALL);
        try {
            Assertions.assertSame(statement, typeHandler.initializeStatement(connection, "SELECT 1", 1));
            ColumnValueConverter converter = typeHandler.getOutputConverter(
                    new ColumnType("value", ColumnType.Type.STRING), "");
            Assertions.assertArrayEquals(new Object[] {null}, converter.convert(new Object[] {clob}));
            Assertions.assertEquals(2, messages.size());
            Assertions.assertTrue(rawCauses.isEmpty());
            for (String message : messages) {
                Assertions.assertTrue(message.contains("remote_sqlstate=08001"), message);
                Assertions.assertFalse(message.contains(password), message);
                Assertions.assertFalse(message.contains("url-secret"), message);
            }
        } finally {
            log4j.removeAppender(appender);
            log4j.setLevel(previousLevel);
            appender.stop();
            jul.removeHandler(julHandler);
            jul.setLevel(previousJulLevel);
            julHandler.close();
        }
    }
}
