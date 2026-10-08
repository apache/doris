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

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

class JdbcLegacyDiagnosticsTest {
    @Test
    void schemaFailureKeepsCauseAndRedactsConfiguredSecrets() throws Exception {
        SQLException cause = new SQLException("columns denied raw=test-password", "42000", 1142);
        cause.setNextException(new SQLException("connection failed", "08001", 0));
        DatabaseMetaData metadata = proxy(DatabaseMetaData.class, (object, method, args) -> {
            if (method.getName().equals("getSearchStringEscape")) {
                return "";
            }
            if (method.getName().equals("getColumns")) {
                throw cause;
            }
            throw new UnsupportedOperationException(method.getName());
        });
        Connection connection = proxy(Connection.class, (object, method, args) -> {
            switch (method.getName()) {
                case "getMetaData": return metadata;
                case "getCatalog": return "remote";
                case "close": return null;
                default: throw new UnsupportedOperationException(method.getName());
            }
        });
        JdbcClient client = Mockito.mock(JdbcClient.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doReturn(connection).when(client).getConnection();
        set(client, "jdbcPassword", "test-password");
        set(client, "diagnosticJdbcUrl", "jdbc:mysql://localhost/remote?password=url-secret");
        JdbcClientException error = Assertions.assertThrows(JdbcClientException.class,
                () -> client.getJdbcColumnsInfo("remote", "diagnostics_table"));
        Assertions.assertSame(cause, error.getCause());
        Assertions.assertTrue(error.getMessage().contains("remote_sqlstate=42000"));
        Assertions.assertTrue(error.getMessage().contains("remote_vendor_error_code=1142"));
        Assertions.assertTrue(error.getMessage().contains("remote_sqlstate=08001"));
        Assertions.assertTrue(error.getMessage().contains("remote.diagnostics_table"));
        Assertions.assertFalse(error.getMessage().contains("test-password"));
    }

    @Test
    void connectionFailureIncludesRemoteCodesAndRedactsUrl() throws Exception {
        String url = "jdbc:mysql://localhost/db?password=url-secret";
        SQLException cause = new SQLException("connect " + url + " raw=test-password", "08001", 0);
        JdbcClient client = Mockito.mock(JdbcClient.class, Mockito.CALLS_REAL_METHODS);
        HikariDataSource dataSource = Mockito.mock(HikariDataSource.class);
        Mockito.when(dataSource.getConnection()).thenThrow(cause);
        client.dataSource = dataSource;
        set(client, "jdbcPassword", "test-password");
        set(client, "diagnosticJdbcUrl", url);
        JdbcClientException error = Assertions.assertThrows(JdbcClientException.class, client::getConnection);
        Assertions.assertSame(cause, error.getCause());
        Assertions.assertTrue(error.getMessage().contains("remote_sqlstate=08001, remote_vendor_error_code=0"));
        Assertions.assertFalse(error.getMessage().contains("test-password"));
        Assertions.assertFalse(error.getMessage().contains("url-secret"));
    }

    private static void set(JdbcClient client, String name, String value) throws Exception {
        Field field = JdbcClient.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(client, value);
    }

    private static <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler));
    }
}
