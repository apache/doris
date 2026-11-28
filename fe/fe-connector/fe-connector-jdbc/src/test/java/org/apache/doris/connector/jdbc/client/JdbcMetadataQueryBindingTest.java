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

package org.apache.doris.connector.jdbc.client;

import org.apache.doris.connector.jdbc.JdbcDbType;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Remote database and table names reach the metadata queries below as bound parameters, never as
 * SQL text, so a name containing a quote cannot change the query.
 */
class JdbcMetadataQueryBindingTest {
    private static final String URL = "jdbc:fake://localhost/remote";
    private static final String HOSTILE_TABLE = "t' OR '1'='1";

    @Test
    void oracleTableExistenceProbeBindsTheTableName() {
        RecordingConnection recorder = new RecordingConnection();
        JdbcOracleConnectorClient client = new JdbcOracleConnectorClient("binding", JdbcDbType.ORACLE, URL,
                false, Collections.emptyMap(), Collections.emptyMap(), false, false);
        HikariDataSource pool = recorder.pool();
        client.dataSource = pool;
        try {
            Assertions.assertFalse(client.isTableExist("REMOTE", HOSTILE_TABLE));
        } finally {
            pool.close();
        }
        Assertions.assertEquals(
                Collections.singletonList(
                        "SELECT SYNONYM_NAME AS TABLE_NAME FROM ALL_SYNONYMS WHERE OWNER = ? AND SYNONYM_NAME = ?"),
                recorder.preparedSql);
        Assertions.assertEquals(Arrays.asList("REMOTE", HOSTILE_TABLE), recorder.boundStrings);
    }

    @Test
    void oracleSynonymListingBindsOnlyTheOwner() {
        RecordingConnection recorder = new RecordingConnection();
        JdbcOracleConnectorClient client = new JdbcOracleConnectorClient("binding", JdbcDbType.ORACLE, URL,
                false, Collections.emptyMap(), Collections.emptyMap(), false, false);
        HikariDataSource pool = recorder.pool();
        client.dataSource = pool;
        try {
            Assertions.assertEquals(Collections.emptyList(), client.getTablesNameList("REMOTE"));
        } finally {
            pool.close();
        }
        Assertions.assertEquals(
                Collections.singletonList("SELECT SYNONYM_NAME AS TABLE_NAME FROM ALL_SYNONYMS WHERE OWNER = ?"),
                recorder.preparedSql);
        Assertions.assertEquals(Collections.singletonList("REMOTE"), recorder.boundStrings);
    }

    @Test
    void oracleSynonymResolutionBindsTheNames() {
        RecordingConnection recorder = new RecordingConnection();
        JdbcOracleConnectorClient client = new JdbcOracleConnectorClient("binding", JdbcDbType.ORACLE, URL,
                false, Collections.emptyMap(), Collections.emptyMap(), false, false);
        HikariDataSource pool = recorder.pool();
        client.dataSource = pool;
        try {
            // getColumns() finds nothing, so the client looks the name up as a synonym.
            Assertions.assertEquals(Collections.emptyList(), client.getJdbcColumnsInfo("REMOTE", HOSTILE_TABLE));
        } finally {
            pool.close();
        }
        Assertions.assertEquals(
                Collections.singletonList(
                        "SELECT TABLE_OWNER, TABLE_NAME FROM ALL_SYNONYMS WHERE OWNER = ? AND SYNONYM_NAME = ?"),
                recorder.preparedSql);
        Assertions.assertEquals(Arrays.asList("REMOTE", HOSTILE_TABLE), recorder.boundStrings);
    }

    @Test
    void rowCountQueriesBindTheRemoteNames() {
        List<JdbcConnectorClient> clients = Arrays.asList(
                new JdbcMySQLConnectorClient("binding", JdbcDbType.MYSQL, URL, false,
                        Collections.emptyMap(), Collections.emptyMap(), false, false),
                new JdbcOracleConnectorClient("binding", JdbcDbType.ORACLE, URL, false,
                        Collections.emptyMap(), Collections.emptyMap(), false, false),
                new JdbcPostgreSQLConnectorClient("binding", JdbcDbType.POSTGRESQL, URL, false,
                        Collections.emptyMap(), Collections.emptyMap(), false, false),
                new JdbcSQLServerConnectorClient("binding", JdbcDbType.SQLSERVER, URL, false,
                        Collections.emptyMap(), Collections.emptyMap(), false, false));
        List<List<String>> expectedBindings = Arrays.asList(
                Arrays.asList("remote", HOSTILE_TABLE, "remote", HOSTILE_TABLE),
                Arrays.asList("remote", HOSTILE_TABLE),
                Arrays.asList("remote", HOSTILE_TABLE),
                Collections.singletonList("remote." + HOSTILE_TABLE));
        for (int i = 0; i < clients.size(); i++) {
            JdbcConnectorClient client = clients.get(i);
            RecordingConnection recorder = new RecordingConnection();
            HikariDataSource pool = recorder.pool();
            client.dataSource = pool;
            try {
                Assertions.assertEquals(-1, client.getRowCount("remote", HOSTILE_TABLE));
            } finally {
                pool.close();
            }
            String name = client.getClass().getSimpleName();
            Assertions.assertEquals(1, recorder.preparedSql.size(), name);
            Assertions.assertFalse(recorder.preparedSql.get(0).contains("remote"), name);
            Assertions.assertFalse(recorder.preparedSql.get(0).contains(HOSTILE_TABLE), name);
            Assertions.assertEquals(expectedBindings.get(i), recorder.boundStrings, name);
        }
    }

    /**
     * A connection that records every prepared SQL text and every bound string, returns empty
     * results, and fails the test if a plain {@code Statement} is created.
     */
    private static final class RecordingConnection {
        private final List<String> preparedSql = new ArrayList<>();
        private final List<String> boundStrings = new ArrayList<>();

        HikariDataSource pool() {
            Connection connection = connection();
            return new HikariDataSource() {
                @Override
                public Connection getConnection() {
                    return connection;
                }
            };
        }

        private Connection connection() {
            ResultSet empty = proxy(ResultSet.class, (object, method, args) -> {
                switch (method.getName()) {
                    case "next": return false;
                    case "close": return null;
                    default: throw new UnsupportedOperationException(method.getName());
                }
            });
            DatabaseMetaData metaData = proxy(DatabaseMetaData.class, (object, method, args) -> {
                switch (method.getName()) {
                    case "getSearchStringEscape": return "\\";
                    case "getTables":
                    case "getColumns":
                        return empty;
                    default: throw new UnsupportedOperationException(method.getName());
                }
            });
            PreparedStatement statement = proxy(PreparedStatement.class, (object, method, args) -> {
                switch (method.getName()) {
                    case "setString":
                        boundStrings.add((String) args[1]);
                        return null;
                    case "executeQuery": return empty;
                    case "close": return null;
                    default: throw new UnsupportedOperationException(method.getName());
                }
            });
            return proxy(Connection.class, (object, method, args) -> {
                switch (method.getName()) {
                    case "getMetaData": return metaData;
                    case "getCatalog": return null;
                    case "prepareStatement":
                        preparedSql.add((String) args[0]);
                        return statement;
                    case "createStatement": throw new AssertionError("metadata query must bind names");
                    case "close": return null;
                    default: throw new UnsupportedOperationException(method.getName());
                }
            });
        }

        private static <T> T proxy(Class<T> type, InvocationHandler handler) {
            return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler));
        }
    }
}
