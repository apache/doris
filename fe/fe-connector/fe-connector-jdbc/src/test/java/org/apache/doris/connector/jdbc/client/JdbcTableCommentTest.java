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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class JdbcTableCommentTest {

    /** Records the SQL and parameters of one prepared query and returns at most one comment row. */
    private static class FakeQuery {
        private final boolean hasRow;
        private final String comment;
        private String sql;
        private final List<String> params = new ArrayList<>();
        private boolean closed;

        FakeQuery(boolean hasRow, String comment) {
            this.hasRow = hasRow;
            this.comment = comment;
        }

        Connection connection() {
            boolean[] rowRead = {false};
            ResultSet rs = proxy(ResultSet.class, (method, args) -> {
                switch (method) {
                    case "next":
                        boolean next = hasRow && !rowRead[0];
                        rowRead[0] = true;
                        return next;
                    case "getString":
                        Assertions.assertEquals(1, args[0]);
                        return comment;
                    default:
                        return null;
                }
            });
            PreparedStatement ps = proxy(PreparedStatement.class, (method, args) -> {
                switch (method) {
                    case "setString":
                        Assertions.assertEquals(params.size() + 1, args[0]);
                        params.add((String) args[1]);
                        return null;
                    case "executeQuery":
                        return rs;
                    default:
                        return null;
                }
            });
            return proxy(Connection.class, (method, args) -> {
                switch (method) {
                    case "prepareStatement":
                        sql = (String) args[0];
                        return ps;
                    case "close":
                        closed = true;
                        return null;
                    default:
                        return null;
                }
            });
        }
    }

    private interface Handler {
        Object handle(String method, Object[] args);
    }

    private static <T> T proxy(Class<T> type, Handler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
                (p, method, args) -> handler.handle(method.getName(), args)));
    }

    private static JdbcConnectorClient oracle(FakeQuery query) {
        return new JdbcOracleConnectorClient("test_catalog", JdbcDbType.ORACLE,
                "jdbc:oracle:thin:@localhost:1521:XE", false, Collections.emptyMap(), Collections.emptyMap(),
                false, false) {
            @Override
            public Connection getConnection() {
                return query.connection();
            }
        };
    }

    private static JdbcConnectorClient sapHana(FakeQuery query) {
        return new JdbcSapHanaConnectorClient("test_catalog", JdbcDbType.SAP_HANA,
                "jdbc:sap://localhost:39017", false, Collections.emptyMap(), Collections.emptyMap(),
                false, false) {
            @Override
            public Connection getConnection() {
                return query.connection();
            }
        };
    }

    private static JdbcConnectorClient mysql(FakeQuery query) {
        return new JdbcMySQLConnectorClient("test_catalog", JdbcDbType.MYSQL,
                "jdbc:mysql://localhost:3306", false, Collections.emptyMap(), Collections.emptyMap(),
                false, false) {
            @Override
            public Connection getConnection() {
                return query.connection();
            }
        };
    }

    @Test
    void testOracleTableComment() {
        FakeQuery query = new FakeQuery(true, "订单表 orders");
        Assertions.assertEquals("订单表 orders", oracle(query).getTableComment("SALES", "ORDERS"));
        Assertions.assertTrue(query.sql.contains("ALL_TAB_COMMENTS"), query.sql);
        Assertions.assertEquals(Arrays.asList("SALES", "ORDERS"), query.params);
        Assertions.assertTrue(query.closed);
    }

    @Test
    void testSapHanaTableAndViewComment() {
        FakeQuery query = new FakeQuery(true, "orders");
        Assertions.assertEquals("orders", sapHana(query).getTableComment("SALES", "ORDERS"));
        Assertions.assertTrue(query.sql.contains("SYS.TABLES"), query.sql);
        Assertions.assertTrue(query.sql.contains("SYS.VIEWS"), query.sql);
        Assertions.assertEquals(Arrays.asList("SALES", "ORDERS", "SALES", "ORDERS"), query.params);
    }

    @Test
    void testMySQLTableComment() {
        FakeQuery query = new FakeQuery(true, "orders");
        Assertions.assertEquals("orders", mysql(query).getTableComment("sales", "orders"));
        Assertions.assertTrue(query.sql.contains("INFORMATION_SCHEMA.TABLES"), query.sql);
        Assertions.assertEquals(Arrays.asList("sales", "orders"), query.params);
    }

    @Test
    void testMissingOrNullCommentIsEmpty() {
        Assertions.assertEquals("", oracle(new FakeQuery(false, null)).getTableComment("SALES", "MISSING"));
        Assertions.assertEquals("", oracle(new FakeQuery(true, null)).getTableComment("SALES", "ORDERS"));
        Assertions.assertEquals("", sapHana(new FakeQuery(false, null)).getTableComment("SALES", "MISSING"));
    }
}
