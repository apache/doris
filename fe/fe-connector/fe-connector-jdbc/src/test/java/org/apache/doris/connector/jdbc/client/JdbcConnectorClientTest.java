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
import org.apache.doris.connector.spi.ConnectorQueryResult;
import org.apache.doris.connector.spi.DorisConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class JdbcConnectorClientTest {

    private static Map<String, Object> pk(String column, int keySeq) {
        return FakeJdbc.row("COLUMN_NAME", column, "KEY_SEQ", keySeq);
    }

    @Test
    public void primaryKeysAreOrderedByKeySeqNotByRowOrder() throws Exception {
        // DatabaseMetaData.getPrimaryKeys orders rows by COLUMN_NAME, so a composite key (b, a) arrives as
        // (a, b); KEY_SEQ is the real position (#64740).
        ResultSet rs = FakeJdbc.resultSet(Arrays.asList(pk("a", 2), pk("b", 1), pk("c", 3)));
        Assertions.assertEquals(Arrays.asList("b", "a", "c"), JdbcConnectorClient.readPrimaryKeysInKeyOrder(rs));
    }

    @Test
    public void primaryKeysKeepRowOrderWhenKeySeqIsNotReported() throws Exception {
        ResultSet rs = FakeJdbc.resultSet(Arrays.asList(pk("a", 0), pk("b", 0)));
        Assertions.assertEquals(Arrays.asList("a", "b"), JdbcConnectorClient.readPrimaryKeysInKeyOrder(rs));
        ResultSet dup = FakeJdbc.resultSet(Arrays.asList(pk("a", 1), pk("b", 1)));
        Assertions.assertEquals(Arrays.asList("a", "b"), JdbcConnectorClient.readPrimaryKeysInKeyOrder(dup));
        Assertions.assertTrue(JdbcConnectorClient.readPrimaryKeysInKeyOrder(
                FakeJdbc.resultSet(Collections.emptyList())).isEmpty());
    }

    private static JdbcConnectorClient clientOver(Connection connection) {
        return new JdbcPostgreSQLConnectorClient("test_catalog", JdbcDbType.POSTGRESQL,
                "jdbc:postgresql://localhost:5432/test", false,
                Collections.emptyMap(), Collections.emptyMap(), false, false) {
            @Override
            public Connection getConnection() {
                return connection;
            }
        };
    }

    @Test
    public void executeQueryBindsParametersAndMaterializesRows() {
        List<Object> bound = new ArrayList<>();
        ResultSet slots = FakeJdbc.resultSet(Arrays.asList(
                FakeJdbc.row("slot_name", "doris_cdc_1", "active", Boolean.TRUE),
                FakeJdbc.row("slot_name", "other", "active", null)));
        Connection conn = FakeJdbc.connection(null, null,
                sql -> sql.startsWith("SELECT slot_name") ? slots : null, bound);

        ConnectorQueryResult result = clientOver(conn).executeQuery(
                "SELECT slot_name, active FROM pg_replication_slots WHERE slot_name = ?",
                Collections.singletonList("doris_cdc_1"));

        // The caller's value reaches the source as a bound parameter, never as SQL text.
        Assertions.assertEquals(Collections.singletonList("doris_cdc_1"), bound);
        Assertions.assertEquals(Arrays.asList("slot_name", "active"), result.getColumnNames());
        Assertions.assertEquals(2, result.getRows().size());
        // Values are the driver's objects: a PostgreSQL boolean is a Boolean, SQL NULL is null.
        Assertions.assertEquals(Boolean.TRUE, result.getRows().get(0).get(1));
        Assertions.assertNull(result.getRows().get(1).get(1));
        Assertions.assertEquals("other", result.getRows().get(1).get(0));
    }

    @Test
    public void executeQueryFailuresCarryTheStatement() {
        Connection conn = FakeJdbc.connection(null, null, sql -> null, new ArrayList<>());
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> clientOver(conn).executeQuery("SELECT 1", Collections.emptyList()));
        Assertions.assertTrue(e.getMessage().contains("SELECT 1"), e.getMessage());
    }
}
