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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;

public class JdbcOceanBaseConnectorClientTest {

    private static JdbcOceanBaseConnectorClient clientAnswering(String compatibilityMode) {
        ResultSet mode = FakeJdbc.resultSet(compatibilityMode == null
                ? Collections.emptyList()
                : Collections.singletonList(FakeJdbc.row("ob_compatibility_mode()", compatibilityMode)));
        Connection conn = FakeJdbc.connection(null, null,
                sql -> sql.contains("ob_compatibility_mode") ? mode : null, new ArrayList<>());
        return new JdbcOceanBaseConnectorClient("test_catalog", JdbcDbType.OCEANBASE,
                "jdbc:oceanbase://localhost:2881/test", false,
                Collections.emptyMap(), Collections.emptyMap(), false, false) {
            @Override
            public Connection getConnection() {
                return conn;
            }
        };
    }

    private static JdbcFieldInfo number() {
        // NUMBER(10,0) is an Oracle-mode type; MySQL mode has no such type name.
        return new JdbcFieldInfo("col", Optional.of("NUMBER"), Types.NUMERIC, Optional.of(10), Optional.of(0),
                Optional.empty());
    }

    @Test
    public void oracleModeDelegatesToTheOracleMapping() {
        JdbcOceanBaseConnectorClient client = clientAnswering("ORACLE");
        // The compatibility mode is probed lazily, on the first delegated call.
        Assertions.assertEquals(JdbcDbType.OCEANBASE, client.getDbType());
        // NUMBER(10,0) is BIGINT under the Oracle mapping (10 integer digits do not fit an INT).
        Assertions.assertEquals("BIGINT", client.jdbcTypeToConnectorType(number()).getTypeName());
        Assertions.assertEquals(JdbcDbType.OCEANBASE_ORACLE, client.getDbType());
    }

    @Test
    public void mysqlModeDelegatesToTheMysqlMapping() {
        JdbcOceanBaseConnectorClient client = clientAnswering("MYSQL");
        Assertions.assertEquals("UNSUPPORTED", client.jdbcTypeToConnectorType(number()).getTypeName());
        Assertions.assertEquals(JdbcDbType.OCEANBASE, client.getDbType());
    }

    @Test
    public void unknownOrFailedProbeDefaultsToMysqlMode() {
        for (JdbcOceanBaseConnectorClient client : new JdbcOceanBaseConnectorClient[] {
                clientAnswering(null), clientAnswering("SOMETHING")}) {
            Assertions.assertEquals("UNSUPPORTED", client.jdbcTypeToConnectorType(number()).getTypeName());
            Assertions.assertEquals(JdbcDbType.OCEANBASE, client.getDbType());
        }
    }
}
