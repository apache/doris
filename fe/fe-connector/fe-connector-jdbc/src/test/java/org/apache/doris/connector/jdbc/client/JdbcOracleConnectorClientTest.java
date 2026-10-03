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
import org.apache.doris.connector.spi.ConnectorType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.Collections;
import java.util.Optional;

public class JdbcOracleConnectorClientTest {

    private JdbcOracleConnectorClient createClient() {
        return new JdbcOracleConnectorClient(
                "test_catalog",
                JdbcDbType.ORACLE,
                "jdbc:oracle:thin:@localhost:1521:XE",
                false,
                Collections.emptyMap(),
                Collections.emptyMap(),
                false,
                false);
    }

    private JdbcFieldInfo column(String typeName, int dataType) {
        return new JdbcFieldInfo(
                "col", Optional.of(typeName), dataType,
                Optional.of(8), Optional.of(0), Optional.empty());
    }

    @Test
    void testOracleBinaryFloatingPointTypes() {
        JdbcOracleConnectorClient client = createClient();
        Assertions.assertEquals(
                ConnectorType.of("FLOAT"),
                client.jdbcTypeToConnectorType(column("BINARY_FLOAT", Types.FLOAT)));
        Assertions.assertEquals(
                ConnectorType.of("DOUBLE"),
                client.jdbcTypeToConnectorType(column("BINARY_DOUBLE", Types.DOUBLE)));
    }
}
