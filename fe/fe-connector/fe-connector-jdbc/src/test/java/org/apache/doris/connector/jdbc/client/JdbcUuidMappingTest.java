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

class JdbcUuidMappingTest {
    @Test
    void nativeUuidNamesRetainTheirLogicalType() {
        JdbcConnectorClient[] clients = {
                new JdbcPostgreSQLConnectorClient("test", JdbcDbType.POSTGRESQL, "jdbc:postgresql://localhost/test",
                        false, Collections.emptyMap(), Collections.emptyMap(), false, false),
                new JdbcSQLServerConnectorClient("test", JdbcDbType.SQLSERVER, "jdbc:sqlserver://localhost",
                        false, Collections.emptyMap(), Collections.emptyMap(), false, false),
                new JdbcTrinoConnectorClient("test", JdbcDbType.TRINO, "jdbc:trino://localhost",
                        false, Collections.emptyMap(), Collections.emptyMap(), false, false)};
        String[] names = {"uuid", "uniqueidentifier", "uuid"};
        for (int i = 0; i < clients.length; i++) {
            JdbcFieldInfo field = new JdbcFieldInfo("u", Optional.of(names[i]), Types.OTHER,
                    Optional.of(36), Optional.of(0), Optional.empty());
            Assertions.assertEquals(ConnectorType.of("UUID"), clients[i].jdbcTypeToConnectorType(field));
        }
        JdbcFieldInfo field = new JdbcFieldInfo("items", Optional.of("array(uuid)"), Types.ARRAY,
                Optional.of(0), Optional.of(0), Optional.empty());
        Assertions.assertEquals(ConnectorType.arrayOf(ConnectorType.of("UUID")),
                clients[2].jdbcTypeToConnectorType(field));
    }
}
