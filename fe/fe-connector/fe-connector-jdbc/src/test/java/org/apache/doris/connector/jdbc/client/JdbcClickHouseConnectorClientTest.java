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

import java.util.Collections;

public class JdbcClickHouseConnectorClientTest {

    private JdbcClickHouseConnectorClient createClient() {
        return new JdbcClickHouseConnectorClient(
                "test_catalog",
                JdbcDbType.CLICKHOUSE,
                "jdbc:clickhouse://localhost:8123/default",
                false,
                Collections.emptyMap(),
                Collections.emptyMap(),
                false,
                false);
    }

    @Test
    void testDriverVersionDetection() {
        Assertions.assertTrue(JdbcClickHouseConnectorClient.isNewClickHouseDriverVersion("0.9.8"));
        Assertions.assertTrue(JdbcClickHouseConnectorClient.isNewClickHouseDriverVersion("0.7.1"));
        Assertions.assertFalse(JdbcClickHouseConnectorClient.isNewClickHouseDriverVersion("0.4.2"));
    }

    @Test
    void testDatabaseTermFollowsDriverMetadata() {
        Assertions.assertFalse(JdbcClickHouseConnectorClient.isDatabaseTermCatalog("0.9.8", false));
        Assertions.assertTrue(JdbcClickHouseConnectorClient.isDatabaseTermCatalog("0.7.1", true));
        Assertions.assertFalse(JdbcClickHouseConnectorClient.isDatabaseTermCatalog("0.4.2", true));
    }

    @Test
    void testClickHouseSpecificTableTypesAreVisible() {
        Assertions.assertArrayEquals(
                new String[] {"TABLE", "VIEW", "SYSTEM TABLE", "REMOTE TABLE", "MATERIALIZED VIEW"},
                createClient().getTableTypes());
    }
}
