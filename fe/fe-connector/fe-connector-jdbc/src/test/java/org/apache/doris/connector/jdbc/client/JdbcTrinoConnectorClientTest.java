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

class JdbcTrinoConnectorClientTest {
    @Test
    void queryMetadataKeepsZoneAndPrecisionInBothTypeNameLayouts() {
        JdbcTrinoConnectorClient client = new JdbcTrinoConnectorClient("test", JdbcDbType.TRINO,
                "jdbc:trino://localhost:8080", false, Collections.emptyMap(), Collections.emptyMap(), false, false);
        for (String name : new String[] {"timestamp(3) with time zone", "timestamp with time zone(3)",
                "array(timestamp(3) with time zone)", "array(timestamp with time zone(3))"}) {
            JdbcFieldInfo field = new JdbcFieldInfo("ts", Optional.of(name), Types.TIMESTAMP_WITH_TIMEZONE,
                    Optional.of(0), Optional.of(0), Optional.empty());
            ConnectorType type = client.jdbcTypeToConnectorType(field);
            if (name.startsWith("array")) {
                type = type.getChildren().get(0);
            }
            Assertions.assertEquals("TIMESTAMPTZ", type.getTypeName(), name);
            Assertions.assertEquals(3, type.getPrecision(), name);
        }
    }
}
