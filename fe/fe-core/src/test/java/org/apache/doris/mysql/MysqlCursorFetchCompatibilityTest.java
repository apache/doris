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

package org.apache.doris.mysql;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class MysqlCursorFetchCompatibilityTest {
    @Test
    public void testConnectorJBehaviorBoundaries() {
        Assertions.assertEquals(MysqlCursorFetchCompatibility.Behavior.CONSUMES_METADATA_TERMINATOR,
                resolve("MySQL Connector Java", "5.1.49"));
        Assertions.assertEquals(MysqlCursorFetchCompatibility.Behavior.CONSUMES_METADATA_TERMINATOR,
                resolve("MySQL Connector/J", "6.0.6"));
        Assertions.assertEquals(MysqlCursorFetchCompatibility.Behavior.CONSUMES_METADATA_TERMINATOR,
                resolve("MySQL Connector/J", "8.2.0"));
        Assertions.assertEquals(MysqlCursorFetchCompatibility.Behavior.CONSUMES_METADATA_TERMINATOR,
                resolve("MySQL Connector/J", "9.4.0"));
        Assertions.assertEquals(MysqlCursorFetchCompatibility.Behavior.STANDARD,
                resolve("MySQL Connector/J", "9.5.0"));
        Assertions.assertEquals(MysqlCursorFetchCompatibility.Behavior.STANDARD,
                resolve("MySQL Connector/J", "9.6.0"));
    }

    @Test
    public void testUnknownAndOtherClients() {
        Assertions.assertEquals(MysqlCursorFetchCompatibility.Behavior.UNKNOWN,
                MysqlCursorFetchCompatibility.resolve(Collections.emptyMap()));
        Assertions.assertEquals(MysqlCursorFetchCompatibility.Behavior.UNKNOWN,
                MysqlCursorFetchCompatibility.resolve(ImmutableMap.of("_client_name", "MySQL Connector/J")));
        Assertions.assertEquals(MysqlCursorFetchCompatibility.Behavior.UNKNOWN,
                resolve("MySQL Connector/J", "custom"));
        Assertions.assertEquals(MysqlCursorFetchCompatibility.Behavior.STANDARD,
                resolve("MariaDB Connector/J", "3.5.6"));
    }

    private MysqlCursorFetchCompatibility.Behavior resolve(String clientName, String clientVersion) {
        return MysqlCursorFetchCompatibility.resolve(ImmutableMap.of(
                "_client_name", clientName, "_client_version", clientVersion));
    }
}
