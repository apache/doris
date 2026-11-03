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

package org.apache.doris.connector.spi;

import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

class ConnectorRelationMetadataDefaultsTest {
    @Test
    void relationNamesMergeBothDomainsWithoutDuplicates() {
        ConnectorMetadata metadata = new ConnectorMetadata() {
            @Override
            public List<String> listTableNames(ConnectorSession session, String dbName) {
                return Arrays.asList("table", "view");
            }

            @Override
            public List<String> listViewNames(ConnectorSession session, String dbName) {
                return Arrays.asList("view", "other_view");
            }
        };
        Assertions.assertEquals(Arrays.asList("table", "view", "other_view"),
                metadata.listRelationNames(null, "db"));
    }

    @Test
    void existenceDefaultPreservesHandleBasedConnectors() {
        ConnectorMetadata metadata = new ConnectorMetadata() {
            @Override
            public Optional<ConnectorTableHandle> getTableHandle(
                    ConnectorSession session, String dbName, String tableName) {
                return "exists".equals(tableName) ? Optional.of(new ConnectorTableHandle() {}) : Optional.empty();
            }
        };
        Assertions.assertTrue(metadata.tableExists(null, "db", "exists"));
        Assertions.assertFalse(metadata.tableExists(null, "db", "missing"));
    }
}
