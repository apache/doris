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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.ConnectorWriteHandle;
import org.apache.doris.connector.spi.handle.WriteOperation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PaimonWritePlanProviderTest {

    @Test
    public void insertColumnNamesFollowBoundSchemaOrder() {
        ConnectorWriteHandle handle = handle(
                columns("score", "name", "id"),
                columns("id", "name", "score"),
                WriteOperation.INSERT);

        Assertions.assertEquals(Arrays.asList("id", "name", "score"),
                PaimonWritePlanProvider.outputColumnNames(handle));
    }

    @Test
    public void changelogColumnNamesPrefixRowKindToBoundSchema() {
        ConnectorWriteHandle handle = handle(
                columns("score", "id"),
                columns("id", "name", "score"),
                WriteOperation.UPDATE);

        Assertions.assertEquals(
                Arrays.asList(PaimonWritePlanProvider.ROW_KIND_COLUMN, "id", "name", "score"),
                PaimonWritePlanProvider.outputColumnNames(handle));
    }

    private static List<ConnectorColumn> columns(String... names) {
        return Arrays.stream(names)
                .map(name -> new ConnectorColumn(
                        name, ConnectorType.of("INT"), null, true, null))
                .collect(Collectors.toList());
    }

    private static ConnectorWriteHandle handle(List<ConnectorColumn> columns,
            List<ConnectorColumn> boundTargetColumns, WriteOperation operation) {
        return new ConnectorWriteHandle() {
            @Override
            public ConnectorTableHandle getTableHandle() {
                return null;
            }

            @Override
            public List<ConnectorColumn> getColumns() {
                return columns;
            }

            @Override
            public List<ConnectorColumn> getBoundTargetColumns() {
                return boundTargetColumns;
            }

            @Override
            public boolean isOverwrite() {
                return false;
            }

            @Override
            public Map<String, String> getStaticPartitionSpec() {
                return Collections.emptyMap();
            }

            @Override
            public WriteOperation getWriteOperation() {
                return operation;
            }
        };
    }
}
