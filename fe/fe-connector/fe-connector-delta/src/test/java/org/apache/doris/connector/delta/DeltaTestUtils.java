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

package org.apache.doris.connector.delta;

import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.spi.ddl.ConnectorPartitionField;
import org.apache.doris.connector.spi.ddl.ConnectorPartitionSpec;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Shared neutral request builders for the Delta connector tests. */
final class DeltaTestUtils {
    private DeltaTestUtils() {
    }

    static ConnectorCreateTableRequest createRequest(String database, ConnectorTableSchema schema,
            List<String> partitionColumns, Map<String, String> properties, String comment, boolean ifNotExists) {
        return ConnectorCreateTableRequest.builder().dbName(database).tableName(schema.getTableName())
                .columns(schema.getColumns()).properties(properties).comment(comment).ifNotExists(ifNotExists)
                .partitionSpec(partitionColumns.isEmpty() ? null : new ConnectorPartitionSpec(
                        ConnectorPartitionSpec.Style.IDENTITY, partitionColumns.stream()
                                .map(column -> new ConnectorPartitionField(column, "identity", List.of()))
                                .collect(Collectors.toList()))).build();
    }
}
