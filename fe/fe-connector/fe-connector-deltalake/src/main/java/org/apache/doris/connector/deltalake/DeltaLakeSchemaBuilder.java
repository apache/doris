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


package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorTableSchema;

import io.delta.kernel.Snapshot;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.TimestampType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class DeltaLakeSchemaBuilder {
    private DeltaLakeSchemaBuilder() {
    }

    static ConnectorTableSchema build(String table, Snapshot snapshot, DeltaLakeCatalogProperties properties) {
        List<ConnectorColumn> columns = new ArrayList<>();
        for (StructField field : snapshot.getSchema().fields()) {
            String comment = field.getMetadata().contains("comment") ? field.getMetadata().getString("comment") : "";
            // Root names stay remote names, matching partition keys and column handles. The engine
            // owns root-name mapping; only nested field names are normalized by the type mapper.
            ConnectorColumn column = new ConnectorColumn(field.getName(),
                    DeltaLakeTypeMapping.toConnectorType(field.getDataType(),
                            properties.isMapTimestampTz(), properties.isMapVarbinary()),
                    comment, field.isNullable(), null);
            if (field.getDataType() instanceof TimestampType) {
                column = column.withTimeZone();
            }
            columns.add(column);
        }
        Map<String, String> tableProperties = new HashMap<>(snapshot.getTableProperties());
        tableProperties.put(ConnectorTableSchema.SHOW_LOCATION_KEY, snapshot.getPath());
        tableProperties.put(ConnectorTableSchema.PARTITION_COLUMNS_KEY,
                String.join(",", snapshot.getPartitionColumnNames()));
        return new ConnectorTableSchema(table, columns, "deltalake", tableProperties);
    }
}
