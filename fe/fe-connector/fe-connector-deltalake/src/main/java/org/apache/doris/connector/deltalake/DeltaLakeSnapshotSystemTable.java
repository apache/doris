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

import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;

import java.util.List;
import java.util.Map;

/** These views are functions of the pinned base snapshot, including its schema and protocol. */
abstract class DeltaLakeSnapshotSystemTable implements DeltaLakeSystemTable {
    private final String name;
    private final List<ConnectorColumn> columns;

    DeltaLakeSnapshotSystemTable(String name, StructType schema) {
        this.name = name;
        columns = schema.fields().stream().map(DeltaLakeSnapshotSystemTable::column).toList();
    }

    private static ConnectorColumn column(StructField field) {
        ConnectorColumn column = new ConnectorColumn(field.getName(),
                DeltaLakeTypeMapping.toConnectorType(field.getDataType(), true, true), "", true, null);
        return field.getDataType() instanceof TimestampType ? column.withTimeZone() : column;
    }

    @Override
    public final String name() {
        return name;
    }

    @Override
    public final ConnectorTableSchema schema(DeltaLakeTableHandle handle) {
        return new ConnectorTableSchema(handle.getTable() + "$" + name, columns, "deltalake", Map.of());
    }

    @Override
    public final boolean supportsTimeTravel() {
        return true;
    }
}
