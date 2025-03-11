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

package org.apache.doris.datasource.paimon;

import org.apache.doris.catalog.Column;
import org.apache.doris.datasource.SchemaCacheValue;

import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PaimonSchemaCacheValue extends SchemaCacheValue {

    private List<Column> partitionColumns;

    private TableSchema tableSchema;
    // Caching TableSchema can reduce the reading of schema files and json parsing.

    private Map<Long, String> columnIdToName;

    public PaimonSchemaCacheValue(List<Column> schema, List<Column> partitionColumns, TableSchema tableSchema) {
        super(schema);
        this.partitionColumns = partitionColumns;
        this.tableSchema = tableSchema;

        columnIdToName = new HashMap<>(tableSchema.fields().size());
        for (DataField dataField : tableSchema.fields()) {
            columnIdToName.put((long) dataField.id(), dataField.name().toLowerCase());
        }
    }

    public List<Column> getPartitionColumns() {
        return partitionColumns;
    }

    public TableSchema getTableSchema() {
        return tableSchema;
    }

    public Map<Long, String> getColumnIdToName() {
        return columnIdToName;
    }
}
