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

package org.apache.doris.datasource.maxcompute;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.SchemaCacheValue;

import com.aliyun.odps.Table;
import com.aliyun.odps.table.TableIdentifier;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

@Getter
@Setter
public class MaxComputeSchemaCacheValue extends SchemaCacheValue {
    private Table odpsTable;
    private TableIdentifier tableIdentifier;
    private List<String> partitionColumnNames;
    private List<String> partitionSpecs;
    private List<Column> partitionColumns;
    private List<Type> partitionTypes;
    private Map<String, com.aliyun.odps.Column> columnNameToOdpsColumn;

    public MaxComputeSchemaCacheValue(List<Column> schema, Table odpsTable, TableIdentifier tableIdentifier,
            List<String> partitionColumnNames, List<String> partitionSpecs, List<Column> partitionColumns,
            List<Type> partitionTypes, Map<String, com.aliyun.odps.Column> columnNameToOdpsColumn) {
        super(schema);
        this.odpsTable = odpsTable;
        this.tableIdentifier = tableIdentifier;
        this.partitionSpecs = partitionSpecs;
        this.partitionColumnNames = partitionColumnNames;
        this.partitionColumns = partitionColumns;
        this.partitionTypes = partitionTypes;
        this.columnNameToOdpsColumn = columnNameToOdpsColumn;
    }

    public List<Column> getPartitionColumns() {
        return partitionColumns;
    }

    public List<String> getPartitionColumnNames() {
        return partitionColumnNames;
    }

    public Map<String, com.aliyun.odps.Column> getColumnNameToOdpsColumn() {
        return columnNameToOdpsColumn;
    }
}
