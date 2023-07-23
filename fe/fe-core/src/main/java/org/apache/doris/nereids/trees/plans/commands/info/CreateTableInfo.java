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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.nereids.util.Utils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * table info in creating table.
 */
public class CreateTableInfo {
    private final String dbName;
    private final String tableName;
    private final List<ColumnDefinition> columns;
    private final List<IndexDef> indexes;
    private final String engineName;
    // private final KeysType keyType;
    private final List<String> keys;
    private final String comment;
    // private final List<PartitionInfo> partitions;
    private final DistributionDescriptor distribution;
    private final List<RollupDefinition> rollups;
    private final Map<String, String> properties;

    /**
     * constructor
     */
    public CreateTableInfo(String dbName, String tableName, List<ColumnDefinition> columns, List<IndexDef> indexes,
            String engineName, List<String> keys, String comment, DistributionDescriptor distribution,
            List<RollupDefinition> rollups, Map<String, String> properties) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.columns = Utils.copyRequiredList(columns);
        this.indexes = Utils.copyRequiredList(indexes);
        this.engineName = engineName;
        this.keys = Utils.copyRequiredList(keys);
        this.comment = comment;
        this.distribution = distribution;
        this.rollups = Utils.copyRequiredList(rollups);
        this.properties = properties;
    }

    /**
     * translate to catalog create table stmt
     */
    public CreateTableStmt translateToCatalogStyle() {
        List<ColumnDef> columnDefs = columns.stream().map(ColumnDefinition::translateToCatalogStyle)
                .collect(Collectors.toList());
        return new CreateTableStmt(false, false,
                new TableName(null, dbName, tableName),
                columnDefs,
                null,
                engineName,
                null,
                null,
                distribution.translateToCatalogStyle(),
                properties,
                null,
                comment,
                null,
                false);
    }
}
