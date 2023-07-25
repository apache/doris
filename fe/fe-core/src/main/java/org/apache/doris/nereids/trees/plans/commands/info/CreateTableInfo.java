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

import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.KeysDesc;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * table info in creating table.
 */
public class CreateTableInfo {
    private String dbName;
    private final String tableName;
    private final List<ColumnDefinition> columns;
    private final List<IndexDefinition> indexes;
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
    public CreateTableInfo(String dbName, String tableName, List<ColumnDefinition> columns,
            List<IndexDefinition> indexes, String engineName, List<String> keys, String comment,
            DistributionDescriptor distribution, List<RollupDefinition> rollups, Map<String, String> properties) {
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

    public void validate(ConnectContext ctx) {
        if (dbName == null) {
            dbName = ClusterNamespace.getFullName(ctx.getClusterName(), ctx.getDatabase());
        } else {
            dbName = ClusterNamespace.getFullName(ctx.getClusterName(), dbName);
        }
    }

    /**
     * translate to catalog create table stmt
     */
    public CreateTableStmt translateToCatalogStyle() {
        List<Column> catalogColumns = columns.stream().map(ColumnDefinition::translateToCatalogStyle)
                .collect(Collectors.toList());
        List<Index> catalogIndexes = indexes.stream().map(IndexDefinition::translateToCatalogStyle)
                .collect(Collectors.toList());
        return new CreateTableStmt(false, false,
                new TableName(Env.getCurrentEnv().getCurrentCatalog().getName(), dbName, tableName),
                catalogColumns,
                catalogIndexes,
                engineName,
                new KeysDesc(KeysType.DUP_KEYS, keys),
                null,
                distribution.translateToCatalogStyle(),
                Maps.newHashMap(properties),
                null,
                comment,
                ImmutableList.of(),
                false,
                null);
    }
}
