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

import org.apache.doris.analysis.AllPartitionDesc;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.KeysDesc;
import org.apache.doris.analysis.ListPartitionDesc;
import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.analysis.RangePartitionDesc;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.Type;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * table info in creating table.
 */
public class CreateTableInfo {
    private final boolean ifNotExists;
    private String dbName;
    private final String tableName;
    private List<ColumnDefinition> columns;
    private final List<IndexDefinition> indexes;
    private final List<String> ctasColumns;
    private final String engineName;
    private final KeysType keysType;
    private List<String> keys;
    private final String comment;
    private final String partitionType;
    private final List<String> partitionColumns;
    private final List<PartitionDefinition> partitions;
    private final DistributionDescriptor distribution;
    private final List<RollupDefinition> rollups;
    private final Map<String, String> properties;

    /**
     * constructor for create table
     */
    public CreateTableInfo(boolean ifNotExists, String dbName, String tableName, List<ColumnDefinition> columns,
            List<IndexDefinition> indexes, String engineName, KeysType keysType, List<String> keys, String comment,
            String partitionType, List<String> partitionColumns, List<PartitionDefinition> partitions,
            DistributionDescriptor distribution, List<RollupDefinition> rollups, Map<String, String> properties) {
        this.ifNotExists = ifNotExists;
        this.dbName = dbName;
        this.tableName = tableName;
        this.ctasColumns = null;
        this.columns = Utils.copyRequiredList(columns);
        this.indexes = Utils.copyRequiredList(indexes);
        this.engineName = engineName;
        this.keysType = keysType;
        this.keys = Utils.copyRequiredList(keys);
        this.comment = comment;
        this.partitionType = partitionType;
        this.partitionColumns = partitionColumns;
        this.partitions = partitions;
        this.distribution = distribution;
        this.rollups = Utils.copyRequiredList(rollups);
        this.properties = properties;
    }

    /**
     * constructor for create table as select
     */
    public CreateTableInfo(boolean ifNotExists, String dbName, String tableName, List<String> cols,
            String engineName, KeysType keysType, List<String> keys, String comment,
            String partitionType, List<String> partitionColumns, List<PartitionDefinition> partitions,
            DistributionDescriptor distribution, List<RollupDefinition> rollups, Map<String, String> properties) {
        this.ifNotExists = ifNotExists;
        this.dbName = dbName;
        this.tableName = tableName;
        this.ctasColumns = cols;
        this.columns = ImmutableList.of();
        this.indexes = ImmutableList.of();
        this.engineName = engineName;
        this.keysType = keysType;
        this.keys = Utils.copyRequiredList(keys);
        this.comment = comment;
        this.partitionType = partitionType;
        this.partitionColumns = partitionColumns;
        this.partitions = partitions;
        this.distribution = distribution;
        this.rollups = Utils.copyRequiredList(rollups);
        this.properties = properties;
    }

    public List<String> getCtasColumns() {
        return ctasColumns;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getTableNameParts() {
        if (dbName != null) {
            return ImmutableList.of(dbName, tableName);
        }
        return ImmutableList.of(tableName);
    }

    /**
     * analyze create table info
     */
    public void validate(ConnectContext ctx) {
        // pre-block in some cases.
        if (columns.isEmpty()) {
            throw new AnalysisException("table should contain at least one column");
        }
        if (distribution == null) {
            throw new AnalysisException("Create olap table should contain distribution desc");
        }
        if (!engineName.equals("olap")) {
            throw new AnalysisException("currently Nereids support olap engine only");
        }

        // analyze table name
        if (dbName == null) {
            dbName = ClusterNamespace.getFullName(ctx.getClusterName(), ctx.getDatabase());
        } else {
            dbName = ClusterNamespace.getFullName(ctx.getClusterName(), dbName);
        }

        // analyze partitions
        Map<String, ColumnDefinition> columnMap = columns.stream()
                .collect(Collectors.toMap(ColumnDefinition::getName, c -> c));

        if (partitions != null) {
            if (!checkPartitionsTypes()) {
                throw new AnalysisException("partitions types is invalid, expected FIXED or LESS in range partitions"
                        + " and IN in list partitions");
            }
            Set<String> partitionColumnSets = Sets.newHashSet(partitionColumns);
            if (partitionColumnSets.size() != partitionColumns.size()) {
                throw new AnalysisException("Duplicate partition keys is not allowed");
            }
            partitionColumns.forEach(c -> {
                if (!columnMap.containsKey(c)) {
                    throw new AnalysisException(String.format("partition key %s is not found", c));
                }
                ColumnDefinition column = columnMap.get(c);
                if (column.getType().isFloatLikeType()) {
                    throw new AnalysisException("Floating point type column can not be partition column");
                }
            });
            partitions.forEach(p -> p.validate(Maps.newHashMap(properties)));
        }

        // analyze distribution descriptor
        distribution.validate(columnMap, keysType);

        // analyze key set.
        boolean enableDuplicateWithoutKeysByDefault = false;
        if (properties != null) {
            try {
                enableDuplicateWithoutKeysByDefault =
                        PropertyAnalyzer.analyzeEnableDuplicateWithoutKeysByDefault(properties);
            } catch (Exception e) {
                throw new AnalysisException(e.getMessage(), e.getCause());
            }
        }
        if (keys.isEmpty()) {
            keys = Lists.newArrayList();
            int keyLength = 0;
            for (ColumnDefinition column : columns) {
                DataType type = column.getType();
                Type catalogType = column.getType().toCatalogDataType();
                keyLength += catalogType.getIndexSize();
                if (keys.size() >= FeConstants.shortkey_max_column_count
                        || keyLength > FeConstants.shortkey_maxsize_bytes) {
                    if (keys.size() == 0 && type.isStringLikeType()) {
                        keys.add(column.getName());
                    }
                    break;
                }
                if (type.isFloatLikeType() || type.isStringType() || type.isJsonType() || catalogType.isComplexType()) {
                    break;
                }
                keys.add(column.getName());
                if (type.isVarcharType()) {
                    break;
                }
            }
            // The OLAP table must have at least one short key,
            // and the float and double should not be short key,
            // so the float and double could not be the first column in OLAP table.
            if (keys.isEmpty()) {
                throw new AnalysisException("The olap table first column could not be float, double, string"
                        + " or array, struct, map, please use decimal or varchar instead.");
            }
        } else if (enableDuplicateWithoutKeysByDefault) {
            throw new AnalysisException("table property 'enable_duplicate_without_keys_by_default' only can"
                    + " set 'true' when create olap table by default.");
        }
        Set<String> keysSet = Sets.newHashSet(keys);
        columns.forEach(c -> c.validate(keysSet));
    }

    public void validateCreateTableAsSelect(List<ColumnDefinition> columns, ConnectContext ctx) {
        this.columns = columns;
        validate(ctx);
    }

    /**
     * check partitions types.
     */
    public boolean checkPartitionsTypes() {
        if (partitionType.equals("RANGE")) {
            if (partitions.stream().allMatch(p -> p instanceof StepPartition)) {
                return true;
            }
            return partitions.stream().allMatch(p -> (p instanceof LessThanPartition)
                    || (p instanceof FixedRangePartition));
        }
        return partitionType.equals("LIST") && partitions.stream().allMatch(p -> p instanceof InPartition);
    }

    /**
     * translate to catalog create table stmt
     */
    public CreateTableStmt translateToCatalogStyle() {
        List<Column> catalogColumns = columns.stream()
                .map(ColumnDefinition::translateToCatalogStyle)
                .collect(Collectors.toList());

        List<Index> catalogIndexes = indexes.stream().map(IndexDefinition::translateToCatalogStyle)
                .collect(Collectors.toList());
        PartitionDesc partitionDesc = null;
        if (partitions != null) {
            List<AllPartitionDesc> partitionDescs = partitions.stream()
                    .map(PartitionDefinition::translateToCatalogStyle).collect(Collectors.toList());
            try {
                if (partitionType.equals("RANGE")) {
                    partitionDesc = new RangePartitionDesc(partitionColumns, partitionDescs);
                } else {
                    partitionDesc = new ListPartitionDesc(partitionColumns, partitionDescs);
                }
            } catch (Exception e) {
                throw new AnalysisException(e.getMessage(), e.getCause());
            }
        }
        return new CreateTableStmt(ifNotExists, false,
                new TableName(Env.getCurrentEnv().getCurrentCatalog().getName(), dbName, tableName),
                catalogColumns,
                catalogIndexes,
                engineName,
                new KeysDesc(keysType, keys),
                partitionDesc,
                distribution.translateToCatalogStyle(),
                Maps.newHashMap(properties),
                null,
                comment,
                ImmutableList.of(),
                false,
                null);
    }
}
