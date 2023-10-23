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
import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.KeysDesc;
import org.apache.doris.analysis.ListPartitionDesc;
import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.analysis.RangePartitionDesc;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.Type;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * table info in creating table.
 */
public class CreateTableInfo {
    private final boolean ifNotExists;
    private String ctlName;
    private String dbName;
    private final String tableName;
    private List<ColumnDefinition> columns;
    private final List<IndexDefinition> indexes;
    private final List<String> ctasColumns;
    private final String engineName;
    private KeysType keysType;
    private List<String> keys;
    private final String comment;
    private final String partitionType;
    private final List<String> partitionColumns;
    private final List<PartitionDefinition> partitions;
    private final DistributionDescriptor distribution;
    private final List<RollupDefinition> rollups;
    private Map<String, String> properties;
    private boolean isEnableMergeOnWrite = false;

    /**
     * constructor for create table
     */
    public CreateTableInfo(boolean ifNotExists, String ctlName, String dbName, String tableName,
            List<ColumnDefinition> columns, List<IndexDefinition> indexes, String engineName,
            KeysType keysType, List<String> keys, String comment,
            String partitionType, List<String> partitionColumns, List<PartitionDefinition> partitions,
            DistributionDescriptor distribution, List<RollupDefinition> rollups, Map<String, String> properties) {
        this.ifNotExists = ifNotExists;
        this.ctlName = ctlName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.ctasColumns = null;
        this.columns = Utils.copyRequiredMutableList(columns);
        this.indexes = Utils.copyRequiredMutableList(indexes);
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
    public CreateTableInfo(boolean ifNotExists, String ctlName, String dbName, String tableName, List<String> cols,
            String engineName, KeysType keysType, List<String> keys, String comment,
            String partitionType, List<String> partitionColumns, List<PartitionDefinition> partitions,
            DistributionDescriptor distribution, List<RollupDefinition> rollups, Map<String, String> properties) {
        this.ifNotExists = ifNotExists;
        this.ctlName = ctlName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.ctasColumns = cols;
        this.columns = null;
        this.indexes = Lists.newArrayList();
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

    public String getCtlName() {
        return ctlName;
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
        if (properties == null) {
            properties = Maps.newHashMap();
        }

        try {
            FeNameFormat.checkTableName(tableName);
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e);
        }

        // analyze catalog name
        if (Strings.isNullOrEmpty(ctlName)) {
            if (ctx.getCurrentCatalog() != null) {
                ctlName = ctx.getCurrentCatalog().getName();
            } else {
                ctlName = InternalCatalog.INTERNAL_CATALOG_NAME;
            }
        }

        // analyze table name
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = ClusterNamespace.getFullName(ctx.getClusterName(), ctx.getDatabase());
        } else {
            dbName = ClusterNamespace.getFullName(ctx.getClusterName(), dbName);
        }

        Preconditions.checkState(!Strings.isNullOrEmpty(ctlName), "catalog name is null or empty");
        Preconditions.checkState(!Strings.isNullOrEmpty(dbName), "database name is null or empty");
        properties = PropertyAnalyzer.rewriteReplicaAllocationProperties(ctlName, dbName, properties);

        boolean enableDuplicateWithoutKeysByDefault = false;
        if (properties != null) {
            try {
                enableDuplicateWithoutKeysByDefault =
                        PropertyAnalyzer.analyzeEnableDuplicateWithoutKeysByDefault(Maps.newHashMap(properties));
            } catch (Exception e) {
                throw new AnalysisException(e.getMessage(), e.getCause());
            }
        }

        if (keys.isEmpty()) {
            boolean hasAggColumn = false;
            for (ColumnDefinition column : columns) {
                if (column.getAggType() != null) {
                    hasAggColumn = true;
                    break;
                }
            }
            keys = Lists.newArrayList();
            if (hasAggColumn) {
                for (ColumnDefinition column : columns) {
                    if (column.getAggType() != null) {
                        break;
                    }
                    keys.add(column.getName());
                }
                keysType = KeysType.AGG_KEYS;
            } else {
                int keyLength = 0;
                for (ColumnDefinition column : columns) {
                    DataType type = column.getType();
                    Type catalogType = column.getType().toCatalogDataType();
                    keyLength += catalogType.getIndexSize();
                    if (keys.size() >= FeConstants.shortkey_max_column_count
                            || keyLength > FeConstants.shortkey_maxsize_bytes) {
                        if (keys.isEmpty() && type.isStringLikeType()) {
                            keys.add(column.getName());
                        }
                        break;
                    }
                    if (type.isFloatLikeType() || type.isStringType() || type.isJsonType()
                            || catalogType.isComplexType()) {
                        break;
                    }
                    keys.add(column.getName());
                    if (type.isVarcharType()) {
                        break;
                    }
                }
                keysType = KeysType.DUP_KEYS;
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

        if (properties != null && properties.containsKey(PropertyAnalyzer.ENABLE_UNIQUE_KEY_MERGE_ON_WRITE)) {
            if (!keysType.equals(KeysType.UNIQUE_KEYS)) {
                throw new AnalysisException(PropertyAnalyzer.ENABLE_UNIQUE_KEY_MERGE_ON_WRITE
                        + " property only support unique key table");
            }
            try {
                isEnableMergeOnWrite = PropertyAnalyzer.analyzeUniqueKeyMergeOnWrite(Maps.newHashMap(properties));
            } catch (Exception e) {
                throw new AnalysisException(e.getMessage(), e.getCause());
            }
        }

        // add hidden column
        if (Config.enable_batch_delete_by_default && keysType.equals(KeysType.UNIQUE_KEYS)) {
            if (isEnableMergeOnWrite) {
                columns.add(ColumnDefinition.newDeleteSignColumnDefinition(AggregateType.NONE));
            } else {
                columns.add(ColumnDefinition.newDeleteSignColumnDefinition(AggregateType.REPLACE));
            }
        }

        // add a hidden column as row store
        boolean storeRowColumn = false;
        if (properties != null) {
            try {
                storeRowColumn = PropertyAnalyzer.analyzeStoreRowColumn(Maps.newHashMap(properties));
            } catch (Exception e) {
                throw new AnalysisException(e.getMessage(), e.getCause());
            }
        }
        if (storeRowColumn) {
            if (keysType.equals(KeysType.AGG_KEYS)) {
                throw new AnalysisException("Aggregate table can't support row column now");
            }
            if (keysType.equals(KeysType.UNIQUE_KEYS)) {
                if (isEnableMergeOnWrite) {
                    columns.add(ColumnDefinition.newRowStoreColumnDefinition(AggregateType.NONE));
                } else {
                    columns.add(ColumnDefinition.newRowStoreColumnDefinition(AggregateType.REPLACE));
                }
            } else {
                columns.add(ColumnDefinition.newRowStoreColumnDefinition(null));
            }
        }
        if (Config.enable_hidden_version_column_by_default && keysType.equals(KeysType.UNIQUE_KEYS)) {
            if (isEnableMergeOnWrite) {
                columns.add(ColumnDefinition.newVersionColumnDefinition(AggregateType.NONE));
            } else {
                columns.add(ColumnDefinition.newVersionColumnDefinition(AggregateType.REPLACE));
            }
        }

        // analyze column
        final boolean finalEnableMergeOnWrite = isEnableMergeOnWrite;
        Set<String> keysSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        keysSet.addAll(keys);
        columns.forEach(c -> c.validate(keysSet, finalEnableMergeOnWrite, keysType));

        // analyze partitions
        Map<String, ColumnDefinition> columnMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        columns.forEach(c -> columnMap.put(c.getName(), c));

        if (partitions != null) {
            partitionColumns.forEach(p -> {
                if (!columnMap.containsKey(p)) {
                    throw new AnalysisException(String.format("partition key %s is not exists", p));
                }
                validatePartitionColumn(columnMap.get(p), ctx);
            });
            if (!checkPartitionsTypes()) {
                throw new AnalysisException("partitions types is invalid, expected FIXED or LESS in range partitions"
                        + " and IN in list partitions");
            }
            Set<String> partitionNames = Sets.newHashSet();
            for (PartitionDefinition partition : partitions) {
                if (partition instanceof StepPartition) {
                    continue;
                }
                String partitionName = partition.getPartitionName();
                if (partitionNames.contains(partitionName)) {
                    throw new AnalysisException("Duplicated named partition: " + partitionName);
                }
                partitionNames.add(partitionName);
            }
            Set<String> partitionColumnSets = Sets.newHashSet();
            List<String> duplicatesKeys = partitionColumns.stream()
                    .filter(c -> !partitionColumnSets.add(c))
                    .collect(Collectors.toList());
            if (!duplicatesKeys.isEmpty()) {
                throw new AnalysisException("Duplicated partition column " + duplicatesKeys.get(0));
            }
            partitions.forEach(p -> {
                p.setPartitionTypes(partitionColumns.stream().map(s -> columnMap.get(s).getType())
                        .collect(Collectors.toList()));
                p.validate(Maps.newHashMap(properties));
            });
        }

        // analyze distribution descriptor
        distribution.updateCols(columns.get(0).getName());
        distribution.validate(columnMap, keysType);

        // analyze key set.
        if (!distribution.isHash()) {
            if (keysType.equals(KeysType.UNIQUE_KEYS)) {
                throw new AnalysisException("Should not be distributed by random when keys type is unique");
            } else if (keysType.equals(KeysType.AGG_KEYS)) {
                for (ColumnDefinition c : columns) {
                    if (AggregateType.REPLACE.equals(c.getAggType())
                            || AggregateType.REPLACE_IF_NOT_NULL.equals(c.getAggType())) {
                        throw new AnalysisException("Should not be distributed by random when keys type is agg"
                                + "and column is in replace, [" + c.getName() + "] is invalid");
                    }
                }
            }
        }
    }

    public void validateCreateTableAsSelect(List<ColumnDefinition> columns, ConnectContext ctx) {
        this.columns = Utils.copyRequiredMutableList(columns);
        validate(ctx);
    }

    /**
     * check partitions types.
     */
    private boolean checkPartitionsTypes() {
        if (partitionType.equalsIgnoreCase(PartitionType.RANGE.name())) {
            if (partitions.stream().allMatch(p -> p instanceof StepPartition)) {
                return true;
            }
            return partitions.stream().allMatch(p -> (p instanceof LessThanPartition)
                    || (p instanceof FixedRangePartition));
        }
        return partitionType.equalsIgnoreCase(PartitionType.LIST.name())
                && partitions.stream().allMatch(p -> p instanceof InPartition);
    }

    private void validatePartitionColumn(ColumnDefinition column, ConnectContext ctx) {
        if (!column.isKey() && (!column.getAggType().equals(AggregateType.NONE) || isEnableMergeOnWrite)) {
            throw new AnalysisException("The partition column could not be aggregated column");
        }
        if (column.getType().isFloatLikeType()) {
            throw new AnalysisException("Floating point type column can not be partition column");
        }
        if (column.getType().isStringType()) {
            throw new AnalysisException("String Type should not be used in partition column["
                    + column.getName() + "].");
        }
        if (column.getType().isComplexType()) {
            throw new AnalysisException("Complex type column can't be partition column: "
                    + column.getType().toString());
        }
        if (!ctx.getSessionVariable().isAllowPartitionColumnNullable() && column.isNullable()) {
            throw new AnalysisException("The partition column must be NOT NULL");
        }
        if (partitionType.equalsIgnoreCase(PartitionType.LIST.name()) && column.isNullable()) {
            throw new AnalysisException("The list partition column must be NOT NULL");
        }
    }

    /**
     * translate to catalog create table stmt
     */
    public CreateTableStmt translateToLegacyStmt() {
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
                if (partitionType.equals(PartitionType.RANGE.name())) {
                    partitionDesc = new RangePartitionDesc(partitionColumns, partitionDescs);
                } else {
                    partitionDesc = new ListPartitionDesc(partitionColumns, partitionDescs);
                }
            } catch (Exception e) {
                throw new AnalysisException(e.getMessage(), e.getCause());
            }
        }
        List<AlterClause> addRollups = Lists.newArrayList();
        if (rollups != null) {
            addRollups.addAll(rollups.stream()
                    .map(RollupDefinition::translateToCatalogStyle)
                    .collect(Collectors.toList()));
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
                addRollups,
                null);
    }
}
