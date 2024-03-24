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

import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DistributionDesc;
import org.apache.doris.analysis.IndexDef;
import org.apache.doris.analysis.KeysDesc;
import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.AutoBucketUtils;
import org.apache.doris.common.util.InternalDatabaseUtil;
import org.apache.doris.common.util.ParseUtil;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.es.EsUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.PartitionTableInfo;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
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
    private DistributionDescriptor distribution;
    private final List<RollupDefinition> rollups;
    private Map<String, String> properties;
    private Map<String, String> extProperties;
    private boolean isEnableMergeOnWrite = false;

    private boolean isExternal = false;
    private String clusterName = null;
    private List<String> clusterKeysColumnNames = null;
    private List<Integer> clusterKeysColumnIds = null;
    private PartitionTableInfo partitionTableInfo;

    /**
     * constructor for create table
     */
    public CreateTableInfo(boolean ifNotExists, boolean isExternal, String ctlName, String dbName,
            String tableName, List<ColumnDefinition> columns, List<IndexDefinition> indexes,
            String engineName, KeysType keysType, List<String> keys, String comment,
            PartitionTableInfo partitionTableInfo,
            DistributionDescriptor distribution, List<RollupDefinition> rollups,
            Map<String, String> properties, Map<String, String> extProperties,
            List<String> clusterKeyColumnNames) {
        this.ifNotExists = ifNotExists;
        this.isExternal = isExternal;
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
        this.partitionTableInfo = partitionTableInfo;
        this.distribution = distribution;
        this.rollups = Utils.copyRequiredList(rollups);
        this.properties = properties;
        this.extProperties = extProperties;
        this.clusterKeysColumnNames = Utils.copyRequiredList(clusterKeyColumnNames);
    }

    /**
     * constructor for create table as select
     */
    public CreateTableInfo(boolean ifNotExists, boolean isExternal, String ctlName, String dbName,
            String tableName, List<String> cols, String engineName, KeysType keysType,
            List<String> keys, String comment,
            PartitionTableInfo partitionTableInfo,
            DistributionDescriptor distribution, List<RollupDefinition> rollups,
            Map<String, String> properties, Map<String, String> extProperties,
            List<String> clusterKeyColumnNames) {
        this.ifNotExists = ifNotExists;
        this.isExternal = isExternal;
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
        this.partitionTableInfo = partitionTableInfo;
        this.distribution = distribution;
        this.rollups = Utils.copyRequiredList(rollups);
        this.properties = properties;
        this.extProperties = extProperties;
        this.clusterKeysColumnNames = Utils.copyRequiredList(clusterKeyColumnNames);
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

    /**
     * full qualifier table name.
     */
    public List<String> getTableNameParts() {
        if (ctlName != null && dbName != null) {
            return ImmutableList.of(ctlName, dbName, tableName);
        }
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

        checkEngineName();

        if (properties == null) {
            properties = Maps.newHashMap();
        }

        if (Strings.isNullOrEmpty(engineName) || engineName.equalsIgnoreCase("olap")) {
            if (distribution == null) {
                throw new AnalysisException("Create olap table should contain distribution desc");
            }
            properties = maybeRewriteByAutoBucket(distribution, properties);
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
            dbName = ctx.getDatabase();
        }
        try {
            InternalDatabaseUtil.checkDatabase(dbName, ConnectContext.get());
        } catch (org.apache.doris.common.AnalysisException e) {
            throw new AnalysisException(e.getMessage(), e.getCause());
        }
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(), dbName,
                tableName, PrivPredicate.CREATE)) {
            try {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                        "CREATE");
            } catch (Exception ex) {
                throw new AnalysisException(ex.getMessage(), ex.getCause());
            }
        }

        Preconditions.checkState(!Strings.isNullOrEmpty(ctlName), "catalog name is null or empty");
        Preconditions.checkState(!Strings.isNullOrEmpty(dbName), "database name is null or empty");

        //check datev1 and decimalv2
        for (ColumnDefinition columnDef : columns) {
            if (columnDef.getType().isDateType() && Config.disable_datev1) {
                throw new AnalysisException(
                        "Disable to create table with `DATE` type columns, please use `DATEV2`.");
            }
            if (columnDef.getType().isDecimalV2Type() && Config.disable_decimalv2) {
                throw new AnalysisException("Disable to create table with `DECIMAL` type columns,"
                        + "please use `DECIMALV3`.");
            }
        }

        if (engineName.equalsIgnoreCase("olap")) {
            boolean enableDuplicateWithoutKeysByDefault = false;
            properties = PropertyAnalyzer.getInstance().rewriteOlapProperties(ctlName, dbName, properties);
            try {
                if (properties != null) {
                    enableDuplicateWithoutKeysByDefault =
                        PropertyAnalyzer.analyzeEnableDuplicateWithoutKeysByDefault(properties);
                }
            } catch (Exception e) {
                throw new AnalysisException(e.getMessage(), e.getCause());
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
                    if (!enableDuplicateWithoutKeysByDefault) {
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
                    }
                    keysType = KeysType.DUP_KEYS;
                }
                // The OLAP table must have at least one short key,
                // and the float and double should not be short key,
                // so the float and double could not be the first column in OLAP table.
                if (keys.isEmpty() && (keysType != KeysType.DUP_KEYS
                        || !enableDuplicateWithoutKeysByDefault)) {
                    throw new AnalysisException(
                            "The olap table first column could not be float, double, string"
                                    + " or array, struct, map, please use decimal or varchar instead.");
                }
            } else if (enableDuplicateWithoutKeysByDefault) {
                throw new AnalysisException(
                        "table property 'enable_duplicate_without_keys_by_default' only can"
                                + " set 'true' when create olap table by default.");
            }

            if (properties != null
                    && properties.containsKey(PropertyAnalyzer.ENABLE_UNIQUE_KEY_MERGE_ON_WRITE)) {
                if (!keysType.equals(KeysType.UNIQUE_KEYS)) {
                    throw new AnalysisException(PropertyAnalyzer.ENABLE_UNIQUE_KEY_MERGE_ON_WRITE
                            + " property only support unique key table");
                }
            }

            if (keysType == KeysType.UNIQUE_KEYS) {
                isEnableMergeOnWrite = false;
                if (properties != null) {
                    properties = PropertyAnalyzer.enableUniqueKeyMergeOnWriteIfNotExists(properties);
                    // `analyzeXXX` would modify `properties`, which will be used later,
                    // so we just clone a properties map here.
                    try {
                        isEnableMergeOnWrite = PropertyAnalyzer.analyzeUniqueKeyMergeOnWrite(
                                new HashMap<>(properties));
                    } catch (Exception e) {
                        throw new AnalysisException(e.getMessage(), e.getCause());
                    }
                }
            }

            validateKeyColumns();
            if (!clusterKeysColumnNames.isEmpty() && !isEnableMergeOnWrite) {
                throw new AnalysisException(
                        "Cluster keys only support unique keys table which enabled "
                                + PropertyAnalyzer.ENABLE_UNIQUE_KEY_MERGE_ON_WRITE);
            }
            for (int i = 0; i < keys.size(); ++i) {
                columns.get(i).setIsKey(true);
            }

            if (keysType != KeysType.AGG_KEYS) {
                AggregateType type = AggregateType.REPLACE;
                if (keysType == KeysType.DUP_KEYS) {
                    type = AggregateType.NONE;
                }
                if (keysType == KeysType.UNIQUE_KEYS && isEnableMergeOnWrite) {
                    type = AggregateType.NONE;
                }
                for (int i = keys.size(); i < columns.size(); ++i) {
                    columns.get(i).setAggType(type);
                }
            }

            // add hidden column
            if (Config.enable_batch_delete_by_default && keysType.equals(KeysType.UNIQUE_KEYS)) {
                if (isEnableMergeOnWrite) {
                    columns.add(ColumnDefinition.newDeleteSignColumnDefinition(AggregateType.NONE));
                } else {
                    columns.add(
                            ColumnDefinition.newDeleteSignColumnDefinition(AggregateType.REPLACE));
                }
            }

            // add a hidden column as row store
            boolean storeRowColumn = false;
            if (properties != null) {
                try {
                    storeRowColumn =
                            PropertyAnalyzer.analyzeStoreRowColumn(Maps.newHashMap(properties));
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
                        columns.add(
                                ColumnDefinition.newRowStoreColumnDefinition(AggregateType.NONE));
                    } else {
                        columns.add(ColumnDefinition
                                .newRowStoreColumnDefinition(AggregateType.REPLACE));
                    }
                } else {
                    columns.add(ColumnDefinition.newRowStoreColumnDefinition(null));
                }
            }
            if (Config.enable_hidden_version_column_by_default
                    && keysType.equals(KeysType.UNIQUE_KEYS)) {
                if (isEnableMergeOnWrite) {
                    columns.add(ColumnDefinition.newVersionColumnDefinition(AggregateType.NONE));
                } else {
                    columns.add(ColumnDefinition.newVersionColumnDefinition(AggregateType.REPLACE));
                }
            }

            // validate partitions
            Map<String, ColumnDefinition> columnMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            columns.forEach(c -> {
                if (columnMap.put(c.getName(), c) != null) {
                    try {
                        ErrorReport.reportAnalysisException(ErrorCode.ERR_DUP_FIELDNAME,
                                c.getName());
                    } catch (Exception e) {
                        throw new AnalysisException(e.getMessage(), e.getCause());
                    }
                }
            });

            // validate partition
            partitionTableInfo.validatePartitionInfo(columnMap, properties, ctx, isEnableMergeOnWrite, isExternal);

            // validate distribution descriptor
            distribution.updateCols(columns.get(0).getName());
            distribution.validate(columnMap, keysType);

            // validate key set.
            if (!distribution.isHash()) {
                if (keysType.equals(KeysType.UNIQUE_KEYS)) {
                    throw new AnalysisException(
                            "Should not be distributed by random when keys type is unique");
                } else if (keysType.equals(KeysType.AGG_KEYS)) {
                    for (ColumnDefinition c : columns) {
                        if (AggregateType.REPLACE.equals(c.getAggType())
                                || AggregateType.REPLACE_IF_NOT_NULL.equals(c.getAggType())) {
                            throw new AnalysisException(
                                    "Should not be distributed by random when keys type is agg"
                                            + "and column is in replace, [" + c.getName()
                                            + "] is invalid");
                        }
                    }
                }
            }
        } else {
            // mysql, broker and hive do not need key desc
            if (keysType != null) {
                throw new AnalysisException(
                        "Create " + engineName + " table should not contain keys desc");
            }

            if (!rollups.isEmpty()) {
                throw new AnalysisException(engineName + " catalog doesn't support rollup tables.");
            }

            if (engineName.equalsIgnoreCase("iceberg") && distribution != null) {
                throw new AnalysisException(
                    "Iceberg doesn't support 'DISTRIBUTE BY', "
                        + "and you can use 'bucket(num, column)' in 'PARTITIONED BY'.");
            }

            for (ColumnDefinition columnDef : columns) {
                columnDef.setIsKey(true);
            }
        }

        // validate column
        try {
            if (!engineName.equals("elasticsearch") && columns.isEmpty()) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLE_MUST_HAVE_COLUMNS);
            }
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e.getCause());
        }

        final boolean finalEnableMergeOnWrite = isEnableMergeOnWrite;
        Set<String> keysSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        keysSet.addAll(keys);
        columns.forEach(c -> c.validate(engineName.equals("olap"), keysSet, finalEnableMergeOnWrite,
                keysType));

        // validate index
        if (!indexes.isEmpty()) {
            Set<String> distinct = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            Set<Pair<IndexDef.IndexType, List<String>>> distinctCol = new HashSet<>();

            for (IndexDefinition indexDef : indexes) {
                indexDef.validate();
                if (!engineName.equalsIgnoreCase("olap")) {
                    throw new AnalysisException(
                            "index only support in olap engine at current version.");
                }
                for (String indexColName : indexDef.getColumnNames()) {
                    boolean found = false;
                    for (ColumnDefinition column : columns) {
                        if (column.getName().equalsIgnoreCase(indexColName)) {
                            indexDef.checkColumn(column, keysType, isEnableMergeOnWrite);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        throw new AnalysisException(
                                "Column does not exist in table. invalid column: " + indexColName);
                    }
                }
                distinct.add(indexDef.getIndexName());
                distinctCol.add(Pair.of(indexDef.getIndexType(), indexDef.getColumnNames().stream()
                        .map(String::toUpperCase).collect(Collectors.toList())));
            }
            if (distinct.size() != indexes.size()) {
                throw new AnalysisException("index name must be unique.");
            }
            if (distinctCol.size() != indexes.size()) {
                throw new AnalysisException(
                        "same index columns have multiple same type index is not allowed.");
            }
        }
    }

    /**
     * validate ctas definition
     */
    public void validateCreateTableAsSelect(List<String> qualifierTableName, List<ColumnDefinition> columns,
                                            ConnectContext ctx) {
        this.columns = Utils.copyRequiredMutableList(columns);
        // bucket num is hard coded 10 to be consistent with legacy planner
        if (engineName.equals("olap") && this.distribution == null) {
            String catalogName = qualifierTableName.get(0);
            if (!catalogName.equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
                throw new AnalysisException("Cannot create olap table out of internal catalog."
                        + "Make sure 'engine' type is specified when use the catalog: " + catalogName);
            }
            this.distribution = new DistributionDescriptor(true, false, 10,
                    Lists.newArrayList(columns.get(0).getName()));
        }
        validate(ctx);
    }

    private void checkEngineName() {
        if (engineName.equals("mysql") || engineName.equals("odbc") || engineName.equals("broker")
                || engineName.equals("elasticsearch") || engineName.equals("hive") || engineName.equals("iceberg")
                || engineName.equals("jdbc")) {
            if (!isExternal) {
                // this is for compatibility
                isExternal = true;
            }
        } else {
            if (isExternal) {
                throw new AnalysisException(
                        "Do not support external table with engine name = olap");
            } else if (!engineName.equals("olap")) {
                throw new AnalysisException(
                        "Do not support table with engine name = " + engineName);
            }
        }

        if (!Config.enable_odbc_mysql_broker_table && (engineName.equals("odbc")
                || engineName.equals("mysql") || engineName.equals("broker"))) {
            throw new AnalysisException("odbc, mysql and broker table is no longer supported."
                    + " For odbc and mysql external table, use jdbc table or jdbc catalog instead."
                    + " For broker table, use table valued function instead."
                    + ". Or you can temporarily set 'enable_odbc_mysql_broker_table=true'"
                    + " in fe.conf to reopen this feature.");
        }
    }

    // if auto bucket auto bucket enable, rewrite distribution bucket num &&
    // set properties[PropertyAnalyzer.PROPERTIES_AUTO_BUCKET] = "true"
    private static Map<String, String> maybeRewriteByAutoBucket(
            DistributionDescriptor distributionDesc, Map<String, String> properties) {
        if (distributionDesc == null || !distributionDesc.isAutoBucket()) {
            return properties;
        }

        // auto bucket is enable
        Map<String, String> newProperties = properties;
        if (newProperties == null) {
            newProperties = new HashMap<String, String>();
        }
        newProperties.put(PropertyAnalyzer.PROPERTIES_AUTO_BUCKET, "true");

        try {
            if (!newProperties.containsKey(PropertyAnalyzer.PROPERTIES_ESTIMATE_PARTITION_SIZE)) {
                distributionDesc.updateBucketNum(FeConstants.default_bucket_num);
            } else {
                long partitionSize = ParseUtil.analyzeDataVolumn(
                        newProperties.get(PropertyAnalyzer.PROPERTIES_ESTIMATE_PARTITION_SIZE));
                distributionDesc.updateBucketNum(AutoBucketUtils.getBucketsNum(partitionSize,
                        Config.autobucket_min_buckets));
            }
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e.getCause());
        }
        return newProperties;
    }

    private void validateKeyColumns() {
        if (keysType == null) {
            throw new AnalysisException("Keys type is null.");
        }

        if (keys.isEmpty() && keysType != KeysType.DUP_KEYS) {
            throw new AnalysisException("The number of key columns is 0.");
        }

        if (keys.size() > columns.size()) {
            throw new AnalysisException(
                    "The number of key columns should be less than the number of columns.");
        }

        if (!clusterKeysColumnNames.isEmpty()) {
            if (keysType != KeysType.UNIQUE_KEYS) {
                throw new AnalysisException("Cluster keys only support unique keys table.");
            }
            clusterKeysColumnIds = Lists.newArrayList();
            for (int i = 0; i < clusterKeysColumnNames.size(); ++i) {
                String name = clusterKeysColumnNames.get(i);
                // check if key is duplicate
                for (int j = 0; j < i; j++) {
                    if (clusterKeysColumnNames.get(j).equalsIgnoreCase(name)) {
                        throw new AnalysisException("Duplicate cluster key column[" + name + "].");
                    }
                }
                // check if key exists and generate key column ids
                for (int j = 0; j < columns.size(); j++) {
                    if (columns.get(j).getName().equalsIgnoreCase(name)) {
                        columns.get(j).setClusterKeyId(clusterKeysColumnIds.size());
                        clusterKeysColumnIds.add(j);
                        break;
                    }
                    if (j == columns.size() - 1) {
                        throw new AnalysisException(
                                "Key cluster column[" + name + "] doesn't exist.");
                    }
                }
            }

            int minKeySize = keys.size() < clusterKeysColumnNames.size() ? keys.size()
                    : clusterKeysColumnNames.size();
            boolean sameKey = true;
            for (int i = 0; i < minKeySize; ++i) {
                if (!keys.get(i).equalsIgnoreCase(clusterKeysColumnNames.get(i))) {
                    sameKey = false;
                    break;
                }
            }
            if (sameKey) {
                throw new AnalysisException("Unique keys and cluster keys should be different.");
            }
        }

        for (int i = 0; i < keys.size(); ++i) {
            String name = columns.get(i).getName();
            if (!keys.get(i).equalsIgnoreCase(name)) {
                String keyName = keys.get(i);
                if (columns.stream().noneMatch(col -> col.getName().equalsIgnoreCase(keyName))) {
                    throw new AnalysisException("Key column[" + keyName + "] doesn't exist.");
                }
                throw new AnalysisException("Key columns should be a ordered prefix of the schema."
                        + " KeyColumns[" + i + "] (starts from zero) is " + keyName + ", "
                        + "but corresponding column is " + name + " in the previous "
                        + "columns declaration.");
            }

            if (columns.get(i).getAggType() != null) {
                throw new AnalysisException(
                        "Key column[" + name + "] should not specify aggregate type.");
            }
        }

        // for olap table
        for (int i = keys.size(); i < columns.size(); ++i) {
            if (keysType == KeysType.AGG_KEYS) {
                if (columns.get(i).getAggType() == null) {
                    throw new AnalysisException(
                            keysType.name() + " table should specify aggregate type for "
                                    + "non-key column[" + columns.get(i).getName() + "]");
                }
            } else {
                if (columns.get(i).getAggType() != null) {
                    throw new AnalysisException(
                            keysType.name() + " table should not specify aggregate type for "
                                    + "non-key column[" + columns.get(i).getName() + "]");
                }
            }
        }
    }

    /**
     * translate to catalog create table stmt
     */
    public CreateTableStmt translateToLegacyStmt() {
        PartitionDesc partitionDesc = partitionTableInfo.convertToPartitionDesc(isExternal);
        List<AlterClause> addRollups = Lists.newArrayList();
        if (!rollups.isEmpty()) {
            addRollups.addAll(rollups.stream().map(RollupDefinition::translateToCatalogStyle)
                    .collect(Collectors.toList()));
        }

        List<Column> catalogColumns = columns.stream()
                .map(ColumnDefinition::translateToCatalogStyle).collect(Collectors.toList());

        List<Index> catalogIndexes = indexes.stream().map(IndexDefinition::translateToCatalogStyle)
                .collect(Collectors.toList());
        DistributionDesc distributionDesc =
                distribution != null ? distribution.translateToCatalogStyle() : null;

        // TODO should move this code to validate function
        // EsUtil.analyzePartitionAndDistributionDesc only accept DistributionDesc and PartitionDesc
        if (engineName.equals("elasticsearch")) {
            try {
                EsUtil.analyzePartitionAndDistributionDesc(partitionDesc, distributionDesc);
            } catch (Exception e) {
                throw new AnalysisException(e.getMessage(), e.getCause());
            }
        } else if (!engineName.equals("olap")) {
            if (!engineName.equals("hive") && distributionDesc != null) {
                throw new AnalysisException("Create " + engineName
                    + " table should not contain distribution desc");
            }
            if (!engineName.equals("hive") && !engineName.equals("iceberg") && partitionDesc != null) {
                throw new AnalysisException("Create " + engineName
                        + " table should not contain partition desc");
            }
        }

        return new CreateTableStmt(ifNotExists, isExternal,
                new TableName(ctlName, dbName, tableName),
                catalogColumns, catalogIndexes, engineName,
                new KeysDesc(keysType, keys, clusterKeysColumnNames, clusterKeysColumnIds),
                partitionDesc, distributionDesc, Maps.newHashMap(properties), extProperties,
                comment, addRollups, null);
    }
}
