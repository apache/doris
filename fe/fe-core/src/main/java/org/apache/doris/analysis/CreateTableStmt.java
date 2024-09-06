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

package org.apache.doris.analysis;

import org.apache.doris.analysis.IndexDef.IndexType;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.GeneratedColumnInfo;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.AutoBucketUtils;
import org.apache.doris.common.util.GeneratedColumnUtil;
import org.apache.doris.common.util.InternalDatabaseUtil;
import org.apache.doris.common.util.ParseUtil;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.datasource.es.EsUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rewrite.ExprRewriteRule;
import org.apache.doris.rewrite.ExprRewriter;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

@Deprecated
public class CreateTableStmt extends DdlStmt implements NotFallbackInParser {
    private static final Logger LOG = LogManager.getLogger(CreateTableStmt.class);

    protected static final String DEFAULT_ENGINE_NAME = "olap";
    private static final ImmutableSet<AggregateType> GENERATED_COLUMN_ALLOW_AGG_TYPE =
            ImmutableSet.of(AggregateType.REPLACE, AggregateType.REPLACE_IF_NOT_NULL);

    protected boolean ifNotExists;
    private boolean isExternal;
    protected TableName tableName;
    protected List<ColumnDef> columnDefs;
    private List<IndexDef> indexDefs;
    protected KeysDesc keysDesc;
    protected PartitionDesc partitionDesc;
    protected DistributionDesc distributionDesc;
    protected Map<String, String> properties;
    private Map<String, String> extProperties;
    protected String engineName;
    private String comment;
    private List<AlterClause> rollupAlterClauseList = Lists.newArrayList();

    private static Set<String> engineNames;

    // set in analyze
    private List<Column> columns = Lists.newArrayList();
    private List<Index> indexes = Lists.newArrayList();

    static {
        engineNames = Sets.newHashSet();
        engineNames.add("olap");
        engineNames.add("jdbc");
        engineNames.add("elasticsearch");
        engineNames.add("odbc");
        engineNames.add("mysql");
        engineNames.add("broker");
    }

    // if auto bucket enable, rewrite distribution bucket num &&
    // set properties[PropertyAnalyzer.PROPERTIES_AUTO_BUCKET] = "true"
    private static Map<String, String> maybeRewriteByAutoBucket(DistributionDesc distributionDesc,
            Map<String, String> properties) throws AnalysisException {
        if (distributionDesc == null || !distributionDesc.isAutoBucket()) {
            return properties;
        }

        // auto bucket is enable
        Map<String, String> newProperties = properties;
        if (newProperties == null) {
            newProperties = new HashMap<String, String>();
        }
        newProperties.put(PropertyAnalyzer.PROPERTIES_AUTO_BUCKET, "true");

        if (!newProperties.containsKey(PropertyAnalyzer.PROPERTIES_ESTIMATE_PARTITION_SIZE)) {
            distributionDesc.setBuckets(FeConstants.default_bucket_num);
        } else {
            long partitionSize = ParseUtil
                    .analyzeDataVolume(newProperties.get(PropertyAnalyzer.PROPERTIES_ESTIMATE_PARTITION_SIZE));
            distributionDesc.setBuckets(AutoBucketUtils.getBucketsNum(partitionSize, Config.autobucket_min_buckets));
        }

        return newProperties;
    }

    public CreateTableStmt() {
        // for persist
        tableName = new TableName();
        columnDefs = Lists.newArrayList();
    }

    public CreateTableStmt(boolean ifNotExists, boolean isExternal, TableName tableName,
            List<ColumnDef> columnDefinitions, String engineName, KeysDesc keysDesc, PartitionDesc partitionDesc,
            DistributionDesc distributionDesc, Map<String, String> properties, Map<String, String> extProperties,
            String comment) {
        this(ifNotExists, isExternal, tableName, columnDefinitions, null, engineName, keysDesc, partitionDesc,
                distributionDesc, properties, extProperties, comment, null);
    }

    public CreateTableStmt(boolean ifNotExists, boolean isExternal, TableName tableName,
            List<ColumnDef> columnDefinitions, String engineName, KeysDesc keysDesc, PartitionDesc partitionDesc,
            DistributionDesc distributionDesc, Map<String, String> properties, Map<String, String> extProperties,
            String comment, List<AlterClause> ops) {
        this(ifNotExists, isExternal, tableName, columnDefinitions, null, engineName, keysDesc, partitionDesc,
                distributionDesc, properties, extProperties, comment, ops);
    }

    public CreateTableStmt(boolean ifNotExists, boolean isExternal, TableName tableName,
            List<ColumnDef> columnDefinitions, List<IndexDef> indexDefs, String engineName, KeysDesc keysDesc,
            PartitionDesc partitionDesc, DistributionDesc distributionDesc, Map<String, String> properties,
            Map<String, String> extProperties, String comment, List<AlterClause> rollupAlterClauseList) {
        this.tableName = tableName;
        if (columnDefinitions == null) {
            this.columnDefs = Lists.newArrayList();
        } else {
            this.columnDefs = columnDefinitions;
        }
        this.indexDefs = indexDefs;
        if (Strings.isNullOrEmpty(engineName)) {
            this.engineName = DEFAULT_ENGINE_NAME;
        } else {
            this.engineName = engineName;
        }

        this.keysDesc = keysDesc;
        this.partitionDesc = partitionDesc;
        this.distributionDesc = distributionDesc;
        this.properties = properties;
        PropertyAnalyzer.getInstance().rewriteForceProperties(this.properties);
        this.extProperties = extProperties;
        this.isExternal = isExternal;
        this.ifNotExists = ifNotExists;
        this.comment = Strings.nullToEmpty(comment);

        this.rollupAlterClauseList = (rollupAlterClauseList == null) ? Lists.newArrayList() : rollupAlterClauseList;
    }

    // for Nereids
    public CreateTableStmt(boolean ifNotExists, boolean isExternal, TableName tableName, List<Column> columns,
            List<Index> indexes, String engineName, KeysDesc keysDesc, PartitionDesc partitionDesc,
            DistributionDesc distributionDesc, Map<String, String> properties, Map<String, String> extProperties,
            String comment, List<AlterClause> rollupAlterClauseList, Void unused) {
        this.ifNotExists = ifNotExists;
        this.isExternal = isExternal;
        this.tableName = tableName;
        this.columns = columns;
        this.indexes = indexes;
        this.engineName = engineName;
        this.keysDesc = keysDesc;
        this.partitionDesc = partitionDesc;
        this.distributionDesc = distributionDesc;
        this.properties = properties;
        PropertyAnalyzer.getInstance().rewriteForceProperties(this.properties);
        this.extProperties = extProperties;
        this.columnDefs = Lists.newArrayList();
        this.comment = Strings.nullToEmpty(comment);
        this.rollupAlterClauseList = (rollupAlterClauseList == null) ? Lists.newArrayList() : rollupAlterClauseList;
    }

    public void addColumnDef(ColumnDef columnDef) {
        columnDefs.add(columnDef);
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    public boolean isExternal() {
        return isExternal;
    }

    public TableName getDbTbl() {
        return tableName;
    }

    public String getTableName() {
        return tableName.getTbl();
    }

    public List<Column> getColumns() {
        return this.columns;
    }

    public KeysDesc getKeysDesc() {
        return this.keysDesc;
    }

    public PartitionDesc getPartitionDesc() {
        return this.partitionDesc;
    }

    public DistributionDesc getDistributionDesc() {
        return this.distributionDesc;
    }

    public void setDistributionDesc(DistributionDesc desc) {
        this.distributionDesc = desc;
    }

    public Map<String, String> getProperties() {
        if (this.properties == null) {
            this.properties = Maps.newHashMap();
        }
        return this.properties;
    }

    public Map<String, String> getExtProperties() {
        return this.extProperties;
    }

    public String getEngineName() {
        return engineName;
    }

    public String getCatalogName() {
        return tableName.getCtl();
    }

    public String getDbName() {
        return tableName.getDb();
    }

    public void setTableName(String newTableName) {
        tableName = new TableName(tableName.getCtl(), tableName.getDb(), newTableName);
    }

    public String getComment() {
        return comment;
    }

    public List<AlterClause> getRollupAlterClauseList() {
        return rollupAlterClauseList;
    }

    public List<Index> getIndexes() {
        return indexes;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        if (Strings.isNullOrEmpty(engineName) || engineName.equalsIgnoreCase(DEFAULT_ENGINE_NAME)) {
            this.properties = maybeRewriteByAutoBucket(distributionDesc, properties);
        }

        super.analyze(analyzer);
        tableName.analyze(analyzer);
        FeNameFormat.checkTableName(tableName.getTbl());
        InternalDatabaseUtil.checkDatabase(tableName.getDb(), ConnectContext.get());
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), tableName.getCtl(), tableName.getDb(), tableName.getTbl(),
                        PrivPredicate.CREATE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE");
        }

        analyzeEngineName();

        boolean enableDuplicateWithoutKeysByDefault = false;
        if (properties != null) {
            enableDuplicateWithoutKeysByDefault = PropertyAnalyzer.analyzeEnableDuplicateWithoutKeysByDefault(
                    properties);
        }
        // pre-block creation with column type ALL
        for (ColumnDef columnDef : columnDefs) {
            if (Objects.equals(columnDef.getType(), Type.ALL)) {
                throw new AnalysisException("Disable to create table with `ALL` type columns.");
            }
            String columnNameUpperCase = columnDef.getName().toUpperCase();
            if (columnNameUpperCase.startsWith("__DORIS_")) {
                throw new AnalysisException(
                        "Disable to create table column with name start with __DORIS_: " + columnNameUpperCase);
            }
            if (Objects.equals(columnDef.getType(), Type.DATE) && Config.disable_datev1) {
                throw new AnalysisException("Disable to create table with `DATE` type columns, please use `DATEV2`.");
            }
            if (Objects.equals(columnDef.getType(), Type.DECIMALV2) && Config.disable_decimalv2) {
                throw new AnalysisException(
                        "Disable to create table with `DECIMAL` type columns," + "please use `DECIMALV3`.");
            }
        }

        boolean enableUniqueKeyMergeOnWrite = false;
        // analyze key desc
        if (engineName.equalsIgnoreCase(DEFAULT_ENGINE_NAME)) {
            // olap table
            if (keysDesc == null) {
                List<String> keysColumnNames = Lists.newArrayList();
                int keyLength = 0;
                boolean hasAggregate = false;
                for (ColumnDef columnDef : columnDefs) {
                    if (columnDef.getAggregateType() != null) {
                        hasAggregate = true;
                        break;
                    }
                }
                if (hasAggregate) {
                    for (ColumnDef columnDef : columnDefs) {
                        if (columnDef.getAggregateType() == null && !columnDef.getType()
                                .isScalarType(PrimitiveType.STRING) && !columnDef.getType()
                                .isScalarType(PrimitiveType.JSONB)) {
                            keysColumnNames.add(columnDef.getName());
                        }
                    }
                    keysDesc = new KeysDesc(KeysType.AGG_KEYS, keysColumnNames);
                } else {
                    if (!enableDuplicateWithoutKeysByDefault) {
                        for (ColumnDef columnDef : columnDefs) {
                            keyLength += columnDef.getType().getIndexSize();
                            if (keysColumnNames.size() >= FeConstants.shortkey_max_column_count
                                    || keyLength > FeConstants.shortkey_maxsize_bytes) {
                                if (keysColumnNames.isEmpty() && columnDef.getType().getPrimitiveType()
                                        .isCharFamily()) {
                                    keysColumnNames.add(columnDef.getName());
                                }
                                break;
                            }
                            if (columnDef.getType().isFloatingPointType()) {
                                break;
                            }
                            if (columnDef.getType().getPrimitiveType() == PrimitiveType.STRING) {
                                break;
                            }
                            if (columnDef.getType().getPrimitiveType() == PrimitiveType.JSONB) {
                                break;
                            }
                            if (columnDef.getType().isComplexType()) {
                                break;
                            }
                            if (columnDef.getType().getPrimitiveType() == PrimitiveType.VARCHAR) {
                                keysColumnNames.add(columnDef.getName());
                                break;
                            }
                            keysColumnNames.add(columnDef.getName());
                        }
                        // The OLAP table must have at least one short key,
                        // and the float and double should not be short key,
                        // so the float and double could not be the first column in OLAP table.
                        if (keysColumnNames.isEmpty()) {
                            throw new AnalysisException("The olap table first column could not be float, double, string"
                                    + " or array, struct, map, please use decimal or varchar instead.");
                        }
                    }
                    keysDesc = new KeysDesc(KeysType.DUP_KEYS, keysColumnNames);
                }
            } else {
                if (enableDuplicateWithoutKeysByDefault) {
                    throw new AnalysisException("table property 'enable_duplicate_without_keys_by_default' only can"
                            + " set 'true' when create olap table by default.");
                }
            }

            if (properties != null && properties.containsKey(PropertyAnalyzer.ENABLE_UNIQUE_KEY_MERGE_ON_WRITE)
                    && keysDesc.getKeysType() != KeysType.UNIQUE_KEYS) {
                throw new AnalysisException(
                        PropertyAnalyzer.ENABLE_UNIQUE_KEY_MERGE_ON_WRITE + " property only support unique key table");
            }
            if (keysDesc.getKeysType() == KeysType.UNIQUE_KEYS) {
                enableUniqueKeyMergeOnWrite = false;
                if (properties != null) {
                    properties = PropertyAnalyzer.enableUniqueKeyMergeOnWriteIfNotExists(properties);
                    // `analyzeXXX` would modify `properties`, which will be used later,
                    // so we just clone a properties map here.
                    enableUniqueKeyMergeOnWrite = PropertyAnalyzer.analyzeUniqueKeyMergeOnWrite(
                            new HashMap<>(properties));
                }
            }

            keysDesc.analyze(columnDefs);
            if (!CollectionUtils.isEmpty(keysDesc.getClusterKeysColumnNames())) {
                if (Config.isCloudMode()) {
                    throw new AnalysisException("Cluster key is not supported in cloud mode");
                }
                if (!enableUniqueKeyMergeOnWrite) {
                    throw new AnalysisException("Cluster keys only support unique keys table which enabled "
                            + PropertyAnalyzer.ENABLE_UNIQUE_KEY_MERGE_ON_WRITE);
                }
            }
            for (int i = 0; i < keysDesc.keysColumnSize(); ++i) {
                columnDefs.get(i).setIsKey(true);
            }
            if (keysDesc.getKeysType() != KeysType.AGG_KEYS) {
                AggregateType type = AggregateType.REPLACE;
                if (keysDesc.getKeysType() == KeysType.DUP_KEYS) {
                    type = AggregateType.NONE;
                }
                if (keysDesc.getKeysType() == KeysType.UNIQUE_KEYS && enableUniqueKeyMergeOnWrite) {
                    type = AggregateType.NONE;
                }
                for (int i = keysDesc.keysColumnSize(); i < columnDefs.size(); ++i) {
                    columnDefs.get(i).setAggregateType(type);
                }
            }
        } else {
            // mysql, broker and hive do not need key desc
            if (keysDesc != null) {
                throw new AnalysisException("Create " + engineName + " table should not contain keys desc");
            }

            for (ColumnDef columnDef : columnDefs) {
                columnDef.setIsKey(true);
            }
        }

        // analyze column def
        if (!(engineName.equalsIgnoreCase("elasticsearch")) && (columnDefs == null || columnDefs.isEmpty())) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLE_MUST_HAVE_COLUMNS);
        }
        // add a hidden column as delete flag for unique table
        if (keysDesc != null && keysDesc.getKeysType() == KeysType.UNIQUE_KEYS) {
            if (enableUniqueKeyMergeOnWrite) {
                columnDefs.add(ColumnDef.newDeleteSignColumnDef(AggregateType.NONE));
            } else {
                columnDefs.add(ColumnDef.newDeleteSignColumnDef(AggregateType.REPLACE));
            }
        }
        // add a hidden column as row store
        if (properties != null && PropertyAnalyzer.analyzeStoreRowColumn(new HashMap<>(properties))) {
            if (keysDesc != null && keysDesc.getKeysType() == KeysType.AGG_KEYS) {
                throw new AnalysisException("Aggregate table can't support row column now");
            }
            if (keysDesc != null && keysDesc.getKeysType() == KeysType.UNIQUE_KEYS) {
                if (enableUniqueKeyMergeOnWrite) {
                    columnDefs.add(ColumnDef.newRowStoreColumnDef(AggregateType.NONE));
                } else {
                    columnDefs.add(ColumnDef.newRowStoreColumnDef(AggregateType.REPLACE));
                }
            } else {
                columnDefs.add(ColumnDef.newRowStoreColumnDef(null));
            }
        }
        if (Config.enable_hidden_version_column_by_default && keysDesc != null
                && keysDesc.getKeysType() == KeysType.UNIQUE_KEYS) {
            if (enableUniqueKeyMergeOnWrite) {
                columnDefs.add(ColumnDef.newVersionColumnDef(AggregateType.NONE));
            } else {
                columnDefs.add(ColumnDef.newVersionColumnDef(AggregateType.REPLACE));
            }
        }

        Set<String> columnSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (ColumnDef columnDef : columnDefs) {
            columnDef.analyze(engineName.equalsIgnoreCase(DEFAULT_ENGINE_NAME));

            if (columnDef.getType().isComplexType() && engineName.equalsIgnoreCase(DEFAULT_ENGINE_NAME)) {
                if (columnDef.getAggregateType() != null && columnDef.getAggregateType() != AggregateType.NONE
                        && columnDef.getAggregateType() != AggregateType.REPLACE
                        && columnDef.getAggregateType() != AggregateType.REPLACE_IF_NOT_NULL) {
                    throw new AnalysisException(
                            columnDef.getType().getPrimitiveType() + " column can't support aggregation "
                                    + columnDef.getAggregateType());
                }
                if (columnDef.isKey()) {
                    throw new AnalysisException(columnDef.getType().getPrimitiveType()
                            + " can only be used in the non-key column of the duplicate table at present.");
                }
            }

            if (columnDef.getType().isTime() || columnDef.getType().isTimeV2()) {
                throw new AnalysisException("Time type is not supported for olap table");
            }

            if (!columnSet.add(columnDef.getName())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DUP_FIELDNAME, columnDef.getName());
            }
        }

        if (engineName.equalsIgnoreCase(DEFAULT_ENGINE_NAME)) {
            // before analyzing partition, handle the replication allocation info
            properties = PropertyAnalyzer.getInstance().rewriteOlapProperties(tableName.getCtl(),
                    tableName.getDb(), properties);
            // analyze partition
            if (partitionDesc != null) {
                if (partitionDesc instanceof ListPartitionDesc || partitionDesc instanceof RangePartitionDesc) {
                    partitionDesc.analyze(columnDefs, properties);
                } else {
                    throw new AnalysisException(
                            "Currently only support range" + " and list partition with engine type olap");
                }

            }

            // analyze distribution
            if (distributionDesc == null) {
                distributionDesc = new RandomDistributionDesc(FeConstants.default_bucket_num, true);
            }
            distributionDesc.analyze(columnSet, columnDefs, keysDesc);
            if (distributionDesc.type == DistributionInfo.DistributionInfoType.RANDOM) {
                if (keysDesc.getKeysType() == KeysType.UNIQUE_KEYS) {
                    throw new AnalysisException("Create unique keys table should not contain random distribution desc");
                } else if (keysDesc.getKeysType() == KeysType.AGG_KEYS) {
                    for (ColumnDef columnDef : columnDefs) {
                        if (columnDef.getAggregateType() == AggregateType.REPLACE
                                || columnDef.getAggregateType() == AggregateType.REPLACE_IF_NOT_NULL) {
                            throw new AnalysisException(
                                    "Create aggregate keys table with value columns of which" + " aggregate type is "
                                            + columnDef.getAggregateType() + " should not contain random"
                                            + " distribution desc");
                        }
                    }
                }
            }
        } else if (engineName.equalsIgnoreCase("elasticsearch")) {
            EsUtil.analyzePartitionAndDistributionDesc(partitionDesc, distributionDesc);
        } else {
            if (partitionDesc != null || distributionDesc != null) {
                throw new AnalysisException(
                        "Create " + engineName + " table should not contain partition or distribution desc");
            }
        }

        for (ColumnDef columnDef : columnDefs) {
            Column col = columnDef.toColumn();
            if (keysDesc != null && keysDesc.getKeysType() == KeysType.UNIQUE_KEYS) {
                if (!col.isKey()) {
                    col.setAggregationTypeImplicit(true);
                }
            }
            columns.add(col);
        }

        if (CollectionUtils.isNotEmpty(indexDefs)) {
            Set<String> distinct = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            Set<Pair<IndexType, List<String>>> distinctCol = new HashSet<>();

            for (IndexDef indexDef : indexDefs) {
                indexDef.analyze();
                if (!engineName.equalsIgnoreCase(DEFAULT_ENGINE_NAME)) {
                    throw new AnalysisException("index only support in olap engine at current version.");
                }
                for (String indexColName : indexDef.getColumns()) {
                    boolean found = false;
                    for (Column column : columns) {
                        if (column.getName().equalsIgnoreCase(indexColName)) {
                            indexDef.checkColumn(column, getKeysDesc().getKeysType(), enableUniqueKeyMergeOnWrite);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        throw new AnalysisException("Column does not exist in table. invalid column: " + indexColName);
                    }
                }
                indexes.add(new Index(Env.getCurrentEnv().getNextId(), indexDef.getIndexName(), indexDef.getColumns(),
                        indexDef.getIndexType(), indexDef.getProperties(), indexDef.getComment()));
                distinct.add(indexDef.getIndexName());
                distinctCol.add(Pair.of(indexDef.getIndexType(),
                        indexDef.getColumns().stream().map(String::toUpperCase).collect(Collectors.toList())));
            }
            if (distinct.size() != indexes.size()) {
                throw new AnalysisException("index name must be unique.");
            }
            if (distinctCol.size() != indexes.size()) {
                throw new AnalysisException("same index columns have multiple same type index is not allowed.");
            }
        }
        generatedColumnCheck(analyzer);
    }

    private void analyzeEngineName() throws AnalysisException {
        if (Strings.isNullOrEmpty(engineName)) {
            engineName = "olap";
        }
        engineName = engineName.toLowerCase();

        if (!engineNames.contains(engineName)) {
            throw new AnalysisException("Unknown engine name: " + engineName);
        }

        if (engineName.equals("mysql") || engineName.equals("odbc") || engineName.equals("broker")
                || engineName.equals("elasticsearch") || engineName.equals("hive")
                || engineName.equals("jdbc")) {
            if (!isExternal) {
                // this is for compatibility
                isExternal = true;
                LOG.warn("create " + engineName + " table without keyword external");
                // throw new AnalysisException("Only support external table with engine name = mysql or broker");
            }
        } else {
            if (isExternal) {
                throw new AnalysisException("Do not support external table with engine name = olap");
            }
        }

        if (!Config.enable_odbc_mysql_broker_table && (engineName.equals("odbc")
                || engineName.equals("mysql") || engineName.equals("broker"))) {
            throw new AnalysisException(
                    "odbc, mysql and broker table is no longer supported."
                            + " For odbc and mysql external table, use jdbc table or jdbc catalog instead."
                            + " For broker table, use table valued function instead."
                            + ". Or you can temporarily set 'disable_odbc_mysql_broker_table=false'"
                            + " in fe.conf to reopen this feature.");
        }
    }

    public static CreateTableStmt read(DataInput in) throws IOException {
        throw new RuntimeException("CreateTableStmt serialization is not supported anymore.");
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE ");
        if (isExternal) {
            sb.append("EXTERNAL ");
        }
        sb.append("TABLE ");
        if (ifNotExists) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(tableName.toSql()).append(" (\n");
        int idx = 0;
        for (ColumnDef columnDef : columnDefs) {
            if (idx != 0) {
                sb.append(",\n");
            }
            sb.append("  ").append(columnDef.toSql());
            idx++;
        }
        if (CollectionUtils.isNotEmpty(indexDefs)) {
            sb.append(",\n");
            for (IndexDef indexDef : indexDefs) {
                sb.append("  ").append(indexDef.toSql());
            }
        }
        sb.append("\n)");
        sb.append(" ENGINE = ").append(engineName.toLowerCase());

        if (keysDesc != null) {
            sb.append("\n").append(keysDesc.toSql());
        }

        if (!Strings.isNullOrEmpty(comment)) {
            sb.append("\nCOMMENT \"").append(comment).append("\"");
        }

        if (partitionDesc != null) {
            sb.append("\n").append(partitionDesc.toSql());
        }

        if (distributionDesc != null) {
            sb.append("\n").append(distributionDesc.toSql());
        }

        if (rollupAlterClauseList != null && !rollupAlterClauseList.isEmpty()) {
            sb.append("\n rollup(");
            StringBuilder opsSb = new StringBuilder();
            for (int i = 0; i < rollupAlterClauseList.size(); i++) {
                opsSb.append(rollupAlterClauseList.get(i).toSql());
                if (i != rollupAlterClauseList.size() - 1) {
                    opsSb.append(",");
                }
            }
            sb.append(opsSb.toString().replace("ADD ROLLUP", "")).append(")");
        }

        // properties may contains password and other sensitive information,
        // so do not print properties.
        // This toSql() method is only used for log, user can see detail info by using show create table stmt,
        // which is implemented in Catalog.getDdlStmt()
        if (properties != null && !properties.isEmpty()) {
            sb.append("\nPROPERTIES (");
            sb.append(new PrintableMap<>(properties, " = ", true, true, true));
            sb.append(")");
        }

        if (extProperties != null && !extProperties.isEmpty()) {
            sb.append("\n").append(engineName.toUpperCase()).append(" PROPERTIES (");
            sb.append(new PrintableMap<>(extProperties, " = ", true, true, true));
            sb.append(")");
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public boolean needAuditEncryption() {
        return !engineName.equals("olap");
    }

    private void generatedColumnCheck(Analyzer analyzer) throws AnalysisException {
        generatedColumnCommonCheck();
        Map<String, Pair<ColumnDef, Integer>> nameToColumnDef = Maps.newHashMap();
        for (int i = 0; i < columnDefs.size(); i++) {
            ColumnDef columnDef = columnDefs.get(i);
            nameToColumnDef.put(columnDef.getName(), Pair.of(columnDef, i));
        }
        List<GeneratedColumnUtil.ExprAndname> exprAndnames = Lists.newArrayList();
        for (int i = 0; i < columnDefs.size(); i++) {
            ColumnDef columnDef = columnDefs.get(i);
            if (!columnDef.getGeneratedColumnInfo().isPresent()) {
                continue;
            }
            SlotRefRewriteRule slotRefRewriteRule = new SlotRefRewriteRule(i, nameToColumnDef);
            ExprRewriter rewriter = new ExprRewriter(slotRefRewriteRule);
            GeneratedColumnInfo generatedColumnInfo = columnDef.getGeneratedColumnInfo().get();
            Expr expr = rewriter.rewrite(generatedColumnInfo.getExpr(), analyzer);
            expr.foreach(e -> {
                if (e instanceof LambdaFunctionCallExpr) {
                    throw new AnalysisException("Generated column does not support lambda.");
                } else if (e instanceof Subquery) {
                    throw new AnalysisException("Generated column does not support subquery.");
                }
            });
            try {
                expr.analyze(analyzer);
            } catch (AnalysisException e) {
                throw new AnalysisException("In generated column '" + columnDef.getName() + "', "
                        + Utils.convertFirstChar(e.getDetailMessage()));
            }
            expr.foreach(e -> {
                if (e instanceof VariableExpr) {
                    throw new AnalysisException("Generated column expression cannot contain variable.");
                } else if (e instanceof SlotRef && nameToColumnDef.containsKey(((SlotRef) e).getColumnName())) {
                    ColumnDef refColumnDef = nameToColumnDef.get(((SlotRef) e).getColumnName()).first;
                    if (refColumnDef.getAutoIncInitValue() != -1) {
                        throw new AnalysisException("Generated column '" + columnDef.getName()
                                + "' cannot refer to auto-increment column.");
                    }
                } else if (e instanceof FunctionCallExpr && !checkFunctionInGeneratedColumn((FunctionCallExpr) e)) {
                    throw new AnalysisException(
                            "Expression of generated column '"
                                    + columnDef.getName() + "' contains a disallowed function:'"
                                    + ((FunctionCallExpr) e).getFnName() + "'");
                } else if (e instanceof AnalyticExpr) {
                    throw new AnalysisException(
                            "Expression of generated column '"
                                    + columnDef.getName() + "' contains a disallowed expression:'"
                                    + ((AnalyticExpr) e).toSqlWithoutTbl() + "'");
                }
            });
            if (!Type.canCastTo(expr.getType(), columnDef.getType())) {
                throw new AnalysisException("can not cast from origin type "
                        + expr.getType() + " to target type=" + columnDef.getType());
            }
            exprAndnames.add(new GeneratedColumnUtil.ExprAndname(expr, columnDef.getName()));
        }

        // for alter drop column
        Map<String, List<String>> columnToListOfGeneratedColumnsThatReferToThis = new HashMap<>();
        for (ColumnDef column : columnDefs) {
            Optional<GeneratedColumnInfo> info = column.getGeneratedColumnInfo();
            if (!info.isPresent()) {
                continue;
            }
            Expr generatedExpr = info.get().getExpr();
            Set<Expr> slotRefsInGeneratedExpr = new HashSet<>();
            generatedExpr.collect(e -> e instanceof SlotRef, slotRefsInGeneratedExpr);
            for (Expr slotRefExpr : slotRefsInGeneratedExpr) {
                String name = ((SlotRef) slotRefExpr).getColumnName();
                columnToListOfGeneratedColumnsThatReferToThis
                        .computeIfAbsent(name, k -> new ArrayList<>())
                        .add(column.getName());
            }
        }
        for (Map.Entry<String, List<String>> entry : columnToListOfGeneratedColumnsThatReferToThis.entrySet()) {
            ColumnDef col = nameToColumnDef.get(entry.getKey()).first;
            col.addGeneratedColumnsThatReferToThis(entry.getValue());
        }

        GeneratedColumnUtil.rewriteColumns(exprAndnames);
        for (GeneratedColumnUtil.ExprAndname exprAndname : exprAndnames) {
            if (nameToColumnDef.containsKey(exprAndname.getName())) {
                ColumnDef columnDef = nameToColumnDef.get(exprAndname.getName()).first;
                Optional<GeneratedColumnInfo> info = columnDef.getGeneratedColumnInfo();
                info.ifPresent(columnInfo -> columnInfo.setExpandExprForLoad(exprAndname.getExpr()));
            }
        }
    }

    private boolean checkFunctionInGeneratedColumn(FunctionCallExpr funcExpr) {
        if (!funcExpr.isScalarFunction()) {
            return false;
        }
        if (funcExpr.fn.isUdf()) {
            return false;
        }
        if (funcExpr instanceof GroupingFunctionCallExpr) {
            return false;
        }
        return true;
    }

    public static final class SlotRefRewriteRule implements ExprRewriteRule {
        private final Map<String, Pair<ColumnDef, Integer>> nameToColumnDefMap;
        private final int index;

        public SlotRefRewriteRule(int index, Map<String, Pair<ColumnDef, Integer>> nameToColumnDefMap) {
            this.index = index;
            this.nameToColumnDefMap = nameToColumnDefMap;
        }

        @Override
        public Expr apply(Expr expr, Analyzer analyzer, ExprRewriter.ClauseType clauseType) throws AnalysisException {
            if (!(expr instanceof SlotRef)) {
                return expr;
            }
            SlotRef slotRef = (SlotRef) expr;
            if (!nameToColumnDefMap.containsKey(slotRef.getColumnName())) {
                throw new AnalysisException("Unknown column '" + slotRef.getColumnName()
                        + "' in 'generated column function'");
            }
            Pair<ColumnDef, Integer> columnAndIdx = nameToColumnDefMap.get(slotRef.getColumnName());
            ColumnDef columnDef = columnAndIdx.first;
            if (columnDef.getGeneratedColumnInfo().isPresent() && columnAndIdx.second >= index) {
                throw new AnalysisException(
                        "Generated column can refer only to generated columns defined prior to it.");
            }
            slotRef.setType(columnDef.getType());
            slotRef.setAnalyzed(true);
            return slotRef;
        }
    }

    private void generatedColumnCommonCheck() throws AnalysisException {
        for (ColumnDef column : columnDefs) {
            if (keysDesc != null && keysDesc.getKeysType() == KeysType.AGG_KEYS
                    && column.getGeneratedColumnInfo().isPresent()
                    && (!column.isKey() && !GENERATED_COLUMN_ALLOW_AGG_TYPE.contains(column.getAggregateType()))) {
                throw new AnalysisException("The generated columns can be key columns, "
                        + "or value columns of replace and replace_if_not_null aggregation type.");
            }
            if (column.getGeneratedColumnInfo().isPresent() && !engineName.equalsIgnoreCase("olap")) {
                throw new AnalysisException(
                        "Tables can only have generated columns if the olap engine is used");
            }
        }
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }
}
