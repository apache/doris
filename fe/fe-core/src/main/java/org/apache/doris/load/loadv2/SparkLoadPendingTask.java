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

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.DistributionInfo.DistributionInfoType;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.HiveTable;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.SparkResource;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.MetaLockUtils;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.Load;
import org.apache.doris.sparkdpp.EtlJobConfig;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlColumn;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlColumnMapping;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlFileGroup;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlIndex;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlJobProperty;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlPartition;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlPartitionInfo;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlTable;
import org.apache.doris.sparkdpp.EtlJobConfig.FilePatternVersion;
import org.apache.doris.sparkdpp.EtlJobConfig.SourceType;
import org.apache.doris.transaction.TransactionState;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

// 1. create etl job config and write it into jobconfig.json file
// 2. submit spark etl job
public class SparkLoadPendingTask extends LoadTask {
    private static final Logger LOG = LogManager.getLogger(SparkLoadPendingTask.class);

    private final Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToBrokerFileGroups;
    private final SparkResource resource;
    private final BrokerDesc brokerDesc;
    private final long dbId;
    private final String loadLabel;
    private final long loadJobId;
    private final long transactionId;
    private EtlJobConfig etlJobConfig;
    private SparkLoadAppHandle sparkLoadAppHandle;

    public SparkLoadPendingTask(SparkLoadJob loadTaskCallback,
                                Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToBrokerFileGroups,
                                SparkResource resource, BrokerDesc brokerDesc, Priority priority) {
        super(loadTaskCallback, TaskType.PENDING, priority);
        this.retryTime = 3;
        this.attachment = new SparkPendingTaskAttachment(signature);
        this.aggKeyToBrokerFileGroups = aggKeyToBrokerFileGroups;
        this.resource = resource;
        this.brokerDesc = brokerDesc;
        this.dbId = loadTaskCallback.getDbId();
        this.loadJobId = loadTaskCallback.getId();
        this.loadLabel = loadTaskCallback.getLabel();
        this.transactionId = loadTaskCallback.getTransactionId();
        this.sparkLoadAppHandle = loadTaskCallback.getHandle();
        this.failMsg = new FailMsg(FailMsg.CancelType.ETL_SUBMIT_FAIL);
        toLowCaseForFileGroups();
    }

    void toLowCaseForFileGroups() {
        aggKeyToBrokerFileGroups.values().forEach(fgs -> {
            fgs.forEach(fg -> {
                fg.getColumnExprList()
                        .forEach(expr -> expr.setColumnName(expr.getColumnName().toLowerCase(Locale.ROOT)));
            });
        });
    }

    @Override
    void executeTask() throws UserException {
        LOG.info("begin to execute spark pending task. load job id: {}", loadJobId);
        submitEtlJob();
    }

    private void submitEtlJob() throws LoadException {
        SparkPendingTaskAttachment sparkAttachment = (SparkPendingTaskAttachment) attachment;
        // retry different output path
        etlJobConfig.outputPath = EtlJobConfig.getOutputPath(resource.getWorkingDir(), dbId, loadLabel, signature);
        sparkAttachment.setOutputPath(etlJobConfig.outputPath);

        // handler submit etl job
        SparkEtlJobHandler handler = new SparkEtlJobHandler();
        handler.submitEtlJob(loadJobId, loadLabel, etlJobConfig, resource,
                brokerDesc, sparkLoadAppHandle, sparkAttachment);
        LOG.info("submit spark etl job success. load job id: {}, attachment: {}", loadJobId, sparkAttachment);
    }

    @Override
    public void init() throws LoadException {
        createEtlJobConf();
    }

    private void createEtlJobConf() throws LoadException {
        Database db = Env.getCurrentInternalCatalog()
                .getDbOrException(dbId, s -> new LoadException("db does not exist. id: " + s));

        Map<Long, EtlTable> tables = Maps.newHashMap();
        Map<Long, Set<Long>> tableIdToPartitionIds = Maps.newHashMap();
        Set<Long> allPartitionsTableIds = Sets.newHashSet();
        prepareTablePartitionInfos(db, tableIdToPartitionIds, allPartitionsTableIds);
        List<Table> tableList;
        try {
            tableList = db.getTablesOnIdOrderOrThrowException(Lists.newArrayList(allPartitionsTableIds));
        } catch (MetaNotFoundException e) {
            throw new LoadException(e.getMessage());
        }

        MetaLockUtils.readLockTables(tableList);
        try {
            for (Map.Entry<FileGroupAggKey, List<BrokerFileGroup>> entry : aggKeyToBrokerFileGroups.entrySet()) {
                FileGroupAggKey aggKey = entry.getKey();
                long tableId = aggKey.getTableId();

                OlapTable table = (OlapTable) db.getTableOrException(
                        tableId, s -> new LoadException("table does not exist. id: " + s));

                EtlTable etlTable = null;
                if (tables.containsKey(tableId)) {
                    etlTable = tables.get(tableId);
                } else {
                    // indexes
                    List<EtlIndex> etlIndexes = createEtlIndexes(table);
                    // partition info
                    EtlPartitionInfo etlPartitionInfo = createEtlPartitionInfo(table,
                            tableIdToPartitionIds.get(tableId));
                    etlTable = new EtlTable(etlIndexes, etlPartitionInfo);
                    tables.put(tableId, etlTable);

                    // add table indexes to transaction state
                    TransactionState txnState = Env.getCurrentGlobalTransactionMgr()
                            .getTransactionState(dbId, transactionId);
                    if (txnState == null) {
                        throw new LoadException("txn does not exist. id: " + transactionId);
                    }
                    txnState.addTableIndexes(table);
                }

                // file group
                for (BrokerFileGroup fileGroup : entry.getValue()) {
                    etlTable.addFileGroup(createEtlFileGroup(fileGroup, tableIdToPartitionIds.get(tableId), db, table));
                }
            }
        } finally {
            MetaLockUtils.readUnlockTables(tableList);
        }

        String outputFilePattern = EtlJobConfig.getOutputFilePattern(loadLabel, FilePatternVersion.V1);
        // strictMode timezone properties
        EtlJobProperty properties = new EtlJobProperty();
        properties.strictMode = ((LoadJob) callback).isStrictMode();
        properties.timezone = ((LoadJob) callback).getTimeZone();
        etlJobConfig = new EtlJobConfig(tables, outputFilePattern, loadLabel, properties);
    }

    private void prepareTablePartitionInfos(Database db, Map<Long, Set<Long>> tableIdToPartitionIds,
                                            Set<Long> allPartitionsTableIds) throws LoadException {
        for (FileGroupAggKey aggKey : aggKeyToBrokerFileGroups.keySet()) {
            long tableId = aggKey.getTableId();
            if (allPartitionsTableIds.contains(tableId)) {
                continue;
            }

            OlapTable table = (OlapTable) db.getTableOrException(
                    tableId, s -> new LoadException("table does not exist. id: " + s));
            table.readLock();
            try {
                Set<Long> partitionIds;
                if (tableIdToPartitionIds.containsKey(tableId)) {
                    partitionIds = tableIdToPartitionIds.get(tableId);
                } else {
                    partitionIds = Sets.newHashSet();
                    tableIdToPartitionIds.put(tableId, partitionIds);
                }

                Set<Long> groupPartitionIds = aggKey.getPartitionIds();
                // if not assign partition, use all partitions
                if (groupPartitionIds == null || groupPartitionIds.isEmpty()) {
                    for (Partition partition : table.getPartitions()) {
                        partitionIds.add(partition.getId());
                    }

                    allPartitionsTableIds.add(tableId);
                } else {
                    partitionIds.addAll(groupPartitionIds);
                }
            } finally {
                table.readUnlock();
            }
        }
    }

    private List<EtlIndex> createEtlIndexes(OlapTable table) throws LoadException {
        List<EtlIndex> etlIndexes = Lists.newArrayList();

        for (Map.Entry<Long, List<Column>> entry : table.getIndexIdToSchema().entrySet()) {
            long indexId = entry.getKey();
            int schemaHash = table.getSchemaHashByIndexId(indexId);

            boolean changeAggType = table.getKeysTypeByIndexId(indexId).equals(KeysType.UNIQUE_KEYS)
                    && table.getTableProperty().getEnableUniqueKeyMergeOnWrite();

            // columns
            List<EtlColumn> etlColumns = Lists.newArrayList();
            for (Column column : entry.getValue()) {
                etlColumns.add(createEtlColumn(column, changeAggType));
            }

            // check distribution type
            DistributionInfo distributionInfo = table.getDefaultDistributionInfo();
            if (distributionInfo.getType() != DistributionInfoType.HASH) {
                // RANDOM not supported
                String errMsg = "Unsupported distribution type. type: " + distributionInfo.getType().name();
                LOG.warn(errMsg);
                throw new LoadException(errMsg);
            }

            // index type
            String indexType = null;
            KeysType keysType = table.getKeysTypeByIndexId(indexId);
            switch (keysType) {
                case DUP_KEYS:
                    indexType = "DUPLICATE";
                    break;
                case AGG_KEYS:
                    indexType = "AGGREGATE";
                    break;
                case UNIQUE_KEYS:
                    indexType = "UNIQUE";
                    break;
                default:
                    String errMsg = "unknown keys type. type: " + keysType.name();
                    LOG.warn(errMsg);
                    throw new LoadException(errMsg);
            }

            // is base index
            boolean isBaseIndex = indexId == table.getBaseIndexId() ? true : false;

            etlIndexes.add(new EtlIndex(indexId, etlColumns, schemaHash, indexType, isBaseIndex));
        }

        return etlIndexes;
    }

    private EtlColumn createEtlColumn(Column column, boolean changeAggType) {
        // column name
        String name = column.getName().toLowerCase(Locale.ROOT);
        // column type
        PrimitiveType type = column.getDataType();
        String columnType = column.getDataType().toString();
        // is allow null
        boolean isAllowNull = column.isAllowNull();
        // is key
        boolean isKey = column.isKey();

        // aggregation type
        String aggregationType = null;
        if (column.getAggregationType() != null) {
            if (changeAggType && !column.isKey()) {
                aggregationType = AggregateType.REPLACE.toSql();
            } else {
                aggregationType = column.getAggregationType().toString();
            }
        }

        // default value
        String defaultValue = null;
        if (column.getDefaultValue() != null) {
            defaultValue = column.getDefaultValue();
        }
        if (column.isAllowNull() && column.getDefaultValue() == null) {
            defaultValue = "\\N";
        }

        // string length
        int stringLength = 0;
        if (type.isStringType()) {
            stringLength = column.getStrLen();
        }

        // decimal precision scale
        int precision = 0;
        int scale = 0;
        if (type.isDecimalV2Type() || type.isDecimalV3Type()) {
            precision = column.getPrecision();
            scale = column.getScale();
        }

        return new EtlColumn(name, columnType, isAllowNull, isKey, aggregationType, defaultValue,
                stringLength, precision, scale);
    }

    private EtlPartitionInfo createEtlPartitionInfo(OlapTable table, Set<Long> partitionIds) throws LoadException {
        PartitionType type = table.getPartitionInfo().getType();

        List<String> partitionColumnRefs = Lists.newArrayList();
        List<EtlPartition> etlPartitions = Lists.newArrayList();
        if (type == PartitionType.RANGE) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) table.getPartitionInfo();
            for (Column column : rangePartitionInfo.getPartitionColumns()) {
                partitionColumnRefs.add(column.getName());
            }

            for (Map.Entry<Long, PartitionItem> entry : rangePartitionInfo.getAllPartitionItemEntryList(true)) {
                long partitionId = entry.getKey();
                if (!partitionIds.contains(partitionId)) {
                    continue;
                }

                Partition partition = table.getPartition(partitionId);
                if (partition == null) {
                    throw new LoadException("partition does not exist. id: " + partitionId);
                }

                // bucket num
                int bucketNum = partition.getDistributionInfo().getBucketNum();

                // is max partition
                Range<PartitionKey> range = entry.getValue().getItems();
                boolean isMaxPartition = range.upperEndpoint().isMaxValue();

                // start keys
                List<LiteralExpr> rangeKeyExprs = range.lowerEndpoint().getKeys();
                List<Object> startKeys = Lists.newArrayList();
                for (int i = 0; i < rangeKeyExprs.size(); ++i) {
                    LiteralExpr literalExpr = rangeKeyExprs.get(i);
                    Object keyValue = literalExpr.getRealValue();
                    startKeys.add(keyValue);
                }

                // end keys
                // is empty list when max partition
                List<Object> endKeys = Lists.newArrayList();
                if (!isMaxPartition) {
                    rangeKeyExprs = range.upperEndpoint().getKeys();
                    for (int i = 0; i < rangeKeyExprs.size(); ++i) {
                        LiteralExpr literalExpr = rangeKeyExprs.get(i);
                        Object keyValue = literalExpr.getRealValue();
                        endKeys.add(keyValue);
                    }
                }

                etlPartitions.add(new EtlPartition(partitionId, startKeys, endKeys, isMaxPartition, bucketNum));
            }
        } else if (type == PartitionType.UNPARTITIONED) {
            Preconditions.checkState(partitionIds.size() == 1);

            for (Long partitionId : partitionIds) {
                Partition partition = table.getPartition(partitionId);
                if (partition == null) {
                    throw new LoadException("partition does not exist. id: " + partitionId);
                }

                // bucket num
                int bucketNum = partition.getDistributionInfo().getBucketNum();

                etlPartitions.add(new EtlPartition(partitionId, Lists.newArrayList(), Lists.newArrayList(),
                        true, bucketNum));
            }
        } else {
            throw new LoadException("Spark Load does not support list partition yet");
        }

        // distribution column refs
        List<String> distributionColumnRefs = Lists.newArrayList();
        DistributionInfo distributionInfo = table.getDefaultDistributionInfo();
        Preconditions.checkState(distributionInfo.getType() == DistributionInfoType.HASH);
        for (Column column : ((HashDistributionInfo) distributionInfo).getDistributionColumns()) {
            distributionColumnRefs.add(column.getName());
        }

        return new EtlPartitionInfo(type.typeString, partitionColumnRefs, distributionColumnRefs, etlPartitions);
    }

    private EtlFileGroup createEtlFileGroup(BrokerFileGroup fileGroup, Set<Long> tablePartitionIds,
                                            Database db, OlapTable table) throws LoadException {
        List<ImportColumnDesc> copiedColumnExprList = Lists.newArrayList(fileGroup.getColumnExprList());
        Map<String, Expr> exprByName = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (ImportColumnDesc columnDesc : copiedColumnExprList) {
            if (!columnDesc.isColumn()) {
                exprByName.put(columnDesc.getColumnName(), columnDesc.getExpr());
            }
        }

        // check columns
        try {
            Load.initColumns(table, copiedColumnExprList, fileGroup.getColumnToHadoopFunction());
        } catch (UserException e) {
            throw new LoadException(e.getMessage());
        }
        // add shadow column mapping when schema change
        for (ImportColumnDesc columnDesc : Load.getSchemaChangeShadowColumnDesc(table, exprByName)) {
            copiedColumnExprList.add(columnDesc);
            exprByName.put(columnDesc.getColumnName(), columnDesc.getExpr());
        }

        // check negative for sum aggregate type
        if (fileGroup.isNegative()) {
            for (Column column : table.getBaseSchema()) {
                if (!column.isKey() && column.getAggregationType() != AggregateType.SUM) {
                    throw new LoadException("Column is not SUM AggreateType. column:" + column.getName());
                }
            }
        }

        // fill file field names if empty
        List<String> fileFieldNames = fileGroup.getFileFieldNames();
        if (fileFieldNames == null || fileFieldNames.isEmpty()) {
            fileFieldNames = Lists.newArrayList();
            for (Column column : table.getBaseSchema()) {
                fileFieldNames.add(column.getName());
            }
        }

        // column mappings
        Map<String, Pair<String, List<String>>> columnToHadoopFunction = fileGroup.getColumnToHadoopFunction();
        Map<String, EtlColumnMapping> columnMappings = Maps.newHashMap();
        if (columnToHadoopFunction != null) {
            for (Map.Entry<String, Pair<String, List<String>>> entry : columnToHadoopFunction.entrySet()) {
                columnMappings.put(entry.getKey(),
                        new EtlColumnMapping(entry.getValue().first, entry.getValue().second));
            }
        }
        for (ImportColumnDesc columnDesc : copiedColumnExprList) {
            if (columnDesc.isColumn() || columnMappings.containsKey(columnDesc.getColumnName())) {
                continue;
            }
            // the left must be column expr
            columnMappings.put(columnDesc.getColumnName(), new EtlColumnMapping(columnDesc.getExpr().toSql()));
        }

        // partition ids
        List<Long> partitionIds = fileGroup.getPartitionIds();
        if (partitionIds == null || partitionIds.isEmpty()) {
            partitionIds = Lists.newArrayList(tablePartitionIds);
        }

        // where
        // TODO: check
        String where = "";
        if (fileGroup.getWhereExpr() != null) {
            where = fileGroup.getWhereExpr().toSql();
        }

        // load from table
        String hiveDbTableName = "";
        Map<String, String> hiveTableProperties = Maps.newHashMap();
        if (fileGroup.isLoadFromTable()) {
            long srcTableId = fileGroup.getSrcTableId();
            HiveTable srcHiveTable = (HiveTable) db.getTableOrException(
                    srcTableId, s -> new LoadException("table does not exist. id: " + s));
            hiveDbTableName = srcHiveTable.getHiveDbTable();
            hiveTableProperties.putAll(srcHiveTable.getHiveProperties());
        }

        // check hll and bitmap func
        // TODO: more check
        for (Column column : table.getBaseSchema()) {
            String columnName = column.getName();
            PrimitiveType columnType = column.getDataType();
            Expr expr = exprByName.get(columnName);
            if (columnType == PrimitiveType.HLL) {
                checkHllMapping(columnName, expr);
            }
            if (columnType == PrimitiveType.BITMAP) {
                checkBitmapMapping(columnName, expr, fileGroup.isLoadFromTable());
            }
        }

        EtlFileGroup etlFileGroup = null;
        if (fileGroup.isLoadFromTable()) {
            etlFileGroup = new EtlFileGroup(SourceType.HIVE, hiveDbTableName, hiveTableProperties,
                    fileGroup.isNegative(), columnMappings, where, partitionIds);
        } else {
            etlFileGroup = new EtlFileGroup(SourceType.FILE, fileGroup.getFilePaths(), fileFieldNames,
                    fileGroup.getColumnNamesFromPath(), fileGroup.getColumnSeparator(), fileGroup.getLineDelimiter(),
                    fileGroup.isNegative(), fileGroup.getFileFormat(), columnMappings, where, partitionIds);
        }

        return etlFileGroup;
    }

    private void checkHllMapping(String columnName, Expr expr) throws LoadException {
        if (expr == null) {
            throw new LoadException("HLL column func is not assigned. column:" + columnName);
        }

        String msg = "HLL column must use hll function, like " + columnName + "=hll_hash(xxx) or "
                + columnName + "=hll_empty()";
        if (!(expr instanceof FunctionCallExpr)) {
            throw new LoadException(msg);
        }
        FunctionCallExpr fn = (FunctionCallExpr) expr;
        String functionName = fn.getFnName().getFunction();
        if (!functionName.equalsIgnoreCase("hll_hash")
                && !functionName.equalsIgnoreCase("hll_empty")) {
            throw new LoadException(msg);
        }
    }

    private void checkBitmapMapping(String columnName, Expr expr, boolean isLoadFromTable) throws LoadException {
        if (expr == null) {
            throw new LoadException("BITMAP column func is not assigned. column:" + columnName);
        }

        String msg = "BITMAP column must use bitmap function, like " + columnName + "=to_bitmap(xxx) or "
                + columnName + "=bitmap_hash() or " + columnName + "=bitmap_dict()";
        if (!(expr instanceof FunctionCallExpr)) {
            throw new LoadException(msg);
        }
        FunctionCallExpr fn = (FunctionCallExpr) expr;
        String functionName = fn.getFnName().getFunction();
        if (!functionName.equalsIgnoreCase("to_bitmap")
                && !functionName.equalsIgnoreCase("bitmap_hash")
                && !functionName.equalsIgnoreCase("bitmap_dict")
                && !functionName.equalsIgnoreCase("binary_bitmap")) {
            throw new LoadException(msg);
        }

        if (functionName.equalsIgnoreCase("bitmap_dict") && !isLoadFromTable) {
            throw new LoadException("Bitmap global dict should load data from hive table");
        }
    }
}
