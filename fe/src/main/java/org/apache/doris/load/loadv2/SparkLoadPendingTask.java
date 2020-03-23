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

import org.apache.doris.analysis.EtlClusterDesc;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.DistributionInfo.DistributionInfoType;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.Pair;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.loadv2.etl.EtlJobConfig;
import org.apache.doris.load.loadv2.etl.EtlJobConfig.EtlColumn;
import org.apache.doris.load.loadv2.etl.EtlJobConfig.EtlColumnMapping;
import org.apache.doris.load.loadv2.etl.EtlJobConfig.EtlFileGroup;
import org.apache.doris.load.loadv2.etl.EtlJobConfig.EtlIndex;
import org.apache.doris.load.loadv2.etl.EtlJobConfig.EtlPartition;
import org.apache.doris.load.loadv2.etl.EtlJobConfig.EtlPartitionInfo;
import org.apache.doris.load.loadv2.etl.EtlJobConfig.EtlTable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.launcher.SparkAppHandle;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.List;
import java.util.Map;
import java.util.Set;

// 1. create etl job config and write it into jobconfig.json file
// 2. submit spark etl job
public class SparkLoadPendingTask extends LoadTask {
    private static final Logger LOG = LogManager.getLogger(SparkLoadPendingTask.class);

    private final Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToBrokerFileGroups;
    private final EtlClusterDesc etlClusterDesc;
    private final long dbId;
    private final String loadLabel;
    private final long loadJobId;
    private EtlJobConfig etlJobConfig;

    public SparkLoadPendingTask(SparkLoadJob loadTaskCallback,
                                Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToBrokerFileGroups,
                                EtlClusterDesc etlClusterDesc) {
        super(loadTaskCallback);
        this.retryTime = 3;
        this.attachment = new SparkPendingTaskAttachment(signature);
        this.aggKeyToBrokerFileGroups = aggKeyToBrokerFileGroups;
        this.etlClusterDesc = etlClusterDesc;
        this.dbId = loadTaskCallback.getDbId();
        this.loadJobId = loadTaskCallback.getId();
        this.loadLabel = loadTaskCallback.getLabel();
        this.failMsg = new FailMsg(FailMsg.CancelType.ETL_SUBMIT_FAIL);
    }

    @Override
    void executeTask() throws LoadException {
        LOG.info("begin to execute spark pending task. job: {}", callback.getCallbackId());
        submitEtlJob();
    }

    private void submitEtlJob() throws LoadException {
        // retry different output path
        String outputPath = etlClusterDesc.getProperties().get("output_path");
        String hdfsDefaultName = etlClusterDesc.getProperties().get("fs.default.name");
        etlJobConfig.setOutputPath(EtlJobConfig.getOutputPath(hdfsDefaultName, outputPath, dbId,
                                                              loadLabel, signature));

        // spark configs
        String sparkMaster = etlClusterDesc.getProperties().get("spark.master");
        Map<String, String> sparkConfigs = Maps.newHashMap();

        // handler submit etl job
        SparkEtlJobHandler handler = new SparkEtlJobHandler();
        SparkAppHandle handle = handler.submitEtlJob(loadJobId, loadLabel, sparkMaster, sparkConfigs, configToJson());
        ((SparkPendingTaskAttachment) attachment).setHandle(handle);
        ((SparkPendingTaskAttachment) attachment).setOutputPath(etlJobConfig.getOutputPath());
        LOG.info("submit spark etl job success. attachment: {}", attachment);
    }

    private String configToJson() {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES);
        Gson gson = gsonBuilder.create();
        return gson.toJson(etlJobConfig);
    }

    @Override
    public void init() throws LoadException {
        prepareEtlClusterInfos();
        createEtlJobConf();
    }

    private void prepareEtlClusterInfos() {
        // etlClusterDesc properties merge with cluster infos in user property
    }

    private void createEtlJobConf() throws LoadException {
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            throw new LoadException("db does not exist. id: " + dbId);
        }

        Map<Long, EtlTable> tables = Maps.newHashMap();
        db.readLock();
        try {
            Map<Long, Set<Long>> tableIdToPartitionIds = Maps.newHashMap();
            Set<Long> allPartitionsTableIds = Sets.newHashSet();
            prepareTablePartitionInfos(db, tableIdToPartitionIds, allPartitionsTableIds);


            for (Map.Entry<FileGroupAggKey, List<BrokerFileGroup>> entry : aggKeyToBrokerFileGroups.entrySet()) {
                FileGroupAggKey aggKey = entry.getKey();
                long tableId = aggKey.getTableId();

                OlapTable table = (OlapTable) db.getTable(tableId);
                if (table == null) {
                    throw new LoadException("table does not exist. id: " + tableId);
                }

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
                }

                // file group
                for (BrokerFileGroup fileGroup : entry.getValue()) {
                    etlTable.addFileGroup(createEtlFileGroup(fileGroup, tableIdToPartitionIds.get(tableId)));
                }
            }

            String outputFilePattern = loadLabel + "." + EtlJobConfig.ETL_OUTPUT_FILE_NAME_NO_LABEL_FORMAT;
            etlJobConfig = new EtlJobConfig(tables, outputFilePattern, loadLabel);
        } finally {
            db.readUnlock();
        }
    }

    private void prepareTablePartitionInfos(Database db, Map<Long, Set<Long>> tableIdToPartitionIds,
                                            Set<Long> allPartitionsTableIds) throws LoadException {
        for (FileGroupAggKey aggKey : aggKeyToBrokerFileGroups.keySet()) {
            long tableId = aggKey.getTableId();
            if (allPartitionsTableIds.contains(tableId)) {
                continue;
            }

            OlapTable table = (OlapTable) db.getTable(tableId);
            if (table == null) {
                throw new LoadException("table does not exist. id: " + tableId);
            }

            Set<Long> partitionIds = null;
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
        }
    }

    private List<EtlIndex> createEtlIndexes(OlapTable table) throws LoadException {
        List<EtlIndex> etlIndexes = Lists.newArrayList();

        for (Map.Entry<Long, List<Column>> entry : table.getIndexIdToSchema().entrySet()) {
            long indexId = entry.getKey();
            int schemaHash = table.getSchemaHashByIndexId(indexId);

            // columns
            List<EtlColumn> etlColumns = Lists.newArrayList();
            for (Column column : entry.getValue()) {
                etlColumns.add(createEtlColumn(column));
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

    private EtlColumn createEtlColumn(Column column) {
        // column name
        String name = column.getName();
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
            aggregationType = column.getAggregationType().toString();
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
        if (type.isDecimalType() || type.isDecimalV2Type()) {
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

            for (Map.Entry<Long, Range<PartitionKey>> entry : rangePartitionInfo.getSortedRangeMap()) {
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
                Range<PartitionKey> range = entry.getValue();
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
        } else {
            Preconditions.checkState(type == PartitionType.UNPARTITIONED);
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

    private EtlFileGroup createEtlFileGroup(BrokerFileGroup fileGroup, Set<Long> tablePartitionIds) {
        // column mappings
        Map<String, Pair<String, List<String>>> columnToHadoopFunction = fileGroup.getColumnToHadoopFunction();
        Map<String, EtlColumnMapping> columnMappings = Maps.newHashMap();
        if (columnToHadoopFunction != null) {
            for (Map.Entry<String, Pair<String, List<String>>> entry : columnToHadoopFunction.entrySet()) {
                columnMappings.put(entry.getKey(),
                                   new EtlColumnMapping(entry.getValue().first, entry.getValue().second));
            }
        }

        // partition ids
        List<Long> partitionIds = fileGroup.getPartitionIds();
        if (partitionIds == null || partitionIds.isEmpty()) {
            partitionIds = Lists.newArrayList(tablePartitionIds);
        }

        // where
        String where = null;
        if (fileGroup.getWhereExpr() != null) {
            where = fileGroup.getWhereExpr().toSql();
        }
        EtlFileGroup etlFileGroup = new EtlFileGroup(fileGroup.getFilePaths(), fileGroup.getFileFieldNames(),
                                                     fileGroup.getColumnsFromPath(), fileGroup.getValueSeparator(),
                                                     fileGroup.getLineDelimiter(), fileGroup.isNegative(),
                                                     fileGroup.getFileFormat(), columnMappings,
                                                     where, partitionIds);

        // set hive table
        etlFileGroup.setHiveTableName(((SparkLoadJob) callback).getHiveTableName());

        return etlFileGroup;
    }
}
