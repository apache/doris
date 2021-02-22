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

package org.apache.doris.task;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.Pair;
import org.apache.doris.load.DppConfig;
import org.apache.doris.load.DppScheduler;
import org.apache.doris.load.EtlSubmitResult;
import org.apache.doris.load.LoadErrorHub;
import org.apache.doris.load.LoadErrorHub.HubType;
import org.apache.doris.load.LoadJob;
import org.apache.doris.load.PartitionLoadInfo;
import org.apache.doris.load.Source;
import org.apache.doris.load.TableLoadInfo;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.transaction.TransactionState;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class HadoopLoadPendingTask extends LoadPendingTask {
    private static final Logger LOG = LogManager.getLogger(HadoopLoadPendingTask.class);

    private Map<String, Object> etlTaskConf;

    public HadoopLoadPendingTask(LoadJob job) {
        super(job);
    }

    @Override
    protected void createEtlRequest() throws Exception {
        EtlTaskConf taskConf = new EtlTaskConf();
        // output path
        taskConf.setOutputPath(getOutputPath());
        // output file pattern
        taskConf.setOutputFilePattern(job.getLabel() + ".%(table)s.%(view)s.%(bucket)s");
        // tables (partitions)
        Map<String, EtlPartitionConf> etlPartitions = createEtlPartitions();
        Preconditions.checkNotNull(etlPartitions);
        taskConf.setEtlPartitions(etlPartitions);
    
        LoadErrorHub.Param info = load.getLoadErrorHubInfo();
        // hadoop load only support mysql load error hub
        if (info != null && info.getType() == HubType.MYSQL_TYPE) {
            taskConf.setHubInfo(new EtlErrorHubInfo(this.job.getId(), info));
        }
    
        etlTaskConf = taskConf.toDppTaskConf();
        Preconditions.checkNotNull(etlTaskConf);

        // add table indexes to transaction state
        TransactionState txnState = Catalog.getCurrentGlobalTransactionMgr().getTransactionState(job.getDbId(), job.getTransactionId());
        if (txnState == null) {
            throw new LoadException("txn does not exist: " + job.getTransactionId());
        }
        for (long tableId : job.getIdToTableLoadInfo().keySet()) {
            OlapTable table = (OlapTable) db.getTable(tableId);
            if (table == null) {
                throw new LoadException("table does not exist. id: " + tableId);
            }
            table.readLock();
            try {
                txnState.addTableIndexes(table);
            } finally {
                table.readUnlock();
            }
        }
    }

    @Override
    protected EtlSubmitResult submitEtlJob(int retry) {
        LOG.info("begin submit hadoop etl job: {}", job);
        // retry different output path
        etlTaskConf.put("output_path", getOutputPath());

        DppScheduler dppScheduler = new DppScheduler(job.getHadoopDppConfig());
        EtlSubmitResult result = dppScheduler.submitEtlJob(job.getId(), job.getLabel(), job.getHadoopCluster(),
                db.getFullName(), etlTaskConf, retry);

        if (result != null && result.getStatus().getStatusCode() == TStatusCode.OK) {
            job.setHadoopEtlJobId(result.getEtlJobId());
        }

        return result;
    }

    private Map<String, EtlPartitionConf> createEtlPartitions() throws LoadException {
        Map<String, EtlPartitionConf> etlPartitions = Maps.newHashMap();

        for (Entry<Long, TableLoadInfo> tableEntry : job.getIdToTableLoadInfo().entrySet()) {
            long tableId = tableEntry.getKey();
            TableLoadInfo tableLoadInfo = tableEntry.getValue();

            OlapTable table = (OlapTable) db.getTable(tableId);
            if (table == null) {
                throw new LoadException("table does not exist. id: " + tableId);
            }
            table.readLock();
            try {
                // columns
                Map<String, EtlColumn> etlColumns = createEtlColumns(table);

                // partitions
                Map<Long, PartitionLoadInfo> idToPartitionLoadInfo = tableLoadInfo.getIdToPartitionLoadInfo();
                for (Entry<Long, PartitionLoadInfo> partitionEntry : idToPartitionLoadInfo.entrySet()) {
                    long partitionId = partitionEntry.getKey();
                    PartitionLoadInfo partitionLoadInfo = partitionEntry.getValue();

                    EtlPartitionConf etlPartitionConf = new EtlPartitionConf();
                    // columns
                    etlPartitionConf.setColumns(etlColumns);

                    // indices (views)
                    Map<String, EtlIndex> etlIndices = createEtlIndices(table, partitionId);
                    Preconditions.checkNotNull(etlIndices);
                    etlPartitionConf.setIndices(etlIndices);

                    // source file schema
                    etlPartitionConf.setSources(createSources(partitionLoadInfo));

                    // partition info
                    etlPartitionConf.setPartitionInfo(createPartitionInfo(table, partitionId));

                    etlPartitions.put(String.valueOf(partitionId), etlPartitionConf);
                }
            } finally {
                table.readUnlock();
            }
        }

        return etlPartitions;
    }


    private Map<String, EtlColumn> createEtlColumns(OlapTable table) {
        Map<String, EtlColumn> etlColumns = Maps.newHashMap();
        for (Column column : table.getBaseSchema()) {
            etlColumns.put(column.getName(), new EtlColumn(column));
        }
        return etlColumns;
    }

    private Map<String, EtlIndex> createEtlIndices(OlapTable table, long partitionId) throws LoadException {
        Map<String, EtlIndex> etlIndices = Maps.newHashMap();

        TableLoadInfo tableLoadInfo = job.getTableLoadInfo(table.getId());
        for (Entry<Long, MaterializedIndexMeta> entry : table.getIndexIdToMeta().entrySet()) {
            long indexId = entry.getKey();
            MaterializedIndexMeta indexMeta = entry.getValue();

            List<Column> indexColumns = indexMeta.getSchema();

            Partition partition = table.getPartition(partitionId);
            if (partition == null) {
                throw new LoadException("partition does not exist. id: " + partitionId);
            }

            EtlIndex etlIndex = new EtlIndex();
            etlIndex.setKeysType(table.getKeysType());
            // index id
            etlIndex.setIndexId(indexId);

            // schema hash
            int schemaHash = indexMeta.getSchemaHash();
            etlIndex.setSchemaHash(schemaHash);
            tableLoadInfo.addIndexSchemaHash(indexId, schemaHash);

            int keySize = 0;
            List<Map<String, Object>> columnRefs = Lists.newArrayList();
            for (Column column : indexColumns) {
                Map<String, Object> dppColumn = Maps.newHashMap();
                dppColumn.put("name", column.getName());
                if (column.isKey()) {
                    dppColumn.put("is_key", true);
                    ++keySize;
                } else {
                    dppColumn.put("is_key", false);
                    String aggregation = "none";
                    if ("AGG_KEYS" == table.getKeysType().name()) {
                        AggregateType aggregateType = column.getAggregationType();
                        if (AggregateType.SUM == aggregateType) {
                            aggregation = "ADD";
                        } else {
                            aggregation = aggregateType.name();
                        }
                    } else if (table.getKeysType().name().equalsIgnoreCase("UNIQUE_KEYS")) {
                        aggregation = "REPLACE";
                    }
                    dppColumn.put("aggregation_method", aggregation);
                }
                columnRefs.add(dppColumn);
            }

            // distribution infos
            DistributionInfo distributionInfo = partition.getDistributionInfo();
            List<String> distributionColumnRefs = Lists.newArrayList();
            etlIndex.setDistributionColumnRefs(distributionColumnRefs);
            etlIndex.setPartitionMethod("hash");
            etlIndex.setHashMod(distributionInfo.getBucketNum());
            switch (distributionInfo.getType()) {
                case RANDOM:
                    etlIndex.setHashMethod("CRC32");
                    for (int i = 0; i < keySize; ++i) {
                        distributionColumnRefs.add(columnRefs.get(i).get("name").toString());
                    }
                    break;
                case HASH:
                    etlIndex.setHashMethod("CRC32");
                    HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                    for (Column column : hashDistributionInfo.getDistributionColumns()) {
                        distributionColumnRefs.add(column.getName());
                        boolean isImplicit = true;
                        Iterator<Map<String, Object>> iter = columnRefs.iterator();
                        while (iter.hasNext()) {
                            if (iter.next().get("name").equals(column.getName())) {
                                isImplicit = false;
                                break;
                            }
                        }
                        if (isImplicit) {
                            Map<String, Object> dppColumn = Maps.newHashMap();
                            dppColumn.put("name", column.getName());
                            dppColumn.put("is_key", true);
                            dppColumn.put("is_implicit", true);
                            columnRefs.add(keySize, dppColumn); 
                            ++keySize;
                        }
                    }
                    break;
                default:
                    LOG.warn("unknown distribution type. type: {}", distributionInfo.getType().name());
                    throw new LoadException("unknown distribution type. type: " + distributionInfo.getType().name());
            }

            etlIndex.setPidKeyCount(keySize);
            etlIndex.setColumnRefs(columnRefs);
            etlIndices.put(String.valueOf(indexId), etlIndex);
        }

        return etlIndices;
    }

    private Map<String, EtlSource> createSources(PartitionLoadInfo partitionLoadInfo) {
        Map<String, EtlSource> sources = Maps.newHashMap();
        int sourceIndex = 0;
        for (Source source : partitionLoadInfo.getSources()) {
            sources.put("schema" + sourceIndex, new EtlSource(source));
            ++sourceIndex;
        }
        return sources;
    }

    private EtlPartitionInfo createPartitionInfo(OlapTable table, long partitionId) {
        PartitionInfo partitionInfo = table.getPartitionInfo();
        switch (partitionInfo.getType()) {
            case RANGE:
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;

                // partition columns
                List<String> partitionColumnNames = Lists.newArrayList();
                for (Column column : rangePartitionInfo.getPartitionColumns()) {
                    partitionColumnNames.add(column.getName());
                }

                // begin keys
                // is max partition
                Range<PartitionKey> range = rangePartitionInfo.getRange(partitionId);
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

                return new EtlPartitionInfo(table.getId(), partitionColumnNames, "range", startKeys, endKeys,
                        isMaxPartition);
            case UNPARTITIONED:
                break;
            default:
                LOG.error("unknown partition type. type: {}", partitionInfo.getType().name());
                break;
        }
        return null;
    }

    private String getOutputPath() {
        job.setHadoopEtlOutputDir(String.valueOf(System.currentTimeMillis()));
        String loadLabel = job.getLabel();
        DppConfig dppConfig = job.getHadoopDppConfig();
        String outputPath = DppScheduler.getEtlOutputPath(dppConfig.getFsDefaultName(), dppConfig.getOutputPath(),
                job.getDbId(), loadLabel, job.getHadoopEtlOutputDir());
        return outputPath;
    }

    private class EtlTaskConf {
        private static final String JOB_TYPE = "palo";

        private String outputFilePattern;
        private String outputPath;
        private Map<String, EtlPartitionConf> etlPartitions;

        private EtlErrorHubInfo hubInfo;

        public void setOutputFilePattern(String outputFilePattern) {
            this.outputFilePattern = outputFilePattern;
        }

        public void setOutputPath(String outputPath) {
            this.outputPath = outputPath;
        }

        public void setEtlPartitions(Map<String, EtlPartitionConf> etlPartitions) {
            this.etlPartitions = etlPartitions;
        }

        public void setHubInfo(EtlErrorHubInfo info) {
            this.hubInfo = info;
        }

        public Map<String, Object> toDppTaskConf() {
            // dpp group -> tables
            // dpp tables -> partitions
            // dpp views -> indices
            Map<String, Object> taskConf = Maps.newHashMap();
            taskConf.put("job_type", JOB_TYPE);
            taskConf.put("output_file_pattern", outputFilePattern);
            taskConf.put("output_path", outputPath);
            taskConf.put("tables",
                    Maps.transformValues(etlPartitions, new Function<EtlPartitionConf, Map<String, Object>>() {
                        @Override
                        public Map<String, Object> apply(EtlPartitionConf partition) {
                            return partition.toDppPartitionConf();
                        }
                    }));
            if (hubInfo != null) {
                taskConf.put("hub_info", hubInfo.toDppHubInfo());
            }
            return taskConf;
        }
    }

    private class EtlErrorHubInfo {
        LoadErrorHub.Param hubInfo;
        long jobId;
        private static final int MAX_EXPORT_LINE_NUM = 10;
        private static final int MAX_EXPORT_LINE_SIZE = 500;

        public EtlErrorHubInfo(long jobId, LoadErrorHub.Param info) {
            this.jobId = jobId;
            this.hubInfo = info;
        }

        public Map<String, Object> toDppHubInfo() {
            Map<String, Object> dppHubInfo = hubInfo.toDppConfigInfo();

            dppHubInfo.put("job_id", jobId);
            dppHubInfo.put("max_export_line_num", MAX_EXPORT_LINE_NUM);
            dppHubInfo.put("max_export_line_size", MAX_EXPORT_LINE_SIZE);

            return dppHubInfo;
        }

    }

    private class EtlPartitionConf {
        private Map<String, EtlSource> sources;
        private Map<String, EtlColumn> columns;
        private Map<String, EtlIndex> indices;
        private EtlPartitionInfo partitionInfo;

        public void setSources(Map<String, EtlSource> sources) {
            this.sources = sources;
        }

        public void setColumns(Map<String, EtlColumn> columns) {
            this.columns = columns;
        }

        public void setIndices(Map<String, EtlIndex> indices) {
            this.indices = indices;
        }

        public void setPartitionInfo(EtlPartitionInfo partitionInfo) {
            this.partitionInfo = partitionInfo;
        }

        public Map<String, Object> toDppPartitionConf() {
            Map<String, Object> partitionConf = Maps.newHashMap();
            partitionConf.put("source_file_schema",
                    Maps.transformValues(sources, new Function<EtlSource, Map<String, Object>>() {
                        @Override
                        public Map<String, Object> apply(EtlSource source) {
                            return source.toDppSource();
                        }
                    }));
            partitionConf.put("columns",
                    Maps.transformValues(columns, new Function<EtlColumn, Map<String, Object>>() {
                        @Override
                        public Map<String, Object> apply(EtlColumn column) {
                            return column.toDppColumn();
                        }
                    }));
            partitionConf.put("views",
                    Maps.transformValues(indices, new Function<EtlIndex, Map<String, Object>>() {
                        @Override
                        public Map<String, Object> apply(EtlIndex index) {
                            return index.toDppView();
                        }
                    }));
            if (partitionInfo != null) {
                partitionConf.put("partition_info", partitionInfo.toDppPartitionInfo());
            }
            return partitionConf;
        }
    }

    private class EtlSource {
        private final Source source;

        public EtlSource(Source source) {
            this.source = source;
        }

        public Map<String, Object> toDppSource() {
            Map<String, Object> dppSource = Maps.newHashMap();
            dppSource.put("file_urls", source.getFileUrls());
            dppSource.put("columns", source.getColumnNames());
            dppSource.put("column_separator", source.getColumnSeparator());
            dppSource.put("is_negative", source.isNegative());
            Map<String, Pair<String, List<String>>> columnToFunction = source.getColumnToFunction();
            if (columnToFunction != null && !columnToFunction.isEmpty()) {
                Map<String, Map<String, Object>> columnMapping = Maps.newHashMap();
                for (Entry<String, Pair<String, List<String>>> entry : columnToFunction.entrySet()) {
                    Pair<String, List<String>> functionPair = entry.getValue();
                    Map<String, Object> functionMap = Maps.newHashMap();
                    functionMap.put("function_name", functionPair.first);
                    functionMap.put("args", functionPair.second);
                    columnMapping.put(entry.getKey(), functionMap);
                }
                dppSource.put("column_mappings", columnMapping);
            }
            return dppSource;
        }
    }

    private class EtlColumn {
        private final Column column;

        public EtlColumn(Column column) {
            this.column = column;
        }

        public Map<String, Object> toDppColumn() {
            Map<String, Object> dppColumn = Maps.newHashMap();
            // column type
            PrimitiveType type = column.getDataType();
            String columnType = null;
            switch (type) {
                case TINYINT:
                    columnType = "TINY";
                    break;
                case SMALLINT:
                    columnType = "SHORT";
                    break;
                case INT:
                    columnType = "INT";
                    break;
                case BIGINT:
                    columnType = "LONG";
                    break;
                case LARGEINT:
                    columnType = "LARGEINT";
                    break;
                case FLOAT:
                    columnType = "FLOAT";
                    break;
                case DOUBLE:
                    columnType = "DOUBLE";
                    break;
                case DATE:
                    columnType = "DATE";
                    break;
                case DATETIME:
                    columnType = "DATETIME";
                    break;
                case CHAR:
                    columnType = "STRING";
                    break;
                case VARCHAR:
                    columnType = "VARCHAR";
                    break;
                case HLL:
                    columnType = "HLL";
                    break;
                case BITMAP:
                    columnType = "BITMAP";
                    break;
                case DECIMAL:
                    columnType = "DECIMAL";
                    break;
                case DECIMALV2:
                    columnType = "DECIMAL";
                    break;
                default:
                    columnType = type.toString();
                    break;
            }
            dppColumn.put("column_type", columnType);

            // is allow null
            if (column.isAllowNull()) {
                dppColumn.put("is_allow_null", column.isAllowNull());
            }

            // default value
            if (column.getDefaultValue() != null) {
                dppColumn.put("default_value", column.getDefaultValue());
            }

            if (column.isAllowNull() && null == column.getDefaultValue()) {
                dppColumn.put("default_value", "\\N");
            }

            // string length
            if (type == PrimitiveType.CHAR || type == PrimitiveType.VARCHAR || type == PrimitiveType.HLL) {
                dppColumn.put("string_length", column.getStrLen());
            }

            // decimal precision scale
            if (type == PrimitiveType.DECIMAL || type == PrimitiveType.DECIMALV2) {
                dppColumn.put("precision", column.getPrecision());
                dppColumn.put("scale", column.getScale());
            }

            return dppColumn;
        }

        public boolean isKey() {
            return column.isKey();
        }

        public AggregateType getAggregationType() {
            return column.getAggregationType();
        }

    }

    private class EtlIndex {
        private static final String OUTPUT_FORMAT = "palo";
        private static final String BE_INDEX_NAME = "PRIMARY";

        private KeysType keysType;
        private long indexId;
        private List<Map<String, Object>> columnRefs;
        private int schemaHash;
        private String partitionMethod;
        private String hashMethod;
        private int hashMod;
        private List<String> distributionColumnRefs;
        private int pidKeyCount;

        public void setKeysType(KeysType keysType) {
            this.keysType = keysType;
        }

        public void setIndexId(long indexId) {
            this.indexId = indexId;
        }

        public void setColumnRefs(List<Map<String, Object>> columnRefs) {
            this.columnRefs = columnRefs;
        }

        public void setSchemaHash(int schemaHash) {
            this.schemaHash = schemaHash;
        }

        public void setPartitionMethod(String partitionMethod) {
            this.partitionMethod = partitionMethod;
        }

        public void setHashMethod(String hashMethod) {
            this.hashMethod = hashMethod;
        }

        public void setHashMod(int hashMod) {
            this.hashMod = hashMod;
        }

        public void setDistributionColumnRefs(List<String> distributionColumnRefs) {
            this.distributionColumnRefs = distributionColumnRefs;
        }

        public void setPidKeyCount(int pidKeyCount) {
            this.pidKeyCount = pidKeyCount;
        }

        public Map<String, Object> toDppView() {
            Map<String, Object> index = Maps.newHashMap();
            index.put("keys_type", keysType.name());
            index.put("output_format", OUTPUT_FORMAT);
            index.put("index_name", BE_INDEX_NAME);
            index.put("table_name", String.valueOf(indexId));
            index.put("table_id", indexId);
            index.put("column_refs", columnRefs);
            index.put("schema_hash", schemaHash);
            index.put("partition_method", partitionMethod);
            index.put("hash_method", hashMethod);
            index.put("hash_mod", hashMod);
            index.put("partition_column_refs", distributionColumnRefs);
            index.put("pid_key_count", pidKeyCount);
            return index;
        }
    }

    private class EtlPartitionInfo {
        private long tableId;
        private List<String> tablePartitionColumnRefs;
        private String tablePartitionMethod;
        private List<Object> startKeys;
        private List<Object> endKeys;
        private boolean isMaxPartition;

        public EtlPartitionInfo(long tableId, List<String> tablePartitionColumnRefs, String tablePartitionMethod,
                                List<Object> startKeys, List<Object> endKeys, boolean isMaxPartition) {
            this.tableId = tableId;
            this.tablePartitionColumnRefs = tablePartitionColumnRefs;
            this.tablePartitionMethod = tablePartitionMethod;
            this.startKeys = startKeys;
            this.endKeys = endKeys;
            this.isMaxPartition = isMaxPartition;
        }

        public Map<String, Object> toDppPartitionInfo() {
            Map<String, Object> partitionInfo = Maps.newHashMap();
            partitionInfo.put("group_id", tableId);
            partitionInfo.put("group_partition_column_refs", tablePartitionColumnRefs);
            partitionInfo.put("group_partition_method", tablePartitionMethod);
            partitionInfo.put("start_keys", startKeys);
            partitionInfo.put("end_keys", endKeys);
            partitionInfo.put("is_max_partition", isMaxPartition);
            return partitionInfo;
        }
    }
}
