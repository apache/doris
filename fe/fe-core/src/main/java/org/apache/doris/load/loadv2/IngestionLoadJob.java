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

import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DataQualityException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.QuotaExceedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.common.util.MetaLockUtils;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.FailMsg;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.sparkdpp.DppResult;
import org.apache.doris.sparkdpp.EtlJobConfig;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.PushTask;
import org.apache.doris.thrift.TBrokerRangeDesc;
import org.apache.doris.thrift.TBrokerScanRange;
import org.apache.doris.thrift.TBrokerScanRangeParams;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TDescriptorTable;
import org.apache.doris.thrift.TEtlState;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TPriority;
import org.apache.doris.thrift.TPushType;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TabletQuorumFailedException;
import org.apache.doris.transaction.TransactionState;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Ingestion Load
 * </p>
 * Load data file which has been pre-processed
 * </p>
 * There are 4 steps in IngestionLoadJob:
 * Step1: Outside system execute ingestion etl job.
 * Step2: LoadEtlChecker will check ingestion etl job status periodically
 * and send push tasks to be when ingestion etl job is finished.
 * Step3: LoadLoadingChecker will check loading status periodically and commit transaction when push tasks are finished.
 * Step4: PublishVersionDaemon will send publish version tasks to be and finish transaction.
 */
public class IngestionLoadJob extends LoadJob {

    public static final Logger LOG = LogManager.getLogger(IngestionLoadJob.class);

    @Setter
    @SerializedName("ests")
    private EtlStatus etlStatus;

    // members below updated when job state changed to loading
    // { tableId.partitionId.indexId.bucket.schemaHash -> (etlFilePath, etlFileSize) }
    @SerializedName(value = "tm2fi")
    private final Map<String, Pair<String, Long>> tabletMetaToFileInfo = Maps.newHashMap();

    @SerializedName(value = "hp")
    private final Map<String, String> hadoopProperties = new HashMap<>();

    @SerializedName(value = "i2sv")
    private final Map<Long, Integer> indexToSchemaVersion = new HashMap<>();

    private final Map<Long, Integer> indexToSchemaHash = Maps.newHashMap();

    private final Map<String, Long> filePathToSize = new HashMap<>();

    private final Set<Long> finishedReplicas = Sets.newHashSet();
    private final Set<Long> quorumTablets = Sets.newHashSet();
    private final Set<Long> fullTablets = Sets.newHashSet();

    private final List<TabletCommitInfo> commitInfos = Lists.newArrayList();

    private final Map<Long, Set<Long>> tableToLoadPartitions = Maps.newHashMap();

    private final Map<Long, Map<Long, PushTask>> tabletToSentReplicaPushTask = Maps.newHashMap();

    private long etlStartTimestamp = -1;

    private long quorumFinishTimestamp = -1;

    private List<Long> loadTableIds = new ArrayList<>();

    public IngestionLoadJob() {
        super(EtlJobType.INGESTION);
    }

    public IngestionLoadJob(long dbId, String label, List<String> tableNames, UserIdentity userInfo)
            throws LoadException {
        super(EtlJobType.INGESTION, dbId, label);
        this.loadTableIds = getLoadTableIds(tableNames);
        this.userInfo = userInfo;
    }

    @Override
    public Set<String> getTableNamesForShow() {
        return Collections.emptySet();
    }

    @Override
    public Set<String> getTableNames() throws MetaNotFoundException {
        Set<String> result = Sets.newHashSet();
        Database database = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
        for (long tableId : loadTableIds) {
            Table table = database.getTableOrMetaException(tableId);
            result.add(table.getName());
        }
        return result;
    }

    @Override
    public void afterVisible(TransactionState txnState, boolean txnOperated) {
        super.afterVisible(txnState, txnOperated);
        clearJob();
    }

    @Override
    public void afterAborted(TransactionState txnState, boolean txnOperated, String txnStatusChangeReason)
            throws UserException {
        super.afterAborted(txnState, txnOperated, txnStatusChangeReason);
        clearJob();
    }

    @Override
    public void cancelJobWithoutCheck(FailMsg failMsg, boolean abortTxn, boolean needLog) {
        super.cancelJobWithoutCheck(failMsg, abortTxn, needLog);
        clearJob();
    }

    @Override
    public void cancelJob(FailMsg failMsg) throws DdlException {
        super.cancelJob(failMsg);
        clearJob();
    }

    private List<Long> getLoadTableIds(List<String> tableNames) throws LoadException {
        Database db = Env.getCurrentInternalCatalog()
                .getDbOrException(dbId, s -> new LoadException("db does not exist. id: " + s));
        List<Long> list = new ArrayList<>(tableNames.size());
        for (String tableName : tableNames) {
            OlapTable olapTable = (OlapTable) db.getTableOrException(tableName,
                    s -> new LoadException("table does not exist. id: " + s));
            list.add(olapTable.getId());
        }
        return list;
    }

    @Override
    protected long getEtlStartTimestamp() {
        return etlStartTimestamp;
    }

    public long beginTransaction()
            throws BeginTransactionException, MetaNotFoundException, AnalysisException, QuotaExceedException,
            LabelAlreadyUsedException, DuplicatedRequestException {
        this.transactionId = Env.getCurrentGlobalTransactionMgr()
                .beginTransaction(dbId, loadTableIds, label, null,
                        new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, 0,
                                FrontendOptions.getLocalHostAddress(), ExecuteEnv.getInstance().getStartupTime()),
                        TransactionState.LoadJobSourceType.FRONTEND, id, getTimeout());
        return transactionId;
    }

    public Map<String, Object> getLoadMeta(Map<String, List<String>> tableToPartitionMap)
            throws LoadException {

        if (tableToPartitionMap == null || tableToPartitionMap.isEmpty()) {
            throw new IllegalArgumentException("tableToPartitionMap is empty");
        }

        Database db = Env.getCurrentInternalCatalog()
                .getDbOrException(dbId, s -> new LoadException("db does not exist. id: " + s));
        Map<String, Object> loadMeta = new HashMap<>();
        loadMeta.put("dbId", db.getId());
        Long signature = Env.getCurrentEnv().getNextId();
        loadMeta.put("signature", signature);

        List<Table> tables;
        try {
            tables = db.getTablesOnIdOrderOrThrowException(loadTableIds);
        } catch (MetaNotFoundException e) {
            throw new LoadException(e.getMessage());
        }

        MetaLockUtils.readLockTables(tables);
        try {
            Map<String, Map<String, Object>> tableMeta = new HashMap<>(tableToPartitionMap.size());
            for (Map.Entry<String, List<String>> entry : tableToPartitionMap.entrySet()) {
                String tableName = entry.getKey();
                Map<String, Object> meta = tableMeta.getOrDefault(tableName, new HashMap<>());
                OlapTable olapTable = (OlapTable) db.getTableOrException(tableName,
                        s -> new LoadException("table does not exist. id: " + s));
                meta.put("id", olapTable.getId());
                List<EtlJobConfig.EtlIndex> indices = createEtlIndexes(olapTable);
                meta.put("indexes", indices);
                List<String> partitionNames = entry.getValue();
                Set<Long> partitionIds;
                if (partitionNames != null && !partitionNames.isEmpty()) {
                    partitionIds = new HashSet<>(partitionNames.size());
                    for (String partitionName : partitionNames) {
                        Partition partition = olapTable.getPartition(partitionName);
                        if (partition == null) {
                            throw new LoadException(String.format("partition %s is not exists", partitionName));
                        }
                        partitionIds.add(partition.getId());
                    }
                } else {
                    partitionIds =
                            olapTable.getAllPartitions().stream().map(Partition::getId).collect(Collectors.toSet());
                }
                EtlJobConfig.EtlPartitionInfo etlPartitionInfo = createEtlPartitionInfo(olapTable, partitionIds);
                meta.put("partitionInfo", etlPartitionInfo);
                tableMeta.put(tableName, meta);

                if (tableToLoadPartitions.containsKey(olapTable.getId())) {
                    tableToLoadPartitions.get(olapTable.getId()).addAll(partitionIds);
                } else {
                    tableToLoadPartitions.put(olapTable.getId(), partitionIds);
                }

            }
            loadMeta.put("tableMeta", tableMeta);
        } finally {
            MetaLockUtils.readUnlockTables(tables);
        }
        return loadMeta;

    }

    private List<EtlJobConfig.EtlIndex> createEtlIndexes(OlapTable table) throws LoadException {
        List<EtlJobConfig.EtlIndex> etlIndexes = Lists.newArrayList();

        for (Map.Entry<Long, List<Column>> entry : table.getIndexIdToSchema().entrySet()) {
            long indexId = entry.getKey();
            // todo(liheng): get schema hash and version from materialized index meta directly
            MaterializedIndexMeta indexMeta = table.getIndexMetaByIndexId(indexId);
            int schemaHash = indexMeta.getSchemaHash();
            int schemaVersion = indexMeta.getSchemaVersion();

            boolean changeAggType = table.getKeysTypeByIndexId(indexId).equals(KeysType.UNIQUE_KEYS)
                    && table.getTableProperty().getEnableUniqueKeyMergeOnWrite();

            // columns
            List<EtlJobConfig.EtlColumn> etlColumns = Lists.newArrayList();
            for (Column column : entry.getValue()) {
                etlColumns.add(createEtlColumn(column, changeAggType));
            }

            // check distribution type
            DistributionInfo distributionInfo = table.getDefaultDistributionInfo();
            if (distributionInfo.getType() != DistributionInfo.DistributionInfoType.HASH) {
                // RANDOM not supported
                String errMsg = "Unsupported distribution type. type: " + distributionInfo.getType().name();
                LOG.warn(errMsg);
                throw new LoadException(errMsg);
            }

            // index type
            String indexType;
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

            indexToSchemaVersion.put(indexId, schemaVersion);

            etlIndexes.add(new EtlJobConfig.EtlIndex(indexId, etlColumns, schemaHash, indexType,
                    indexId == table.getBaseIndexId(), schemaVersion));
        }

        return etlIndexes;
    }

    private EtlJobConfig.EtlColumn createEtlColumn(Column column, boolean changeAggType) {
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

        return new EtlJobConfig.EtlColumn(name, columnType, isAllowNull, isKey, aggregationType, defaultValue,
                stringLength, precision, scale);
    }

    private EtlJobConfig.EtlPartitionInfo createEtlPartitionInfo(OlapTable table, Set<Long> partitionIds)
            throws LoadException {
        PartitionType type = table.getPartitionInfo().getType();

        List<String> partitionColumnRefs = Lists.newArrayList();
        List<EtlJobConfig.EtlPartition> etlPartitions = Lists.newArrayList();
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
                for (LiteralExpr literalExpr : rangeKeyExprs) {
                    Object keyValue = literalExpr.getRealValue();
                    startKeys.add(keyValue);
                }

                // end keys
                // is empty list when max partition
                List<Object> endKeys = Lists.newArrayList();
                if (!isMaxPartition) {
                    rangeKeyExprs = range.upperEndpoint().getKeys();
                    for (LiteralExpr literalExpr : rangeKeyExprs) {
                        Object keyValue = literalExpr.getRealValue();
                        endKeys.add(keyValue);
                    }
                }

                etlPartitions.add(
                        new EtlJobConfig.EtlPartition(partitionId, startKeys, endKeys, isMaxPartition, bucketNum));
            }
        } else if (type == PartitionType.UNPARTITIONED) {
            Preconditions.checkState(partitionIds.size() == 1, "partition size must be eqauls to 1");

            for (Long partitionId : partitionIds) {
                Partition partition = table.getPartition(partitionId);
                if (partition == null) {
                    throw new LoadException("partition does not exist. id: " + partitionId);
                }

                // bucket num
                int bucketNum = partition.getDistributionInfo().getBucketNum();

                etlPartitions.add(new EtlJobConfig.EtlPartition(partitionId, Lists.newArrayList(), Lists.newArrayList(),
                        true, bucketNum));
            }
        } else {
            throw new LoadException("Spark Load does not support list partition yet");
        }

        // distribution column refs
        List<String> distributionColumnRefs = Lists.newArrayList();
        DistributionInfo distributionInfo = table.getDefaultDistributionInfo();
        Preconditions.checkState(distributionInfo.getType() == DistributionInfo.DistributionInfoType.HASH);
        for (Column column : ((HashDistributionInfo) distributionInfo).getDistributionColumns()) {
            distributionColumnRefs.add(column.getName());
        }

        return new EtlJobConfig.EtlPartitionInfo(type.typeString, partitionColumnRefs, distributionColumnRefs,
                etlPartitions);
    }

    public void updateEtlStatus() throws Exception {

        if (!checkState(JobState.ETL) || etlStatus == null) {
            return;
        }

        writeLock();
        try {
            switch (etlStatus.getState()) {
                case FINISHED:
                    unprotectedProcessEtlFinish();
                    break;
                case CANCELLED:
                    throw new LoadException("spark etl job failed. msg: " + etlStatus.getFailMsg());
                default:
                    break;
            }
        } finally {
            writeUnlock();
        }

        if (checkState(JobState.LOADING)) {
            submitPushTasks();
        }

    }

    private boolean checkState(JobState expectState) {
        readLock();
        try {
            return state == expectState;
        } finally {
            readUnlock();
        }
    }

    private Set<Long> submitPushTasks() throws UserException {

        // check db exist
        Database db = null;
        try {
            db = getDb();
        } catch (MetaNotFoundException e) {
            String errMsg = new LogBuilder(LogKey.LOAD_JOB, id).add("database_id", dbId).add("label", label)
                    .add("error_msg", "db has been deleted when job is loading").build();
            throw new MetaNotFoundException(errMsg);
        }

        AgentBatchTask batchTask = new AgentBatchTask();
        boolean hasLoadPartitions = false;
        Set<Long> totalTablets = Sets.newHashSet();
        List<? extends TableIf> tableList = db.getTablesOnIdOrderOrThrowException(
                Lists.newArrayList(tableToLoadPartitions.keySet()));
        MetaLockUtils.readLockTables(tableList);
        try {
            writeLock();
            try {
                // check state is still loading. If state is cancelled or finished, return.
                // if state is cancelled or finished and not return,
                // this would throw all partitions have no load data exception,
                // because tableToLoadPartitions was already cleaned up,
                if (state != JobState.LOADING) {
                    LOG.warn("job state is not loading. job id: {}, state: {}", id, state);
                    return totalTablets;
                }

                for (TableIf table : tableList) {
                    Set<Long> partitionIds = tableToLoadPartitions.get(table.getId());
                    OlapTable olapTable = (OlapTable) table;
                    for (long partitionId : partitionIds) {
                        Partition partition = olapTable.getPartition(partitionId);
                        if (partition == null) {
                            throw new LoadException("partition does not exist. id: " + partitionId);
                        }

                        hasLoadPartitions = true;
                        int quorumReplicaNum =
                                olapTable.getPartitionInfo().getReplicaAllocation(partitionId).getTotalReplicaNum() / 2
                                        + 1;

                        List<MaterializedIndex> indexes = partition.getMaterializedIndices(
                                MaterializedIndex.IndexExtState.ALL);
                        for (MaterializedIndex index : indexes) {
                            long indexId = index.getId();
                            MaterializedIndexMeta indexMeta = olapTable.getIndexMetaByIndexId(indexId);
                            int schemaVersion = indexMeta.getSchemaVersion();
                            int schemaHash = indexMeta.getSchemaHash();

                            // check schemaHash and schemaVersion whether is changed
                            checkIndexSchema(indexId, schemaHash, schemaVersion);

                            int bucket = 0;
                            for (Tablet tablet : index.getTablets()) {
                                long tabletId = tablet.getId();
                                totalTablets.add(tabletId);
                                Set<Long> tabletAllReplicas = Sets.newHashSet();
                                Set<Long> tabletFinishedReplicas = Sets.newHashSet();
                                for (Replica replica : tablet.getReplicas()) {
                                    long replicaId = replica.getId();
                                    tabletAllReplicas.add(replicaId);
                                    if (!tabletToSentReplicaPushTask.containsKey(tabletId)
                                            || !tabletToSentReplicaPushTask.get(tabletId).containsKey(replicaId)) {
                                        long backendId = replica.getBackendId();
                                        long taskSignature = Env.getCurrentGlobalTransactionMgr()
                                                .getNextTransactionId();

                                        PushTask pushTask =
                                                buildPushTask(backendId, olapTable, taskSignature, partitionId, indexId,
                                                        tabletId, replicaId, schemaHash, schemaVersion, bucket++);
                                        if (AgentTaskQueue.addTask(pushTask)) {
                                            batchTask.addTask(pushTask);
                                            if (!tabletToSentReplicaPushTask.containsKey(tabletId)) {
                                                tabletToSentReplicaPushTask.put(tabletId, Maps.newHashMap());
                                            }
                                            tabletToSentReplicaPushTask.get(tabletId).put(replicaId, pushTask);
                                        }
                                    }

                                    if (finishedReplicas.contains(replicaId) && replica.getLastFailedVersion() < 0) {
                                        tabletFinishedReplicas.add(replicaId);
                                    }
                                }

                                if (tabletAllReplicas.isEmpty()) {
                                    LOG.error("invalid situation. tablet is empty. id: {}", tabletId);
                                }

                                // check tablet push states
                                if (tabletFinishedReplicas.size() >= quorumReplicaNum) {
                                    quorumTablets.add(tabletId);
                                    if (tabletFinishedReplicas.size() == tabletAllReplicas.size()) {
                                        fullTablets.add(tabletId);
                                    }
                                }
                            }
                        }
                    }
                }

                if (batchTask.getTaskNum() > 0) {
                    AgentTaskExecutor.submit(batchTask);
                }

                if (!hasLoadPartitions) {
                    String errMsg = new LogBuilder(LogKey.LOAD_JOB, id).add("database_id", dbId).add("label", label)
                            .add("error_msg", "all partitions have no load data").build();
                    throw new LoadException(errMsg);
                }

                return totalTablets;
            } finally {
                writeUnlock();
            }
        } finally {
            MetaLockUtils.readUnlockTables(tableList);
        }

    }

    public void updateJobStatus(Map<String, String> statusInfo) {

        updateState(statusInfo.get("status"), statusInfo.get("msg"));

        etlStatus.setTrackingUrl(statusInfo.get("appId"));
        etlStatus.setProgress(progress);

        if (etlStatus.getState() == TEtlState.FINISHED) {
            Gson gson = new Gson();
            DppResult dppResult = gson.fromJson(statusInfo.get("dppResult"), DppResult.class);
            loadStatistic.fileNum = (int) dppResult.fileNumber;
            loadStatistic.totalFileSizeB = dppResult.fileSize;
            TUniqueId dummyId = new TUniqueId(0, 0);
            long dummyBackendId = -1L;
            loadStatistic.initLoad(dummyId, Sets.newHashSet(dummyId), Lists.newArrayList(dummyBackendId));
            loadStatistic.updateLoadProgress(dummyBackendId, dummyId, dummyId, dppResult.scannedRows,
                    dppResult.scannedBytes, true);
            loadingStatus.setDppResult(dppResult);
            Map<String, String> counters = loadingStatus.getCounters();
            counters.put(DPP_NORMAL_ALL, String.valueOf(dppResult.normalRows));
            counters.put(DPP_ABNORMAL_ALL, String.valueOf(dppResult.abnormalRows));
            counters.put(UNSELECTED_ROWS, String.valueOf(dppResult.unselectRows));
            filePathToSize.putAll(
                    gson.fromJson(statusInfo.get("filePathToSize"), new TypeToken<HashMap<String, Long>>() {
                    }));
            hadoopProperties.putAll(
                    gson.fromJson(statusInfo.get("hadoopProperties"), new TypeToken<HashMap<String, String>>() {
                    }));
        }

    }

    private void updateState(String stateStr, String msg) {

        switch (stateStr.toLowerCase()) {
            case "running":
                etlStatus.setState(TEtlState.RUNNING);
                break;
            case "success":
                etlStatus.setState(TEtlState.FINISHED);
                break;
            case "failed":
                boolean res = etlStatus.setState(TEtlState.CANCELLED);
                if (!res) {
                    etlStatus = new EtlStatus();
                    etlStatus.setState(TEtlState.CANCELLED);
                }
                etlStatus.setFailMsg(msg);
                break;
            default:
                etlStatus.setState(TEtlState.UNKNOWN);
                break;
        }

    }

    public void startEtlJob() {
        etlStartTimestamp = System.currentTimeMillis();
        state = JobState.ETL;
        etlStatus = new EtlStatus();
        unprotectedLogUpdateStateInfo();
    }

    private void unprotectedUpdateToLoadingState(EtlStatus etlStatus, Map<String, Long> filePathToSize)
            throws LoadException {
        try {
            for (Map.Entry<String, Long> entry : filePathToSize.entrySet()) {
                String filePath = entry.getKey();
                if (!filePath.endsWith(EtlJobConfig.ETL_OUTPUT_FILE_FORMAT)) {
                    continue;
                }
                String tabletMetaStr = EtlJobConfig.getTabletMetaStr(filePath);
                tabletMetaToFileInfo.put(tabletMetaStr, Pair.of(filePath, entry.getValue()));
            }

            loadingStatus = etlStatus;
            progress = 0;
            Env.getCurrentProgressManager().registerProgressSimple(String.valueOf(id));
            unprotectedUpdateState(JobState.LOADING);
            LOG.info("update to {} state success. job id: {}", state, id);
        } catch (Exception e) {
            LOG.warn("update to {} state failed. job id: {}", state, id, e);
            throw new LoadException(e.getMessage(), e);
        }
    }

    private void unprotectedPrepareLoadingInfos() {
        for (String tabletMetaStr : tabletMetaToFileInfo.keySet()) {
            String[] fileNameArr = tabletMetaStr.split("\\.");
            // tableId.partitionId.indexId.bucket.schemaHash
            Preconditions.checkState(fileNameArr.length == 5);
            long tableId = Long.parseLong(fileNameArr[0]);
            long partitionId = Long.parseLong(fileNameArr[1]);
            long indexId = Long.parseLong(fileNameArr[2]);
            int schemaHash = Integer.parseInt(fileNameArr[4]);

            if (!tableToLoadPartitions.containsKey(tableId)) {
                tableToLoadPartitions.put(tableId, Sets.newHashSet());
            }
            tableToLoadPartitions.get(tableId).add(partitionId);

            indexToSchemaHash.put(indexId, schemaHash);
        }
    }

    private void unprotectedProcessEtlFinish() throws Exception {
        // checkDataQuality
        if (!checkDataQuality()) {
            throw new DataQualityException(DataQualityException.QUALITY_FAIL_MSG);
        }

        // get etl output files and update loading state
        unprotectedUpdateToLoadingState(etlStatus, filePathToSize);
        // log loading state
        unprotectedLogUpdateStateInfo();
        // prepare loading infos
        unprotectedPrepareLoadingInfos();
    }

    private TBrokerScanRange getTBrokerScanRange(DescriptorTable descTable, TupleDescriptor destTupleDesc,
                                                 List<Column> columns, Map<String, String> properties)
            throws AnalysisException {

        TBrokerScanRange brokerScanRange = new TBrokerScanRange();

        TBrokerScanRangeParams params = new TBrokerScanRangeParams();
        params.setStrictMode(false);
        params.setProperties(properties);
        TupleDescriptor srcTupleDesc = descTable.createTupleDescriptor();
        Map<String, SlotDescriptor> srcSlotDescByName = Maps.newHashMap();
        for (Column column : columns) {
            SlotDescriptor srcSlotDesc = descTable.addSlotDescriptor(srcTupleDesc);
            srcSlotDesc.setIsMaterialized(true);
            srcSlotDesc.setIsNullable(true);

            if (column.getDataType() == PrimitiveType.BITMAP) {
                // cast to bitmap when the target column type is bitmap
                srcSlotDesc.setType(ScalarType.createType(PrimitiveType.BITMAP));
                srcSlotDesc.setColumn(new Column(column.getName(), PrimitiveType.BITMAP));
            } else {
                srcSlotDesc.setType(ScalarType.createType(PrimitiveType.VARCHAR));
                srcSlotDesc.setColumn(new Column(column.getName(), PrimitiveType.VARCHAR));
            }

            params.addToSrcSlotIds(srcSlotDesc.getId().asInt());
            srcSlotDescByName.put(column.getName(), srcSlotDesc);
        }

        Map<Integer, Integer> destSidToSrcSidWithoutTrans = Maps.newHashMap();
        for (SlotDescriptor destSlotDesc : destTupleDesc.getSlots()) {
            if (!destSlotDesc.isMaterialized()) {
                continue;
            }

            SlotDescriptor srcSlotDesc = srcSlotDescByName.get(destSlotDesc.getColumn().getName());
            destSidToSrcSidWithoutTrans.put(destSlotDesc.getId().asInt(), srcSlotDesc.getId().asInt());
            Expr expr = new SlotRef(srcSlotDesc);
            expr = castToSlot(destSlotDesc, expr);
            params.putToExprOfDestSlot(destSlotDesc.getId().asInt(), expr.treeToThrift());
        }
        params.setDestSidToSrcSidWithoutTrans(destSidToSrcSidWithoutTrans);
        params.setSrcTupleId(srcTupleDesc.getId().asInt());
        params.setDestTupleId(destTupleDesc.getId().asInt());
        brokerScanRange.setParams(params);

        // broker address updated for each replica
        brokerScanRange.setBrokerAddresses(Lists.newArrayList());

        // broker range desc
        TBrokerRangeDesc tBrokerRangeDesc = new TBrokerRangeDesc();
        tBrokerRangeDesc.setFileType(TFileType.FILE_HDFS);
        tBrokerRangeDesc.setFormatType(TFileFormatType.FORMAT_PARQUET);
        tBrokerRangeDesc.setSplittable(false);
        tBrokerRangeDesc.setStartOffset(0);
        tBrokerRangeDesc.setSize(-1);
        // path and file size updated for each replica
        brokerScanRange.setRanges(Collections.singletonList(tBrokerRangeDesc));

        return brokerScanRange;

    }

    private Expr castToSlot(SlotDescriptor slotDesc, Expr expr) throws AnalysisException {
        PrimitiveType dstType = slotDesc.getType().getPrimitiveType();
        PrimitiveType srcType = expr.getType().getPrimitiveType();
        if (dstType == PrimitiveType.BOOLEAN && srcType == PrimitiveType.VARCHAR) {
            // there is no cast VARCHAR to BOOLEAN function,
            // so we cast VARCHAR to TINYINT first, then cast TINYINT to BOOLEAN
            return new CastExpr(Type.BOOLEAN, new CastExpr(Type.TINYINT, expr));
        }
        if (dstType != srcType) {
            return expr.castTo(slotDesc.getType());
        }
        return expr;
    }

    private TDescriptorTable getTDescriptorTable(DescriptorTable descTable) {
        descTable.computeStatAndMemLayout();
        return descTable.toThrift();
    }

    private PushTask buildPushTask(long backendId, OlapTable olapTable, long taskSignature, long partitionId,
                                   long indexId, long tabletId, long replicaId, int schemaHash, int schemaVersion,
                                   long bucket)
            throws AnalysisException {

        DescriptorTable descTable = new DescriptorTable();
        TupleDescriptor destTupleDesc = descTable.createTupleDescriptor();

        List<TColumn> columnsDesc = new ArrayList<>();
        List<Column> columns = new ArrayList<>();
        Map<String, TColumn> colNameToColDesc = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (Column column : olapTable.getSchemaByIndexId(indexId)) {
            Column col = new Column(column);
            col.setName(column.getName().toLowerCase(Locale.ROOT));
            columns.add(col);
            TColumn tCol = col.toThrift();
            columnsDesc.add(tCol);
            String colName = col.tryGetBaseColumnName();
            colNameToColDesc.put(colName, tCol);
            // use index schema to fill the descriptor table
            SlotDescriptor destSlotDesc = descTable.addSlotDescriptor(destTupleDesc);
            destSlotDesc.setIsMaterialized(true);
            destSlotDesc.setColumn(col);
            destSlotDesc.setIsNullable(col.isAllowNull());
        }

        // deep copy TBrokerScanRange because filePath and fileSize will be updated
        // in different tablet push task
        TBrokerScanRange tBrokerScanRange =
                getTBrokerScanRange(descTable, destTupleDesc, columns, hadoopProperties);
        // update filePath fileSize
        TBrokerRangeDesc tBrokerRangeDesc = tBrokerScanRange.getRanges().get(0);
        tBrokerRangeDesc.setFileType(TFileType.FILE_HDFS);
        tBrokerRangeDesc.setPath("");
        tBrokerRangeDesc.setFileSize(-1);
        String tabletMetaStr = String.format("%d.%d.%d.%d.%d", olapTable.getId(), partitionId,
                indexId, bucket, schemaHash);
        if (tabletMetaToFileInfo.containsKey(tabletMetaStr)) {
            Pair<String, Long> fileInfo = tabletMetaToFileInfo.get(tabletMetaStr);
            tBrokerRangeDesc.setPath(fileInfo.first);
            tBrokerRangeDesc.setFileSize(fileInfo.second);
        }

        TDescriptorTable tDescriptorTable = getTDescriptorTable(descTable);

        return new PushTask(backendId, dbId, olapTable.getId(),
                partitionId, indexId, tabletId, replicaId, schemaHash, 0, id,
                TPushType.LOAD_V2, TPriority.NORMAL, transactionId, taskSignature,
                tBrokerScanRange, tDescriptorTable, columnsDesc, colNameToColDesc,
                olapTable.getStorageVaultId(), schemaVersion);
    }

    public void updateLoadingStatus() throws UserException {
        if (!checkState(JobState.LOADING)) {
            return;
        }

        if (etlStatus.getState() == TEtlState.CANCELLED) {
            throw new LoadException(etlStatus.getFailMsg());
        }

        // submit push tasks
        Set<Long> totalTablets = submitPushTasks();
        if (totalTablets.isEmpty()) {
            LOG.warn("total tablets set is empty. job id: {}, state: {}", id, state);
            return;
        }

        // update status
        boolean canCommitJob = false;
        writeLock();
        try {
            // loading progress
            // 100: txn status is visible and load has been finished
            progress = fullTablets.size() * 100 / totalTablets.size();
            if (progress == 100) {
                progress = 99;
            }

            // quorum finish ts
            if (quorumFinishTimestamp < 0 && quorumTablets.containsAll(totalTablets)) {
                quorumFinishTimestamp = System.currentTimeMillis();
            }

            // if all replicas are finished or stay in quorum finished for long time, try to commit it.
            long stragglerTimeout = 300 * 1000;
            if ((quorumFinishTimestamp > 0 && System.currentTimeMillis() - quorumFinishTimestamp > stragglerTimeout)
                    || fullTablets.containsAll(totalTablets)) {
                canCommitJob = true;
            }
        } finally {
            writeUnlock();
        }

        // try commit transaction
        if (canCommitJob) {
            tryCommitJob();
        }
    }

    private void tryCommitJob() throws UserException {
        LOG.info(new LogBuilder(LogKey.LOAD_JOB, id).add("txn_id", transactionId)
                .add("msg", "Load job try to commit txn").build());
        Database db = getDb();
        List<Table> tableList = db.getTablesOnIdOrderOrThrowException(
                Lists.newArrayList(tableToLoadPartitions.keySet()));
        MetaLockUtils.writeLockTablesOrMetaException(tableList);
        try {
            Env.getCurrentGlobalTransactionMgr().commitTransactionWithoutLock(
                    dbId, tableList, transactionId, commitInfos,
                    new LoadJobFinalOperation(id, loadingStatus, progress, loadStartTimestamp,
                            finishTimestamp, state, failMsg));
        } catch (TabletQuorumFailedException e) {
            // retry in next loop
        } finally {
            MetaLockUtils.writeUnlockTables(tableList);
        }
    }

    public void addFinishedReplica(long replicaId, long tabletId, long backendId) {
        writeLock();
        try {
            if (finishedReplicas.add(replicaId)) {
                commitInfos.add(new TabletCommitInfo(tabletId, backendId));
                // set replica push task null
                Map<Long, PushTask> sentReplicaPushTask = tabletToSentReplicaPushTask.get(tabletId);
                if (sentReplicaPushTask != null) {
                    if (sentReplicaPushTask.containsKey(replicaId)) {
                        sentReplicaPushTask.put(replicaId, null);
                    }
                }
            }
        } finally {
            writeUnlock();
        }
    }

    private void clearJob() {
        Preconditions.checkState(state == JobState.FINISHED || state == JobState.CANCELLED);

        if (LOG.isDebugEnabled()) {
            LOG.debug("clear push tasks and infos that not persist. id: {}, state: {}", id, state);
        }
        writeLock();
        try {
            // clear push task first
            for (Map<Long, PushTask> sentReplicaPushTask : tabletToSentReplicaPushTask.values()) {
                for (PushTask pushTask : sentReplicaPushTask.values()) {
                    if (pushTask == null) {
                        continue;
                    }
                    AgentTaskQueue.removeTask(pushTask.getBackendId(), pushTask.getTaskType(), pushTask.getSignature());
                }
            }
            tableToLoadPartitions.clear();
            indexToSchemaHash.clear();
            tabletToSentReplicaPushTask.clear();
            finishedReplicas.clear();
            quorumTablets.clear();
            fullTablets.clear();

            Env.getCurrentProgressManager().removeProgress(String.valueOf(progress));
        } finally {
            writeUnlock();
        }
    }

    private void unprotectedLogUpdateStateInfo() {
        IngestionLoadJobStateUpdateInfo info =
                new IngestionLoadJobStateUpdateInfo(id, state, transactionId, etlStartTimestamp, loadStartTimestamp,
                        etlStatus, tabletMetaToFileInfo, hadoopProperties, indexToSchemaVersion);
        Env.getCurrentEnv().getEditLog().logUpdateLoadJob(info);
    }

    public static class IngestionLoadJobStateUpdateInfo extends LoadJobStateUpdateInfo {

        @SerializedName(value = "etlStartTimestamp")
        private long etlStartTimestamp;
        @SerializedName(value = "etlStatus")
        private EtlStatus etlStatus;
        @SerializedName(value = "tabletMetaToFileInfo")
        private Map<String, Pair<String, Long>> tabletMetaToFileInfo;
        @SerializedName(value = "hadoopProperties")
        private Map<String, String> hadoopProperties;
        @SerializedName(value = "indexToSchemaVersion")
        private Map<Long, Integer> indexToSchemaVersion;

        public IngestionLoadJobStateUpdateInfo(long jobId, JobState state, long transactionId,
                                               long etlStartTimestamp, long loadStartTimestamp, EtlStatus etlStatus,
                                               Map<String, Pair<String, Long>> tabletMetaToFileInfo,
                                               Map<String, String> hadoopProperties,
                                               Map<Long, Integer> indexToSchemaVersion) {
            super(jobId, state, transactionId, loadStartTimestamp);
            this.etlStartTimestamp = etlStartTimestamp;
            this.etlStatus = etlStatus;
            this.tabletMetaToFileInfo = tabletMetaToFileInfo;
            this.hadoopProperties = hadoopProperties;
            this.indexToSchemaVersion = indexToSchemaVersion;
        }

        public long getEtlStartTimestamp() {
            return etlStartTimestamp;
        }

        public EtlStatus getEtlStatus() {
            return etlStatus;
        }

        public Map<String, Pair<String, Long>> getTabletMetaToFileInfo() {
            return tabletMetaToFileInfo;
        }

        public Map<String, String> getHadoopProperties() {
            return hadoopProperties;
        }

        public Map<Long, Integer> getIndexToSchemaVersion() {
            return indexToSchemaVersion;
        }
    }

    @Override
    public void replayUpdateStateInfo(LoadJobStateUpdateInfo info) {
        super.replayUpdateStateInfo(info);
        IngestionLoadJobStateUpdateInfo stateUpdateInfo = (IngestionLoadJobStateUpdateInfo) info;
        this.etlStartTimestamp = stateUpdateInfo.getEtlStartTimestamp();
        this.etlStatus = stateUpdateInfo.getEtlStatus();
        if (stateUpdateInfo.getTabletMetaToFileInfo() != null) {
            this.tabletMetaToFileInfo.putAll(stateUpdateInfo.getTabletMetaToFileInfo());
        }
        if (stateUpdateInfo.getHadoopProperties() != null) {
            this.hadoopProperties.putAll(stateUpdateInfo.getHadoopProperties());
        }
        if (stateUpdateInfo.getIndexToSchemaVersion() != null) {
            this.indexToSchemaVersion.putAll(stateUpdateInfo.getIndexToSchemaVersion());
        }
        switch (state) {
            case ETL:
                break;
            case LOADING:
                unprotectedPrepareLoadingInfos();
                break;
            default:
                LOG.warn("replay update load job state info failed. error: wrong state. job id: {}, state: {}", id,
                        state);
                break;
        }
    }

    private void checkIndexSchema(long indexId, int schemaHash, int schemaVersion) throws LoadException {
        if (indexToSchemaHash.containsKey(indexId) && indexToSchemaHash.get(indexId) == schemaHash
                && indexToSchemaVersion.containsKey(indexId) && indexToSchemaVersion.get(indexId) == schemaVersion) {
            return;
        }
        throw new LoadException(
                "schema of index [" + indexId + "] has changed, old schemaHash: " + indexToSchemaHash.get(indexId)
                        + ", current schemaHash: " + schemaHash + ", old schemaVersion: "
                        + indexToSchemaVersion.get(indexId) + ", current schemaVersion: " + schemaVersion);
    }

}
