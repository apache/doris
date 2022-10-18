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

package org.apache.doris.load;

import org.apache.doris.alter.SchemaChangeHandler;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.DataDescription;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionName;
import org.apache.doris.analysis.FunctionParams;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.backup.BlobStorage;
import org.apache.doris.backup.Status;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.catalog.Type;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.MetaLockUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.FailMsg.CancelType;
import org.apache.doris.load.LoadJob.JobState;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.ReplicaPersistInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.LoadTaskInfo;
import org.apache.doris.task.PushTask;
import org.apache.doris.thrift.TEtlState;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPriority;
import org.apache.doris.transaction.TransactionNotFoundException;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Load {
    private static final Logger LOG = LogManager.getLogger(Load.class);
    public static final String VERSION = "v1";

    // valid state change map
    private static final Map<JobState, Set<JobState>> STATE_CHANGE_MAP = Maps.newHashMap();

    // system dpp config
    public static DppConfig dppDefaultConfig = null;
    public static Map<String, DppConfig> clusterToDppConfig = Maps.newHashMap();

    // load job meta
    private Map<Long, LoadJob> idToLoadJob; // loadJobId to loadJob
    private Map<Long, List<LoadJob>> dbToLoadJobs; // db to loadJob list
    private Map<Long, Map<String, List<LoadJob>>> dbLabelToLoadJobs; // db label to loadJob list
    private Map<Long, LoadJob> idToPendingLoadJob; // loadJobId to pending loadJob
    private Map<Long, LoadJob> idToEtlLoadJob; // loadJobId to etl loadJob
    private Map<Long, LoadJob> idToLoadingLoadJob; // loadJobId to loading loadJob
    private Map<Long, LoadJob> idToQuorumFinishedLoadJob; // loadJobId to quorum finished loadJob
    private Set<Long> loadingPartitionIds; // loading partition id set
    // dbId -> set of (label, timestamp)
    private Map<Long, Map<String, Long>> dbToMiniLabels; // db to mini uncommitted label

    private volatile LoadErrorHub.Param loadErrorHubParam = new LoadErrorHub.Param();

    // lock for load job
    // lock is private and must use after db lock
    private ReentrantReadWriteLock lock;

    static {
        Set<JobState> pendingDestStates = Sets.newHashSet();
        pendingDestStates.add(JobState.ETL);
        pendingDestStates.add(JobState.CANCELLED);
        STATE_CHANGE_MAP.put(JobState.PENDING, pendingDestStates);

        Set<JobState> etlDestStates = Sets.newHashSet();
        etlDestStates.add(JobState.LOADING);
        etlDestStates.add(JobState.CANCELLED);
        STATE_CHANGE_MAP.put(JobState.ETL, etlDestStates);

        Set<JobState> loadingDestStates = Sets.newHashSet();
        loadingDestStates.add(JobState.FINISHED);
        loadingDestStates.add(JobState.QUORUM_FINISHED);
        loadingDestStates.add(JobState.CANCELLED);
        STATE_CHANGE_MAP.put(JobState.LOADING, loadingDestStates);

        Set<JobState> quorumFinishedDestStates = Sets.newHashSet();
        quorumFinishedDestStates.add(JobState.FINISHED);
        STATE_CHANGE_MAP.put(JobState.QUORUM_FINISHED, quorumFinishedDestStates);

        // system dpp config
        Gson gson = new Gson();
        try {
            Map<String, String> defaultConfig =
                    (HashMap<String, String>) gson.fromJson(Config.dpp_default_config_str, HashMap.class);
            dppDefaultConfig = DppConfig.create(defaultConfig);

            Map<String, Map<String, String>> clusterToConfig =
                    (HashMap<String, Map<String, String>>) gson.fromJson(Config.dpp_config_str, HashMap.class);
            for (Entry<String, Map<String, String>> entry : clusterToConfig.entrySet()) {
                String cluster = entry.getKey();
                DppConfig dppConfig = dppDefaultConfig.getCopiedDppConfig();
                dppConfig.update(DppConfig.create(entry.getValue()));
                dppConfig.check();

                clusterToDppConfig.put(cluster, dppConfig);
            }

            if (!clusterToDppConfig.containsKey(Config.dpp_default_cluster)) {
                throw new LoadException("Default cluster not exist");
            }
        } catch (Throwable e) {
            LOG.error("dpp default config ill-formed", e);
            System.exit(-1);
        }
    }

    public Load() {
        idToLoadJob = Maps.newHashMap();
        dbToLoadJobs = Maps.newHashMap();
        dbLabelToLoadJobs = Maps.newHashMap();
        idToPendingLoadJob = Maps.newLinkedHashMap();
        idToEtlLoadJob = Maps.newLinkedHashMap();
        idToLoadingLoadJob = Maps.newLinkedHashMap();
        idToQuorumFinishedLoadJob = Maps.newLinkedHashMap();
        loadingPartitionIds = Sets.newHashSet();
        dbToMiniLabels = Maps.newHashMap();
        lock = new ReentrantReadWriteLock(true);
    }

    public void readLock() {
        lock.readLock().lock();
    }

    public void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    public void addLoadJob(LoadStmt stmt, EtlJobType etlJobType, long timestamp) throws DdlException {
        // get db
        String dbName = stmt.getLabel().getDbName();
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);

        // create job
        LoadJob job = createLoadJob(stmt, etlJobType, db, timestamp);
        addLoadJob(job, db);
    }

    // This is the final step of all addLoadJob() methods
    private void addLoadJob(LoadJob job, Database db) throws DdlException {
        // check cluster capacity
        Env.getCurrentSystemInfo().checkClusterCapacity(db.getClusterName());
        // for original job, check quota
        // for delete job, not check
        if (!job.isSyncDeleteJob()) {
            db.checkDataSizeQuota();
        }

        // check if table is in restore process
        readLock();
        try {
            for (Long tblId : job.getIdToTableLoadInfo().keySet()) {
                Table tbl = db.getTableNullable(tblId);
                if (tbl != null && tbl.getType() == TableType.OLAP
                        && ((OlapTable) tbl).getState() == OlapTableState.RESTORE) {
                    throw new DdlException("Table " + tbl.getName() + " is in restore process. "
                            + "Can not load into it");
                }
            }
        } finally {
            readUnlock();
        }

        writeLock();
        try {
            unprotectAddLoadJob(job, false /* not replay */);
            Env.getCurrentEnv().getEditLog().logLoadStart(job);
        } finally {
            writeUnlock();
        }
        LOG.info("add load job. job: {}", job);
    }

    private LoadJob createLoadJob(LoadStmt stmt, EtlJobType etlJobType,
                                  Database db, long timestamp) throws DdlException {
        // get params
        String label = stmt.getLabel().getLabelName();
        List<DataDescription> dataDescriptions = stmt.getDataDescriptions();
        Map<String, String> properties = stmt.getProperties();

        // check params
        try {
            FeNameFormat.checkLabel(label);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        if (dataDescriptions == null || dataDescriptions.isEmpty()) {
            throw new DdlException("No data file in load statement.");
        }

        // create job
        LoadJob job = new LoadJob(label);
        job.setEtlJobType(etlJobType);
        job.setDbId(db.getId());
        job.setTimestamp(timestamp);
        job.setBrokerDesc(stmt.getBrokerDesc());

        // resource info
        if (ConnectContext.get() != null) {
            job.setResourceInfo(ConnectContext.get().toResourceCtx());
            job.setExecMemLimit(ConnectContext.get().getSessionVariable().getMaxExecMemByte());
        }

        // job properties
        if (properties != null) {
            if (properties.containsKey(LoadStmt.TIMEOUT_PROPERTY)) {
                try {
                    job.setTimeoutSecond(Integer.parseInt(properties.get(LoadStmt.TIMEOUT_PROPERTY)));
                } catch (NumberFormatException e) {
                    throw new DdlException("Timeout is not INT", e);
                }
            }

            if (properties.containsKey(LoadStmt.MAX_FILTER_RATIO_PROPERTY)) {
                try {
                    job.setMaxFilterRatio(Double.parseDouble(properties.get(LoadStmt.MAX_FILTER_RATIO_PROPERTY)));
                } catch (NumberFormatException e) {
                    throw new DdlException("Max filter ratio is not DOUBLE", e);
                }
            }

            if (properties.containsKey(LoadStmt.EXEC_MEM_LIMIT)) {
                try {
                    job.setExecMemLimit(Long.parseLong(properties.get(LoadStmt.EXEC_MEM_LIMIT)));
                } catch (NumberFormatException e) {
                    throw new DdlException("Execute memory limit is not Long", e);
                }
            }
        }

        // job table load info
        Map<Long, TableLoadInfo> idToTableLoadInfo = Maps.newHashMap();
        // tableId partitionId sources
        Map<Long, Map<Long, List<Source>>> tableToPartitionSources = Maps.newHashMap();
        for (DataDescription dataDescription : dataDescriptions) {
            // create source
            checkAndCreateSource(db, dataDescription, tableToPartitionSources, etlJobType);
            job.addTableName(dataDescription.getTableName());
        }
        for (Entry<Long, Map<Long, List<Source>>> tableEntry : tableToPartitionSources.entrySet()) {
            long tableId = tableEntry.getKey();
            Map<Long, List<Source>> partitionToSources = tableEntry.getValue();

            Map<Long, PartitionLoadInfo> idToPartitionLoadInfo = Maps.newHashMap();
            for (Entry<Long, List<Source>> partitionEntry : partitionToSources.entrySet()) {
                PartitionLoadInfo info = new PartitionLoadInfo(partitionEntry.getValue());
                idToPartitionLoadInfo.put(partitionEntry.getKey(), info);
            }
            idToTableLoadInfo.put(tableId, new TableLoadInfo(idToPartitionLoadInfo));
        }
        job.setIdToTableLoadInfo(idToTableLoadInfo);

        if (etlJobType == EtlJobType.BROKER) {
            BrokerFileGroupAggInfo sourceInfo = new BrokerFileGroupAggInfo();
            for (DataDescription dataDescription : dataDescriptions) {
                BrokerFileGroup fileGroup = new BrokerFileGroup(dataDescription);
                fileGroup.parse(db, dataDescription);
                sourceInfo.addFileGroup(fileGroup);
            }
            job.setPullLoadSourceInfo(sourceInfo);
            LOG.info("source info is {}", sourceInfo);
        }

        if (etlJobType == EtlJobType.MINI) {
            // mini etl tasks
            Map<Long, MiniEtlTaskInfo> idToEtlTask = Maps.newHashMap();
            long etlTaskId = 0;

            for (DataDescription dataDescription : dataDescriptions) {
                String tableName = dataDescription.getTableName();
                OlapTable table = db.getOlapTableOrDdlException(tableName);

                table.readLock();
                try {
                    TNetworkAddress beAddress = dataDescription.getBeAddr();
                    Backend backend = Env.getCurrentSystemInfo().getBackendWithBePort(beAddress.getHostname(),
                            beAddress.getPort());
                    if (!Env.getCurrentSystemInfo().checkBackendLoadAvailable(backend.getId())) {
                        throw new DdlException("Etl backend is null or not available");
                    }

                    MiniEtlTaskInfo taskInfo = new MiniEtlTaskInfo(etlTaskId++, backend.getId(), table.getId());
                    idToEtlTask.put(taskInfo.getId(), taskInfo);
                } finally {
                    table.readUnlock();
                }
            }

            job.setMiniEtlTasks(idToEtlTask);
            job.setPriority(TPriority.HIGH);

            if (job.getTimeoutSecond() == 0) {
                // set default timeout
                job.setTimeoutSecond(Config.mini_load_default_timeout_second);
            }

        } else if (etlJobType == EtlJobType.HADOOP) {
            // hadoop dpp cluster config
            // default dpp config
            DppConfig dppConfig = dppDefaultConfig.getCopiedDppConfig();

            // get dpp config by cluster
            // 1. from user
            String cluster = stmt.getCluster();
            if (cluster == null && properties != null) {
                cluster = properties.get(LoadStmt.CLUSTER_PROPERTY);
            }

            Pair<String, DppConfig> clusterInfo = Env.getCurrentEnv().getAuth().getLoadClusterInfo(
                    stmt.getUser(), cluster);
            cluster = clusterInfo.first;
            DppConfig clusterConfig = clusterInfo.second;

            // 2. from system
            if (cluster == null || clusterConfig == null) {
                if (cluster == null) {
                    cluster = Config.dpp_default_cluster;
                }

                clusterConfig = clusterToDppConfig.get(cluster);
                if (clusterConfig == null) {
                    throw new DdlException("Load cluster[" + cluster + "] does not exist");
                }
            }

            dppConfig.update(clusterConfig);

            try {
                // parse user define hadoop and bos configs
                dppConfig.updateHadoopConfigs(properties);

                // check and set cluster info
                dppConfig.check();
                job.setClusterInfo(cluster, dppConfig);
                job.setPriority(dppConfig.getPriority());
            } catch (LoadException e) {
                throw new DdlException(e.getMessage());
            }

            if (job.getTimeoutSecond() == 0) {
                // set default timeout
                job.setTimeoutSecond(Config.hadoop_load_default_timeout_second);
            }
            for (DataDescription dataDescription : dataDescriptions) {
                if (dataDescription.getMergeType() != LoadTask.MergeType.APPEND) {
                    throw new DdlException("MERGE OR DELETE is not supported in hadoop load.");
                }
            }
        } else if (etlJobType == EtlJobType.BROKER) {
            if (job.getTimeoutSecond() == 0) {
                // set default timeout
                job.setTimeoutSecond(Config.broker_load_default_timeout_second);
            }
        } else if (etlJobType == EtlJobType.INSERT) {
            job.setPriority(TPriority.HIGH);
            if (job.getTimeoutSecond() == 0) {
                // set default timeout
                job.setTimeoutSecond(Config.insert_load_default_timeout_second);
            }
        }

        // job id
        job.setId(Env.getCurrentEnv().getNextId());

        return job;
    }

    /*
     * This is only used for hadoop load
     */
    public static void checkAndCreateSource(Database db, DataDescription dataDescription,
            Map<Long, Map<Long, List<Source>>> tableToPartitionSources, EtlJobType jobType) throws DdlException {
        Source source = new Source(dataDescription.getFilePaths());
        long tableId = -1;
        Set<Long> sourcePartitionIds = Sets.newHashSet();

        // source column names and partitions
        String tableName = dataDescription.getTableName();
        Map<String, Pair<String, List<String>>> columnToFunction = null;

        OlapTable table = db.getOlapTableOrDdlException(tableName);
        tableId = table.getId();

        table.readLock();
        try {
            if (table.getPartitionInfo().isMultiColumnPartition() && jobType == EtlJobType.HADOOP) {
                throw new DdlException("Load by hadoop cluster does not support table with multi partition columns."
                        + " Table: " + table.getName() + ". Try using broker load. See 'help broker load;'");
            }

            // check partition
            if (dataDescription.getPartitionNames() != null
                    && table.getPartitionInfo().getType() == PartitionType.UNPARTITIONED) {
                ErrorReport.reportDdlException(ErrorCode.ERR_PARTITION_CLAUSE_NO_ALLOWED);
            }

            if (table.getState() == OlapTableState.RESTORE) {
                throw new DdlException("Table [" + tableName + "] is under restore");
            }

            if (table.getKeysType() != KeysType.AGG_KEYS && dataDescription.isNegative()) {
                throw new DdlException("Load for AGG_KEYS table should not specify NEGATIVE");
            }

            // get table schema
            List<Column> baseSchema = table.getBaseSchema(false);
            // fill the column info if user does not specify them
            dataDescription.fillColumnInfoIfNotSpecified(baseSchema);

            // source columns
            List<String> columnNames = Lists.newArrayList();
            List<String> assignColumnNames = Lists.newArrayList();
            if (dataDescription.getFileFieldNames() != null) {
                assignColumnNames.addAll(dataDescription.getFileFieldNames());
                if (dataDescription.getColumnsFromPath() != null) {
                    assignColumnNames.addAll(dataDescription.getColumnsFromPath());
                }
            }
            if (assignColumnNames.isEmpty()) {
                // use table columns
                for (Column column : baseSchema) {
                    columnNames.add(column.getName());
                }
            } else {
                // convert column to schema format
                for (String assignCol : assignColumnNames) {
                    if (table.getColumn(assignCol) != null) {
                        columnNames.add(table.getColumn(assignCol).getName());
                    } else {
                        columnNames.add(assignCol);
                    }
                }
            }
            source.setColumnNames(columnNames);

            // check default value
            Map<String, Pair<String, List<String>>> columnToHadoopFunction
                    = dataDescription.getColumnToHadoopFunction();
            List<ImportColumnDesc> parsedColumnExprList = dataDescription.getParsedColumnExprList();
            Map<String, Expr> parsedColumnExprMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            for (ImportColumnDesc importColumnDesc : parsedColumnExprList) {
                parsedColumnExprMap.put(importColumnDesc.getColumnName(), importColumnDesc.getExpr());
            }
            for (Column column : baseSchema) {
                String columnName = column.getName();
                if (columnNames.contains(columnName)) {
                    continue;
                }

                if (parsedColumnExprMap.containsKey(columnName)) {
                    continue;
                }

                if (column.getDefaultValue() != null || column.isAllowNull()) {
                    continue;
                }

                throw new DdlException("Column has no default value. column: " + columnName);
            }

            // check negative for sum aggregate type
            if (dataDescription.isNegative()) {
                for (Column column : baseSchema) {
                    if (!column.isKey() && column.getAggregationType() != AggregateType.SUM) {
                        throw new DdlException("Column is not SUM AggregateType. column:" + column.getName());
                    }
                }
            }

            // check hll
            for (Column column : baseSchema) {
                if (column.getDataType() == PrimitiveType.HLL) {
                    if (columnToHadoopFunction != null && !columnToHadoopFunction.containsKey(column.getName())) {
                        throw new DdlException("Hll column is not assigned. column:" + column.getName());
                    }
                }
            }

            // check mapping column exist in table
            // check function
            // convert mapping column and func arg columns to schema format

            // When doing schema change, there may have some 'shadow' columns, with prefix '__doris_shadow_' in
            // their names. These columns are invisible to user, but we need to generate data for these columns.
            // So we add column mappings for these column.
            // eg1:
            // base schema is (A, B, C), and B is under schema change,
            // so there will be a shadow column: '__doris_shadow_B'
            // So the final column mapping should looks like: (A, B, C, __doris_shadow_B = substitute(B));
            for (Column column : table.getFullSchema()) {
                if (column.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PRFIX)) {
                    String originCol = column.getNameWithoutPrefix(SchemaChangeHandler.SHADOW_NAME_PRFIX);
                    if (parsedColumnExprMap.containsKey(originCol)) {
                        Expr mappingExpr = parsedColumnExprMap.get(originCol);
                        if (mappingExpr != null) {
                            /*
                             * eg:
                             * (A, C) SET (B = func(xx))
                             * ->
                             * (A, C) SET (B = func(xx), __doris_shadow_B = func(xxx))
                             */
                            if (columnToHadoopFunction.containsKey(originCol)) {
                                columnToHadoopFunction.put(column.getName(), columnToHadoopFunction.get(originCol));
                            }
                            ImportColumnDesc importColumnDesc = new ImportColumnDesc(column.getName(), mappingExpr);
                            parsedColumnExprList.add(importColumnDesc);
                        } else {
                            /*
                             * eg:
                             * (A, B, C)
                             * ->
                             * (A, B, C) SET (__doris_shadow_B = substitute(B))
                             */
                            columnToHadoopFunction.put(column.getName(),
                                    Pair.of("substitute", Lists.newArrayList(originCol)));
                            ImportColumnDesc importColumnDesc
                                    = new ImportColumnDesc(column.getName(), new SlotRef(null, originCol));
                            parsedColumnExprList.add(importColumnDesc);
                        }
                    } else {
                        /*
                         * There is a case that if user does not specify the related origin column, eg:
                         * COLUMNS (A, C), and B is not specified, but B is being modified
                         * so there is a shadow column '__doris_shadow_B'.
                         * We can not just add a mapping function "__doris_shadow_B = substitute(B)",
                         * because Doris can not find column B.
                         * In this case, __doris_shadow_B can use its default value,
                         * so no need to add it to column mapping
                         */
                        // do nothing
                    }

                } else if (!column.isVisible()) {
                    /*
                     *  For batch delete table add hidden column __DORIS_DELETE_SIGN__ to columns
                     * eg:
                     * (A, B, C)
                     * ->
                     * (A, B, C) SET (__DORIS_DELETE_SIGN__ = 0)
                     */
                    columnToHadoopFunction.put(column.getName(), Pair.of("default_value",
                            Lists.newArrayList(column.getDefaultValue())));
                    ImportColumnDesc importColumnDesc = null;
                    try {
                        importColumnDesc = new ImportColumnDesc(column.getName(),
                                new FunctionCallExpr("default_value", Arrays.asList(column.getDefaultValueExpr())));
                    } catch (AnalysisException e) {
                        throw new DdlException(e.getMessage());
                    }
                    parsedColumnExprList.add(importColumnDesc);
                }
            }

            LOG.debug("after add shadow column. parsedColumnExprList: {}, columnToHadoopFunction: {}",
                    parsedColumnExprList, columnToHadoopFunction);

            Map<String, String> columnNameMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            for (String columnName : columnNames) {
                columnNameMap.put(columnName, columnName);
            }

            // validate hadoop functions
            if (columnToHadoopFunction != null) {
                columnToFunction = Maps.newHashMap();
                for (Entry<String, Pair<String, List<String>>> entry : columnToHadoopFunction.entrySet()) {
                    String mappingColumnName = entry.getKey();
                    Column mappingColumn = table.getColumn(mappingColumnName);
                    if (mappingColumn == null) {
                        throw new DdlException("Mapping column is not in table. column: " + mappingColumnName);
                    }

                    Pair<String, List<String>> function = entry.getValue();
                    try {
                        DataDescription.validateMappingFunction(function.first, function.second, columnNameMap,
                                mappingColumn, dataDescription.isHadoopLoad());
                    } catch (AnalysisException e) {
                        throw new DdlException(e.getMessage());
                    }

                    columnToFunction.put(mappingColumn.getName(), function);
                }
            }

            // partitions of this source
            OlapTable olapTable = table;
            PartitionNames partitionNames = dataDescription.getPartitionNames();
            if (partitionNames == null) {
                for (Partition partition : olapTable.getPartitions()) {
                    sourcePartitionIds.add(partition.getId());
                }
            } else {
                for (String partitionName : partitionNames.getPartitionNames()) {
                    Partition partition = olapTable.getPartition(partitionName, partitionNames.isTemp());
                    if (partition == null) {
                        throw new DdlException("Partition [" + partitionName + "] does not exist");
                    }
                    sourcePartitionIds.add(partition.getId());
                }
            }
        } finally {
            table.readUnlock();
        }

        // column separator
        String columnSeparator = dataDescription.getColumnSeparator();
        if (!Strings.isNullOrEmpty(columnSeparator)) {
            source.setColumnSeparator(columnSeparator);
        }

        // line delimiter
        String lineDelimiter = dataDescription.getLineDelimiter();
        if (!Strings.isNullOrEmpty(lineDelimiter)) {
            source.setLineDelimiter(lineDelimiter);
        }

        // source negative
        source.setNegative(dataDescription.isNegative());

        // column mapping functions
        if (columnToFunction != null) {
            source.setColumnToFunction(columnToFunction);
        }

        // add source to table partition map
        Map<Long, List<Source>> partitionToSources = null;
        if (tableToPartitionSources.containsKey(tableId)) {
            partitionToSources = tableToPartitionSources.get(tableId);
        } else {
            partitionToSources = Maps.newHashMap();
            tableToPartitionSources.put(tableId, partitionToSources);
        }
        for (long partitionId : sourcePartitionIds) {
            List<Source> sources = null;
            if (partitionToSources.containsKey(partitionId)) {
                sources = partitionToSources.get(partitionId);
            } else {
                sources = new ArrayList<Source>();
                partitionToSources.put(partitionId, sources);
            }
            sources.add(source);
        }
    }

    /**
     * When doing schema change, there may have some 'shadow' columns, with prefix '__doris_shadow_' in
     * their names. These columns are invisible to user, but we need to generate data for these columns.
     * So we add column mappings for these column.
     * eg1:
     * base schema is (A, B, C), and B is under schema change, so there will be a shadow column: '__doris_shadow_B'
     * So the final column mapping should looks like: (A, B, C, __doris_shadow_B = substitute(B));
     */
    public static List<ImportColumnDesc> getSchemaChangeShadowColumnDesc(Table tbl, Map<String, Expr> columnExprMap) {
        List<ImportColumnDesc> shadowColumnDescs = Lists.newArrayList();
        for (Column column : tbl.getFullSchema()) {
            if (!column.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PRFIX)) {
                continue;
            }

            String originCol = column.getNameWithoutPrefix(SchemaChangeHandler.SHADOW_NAME_PRFIX);
            if (columnExprMap.containsKey(originCol)) {
                Expr mappingExpr = columnExprMap.get(originCol);
                if (mappingExpr != null) {
                    /*
                     * eg:
                     * (A, C) SET (B = func(xx))
                     * ->
                     * (A, C) SET (B = func(xx), __doris_shadow_B = func(xx))
                     */
                    ImportColumnDesc importColumnDesc = new ImportColumnDesc(column.getName(), mappingExpr);
                    shadowColumnDescs.add(importColumnDesc);
                } else {
                    /*
                     * eg:
                     * (A, B, C)
                     * ->
                     * (A, B, C) SET (__doris_shadow_B = B)
                     */
                    ImportColumnDesc importColumnDesc = new ImportColumnDesc(column.getName(),
                            new SlotRef(null, originCol));
                    shadowColumnDescs.add(importColumnDesc);
                }
            } else {
                /*
                 * There is a case that if user does not specify the related origin column, eg:
                 * COLUMNS (A, C), and B is not specified, but B is being modified
                 * so there is a shadow column '__doris_shadow_B'.
                 * We can not just add a mapping function "__doris_shadow_B = substitute(B)",
                 * because Doris can not find column B.
                 * In this case, __doris_shadow_B can use its default value, so no need to add it to column mapping
                 */
                // do nothing
            }
        }
        return shadowColumnDescs;
    }

    /*
     * used for spark load job
     * not init slot desc and analyze exprs
     */
    public static void initColumns(Table tbl, List<ImportColumnDesc> columnExprs,
            Map<String, Pair<String, List<String>>> columnToHadoopFunction) throws UserException {
        initColumns(tbl, columnExprs, columnToHadoopFunction, null, null, null, null, null, null, null, false, false);
    }

    /*
     * This function should be used for broker load v2 and stream load.
     * And it must be called in same db lock when planing.
     */
    public static void initColumns(Table tbl, LoadTaskInfo.ImportColumnDescs columnDescs,
            Map<String, Pair<String, List<String>>> columnToHadoopFunction, Map<String, Expr> exprsByName,
            Analyzer analyzer, TupleDescriptor srcTupleDesc, Map<String, SlotDescriptor> slotDescByName,
            List<Integer> srcSlotIds, TFileFormatType formatType, List<String> hiddenColumns, boolean useVectorizedLoad)
            throws UserException {
        rewriteColumns(columnDescs);
        initColumns(tbl, columnDescs.descs, columnToHadoopFunction, exprsByName, analyzer, srcTupleDesc, slotDescByName,
                srcSlotIds, formatType, hiddenColumns, useVectorizedLoad, true);
    }

    /*
     * This function will do followings:
     * 1. fill the column exprs if user does not specify any column or column mapping.
     * 2. For not specified columns, check if they have default value.
     * 3. Add any shadow columns if have.
     * 4. validate hadoop functions
     * 5. init slot descs and expr map for load plan
     */
    private static void initColumns(Table tbl, List<ImportColumnDesc> columnExprs,
            Map<String, Pair<String, List<String>>> columnToHadoopFunction, Map<String, Expr> exprsByName,
            Analyzer analyzer, TupleDescriptor srcTupleDesc, Map<String, SlotDescriptor> slotDescByName,
            List<Integer> srcSlotIds, TFileFormatType formatType, List<String> hiddenColumns, boolean useVectorizedLoad,
            boolean needInitSlotAndAnalyzeExprs) throws UserException {
        // We make a copy of the columnExprs so that our subsequent changes
        // to the columnExprs will not affect the original columnExprs.
        // skip the mapping columns not exist in schema
        // eg: the origin column list is:
        //          (k1, k2, tmpk3 = k1 + k2, k3 = tmpk3)
        //     after calling rewriteColumns(), it will become
        //          (k1, k2, tmpk3 = k1 + k2, k3 = k1 + k2)
        //     so "tmpk3 = k1 + k2" is not needed anymore, we can skip it.
        List<ImportColumnDesc> copiedColumnExprs = new ArrayList<>();
        for (ImportColumnDesc importColumnDesc : columnExprs) {
            String mappingColumnName = importColumnDesc.getColumnName();
            if (importColumnDesc.isColumn() || tbl.getColumn(mappingColumnName) != null) {
                copiedColumnExprs.add(importColumnDesc);
            }
        }
        // check whether the OlapTable has sequenceCol
        boolean hasSequenceCol = false;
        if (tbl instanceof OlapTable && ((OlapTable) tbl).hasSequenceCol()) {
            hasSequenceCol = true;
        }

        // If user does not specify the file field names, generate it by using base schema of table.
        // So that the following process can be unified
        boolean specifyFileFieldNames = copiedColumnExprs.stream().anyMatch(p -> p.isColumn());
        if (!specifyFileFieldNames) {
            List<Column> columns = tbl.getBaseSchema(false);
            for (Column column : columns) {
                // columnExprs has sequence column, don't need to generate the sequence column
                if (hasSequenceCol && column.isSequenceColumn()) {
                    continue;
                }
                ImportColumnDesc columnDesc = new ImportColumnDesc(column.getName());
                LOG.debug("add base column {} to stream load task", column.getName());
                copiedColumnExprs.add(columnDesc);
            }
            if (hiddenColumns != null) {
                for (String columnName : hiddenColumns) {
                    Column column = tbl.getColumn(columnName);
                    if (column != null && !column.isVisible()) {
                        ImportColumnDesc columnDesc = new ImportColumnDesc(column.getName());
                        LOG.debug("add hidden column {} to stream load task", column.getName());
                        copiedColumnExprs.add(columnDesc);
                    }
                }
            }
        }
        // generate a map for checking easily
        Map<String, Expr> columnExprMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (ImportColumnDesc importColumnDesc : copiedColumnExprs) {
            columnExprMap.put(importColumnDesc.getColumnName(), importColumnDesc.getExpr());
        }

        // check default value
        for (Column column : tbl.getBaseSchema()) {
            String columnName = column.getName();
            if (columnExprMap.containsKey(columnName)) {
                continue;
            }
            if (column.getDefaultValue() != null || column.isAllowNull()) {
                continue;
            }
            throw new DdlException("Column has no default value. column: " + columnName);
        }

        // get shadow column desc when table schema change
        copiedColumnExprs.addAll(getSchemaChangeShadowColumnDesc(tbl, columnExprMap));

        // validate hadoop functions
        if (columnToHadoopFunction != null) {
            Map<String, String> columnNameMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            for (ImportColumnDesc importColumnDesc : copiedColumnExprs) {
                if (importColumnDesc.isColumn()) {
                    columnNameMap.put(importColumnDesc.getColumnName(), importColumnDesc.getColumnName());
                }
            }
            for (Entry<String, Pair<String, List<String>>> entry : columnToHadoopFunction.entrySet()) {
                String mappingColumnName = entry.getKey();
                Column mappingColumn = tbl.getColumn(mappingColumnName);
                if (mappingColumn == null) {
                    throw new DdlException("Mapping column is not in table. column: " + mappingColumnName);
                }
                Pair<String, List<String>> function = entry.getValue();
                try {
                    DataDescription.validateMappingFunction(function.first, function.second, columnNameMap,
                            mappingColumn, false);
                } catch (AnalysisException e) {
                    throw new DdlException(e.getMessage());
                }
            }
        }

        if (!needInitSlotAndAnalyzeExprs) {
            return;
        }
        Set<String> exprSrcSlotName = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (ImportColumnDesc importColumnDesc : copiedColumnExprs) {
            if (importColumnDesc.isColumn()) {
                continue;
            }
            List<SlotRef> slots = Lists.newArrayList();
            importColumnDesc.getExpr().collect(SlotRef.class, slots);
            for (SlotRef slot : slots) {
                String slotColumnName = slot.getColumnName();
                exprSrcSlotName.add(slotColumnName);
            }
        }
        // excludedColumns is columns that should be varchar type
        Set<String> excludedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        // init slot desc add expr map, also transform hadoop functions
        for (ImportColumnDesc importColumnDesc : copiedColumnExprs) {
            // make column name case match with real column name
            String columnName = importColumnDesc.getColumnName();
            Column tblColumn = tbl.getColumn(columnName);
            String realColName;
            if (tblColumn == null || tblColumn.getName() == null || importColumnDesc.getExpr() == null) {
                realColName = columnName;
            } else {
                realColName = tblColumn.getName();
            }
            if (importColumnDesc.getExpr() != null) {
                Expr expr = transformHadoopFunctionExpr(tbl, realColName, importColumnDesc.getExpr());
                exprsByName.put(realColName, expr);
            } else {
                SlotDescriptor slotDesc = analyzer.getDescTbl().addSlotDescriptor(srcTupleDesc);
                // only support parquet format now
                if (useVectorizedLoad  && formatType == TFileFormatType.FORMAT_PARQUET
                        && tblColumn != null) {
                    // in vectorized load
                    // example: k1 is DATETIME in source file, and INT in schema, mapping exper is k1=year(k1)
                    // we can not determine whether to use the type in the schema or the type inferred from expr
                    // so use varchar type as before
                    if (exprSrcSlotName.contains(columnName)) {
                        // columns in expr args should be varchar type
                        slotDesc.setType(ScalarType.createType(PrimitiveType.VARCHAR));
                        slotDesc.setColumn(new Column(realColName, PrimitiveType.VARCHAR));
                        excludedColumns.add(realColName);
                        // example k1, k2 = k1 + 1, k1 is not nullable, k2 is nullable
                        // so we can not determine columns in expr args whether not nullable or nullable
                        // slot in expr args use nullable as before
                        slotDesc.setIsNullable(true);
                    } else {
                        // columns from files like parquet files can be parsed as the type in table schema
                        slotDesc.setType(tblColumn.getType());
                        slotDesc.setColumn(new Column(realColName, tblColumn.getType()));
                        // non-nullable column is allowed in vectorized load with parquet format
                        slotDesc.setIsNullable(tblColumn.isAllowNull());
                    }
                } else {
                    // columns default be varchar type
                    slotDesc.setType(ScalarType.createType(PrimitiveType.VARCHAR));
                    slotDesc.setColumn(new Column(realColName, PrimitiveType.VARCHAR));
                    // ISSUE A: src slot should be nullable even if the column is not nullable.
                    // because src slot is what we read from file, not represent to real column value.
                    // If column is not nullable, error will be thrown when filling the dest slot,
                    // which is not nullable.
                    slotDesc.setIsNullable(true);
                }
                slotDesc.setIsMaterialized(true);
                srcSlotIds.add(slotDesc.getId().asInt());
                slotDescByName.put(realColName, slotDesc);
            }
        }
        /*
         * The extension column of the materialized view is added to the expression evaluation of load
         * To avoid nested expressions. eg : column(a, tmp_c, c = expr(tmp_c)) ,
         * __doris_materialized_view_bitmap_union_c need be analyzed after exprsByName
         * So the columns of the materialized view are stored separately here
         */
        Map<String, Expr> mvDefineExpr = Maps.newHashMap();
        for (Column column : tbl.getFullSchema()) {
            if (column.getDefineExpr() != null) {
                mvDefineExpr.put(column.getName(), column.getDefineExpr());
            }
        }

        LOG.debug("slotDescByName: {}, exprsByName: {}, mvDefineExpr: {}", slotDescByName, exprsByName, mvDefineExpr);
        // we only support parquet format now
        // use implicit deduction to convert columns
        // that are not in the doris table from varchar to a more appropriate type
        if (useVectorizedLoad && formatType == TFileFormatType.FORMAT_PARQUET) {
            // analyze all exprs
            Map<String, Expr> cloneExprsByName = Maps.newHashMap(exprsByName);
            Map<String, Expr> cloneMvDefineExpr = Maps.newHashMap(mvDefineExpr);
            analyzeAllExprs(tbl, analyzer, cloneExprsByName, cloneMvDefineExpr, slotDescByName, useVectorizedLoad);
            // columns that only exist in mapping expr args, replace type with inferred from exprs,
            // if there are more than one, choose the last except varchar type
            // for example:
            // k1 involves two mapping expr args: year(k1), t1=k1, k1's varchar type will be replaced by DATETIME
            replaceVarcharWithCastType(cloneExprsByName, srcTupleDesc, excludedColumns);
        }

        // in vectorized load, reanalyze exprs with castExpr type
        // otherwise analyze exprs with varchar type
        analyzeAllExprs(tbl, analyzer, exprsByName, mvDefineExpr, slotDescByName, useVectorizedLoad);
        LOG.debug("after init column, exprMap: {}", exprsByName);
    }

    private static void analyzeAllExprs(Table tbl, Analyzer analyzer, Map<String, Expr> exprsByName,
                                            Map<String, Expr> mvDefineExpr, Map<String, SlotDescriptor> slotDescByName,
                                            boolean useVectorizedLoad) throws UserException {
        // analyze all exprs
        for (Map.Entry<String, Expr> entry : exprsByName.entrySet()) {
            ExprSubstitutionMap smap = new ExprSubstitutionMap();
            List<SlotRef> slots = Lists.newArrayList();
            entry.getValue().collect(SlotRef.class, slots);
            for (SlotRef slot : slots) {
                SlotDescriptor slotDesc = slotDescByName.get(slot.getColumnName());
                if (slotDesc == null) {
                    if (entry.getKey() != null) {
                        if (entry.getKey().equalsIgnoreCase(Column.DELETE_SIGN)) {
                            throw new UserException("unknown reference column in DELETE ON clause:"
                                + slot.getColumnName());
                        } else if (entry.getKey().equalsIgnoreCase(Column.SEQUENCE_COL)) {
                            throw new UserException("unknown reference column in ORDER BY clause:"
                                + slot.getColumnName());
                        }
                    }
                    throw new UserException("unknown reference column, column=" + entry.getKey()
                            + ", reference=" + slot.getColumnName());
                }
                smap.getLhs().add(slot);
                smap.getRhs().add(new SlotRef(slotDesc));
            }
            Expr expr = entry.getValue().clone(smap);
            expr.analyze(analyzer);

            // check if contain aggregation
            List<FunctionCallExpr> funcs = Lists.newArrayList();
            expr.collect(FunctionCallExpr.class, funcs);
            for (FunctionCallExpr fn : funcs) {
                if (fn.isAggregateFunction()) {
                    throw new AnalysisException("Don't support aggregation function in load expression");
                }
            }
            exprsByName.put(entry.getKey(), expr);
        }

        for (Map.Entry<String, Expr> entry : mvDefineExpr.entrySet()) {
            ExprSubstitutionMap smap = new ExprSubstitutionMap();
            List<SlotRef> slots = Lists.newArrayList();
            entry.getValue().collect(SlotRef.class, slots);
            for (SlotRef slot : slots) {
                if (slotDescByName.get(slot.getColumnName()) != null) {
                    smap.getLhs().add(slot);
                    smap.getRhs().add(new CastExpr(tbl.getColumn(slot.getColumnName()).getType(),
                            new SlotRef(slotDescByName.get(slot.getColumnName()))));
                } else if (exprsByName.get(slot.getColumnName()) != null) {
                    smap.getLhs().add(slot);
                    smap.getRhs().add(new CastExpr(tbl.getColumn(slot.getColumnName()).getType(),
                            exprsByName.get(slot.getColumnName())));
                } else {
                    if (entry.getKey().equalsIgnoreCase(Column.DELETE_SIGN)) {
                        throw new UserException("unknown reference column in DELETE ON clause:" + slot.getColumnName());
                    } else if (entry.getKey().equalsIgnoreCase(Column.SEQUENCE_COL)) {
                        throw new UserException("unknown reference column in ORDER BY clause:" + slot.getColumnName());
                    } else {
                        throw new UserException("unknown reference column, column=" + entry.getKey()
                                + ", reference=" + slot.getColumnName());
                    }
                }
            }
            Expr expr = entry.getValue().clone(smap);
            expr.analyze(analyzer);

            exprsByName.put(entry.getKey(), expr);
        }
    }

    /**
     * columns that only exist in mapping expr args, replace type with inferred from exprs.
     *
     * @param excludedColumns columns that the type should not be inferred from expr.
     *                         1. column exists in both schema and expr args.
     */
    private static void replaceVarcharWithCastType(Map<String, Expr> exprsByName, TupleDescriptor srcTupleDesc,
                                               Set<String> excludedColumns) throws UserException {
        // if there are more than one, choose the last except varchar type.
        // for example:
        // k1 involves two mapping expr args: year(k1), t1=k1, k1's varchar type will be replaced by DATETIME.
        for (Map.Entry<String, Expr> entry : exprsByName.entrySet()) {
            List<CastExpr> casts = Lists.newArrayList();
            // exclude explicit cast. for example: cast(k1 as date)
            entry.getValue().collect(Expr.IS_VARCHAR_SLOT_REF_IMPLICIT_CAST, casts);
            if (casts.isEmpty()) {
                continue;
            }

            for (CastExpr cast : casts) {
                Expr child = cast.getChild(0);
                Type type = cast.getType();
                if (type.isVarchar()) {
                    continue;
                }

                SlotRef slotRef = (SlotRef) child;
                String columnName = slotRef.getColumn().getName();
                if (excludedColumns.contains(columnName)) {
                    continue;
                }

                // replace src slot desc with cast return type
                int slotId = slotRef.getSlotId().asInt();
                SlotDescriptor srcSlotDesc = srcTupleDesc.getSlot(slotId);
                if (srcSlotDesc == null) {
                    throw new UserException("Unknown source slot descriptor. id: " + slotId);
                }
                srcSlotDesc.setType(type);
                srcSlotDesc.setColumn(new Column(columnName, type));
            }
        }
    }

    public static void rewriteColumns(LoadTaskInfo.ImportColumnDescs columnDescs) {
        if (columnDescs.isColumnDescsRewrited) {
            return;
        }

        Map<String, Expr> derivativeColumns = new HashMap<>();
        // find and rewrite the derivative columns
        // e.g. (v1,v2=v1+1,v3=v2+1) --> (v1, v2=v1+1, v3=v1+1+1)
        // 1. find the derivative columns
        for (ImportColumnDesc importColumnDesc : columnDescs.descs) {
            if (!importColumnDesc.isColumn()) {
                if (importColumnDesc.getExpr() instanceof SlotRef) {
                    String columnName = ((SlotRef) importColumnDesc.getExpr()).getColumnName();
                    if (derivativeColumns.containsKey(columnName)) {
                        importColumnDesc.setExpr(derivativeColumns.get(columnName));
                    }
                } else {
                    recursiveRewrite(importColumnDesc.getExpr(), derivativeColumns);
                }
                derivativeColumns.put(importColumnDesc.getColumnName(), importColumnDesc.getExpr());
            }
        }

        columnDescs.isColumnDescsRewrited = true;
    }

    private static void recursiveRewrite(Expr expr, Map<String, Expr> derivativeColumns) {
        if (CollectionUtils.isEmpty(expr.getChildren())) {
            return;
        }
        for (int i = 0; i < expr.getChildren().size(); i++) {
            Expr e = expr.getChild(i);
            if (e instanceof SlotRef) {
                String columnName = ((SlotRef) e).getColumnName();
                if (derivativeColumns.containsKey(columnName)) {
                    expr.setChild(i, derivativeColumns.get(columnName));
                }
            } else {
                recursiveRewrite(e, derivativeColumns);
            }
        }
    }

    /**
     * This method is used to transform hadoop function.
     * The hadoop function includes: replace_value, strftime, time_format, alignment_timestamp, default_value, now.
     * It rewrites those function with real function name and param.
     * For the other function, the expr only go through this function and the origin expr is returned.
     *
     * @param columnName
     * @param originExpr
     * @return
     * @throws UserException
     */
    private static Expr transformHadoopFunctionExpr(Table tbl, String columnName, Expr originExpr)
            throws UserException {
        Column column = tbl.getColumn(columnName);
        if (column == null) {
            // the unknown column will be checked later.
            return originExpr;
        }

        // To compatible with older load version
        if (originExpr instanceof FunctionCallExpr) {
            FunctionCallExpr funcExpr = (FunctionCallExpr) originExpr;
            String funcName = funcExpr.getFnName().getFunction();

            if (funcName.equalsIgnoreCase("replace_value")) {
                List<Expr> exprs = Lists.newArrayList();
                SlotRef slotRef = new SlotRef(null, columnName);
                // We will convert this to IF(`col` != child0, `col`, child1),
                // because we need the if return type equal to `col`, we use NE

                /*
                 * We will convert this based on different cases:
                 * case 1: k1 = replace_value(null, anyval);
                 *     to: k1 = if (k1 is not null, k1, anyval);
                 *
                 * case 2: k1 = replace_value(anyval1, anyval2);
                 *     to: k1 = if (k1 is not null, if(k1 != anyval1, k1, anyval2), null);
                 */
                if (funcExpr.getChild(0) instanceof NullLiteral) {
                    // case 1
                    exprs.add(new IsNullPredicate(slotRef, true));
                    exprs.add(slotRef);
                    if (funcExpr.hasChild(1)) {
                        exprs.add(funcExpr.getChild(1));
                    } else {
                        if (column.getDefaultValue() != null) {
                            if (column.getDefaultValueExprDef() != null) {
                                exprs.add(column.getDefaultValueExpr());
                            } else {
                                exprs.add(new StringLiteral(column.getDefaultValue()));
                            }
                        } else {
                            if (column.isAllowNull()) {
                                exprs.add(NullLiteral.create(Type.VARCHAR));
                            } else {
                                throw new UserException("Column(" + columnName + ") has no default value.");
                            }
                        }
                    }
                } else {
                    // case 2
                    exprs.add(new IsNullPredicate(slotRef, true));
                    List<Expr> innerIfExprs = Lists.newArrayList();
                    innerIfExprs.add(new BinaryPredicate(BinaryPredicate.Operator.NE, slotRef, funcExpr.getChild(0)));
                    innerIfExprs.add(slotRef);
                    if (funcExpr.hasChild(1)) {
                        innerIfExprs.add(funcExpr.getChild(1));
                    } else {
                        if (column.getDefaultValue() != null) {
                            if (column.getDefaultValueExprDef() != null) {
                                innerIfExprs.add(column.getDefaultValueExpr());
                            } else {
                                innerIfExprs.add(new StringLiteral(column.getDefaultValue()));
                            }
                        } else {
                            if (column.isAllowNull()) {
                                innerIfExprs.add(NullLiteral.create(Type.VARCHAR));
                            } else {
                                throw new UserException("Column(" + columnName + ") has no default value.");
                            }
                        }
                    }
                    FunctionCallExpr innerIfFn = new FunctionCallExpr("if", innerIfExprs);
                    exprs.add(innerIfFn);
                    exprs.add(NullLiteral.create(Type.VARCHAR));
                }

                LOG.debug("replace_value expr: {}", exprs);
                FunctionCallExpr newFn = new FunctionCallExpr("if", exprs);
                return newFn;
            } else if (funcName.equalsIgnoreCase("strftime")) {
                // FROM_UNIXTIME(val)
                FunctionName fromUnixName = new FunctionName("FROM_UNIXTIME");
                List<Expr> fromUnixArgs = Lists.newArrayList(funcExpr.getChild(1));
                FunctionCallExpr fromUnixFunc = new FunctionCallExpr(
                        fromUnixName, new FunctionParams(false, fromUnixArgs));

                return fromUnixFunc;
            } else if (funcName.equalsIgnoreCase("time_format")) {
                // DATE_FORMAT(STR_TO_DATE(dt_str, dt_fmt))
                FunctionName strToDateName = new FunctionName("STR_TO_DATE");
                List<Expr> strToDateExprs = Lists.newArrayList(funcExpr.getChild(2), funcExpr.getChild(1));
                FunctionCallExpr strToDateFuncExpr = new FunctionCallExpr(
                        strToDateName, new FunctionParams(false, strToDateExprs));

                FunctionName dateFormatName = new FunctionName("DATE_FORMAT");
                List<Expr> dateFormatArgs = Lists.newArrayList(strToDateFuncExpr, funcExpr.getChild(0));
                FunctionCallExpr dateFormatFunc = new FunctionCallExpr(
                        dateFormatName, new FunctionParams(false, dateFormatArgs));

                return dateFormatFunc;
            } else if (funcName.equalsIgnoreCase("alignment_timestamp")) {
                /*
                 * change to:
                 * UNIX_TIMESTAMP(DATE_FORMAT(FROM_UNIXTIME(ts), "%Y-01-01 00:00:00"));
                 *
                 */

                // FROM_UNIXTIME
                FunctionName fromUnixName = new FunctionName("FROM_UNIXTIME");
                List<Expr> fromUnixArgs = Lists.newArrayList(funcExpr.getChild(1));
                FunctionCallExpr fromUnixFunc = new FunctionCallExpr(
                        fromUnixName, new FunctionParams(false, fromUnixArgs));

                // DATE_FORMAT
                StringLiteral precision = (StringLiteral) funcExpr.getChild(0);
                StringLiteral format;
                if (precision.getStringValue().equalsIgnoreCase("year")) {
                    format = new StringLiteral("%Y-01-01 00:00:00");
                } else if (precision.getStringValue().equalsIgnoreCase("month")) {
                    format = new StringLiteral("%Y-%m-01 00:00:00");
                } else if (precision.getStringValue().equalsIgnoreCase("day")) {
                    format = new StringLiteral("%Y-%m-%d 00:00:00");
                } else if (precision.getStringValue().equalsIgnoreCase("hour")) {
                    format = new StringLiteral("%Y-%m-%d %H:00:00");
                } else {
                    throw new UserException("Unknown precision(" + precision.getStringValue() + ")");
                }
                FunctionName dateFormatName = new FunctionName("DATE_FORMAT");
                List<Expr> dateFormatArgs = Lists.newArrayList(fromUnixFunc, format);
                FunctionCallExpr dateFormatFunc = new FunctionCallExpr(
                        dateFormatName, new FunctionParams(false, dateFormatArgs));

                // UNIX_TIMESTAMP
                FunctionName unixTimeName = new FunctionName("UNIX_TIMESTAMP");
                List<Expr> unixTimeArgs = Lists.newArrayList();
                unixTimeArgs.add(dateFormatFunc);
                FunctionCallExpr unixTimeFunc = new FunctionCallExpr(
                        unixTimeName, new FunctionParams(false, unixTimeArgs));

                return unixTimeFunc;
            } else if (funcName.equalsIgnoreCase("default_value")) {
                return funcExpr.getChild(0);
            } else if (funcName.equalsIgnoreCase("now")) {
                FunctionName nowFunctionName = new FunctionName("NOW");
                FunctionCallExpr newFunc = new FunctionCallExpr(nowFunctionName, new FunctionParams(null));
                return newFunc;
            } else if (funcName.equalsIgnoreCase("substitute")) {
                return funcExpr.getChild(0);
            }
        }
        return originExpr;
    }

    public void unprotectAddLoadJob(LoadJob job, boolean isReplay) throws DdlException {
        long jobId = job.getId();
        long dbId = job.getDbId();
        String label = job.getLabel();

        if (!isReplay && getAllUnfinishedLoadJob() > Config.max_unfinished_load_job) {
            throw new DdlException(
                    "Number of unfinished load jobs exceed the max number: " + Config.max_unfinished_load_job);
        }

        Preconditions.checkState(!job.isSyncDeleteJob(), "delete job is deprecated");
        // check label exist
        boolean checkMini = true;
        if (job.getEtlJobType() == EtlJobType.MINI) {
            // already registered, do not need check
            checkMini = false;
        }

        unprotectIsLabelUsed(dbId, label, -1, checkMini);

        // add job
        Map<String, List<LoadJob>> labelToLoadJobs = null;
        if (dbLabelToLoadJobs.containsKey(dbId)) {
            labelToLoadJobs = dbLabelToLoadJobs.get(dbId);
        } else {
            labelToLoadJobs = Maps.newHashMap();
            dbLabelToLoadJobs.put(dbId, labelToLoadJobs);
        }
        List<LoadJob> labelLoadJobs = null;
        if (labelToLoadJobs.containsKey(label)) {
            labelLoadJobs = labelToLoadJobs.get(label);
        } else {
            labelLoadJobs = Lists.newArrayList();
            labelToLoadJobs.put(label, labelLoadJobs);
        }

        List<LoadJob> dbLoadJobs = null;
        if (dbToLoadJobs.containsKey(dbId)) {
            dbLoadJobs = dbToLoadJobs.get(dbId);
        } else {
            dbLoadJobs = Lists.newArrayList();
            dbToLoadJobs.put(dbId, dbLoadJobs);
        }
        idToLoadJob.put(jobId, job);
        dbLoadJobs.add(job);
        labelLoadJobs.add(job);

        // beginTransaction Here
        switch (job.getState()) {
            case PENDING:
                idToPendingLoadJob.put(jobId, job);
                break;
            case ETL:
                idToEtlLoadJob.put(jobId, job);
                break;
            case LOADING:
                idToLoadingLoadJob.put(jobId, job);
                // recover loadingPartitionIds
                recoverLoadingPartitions(job);
                break;
            case QUORUM_FINISHED:
                // The state QUORUM_FINISHED could only occur when loading image file
                idToQuorumFinishedLoadJob.put(jobId, job);
                break;
            case FINISHED:
                break;
            case CANCELLED:
                break;
            default:
                // Impossible to be other state
                Preconditions.checkNotNull(null, "Should not be here");
        }
    }

    private long getAllUnfinishedLoadJob() {
        return idToPendingLoadJob.size() + idToEtlLoadJob.size() + idToLoadingLoadJob.size()
                + idToQuorumFinishedLoadJob.size();
    }

    public void replayAddLoadJob(LoadJob job) throws DdlException {
        writeLock();
        try {
            unprotectAddLoadJob(job, true /* replay */);
        } finally {
            writeUnlock();
        }
    }

    public void unprotectEtlLoadJob(LoadJob job) {
        long jobId = job.getId();
        idToPendingLoadJob.remove(jobId);
        idToEtlLoadJob.put(jobId, job);

        replaceLoadJob(job);
    }

    public void replayEtlLoadJob(LoadJob job) throws DdlException {
        writeLock();
        try {
            unprotectEtlLoadJob(job);
        } finally {
            writeUnlock();
        }
    }

    public void unprotectLoadingLoadJob(LoadJob job) {
        long jobId = job.getId();
        idToEtlLoadJob.remove(jobId);
        idToLoadingLoadJob.put(jobId, job);

        // recover loadingPartitionIds
        recoverLoadingPartitions(job);

        replaceLoadJob(job);
    }

    public void replayLoadingLoadJob(LoadJob job) throws DdlException {
        writeLock();
        try {
            unprotectLoadingLoadJob(job);
        } finally {
            writeUnlock();
        }
    }

    // return true if we truly register a mini load label
    // return false otherwise (eg: a retry request)
    public boolean registerMiniLabel(String fullDbName, String label, long timestamp) throws DdlException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(fullDbName);

        long dbId = db.getId();
        writeLock();
        try {
            if (unprotectIsLabelUsed(dbId, label, timestamp, true)) {
                // label is used and this is a retry request.
                // no need to do further operation, just return.
                return false;
            }

            Map<String, Long> miniLabels = null;
            if (dbToMiniLabels.containsKey(dbId)) {
                miniLabels = dbToMiniLabels.get(dbId);
            } else {
                miniLabels = Maps.newHashMap();
                dbToMiniLabels.put(dbId, miniLabels);
            }
            miniLabels.put(label, timestamp);

            return true;
        } finally {
            writeUnlock();
        }
    }

    public void deregisterMiniLabel(String fullDbName, String label) throws DdlException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(fullDbName);

        long dbId = db.getId();
        writeLock();
        try {
            if (!dbToMiniLabels.containsKey(dbId)) {
                return;
            }

            Map<String, Long> miniLabels = dbToMiniLabels.get(dbId);
            miniLabels.remove(label);
            if (miniLabels.isEmpty()) {
                dbToMiniLabels.remove(dbId);
            }
        } finally {
            writeUnlock();
        }
    }

    public boolean isUncommittedLabel(long dbId, String label) throws DdlException {
        readLock();
        try {
            if (dbToMiniLabels.containsKey(dbId)) {
                Map<String, Long> uncommittedLabels = dbToMiniLabels.get(dbId);
                return uncommittedLabels.containsKey(label);
            }
        } finally {
            readUnlock();
        }
        return false;
    }

    public boolean isLabelUsed(long dbId, String label) throws DdlException {
        readLock();
        try {
            return unprotectIsLabelUsed(dbId, label, -1, true);
        } finally {
            readUnlock();
        }
    }

    /*
     * 1. if label is already used, and this is not a retry request,
     *    throw exception ("Label already used")
     * 2. if label is already used, but this is a retry request,
     *    return true
     * 3. if label is not used, return false
     * 4. throw exception if encounter error.
     */
    private boolean unprotectIsLabelUsed(long dbId, String label, long timestamp, boolean checkMini)
            throws DdlException {
        // check dbLabelToLoadJobs
        if (dbLabelToLoadJobs.containsKey(dbId)) {
            Map<String, List<LoadJob>> labelToLoadJobs = dbLabelToLoadJobs.get(dbId);
            if (labelToLoadJobs.containsKey(label)) {
                List<LoadJob> labelLoadJobs = labelToLoadJobs.get(label);
                for (LoadJob oldJob : labelLoadJobs) {
                    JobState oldJobState = oldJob.getState();
                    if (oldJobState != JobState.CANCELLED) {
                        if (timestamp == -1) {
                            // timestamp == -1 is for compatibility
                            throw new LabelAlreadyUsedException(label);
                        } else {
                            if (timestamp == oldJob.getTimestamp()) {
                                // this timestamp is used to verify if this label check is a retry request from backend.
                                // if the timestamp in request is same as timestamp in existing load job,
                                // which means this load job is already submitted
                                LOG.info("get a retry request with label: {}, timestamp: {}. return ok",
                                        label, timestamp);
                                return true;
                            } else {
                                throw new LabelAlreadyUsedException(label);
                            }
                        }
                    }
                }
            }
        }

        // check dbToMiniLabel
        if (checkMini) {
            return checkMultiLabelUsed(dbId, label, timestamp);
        }

        return false;
    }

    private boolean checkMultiLabelUsed(long dbId, String label, long timestamp) throws DdlException {
        if (dbToMiniLabels.containsKey(dbId)) {
            Map<String, Long> uncommittedLabels = dbToMiniLabels.get(dbId);
            if (uncommittedLabels.containsKey(label)) {
                if (timestamp == -1) {
                    throw new LabelAlreadyUsedException(label);
                } else {
                    if (timestamp == uncommittedLabels.get(label)) {
                        // this timestamp is used to verify if this label check is a retry request from backend.
                        // if the timestamp in request is same as timestamp in existing load job,
                        // which means this load job is already submitted
                        LOG.info("get a retry mini load request with label: {}, timestamp: {}. return ok",
                                label, timestamp);
                        return true;
                    } else {
                        throw new LabelAlreadyUsedException(label);
                    }
                }
            }
        }
        return false;
    }

    public boolean cancelLoadJob(LoadJob job, CancelType cancelType, String msg) {
        return cancelLoadJob(job, cancelType, msg, null);
    }

    public boolean cancelLoadJob(LoadJob job, CancelType cancelType, String msg, List<String> failedMsg) {
        // update job to cancelled
        LOG.info("try to cancel load job: {}", job);
        JobState srcState = job.getState();
        if (!updateLoadJobState(job, JobState.CANCELLED, cancelType, msg, failedMsg)) {
            LOG.warn("cancel load job failed. job: {}", job);
            return false;
        }

        // clear
        if (job.getHadoopDppConfig() != null) {
            clearJob(job, srcState);
        }

        Preconditions.checkState(job.getBrokerDesc() == null);
        LOG.info("cancel load job success. job: {}", job);
        return true;
    }

    public void unprotectCancelLoadJob(LoadJob job) {
        long jobId = job.getId();
        LoadJob oldJob = idToLoadJob.get(jobId);
        if (oldJob == null) {
            LOG.warn("cancel job does not exist. id: {}", jobId);
            return;
        }

        switch (oldJob.getState()) {
            case PENDING:
                idToPendingLoadJob.remove(jobId);
                break;
            case ETL:
                idToEtlLoadJob.remove(jobId);
                break;
            case LOADING:
                idToLoadingLoadJob.remove(jobId);
                // remove loading partitions
                removeLoadingPartitions(oldJob);
                break;
            default:
                LOG.warn("cancel job has wrong src state: {}", oldJob.getState().name());
                return;
        }

        replaceLoadJob(job);
    }

    public void replayCancelLoadJob(LoadJob job) {
        writeLock();
        try {
            unprotectCancelLoadJob(job);
        } finally {
            writeUnlock();
        }
    }

    public Map<Long, LoadJob> getIdToLoadJob() {
        return idToLoadJob;
    }

    public Map<Long, List<LoadJob>> getDbToLoadJobs() {
        return dbToLoadJobs;
    }

    public List<LoadJob> getLoadJobs(JobState jobState) {
        List<LoadJob> jobs = new ArrayList<LoadJob>();
        Collection<LoadJob> stateJobs = null;
        readLock();
        try {
            switch (jobState) {
                case PENDING:
                    stateJobs = idToPendingLoadJob.values();
                    break;
                case ETL:
                    stateJobs = idToEtlLoadJob.values();
                    break;
                case LOADING:
                    stateJobs = idToLoadingLoadJob.values();
                    break;
                case QUORUM_FINISHED:
                    stateJobs = idToQuorumFinishedLoadJob.values();
                    break;
                default:
                    break;
            }
            if (stateJobs != null) {
                jobs.addAll(stateJobs);
            }
        } finally {
            readUnlock();
        }
        return jobs;
    }

    public long getLoadJobNum(JobState jobState, long dbId) {
        readLock();
        try {
            List<LoadJob> loadJobs = this.dbToLoadJobs.get(dbId);
            if (loadJobs == null) {
                return 0;
            }

            int jobNum = 0;
            for (LoadJob job : loadJobs) {
                if (job.getState() == jobState) {
                    ++jobNum;
                }
            }
            return jobNum;
        } finally {
            readUnlock();
        }
    }

    public LoadJob getLoadJob(long jobId) {
        readLock();
        try {
            return idToLoadJob.get(jobId);
        } finally {
            readUnlock();
        }
    }

    public LinkedList<List<Comparable>> getLoadJobInfosByDb(long dbId, String dbName, String labelValue,
            boolean accurateMatch, Set<JobState> states) throws AnalysisException {
        LinkedList<List<Comparable>> loadJobInfos = new LinkedList<List<Comparable>>();
        readLock();
        try {
            List<LoadJob> loadJobs = this.dbToLoadJobs.get(dbId);
            if (loadJobs == null) {
                return loadJobInfos;
            }

            long start = System.currentTimeMillis();
            LOG.debug("begin to get load job info, size: {}", loadJobs.size());
            PatternMatcher matcher = null;
            if (labelValue != null && !accurateMatch) {
                matcher = PatternMatcher.createMysqlPattern(labelValue, CaseSensibility.LABEL.getCaseSensibility());
            }

            for (LoadJob loadJob : loadJobs) {
                // filter first
                String label = loadJob.getLabel();
                JobState state = loadJob.getState();

                if (labelValue != null) {
                    if (accurateMatch) {
                        if (!label.equals(labelValue)) {
                            continue;
                        }
                    } else {
                        if (!matcher.match(label)) {
                            continue;
                        }
                    }
                }

                if (states != null) {
                    if (!states.contains(state)) {
                        continue;
                    }
                }

                // check auth
                Set<String> tableNames = loadJob.getTableNames();
                if (tableNames.isEmpty()) {
                    // forward compatibility
                    if (!Env.getCurrentEnv().getAuth().checkDbPriv(ConnectContext.get(), dbName,
                            PrivPredicate.LOAD)) {
                        continue;
                    }
                } else {
                    boolean auth = true;
                    for (String tblName : tableNames) {
                        if (!Env.getCurrentEnv().getAuth().checkTblPriv(ConnectContext.get(), dbName,
                                tblName, PrivPredicate.LOAD)) {
                            auth = false;
                            break;
                        }
                    }
                    if (!auth) {
                        continue;
                    }
                }

                List<Comparable> jobInfo = new ArrayList<Comparable>();

                // jobId
                jobInfo.add(loadJob.getId());
                // label
                jobInfo.add(label);
                // state
                jobInfo.add(state.name());

                // progress
                switch (loadJob.getState()) {
                    case PENDING:
                        jobInfo.add("ETL:0%; LOAD:0%");
                        break;
                    case ETL:
                        jobInfo.add("ETL:" + loadJob.getProgress() + "%; LOAD:0%");
                        break;
                    case LOADING:
                        jobInfo.add("ETL:100%; LOAD:" + loadJob.getProgress() + "%");
                        break;
                    case QUORUM_FINISHED:
                        jobInfo.add("ETL:100%; LOAD:100%");
                        break;
                    case FINISHED:
                        jobInfo.add("ETL:100%; LOAD:100%");
                        break;
                    case CANCELLED:
                        jobInfo.add("ETL:N/A; LOAD:N/A");
                        break;
                    default:
                        jobInfo.add("ETL:N/A; LOAD:N/A");
                        break;
                }

                // type
                jobInfo.add(loadJob.getEtlJobType().name());

                // etl info
                EtlStatus status = loadJob.getEtlJobStatus();
                if (status == null || status.getState() == TEtlState.CANCELLED) {
                    jobInfo.add(FeConstants.null_string);
                } else {
                    Map<String, String> counters = status.getCounters();
                    List<String> info = Lists.newArrayList();
                    for (String key : counters.keySet()) {
                        // XXX: internal etl job return all counters
                        if (key.equalsIgnoreCase("HDFS bytes read")
                                || key.equalsIgnoreCase("Map input records")
                                || key.startsWith("dpp.")
                                || loadJob.getEtlJobType() == EtlJobType.MINI) {
                            info.add(key + "=" + counters.get(key));
                        }
                    } // end for counters
                    if (info.isEmpty()) {
                        jobInfo.add(FeConstants.null_string);
                    } else {
                        jobInfo.add(StringUtils.join(info, "; "));
                    }
                }

                // task info
                jobInfo.add("cluster:" + loadJob.getHadoopCluster()
                        + "; timeout(s):" + loadJob.getTimeoutSecond()
                        + "; max_filter_ratio:" + loadJob.getMaxFilterRatio());

                // error msg
                if (loadJob.getState() == JobState.CANCELLED) {
                    FailMsg failMsg = loadJob.getFailMsg();
                    jobInfo.add("type:" + failMsg.getCancelType() + "; msg:" + failMsg.getMsg());
                } else {
                    jobInfo.add(FeConstants.null_string);
                }

                // create time
                jobInfo.add(TimeUtils.longToTimeString(loadJob.getCreateTimeMs()));
                // etl start time
                jobInfo.add(TimeUtils.longToTimeString(loadJob.getEtlStartTimeMs()));
                // etl end time
                jobInfo.add(TimeUtils.longToTimeString(loadJob.getEtlFinishTimeMs()));
                // load start time
                jobInfo.add(TimeUtils.longToTimeString(loadJob.getLoadStartTimeMs()));
                // load end time
                jobInfo.add(TimeUtils.longToTimeString(loadJob.getLoadFinishTimeMs()));
                // tracking url
                jobInfo.add(status.getTrackingUrl());
                // job detail(not used for hadoop load, just return an empty string)
                jobInfo.add("");
                // transaction id
                jobInfo.add(loadJob.getTransactionId());
                // error tablets(not used for hadoop load, just return an empty string)
                jobInfo.add("");

                loadJobInfos.add(jobInfo);
            } // end for loadJobs

            LOG.debug("finished to get load job info, cost: {}", (System.currentTimeMillis() - start));
        } finally {
            readUnlock();
        }

        return loadJobInfos;
    }

    public long getLatestJobIdByLabel(long dbId, String labelValue) {
        long jobId = 0;
        readLock();
        try {
            List<LoadJob> loadJobs = this.dbToLoadJobs.get(dbId);
            if (loadJobs == null) {
                return 0;
            }

            for (LoadJob loadJob : loadJobs) {
                String label = loadJob.getLabel();

                if (labelValue != null) {
                    if (!label.equals(labelValue)) {
                        continue;
                    }
                }

                long currJobId = loadJob.getId();

                if (currJobId > jobId) {
                    jobId = currJobId;
                }
            }
        } finally {
            readUnlock();
        }

        return jobId;
    }

    public List<List<Comparable>> getLoadJobUnfinishedInfo(long jobId) {
        LinkedList<List<Comparable>> infos = new LinkedList<List<Comparable>>();
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();

        LoadJob loadJob = getLoadJob(jobId);
        if (loadJob == null
                || (loadJob.getState() != JobState.LOADING && loadJob.getState() != JobState.QUORUM_FINISHED)) {
            return infos;
        }

        long dbId = loadJob.getDbId();
        Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
        if (db == null) {
            return infos;
        }


        readLock();
        try {
            Map<Long, TabletLoadInfo> tabletMap = loadJob.getIdToTabletLoadInfo();
            for (long tabletId : tabletMap.keySet()) {
                TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
                if (tabletMeta == null) {
                    // tablet may be dropped during loading
                    continue;
                }

                long tableId = tabletMeta.getTableId();

                OlapTable table = (OlapTable) db.getTableNullable(tableId);
                if (table == null) {
                    continue;
                }
                table.readLock();
                try {
                    long partitionId = tabletMeta.getPartitionId();
                    Partition partition = table.getPartition(partitionId);
                    if (partition == null) {
                        continue;
                    }

                    long indexId = tabletMeta.getIndexId();
                    MaterializedIndex index = partition.getIndex(indexId);
                    if (index == null) {
                        continue;
                    }

                    Tablet tablet = index.getTablet(tabletId);
                    if (tablet == null) {
                        continue;
                    }

                    PartitionLoadInfo partitionLoadInfo = loadJob.getPartitionLoadInfo(tableId, partitionId);
                    long version = partitionLoadInfo.getVersion();

                    for (Replica replica : tablet.getReplicas()) {
                        if (replica.checkVersionCatchUp(version, false)) {
                            continue;
                        }

                        List<Comparable> info = Lists.newArrayList();
                        info.add(replica.getBackendId());
                        info.add(tabletId);
                        info.add(replica.getId());
                        info.add(replica.getVersion());
                        info.add(partitionId);
                        info.add(version);

                        infos.add(info);
                    }
                } finally {
                    table.readUnlock();
                }
            }
        } finally {
            readUnlock();
        }

        // sort by version, backendId
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(3, 0);
        Collections.sort(infos, comparator);

        return infos;
    }

    public LoadErrorHub.Param getLoadErrorHubInfo() {
        return loadErrorHubParam;
    }

    public void setLoadErrorHubInfo(LoadErrorHub.Param info) {
        this.loadErrorHubParam = info;
    }

    public void setLoadErrorHubInfo(Map<String, String> properties) throws DdlException {
        String type = properties.get("type");
        if (type.equalsIgnoreCase("MYSQL")) {
            String host = properties.get("host");
            if (Strings.isNullOrEmpty(host)) {
                throw new DdlException("mysql host is missing");
            }

            int port = -1;
            try {
                port = Integer.valueOf(properties.get("port"));
            } catch (NumberFormatException e) {
                throw new DdlException("invalid mysql port: " + properties.get("port"));
            }

            String user = properties.get("user");
            if (Strings.isNullOrEmpty(user)) {
                throw new DdlException("mysql user name is missing");
            }

            String db = properties.get("database");
            if (Strings.isNullOrEmpty(db)) {
                throw new DdlException("mysql database is missing");
            }

            String tbl = properties.get("table");
            if (Strings.isNullOrEmpty(tbl)) {
                throw new DdlException("mysql table is missing");
            }

            String pwd = Strings.nullToEmpty(properties.get("password"));

            MysqlLoadErrorHub.MysqlParam param = new MysqlLoadErrorHub.MysqlParam(host, port, user, pwd, db, tbl);
            loadErrorHubParam = LoadErrorHub.Param.createMysqlParam(param);
        } else if (type.equalsIgnoreCase("BROKER")) {
            String brokerName = properties.get("name");
            if (Strings.isNullOrEmpty(brokerName)) {
                throw new DdlException("broker name is missing");
            }
            properties.remove("name");

            if (!Env.getCurrentEnv().getBrokerMgr().containsBroker(brokerName)) {
                throw new DdlException("broker does not exist: " + brokerName);
            }

            String path = properties.get("path");
            if (Strings.isNullOrEmpty(path)) {
                throw new DdlException("broker path is missing");
            }
            properties.remove("path");

            // check if broker info is invalid
            BlobStorage blobStorage = BlobStorage.create(brokerName, StorageBackend.StorageType.BROKER, properties);
            Status st = blobStorage.checkPathExist(path);
            if (!st.ok()) {
                throw new DdlException("failed to visit path: " + path + ", err: " + st.getErrMsg());
            }

            BrokerLoadErrorHub.BrokerParam param = new BrokerLoadErrorHub.BrokerParam(brokerName, path, properties);
            loadErrorHubParam = LoadErrorHub.Param.createBrokerParam(param);
        } else if (type.equalsIgnoreCase("null")) {
            loadErrorHubParam = LoadErrorHub.Param.createNullParam();
        }

        Env.getCurrentEnv().getEditLog().logSetLoadErrorHub(loadErrorHubParam);

        LOG.info("set load error hub info: {}", loadErrorHubParam);
    }

    public static class JobInfo {
        public String dbName;
        public Set<String> tblNames = Sets.newHashSet();
        public String label;
        public String clusterName;
        public JobState state;
        public String failMsg;
        public String trackingUrl;

        public JobInfo(String dbName, String label, String clusterName) {
            this.dbName = dbName;
            this.label = label;
            this.clusterName = clusterName;
        }
    }

    // Get job state
    // result saved in info
    public void getJobInfo(JobInfo info) throws DdlException, MetaNotFoundException {
        String fullDbName = ClusterNamespace.getFullName(info.clusterName, info.dbName);
        info.dbName = fullDbName;
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
        readLock();
        try {
            Map<String, List<LoadJob>> labelToLoadJobs = dbLabelToLoadJobs.get(db.getId());
            if (labelToLoadJobs == null) {
                throw new DdlException("No jobs belong to database(" + info.dbName + ")");
            }
            List<LoadJob> loadJobs = labelToLoadJobs.get(info.label);
            if (loadJobs == null) {
                throw new DdlException("Unknown job(" + info.label + ")");
            }
            // only the last one should be running
            LoadJob job = loadJobs.get(loadJobs.size() - 1);

            if (!job.getTableNames().isEmpty()) {
                info.tblNames.addAll(job.getTableNames());
            }

            info.state = job.getState();
            if (info.state == JobState.QUORUM_FINISHED) {
                info.state = JobState.FINISHED;
            }

            info.failMsg = job.getFailMsg().getMsg();
            info.trackingUrl = job.getEtlJobStatus().getTrackingUrl();
        } finally {
            readUnlock();
        }
    }

    public void unprotectQuorumLoadJob(LoadJob job, Database db) {
        // in real time load replica info and partition version is set by transaction manager not by job
        if (job.getTransactionId() < 0) {
            // remove loading partitions
            removeLoadingPartitions(job);

            // Update database information first
            Map<Long, ReplicaPersistInfo> replicaInfos = job.getReplicaPersistInfos();
            if (replicaInfos != null) {
                for (ReplicaPersistInfo info : replicaInfos.values()) {
                    OlapTable table = (OlapTable) db.getTableNullable(info.getTableId());
                    if (table == null) {
                        LOG.warn("the table[{}] is missing", info.getIndexId());
                        continue;
                    }
                    Partition partition = table.getPartition(info.getPartitionId());
                    if (partition == null) {
                        LOG.warn("the partition[{}] is missing", info.getIndexId());
                        continue;
                    }
                    MaterializedIndex index = partition.getIndex(info.getIndexId());
                    if (index == null) {
                        LOG.warn("the index[{}] is missing", info.getIndexId());
                        continue;
                    }
                    Tablet tablet = index.getTablet(info.getTabletId());
                    if (tablet == null) {
                        LOG.warn("the tablet[{}] is missing", info.getTabletId());
                        continue;
                    }

                    Replica replica = tablet.getReplicaById(info.getReplicaId());
                    if (replica == null) {
                        LOG.warn("the replica[{}] is missing", info.getReplicaId());
                        continue;
                    }
                    replica.updateVersionInfo(info.getVersion(), info.getDataSize(), info.getRemoteDataSize(),
                            info.getRowCount());
                }
            }

            long jobId = job.getId();
            Map<Long, TableLoadInfo> idToTableLoadInfo = job.getIdToTableLoadInfo();
            if (idToTableLoadInfo != null) {
                for (Entry<Long, TableLoadInfo> tableEntry : idToTableLoadInfo.entrySet()) {
                    long tableId = tableEntry.getKey();
                    OlapTable table = (OlapTable) db.getTableNullable(tableId);
                    if (table == null) {
                        continue;
                    }
                    TableLoadInfo tableLoadInfo = tableEntry.getValue();
                    for (Entry<Long, PartitionLoadInfo> entry : tableLoadInfo.getIdToPartitionLoadInfo().entrySet()) {
                        long partitionId = entry.getKey();
                        Partition partition = table.getPartition(partitionId);
                        PartitionLoadInfo partitionLoadInfo = entry.getValue();
                        if (!partitionLoadInfo.isNeedLoad()) {
                            continue;
                        }
                        updatePartitionVersion(partition, partitionLoadInfo.getVersion(), jobId);

                        // update table row count
                        for (MaterializedIndex materializedIndex
                                : partition.getMaterializedIndices(IndexExtState.ALL)) {
                            long indexRowCount = 0L;
                            for (Tablet tablet : materializedIndex.getTablets()) {
                                long tabletRowCount = 0L;
                                for (Replica replica : tablet.getReplicas()) {
                                    long replicaRowCount = replica.getRowCount();
                                    if (replicaRowCount > tabletRowCount) {
                                        tabletRowCount = replicaRowCount;
                                    }
                                }
                                indexRowCount += tabletRowCount;
                            }
                            materializedIndex.setRowCount(indexRowCount);
                        } // end for indices
                    } // end for partitions
                } // end for tables
            }

            idToLoadingLoadJob.remove(jobId);
            idToQuorumFinishedLoadJob.put(jobId, job);
        }
        replaceLoadJob(job);
    }

    public void replayQuorumLoadJob(LoadJob job, Env env) throws MetaNotFoundException {
        // TODO: need to call this.writeLock()?
        Database db = env.getInternalCatalog().getDbOrMetaException(job.getDbId());

        List<Long> tableIds = Lists.newArrayList();
        long tblId = job.getTableId();
        if (tblId > 0) {
            tableIds.add(tblId);
        } else {
            tableIds.addAll(job.getIdToTableLoadInfo().keySet());
        }

        List<Table> tables = db.getTablesOnIdOrderOrThrowException(tableIds);

        MetaLockUtils.writeLockTables(tables);
        try {
            writeLock();
            try {
                unprotectQuorumLoadJob(job, db);
            } finally {
                writeUnlock();
            }
        } finally {
            MetaLockUtils.writeUnlockTables(tables);
        }
    }

    public void unprotectFinishLoadJob(LoadJob job, Database db) {
        // in real time load, replica info is not set by job, it is set by transaction manager
        long jobId = job.getId();
        if (job.getTransactionId() < 0) {
            idToQuorumFinishedLoadJob.remove(jobId);

            // Update database information
            Map<Long, ReplicaPersistInfo> replicaInfos = job.getReplicaPersistInfos();
            if (replicaInfos != null) {
                for (ReplicaPersistInfo info : replicaInfos.values()) {
                    OlapTable table = (OlapTable) db.getTableNullable(info.getTableId());
                    if (table == null) {
                        LOG.warn("the table[{}] is missing", info.getIndexId());
                        continue;
                    }
                    Partition partition = table.getPartition(info.getPartitionId());
                    if (partition == null) {
                        LOG.warn("the partition[{}] is missing", info.getIndexId());
                        continue;
                    }
                    MaterializedIndex index = partition.getIndex(info.getIndexId());
                    if (index == null) {
                        LOG.warn("the index[{}] is missing", info.getIndexId());
                        continue;
                    }
                    Tablet tablet = index.getTablet(info.getTabletId());
                    if (tablet == null) {
                        LOG.warn("the tablet[{}] is missing", info.getTabletId());
                        continue;
                    }

                    Replica replica = tablet.getReplicaById(info.getReplicaId());
                    if (replica == null) {
                        LOG.warn("the replica[{}] is missing", info.getReplicaId());
                        continue;
                    }
                    replica.updateVersionInfo(info.getVersion(), info.getDataSize(), info.getRemoteDataSize(),
                            info.getRowCount());
                }
            }
        } else {
            // in realtime load, does not exist a quorum finish stage, so that should remove job from pending queue and
            // loading queue at finish stage
            idToPendingLoadJob.remove(jobId);
            // for delete load job, it also in id to loading job
            idToLoadingLoadJob.remove(jobId);
            job.setProgress(100);
            job.setLoadFinishTimeMs(System.currentTimeMillis());
        }
        replaceLoadJob(job);
    }

    public void replayFinishLoadJob(LoadJob job, Env env) throws MetaNotFoundException {
        // TODO: need to call this.writeLock()?
        Database db = env.getCurrentInternalCatalog().getDbOrMetaException(job.getDbId());
        // After finish, the idToTableLoadInfo in load job will be set to null.
        // We lost table info. So we have to use db lock here.
        db.writeLock();
        try {
            writeLock();
            try {
                unprotectFinishLoadJob(job, db);
            } finally {
                writeUnlock();
            }
        } finally {
            db.writeUnlock();
        }
    }

    public void replayClearRollupInfo(ReplicaPersistInfo info, Env env) throws MetaNotFoundException {
        Database db = env.getInternalCatalog().getDbOrMetaException(info.getDbId());
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(info.getTableId(), TableType.OLAP);
        olapTable.writeLock();
        try {
            Partition partition = olapTable.getPartition(info.getPartitionId());
            MaterializedIndex index = partition.getIndex(info.getIndexId());
            index.clearRollupIndexInfo();
        } finally {
            olapTable.writeUnlock();
        }
    }

    private void replaceLoadJob(LoadJob job) {
        long jobId = job.getId();

        // Replace LoadJob in idToLoadJob
        if (!idToLoadJob.containsKey(jobId)) {
            // This may happen when we drop db while there are still load jobs running
            LOG.warn("Does not find load job in idToLoadJob. JobId : {}", jobId);
            return;
        }
        idToLoadJob.put(jobId, job);

        Preconditions.checkState(!job.isSyncDeleteJob(), "delete job is deprecated");

        // Replace LoadJob in dbToLoadJobs
        List<LoadJob> jobs = dbToLoadJobs.get(job.getDbId());
        if (jobs == null) {
            LOG.warn("Does not find db in dbToLoadJobs. DbId : {}",
                    job.getDbId());
            return;
        }
        int pos = 0;
        for (LoadJob oneJob : jobs) {
            if (oneJob.getId() == jobId) {
                break;
            }
            pos++;
        }
        if (pos == jobs.size()) {
            LOG.warn("Does not find load job for db. DbId : {}, jobId : {}",
                    job.getDbId(), jobId);
            return;
        }
        jobs.remove(pos);
        jobs.add(pos, job);

        // Replace LoadJob in dbLabelToLoadJobs
        if (dbLabelToLoadJobs.get(job.getDbId()) == null) {
            LOG.warn("Does not find db in dbLabelToLoadJobs. DbId : {}",
                    job.getDbId());
            return;
        }
        jobs = dbLabelToLoadJobs.get(job.getDbId()).get(job.getLabel());
        if (jobs == null) {
            LOG.warn("Does not find label for db. label : {}, DbId : {}",
                    job.getLabel(), job.getDbId());
            return;
        }
        pos = 0;
        for (LoadJob oneJob : jobs) {
            if (oneJob.getId() == jobId) {
                break;
            }
            pos++;
        }
        if (pos == jobs.size()) {
            LOG.warn("Does not find load job for label. label : {}, DbId : {}",
                    job.getLabel(), job.getDbId());
            return;
        }
        jobs.remove(pos);
        jobs.add(pos, job);
    }

    // remove all db jobs from dbToLoadJobs and dbLabelToLoadJobs
    // only remove finished or cancelled job from idToLoadJob
    // LoadChecker will update other state jobs to cancelled or finished,
    //     and they will be removed by removeOldLoadJobs periodically
    public void removeDbLoadJob(long dbId) {
        writeLock();
        try {
            if (dbToLoadJobs.containsKey(dbId)) {
                List<LoadJob> dbLoadJobs = dbToLoadJobs.remove(dbId);
                for (LoadJob job : dbLoadJobs) {
                    JobState state = job.getState();
                    if (state == JobState.CANCELLED || state == JobState.FINISHED) {
                        idToLoadJob.remove(job.getId());
                    }
                }
            }
            if (dbLabelToLoadJobs.containsKey(dbId)) {
                dbLabelToLoadJobs.remove(dbId);
            }
        } finally {
            writeUnlock();
        }
    }

    // Added by ljb. Remove old load jobs from idToLoadJob, dbToLoadJobs and dbLabelToLoadJobs
    // This function is called periodically. every Configure.label_clean_interval_second seconds
    public void removeOldLoadJobs() {
        long currentTimeMs = System.currentTimeMillis();

        writeLock();
        try {
            Iterator<Map.Entry<Long, LoadJob>> iter = idToLoadJob.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Long, LoadJob> entry = iter.next();
                LoadJob job = entry.getValue();
                if (job.isExpired(currentTimeMs)) {
                    long dbId = job.getDbId();
                    String label = job.getLabel();
                    // Remove job from idToLoadJob
                    iter.remove();

                    // Remove job from dbToLoadJobs
                    List<LoadJob> loadJobs = dbToLoadJobs.get(dbId);
                    if (loadJobs != null) {
                        loadJobs.remove(job);
                        if (loadJobs.size() == 0) {
                            dbToLoadJobs.remove(dbId);
                        }
                    }

                    // Remove job from dbLabelToLoadJobs
                    Map<String, List<LoadJob>> mapLabelToJobs = dbLabelToLoadJobs.get(dbId);
                    if (mapLabelToJobs != null) {
                        loadJobs = mapLabelToJobs.get(label);
                        if (loadJobs != null) {
                            loadJobs.remove(job);
                            if (loadJobs.isEmpty()) {
                                mapLabelToJobs.remove(label);
                                if (mapLabelToJobs.isEmpty()) {
                                    dbLabelToLoadJobs.remove(dbId);
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            writeUnlock();
        }
    }

    // clear dpp output and kill etl job
    public void clearJob(LoadJob job, JobState srcState) {
        JobState state = job.getState();
        if (state != JobState.CANCELLED && state != JobState.FINISHED) {
            LOG.warn("job state error. state: {}", state);
            return;
        }

        EtlJobType etlJobType = job.getEtlJobType();
        switch (etlJobType) {
            case HADOOP:
                DppScheduler dppScheduler = new DppScheduler(job.getHadoopDppConfig());
                // kill etl job
                if (state == JobState.CANCELLED && srcState == JobState.ETL) {
                    try {
                        dppScheduler.killEtlJob(job.getHadoopEtlJobId());
                    } catch (Exception e) {
                        LOG.warn("kill etl job error", e);
                    }
                }

                // delete all dirs related to job label, use "" instead of job.getEtlOutputDir()
                // hdfs://host:port/outputPath/dbId/loadLabel/
                DppConfig dppConfig = job.getHadoopDppConfig();
                String outputPath = DppScheduler.getEtlOutputPath(dppConfig.getFsDefaultName(),
                        dppConfig.getOutputPath(), job.getDbId(), job.getLabel(), "");
                try {
                    dppScheduler.deleteEtlOutputPath(outputPath);
                } catch (Exception e) {
                    LOG.warn("delete etl output path error", e);
                }
                break;
            case MINI:
                break;
            case INSERT:
                break;
            case BROKER:
                break;
            case DELETE:
                break;
            default:
                LOG.warn("unknown etl job type. type: {}, job id: {}", etlJobType.name(), job.getId());
                break;
        }
    }

    public boolean updateLoadJobState(LoadJob job, JobState destState) {
        return updateLoadJobState(job, destState, CancelType.UNKNOWN, null, null);
    }

    public boolean updateLoadJobState(LoadJob job, JobState destState, CancelType cancelType, String msg,
                                      List<String> failedMsg) {
        boolean result = true;
        JobState srcState;

        long jobId = job.getId();
        long dbId = job.getDbId();
        Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
        String errMsg = msg;
        if (db == null) {
            // if db is null, update job to cancelled
            errMsg = "db does not exist. id: " + dbId;
            LOG.warn(errMsg);
            writeLock();
            try {
                // sometimes db is dropped and then cancel the job, the job must have transactionid
                // transaction state should only be dropped when job is dropped
                processCancelled(job, cancelType, errMsg, failedMsg);
            } finally {
                writeUnlock();
            }
        } else {
            db.writeLock();
            try {
                writeLock();
                try {
                    // check state
                    srcState = job.getState();
                    if (!STATE_CHANGE_MAP.containsKey(srcState)) {
                        LOG.warn("src state error. src state: {}", srcState.name());
                        return false;
                    }
                    Set<JobState> destStates = STATE_CHANGE_MAP.get(srcState);
                    if (!destStates.contains(destState)) {
                        LOG.warn("state change error. src state: {}, dest state: {}",
                                srcState.name(), destState.name());
                        return false;
                    }

                    switch (destState) {
                        case ETL:
                            idToPendingLoadJob.remove(jobId);
                            idToEtlLoadJob.put(jobId, job);
                            job.setProgress(0);
                            job.setEtlStartTimeMs(System.currentTimeMillis());
                            job.setState(destState);
                            Env.getCurrentEnv().getEditLog().logLoadEtl(job);
                            break;
                        case LOADING:
                            idToEtlLoadJob.remove(jobId);
                            idToLoadingLoadJob.put(jobId, job);
                            job.setProgress(0);
                            job.setLoadStartTimeMs(System.currentTimeMillis());
                            job.setState(destState);
                            Env.getCurrentEnv().getEditLog().logLoadLoading(job);
                            break;
                        case QUORUM_FINISHED:
                            if (processQuorumFinished(job, db)) {
                                // Write edit log
                                Env.getCurrentEnv().getEditLog().logLoadQuorum(job);
                            } else {
                                errMsg = "process loading finished fail";
                                processCancelled(job, cancelType, errMsg, failedMsg);
                            }
                            break;
                        case FINISHED:
                            if (job.getTransactionId() > 0) {
                                idToPendingLoadJob.remove(jobId);
                                idToLoadingLoadJob.remove(jobId);
                                job.setProgress(100);
                                job.setLoadFinishTimeMs(System.currentTimeMillis());
                            }
                            // job will transfer from LOADING to FINISHED, skip QUORUM_FINISHED
                            idToLoadingLoadJob.remove(jobId);
                            idToQuorumFinishedLoadJob.remove(jobId);
                            job.setState(destState);

                            // clear push tasks
                            for (PushTask pushTask : job.getPushTasks()) {
                                AgentTaskQueue.removePushTask(pushTask.getBackendId(), pushTask.getSignature(),
                                        pushTask.getVersion(),
                                        pushTask.getPushType(), pushTask.getTaskType());
                            }
                            // Clear the Map and Set in this job, reduce the memory cost for finished load job.
                            // for delete job, keep the map and set because some of them is used in show proc method
                            if (!job.isSyncDeleteJob()) {
                                job.clearRedundantInfoForHistoryJob();
                            }
                            // Write edit log
                            Env.getCurrentEnv().getEditLog().logLoadDone(job);
                            break;
                        case CANCELLED:
                            processCancelled(job, cancelType, errMsg, failedMsg);
                            break;
                        default:
                            Preconditions.checkState(false, "wrong job state: " + destState.name());
                            break;
                    }
                } finally {
                    writeUnlock();
                }
            } finally {
                db.writeUnlock();
            }
        }

        // check current job state
        if (destState != job.getState()) {
            result = false;
        }
        return result;
    }

    private boolean processQuorumFinished(LoadJob job, Database db) {
        long jobId = job.getId();
        // remove partition from loading set
        removeLoadingPartitions(job);

        // check partition exist
        Map<Long, TableLoadInfo> idToTableLoadInfo = job.getIdToTableLoadInfo();
        for (Entry<Long, TableLoadInfo> tableEntry : idToTableLoadInfo.entrySet()) {
            long tableId = tableEntry.getKey();
            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            if (table == null) {
                LOG.warn("table does not exist, id: {}", tableId);
                return false;
            }

            TableLoadInfo tableLoadInfo = tableEntry.getValue();
            for (Entry<Long, PartitionLoadInfo> partitionEntry : tableLoadInfo.getIdToPartitionLoadInfo().entrySet()) {
                long partitionId = partitionEntry.getKey();
                PartitionLoadInfo partitionLoadInfo = partitionEntry.getValue();
                if (!partitionLoadInfo.isNeedLoad()) {
                    continue;
                }

                Partition partition = table.getPartition(partitionId);
                if (partition == null) {
                    LOG.warn("partition does not exist, id: {}", partitionId);
                    return false;
                }
            }
        }

        // update partition version and index row count
        for (Entry<Long, TableLoadInfo> tableEntry : idToTableLoadInfo.entrySet()) {
            long tableId = tableEntry.getKey();
            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            if (table == null) {
                continue;
            }

            TableLoadInfo tableLoadInfo = tableEntry.getValue();
            for (Entry<Long, PartitionLoadInfo> entry : tableLoadInfo.getIdToPartitionLoadInfo().entrySet()) {
                long partitionId = entry.getKey();
                Partition partition = table.getPartition(partitionId);
                PartitionLoadInfo partitionLoadInfo = entry.getValue();
                if (!partitionLoadInfo.isNeedLoad()) {
                    continue;
                }

                updatePartitionVersion(partition, partitionLoadInfo.getVersion(), jobId);

                for (MaterializedIndex materializedIndex : partition.getMaterializedIndices(IndexExtState.ALL)) {
                    long tableRowCount = 0L;
                    for (Tablet tablet : materializedIndex.getTablets()) {
                        long tabletRowCount = 0L;
                        for (Replica replica : tablet.getReplicas()) {
                            long replicaRowCount = replica.getRowCount();
                            if (replicaRowCount > tabletRowCount) {
                                tabletRowCount = replicaRowCount;
                            }
                        }
                        tableRowCount += tabletRowCount;
                    }
                    materializedIndex.setRowCount(tableRowCount);
                }
            }
        }

        // When start up or checkpoint, Job may stay in pending queue. So remove it.
        idToPendingLoadJob.remove(jobId);

        idToLoadingLoadJob.remove(jobId);
        idToQuorumFinishedLoadJob.put(jobId, job);
        job.setProgress(100);
        job.setLoadFinishTimeMs(System.currentTimeMillis());
        job.setState(JobState.QUORUM_FINISHED);
        return true;
    }

    private void updatePartitionVersion(Partition partition, long version, long jobId) {
        long partitionId = partition.getId();
        partition.updateVisibleVersion(version);
        LOG.info("update partition version success. version: {}, job id: {}, partition id: {}",
                version, jobId, partitionId);
    }

    private boolean processCancelled(LoadJob job, CancelType cancelType, String msg, List<String> failedMsg) {
        long jobId = job.getId();
        JobState srcState = job.getState();
        CancelType tmpCancelType = CancelType.UNKNOWN;
        // should abort in transaction manager first because it maybe aborts job successfully
        // and abort in transaction manager failed then there will be rubbish transactions in transaction manager
        try {
            Env.getCurrentGlobalTransactionMgr().abortTransaction(
                    job.getDbId(),
                    job.getTransactionId(),
                    job.getFailMsg().toString());
        } catch (TransactionNotFoundException e) {
            // the transaction may already be aborted due to timeout by transaction manager.
            // just print a log and continue to cancel the job.
            LOG.info("transaction not found when try to abort it: {}", e.getTransactionId());
        } catch (AnalysisException e) {
            // This is because the database has been dropped, so dbTransactionMgr can not be found.
            // In this case, just continue to cancel the load.
            if (!e.getMessage().contains("does not exist")) {
                if (failedMsg != null) {
                    failedMsg.add("Abort transaction failed: " + e.getMessage());
                }
                return false;
            }
        } catch (Exception e) {
            LOG.info("errors while abort transaction", e);
            if (failedMsg != null) {
                failedMsg.add("Abort transaction failed: " + e.getMessage());
            }
            return false;
        }
        switch (srcState) {
            case PENDING:
                idToPendingLoadJob.remove(jobId);
                tmpCancelType = CancelType.ETL_SUBMIT_FAIL;
                break;
            case ETL:
                idToEtlLoadJob.remove(jobId);
                tmpCancelType = CancelType.ETL_RUN_FAIL;
                break;
            case LOADING:
                // remove partition from loading set
                removeLoadingPartitions(job);
                idToLoadingLoadJob.remove(jobId);
                tmpCancelType = CancelType.LOAD_RUN_FAIL;
                break;
            case QUORUM_FINISHED:
                idToQuorumFinishedLoadJob.remove(jobId);
                tmpCancelType = CancelType.LOAD_RUN_FAIL;
                break;
            default:
                Preconditions.checkState(false, "wrong job state: " + srcState.name());
                break;
        }

        // set failMsg and state
        CancelType newCancelType = cancelType;
        if (newCancelType == CancelType.UNKNOWN) {
            newCancelType = tmpCancelType;
        }
        FailMsg failMsg = new FailMsg(newCancelType, msg);
        job.setFailMsg(failMsg);
        job.setLoadFinishTimeMs(System.currentTimeMillis());
        job.setState(JobState.CANCELLED);

        // clear push tasks
        if (srcState == JobState.LOADING || srcState == JobState.QUORUM_FINISHED) {
            for (PushTask pushTask : job.getPushTasks()) {
                AgentTaskQueue.removePushTask(pushTask.getBackendId(), pushTask.getSignature(),
                        pushTask.getVersion(),
                        pushTask.getPushType(), pushTask.getTaskType());
            }
        }

        // Clear the Map and Set in this job, reduce the memory cost of canceled load job.
        job.clearRedundantInfoForHistoryJob();
        Env.getCurrentEnv().getEditLog().logLoadCancel(job);

        return true;
    }

    public boolean addLoadingPartitions(Set<Long> partitionIds) {
        writeLock();
        try {
            for (long partitionId : partitionIds) {
                if (loadingPartitionIds.contains(partitionId)) {
                    LOG.info("partition {} is loading", partitionId);
                    return false;
                }
            }
            loadingPartitionIds.addAll(partitionIds);
            return true;
        } finally {
            writeUnlock();
        }
    }

    private void recoverLoadingPartitions(LoadJob job) {
        // loading partition ids is used to avoid concurrent loading to a single partition
        // but in realtime load, concurrent loading is allowed, so it is useless
        if (job.getTransactionId() > 0) {
            return;
        }
        for (TableLoadInfo tableLoadInfo : job.getIdToTableLoadInfo().values()) {
            Map<Long, PartitionLoadInfo> idToPartitionLoadInfo = tableLoadInfo.getIdToPartitionLoadInfo();
            for (Entry<Long, PartitionLoadInfo> entry : idToPartitionLoadInfo.entrySet()) {
                PartitionLoadInfo partitionLoadInfo = entry.getValue();
                if (partitionLoadInfo.isNeedLoad()) {
                    loadingPartitionIds.add(entry.getKey());
                }
            }
        }
    }

    public void removeLoadingPartitions(Set<Long> partitionIds) {
        writeLock();
        try {
            loadingPartitionIds.removeAll(partitionIds);
        } finally {
            writeUnlock();
        }
    }

    private void removeLoadingPartitions(LoadJob job) {
        for (TableLoadInfo tableLoadInfo : job.getIdToTableLoadInfo().values()) {
            Map<Long, PartitionLoadInfo> idToPartitionLoadInfo = tableLoadInfo.getIdToPartitionLoadInfo();
            for (Entry<Long, PartitionLoadInfo> entry : idToPartitionLoadInfo.entrySet()) {
                PartitionLoadInfo partitionLoadInfo = entry.getValue();
                if (partitionLoadInfo.isNeedLoad()) {
                    loadingPartitionIds.remove(entry.getKey());
                }
            }
        }
    }

    public boolean checkPartitionLoadFinished(long partitionId, List<LoadJob> quorumFinishedLoadJobs) {
        readLock();
        try {
            for (JobState state : JobState.values()) {
                if (state == JobState.FINISHED || state == JobState.CANCELLED) {
                    continue;
                }

                // we check PENDING / ETL / LOADING
                List<LoadJob> loadJobs = this.getLoadJobs(state);
                for (LoadJob loadJob : loadJobs) {
                    Preconditions.checkNotNull(loadJob.getIdToTableLoadInfo());
                    for (TableLoadInfo tableLoadInfo : loadJob.getIdToTableLoadInfo().values()) {
                        if (tableLoadInfo.getIdToPartitionLoadInfo().containsKey(partitionId)) {
                            if (state == JobState.QUORUM_FINISHED) {
                                if (quorumFinishedLoadJobs != null) {
                                    quorumFinishedLoadJobs.add(loadJob);
                                } else {
                                    return false;
                                }
                            } else {
                                return false;
                            }
                        }
                    }
                }
            }
            return true;
        } finally {
            readUnlock();
        }
    }
}
