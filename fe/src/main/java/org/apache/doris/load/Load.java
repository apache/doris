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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;

import org.apache.commons.lang.StringUtils;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CancelLoadStmt;
import org.apache.doris.analysis.ColumnSeparator;
import org.apache.doris.analysis.DataDescription;
import org.apache.doris.analysis.DeleteStmt;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LabelName;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.analysis.Predicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Partition.PartitionState;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.catalog.Type;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.load.AsyncDeleteJob.DeleteState;
import org.apache.doris.load.FailMsg.CancelType;
import org.apache.doris.load.LoadJob.EtlJobType;
import org.apache.doris.load.LoadJob.JobState;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.ReplicaPersistInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentClient;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.CancelDeleteTask;
import org.apache.doris.task.PushTask;
import org.apache.doris.thrift.TEtlState;
import org.apache.doris.thrift.TMiniLoadRequest;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPriority;
import org.apache.doris.thrift.TPushType;
import org.apache.doris.transaction.PartitionCommitInfo;
import org.apache.doris.transaction.TableCommitInfo;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionStatus;
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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Load {
    private static final Logger LOG = LogManager.getLogger(Load.class);

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


    private Map<Long, List<DeleteInfo>> dbToDeleteInfos; // db to delete job list
    private Map<Long, List<LoadJob>> dbToDeleteJobs; // db to delete loadJob list

    private Set<Long> partitionUnderDelete; // save partitions which are running delete jobs
    private Map<Long, AsyncDeleteJob> idToQuorumFinishedDeleteJob;

    private volatile LoadErrorHub.Param loadErrorHubInfo = new LoadErrorHub.Param();

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
        dbToDeleteInfos = Maps.newHashMap();
        dbToDeleteJobs = Maps.newHashMap();
        partitionUnderDelete = Sets.newHashSet();
        idToQuorumFinishedDeleteJob = Maps.newLinkedHashMap();
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

    public void addLoadJob(TMiniLoadRequest request) throws DdlException {
        // get params
        String fullDbName = request.getDb();
        String tableName = request.getTbl();
        String label = request.getLabel();
        long timestamp = 0;
        if (request.isSetTimestamp()) {
            timestamp = request.getTimestamp();
        }
        TNetworkAddress beAddr = request.getBackend();
        String filePathsValue = request.getFiles().get(0);
        Map<String, String> params = request.getProperties();

        // create load stmt
        // label name
        LabelName labelName = new LabelName(fullDbName, label);

        // data descriptions
        // file paths
        if (Strings.isNullOrEmpty(filePathsValue)) {
            throw new DdlException("File paths are not specified");
        }
        List<String> filePaths = Arrays.asList(filePathsValue.split(","));

        // partitions | column names | separator | line delimiter
        List<String> partitionNames = null;
        List<String> columnNames = null;
        ColumnSeparator columnSeparator = null;
        List<String> hllColumnPairList = null;
        String lineDelimiter = null;
        if (params != null) {
            String specifiedPartitions = params.get(LoadStmt.KEY_IN_PARAM_PARTITIONS);
            if (!Strings.isNullOrEmpty(specifiedPartitions)) {
                partitionNames = Arrays.asList(specifiedPartitions.split(","));
            }
            String specifiedColumns = params.get(LoadStmt.KEY_IN_PARAM_COLUMNS);
            if (!Strings.isNullOrEmpty(specifiedColumns)) {
                columnNames = Arrays.asList(specifiedColumns.split(","));
            }
      
            final String hll = params.get(LoadStmt.KEY_IN_PARAM_HLL);
            if (!Strings.isNullOrEmpty(hll)) {
                hllColumnPairList = Arrays.asList(hll.split(":"));
            }

            String columnSeparatorStr = params.get(LoadStmt.KEY_IN_PARAM_COLUMN_SEPARATOR);
            if (columnSeparatorStr != null) {
                columnSeparator = new ColumnSeparator(columnSeparatorStr);
                try {
                    columnSeparator.analyze();
                } catch (AnalysisException e) {
                    throw new DdlException(e.getMessage());
                }
            }
            lineDelimiter = params.get(LoadStmt.KEY_IN_PARAM_LINE_DELIMITER);
        }

        DataDescription dataDescription = new DataDescription(tableName, partitionNames, filePaths, columnNames,
                columnSeparator, false, null);
        dataDescription.setLineDelimiter(lineDelimiter);
        dataDescription.setBeAddr(beAddr);
        // parse hll param pair
        if (hllColumnPairList != null) {
            for (int i = 0; i < hllColumnPairList.size(); i++) {
                final String pairStr = hllColumnPairList.get(i);
                final List<String> pairList = Arrays.asList(pairStr.split(","));
                if (pairList.size() != 2) {
                    throw new DdlException("hll param format error");
                }   

                final String resultColumn = pairList.get(0);
                final String hashColumn = pairList.get(1);
                final Pair<String, List<String>> pair = new Pair<String, List<String>>(
                        DataDescription.FUNCTION_HASH_HLL,
                        Arrays.asList(hashColumn));
                dataDescription.addColumnMapping(resultColumn, pair);
            }
        }

        List<DataDescription> dataDescriptions = Lists.newArrayList(dataDescription);

        // job properties
        Map<String, String> properties = Maps.newHashMap();
        if (params != null) {
            String maxFilterRatio = params.get(LoadStmt.MAX_FILTER_RATIO_PROPERTY);
            if (!Strings.isNullOrEmpty(maxFilterRatio)) {
                properties.put(LoadStmt.MAX_FILTER_RATIO_PROPERTY, maxFilterRatio);
            }
            String timeout = params.get(LoadStmt.TIMEOUT_PROPERTY);
            if (!Strings.isNullOrEmpty(timeout)) {
                properties.put(LoadStmt.TIMEOUT_PROPERTY, timeout);
            }
        }
        LoadStmt stmt = new LoadStmt(labelName, dataDescriptions, null, null, properties);

        // try to register mini label
        if (!registerMiniLabel(fullDbName, label, timestamp)) {
            throw new DdlException("Same data label[" + label + "] already used");
        }

        try {
            addLoadJob(stmt, EtlJobType.MINI, timestamp);
        } finally {
            deregisterMiniLabel(fullDbName, label);
        }
    }

    public void addLoadJob(LoadStmt stmt, EtlJobType etlJobType, long timestamp) throws DdlException {
        // get db
        String dbName = stmt.getLabel().getDbName();
        Database db = Catalog.getInstance().getDb(dbName);
        if (db == null) {
            throw new DdlException("Database[" + dbName + "] does not exist");
        }

        // create job
        LoadJob job = createLoadJob(stmt, etlJobType, db, timestamp);
        addLoadJob(job, db);
    }

    // for insert select from or create as stmt
    public void addLoadJob(String label, String dbName, 
                long tableId, Map<Long, Integer> indexIdToSchemaHash, 
                long transactionId,
                List<String> fileList, long timestamp) throws DdlException {
        // get db and table
        Database db = Catalog.getInstance().getDb(dbName);
        if (db == null) {
            throw new DdlException("Database[" + dbName + "] does not exist");
        }

        OlapTable table = null;
        db.readLock();
        try {
            table = (OlapTable) db.getTable(tableId);
        } finally {
            db.readUnlock();
        }
        if (table == null) {
            throw new DdlException("Table[" + tableId + "] does not exist");
        }

        // create job
        DataDescription desc = new DataDescription(table.getName(), null, Lists.newArrayList(""),
                null, null, false, null);
        LoadStmt stmt = new LoadStmt(new LabelName(dbName, label), Lists.newArrayList(desc), null, null, null);
        LoadJob job = createLoadJob(stmt, EtlJobType.INSERT, db, timestamp);

        // add schema hash
        for (Map.Entry<Long, Integer> entry : indexIdToSchemaHash.entrySet()) {
            job.getTableLoadInfo(tableId).addIndexSchemaHash(entry.getKey(), entry.getValue());
        }

        // file size use -1 temporarily
        Map<String, Long> fileMap = Maps.newHashMap();
        for (String filePath : fileList) {
            fileMap.put(filePath, -1L);
        }

        // update job info to etl finish
        EtlStatus status = job.getEtlJobStatus();
        status.setState(TEtlState.FINISHED);
        status.setFileMap(fileMap);
        job.setState(JobState.ETL);
        job.setTransactionId(transactionId);

        // add load job
        addLoadJob(job, db);
    }

    // This is the final step of all addLoadJob() methods
    private void addLoadJob(LoadJob job, Database db) throws DdlException {
        // check cluster capacity
        Catalog.getCurrentSystemInfo().checkClusterCapacity(db.getClusterName());
        // for original job, check quota
        // for delete job, not check
        if (!job.isSyncDeleteJob()) {
            db.checkQuota();
        }

		// check if table is in restore process
		db.readLock();
		try {
			for (Long tblId : job.getIdToTableLoadInfo().keySet()) {
				Table tbl = db.getTable(tblId);
				if (tbl != null && tbl.getType() == TableType.OLAP
						&& ((OlapTable) tbl).getState() == OlapTableState.RESTORE) {
                    throw new DdlException("Table " + tbl.getName() + " is in restore process. "
							+ "Can not load into it"); 
				}
			}
		} finally {
			db.readUnlock();
		}

        writeLock();
        try {
            unprotectAddLoadJob(job, false /* not replay */);
            MetricRepo.COUNTER_LOAD_ADD.increase(1L);
            Catalog.getInstance().getEditLog().logLoadStart(job);
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

            if (properties.containsKey(LoadStmt.LOAD_DELETE_FLAG_PROPERTY)) {
                String flag = properties.get(LoadStmt.LOAD_DELETE_FLAG_PROPERTY);
                if (flag.equalsIgnoreCase("true") || flag.equalsIgnoreCase("false")) {
                    job.setDeleteFlag(Boolean.parseBoolean(flag));
                } else {
                    throw new DdlException("Value of delete flag is invalid");
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
            createSource(db, dataDescription, tableToPartitionSources, job.getDeleteFlag());
			job.addTableName(dataDescription.getTableName());
        }
        for (Entry<Long, Map<Long, List<Source>>> tableEntry : tableToPartitionSources.entrySet()) {
            long tableId = tableEntry.getKey();
            Map<Long, List<Source>> partitionToSources = tableEntry.getValue();
            
            Map<Long, PartitionLoadInfo>  idToPartitionLoadInfo = Maps.newHashMap();
            for (Entry<Long, List<Source>> partitionEntry : partitionToSources.entrySet()) {
                PartitionLoadInfo info = new PartitionLoadInfo(partitionEntry.getValue());
                idToPartitionLoadInfo.put(partitionEntry.getKey(), info);
            }
            idToTableLoadInfo.put(tableId, new TableLoadInfo(idToPartitionLoadInfo));
        }
        job.setIdToTableLoadInfo(idToTableLoadInfo);

        if (etlJobType == EtlJobType.BROKER) {
            PullLoadSourceInfo sourceInfo = new PullLoadSourceInfo();
            for (DataDescription dataDescription : dataDescriptions) {
                BrokerFileGroup fileGroup = new BrokerFileGroup(dataDescription, stmt.getBrokerDesc());
                fileGroup.parse(db);
                sourceInfo.addFileGroup(fileGroup);
            }
            job.setPullLoadSourceInfo(sourceInfo);
            LOG.info("Source info is {}", sourceInfo);
        }
        
        if (etlJobType == EtlJobType.MINI) {
            // mini etl tasks
            Map<Long, MiniEtlTaskInfo> idToEtlTask = Maps.newHashMap();
            long etlTaskId = 0;
            db.readLock();
            try {
                for (DataDescription dataDescription : dataDescriptions) {
                    String tableName = dataDescription.getTableName();
                    OlapTable table = (OlapTable) db.getTable(tableName);
                    if (table == null) {
                        throw new DdlException("Table[" + tableName + "] does not exist");
                    }

                    TNetworkAddress beAddress = dataDescription.getBeAddr();
                    Backend backend = Catalog.getCurrentSystemInfo().getBackendWithBePort(beAddress.getHostname(),
                                                                                           beAddress.getPort());
                    if (!Catalog.getCurrentSystemInfo().checkBackendAvailable(backend.getId())) {
                        throw new DdlException("Etl backend is null or not available");
                    }

                    MiniEtlTaskInfo taskInfo = new MiniEtlTaskInfo(etlTaskId++, backend.getId(), table.getId());
                    idToEtlTask.put(taskInfo.getId(), taskInfo);
                }
            } finally {
                db.readUnlock();
            }
            job.setMiniEtlTasks(idToEtlTask);
            job.setPrority(TPriority.HIGH);

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

            Pair<String, DppConfig> clusterInfo = Catalog.getCurrentCatalog().getAuth().getLoadClusterInfo(
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
                job.setPrority(dppConfig.getPriority());
            } catch (LoadException e) {
                throw new DdlException(e.getMessage());
            }

            if (job.getTimeoutSecond() == 0) {
                // set default timeout
                job.setTimeoutSecond(Config.hadoop_load_default_timeout_second);
            }
        } else if (etlJobType == EtlJobType.BROKER) {
            if (job.getTimeoutSecond() == 0) {
                // set default timeout
                job.setTimeoutSecond(Config.pull_load_task_default_timeout_second);
            }
        } else if (etlJobType == EtlJobType.INSERT) {
            job.setPrority(TPriority.HIGH);
        }

        // job id
        job.setId(Catalog.getInstance().getNextId());
        
        return job;
    }

    private void createSource(Database db, DataDescription dataDescription, 
            Map<Long, Map<Long, List<Source>>> tableToPartitionSources, boolean deleteFlag) throws DdlException {
        Source source = new Source(dataDescription.getFilePathes());
        long tableId = -1;
        Set<Long> sourcePartitionIds = Sets.newHashSet();

        // source column names and partitions
        String tableName = dataDescription.getTableName();
        Map<String, Pair<String, List<String>>> columnToFunction = null;
        db.readLock();
        try {
            Table table = db.getTable(tableName);
            if (table == null) {
                throw new DdlException("Table [" + tableName + "] does not exist");
            }
            tableId = table.getId();
            if (table.getType() != TableType.OLAP) {
                throw new DdlException("Table [" + tableName + "] is not olap table");
            }

            if (((OlapTable) table).getState() == OlapTableState.RESTORE) {
                throw new DdlException("Table [" + tableName + "] is under restore");
            }

            if (((OlapTable) table).getKeysType() != KeysType.AGG_KEYS && dataDescription.isNegative()) {
                throw new DdlException("Load for AGG_KEYS table should not specify NEGATIVE");
            }
            
            if (((OlapTable) table).getKeysType() != KeysType.UNIQUE_KEYS && deleteFlag) {
                throw new DdlException("Delete flag can only be used for UNIQUE_KEYS table");
            }

            // get table schema
            List<Column> tableSchema = table.getBaseSchema();
            Map<String, Column> nameToTableColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            for (Column column : tableSchema) {
                nameToTableColumn.put(column.getName(), column);
            }

            // source columns
            List<String> columnNames = Lists.newArrayList();
            List<String> assignColumnNames = dataDescription.getColumnNames();
            if (assignColumnNames == null) {
                // use table columns
                for (Column column : tableSchema) {
                    columnNames.add(column.getName());
                }
            } else {
                // convert column to schema format
                for (String assignCol : assignColumnNames) {
                    if (nameToTableColumn.containsKey(assignCol)) {
                        columnNames.add(nameToTableColumn.get(assignCol).getName());
                    } else {
                        columnNames.add(assignCol);
                    }
                }
            }
            source.setColumnNames(columnNames);

            // check default value
            Map<String, Pair<String, List<String>>> assignColumnToFunction = dataDescription.getColumnMapping();
            for (Column column : tableSchema) {
                String columnName = column.getName();
                if (columnNames.contains(columnName)) {
                    continue;
                }

                if (assignColumnToFunction != null && assignColumnToFunction.containsKey(columnName)) {
                    continue;
                }

                if (column.getDefaultValue() != null || column.isAllowNull()) {
                    continue;
                }

                if (deleteFlag && !column.isKey()) {
                    List<String> args = Lists.newArrayList();
                    args.add("0");
                    Pair<String, List<String>> functionPair = new Pair<String, List<String>>("default_value", args);
                    assignColumnToFunction.put(columnName, functionPair);
                    continue;
                }

                throw new DdlException("Column has no default value. column: " + columnName);
            }

            // check hll 
            for (Column column : tableSchema) {
                if (column.getColumnType().getType() == PrimitiveType.HLL) {
                    if (assignColumnToFunction != null && !assignColumnToFunction.containsKey(column.getName())) {
                        throw new DdlException("Hll column is not assigned. column:" + column.getName());
                    }
                }
            }
            // check mapping column exist in table
            // check function
            // convert mapping column and func arg columns to schema format
            Map<String, String> columnNameMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            for (String columnName : columnNames) {
                columnNameMap.put(columnName, columnName);
            }
            if (assignColumnToFunction != null) {
                columnToFunction = Maps.newHashMap();
                for (Entry<String, Pair<String, List<String>>> entry : assignColumnToFunction.entrySet()) {
                    String mappingColumnName = entry.getKey();
                    if (!nameToTableColumn.containsKey(mappingColumnName)) {
                        throw new DdlException("Mapping column is not in table. column: " + mappingColumnName);
                    }

                    Column mappingColumn = nameToTableColumn.get(mappingColumnName);
                    Pair<String, List<String>> function = entry.getValue();
                    try {
                        DataDescription.validateMappingFunction(function.first, function.second, columnNameMap,
                                mappingColumn, dataDescription.isPullLoad());
                    } catch (AnalysisException e) {
                        throw new DdlException(e.getMessage());
                    }

                    columnToFunction.put(mappingColumn.getName(), function);
                }
            }

            // partitions of this source
            OlapTable olapTable = (OlapTable) table;
            List<String> partitionNames = dataDescription.getPartitionNames();
            if (partitionNames == null || partitionNames.isEmpty()) {
                partitionNames = new ArrayList<String>();
                for (Partition partition : olapTable.getPartitions()) {
                    partitionNames.add(partition.getName());
                }
            }
            for (String partitionName : partitionNames) {
                Partition partition = olapTable.getPartition(partitionName);
                if (partition == null) {
                    throw new DdlException("Partition [" + partitionName + "] does not exist");
                }
                sourcePartitionIds.add(partition.getId());
            }
        } finally {
            db.readUnlock();
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

    public void unprotectAddLoadJob(LoadJob job, boolean isReplay) throws DdlException {
        long jobId = job.getId();
        long dbId = job.getDbId();
        String label = job.getLabel();
        
        if (!isReplay && getAllUnfinishedLoadJob() > Config.max_unfinished_load_job) {
            throw new DdlException(
                    "Number of unfinished load jobs exceed the max number: " + Config.max_unfinished_load_job);
        }

        if (!job.isSyncDeleteJob()) {
            // check label exist
            boolean checkMini = true;
            if (job.getEtlJobType() == EtlJobType.MINI) {
                // already registered, do not need check
                checkMini = false;
            }

            if (isLabelUsed(dbId, label, -1, checkMini)) {
                throw new DdlException("Same data label[" + label + "] already used");
            }
    
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
        } else {
            List<LoadJob> dbDeleteJobs = null;
            if (dbToDeleteJobs.containsKey(dbId)) {
                dbDeleteJobs = dbToDeleteJobs.get(dbId);
            } else {
                dbDeleteJobs = Lists.newArrayList();
                dbToDeleteJobs.put(dbId, dbDeleteJobs);
            }
            idToLoadJob.put(jobId, job);
            dbDeleteJobs.add(job);
        }
        
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

    public boolean registerMiniLabel(
              String fullDbName, String label, long timestamp) throws DdlException {
        Database db = Catalog.getInstance().getDb(fullDbName);
        if (db == null) {
            throw new DdlException("Db does not exist. name: " + fullDbName);
        }

        long dbId = db.getId();
        writeLock();
        try {
            if (isLabelUsed(dbId, label, -1, true)) {
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
        } finally {
            writeUnlock();
        }

        return true;
    }
    
    public void deregisterMiniLabel(String fullDbName, String label) throws DdlException {
        Database db = Catalog.getInstance().getDb(fullDbName);
        if (db == null) {
            throw new DdlException("Db does not exist. name: " + fullDbName);
        }

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

    public boolean isLabelUsed(String fullDbName, String label, long timestamp) throws DdlException {
        Database db = Catalog.getInstance().getDb(fullDbName);
        if (db == null) {
            throw new DdlException("Db does not exist. name: " + fullDbName);
        }

        readLock();
        try {
            return isLabelUsed(db.getId(), label, timestamp, true);
        } finally {
            readUnlock();
        }
    }
    
    /*
     * if timestamp does not equals to -1, this is a retry request:
     * 1. if label has been used, and job's timestamp equals to the given one, return true.
     * 2. if label has been used, but timestamp is not equal, return false. The caller will finally call
     *    this method again will timestamp == -1, and return if label has been used or not.
     * 3. if label does not exist, just return false, as usual.
     * 
     * if timestamp equals to -1, return true if label has been used, otherwise, return false. 
     * 
     * throw DdlException if encounter other exception.
     */
    private boolean isLabelUsed(long dbId, String label, long timestamp, boolean checkMini)
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
                            return true;
                        } else {
                            if (timestamp == oldJob.getTimestamp()) {
                                // this timestamp is used to verify if this label check is a retry request from backend.
                                // if the timestamp in request is same as timestamp in existing load job,
                                // which means this load job is already submitted
                                return true;
                            } else {
                                return false;
                            }
                        }
                    }
                }
            }
        }

        // check dbToMiniLabel
        if (checkMini) {
            if (dbToMiniLabels.containsKey(dbId)) {
                Map<String, Long> uncommittedLabels = dbToMiniLabels.get(dbId);
                if (uncommittedLabels.containsKey(label)) {
                    if (timestamp == -1) {
                        return true;
                    } else {
                        if (timestamp == uncommittedLabels.get(label)) {
                            // this timestamp is used to verify if this label check is a retry request from backend.
                            // if the timestamp in request is same as timestamp in existing load job,
                            // which means this load job is already submitted
                            return true;
                        } else {
                            return false;
                        }
                    }
                }
            }
        }

        return false;
    }

    public boolean cancelLoadJob(CancelLoadStmt stmt) throws DdlException {
        // get params
        String dbName = stmt.getDbName();
        String label = stmt.getLabel();
        
        // get load job and check state
        Database db = Catalog.getInstance().getDb(dbName);
        if (db == null) {
            throw new DdlException("Db does not exist. name: " + dbName);
        }
        LoadJob job = null;
        readLock();
        try {
            Map<String, List<LoadJob>> labelToLoadJobs = dbLabelToLoadJobs.get(db.getId());
            if (labelToLoadJobs == null) {
                throw new DdlException("Load job does not exist");
            }
            
            List<LoadJob> loadJobs = labelToLoadJobs.get(label);
            if (loadJobs == null) {
                throw new DdlException("Load job does not exist");
            }
            // only the last one should be running
            job = loadJobs.get(loadJobs.size() - 1);
            JobState state = job.getState();
            if (state == JobState.CANCELLED) {
                throw new DdlException("Load job has been cancelled");
            } else if (state == JobState.QUORUM_FINISHED || state == JobState.FINISHED) {
                throw new DdlException("Load job has been finished");
            }
        } finally {
            readUnlock();
        }

		// check auth here, cause we need table info
		Set<String> tableNames = job.getTableNames();
		if (tableNames.isEmpty()) {
			// forward compatibility
			if (!Catalog.getCurrentCatalog().getAuth().checkDbPriv(ConnectContext.get(), dbName,
						PrivPredicate.LOAD)) {
				ErrorReport.reportDdlException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CANCEL LOAD");
			}
		} else {
			for (String tblName : tableNames) {
				if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), dbName, tblName,
							PrivPredicate.LOAD)) {
					ErrorReport.reportDdlException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "CANCEL LOAD",
							ConnectContext.get().getQualifiedUser(),
							ConnectContext.get().getRemoteIP(), tblName);
				}
			}
		}

        // cancel job
        if (!cancelLoadJob(job, CancelType.USER_CANCEL, "user cancel")) {
            throw new DdlException("Cancel load job fail");
        }
        
        return true;
    }
     
    public boolean cancelLoadJob(LoadJob job, CancelType cancelType, String msg) {
        // update job to cancelled
        JobState srcState = job.getState();
        if (!updateLoadJobState(job, JobState.CANCELLED, cancelType, msg)) {
            LOG.warn("cancel load job failed. job: {}", job, new Exception());
            return false;
        }
        
        // clear
        if (job.getHadoopDppConfig() != null) {
            clearJob(job, srcState);
        }

        if (job.getBrokerDesc() != null) {
            if (srcState == JobState.ETL) {
                // Cancel job id
                Catalog.getInstance().getPullLoadJobMgr().cancelJob(job.getId());
            }
        }
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
    
    public void removeDeleteJobAndSetState(AsyncDeleteJob job) {
        job.clearTasks();
        writeLock();
        try {
            idToQuorumFinishedDeleteJob.remove(job.getJobId());

            List<DeleteInfo> deleteInfos = dbToDeleteInfos.get(job.getDbId());
            Preconditions.checkNotNull(deleteInfos);

            for (DeleteInfo deleteInfo : deleteInfos) {
                if (deleteInfo.getJobId() == job.getJobId()) {
                    deleteInfo.getAsyncDeleteJob().setState(DeleteState.FINISHED);
                    LOG.info("replay set async delete job to finished: {}", job.getJobId());
                }
            }

        } finally {
            writeUnlock();
        }
    }

    public List<AsyncDeleteJob> getQuorumFinishedDeleteJobs() {
        List<AsyncDeleteJob> jobs = Lists.newArrayList();
        Collection<AsyncDeleteJob> stateJobs = null;
        readLock();
        try {
            stateJobs = idToQuorumFinishedDeleteJob.values();
            if (stateJobs != null) {
                jobs.addAll(stateJobs);
            }
        } finally {
            readUnlock();
        }
        return jobs;
    }

    public int getLoadJobNumber() {
        readLock();
        try {
            if (idToLoadJob == null) {
                return 0;
            }
            int loadJobNum = 0;
            for (LoadJob loadJob : idToLoadJob.values()) {
                if (!loadJob.isSyncDeleteJob()) {
                    ++ loadJobNum;
                }
            }
            return loadJobNum;
        } finally {
            readUnlock();
        }
    }
    
    public Map<Long, LoadJob> getIdToLoadJob() {
        return idToLoadJob;
    }
    
    public Map<Long, List<LoadJob>> getDbToLoadJobs() {
        return dbToLoadJobs;
    }
    
    public Map<Long, List<LoadJob>> getDbToDeleteJobs() {
        return dbToDeleteJobs;
    }
    
    public Map<Long, List<DeleteInfo>> getDbToDeleteInfos() {
        return dbToDeleteInfos;
    }

    public Set<Long> getTxnIdsByDb(Long dbId) {
        Set<Long> txnIds = Sets.newHashSet();
        readLock();
        try {
            List<LoadJob> jobs = dbToLoadJobs.get(dbId);
            if (jobs != null) {
                for (LoadJob loadJob : jobs) {
                    txnIds.add(loadJob.getTransactionId());
                }
            }
        } finally {
            readUnlock();
        }
        return txnIds;
    }

    public List<LoadJob> getDbLoadJobs(long dbId) {
        readLock();
        try {
            return dbToLoadJobs.get(dbId);
        } finally {
            readUnlock();
        }
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

    public int getLoadJobNum(JobState jobState, long dbId) {
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

    public AsyncDeleteJob getAsyncDeleteJob(long jobId) {
        readLock();
        try {
            return idToQuorumFinishedDeleteJob.get(jobId);
        } finally {
            readUnlock();
        }
    }

    public List<AsyncDeleteJob> getCopiedAsyncDeleteJobs() {
        readLock();
        try {
            return Lists.newArrayList(idToQuorumFinishedDeleteJob.values());
        } finally {
            readUnlock();
        }
    }
  
    public LinkedList<List<Comparable>> getLoadJobInfosByDb(long dbId, String dbName, String labelValue,
            boolean accurateMatch, Set<JobState> states, ArrayList<OrderByPair> orderByPairs) {
        LinkedList<List<Comparable>> loadJobInfos = new LinkedList<List<Comparable>>();
        readLock();
        try {
            List<LoadJob> loadJobs = this.dbToLoadJobs.get(dbId);
            if (loadJobs == null) {
                return loadJobInfos;
            }

			long start = System.currentTimeMillis();
			LOG.debug("begin to get load job info, size: {}", loadJobs.size());
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
                        if (!label.contains(labelValue)) {
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
					if (!Catalog.getCurrentCatalog().getAuth().checkDbPriv(ConnectContext.get(), dbName,
								PrivPredicate.SHOW)) {
						continue;
					}
				} else {
					boolean auth = true;
					for (String tblName : tableNames) {
						if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), dbName,
									tblName, PrivPredicate.SHOW)) {
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

                // etl info
                EtlStatus status = loadJob.getEtlJobStatus();
                if (status == null || status.getState() == TEtlState.CANCELLED) {
                    jobInfo.add("N/A");
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
                        jobInfo.add("N/A");
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
                    jobInfo.add("N/A");
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

                loadJobInfos.add(jobInfo);
            } // end for loadJobs

			LOG.debug("finished to get load job info, cost: {}", (System.currentTimeMillis() - start));
        } finally {
            readUnlock();
        }

        ListComparator<List<Comparable>> comparator = null;
        if (orderByPairs != null) {
            OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
            comparator = new ListComparator<List<Comparable>>(orderByPairs.toArray(orderByPairArr));
        } else {
            // sort by id asc
            comparator = new ListComparator<List<Comparable>>(0);
        }
        Collections.sort(loadJobInfos, comparator);
        return loadJobInfos;
    }

    public long getLatestJobIdByLabel(long dbId, String labelValue) {
		LoadJob job = null;
        long jobId = 0;
        try {
            readLock();
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
					job = loadJob;
                }
            }
        } finally {
            readUnlock();
        }

        return jobId;
    }

    public List<List<Comparable>> getLoadJobUnfinishedInfo(long jobId) {
        LinkedList<List<Comparable>> infos = new LinkedList<List<Comparable>>();
        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
        readLock();
        try {
            LoadJob loadJob = getLoadJob(jobId);
            if (loadJob == null
                    || (loadJob.getState() != JobState.LOADING && loadJob.getState() != JobState.QUORUM_FINISHED)) {
                return infos;
            }

            long dbId = loadJob.getDbId();
            Database db = Catalog.getInstance().getDb(dbId);
            if (db == null) {
                return infos;
            }

            db.readLock();
            try {
                Map<Long, TabletLoadInfo> tabletMap = loadJob.getIdToTabletLoadInfo();
                for (long tabletId : tabletMap.keySet()) {
                    TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
                    if (tabletMeta == null) {
                        // tablet may be dropped during loading
                        continue;
                    }

                    long tableId = tabletMeta.getTableId();

                    OlapTable table = (OlapTable) db.getTable(tableId);
                    if (table == null) {
                        continue;
                    }

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
                    long versionHash = partitionLoadInfo.getVersionHash();

                    for (Replica replica : tablet.getReplicas()) {
                        if (replica.checkVersionCatchUp(version, versionHash)) {
                            continue;
                        }

                        List<Comparable> info = Lists.newArrayList();
                        info.add(replica.getBackendId());
                        info.add(tabletId);
                        info.add(replica.getId());
                        info.add(replica.getVersion());
                        info.add(replica.getVersionHash());
                        info.add(partitionId);
                        info.add(version);
                        info.add(versionHash);

                        infos.add(info);
                    }
                } // end for tablet

            } finally {
                db.readUnlock();
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
        return loadErrorHubInfo;
    }

    public void setLoadErrorHubInfo(LoadErrorHub.Param info) {
        this.loadErrorHubInfo = info;
    }

    // Note: althrough this.loadErrorHubInfo is volatile, no need to lock.
    //       but editlog need be locked
    public void changeLoadErrorHubInfo(LoadErrorHub.Param info) {
		writeLock();
		try {
            this.loadErrorHubInfo = info;
            Catalog.getInstance().getEditLog().logSetLoadErrorHub(info);
        } finally {
            writeUnlock();
        }
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
    public void getJobInfo(JobInfo info) throws DdlException {
        String fullDbName = ClusterNamespace.getFullName(info.clusterName, info.dbName);
		info.dbName = fullDbName;
        Database db = Catalog.getInstance().getDb(fullDbName);
        if (db == null) {
            throw new DdlException("Unknown database(" + info.dbName + ")");
        }
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
                    OlapTable table = (OlapTable) db.getTable(info.getTableId());
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
                    replica.updateVersionInfo(info.getVersion(), info.getVersionHash(),
                                       info.getDataSize(), info.getRowCount());
                }
            }
    
            long jobId = job.getId();
            Map<Long, TableLoadInfo> idToTableLoadInfo = job.getIdToTableLoadInfo();
            if (idToTableLoadInfo != null) {
                for (Entry<Long, TableLoadInfo> tableEntry : idToTableLoadInfo.entrySet()) {
                    long tableId = tableEntry.getKey();
                    OlapTable table = (OlapTable) db.getTable(tableId);
                    TableLoadInfo tableLoadInfo = tableEntry.getValue();
                    for (Entry<Long, PartitionLoadInfo> entry : tableLoadInfo.getIdToPartitionLoadInfo().entrySet()) {
                        long partitionId = entry.getKey();
                        Partition partition = table.getPartition(partitionId);
                        PartitionLoadInfo partitionLoadInfo = entry.getValue();
                        if (!partitionLoadInfo.isNeedLoad()) {
                            continue;
                        }
                        updatePartitionVersion(partition, partitionLoadInfo.getVersion(), 
                                               partitionLoadInfo.getVersionHash(), jobId);
                        
                        // update table row count
                        for (MaterializedIndex materializedIndex : partition.getMaterializedIndices()) {
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
                        } // end for indices
                    } // end for partitions
                } // end for tables
            }
            
            idToLoadingLoadJob.remove(jobId);
            idToQuorumFinishedLoadJob.put(jobId, job);
        }
        replaceLoadJob(job);
    }
    
    public void replayQuorumLoadJob(LoadJob job, Catalog catalog) throws DdlException {
        // TODO: need to call this.writeLock()?
        Database db = catalog.getDb(job.getDbId());
        db.writeLock();
        try {
            writeLock();
            try {
                unprotectQuorumLoadJob(job, db);
            } finally {
                writeUnlock();
            }
        } finally {
            db.writeUnlock();
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
                    OlapTable table = (OlapTable) db.getTable(info.getTableId());
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
                    replica.updateVersionInfo(info.getVersion(), info.getVersionHash(),
                                       info.getDataSize(), info.getRowCount());
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
    
    public void replayFinishLoadJob(LoadJob job, Catalog catalog) {
        // TODO: need to call this.writeLock()?
        Database db = catalog.getDb(job.getDbId());
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
    
    public void replayClearRollupInfo(ReplicaPersistInfo info, Catalog catalog) {
        Database db = catalog.getDb(info.getDbId());
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(info.getTableId());
            Partition partition = olapTable.getPartition(info.getPartitionId());
            MaterializedIndex index = partition.getIndex(info.getIndexId());
            index.clearRollupIndexInfo();
        } finally {
            db.writeUnlock();
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
        
        if (!job.isSyncDeleteJob()) {
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
        } else {
            // Replace LoadJob in dbToLoadJobs
            List<LoadJob> jobs = dbToDeleteJobs.get(job.getDbId());
            if (jobs == null) {
                LOG.warn("Does not find db in dbToDeleteJobs. DbId : {}", 
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
                LOG.warn("Does not find delete load job for db. DbId : {}, jobId : {}", 
                        job.getDbId(), jobId);
                return;
            }
            jobs.remove(pos);
            jobs.add(pos, job);
        }
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
            if (dbToDeleteJobs.containsKey(dbId)) {
                dbToDeleteJobs.remove(dbId);
            }
        } finally {
            writeUnlock();
        }
    }

    // Added by ljb. Remove old load jobs from idToLoadJob, dbToLoadJobs and dbLabelToLoadJobs
    // This function is called periodically. every Configure.label_keep_max_second seconds
    public void removeOldLoadJobs() {
        long currentTimeMs = System.currentTimeMillis();
        
        writeLock();
        try {
            Iterator<Map.Entry<Long, LoadJob>> iter = idToLoadJob.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Long, LoadJob> entry = iter.next();
                LoadJob job = entry.getValue();
                if ((currentTimeMs - job.getCreateTimeMs()) / 1000 > Config.label_keep_max_second
                        && (job.getState() == JobState.FINISHED || job.getState() == JobState.CANCELLED)) {
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
                    
                    // remove delete job from dbToDeleteJobs
                    List<LoadJob> deleteJobs = dbToDeleteJobs.get(dbId);
                    if (deleteJobs != null) {
                        deleteJobs.remove(job);
                        if (deleteJobs.size() == 0) {
                            dbToDeleteJobs.remove(dbId);
                        }
                    }
                    
                    // Remove job from dbLabelToLoadJobs
                    Map<String, List<LoadJob>> mapLabelToJobs = dbLabelToLoadJobs.get(dbId);
                    if (mapLabelToJobs != null) {
                        loadJobs = mapLabelToJobs.get(label);
                        if (loadJobs != null) {
                            loadJobs.remove(job);
                            if (loadJobs.size() == 0) {
                                mapLabelToJobs.remove(label);
                                if (mapLabelToJobs.size() == 0) {
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
                
                // delete all dirs releated to job label, use "" instead of job.getEtlOutputDir()
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
                for (MiniEtlTaskInfo taskInfo : job.getMiniEtlTasks().values()) {
                    long backendId = taskInfo.getBackendId();
                    Backend backend = Catalog.getCurrentSystemInfo().getBackend(backendId);
                    if (backend == null) {
                        LOG.warn("backend does not exist. id: {}", backendId);
                        break;
                    }
                    
                    long dbId = job.getDbId();
                    Database db = Catalog.getInstance().getDb(dbId);
                    if (db == null) {
                        LOG.warn("db does not exist. id: {}", dbId);
                        break;
                    }
                    
                    AgentClient client = new AgentClient(backend.getHost(), backend.getBePort());
                    client.deleteEtlFiles(dbId, job.getId(), db.getFullName(), job.getLabel());
                }
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
        return updateLoadJobState(job, destState, CancelType.UNKNOWN, null);
    }
   
    public boolean updateLoadJobState(LoadJob job, JobState destState, CancelType cancelType, String msg) {
        boolean result = true;
        JobState srcState = null;

        long jobId = job.getId();
        long dbId = job.getDbId();
        Database db = Catalog.getInstance().getDb(dbId);
        String errMsg = msg;
        if (db == null) {
            // if db is null, update job to cancelled
            errMsg = "db does not exist. id: " + dbId;
            LOG.warn(errMsg);
            writeLock();
            try {
                // sometimes db is dropped and then cancel the job, the job must have transactionid
                // transaction state should only be dropped when job is dropped
                processCancelled(job, cancelType, errMsg);
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
                            Catalog.getInstance().getEditLog().logLoadEtl(job);
                            break;
                        case LOADING:
                            idToEtlLoadJob.remove(jobId);
                            idToLoadingLoadJob.put(jobId, job);
                            job.setProgress(0);
                            job.setLoadStartTimeMs(System.currentTimeMillis());
                            job.setState(destState);
                            Catalog.getInstance().getEditLog().logLoadLoading(job);
                            break;
                        case QUORUM_FINISHED:
                            if (processQuorumFinished(job, db)) {
                                // Write edit log
                                Catalog.getInstance().getEditLog().logLoadQuorum(job);
                            } else {
                                errMsg = "process loading finished fail";
                                processCancelled(job, cancelType, errMsg);
                            }
                            break;
                        case FINISHED:
                            if (job.getTransactionId() > 0) {
                                idToPendingLoadJob.remove(jobId);
                                idToLoadingLoadJob.remove(jobId);
                                job.setProgress(100);
                                job.setLoadFinishTimeMs(System.currentTimeMillis());
                                // if this is a sync delete job, then update affected version and version hash
                                if (job.isSyncDeleteJob()) {
                                    TransactionState transactionState = Catalog.getCurrentGlobalTransactionMgr()
                                            .getTransactionState(job.getTransactionId());
                                    DeleteInfo deleteInfo = job.getDeleteInfo();
                                    TableCommitInfo tableCommitInfo = transactionState.getTableCommitInfo(deleteInfo.getTableId());
                                    PartitionCommitInfo partitionCommitInfo = tableCommitInfo.getPartitionCommitInfo(deleteInfo.getPartitionId());
                                    deleteInfo.updatePartitionVersionInfo(partitionCommitInfo.getVersion(), 
                                            partitionCommitInfo.getVersionHash());
                                }
                            }
                            MetricRepo.COUNTER_LOAD_FINISHED.increase(1L);
                            idToQuorumFinishedLoadJob.remove(jobId);
                            job.setState(destState);

                            // clear push tasks
                            for (PushTask pushTask : job.getPushTasks()) {
                                AgentTaskQueue.removePushTask(pushTask.getBackendId(), pushTask.getSignature(),
                                                              pushTask.getVersion(), pushTask.getVersionHash(),
                                                              pushTask.getPushType(), pushTask.getTaskType());
                            }
                            // Clear the Map and Set in this job, reduce the memory cost for finished load job.
                            // for delete job, keep the map and set because some of them is used in show proc method
                            if (!job.isSyncDeleteJob()) {
                                job.clearRedundantInfoForHistoryJob();
                            }
                            // Write edit log
                            Catalog.getInstance().getEditLog().logLoadDone(job);
                            break;
                        case CANCELLED:
                            processCancelled(job, cancelType, errMsg);
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
            OlapTable table = (OlapTable) db.getTable(tableId);
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
            OlapTable table = (OlapTable) db.getTable(tableId);

            TableLoadInfo tableLoadInfo = tableEntry.getValue();
            for (Entry<Long, PartitionLoadInfo> entry : tableLoadInfo.getIdToPartitionLoadInfo().entrySet()) {
                long partitionId = entry.getKey();
                Partition partition = table.getPartition(partitionId);
                PartitionLoadInfo partitionLoadInfo = entry.getValue();
                if (!partitionLoadInfo.isNeedLoad()) {
                    continue;
                }

                updatePartitionVersion(partition, partitionLoadInfo.getVersion(), 
                                       partitionLoadInfo.getVersionHash(), jobId);
                
                for (MaterializedIndex materializedIndex : partition.getMaterializedIndices()) {
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

    private void updatePartitionVersion(Partition partition, long version, long versionHash, long jobId) {
        long partitionId = partition.getId();
        partition.updateVisibleVersionAndVersionHash(version, versionHash);
        LOG.info("update partition version success. version: {}, version hash: {}, job id: {}, partition id: {}",
                 version, versionHash, jobId, partitionId);
    }
   
    private boolean processCancelled(LoadJob job, CancelType cancelType, String msg) {
        long jobId = job.getId();
        JobState srcState = job.getState();
        CancelType tmpCancelType = CancelType.UNKNOWN;
        // should abort in transaction manager first because it maybe abort job successfully and abort in transaction manager failed
        // then there will be rubbish transactions in transaction manager
        try {
            Catalog.getCurrentGlobalTransactionMgr().abortTransaction(
                    job.getTransactionId(), 
                    job.getFailMsg().toString());
        } catch (Exception e) {
            LOG.info("errors while abort transaction", e);
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
                                              pushTask.getVersion(), pushTask.getVersionHash(),
                                              pushTask.getPushType(), pushTask.getTaskType());
            }
        }
        
        // Clear the Map and Set in this job, reduce the memory cost of canceled load job.
        job.clearRedundantInfoForHistoryJob();
        Catalog.getInstance().getEditLog().logLoadCancel(job);

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

    public void unprotectAddDeleteInfo(DeleteInfo deleteInfo) {
        long dbId = deleteInfo.getDbId();
        List<DeleteInfo> deleteInfos = dbToDeleteInfos.get(dbId);
        if (deleteInfos == null) {
            deleteInfos = Lists.newArrayList();
            dbToDeleteInfos.put(dbId, deleteInfos);
        }
        deleteInfos.add(deleteInfo);

        if (deleteInfo.getAsyncDeleteJob() != null && deleteInfo.getState() == DeleteState.QUORUM_FINISHED) {
            AsyncDeleteJob asyncDeleteJob = deleteInfo.getAsyncDeleteJob();
            idToQuorumFinishedDeleteJob.put(asyncDeleteJob.getJobId(), asyncDeleteJob);
            LOG.info("unprotected add asyncDeleteJob when load image: {}", asyncDeleteJob.getJobId());
        }
    }

    public void unprotectDelete(DeleteInfo deleteInfo, Database db) {
        OlapTable table = (OlapTable) db.getTable(deleteInfo.getTableId());
        Partition partition = table.getPartition(deleteInfo.getPartitionId());
        updatePartitionVersion(partition, deleteInfo.getPartitionVersion(), deleteInfo.getPartitionVersionHash(), -1);

        List<ReplicaPersistInfo> replicaInfos = deleteInfo.getReplicaPersistInfos();
        if (replicaInfos != null) {
            for (ReplicaPersistInfo info : replicaInfos) {
                MaterializedIndex index = partition.getIndex(info.getIndexId());
                Tablet tablet = index.getTablet(info.getTabletId());
                Replica replica = tablet.getReplicaById(info.getReplicaId());
                replica.updateVersionInfo(info.getVersion(), info.getVersionHash(),
                                   info.getDataSize(), info.getRowCount());
            }
        }

        // add to deleteInfos
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_11) {
            long dbId = deleteInfo.getDbId();
            List<DeleteInfo> deleteInfos = dbToDeleteInfos.get(dbId);
            if (deleteInfos == null) {
                deleteInfos = Lists.newArrayList();
                dbToDeleteInfos.put(dbId, deleteInfos);
            }
            deleteInfos.add(deleteInfo);
        }

        if (deleteInfo.getAsyncDeleteJob() != null) {
            AsyncDeleteJob asyncDeleteJob = deleteInfo.getAsyncDeleteJob();
            idToQuorumFinishedDeleteJob.put(asyncDeleteJob.getJobId(), asyncDeleteJob);
            LOG.info("unprotected add asyncDeleteJob: {}", asyncDeleteJob.getJobId());
        }
    }
    
    public void replayFinishAsyncDeleteJob(AsyncDeleteJob deleteJob, Catalog catalog) {
        Database db = catalog.getDb(deleteJob.getDbId());
        db.writeLock();
        try {
            writeLock();
            try {
                // Update database information
                Map<Long, ReplicaPersistInfo> replicaInfos = deleteJob.getReplicaPersistInfos();
                if (replicaInfos != null) {
                    for (ReplicaPersistInfo info : replicaInfos.values()) {
                        OlapTable table = (OlapTable) db.getTable(info.getTableId());
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
                        replica.updateVersionInfo(info.getVersion(), info.getVersionHash(),
                                           info.getDataSize(), info.getRowCount());
                    }
                }
            } finally {
                writeUnlock();
            }
        } finally {
            db.writeUnlock();
        }

        removeDeleteJobAndSetState(deleteJob);
        LOG.info("unprotected finish asyncDeleteJob: {}", deleteJob.getJobId());
    }

    public void replayDelete(DeleteInfo deleteInfo, Catalog catalog) {
        Database db = catalog.getDb(deleteInfo.getDbId());
        db.writeLock();
        try {
            writeLock();
            try {
                unprotectDelete(deleteInfo, db);
            } finally {
                writeUnlock();
            }
        } finally {
            db.writeUnlock();
        }
    }
    private void checkDeleteV2(OlapTable table, Partition partition, List<Predicate> conditions, List<String> deleteConditions, boolean preCheck)
                    throws DdlException {

        // check partition state
        PartitionState state = partition.getState();
        if (state != PartitionState.NORMAL) {
            // ErrorReport.reportDdlException(ErrorCode.ERR_BAD_PARTITION_STATE, partition.getName(), state.name());
            throw new DdlException("Partition[" + partition.getName() + "]' state is not NORNAL: " + state.name());
        }
        // do not need check whether partition has loading job
        
        // async delete job does not exist any more

        // check condition column is key column and condition value
        Map<String, Column> nameToColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (Column column : table.getBaseSchema()) {
            nameToColumn.put(column.getName(), column);
        }
        for (Predicate condition : conditions) {
            SlotRef slotRef = null;
            if (condition instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) condition;
                slotRef = (SlotRef) binaryPredicate.getChild(0);
            } else if (condition instanceof IsNullPredicate) {
                IsNullPredicate isNullPredicate = (IsNullPredicate) condition;
                slotRef = (SlotRef) isNullPredicate.getChild(0);
            }
            String columnName = slotRef.getColumnName();
            if (!nameToColumn.containsKey(columnName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_FIELD_ERROR, columnName, table.getName());
            }
            
            Column column = nameToColumn.get(columnName);
            if (!column.isKey()) {
                // ErrorReport.reportDdlException(ErrorCode.ERR_NOT_KEY_COLUMN, columnName);
                throw new DdlException("Column[" + columnName + "] is not key column");
            }
            
            if (condition instanceof BinaryPredicate) {
                String value = null;
                try {
                    BinaryPredicate binaryPredicate = (BinaryPredicate) condition;
                    value = ((LiteralExpr) binaryPredicate.getChild(1)).getStringValue();
                    LiteralExpr.create(value, Type.fromPrimitiveType(column.getDataType()));
                } catch (AnalysisException e) {
                    // ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_VALUE, value);
                    throw new DdlException("Invalid column value[" + value + "]");
                }
            }

            // set schema column name
            slotRef.setCol(column.getName());
        }
        Map<Long, List<Column>> indexIdToSchema = table.getIndexIdToSchema();
        for (MaterializedIndex index : partition.getMaterializedIndices()) {
            // check table has condition column
            Map<String, Column> indexNameToColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            for (Column column : indexIdToSchema.get(index.getId())) {
                indexNameToColumn.put(column.getName(), column);
            }
            String indexName = table.getIndexNameById(index.getId());
            for (Predicate condition : conditions) {
                String columnName = null;
                if (condition instanceof BinaryPredicate) {
                    BinaryPredicate binaryPredicate = (BinaryPredicate) condition;
                    columnName = ((SlotRef) binaryPredicate.getChild(0)).getColumnName();
                } else if (condition instanceof IsNullPredicate) {
                    IsNullPredicate isNullPredicate = (IsNullPredicate) condition;
                    columnName = ((SlotRef) isNullPredicate.getChild(0)).getColumnName();
                }
                Column column = indexNameToColumn.get(columnName);
                if (column == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_BAD_FIELD_ERROR, columnName, indexName);
                }

                if (table.getKeysType() == KeysType.DUP_KEYS && !column.isKey()) {
                    throw new DdlException("Column[" + columnName + "] is not key column in index[" + indexName + "]");
                }
            }

            // do not need to check replica version and backend alive
            
        } // end for indices
        
        if (deleteConditions == null) {
            return;
        }

        // save delete conditions
        for (Predicate condition : conditions) {
            if (condition instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) condition;
                SlotRef slotRef = (SlotRef) binaryPredicate.getChild(0);
                String columnName = slotRef.getColumnName();
                StringBuilder sb = new StringBuilder();
                sb.append(columnName).append(" ").append(binaryPredicate.getOp().name()).append(" \"")
                    .append(((LiteralExpr) binaryPredicate.getChild(1)).getStringValue()).append("\"");
                deleteConditions.add(sb.toString());
            } else if (condition instanceof IsNullPredicate) {
                IsNullPredicate isNullPredicate = (IsNullPredicate) condition;
                SlotRef slotRef = (SlotRef) isNullPredicate.getChild(0);
                String columnName = slotRef.getColumnName();
                StringBuilder sb = new StringBuilder();
                sb.append(columnName);
                if (isNullPredicate.isNotNull()) {
                    sb.append(" IS NOT NULL");
                } else {
                    sb.append(" IS NULL");
                }
                deleteConditions.add(sb.toString());
            }
        }
    }
    
    private void checkDelete(OlapTable table, Partition partition, List<Predicate> conditions,
                             long checkVersion, long checkVersionHash, List<String> deleteConditions,
                             Map<Long, Set<Long>> asyncTabletIdToBackends, boolean preCheck)
            throws DdlException {
        // check partition state
        PartitionState state = partition.getState();
        if (state != PartitionState.NORMAL) {
            // ErrorReport.reportDdlException(ErrorCode.ERR_BAD_PARTITION_STATE, partition.getName(), state.name());
            throw new DdlException("Partition[" + partition.getName() + "]' state is not NORNAL: " + state.name());
        }
        
        // check running load job
        List<LoadJob> quorumFinishedLoadJobs = Lists.newArrayList();
        if (!checkPartitionLoadFinished(partition.getId(), quorumFinishedLoadJobs)) {
            // ErrorReport.reportDdlException(ErrorCode.ERR_PARTITION_HAS_LOADING_JOBS, partition.getName());
            throw new DdlException("Partition[" + partition.getName() + "] has unfinished load jobs");
        }
        
        // get running async delete job
        List<AsyncDeleteJob> asyncDeleteJobs = getCopiedAsyncDeleteJobs();

        // check condition column is key column and condition value
        Map<String, Column> nameToColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (Column column : table.getBaseSchema()) {
            nameToColumn.put(column.getName(), column);
        }
        for (Predicate condition : conditions) {
            SlotRef slotRef = null;
            if (condition instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) condition;
                slotRef = (SlotRef) binaryPredicate.getChild(0);
            } else if (condition instanceof IsNullPredicate) {
                IsNullPredicate isNullPredicate = (IsNullPredicate) condition;
                slotRef = (SlotRef) isNullPredicate.getChild(0);
            }
            String columnName = slotRef.getColumnName();
            if (!nameToColumn.containsKey(columnName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_FIELD_ERROR, columnName, table.getName());
            }
            
            Column column = nameToColumn.get(columnName);
            if (!column.isKey()) {
                // ErrorReport.reportDdlException(ErrorCode.ERR_NOT_KEY_COLUMN, columnName);
                throw new DdlException("Column[" + columnName + "] is not key column");
            }
            
            if (condition instanceof BinaryPredicate) {
                String value = null;
                try {
                    BinaryPredicate binaryPredicate = (BinaryPredicate) condition;
                    value = ((LiteralExpr) binaryPredicate.getChild(1)).getStringValue();
                    LiteralExpr.create(value, Type.fromPrimitiveType(column.getDataType()));
                } catch (AnalysisException e) {
                    // ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_VALUE, value);
                    throw new DdlException("Invalid column value[" + value + "]");
                }
            }

            // set schema column name
            slotRef.setCol(column.getName());
        }
        
        long tableId = table.getId();
        long partitionId = partition.getId();
        Map<Long, List<Column>> indexIdToSchema = table.getIndexIdToSchema();
        for (MaterializedIndex index : partition.getMaterializedIndices()) {
            // check table has condition column
            Map<String, Column> indexNameToColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            for (Column column : indexIdToSchema.get(index.getId())) {
                indexNameToColumn.put(column.getName(), column);
            }
            String indexName = table.getIndexNameById(index.getId());
            for (Predicate condition : conditions) {
                String columnName = null;
                if (condition instanceof BinaryPredicate) {
                    BinaryPredicate binaryPredicate = (BinaryPredicate) condition;
                    columnName = ((SlotRef) binaryPredicate.getChild(0)).getColumnName();
                } else if (condition instanceof IsNullPredicate) {
                    IsNullPredicate isNullPredicate = (IsNullPredicate) condition;
                    columnName = ((SlotRef) isNullPredicate.getChild(0)).getColumnName();
                }
                Column column = indexNameToColumn.get(columnName);
                if (column == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_BAD_FIELD_ERROR, columnName, indexName);
                }

                if (table.getKeysType() == KeysType.DUP_KEYS && !column.isKey()) {
                    throw new DdlException("Column[" + columnName + "] is not key column in index[" + indexName + "]");
                }
            }

            // check replica version and backend alive
            short replicationNum = table.getPartitionInfo().getReplicationNum(partition.getId());
            for (Tablet tablet : index.getTablets()) {
                Set<Long> needAsyncBackendIds = Sets.newHashSet();
                for (Replica replica : tablet.getReplicas()) {
                    if (!Catalog.getCurrentSystemInfo().checkBackendAvailable(replica.getBackendId())) {
                        LOG.warn("backend[{}] is not alive when delete check. pre: {}",
                                 replica.getBackendId(), preCheck);
                        needAsyncBackendIds.add(replica.getBackendId());
                        continue;
                    }

                    // check replica version.
                    // here is a little bit confused. the main idea is
                    // 1. check if replica catch up the version
                    // 2. if not catch up and this is pre check, make sure there will be right quorum finished load jobs 
                    //    to fill the version gap between 'replica committed version' and 'partition committed version'.
                    // 3. if not catch up and this is after check
                    //      1) if diff version == 1, some sync delete task may failed. add async delete task.
                    //      2) if diff version > 1, make sure there will be right quorum finished load jobs 
                    //         to fill the version gap between 'replica committed version' and 'delete version - 1'.
                    // if ok, add async delete task.
                    if (!replica.checkVersionCatchUp(checkVersion, checkVersionHash)) {
                        long replicaVersion = replica.getVersion();
                        if (replicaVersion == checkVersion) {
                            // in this case, version is same but version hash is not.
                            // which mean the current replica version is a non-committed version.
                            // so the replica's committed version should be the previous one.
                            --replicaVersion;
                        }

                        // the *diffVersion* is number of versions need to be check
                        // for now:
                        //  *replicaVersion* : the 'committed version' of the replica
                        //  *checkVersion* : 
                        //      1) if preCheck, this is partition committed version
                        //      2) if not preCheck, this is delete version
                        long diffVersion = checkVersion - replicaVersion;
                        Preconditions.checkState(diffVersion > 0);
                        for (int i = 1; i <= diffVersion; i++) {
                            boolean find = false;
                            long theVersion = replicaVersion + i;
                            for (LoadJob loadJob : quorumFinishedLoadJobs) {
                                if (theVersion == loadJob.getPartitionLoadInfo(tableId, partitionId).getVersion()) {
                                    find = true;
                                    break;
                                }
                            }

                            for (AsyncDeleteJob deleteJob : asyncDeleteJobs) {
                                if (tableId == deleteJob.getTableId() && partitionId == deleteJob.getPartitionId()
                                        && theVersion == deleteJob.getPartitionVersion()) {
                                    find = true;
                                    break;
                                }
                            }

                            if (!find) {
                                if (theVersion == checkVersion && !preCheck) {
                                    // the sync delete task of this replica may failed.
                                    // add async delete task after.
                                    continue;
                                } else {
                                    // this should not happend. add log to observe.
                                    LOG.error("replica version does not catch up with version: {}-{}. "
                                            + "replica: {}-{}-{}-{}",
                                              checkVersion, checkVersionHash, replica.getId(), tablet.getId(),
                                              replica.getBackendId(), replica.getState());
                                    throw new DdlException("Replica[" + tablet.getId() + "-" + replica.getId()
                                            + "] is not catch up with version: " + checkVersion + "-"
                                            + replica.getVersion());
                                }
                            }
                        }

                        needAsyncBackendIds.add(replica.getBackendId());
                    } // end check replica version
                } // end for replicas

                if (replicationNum - needAsyncBackendIds.size() < replicationNum / 2 + 1) {
                    String backendsStr = Joiner.on(", ").join(needAsyncBackendIds);
                    LOG.warn("too many unavailable replica in tablet[{}], backends:[{}]", tablet.getId(), backendsStr);
                    throw new DdlException("Too many replicas are not available. Wait 10 mins and try again."
                            + " if still not work, contact Palo RD");
                }

                if (!needAsyncBackendIds.isEmpty()) {
                    LOG.info("add tablet[{}] to async delete. backends: {}",
                             tablet.getId(), needAsyncBackendIds);
                    asyncTabletIdToBackends.put(tablet.getId(), needAsyncBackendIds);
                }
            } // end for tablets
        } // end for indices
        
        if (deleteConditions == null) {
            return;
        }

        // save delete conditions
        for (Predicate condition : conditions) {
            if (condition instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) condition;
                SlotRef slotRef = (SlotRef) binaryPredicate.getChild(0);
                String columnName = slotRef.getColumnName();
                StringBuilder sb = new StringBuilder();
                sb.append(columnName).append(" ").append(binaryPredicate.getOp().name()).append(" \"")
                    .append(((LiteralExpr) binaryPredicate.getChild(1)).getStringValue()).append("\"");
                deleteConditions.add(sb.toString());
            } else if (condition instanceof IsNullPredicate) {
                IsNullPredicate isNullPredicate = (IsNullPredicate) condition;
                SlotRef slotRef = (SlotRef) isNullPredicate.getChild(0);
                String columnName = slotRef.getColumnName();
                StringBuilder sb = new StringBuilder();
                sb.append(columnName);
                if (isNullPredicate.isNotNull()) {
                    sb.append(" IS NOT NULL");
                } else {
                    sb.append(" IS NULL");
                }
                deleteConditions.add(sb.toString());
            }
        }
    }

    private void checkAndAddRunningSyncDeleteJob(long partitionId, String partitionName) throws DdlException {
        // check if there are synchronized delete job under going
        writeLock();
        try {
            checkHasRunningSyncDeleteJob(partitionId, partitionName);
            partitionUnderDelete.add(partitionId);
        } finally {
            writeUnlock();
        }
    }

    private void checkHasRunningSyncDeleteJob(long partitionId, String partitionName) throws DdlException {
        // check if there are synchronized delete job under going
        readLock();
        try {
            if (partitionUnderDelete.contains(partitionId)) {
                throw new DdlException("Partition[" + partitionName + "] has running delete job. See 'SHOW DELETE'");
            }
        } finally {
            readUnlock();
        }
    }

    private void checkHasRunningAsyncDeleteJob(long partitionId, String partitionName) throws DdlException {
        readLock();
        try {
            for (AsyncDeleteJob job : idToQuorumFinishedDeleteJob.values()) {
                if (job.getPartitionId() == partitionId) {
                    throw new DdlException("Partition[" + partitionName + "] has running async delete job. "
                            + "See 'SHOW DELETE'");
                }
            }
            for (long dbId : dbToDeleteJobs.keySet()) {
                List<LoadJob> loadJobs = dbToDeleteJobs.get(dbId);
                for (LoadJob loadJob : loadJobs) {
                    if (loadJob.getDeleteInfo().getPartitionId() == partitionId 
                            && loadJob.getState() == JobState.LOADING) {
                        throw new DdlException("Partition[" + partitionName + "] has running async delete job. "
                                + "See 'SHOW DELETE'");
                    }
                }
            }
        } finally {
            readUnlock();
        }
    }

    public void checkHashRunningDeleteJob(long partitionId, String partitionName) throws DdlException {
        checkHasRunningSyncDeleteJob(partitionId, partitionName);
        checkHasRunningAsyncDeleteJob(partitionId, partitionName);
    }

    public void delete(DeleteStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();
        String partitionName = stmt.getPartitionName();
        List<Predicate> conditions = stmt.getDeleteConditions();
        Database db = Catalog.getInstance().getDb(dbName);
        if (db == null) {
            throw new DdlException("Db does not exist. name: " + dbName);
        }

        long tableId = -1;
        long partitionId = -1;
        LoadJob loadDeleteJob = null;
        db.readLock();
        try {
            Table table = db.getTable(tableName);
            if (table == null) {
                throw new DdlException("Table does not exist. name: " + tableName);
            }

            if (table.getType() != TableType.OLAP) {
                throw new DdlException("Not olap type table. type: " + table.getType().name());
            }
            OlapTable olapTable = (OlapTable) table;

            if (olapTable.getState() != OlapTableState.NORMAL) {
                throw new DdlException("Table's state is not normal: " + tableName);
            }

            tableId = olapTable.getId();
            Partition partition = olapTable.getPartition(partitionName);
            if (partition == null) {
                throw new DdlException("Partition does not exist. name: " + partitionName);
            }
            partitionId = partition.getId();

            List<String> deleteConditions = Lists.newArrayList();
            // pre check
            checkDeleteV2(olapTable, partition, conditions,
                    deleteConditions, true);
            checkAndAddRunningSyncDeleteJob(partitionId, partitionName);
            // do not use transaction id generator, or the id maybe duplicated
            long jobId = Catalog.getInstance().getNextId();
            String jobLabel = "delete_" + UUID.randomUUID();
            // the version info in delete info will be updated after job finished
            DeleteInfo deleteInfo = new DeleteInfo(db.getId(), tableId, tableName,
                                        partition.getId(), partitionName, 
                                        -1, 0, deleteConditions);
            loadDeleteJob = new LoadJob(jobId, db.getId(), tableId, 
                    partitionId, jobLabel, olapTable.getIndexIdToSchemaHash(), conditions, deleteInfo);
            Map<Long, TabletLoadInfo> idToTabletLoadInfo = Maps.newHashMap();
            for (MaterializedIndex materializedIndex : partition.getMaterializedIndices()) {
                for (Tablet tablet : materializedIndex.getTablets()) {
                    long tabletId = tablet.getId();
                    // tabletLoadInfo is empty, because delete load does not need filepath filesize info
                    TabletLoadInfo tabletLoadInfo = new TabletLoadInfo("", -1);
                    idToTabletLoadInfo.put(tabletId, tabletLoadInfo);
                }
            }
            loadDeleteJob.setIdToTabletLoadInfo(idToTabletLoadInfo);
            loadDeleteJob.setState(JobState.LOADING);
            long transactionId = Catalog.getCurrentGlobalTransactionMgr().beginTransaction(db.getId(), jobLabel, 
                    "fe", LoadJobSourceType.FRONTEND);
            loadDeleteJob.setTransactionId(transactionId);
            // the delete job will be persist in editLog
            addLoadJob(loadDeleteJob, db);
        } catch (Throwable t) {
            LOG.debug("error occurred during prepare delete", t);
            throw new DdlException(t.getMessage(), t);
        } finally {
            db.readUnlock();
        }
        try {
            // TODO  wait loadDeleteJob to finished, using while true? or condition wait
            long startDeleteTime = System.currentTimeMillis();
            long timeout = loadDeleteJob.getDeleteJobTimeout();
            while (true) {
                db.writeLock();
                try {
                    if (loadDeleteJob.getState() == JobState.FINISHED 
                            || loadDeleteJob.getState() == JobState.CANCELLED) {
                        break;
                    }
                    if (System.currentTimeMillis() - startDeleteTime > timeout) {
                        TransactionState transactionState = Catalog.getCurrentGlobalTransactionMgr().getTransactionState(loadDeleteJob.getTransactionId());
                        if (transactionState.getTransactionStatus() == TransactionStatus.PREPARE) {
                            boolean isSuccess = cancelLoadJob(loadDeleteJob, CancelType.TIMEOUT, "load delete job timeout");
                            if (isSuccess) {
                                throw new DdlException("timeout when waiting delete");
                            }
                        }
                    }
                } finally {
                    db.writeUnlock();
                }
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            String failMsg = "delete unknown, " + e.getMessage();
            LOG.warn(failMsg, e);
            throw new DdlException(failMsg);
        } finally {
            writeLock();
            try {
                partitionUnderDelete.remove(partitionId);
            } finally {
                writeUnlock();
            }
        }
    }
    
    public void deleteOld(DeleteStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();
        String partitionName = stmt.getPartitionName();
        List<Predicate> conditions = stmt.getDeleteConditions();
        Database db = Catalog.getInstance().getDb(dbName);
        if (db == null) {
            throw new DdlException("Db does not exist. name: " + dbName);
        }
        
        DeleteInfo deleteInfo = null;

        long tableId = -1;
        long partitionId = -1;
        long visibleVersion = -1;
        long visibleVersionHash = -1;
        long newVersion = -1;
        long newVersionHash = -1;
        AgentBatchTask deleteBatchTask = null;
        int totalReplicaNum = 0;
        Map<Long, Set<Long>> asyncTabletIdToBackends = Maps.newHashMap();
        db.readLock();
        try {
            Table table = db.getTable(tableName);
            if (table == null) {
                throw new DdlException("Table does not exist. name: " + tableName);
            }

            if (table.getType() != TableType.OLAP) {
                throw new DdlException("Not olap type table. type: " + table.getType().name());
            }
            OlapTable olapTable = (OlapTable) table;

            if (olapTable.getState() != OlapTableState.NORMAL) {
                throw new DdlException("Table's state is not normal: " + tableName);
            }

            tableId = olapTable.getId();
            Partition partition = olapTable.getPartition(partitionName);
            if (partition == null) {
                throw new DdlException("Partition does not exist. name: " + partitionName);
            }
            partitionId = partition.getId();
            
            // pre check
            visibleVersion = partition.getVisibleVersion();
            visibleVersionHash = partition.getVisibleVersionHash();
            checkDelete(olapTable, partition, conditions, visibleVersion, visibleVersionHash,
                        null, asyncTabletIdToBackends, true);

            newVersion = visibleVersion + 1;
            newVersionHash = Util.generateVersionHash();
            deleteInfo = new DeleteInfo(db.getId(), tableId, tableName,
                                        partition.getId(), partitionName, 
                                        newVersion, newVersionHash, null);
            
            checkAndAddRunningSyncDeleteJob(deleteInfo.getPartitionId(), partitionName);

            // create sync delete tasks
            deleteBatchTask = new AgentBatchTask();
            for (MaterializedIndex materializedIndex : partition.getMaterializedIndices()) {
                int schemaHash = olapTable.getSchemaHashByIndexId(materializedIndex.getId());
                for (Tablet tablet : materializedIndex.getTablets()) {
                    long tabletId = tablet.getId();
                    for (Replica replica : tablet.getReplicas()) {

                        if (asyncTabletIdToBackends.containsKey(tabletId)
                                && asyncTabletIdToBackends.get(tabletId).contains(replica.getBackendId())) {
                            continue;
                        }

                        AgentTask pushTask = new PushTask(null, replica.getBackendId(), db.getId(),
                                                          tableId, partition.getId(),
                                                          materializedIndex.getId(), tabletId, replica.getId(),
                                                          schemaHash, newVersion,
                                                          newVersionHash, null, -1L, 0, -1L, TPushType.DELETE,
                                                          conditions, false, TPriority.HIGH);
                        if (AgentTaskQueue.addTask(pushTask)) {
                            deleteBatchTask.addTask(pushTask);
                            ++totalReplicaNum;
                        }
                    }
                }
            }
        } finally {
            db.readUnlock();
        }
        
        // send tasks to backends
        MarkedCountDownLatch countDownLatch = new MarkedCountDownLatch(totalReplicaNum);
        for (AgentTask task : deleteBatchTask.getAllTasks()) {
            countDownLatch.addMark(task.getBackendId(), task.getSignature());
            ((PushTask) task).setCountDownLatch(countDownLatch);
        }
        AgentTaskExecutor.submit(deleteBatchTask);
        long timeout = Config.tablet_delete_timeout_second * 1000L * totalReplicaNum;
        boolean ok = false;
        try {
            ok = countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOG.warn("InterruptedException: ", e);
            ok = false;
        }

        if (!ok) {
            // sync delete failed for unknown reason.
            // use async delete to try to make up after.
            LOG.warn("sync delete failed. try async delete. table: {}, partition: {}", tableName, partitionName);
        }

        Partition partition = null;
        try {
            // after check
            db.writeLock();
            try {
                OlapTable table = (OlapTable) db.getTable(tableName);
                if (table == null) {
                    throw new DdlException("Table does not exist. name: " + tableName);
                }

                partition = table.getPartition(partitionName);
                if (partition == null) {
                    throw new DdlException("Partition does not exist. name: " + partitionName);
                }

                // after check
                // 1. check partition committed version first
                if (partition.getVisibleVersion() > visibleVersion
                        || (visibleVersion == partition.getVisibleVersion()
                        && visibleVersionHash != partition.getVisibleVersionHash())) {
                    LOG.warn("before delete version: {}-{}. after delete version: {}-{}",
                             visibleVersion, visibleVersionHash,
                             partition.getVisibleVersion(), partition.getVisibleVersionHash());
                    throw new DdlException("There may have some load job done during delete job. Try again");
                }

                // 2. after check
                List<String> deleteConditions = Lists.newArrayList();
                checkDelete(table, partition, conditions, newVersion, newVersionHash, deleteConditions,
                            asyncTabletIdToBackends, false);
                deleteInfo.setDeleteConditions(deleteConditions);

                // update partition's version
                updatePartitionVersion(partition, newVersion, newVersionHash, -1);

                for (MaterializedIndex materializedIndex : partition.getMaterializedIndices()) {
                    long indexId = materializedIndex.getId();
                    for (Tablet tablet : materializedIndex.getTablets()) {
                        long tabletId = tablet.getId();
                        for (Replica replica : tablet.getReplicas()) {
                            ReplicaPersistInfo info =
                                    ReplicaPersistInfo.createForCondDelete(indexId,
                                                                           tabletId,
                                                                           replica.getId(),
                                                                           replica.getVersion(),
                                                                           replica.getVersionHash(),
                                                                           replica.getDataSize(),
                                                                           replica.getRowCount(), 
                                                                           replica.getLastFailedVersion(),
                                                                           replica.getLastFailedVersionHash(),
                                                                           replica.getLastSuccessVersion(),
                                                                           replica.getLastSuccessVersionHash());
                            deleteInfo.addReplicaPersistInfo(info);
                        }
                    }
                }

                writeLock();
                try {
                    // handle async delete jobs
                    if (!asyncTabletIdToBackends.isEmpty()) {
                        AsyncDeleteJob asyncDeleteJob = new AsyncDeleteJob(db.getId(), tableId, partition.getId(),
                                                                           newVersion, newVersionHash,
                                                                           conditions);
                        for (Long tabletId : asyncTabletIdToBackends.keySet()) {
                            asyncDeleteJob.addTabletId(tabletId);
                        }
                        deleteInfo.setAsyncDeleteJob(asyncDeleteJob);
                        idToQuorumFinishedDeleteJob.put(asyncDeleteJob.getJobId(), asyncDeleteJob);
                        LOG.info("finished create async delete job: {}", asyncDeleteJob.getJobId());
                    }
                    
                    // save delete info
                    List<DeleteInfo> deleteInfos = dbToDeleteInfos.get(db.getId());
                    if (deleteInfos == null) {
                        deleteInfos = Lists.newArrayList();
                        dbToDeleteInfos.put(db.getId(), deleteInfos);
                    }
                    deleteInfos.add(deleteInfo);
                } finally {
                    writeUnlock();
                }

                // Write edit log
                Catalog.getInstance().getEditLog().logFinishSyncDelete(deleteInfo);
                LOG.info("delete job finished at: {}. table: {}, partition: {}",
                         TimeUtils.longToTimeString(System.currentTimeMillis()), tableName, partitionName);
            } finally {
                db.writeUnlock();
            }
        } catch (Exception e) {
            // cancel delete
            // need not save cancel delete task in AgentTaskQueue
            AgentBatchTask cancelDeleteBatchTask = new AgentBatchTask();
            for (AgentTask task : deleteBatchTask.getAllTasks()) {
                PushTask pushTask = (PushTask) task;
                CancelDeleteTask cancelDeleteTask =
                        new CancelDeleteTask(task.getBackendId(), task.getDbId(), task.getTableId(),
                                             task.getPartitionId(), task.getIndexId(), task.getTabletId(),
                                             pushTask.getSchemaHash(), pushTask.getVersion(),
                                             pushTask.getVersionHash());
                cancelDeleteBatchTask.addTask(cancelDeleteTask);
            }
            if (cancelDeleteBatchTask.getTaskNum() > 0) {
                AgentTaskExecutor.submit(cancelDeleteBatchTask);
            }

            String failMsg = "delete fail, " + e.getMessage();
            LOG.warn(failMsg);
            throw new DdlException(failMsg);
        } finally {
            // clear tasks
            List<AgentTask> tasks = deleteBatchTask.getAllTasks();
            for (AgentTask task : tasks) {
                PushTask pushTask = (PushTask) task;
                AgentTaskQueue.removePushTask(pushTask.getBackendId(), pushTask.getSignature(),
                                              pushTask.getVersion(), pushTask.getVersionHash(),
                                              pushTask.getPushType(), pushTask.getTaskType());
            }

            writeLock();
            try {
                partitionUnderDelete.remove(partitionId);
            } finally {
                writeUnlock();
            }
        }
    }

    public List<List<Comparable>> getAsyncDeleteJobInfo(long jobId) {
        LinkedList<List<Comparable>> infos = new LinkedList<List<Comparable>>();
        readLock();
        try {
            LoadJob job = null;
            for (long dbId : dbToDeleteJobs.keySet()) {
                List<LoadJob> loadJobs = dbToDeleteJobs.get(dbId);
                for (LoadJob loadJob : loadJobs) {
                    if (loadJob.getId() == jobId) {
                        job = loadJob;
                        break;
                    }
                }
            }
            if (job == null) {
                return infos;
            }

            for (Long tabletId : job.getIdToTabletLoadInfo().keySet()) {
                List<Comparable> info = Lists.newArrayList();
                info.add(tabletId);
                infos.add(info);
            }
        } finally {
            readUnlock();
        }

        return infos;
    }
    
    public int getDeleteJobNumByState(long dbId, JobState state) {
        readLock();
        try {
            List<LoadJob> deleteJobs = dbToDeleteJobs.get(dbId);
            if (deleteJobs == null) {
                return 0;
            } else {
                int deleteJobNum = 0;
                for (LoadJob job : deleteJobs) {
                    if (job.getState() == state) {
                        ++ deleteJobNum;
                    }
                }
                return deleteJobNum;
            }
        } finally {
            readUnlock();
        }
    }

    public int getDeleteInfoNum(long dbId) {
        readLock();
        try {
            List<LoadJob> deleteJobs = dbToDeleteJobs.get(dbId);
            if (deleteJobs == null) {
                return 0;
            } else {
                return deleteJobs.size();
            }
        } finally {
            readUnlock();
        }
    }

    public List<List<Comparable>> getDeleteInfosByDb(long dbId, boolean forUser) {
        LinkedList<List<Comparable>> infos = new LinkedList<List<Comparable>>();
		Database db = Catalog.getInstance().getDb(dbId);
		if (db == null) {
			return infos;
		}

		String dbName = db.getFullName();
        readLock();
        try {
            List<LoadJob> deleteJobs = dbToDeleteJobs.get(dbId);
            if (deleteJobs == null) {
                return infos;
            }

            for (LoadJob loadJob : deleteJobs) {

                DeleteInfo deleteInfo = loadJob.getDeleteInfo();

                if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), dbName,
                                                                        deleteInfo.getTableName(),
                                                                        PrivPredicate.LOAD)) {
                    continue;
                }


                List<Comparable> info = Lists.newArrayList();
                if (!forUser) {
                    // do not get job id from delete info, because async delete job == null
                    // just get it from load job
                    info.add(loadJob.getId());
                    info.add(deleteInfo.getTableId());
                }
                info.add(deleteInfo.getTableName());
                if (!forUser) {
                    info.add(deleteInfo.getPartitionId());
                }
                info.add(deleteInfo.getPartitionName());

                info.add(TimeUtils.longToTimeString(deleteInfo.getCreateTimeMs()));
                String conds = Joiner.on(", ").join(deleteInfo.getDeleteConditions());
                info.add(conds);

                if (!forUser) {
                    info.add(deleteInfo.getPartitionVersion());
                    info.add(deleteInfo.getPartitionVersionHash());
                }
                // for loading state, should not display loading, show deleting instead
                if (loadJob.getState() == JobState.LOADING) {
                    info.add("DELETING");
                } else {
                    info.add(loadJob.getState().name());
                }
                infos.add(info);
            }

        } finally {
            readUnlock();
        }

        // sort by createTimeMs
        int sortIndex;
        if (!forUser) {
            sortIndex = 4;
        } else {
            sortIndex = 2;
        }
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(sortIndex);
        Collections.sort(infos, comparator);
        return infos;
    }

    public void removeOldDeleteJobs() {
        long currentTimeMs = System.currentTimeMillis();

        writeLock();
        try {
            Iterator<Map.Entry<Long, List<DeleteInfo>>> iter1 = dbToDeleteInfos.entrySet().iterator();
            while (iter1.hasNext()) {
                Map.Entry<Long, List<DeleteInfo>> entry = iter1.next();
                Iterator<DeleteInfo> iter2 = entry.getValue().iterator();
                while (iter2.hasNext()) {
                    DeleteInfo deleteInfo = iter2.next();
                    if ((currentTimeMs - deleteInfo.getCreateTimeMs()) / 1000 > Config.label_keep_max_second) {
                        iter2.remove();
                    }
                }

                if (entry.getValue().isEmpty()) {
                    iter1.remove();
                }
            }
        } finally {
            writeUnlock();
        }
    }

    public void removeDbDeleteJob(long dbId) {
        writeLock();
        try {
            dbToDeleteInfos.remove(dbId);
        } finally {
            writeUnlock();
        }
    }

    public LoadJob getLastestFinishedLoadJob(long dbId) {
        LoadJob job = null;
        readLock();
        try {
            long maxTime = Long.MIN_VALUE;
            List<LoadJob> jobs = dbToLoadJobs.get(dbId);
            if (jobs != null) {
                for (LoadJob loadJob : jobs) {
                    if (loadJob.getState() != JobState.QUORUM_FINISHED && loadJob.getState() != JobState.FINISHED) {
                        continue;
                    }
                    if (loadJob.getLoadFinishTimeMs() > maxTime) {
                        maxTime = loadJob.getLoadFinishTimeMs();
                        job = loadJob;
                    }
                }
            }
        } finally {
            readUnlock();
        }

        return job;
    }

    public DeleteInfo getLastestFinishedDeleteInfo(long dbId) {
        DeleteInfo deleteInfo = null;
        readLock();
        try {
            long maxTime = Long.MIN_VALUE;
            List<LoadJob> deleteJobs = dbToDeleteJobs.get(dbId);
            if (deleteJobs != null) {
                for (LoadJob loadJob : deleteJobs) {
                    if (loadJob.getDeleteInfo().getCreateTimeMs() > maxTime
                            && loadJob.getState() == JobState.FINISHED) {
                        maxTime = loadJob.getDeleteInfo().getCreateTimeMs();
                        deleteInfo = loadJob.getDeleteInfo();
                    }
                }
            }
        } finally {
            readUnlock();
        }
        return deleteInfo;
    }

    public Integer getLoadJobNumByTypeAndState(EtlJobType type, JobState state) {
        int num = 0;
        readLock();
        try {
            Map<Long, LoadJob> jobMap = null;
            if (state == null || state == JobState.CANCELLED || state == JobState.FINISHED) {
                jobMap = idToLoadJob;
            } else {
                switch (state) {
                    case PENDING:
                        jobMap = idToPendingLoadJob;
                        break;
                    case ETL:
                        jobMap = idToEtlLoadJob;
                        break;
                    case LOADING:
                        jobMap = idToLoadingLoadJob;
                        break;
                    case QUORUM_FINISHED:
                        jobMap = idToQuorumFinishedLoadJob;
                        break;
                    default:
                        break;
                }
            }
            Preconditions.checkNotNull(jobMap);

            for (LoadJob job : jobMap.values()) {
                if (job.getEtlJobType() == type) {
                    if (state != null && job.getState() != state) {
                        continue;
                    }
                    ++num;
                }
            }
            
        } finally {
            readUnlock();
        }
        return num;
    }
}


