/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.DataDescription;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.Source;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionException;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TxnStateChangeCallback;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class LoadJob implements LoadTaskCallback, TxnStateChangeCallback {

    private static final Logger LOG = LogManager.getLogger(LoadJob.class);

    protected static final String QUALITY_FAIL_MSG = "quality not good enough to cancel";
    protected static final String DPP_NORMAL_ALL = "dpp.norm.ALL";
    protected static final String DPP_ABNORMAL_ALL = "dpp.abnorm.ALL";

    protected long id = Catalog.getCurrentCatalog().getNextId();
    protected long dbId;
    protected String label;
    // from data desc of load stmt
    // <TableId,<PartitionId,<LoadInfoList>>>
    private Map<Long, Map<Long, List<Source>>> loadInfo = Maps.newHashMap();
    protected JobState state = JobState.PENDING;

    // optional properties
    // timeout second need to be reset in constructor of subclass
    protected int timeoutSecond = Config.pull_load_task_default_timeout_second;
    protected long execMemLimit = 2147483648L; // 2GB;
    protected double maxFilterRatio = 0;
    protected boolean deleteFlag = false;

    protected long createTimestamp = System.currentTimeMillis();
    protected long loadStartTimestamp = -1;
    protected long finishTimestamp = -1;

    protected long transactionId;
    protected FailMsg failMsg;
    protected LoadPendingTask loadPendingTask;
    protected List<LoadLoadingTask> loadLoadingTaskList = Lists.newArrayList();
    protected EtlStatus loadingStatus = new EtlStatus();
    // 0: pending job and pending with pending task job
    // n/100: n is the number of finished loading task
    // 99: all of loading tasks have been finished
    // 100: txn has been visible, load has been finished
    protected int progress;
    protected List<TabletCommitInfo> commitInfos = Lists.newArrayList();

    protected ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    public LoadJob(long dbId, String label) {
        this.dbId = dbId;
        this.label = label;
    }

    protected void readLock() {
        lock.readLock().lock();
    }

    protected void readUnlock() {
        lock.readLock().unlock();
    }

    protected void writeLock() {
        lock.writeLock().lock();
    }

    protected void writeUnlock() {
        lock.writeLock().unlock();
    }

    public long getId() {
        return id;
    }

    public Database getDb() throws MetaNotFoundException {
        // get db
        Database db = Catalog.getInstance().getDb(dbId);
        if (db == null) {
            throw new MetaNotFoundException("Database " + dbId + " already has been deleted");
        }
        return db;
    }

    public JobState getState() {
        return state;
    }

    public long getDeadlineMs() {
        return createTimestamp + timeoutSecond * 1000;
    }

    public long getLeftTimeMs() {
        return getDeadlineMs() - System.currentTimeMillis();
    }

    protected void setJobProperties(Map<String, String> properties) throws DdlException {
        // resource info
        if (ConnectContext.get() != null) {
            execMemLimit = ConnectContext.get().getSessionVariable().getMaxExecMemByte();
        }

        // job properties
        if (properties != null) {
            if (properties.containsKey(LoadStmt.TIMEOUT_PROPERTY)) {
                try {
                    timeoutSecond = Integer.parseInt(properties.get(LoadStmt.TIMEOUT_PROPERTY));
                } catch (NumberFormatException e) {
                    throw new DdlException("Timeout is not INT", e);
                }
            }

            if (properties.containsKey(LoadStmt.MAX_FILTER_RATIO_PROPERTY)) {
                try {
                    maxFilterRatio = Double.parseDouble(properties.get(LoadStmt.MAX_FILTER_RATIO_PROPERTY));
                } catch (NumberFormatException e) {
                    throw new DdlException("Max filter ratio is not DOUBLE", e);
                }
            }

            if (properties.containsKey(LoadStmt.LOAD_DELETE_FLAG_PROPERTY)) {
                String flag = properties.get(LoadStmt.LOAD_DELETE_FLAG_PROPERTY);
                if (flag.equalsIgnoreCase("true") || flag.equalsIgnoreCase("false")) {
                    deleteFlag = Boolean.parseBoolean(flag);
                } else {
                    throw new DdlException("Value of delete flag is invalid");
                }
            }

            if (properties.containsKey(LoadStmt.EXEC_MEM_LIMIT)) {
                try {
                    execMemLimit = Long.parseLong(properties.get(LoadStmt.EXEC_MEM_LIMIT));
                } catch (NumberFormatException e) {
                    throw new DdlException("Execute memory limit is not Long", e);
                }
            }
        }
    }

    protected void setLoadInfo(Database db, List<DataDescription> dataDescriptions) throws DdlException {
        for (DataDescription dataDescription : dataDescriptions) {
            createSource(db, dataDescription, loadInfo);
        }
    }

    private void createSource(Database db, DataDescription dataDescription, Map<Long, Map<Long,
            List<Source>>> tableToPartitionSources)
            throws DdlException {
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
            if (table.getType() != Table.TableType.OLAP) {
                throw new DdlException("Table [" + tableName + "] is not olap table");
            }

            if (((OlapTable) table).getState() == OlapTable.OlapTableState.RESTORE) {
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
                if (column.getDataType() == PrimitiveType.HLL) {
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
                for (Map.Entry<String, Pair<String, List<String>>> entry : assignColumnToFunction.entrySet()) {
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

    public boolean hasPendingTask() {
        readLock();
        try {
            return loadPendingTask != null;
        } finally {
            readUnlock();
        }
    }

    public void beginTxn() throws LabelAlreadyUsedException, BeginTransactionException, AnalysisException {
        // register txn state listener
        Catalog.getCurrentGlobalTransactionMgr().getCallbackFactory().addCallback(this);
        transactionId = Catalog.getCurrentGlobalTransactionMgr()
                .beginTransaction(dbId, label, -1, "FE: " + FrontendOptions.getLocalHostAddress(),
                                  TransactionState.LoadJobSourceType.FRONTEND, id,
                                  timeoutSecond);
    }

    // create pending task for load job and add pending task into pool
    public void divideToPendingTask() {
        writeLock();
        try {
            if (loadPendingTask == null) {
                // create pending task
                createPendingTask();
                // add pending task into pool
                Catalog.getCurrentCatalog().getLoadManager().submitPendingTask(loadPendingTask);
            }
        } finally {
            writeUnlock();
        }
    }

    abstract void createPendingTask();

    public void updateState(JobState jobState) {
        updateState(jobState, null, null);
    }

    public void updateState(JobState jobState,
                            FailMsg.CancelType cancelType, String errMsg) {
        writeLock();
        try {
            unprotectedUpdateState(jobState, cancelType, errMsg);
        } finally {
            writeUnlock();
        }
        // TODO(ML): edit log
    }

    protected void unprotectedUpdateState(JobState jobState,
                                          FailMsg.CancelType cancelType, String errMsg) {
        switch (jobState) {
            case LOADING:
                executeLoad();
                break;
            case CANCELLED:
                executeCancel(cancelType, errMsg);
                break;
            default:
                break;
        }
    }

    private void executeLoad() {
        loadStartTimestamp = System.currentTimeMillis();
        state = JobState.LOADING;
    }

    private void executeCancel(FailMsg.CancelType cancelType, String errMsg) {
        // Abort txn
        if (transactionId != -1) {
            try {
                Catalog.getCurrentGlobalTransactionMgr().abortTransaction(transactionId, errMsg);
            } catch (UserException e) {
                LOG.warn("Failed to abort txn when job is cancelled. "
                                 + "Txn will be abort later.", e);
            }
        }
        // Plan tasks will not be removed from task pool.
        // It will be aborted onTaskFinished or onTaskFailed().
        loadPendingTask = null;
        loadLoadingTaskList.clear();

        // set failMsg and state
        failMsg = new FailMsg(cancelType, errMsg);
        finishTimestamp = System.currentTimeMillis();
        state = JobState.CANCELLED;

    }

    protected boolean checkDataQuality() {
        Map<String, String> counters = loadingStatus.getCounters();
        if (!counters.containsKey(DPP_NORMAL_ALL) || !counters.containsKey(DPP_ABNORMAL_ALL)) {
            return true;
        }

        long normalNum = Long.parseLong(counters.get(DPP_NORMAL_ALL));
        long abnormalNum = Long.parseLong(counters.get(DPP_ABNORMAL_ALL));
        if (abnormalNum > (abnormalNum + normalNum) * maxFilterRatio) {
            return false;
        }

        return true;
    }

    @Override
    public long getCallbackId() {
        return id;
    }

    @Override
    public void beforeCommitted(TransactionState txnState) throws TransactionException {
    }

    @Override
    public void beforeAborted(TransactionState txnState) throws TransactionException {
    }

    @Override
    public void afterCommitted(TransactionState txnState, boolean txnOperated) throws UserException {
        // TODO(ml)
    }

    @Override
    public void replayOnCommitted(TransactionState txnState) {
        //TODO(ml)
    }

    @Override
    public void afterAborted(TransactionState txnState, boolean txnOperated, String txnStatusChangeReason)
            throws UserException {
        //TODO(ml)
    }

    @Override
    public void replayOnAborted(TransactionState txnState) {
        //TODO(ml)
    }


}
