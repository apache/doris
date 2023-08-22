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

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.ExportStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FromClause;
import org.apache.doris.analysis.LimitElement;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.analysis.OutFileClause;
import org.apache.doris.analysis.SelectList;
import org.apache.doris.analysis.SelectListItem;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StorageBackend.StorageType;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.scheduler.exception.JobException;
import org.apache.doris.scheduler.registry.ExportTaskRegister;
import org.apache.doris.scheduler.registry.TransientTaskRegister;
import org.apache.doris.task.ExportExportingTask;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

@Data
public class ExportJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(ExportJob.class);

    private static final String BROKER_PROPERTY_PREFIXES = "broker.";

    private static final int MAXIMUM_TABLETS_OF_OUTFILE_IN_EXPORT = Config.maximum_tablets_of_outfile_in_export;

    public static final TransientTaskRegister register = new ExportTaskRegister(
            Env.getCurrentEnv().getTransientTaskManager());

    @SerializedName("id")
    private long id;
    @SerializedName("label")
    private String label;
    @SerializedName("dbId")
    private long dbId;
    @SerializedName("tableId")
    private long tableId;
    @SerializedName("brokerDesc")
    private BrokerDesc brokerDesc;
    @SerializedName("exportPath")
    private String exportPath;
    @SerializedName("columnSeparator")
    private String columnSeparator;
    @SerializedName("lineDelimiter")
    private String lineDelimiter;
    @SerializedName(value = "partitionNames", alternate = {"partitions"})
    private List<String> partitionNames;
    @SerializedName("tableName")
    private TableName tableName;
    @SerializedName("state")
    private ExportJobState state;
    @SerializedName("createTimeMs")
    private long createTimeMs;
    // this is the origin stmt of ExportStmt, we use it to persist where expr of Export job,
    // because we can not serialize the Expressions contained in job.
    @SerializedName("origStmt")
    private OriginStatement origStmt;
    @SerializedName("qualifiedUser")
    private String qualifiedUser;
    @SerializedName("userIdentity")
    private UserIdentity userIdentity;
    @SerializedName("columns")
    private String columns;
    @SerializedName("format")
    private String format;
    @SerializedName("timeoutSecond")
    private int timeoutSecond;
    @SerializedName("maxFileSize")
    private String maxFileSize;
    @SerializedName("deleteExistingFiles")
    private String deleteExistingFiles;
    @SerializedName("startTimeMs")
    private long startTimeMs;
    @SerializedName("finishTimeMs")
    private long finishTimeMs;
    @SerializedName("failMsg")
    private ExportFailMsg failMsg;
    @SerializedName("outfileInfo")
    private String outfileInfo;
    // progress has two functions at EXPORTING stage:
    // 1. when progress < 100, it indicates exporting
    // 2. set progress = 100 ONLY when exporting progress is completely done
    @SerializedName("progress")
    private int progress;

    @SerializedName("tabletsNum")
    private Integer tabletsNum;

    private TableRef tableRef;

    private Expr whereExpr;

    private String sql = "";

    private Integer parallelism;

    public Map<String, Long> getPartitionToVersion() {
        return partitionToVersion;
    }

    private Map<String, Long> partitionToVersion = Maps.newHashMap();

    // The selectStmt is sql 'select ... into outfile ...'
    // TODO(ftw): delete
    private List<SelectStmt> selectStmtList = Lists.newArrayList();

    private List<List<SelectStmt>> selectStmtListPerParallel = Lists.newArrayList();

    private List<StmtExecutor> stmtExecutorList;

    private List<String> exportColumns = Lists.newArrayList();

    private Table exportTable;

    // when set to true, means this job instance is created by replay thread(FE restarted or master changed)
    private boolean isReplayed = false;

    private SessionVariable sessionVariables;

    private Thread doExportingThread;

    private ExportExportingTask task;

    // backend_address => snapshot path
    private List<Pair<TNetworkAddress, String>> snapshotPaths = Lists.newArrayList();

    private List<ExportTaskExecutor> jobExecutorList;

    private ConcurrentHashMap<Long, ExportTaskExecutor> taskIdToExecutor = new ConcurrentHashMap<>();

    private Integer finishedTaskCount = 0;
    private List<List<OutfileInfo>> allOutfileInfo = Lists.newArrayList();

    public ExportJob() {
        this.id = -1;
        this.dbId = -1;
        this.tableId = -1;
        this.state = ExportJobState.PENDING;
        this.progress = 0;
        this.createTimeMs = System.currentTimeMillis();
        this.startTimeMs = -1;
        this.finishTimeMs = -1;
        this.failMsg = new ExportFailMsg(ExportFailMsg.CancelType.UNKNOWN, "");
        this.outfileInfo = "";
        this.exportPath = "";
        this.columnSeparator = "\t";
        this.lineDelimiter = "\n";
        this.columns = "";
    }

    public ExportJob(long jobId) {
        this();
        this.id = jobId;
    }

    /**
     * For an ExportJob:
     * The ExportJob is divided into multiple 'ExportTaskExecutor'
     * according to the 'parallelism' set by the user.
     * The tablets which will be exported by this ExportJob are divided into 'parallelism' copies,
     * and each ExportTaskExecutor is responsible for a list of tablets.
     * The tablets responsible for an ExportTaskExecutor will be assigned to multiple OutfileStmt
     * according to the 'TABLETS_NUM_PER_OUTFILE_IN_EXPORT'.
     *
     * @throws UserException
     */
    public void analyze() throws UserException {
        exportTable.readLock();
        try {
            // generateQueryStmtOld
            generateQueryStmt();
        } finally {
            exportTable.readUnlock();
        }
        generateExportJobExecutor();
    }

    public void generateExportJobExecutor() {
        jobExecutorList = Lists.newArrayList();
        for (List<SelectStmt> selectStmts : selectStmtListPerParallel) {
            ExportTaskExecutor executor = new ExportTaskExecutor(selectStmts, this);
            jobExecutorList.add(executor);
        }
    }

    private void generateQueryStmtOld() throws UserException {
        SelectList list = new SelectList();
        if (exportColumns.isEmpty()) {
            list.addItem(SelectListItem.createStarItem(this.tableName));
        } else {
            for (Column column : exportTable.getBaseSchema()) {
                String colName = column.getName().toLowerCase();
                if (exportColumns.contains(colName)) {
                    SlotRef slotRef = new SlotRef(this.tableName, colName);
                    SelectListItem selectListItem = new SelectListItem(slotRef, null);
                    list.addItem(selectListItem);
                }
            }
        }

        ArrayList<ArrayList<Long>> tabletsListPerQuery = splitTablets();

        ArrayList<ArrayList<TableRef>> tableRefListPerQuery = Lists.newArrayList();
        for (ArrayList<Long> tabletsList : tabletsListPerQuery) {
            TableRef tblRef = new TableRef(this.tableRef.getName(), this.tableRef.getAlias(), null, tabletsList,
                    this.tableRef.getTableSample(), this.tableRef.getCommonHints());
            ArrayList<TableRef> tableRefList = Lists.newArrayList();
            tableRefList.add(tblRef);
            tableRefListPerQuery.add(tableRefList);
        }
        LOG.info("Export task is split into {} outfile statements.", tableRefListPerQuery.size());

        if (LOG.isDebugEnabled()) {
            for (int i = 0; i < tableRefListPerQuery.size(); i++) {
                LOG.debug("Outfile clause {} is responsible for tables: {}", i,
                        tableRefListPerQuery.get(i).get(0).getSampleTabletIds());
            }
        }

        for (ArrayList<TableRef> tableRefList : tableRefListPerQuery) {
            FromClause fromClause = new FromClause(tableRefList);
            // generate outfile clause
            OutFileClause outfile = new OutFileClause(this.exportPath, this.format, convertOutfileProperties());
            SelectStmt selectStmt = new SelectStmt(list, fromClause, this.whereExpr, null,
                    null, null, LimitElement.NO_LIMIT);
            selectStmt.setOutFileClause(outfile);
            selectStmt.setOrigStmt(new OriginStatement(selectStmt.toSql(), 0));
            selectStmtList.add(selectStmt);
        }
        stmtExecutorList = Arrays.asList(new StmtExecutor[selectStmtList.size()]);
        if (LOG.isDebugEnabled()) {
            for (int i = 0; i < selectStmtList.size(); i++) {
                LOG.debug("Outfile clause {} is: {}", i, selectStmtList.get(i).toSql());
            }
        }
    }

    /**
     * Generate outfile select stmt
     * @throws UserException
     */
    private void generateQueryStmt() throws UserException {
        SelectList list = new SelectList();
        if (exportColumns.isEmpty()) {
            list.addItem(SelectListItem.createStarItem(this.tableName));
        } else {
            for (Column column : exportTable.getBaseSchema()) {
                String colName = column.getName().toLowerCase();
                if (exportColumns.contains(colName)) {
                    SlotRef slotRef = new SlotRef(this.tableName, colName);
                    SelectListItem selectListItem = new SelectListItem(slotRef, null);
                    list.addItem(selectListItem);
                }
            }
        }

        ArrayList<ArrayList<TableRef>> tableRefListPerParallel = getTableRefListPerParallel();
        LOG.info("Export Job [{}] is split into {} Export Task Executor.", id, tableRefListPerParallel.size());

        // debug LOG output
        if (LOG.isDebugEnabled()) {
            for (int i = 0; i < tableRefListPerParallel.size(); i++) {
                LOG.debug("ExportTaskExecutor {} is responsible for tablets:", i);
                for (TableRef tableRef : tableRefListPerParallel.get(i)) {
                    LOG.debug("Tablet id: [{}]", tableRef.getSampleTabletIds());
                }
            }
        }

        // generate 'select..outfile..' statement
        for (ArrayList<TableRef> tableRefList : tableRefListPerParallel) {
            List<SelectStmt> selectStmtLists = Lists.newArrayList();
            for (TableRef tableRef : tableRefList) {
                ArrayList<TableRef> tmpTableRefList = Lists.newArrayList(tableRef);
                FromClause fromClause = new FromClause(tmpTableRefList);
                // generate outfile clause
                OutFileClause outfile = new OutFileClause(this.exportPath, this.format, convertOutfileProperties());
                SelectStmt selectStmt = new SelectStmt(list, fromClause, this.whereExpr, null,
                        null, null, LimitElement.NO_LIMIT);
                selectStmt.setOutFileClause(outfile);
                selectStmt.setOrigStmt(new OriginStatement(selectStmt.toSql(), 0));
                selectStmtLists.add(selectStmt);
            }
            selectStmtListPerParallel.add(selectStmtLists);
        }

        // debug LOG output
        if (LOG.isDebugEnabled()) {
            for (int i = 0; i < selectStmtListPerParallel.size(); ++i) {
                LOG.debug("ExportTaskExecutor {} is responsible for outfile:", i);
                for (SelectStmt outfile : selectStmtListPerParallel.get(i)) {
                    LOG.debug("outfile sql: [{}]", outfile.toSql());
                }
            }
        }
    }

    private ArrayList<ArrayList<TableRef>> getTableRefListPerParallel() throws UserException {
        ArrayList<ArrayList<Long>> tabletsListPerParallel = splitTablets();

        ArrayList<ArrayList<TableRef>> tableRefListPerParallel = Lists.newArrayList();
        for (ArrayList<Long> tabletsList : tabletsListPerParallel) {
            ArrayList<TableRef> tableRefList = Lists.newArrayList();
            for (int i = 0; i < tabletsList.size(); i += MAXIMUM_TABLETS_OF_OUTFILE_IN_EXPORT) {
                int end = i + MAXIMUM_TABLETS_OF_OUTFILE_IN_EXPORT < tabletsList.size()
                        ? i + MAXIMUM_TABLETS_OF_OUTFILE_IN_EXPORT : tabletsList.size();
                ArrayList<Long> tablets = new ArrayList<>(tabletsList.subList(i, end));
                TableRef tblRef = new TableRef(this.tableRef.getName(), this.tableRef.getAlias(),
                        this.tableRef.getPartitionNames(), tablets,
                        this.tableRef.getTableSample(), this.tableRef.getCommonHints());
                tableRefList.add(tblRef);
            }
            tableRefListPerParallel.add(tableRefList);
        }
        return tableRefListPerParallel;
    }

    private ArrayList<ArrayList<Long>> splitTablets() throws UserException {
        // get tablets
        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException(this.tableName.getDb());
        OlapTable table = db.getOlapTableOrAnalysisException(this.tableName.getTbl());
        List<Long> tabletIdList = Lists.newArrayList();
        table.readLock();
        try {
            final Collection<Partition> partitions = new ArrayList<Partition>();
            // get partitions
            // user specifies partitions, already checked in ExportStmt
            if (this.partitionNames != null) {
                this.partitionNames.forEach(partitionName -> partitions.add(table.getPartition(partitionName)));
            } else {
                if (table.getPartitions().size() > Config.maximum_number_of_export_partitions) {
                    throw new UserException("The partitions number of this export job is larger than the maximum number"
                            + " of partitions allowed by a export job");
                }
                partitions.addAll(table.getPartitions());
            }

            // get tablets
            for (Partition partition : partitions) {
                partitionToVersion.put(partition.getName(), partition.getVisibleVersion());
                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                    tabletIdList.addAll(index.getTabletIdsInOrder());
                }
            }
        } finally {
            table.readUnlock();
        }

        Integer tabletsAllNum = tabletIdList.size();
        tabletsNum = tabletsAllNum;
        Integer tabletsNumPerParallel = tabletsAllNum / this.parallelism;
        Integer tabletsNumPerQueryRemainder = tabletsAllNum - tabletsNumPerParallel * this.parallelism;

        ArrayList<ArrayList<Long>> tabletsListPerParallel = Lists.newArrayList();
        Integer realParallelism = this.parallelism;
        if (tabletsAllNum < this.parallelism) {
            realParallelism = tabletsAllNum;
            LOG.warn("Export Job [{}]: The number of tablets ({}) is smaller than parallelism ({}), "
                        + "set parallelism to tablets num.", id, tabletsAllNum, this.parallelism);
        }
        Integer start = 0;
        for (int i = 0; i < realParallelism; ++i) {
            Integer tabletsNum = tabletsNumPerParallel;
            if (tabletsNumPerQueryRemainder > 0) {
                tabletsNum = tabletsNum + 1;
                --tabletsNumPerQueryRemainder;
            }
            ArrayList<Long> tablets = new ArrayList<>(tabletIdList.subList(start, start + tabletsNum));
            start += tabletsNum;

            tabletsListPerParallel.add(tablets);
        }
        return tabletsListPerParallel;
    }

    private Map<String, String> convertOutfileProperties() {
        final Map<String, String> outfileProperties = Maps.newHashMap();

        // file properties
        if (format.equals("csv") || format.equals("csv_with_names") || format.equals("csv_with_names_and_types")) {
            outfileProperties.put(OutFileClause.PROP_COLUMN_SEPARATOR, columnSeparator);
            outfileProperties.put(OutFileClause.PROP_LINE_DELIMITER, lineDelimiter);
        }
        if (!maxFileSize.isEmpty()) {
            outfileProperties.put(OutFileClause.PROP_MAX_FILE_SIZE, maxFileSize);
        }
        if (!deleteExistingFiles.isEmpty()) {
            outfileProperties.put(OutFileClause.PROP_DELETE_EXISTING_FILES, deleteExistingFiles);
        }

        // broker properties
        // outfile clause's broker properties need 'broker.' prefix
        if (brokerDesc.getStorageType() == StorageType.BROKER) {
            outfileProperties.put(BROKER_PROPERTY_PREFIXES + "name", brokerDesc.getName());
            brokerDesc.getProperties().forEach((k, v) -> outfileProperties.put(BROKER_PROPERTY_PREFIXES + k, v));
        } else {
            for (Entry<String, String> kv : brokerDesc.getProperties().entrySet()) {
                outfileProperties.put(kv.getKey(), kv.getValue());
            }
        }
        return outfileProperties;
    }

    public synchronized ExportJobState getState() {
        return state;
    }

    private void setExportJobState(ExportJobState newState) {
        this.state = newState;
    }

    // TODO(ftw): delete
    public synchronized Thread getDoExportingThread() {
        return doExportingThread;
    }

    // TODO(ftw): delete
    public synchronized void setDoExportingThread(Thread isExportingThread) {
        this.doExportingThread = isExportingThread;
    }

    // TODO(ftw): delete
    public synchronized void setStmtExecutor(int idx, StmtExecutor executor) {
        this.stmtExecutorList.set(idx, executor);
    }

    public synchronized StmtExecutor getStmtExecutor(int idx) {
        return this.stmtExecutorList.get(idx);
    }

    // TODO(ftw): delete
    public synchronized void cancel(ExportFailMsg.CancelType type, String msg) {
        if (msg != null) {
            failMsg = new ExportFailMsg(type, msg);
        }

        // maybe user cancel this job
        if (task != null && state == ExportJobState.EXPORTING && stmtExecutorList != null) {
            for (int idx = 0; idx < stmtExecutorList.size(); ++idx) {
                stmtExecutorList.get(idx).cancel();
            }
        }

        if (updateState(ExportJobState.CANCELLED, false)) {
            // release snapshot
            // Status releaseSnapshotStatus = releaseSnapshotPaths();
            // if (!releaseSnapshotStatus.ok()) {
            //     // snapshot will be removed by GC thread on BE, finally.
            //     LOG.warn("failed to release snapshot for export job: {}. err: {}", id,
            //             releaseSnapshotStatus.getErrorMsg());
            // }
        }
    }

    public synchronized void updateExportJobState(ExportJobState newState, Long taskId,
            List<OutfileInfo> outfileInfoList, ExportFailMsg.CancelType type, String msg) throws JobException {
        switch (newState) {
            case PENDING:
                throw new JobException("Can not update ExportJob state to 'PENDING', job id: [{}], task id: [{}]",
                        id, taskId);
            case EXPORTING:
                exportExportJob();
                break;
            case CANCELLED:
                cancelExportTask(type, msg);
                break;
            case FINISHED:
                finishExportTask(taskId, outfileInfoList);
                break;
            default:
                return;
        }
    }

    public void cancelReplayedExportJob(ExportFailMsg.CancelType type, String msg) {
        setExportJobState(ExportJobState.CANCELLED);
        failMsg = new ExportFailMsg(type, msg);
    }

    private void cancelExportTask(ExportFailMsg.CancelType type, String msg) throws JobException {
        if (getState() == ExportJobState.CANCELLED) {
            return;
        }

        if (getState() == ExportJobState.FINISHED) {
            throw new JobException("Job {} has finished, can not been cancelled", id);
        }

        if (getState() == ExportJobState.PENDING) {
            startTimeMs = System.currentTimeMillis();
        }

        // we need cancel all task
        taskIdToExecutor.keySet().forEach(id -> {
            try {
                register.cancelTask(id);
            } catch (JobException e) {
                LOG.warn("cancel export task {} exception: {}", id, e);
            }
        });

        cancelExportJobUnprotected(type, msg);
    }

    private void cancelExportJobUnprotected(ExportFailMsg.CancelType type, String msg) {
        setExportJobState(ExportJobState.CANCELLED);
        finishTimeMs = System.currentTimeMillis();
        failMsg = new ExportFailMsg(type, msg);
        Env.getCurrentEnv().getEditLog().logExportUpdateState(id, ExportJobState.CANCELLED);
    }

    // TODO(ftw): delete
    public synchronized boolean finish(List<OutfileInfo> outfileInfoList) {
        outfileInfo = GsonUtils.GSON.toJson(outfileInfoList);
        if (updateState(ExportJobState.FINISHED)) {
            return true;
        }
        return false;
    }

    private void exportExportJob() {
        // The first exportTaskExecutor will set state to EXPORTING,
        // other exportTaskExecutors do not need to set up state.
        if (getState() == ExportJobState.EXPORTING) {
            return;
        }
        setExportJobState(ExportJobState.EXPORTING);
        // if isReplay == true, startTimeMs will be read from LOG
        startTimeMs = System.currentTimeMillis();
    }

    private void finishExportTask(Long taskId, List<OutfileInfo> outfileInfoList) throws JobException {
        if (getState() == ExportJobState.CANCELLED) {
            throw new JobException("Job [{}] has been cancelled, can not finish this task: {}", id, taskId);
        }

        allOutfileInfo.add(outfileInfoList);
        ++finishedTaskCount;

        // calculate progress
        int tmpProgress = finishedTaskCount * 100 / jobExecutorList.size();
        if (finishedTaskCount * 100 / jobExecutorList.size() >= 100) {
            progress = 99;
        } else {
            progress = tmpProgress;
        }

        // if all task finished
        if (finishedTaskCount == jobExecutorList.size()) {
            finishExportJobUnprotected();
        }
    }

    private void finishExportJobUnprotected() {
        progress = 100;
        setExportJobState(ExportJobState.FINISHED);
        finishTimeMs = System.currentTimeMillis();
        outfileInfo = GsonUtils.GSON.toJson(allOutfileInfo);
        Env.getCurrentEnv().getEditLog().logExportUpdateState(id, ExportJobState.FINISHED);
    }

    public void replayExportJobState(ExportJobState newState) {
        switch (newState) {
            // We do not persist EXPORTING state in new version of metadata,
            // but EXPORTING state may still exist in older versions of metadata.
            // So if isReplay == true and newState == EXPORTING, we set newState = CANCELLED.
            case EXPORTING:
            // We do not need IN_QUEUE state in new version of export
            // but IN_QUEUE state may still exist in older versions of metadata.
            // So if isReplay == true and newState == IN_QUEUE, we set newState = CANCELLED.
            case IN_QUEUE:
                newState = ExportJobState.CANCELLED;
                break;
            case PENDING:
            case CANCELLED:
                progress = 0;
                break;
            case FINISHED:
                progress = 100;
                break;
            default:
                Preconditions.checkState(false, "wrong job state: " + newState.name());
                break;
        }
        setExportJobState(newState);
    }

    // TODO(ftw): delete
    public synchronized boolean updateState(ExportJobState newState) {
        return this.updateState(newState, false);
    }

    // TODO(ftw): delete
    public synchronized boolean updateState(ExportJobState newState, boolean isReplay) {
        // We do not persist EXPORTING state in new version of metadata,
        // but EXPORTING state may still exist in older versions of metadata.
        // So if isReplay == true and newState == EXPORTING, we just ignore this update.
        if (isFinalState() || (isReplay && newState == ExportJobState.EXPORTING)) {
            return false;
        }
        state = newState;
        switch (newState) {
            case PENDING:
            case IN_QUEUE:
                progress = 0;
                break;
            case EXPORTING:
                // if isReplay == true, startTimeMs will be read from LOG
                if (!isReplay) {
                    startTimeMs = System.currentTimeMillis();
                }
                break;
            case FINISHED:
                if (!isReplay) {
                    finishTimeMs = System.currentTimeMillis();
                }
                progress = 100;
                break;
            case CANCELLED:
                // if isReplay == true, finishTimeMs will be read from LOG
                if (!isReplay) {
                    finishTimeMs = System.currentTimeMillis();
                }
                break;
            default:
                Preconditions.checkState(false, "wrong job state: " + newState.name());
                break;
        }
        // we only persist Pending/Cancel/Finish state
        if (!isReplay && newState != ExportJobState.IN_QUEUE && newState != ExportJobState.EXPORTING) {
            Env.getCurrentEnv().getEditLog().logExportUpdateState(id, newState);
        }
        return true;
    }

    public synchronized boolean isFinalState() {
        return this.state == ExportJobState.CANCELLED || this.state == ExportJobState.FINISHED;
    }

    public boolean isExpired(long curTime) {
        return (curTime - createTimeMs) / 1000 > Config.history_job_keep_max_second
                && (state == ExportJobState.CANCELLED || state == ExportJobState.FINISHED);
    }

    @Override
    public String toString() {
        return "ExportJob [jobId=" + id
                + ", label=" + label
                + ", dbId=" + dbId
                + ", tableId=" + tableId
                + ", state=" + state
                + ", path=" + exportPath
                + ", partitions=(" + StringUtils.join(partitionNames, ",") + ")"
                + ", progress=" + progress
                + ", createTimeMs=" + TimeUtils.longToTimeString(createTimeMs)
                + ", exportStartTimeMs=" + TimeUtils.longToTimeString(startTimeMs)
                + ", exportFinishTimeMs=" + TimeUtils.longToTimeString(finishTimeMs)
                + ", failMsg=" + failMsg
                + "]";
    }

    public static ExportJob read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_120) {
            ExportJob job = new ExportJob();
            job.readFields(in);
            return job;
        }
        String json = Text.readString(in);
        ExportJob job = GsonUtils.GSON.fromJson(json, ExportJob.class);
        job.isReplayed = true;
        return job;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Deprecated
    private void readFields(DataInput in) throws IOException {
        isReplayed = true;
        id = in.readLong();
        dbId = in.readLong();
        tableId = in.readLong();
        exportPath = Text.readString(in);
        columnSeparator = Text.readString(in);
        lineDelimiter = Text.readString(in);

        // properties
        Map<String, String> properties = Maps.newHashMap();
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            String propertyKey = Text.readString(in);
            String propertyValue = Text.readString(in);
            properties.put(propertyKey, propertyValue);
        }
        // Because before 0.15, export does not contain label information.
        // So for compatibility, a label will be added for historical jobs.
        // This label must be guaranteed to be a certain value to prevent
        // the label from being different each time.
        properties.putIfAbsent(ExportStmt.LABEL, "export_" + id);
        this.label = properties.get(ExportStmt.LABEL);
        this.columns = properties.get(LoadStmt.KEY_IN_PARAM_COLUMNS);
        if (!Strings.isNullOrEmpty(this.columns)) {
            Splitter split = Splitter.on(',').trimResults().omitEmptyStrings();
            this.exportColumns = split.splitToList(this.columns.toLowerCase());
        }
        boolean hasPartition = in.readBoolean();
        if (hasPartition) {
            partitionNames = Lists.newArrayList();
            int partitionSize = in.readInt();
            for (int i = 0; i < partitionSize; ++i) {
                String partitionName = Text.readString(in);
                partitionNames.add(partitionName);
            }
        }

        state = ExportJobState.valueOf(Text.readString(in));
        createTimeMs = in.readLong();
        startTimeMs = in.readLong();
        finishTimeMs = in.readLong();
        progress = in.readInt();
        failMsg.readFields(in);

        if (in.readBoolean()) {
            brokerDesc = BrokerDesc.read(in);
        }

        tableName = new TableName();
        tableName.readFields(in);
        origStmt = OriginStatement.read(in);

        Map<String, String> tmpSessionVariables = Maps.newHashMap();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            tmpSessionVariables.put(key, value);
        }

        if (origStmt.originStmt.isEmpty()) {
            return;
        }
        // parse the origin stmt to get where expr
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(origStmt.originStmt),
                Long.valueOf(tmpSessionVariables.get(SessionVariable.SQL_MODE))));
        ExportStmt stmt = null;
        try {
            stmt = (ExportStmt) SqlParserUtils.getStmt(parser, origStmt.idx);
            this.whereExpr = stmt.getWhereExpr();
        } catch (Exception e) {
            throw new IOException("error happens when parsing export stmt: " + origStmt, e);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof ExportJob)) {
            return false;
        }

        ExportJob job = (ExportJob) obj;

        if (this.id == job.id) {
            return true;
        }

        return false;
    }
}
