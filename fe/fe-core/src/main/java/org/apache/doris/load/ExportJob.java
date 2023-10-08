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
import org.apache.doris.analysis.StatementBase;
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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.scheduler.exception.JobException;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Data
public class ExportJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(ExportJob.class);

    private static final String BROKER_PROPERTY_PREFIXES = "broker.";

    private static final int MAXIMUM_TABLETS_OF_OUTFILE_IN_EXPORT = Config.maximum_tablets_of_outfile_in_export;

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

    private Optional<Expression> whereExpression;

    private int parallelism;

    private Map<String, Long> partitionToVersion = Maps.newHashMap();

    /**
     * Each parallel has an associated Outfile list
     * which are organized into a two-dimensional list.
     * Therefore, we can access the selectStmtListPerParallel
     * to get the outfile logical plans list responsible for each parallel task.
     */
    private List<List<StatementBase>> selectStmtListPerParallel = Lists.newArrayList();

    private List<String> exportColumns = Lists.newArrayList();

    private TableIf exportTable;

    // when set to true, means this job instance is created by replay thread(FE restarted or master changed)
    private boolean isReplayed = false;

    private SessionVariable sessionVariables;

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

    public void generateOutfileStatement() throws UserException {
        exportTable.readLock();
        try {
            // generateQueryStmtOld
            generateQueryStmt();
        } finally {
            exportTable.readUnlock();
        }
        generateExportJobExecutor();
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
    public void generateOutfileLogicalPlans(List<String> qualifiedTableName)
            throws UserException {
        String catalogType = Env.getCurrentEnv().getCatalogMgr().getCatalog(this.tableName.getCtl()).getType();
        exportTable.readLock();
        try {
            if (InternalCatalog.INTERNAL_CATALOG_NAME.equals(catalogType)) {
                if (exportTable.getType() == TableType.VIEW) {
                    // view table
                    generateViewOrExternalTableOutfile(qualifiedTableName);
                } else if (exportTable.getType() == TableType.OLAP) {
                    // olap table
                    generateOlapTableOutfile(qualifiedTableName);
                } else {
                    throw new UserException("Do not support export table type [" + exportTable.getType() + "]");
                }
            } else {
                // external table
                generateViewOrExternalTableOutfile(qualifiedTableName);
            }

            // debug LOG output
            if (LOG.isDebugEnabled()) {
                for (int i = 0; i < selectStmtListPerParallel.size(); ++i) {
                    LOG.debug("ExportTaskExecutor {} is responsible for outfile:", i);
                    for (StatementBase outfile : selectStmtListPerParallel.get(i)) {
                        LOG.debug("outfile sql: [{}]", outfile.toSql());
                    }
                }
            }

        } finally {
            exportTable.readUnlock();
        }
        generateExportJobExecutor();
    }

    private void generateOlapTableOutfile(List<String> qualifiedTableName) throws UserException {
        // build source columns
        List<NamedExpression> selectLists = Lists.newArrayList();
        if (exportColumns.isEmpty()) {
            selectLists.add(new UnboundStar(ImmutableList.of()));
        } else {
            this.exportColumns.stream().forEach(col -> {
                selectLists.add(new UnboundSlot(this.tableName.getTbl(), col));
            });
        }

        // get all tablets
        List<List<Long>> tabletsListPerParallel = splitTablets();

        // Each Outfile clause responsible for MAXIMUM_TABLETS_OF_OUTFILE_IN_EXPORT tablets
        for (List<Long> tabletsList : tabletsListPerParallel) {
            List<StatementBase> logicalPlanAdapters = Lists.newArrayList();
            for (int i = 0; i < tabletsList.size(); i += MAXIMUM_TABLETS_OF_OUTFILE_IN_EXPORT) {
                int end = i + MAXIMUM_TABLETS_OF_OUTFILE_IN_EXPORT < tabletsList.size()
                        ? i + MAXIMUM_TABLETS_OF_OUTFILE_IN_EXPORT : tabletsList.size();
                List<Long> tabletIds = new ArrayList<>(tabletsList.subList(i, end));

                // generate LogicalPlan
                LogicalPlan plan = generateOneLogicalPlan(qualifiedTableName, tabletIds,
                        this.partitionNames, selectLists);
                // generate  LogicalPlanAdapter
                StatementBase statementBase = generateLogicalPlanAdapter(plan);

                logicalPlanAdapters.add(statementBase);
            }
            selectStmtListPerParallel.add(logicalPlanAdapters);
        }
    }

    /**
     * This method used to generate outfile sql for view table or external table.
     * @throws UserException
     */
    private void generateViewOrExternalTableOutfile(List<String> qualifiedTableName) {
        // Because there is no division of tablets in view and external table
        // we set parallelism = 1;
        this.parallelism = 1;
        LOG.debug("Because there is no division of tablets in view and external table, we set parallelism = 1");

        // build source columns
        List<NamedExpression> selectLists = Lists.newArrayList();
        if (exportColumns.isEmpty()) {
            selectLists.add(new UnboundStar(ImmutableList.of()));
        } else {
            this.exportColumns.stream().forEach(col -> {
                selectLists.add(new UnboundSlot(this.tableName.getTbl(), col));
            });
        }

        List<StatementBase> logicalPlanAdapters = Lists.newArrayList();

        // generate LogicalPlan
        LogicalPlan plan = generateOneLogicalPlan(qualifiedTableName, ImmutableList.of(),
                ImmutableList.of(), selectLists);
        // generate  LogicalPlanAdapter
        StatementBase statementBase = generateLogicalPlanAdapter(plan);

        logicalPlanAdapters.add(statementBase);
        selectStmtListPerParallel.add(logicalPlanAdapters);
    }

    private LogicalPlan generateOneLogicalPlan(List<String> qualifiedTableName, List<Long> tabletIds,
            List<String> partitions, List<NamedExpression> selectLists) {
        // UnboundRelation
        LogicalPlan plan = new UnboundRelation(StatementScopeIdGenerator.newRelationId(), qualifiedTableName,
                partitions, false, tabletIds, ImmutableList.of(), Optional.empty());
        // LogicalCheckPolicy
        plan = new LogicalCheckPolicy<>(plan);
        // LogicalFilter
        if (this.whereExpression.isPresent()) {
            plan = new LogicalFilter<>(ExpressionUtils.extractConjunctionToSet(this.whereExpression.get()), plan);
        }
        // LogicalFilter
        plan = new LogicalProject(selectLists, plan);
        // LogicalFileSink
        plan = new LogicalFileSink<>(this.exportPath, this.format, convertOutfileProperties(),
                ImmutableList.of(), plan);
        return plan;
    }

    private StatementBase generateLogicalPlanAdapter(LogicalPlan outfileLogicalPlan) {
        StatementContext statementContext = new StatementContext();
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext != null) {
            connectContext.setStatementContext(statementContext);
            statementContext.setConnectContext(connectContext);
        }

        StatementBase statementBase = new LogicalPlanAdapter(outfileLogicalPlan, statementContext);
        statementBase.setOrigStmt(new OriginStatement(statementBase.toSql(), 0));
        return statementBase;
    }

    private void generateExportJobExecutor() {
        jobExecutorList = Lists.newArrayList();
        for (List<StatementBase> selectStmts : selectStmtListPerParallel) {
            ExportTaskExecutor executor = new ExportTaskExecutor(selectStmts, this);
            jobExecutorList.add(executor);
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

        List<List<TableRef>> tableRefListPerParallel = getTableRefListPerParallel();
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
        for (List<TableRef> tableRefList : tableRefListPerParallel) {
            List<StatementBase> selectStmtLists = Lists.newArrayList();
            for (TableRef tableRef : tableRefList) {
                List<TableRef> tmpTableRefList = Lists.newArrayList(tableRef);
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
                for (StatementBase outfile : selectStmtListPerParallel.get(i)) {
                    LOG.debug("outfile sql: [{}]", outfile.toSql());
                }
            }
        }
    }

    private List<List<TableRef>> getTableRefListPerParallel() throws UserException {
        List<List<Long>> tabletsListPerParallel = splitTablets();

        List<List<TableRef>> tableRefListPerParallel = Lists.newArrayList();
        for (List<Long> tabletsList : tabletsListPerParallel) {
            List<TableRef> tableRefList = Lists.newArrayList();
            for (int i = 0; i < tabletsList.size(); i += MAXIMUM_TABLETS_OF_OUTFILE_IN_EXPORT) {
                int end = i + MAXIMUM_TABLETS_OF_OUTFILE_IN_EXPORT < tabletsList.size()
                        ? i + MAXIMUM_TABLETS_OF_OUTFILE_IN_EXPORT : tabletsList.size();
                List<Long> tablets = new ArrayList<>(tabletsList.subList(i, end));
                TableRef tblRef = new TableRef(this.tableRef.getName(), this.tableRef.getAlias(),
                        this.tableRef.getPartitionNames(), (ArrayList) tablets,
                        this.tableRef.getTableSample(), this.tableRef.getCommonHints());
                tableRefList.add(tblRef);
            }
            tableRefListPerParallel.add(tableRefList);
        }
        return tableRefListPerParallel;
    }

    private List<List<Long>> splitTablets() throws UserException {
        // get tablets
        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException(this.tableName.getDb());
        OlapTable table = db.getOlapTableOrAnalysisException(this.tableName.getTbl());
        List<Long> tabletIdList = Lists.newArrayList();
        table.readLock();
        try {
            final Collection<Partition> partitions = new ArrayList<Partition>();
            // get partitions
            // user specifies partitions, already checked in ExportCommand
            if (!this.partitionNames.isEmpty()) {
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

        /**
         * Assign tablets to per parallel, for example:
         * If the number of all tablets if 10, and the real parallelism is 4,
         * then, the number of tablets of per parallel should be: 3 3 2 2.
         */
        Integer tabletsAllNum = tabletIdList.size();
        tabletsNum = tabletsAllNum;
        Integer tabletsNumPerParallel = tabletsAllNum / this.parallelism;
        Integer tabletsNumPerQueryRemainder = tabletsAllNum - tabletsNumPerParallel * this.parallelism;

        List<List<Long>> tabletsListPerParallel = Lists.newArrayList();
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
                Env.getCurrentEnv().getExportTaskRegister().cancelTask(id);
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

    /**
     * If there are export which state is PENDING or EXPORTING or IN_QUEUE
     * in checkpoint, we translate their state to CANCELLED.
     *
     * This function is only used in replay catalog phase.
     */
    public void cancelReplayedExportJob() {
        if (state == ExportJobState.PENDING || state == ExportJobState.EXPORTING || state == ExportJobState.IN_QUEUE) {
            final String failMsg = "FE restarted or Master changed during exporting. Job must be cancelled.";
            this.failMsg = new ExportFailMsg(ExportFailMsg.CancelType.RUN_FAIL, failMsg);
            setExportJobState(ExportJobState.CANCELLED);
        }
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
