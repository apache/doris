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
import org.apache.doris.analysis.QueryStmt;
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
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.system.Backend;
import org.apache.doris.task.AgentClient;
import org.apache.doris.task.ExportExportingTask;
import org.apache.doris.thrift.TAgentResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloScanRange;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TSnapshotRequest;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TypesConstants;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

// NOTE: we must be carefully if we send next request
//       as soon as receiving one instance's report from one BE,
//       because we may change job's member concurrently.
public class ExportJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(ExportJob.class);

    private static final String BROKER_PROPERTY_PREFIXES = "broker.";

    public enum JobState {
        PENDING,
        IN_QUEUE,
        EXPORTING,
        FINISHED,
        CANCELLED,
    }

    @SerializedName("id")
    private long id;
    @SerializedName("queryId")
    private String queryId;
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
    @SerializedName("partitions")
    private List<String> partitions;
    @SerializedName("tableName")
    private TableName tableName;
    @SerializedName("state")
    private JobState state;
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
    // progress has two functions at EXPORTING stage:
    // 1. when progress < 100, it indicates exporting
    // 2. set progress = 100 ONLY when exporting progress is completely done
    private int progress;
    private long startTimeMs;
    private long finishTimeMs;
    private ExportFailMsg failMsg;
    private String outfileInfo;

    private TableRef tableRef;

    private Expr whereExpr;

    private String sql = "";

    // The selectStmt is sql 'select ... into outfile ...'
    @Getter
    private List<QueryStmt> selectStmtList = Lists.newArrayList();

    private List<String> exportColumns = Lists.newArrayList();

    private Table exportTable;

    // when set to true, means this job instance is created by replay thread(FE restarted or master changed)
    private boolean isReplayed = false;

    private SessionVariable sessionVariables;

    private Thread doExportingThread;

    private ExportExportingTask task;

    private List<TScanRangeLocations> tabletLocations = Lists.newArrayList();
    // backend_address => snapshot path
    private List<Pair<TNetworkAddress, String>> snapshotPaths = Lists.newArrayList();

    public ExportJob() {
        this.id = -1;
        this.queryId = "";
        this.dbId = -1;
        this.tableId = -1;
        this.state = JobState.PENDING;
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

    public void setJob(ExportStmt stmt) throws UserException {
        String dbName = stmt.getTblName().getDb();
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);
        Preconditions.checkNotNull(stmt.getBrokerDesc());
        this.brokerDesc = stmt.getBrokerDesc();
        this.columnSeparator = stmt.getColumnSeparator();
        this.lineDelimiter = stmt.getLineDelimiter();
        this.label = stmt.getLabel();
        this.queryId = ConnectContext.get() != null ? DebugUtil.printId(ConnectContext.get().queryId()) : "N/A";
        String path = stmt.getPath();
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        this.whereExpr = stmt.getWhereExpr();
        this.exportPath = path;
        this.sessionVariables = stmt.getSessionVariables();
        this.timeoutSecond = sessionVariables.getQueryTimeoutS();

        this.qualifiedUser = stmt.getQualifiedUser();
        this.userIdentity = stmt.getUserIdentity();
        this.format = stmt.getFormat();
        this.maxFileSize = stmt.getMaxFileSize();
        this.deleteExistingFiles = stmt.getDeleteExistingFiles();
        this.partitions = stmt.getPartitions();

        this.exportTable = db.getTableOrDdlException(stmt.getTblName().getTbl());
        this.columns = stmt.getColumns();
        this.tableRef = stmt.getTableRef();
        if (!Strings.isNullOrEmpty(this.columns)) {
            Splitter split = Splitter.on(',').trimResults().omitEmptyStrings();
            this.exportColumns = split.splitToList(stmt.getColumns().toLowerCase());
        }
        exportTable.readLock();
        try {
            this.dbId = db.getId();
            this.tableId = exportTable.getId();
            this.tableName = stmt.getTblName();
            if (selectStmtList.isEmpty()) {
                // This scenario is used for 'EXPORT TABLE tbl INTO PATH'
                // we need generate Select Statement
                generateQueryStmt();
            }
        } finally {
            exportTable.readUnlock();
        }
        this.sql = stmt.toSql();
        this.origStmt = stmt.getOrigStmt();
    }

    private void generateQueryStmt() {
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

        List<TableRef> tableRefList = Lists.newArrayList();
        tableRefList.add(this.tableRef);
        FromClause fromClause = new FromClause(tableRefList);

        SelectStmt selectStmt = new SelectStmt(list, fromClause, this.whereExpr, null,
                null, null, LimitElement.NO_LIMIT);
        // generate outfile clause
        OutFileClause outfile = new OutFileClause(this.exportPath, this.format, convertOutfileProperties());
        selectStmt.setOutFileClause(outfile);
        selectStmt.setOrigStmt(new OriginStatement(selectStmt.toSql(), 0));
        selectStmtList.add(selectStmt);
    }

    private Map<String, String> convertOutfileProperties() {
        Map<String, String> outfileProperties = Maps.newHashMap();

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
            for (Entry<String, String> kv : brokerDesc.getProperties().entrySet()) {
                outfileProperties.put(BROKER_PROPERTY_PREFIXES + kv.getKey(), kv.getValue());
            }
        } else {
            for (Entry<String, String> kv : brokerDesc.getProperties().entrySet()) {
                outfileProperties.put(kv.getKey(), kv.getValue());
            }
        }
        return outfileProperties;
    }

    public String getColumns() {
        return columns;
    }

    public long getId() {
        return id;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return this.tableId;
    }

    public Expr getWhereExpr() {
        return whereExpr;
    }

    public synchronized JobState getState() {
        return state;
    }

    public BrokerDesc getBrokerDesc() {
        return brokerDesc;
    }

    public void setBrokerDesc(BrokerDesc brokerDesc) {
        this.brokerDesc = brokerDesc;
    }

    public String getExportPath() {
        return exportPath;
    }

    public String getColumnSeparator() {
        return this.columnSeparator;
    }

    public String getLineDelimiter() {
        return this.lineDelimiter;
    }

    public int getTimeoutSecond() {
        return timeoutSecond;
    }

    public String getFormat() {
        return format;
    }

    public String getMaxFileSize() {
        return maxFileSize;
    }

    public String getDeleteExistingFiles() {
        return deleteExistingFiles;
    }

    public String getQualifiedUser() {
        return qualifiedUser;
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    public List<String> getPartitions() {
        return partitions;
    }

    public int getProgress() {
        return progress;
    }

    public void setProgress(int progress) {
        this.progress = progress;
    }

    public long getCreateTimeMs() {
        return createTimeMs;
    }

    public long getStartTimeMs() {
        return startTimeMs;
    }

    public void setStartTimeMs(long startTimeMs) {
        this.startTimeMs = startTimeMs;
    }

    public long getFinishTimeMs() {
        return finishTimeMs;
    }

    public void setFinishTimeMs(long finishTimeMs) {
        this.finishTimeMs = finishTimeMs;
    }

    public ExportFailMsg getFailMsg() {
        return failMsg;
    }

    public void setFailMsg(ExportFailMsg failMsg) {
        this.failMsg = failMsg;
    }

    public String getOutfileInfo() {
        return outfileInfo;
    }

    public void setOutfileInfo(String outfileInfo) {
        this.outfileInfo = outfileInfo;
    }


    public synchronized Thread getDoExportingThread() {
        return doExportingThread;
    }

    public synchronized void setDoExportingThread(Thread isExportingThread) {
        this.doExportingThread = isExportingThread;
    }

    public List<TScanRangeLocations> getTabletLocations() {
        return tabletLocations;
    }

    public List<Pair<TNetworkAddress, String>> getSnapshotPaths() {
        return this.snapshotPaths;
    }

    public void addSnapshotPath(Pair<TNetworkAddress, String> snapshotPath) {
        this.snapshotPaths.add(snapshotPath);
    }

    public String getSql() {
        return sql;
    }

    public ExportExportingTask getTask() {
        return task;
    }

    public void setTask(ExportExportingTask task) {
        this.task = task;
    }

    public TableName getTableName() {
        return tableName;
    }

    public SessionVariable getSessionVariables() {
        return sessionVariables;
    }

    public synchronized void cancel(ExportFailMsg.CancelType type, String msg) {
        if (msg != null) {
            failMsg = new ExportFailMsg(type, msg);
        }
        if (updateState(ExportJob.JobState.CANCELLED, false)) {
            // release snapshot
            // Status releaseSnapshotStatus = releaseSnapshotPaths();
            // if (!releaseSnapshotStatus.ok()) {
            //     // snapshot will be removed by GC thread on BE, finally.
            //     LOG.warn("failed to release snapshot for export job: {}. err: {}", id,
            //             releaseSnapshotStatus.getErrorMsg());
            // }
        }
    }

    public synchronized boolean finish(List<OutfileInfo> outfileInfoList) {
        outfileInfo = GsonUtils.GSON.toJson(outfileInfoList);
        if (updateState(ExportJob.JobState.FINISHED)) {
            return true;
        }
        return false;
    }

    public synchronized boolean updateState(ExportJob.JobState newState) {
        return this.updateState(newState, false);
    }

    public synchronized boolean updateState(ExportJob.JobState newState, boolean isReplay) {
        // We do not persist EXPORTING state in new version of metadata,
        // but EXPORTING state may still exist in older versions of metadata.
        // So if isReplay == true and newState == EXPORTING, we just ignore this update.
        if (isFinalState() || (isReplay && newState == JobState.EXPORTING)) {
            return false;
        }
        ExportJob.JobState oldState = state;
        state = newState;
        switch (newState) {
            case PENDING:
            case IN_QUEUE:
                progress = 0;
                break;
            case EXPORTING:
                // if isReplay == true, startTimeMs will be read from log
                if (!isReplay) {
                    startTimeMs = System.currentTimeMillis();
                }
                break;
            case FINISHED:
            case CANCELLED:
                // if isReplay == true, finishTimeMs will be read from log
                if (!isReplay) {
                    finishTimeMs = System.currentTimeMillis();
                    // maybe user cancel this job
                    if (task != null && oldState == JobState.EXPORTING && task.getStmtExecutor() != null) {
                        task.getStmtExecutor().cancel();
                    }
                }
                progress = 100;
                break;
            default:
                Preconditions.checkState(false, "wrong job state: " + newState.name());
                break;
        }
        // we only persist Pending/Cancel/Finish state
        if (!isReplay && newState != JobState.IN_QUEUE && newState != JobState.EXPORTING) {
            Env.getCurrentEnv().getEditLog().logExportUpdateState(id, newState);
        }
        return true;
    }

    public synchronized boolean isFinalState() {
        return this.state == ExportJob.JobState.CANCELLED || this.state == ExportJob.JobState.FINISHED;
    }

    private Status makeSnapshots() {
        List<TScanRangeLocations> tabletLocations = getTabletLocations();
        if (tabletLocations == null) {
            return Status.OK;
        }
        for (TScanRangeLocations tablet : tabletLocations) {
            TScanRange scanRange = tablet.getScanRange();
            if (!scanRange.isSetPaloScanRange()) {
                continue;
            }
            TPaloScanRange paloScanRange = scanRange.getPaloScanRange();
            List<TScanRangeLocation> locations = tablet.getLocations();
            for (TScanRangeLocation location : locations) {
                TNetworkAddress address = location.getServer();
                String host = address.getHostname();
                int port = address.getPort();
                Backend backend = Env.getCurrentSystemInfo().getBackendWithBePort(host, port);
                if (backend == null) {
                    return Status.CANCELLED;
                }
                long backendId = backend.getId();
                if (!Env.getCurrentSystemInfo().checkBackendQueryAvailable(backendId)) {
                    return Status.CANCELLED;
                }
                TSnapshotRequest snapshotRequest = new TSnapshotRequest();
                snapshotRequest.setTabletId(paloScanRange.getTabletId());
                snapshotRequest.setSchemaHash(Integer.parseInt(paloScanRange.getSchemaHash()));
                snapshotRequest.setVersion(Long.parseLong(paloScanRange.getVersion()));
                snapshotRequest.setTimeout(getTimeoutSecond());
                snapshotRequest.setPreferredSnapshotVersion(TypesConstants.TPREFER_SNAPSHOT_REQ_VERSION);

                AgentClient client = new AgentClient(host, port);
                TAgentResult result = client.makeSnapshot(snapshotRequest);
                if (result == null || result.getStatus().getStatusCode() != TStatusCode.OK) {
                    String err = "snapshot for tablet " + paloScanRange.getTabletId() + " failed on backend "
                            + address.toString() + ". reason: "
                            + (result == null ? "unknown" : result.getStatus().error_msgs);
                    LOG.warn("{}, export job: {}", err, id);
                    return new Status(TStatusCode.CANCELLED, err);
                }
                addSnapshotPath(Pair.of(address, result.getSnapshotPath()));
            }
        }
        return Status.OK;
    }

    public Status releaseSnapshotPaths() {
        List<Pair<TNetworkAddress, String>> snapshotPaths = getSnapshotPaths();
        LOG.debug("snapshotPaths:{}", snapshotPaths);
        for (Pair<TNetworkAddress, String> snapshotPath : snapshotPaths) {
            TNetworkAddress address = snapshotPath.first;
            String host = address.getHostname();
            int port = address.getPort();
            Backend backend = Env.getCurrentSystemInfo().getBackendWithBePort(host, port);
            if (backend == null) {
                continue;
            }
            long backendId = backend.getId();
            if (!Env.getCurrentSystemInfo().checkBackendQueryAvailable(backendId)) {
                continue;
            }

            AgentClient client = new AgentClient(host, port);
            TAgentResult result = client.releaseSnapshot(snapshotPath.second);
            if (result == null || result.getStatus().getStatusCode() != TStatusCode.OK) {
                continue;
            }
        }
        snapshotPaths.clear();
        return Status.OK;
    }

    public boolean isExpired(long curTime) {
        return (curTime - createTimeMs) / 1000 > Config.history_job_keep_max_second
                && (state == ExportJob.JobState.CANCELLED || state == ExportJob.JobState.FINISHED);
    }

    public String getLabel() {
        return label;
    }

    public String getQueryId() {
        return queryId;
    }

    @Override
    public String toString() {
        return "ExportJob [jobId=" + id
                + ", label=" + label
                + ", dbId=" + dbId
                + ", tableId=" + tableId
                + ", state=" + state
                + ", path=" + exportPath
                + ", partitions=(" + StringUtils.join(partitions, ",") + ")"
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
            partitions = Lists.newArrayList();
            int partitionSize = in.readInt();
            for (int i = 0; i < partitionSize; ++i) {
                String partitionName = Text.readString(in);
                partitions.add(partitionName);
            }
        }

        state = JobState.valueOf(Text.readString(in));
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

    public boolean isReplayed() {
        return isReplayed;
    }

    // for only persist op when switching job state.
    public static class StateTransfer implements Writable {
        @SerializedName("jobId")
        long jobId;
        @SerializedName("state")
        JobState state;
        @SerializedName("startTimeMs")
        private long startTimeMs;
        @SerializedName("finishTimeMs")
        private long finishTimeMs;
        @SerializedName("failMsg")
        private ExportFailMsg failMsg;
        @SerializedName("outFileInfo")
        private String outFileInfo;

        // used for reading from one log
        public StateTransfer() {
            this.jobId = -1;
            this.state = JobState.CANCELLED;
            this.failMsg = new ExportFailMsg(ExportFailMsg.CancelType.UNKNOWN, "");
            this.outFileInfo = "";
        }

        // used for persisting one log
        public StateTransfer(long jobId, JobState state) {
            this.jobId = jobId;
            this.state = state;
            ExportJob job = Env.getCurrentEnv().getExportMgr().getJob(jobId);
            this.startTimeMs = job.getStartTimeMs();
            this.finishTimeMs = job.getFinishTimeMs();
            this.failMsg = job.getFailMsg();
            this.outFileInfo = job.getOutfileInfo();
        }

        public long getJobId() {
            return jobId;
        }

        public JobState getState() {
            return state;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            String json = GsonUtils.GSON.toJson(this);
            Text.writeString(out, json);
        }

        public static StateTransfer read(DataInput in) throws IOException {
            if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_120) {
                StateTransfer transfer = new StateTransfer();
                transfer.readFields(in);
                return transfer;
            }
            String json = Text.readString(in);
            StateTransfer transfer = GsonUtils.GSON.fromJson(json, ExportJob.StateTransfer.class);
            return transfer;
        }

        private void readFields(DataInput in) throws IOException {
            jobId = in.readLong();
            state = JobState.valueOf(Text.readString(in));
        }

        public long getStartTimeMs() {
            return startTimeMs;
        }

        public long getFinishTimeMs() {
            return finishTimeMs;
        }

        public String getOutFileInfo() {
            return outFileInfo;
        }

        public ExportFailMsg getFailMsg() {
            return failMsg;
        }
    }

    public static class OutfileInfo {
        @SerializedName("fileNumber")
        private String fileNumber;
        @SerializedName("totalRows")
        private String totalRows;
        @SerializedName("fileSize")
        private String fileSize;
        @SerializedName("url")
        private String url;

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getFileNumber() {
            return fileNumber;
        }

        public void setFileNumber(String fileNumber) {
            this.fileNumber = fileNumber;
        }

        public String getTotalRows() {
            return totalRows;
        }

        public void setTotalRows(String totalRows) {
            this.totalRows = totalRows;
        }

        public String getFileSize() {
            return fileSize;
        }

        public void setFileSize(String fileSize) {
            this.fileSize = fileSize;
        }
    }
}
