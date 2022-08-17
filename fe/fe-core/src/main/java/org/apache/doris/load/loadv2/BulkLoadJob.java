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
import org.apache.doris.analysis.DataDescription;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AuthorizationInfo;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.BrokerFileGroupAggInfo;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.FailMsg;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.plugin.LoadAuditEvent;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionState;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;

/**
 * parent class of BrokerLoadJob and SparkLoadJob from load stmt
 */
public abstract class BulkLoadJob extends LoadJob {
    private static final Logger LOG = LogManager.getLogger(BulkLoadJob.class);

    // input params
    protected BrokerDesc brokerDesc;
    // this param is used to persist the expr of columns
    // the origin stmt is persisted instead of columns expr
    // the expr of columns will be reanalyze when the log is replayed
    private OriginStatement originStmt;

    private UserIdentity userInfo;

    // include broker desc and data desc
    protected BrokerFileGroupAggInfo fileGroupAggInfo = new BrokerFileGroupAggInfo();
    protected List<TabletCommitInfo> commitInfos = Lists.newArrayList();

    // sessionVariable's name -> sessionVariable's value
    // we persist these sessionVariables due to the session is not available when replaying the job.
    private Map<String, String> sessionVariables = Maps.newHashMap();

    public BulkLoadJob(EtlJobType jobType) {
        super(jobType);
    }

    public BulkLoadJob(EtlJobType jobType, long dbId, String label,
            OriginStatement originStmt, UserIdentity userInfo) throws MetaNotFoundException {
        super(jobType, dbId, label);
        this.originStmt = originStmt;
        this.authorizationInfo = gatherAuthInfo();
        this.userInfo = userInfo;

        if (ConnectContext.get() != null) {
            SessionVariable var = ConnectContext.get().getSessionVariable();
            sessionVariables.put(SessionVariable.SQL_MODE, Long.toString(var.getSqlMode()));
        } else {
            sessionVariables.put(SessionVariable.SQL_MODE, String.valueOf(SqlModeHelper.MODE_DEFAULT));
        }
    }

    public static BulkLoadJob fromLoadStmt(LoadStmt stmt) throws DdlException {
        // get db id
        String dbName = stmt.getLabel().getDbName();
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);

        // create job
        BulkLoadJob bulkLoadJob;
        try {
            switch (stmt.getEtlJobType()) {
                case BROKER:
                    bulkLoadJob = new BrokerLoadJob(db.getId(), stmt.getLabel().getLabelName(), stmt.getBrokerDesc(),
                            stmt.getOrigStmt(), stmt.getUserInfo());
                    break;
                case SPARK:
                    bulkLoadJob = new SparkLoadJob(db.getId(), stmt.getLabel().getLabelName(), stmt.getResourceDesc(),
                            stmt.getOrigStmt(), stmt.getUserInfo());
                    break;
                case MINI:
                case DELETE:
                case HADOOP:
                case INSERT:
                    throw new DdlException("LoadManager only support create broker and spark load job from stmt.");
                default:
                    throw new DdlException("Unknown load job type.");
            }
            bulkLoadJob.setJobProperties(stmt.getProperties());
            bulkLoadJob.checkAndSetDataSourceInfo((Database) db, stmt.getDataDescriptions());
            return bulkLoadJob;
        } catch (MetaNotFoundException e) {
            throw new DdlException(e.getMessage());
        }
    }

    private void checkAndSetDataSourceInfo(Database db, List<DataDescription> dataDescriptions) throws DdlException {
        // check data source info
        db.readLock();
        try {
            LoadTask.MergeType mergeType = null;
            for (DataDescription dataDescription : dataDescriptions) {
                if (mergeType == null) {
                    mergeType = dataDescription.getMergeType();
                }
                if (mergeType != dataDescription.getMergeType()) {
                    throw new DdlException("merge type in all statement must be the same.");
                }
                BrokerFileGroup fileGroup = new BrokerFileGroup(dataDescription);
                fileGroup.parse(db, dataDescription);
                fileGroupAggInfo.addFileGroup(fileGroup);
            }
        } finally {
            db.readUnlock();
        }
    }

    private AuthorizationInfo gatherAuthInfo() throws MetaNotFoundException {
        Database database = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
        return new AuthorizationInfo(database.getFullName(), getTableNames());
    }

    @Override
    public Set<String> getTableNamesForShow() {
        Optional<Database> db = Env.getCurrentInternalCatalog().getDb(dbId);
        return fileGroupAggInfo.getAllTableIds().stream()
                .map(tableId -> db.flatMap(d -> d.getTable(tableId)).map(TableIf::getName)
                        .orElse(String.valueOf(tableId)))
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getTableNames() throws MetaNotFoundException {
        Set<String> result = Sets.newHashSet();
        Database database = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
        // The database will not be locked in here.
        // The getTable is a thread-safe method called without read lock of database
        for (long tableId : fileGroupAggInfo.getAllTableIds()) {
            Table table = database.getTableOrMetaException(tableId);
            result.add(table.getName());
        }
        return result;
    }

    @Override
    public void onTaskFailed(long taskId, FailMsg failMsg) {
        List<LoadTask> retriedTasks = Lists.newArrayList();
        writeLock();
        try {
            // check if job has been completed
            if (isTxnDone()) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("state", state)
                        .add("error_msg", "this task will be ignored when job is: " + state)
                        .build());
                return;
            }
            LoadTask loadTask = idToTasks.get(taskId);
            if (loadTask == null) {
                return;
            }
            if (loadTask.getRetryTime() <= 0) {
                unprotectedExecuteCancel(failMsg, true);
                logFinalOperation();
                return;
            } else {
                // retry task
                idToTasks.remove(loadTask.getSignature());
                if (loadTask instanceof LoadLoadingTask) {
                    loadStatistic.removeLoad(((LoadLoadingTask) loadTask).getLoadId());
                }
                loadTask.updateRetryInfo();
                idToTasks.put(loadTask.getSignature(), loadTask);
                // load id will be added to loadStatistic when executing this task
                retriedTasks.add(loadTask);
            }
        } finally {
            writeUnlock();
        }

        // submit retried loading task outside the job's lock, cause task submitting may be block for a while
        for (LoadTask loadTask : retriedTasks) {
            try {
                if (loadTask.getTaskType() == LoadTask.TaskType.PENDING) {
                    Env.getCurrentEnv().getPendingLoadTaskScheduler().submit(loadTask);
                } else if (loadTask.getTaskType() == LoadTask.TaskType.LOADING) {
                    Env.getCurrentEnv().getLoadingLoadTaskScheduler().submit(loadTask);
                }
            } catch (RejectedExecutionException e) {
                writeLock();
                try {
                    unprotectedExecuteCancel(failMsg, true);
                    logFinalOperation();
                    return;
                } finally {
                    writeUnlock();
                }
            }
        }
    }

    /**
     * If the db or table could not be found, the Broker load job will be cancelled.
     */
    @Override
    public void analyze() {
        if (originStmt == null || Strings.isNullOrEmpty(originStmt.originStmt)) {
            return;
        }
        // Reset dataSourceInfo, it will be re-created in analyze
        fileGroupAggInfo = new BrokerFileGroupAggInfo();
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(originStmt.originStmt),
                Long.valueOf(sessionVariables.get(SessionVariable.SQL_MODE))));
        LoadStmt stmt;
        try {
            Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbId);
            stmt = (LoadStmt) SqlParserUtils.getStmt(parser, originStmt.idx);
            for (DataDescription dataDescription : stmt.getDataDescriptions()) {
                dataDescription.analyzeWithoutCheckPriv(db.getFullName());
            }
            checkAndSetDataSourceInfo(db, stmt.getDataDescriptions());
        } catch (Exception e) {
            LOG.info(new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("origin_stmt", originStmt)
                    .add("msg", "The failure happens in analyze, the load job will be cancelled with error:"
                            + e.getMessage())
                    .build(), e);
            cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, e.getMessage()), false, true);
        }
    }

    @Override
    protected void replayTxnAttachment(TransactionState txnState) {
        if (txnState.getTxnCommitAttachment() == null) {
            // The txn attachment maybe null when broker load has been cancelled without attachment.
            // The end log of broker load has been record but the callback id of txnState hasn't been removed
            // So the callback of txn is executed when log of txn aborted is replayed.
            return;
        }
        unprotectReadEndOperation((LoadJobFinalOperation) txnState.getTxnCommitAttachment());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        brokerDesc.write(out);
        originStmt.write(out);
        userInfo.write(out);

        out.writeInt(sessionVariables.size());
        for (Map.Entry<String, String> entry : sessionVariables.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        brokerDesc = BrokerDesc.read(in);
        originStmt = OriginStatement.read(in);
        // The origin stmt does not be analyzed in here.
        // The reason is that it will thrown MetaNotFoundException when the tableId could not be found by tableName.
        // The origin stmt will be analyzed after the replay is completed.

        userInfo = UserIdentity.read(in);
        // must set is as analyzed, because when write the user info to meta image, it will be checked.
        userInfo.setIsAnalyzed();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            sessionVariables.put(key, value);
        }
    }

    public UserIdentity getUserInfo() {
        return userInfo;
    }

    @Override
    protected void auditFinishedLoadJob() {
        try {
            String dbName = getDb().getFullName();
            String tableListName = StringUtils.join(getTableNames(), ",");
            List<String> filePathList = Lists.newArrayList();
            for (List<BrokerFileGroup> brokerFileGroups : fileGroupAggInfo.getAggKeyToFileGroups().values()) {
                for (BrokerFileGroup brokerFileGroup : brokerFileGroups) {
                    filePathList.add("(" + StringUtils.join(brokerFileGroup.getFilePaths(), ",") + ")");
                }
            }
            String filePathListName = StringUtils.join(filePathList, ",");
            String brokerUserName = getBrokerUserName();
            AuditEvent auditEvent = new LoadAuditEvent.AuditEventBuilder()
                    .setEventType(AuditEvent.EventType.LOAD_SUCCEED)
                    .setJobId(id).setLabel(label).setLoadType(jobType.name()).setDb(dbName).setTableList(tableListName)
                    .setFilePathList(filePathListName).setBrokerUser(brokerUserName).setTimestamp(createTimestamp)
                    .setLoadStartTime(loadStartTimestamp).setLoadFinishTime(finishTimestamp)
                    .setScanRows(loadStatistic.getScannedRows()).setScanBytes(loadStatistic.totalFileSizeB)
                    .setFileNumber(loadStatistic.fileNum)
                    .build();
            Env.getCurrentEnv().getAuditEventProcessor().handleAuditEvent(auditEvent);
        } catch (Exception e) {
            LOG.warn("audit finished load job info failed", e);
        }
    }

    private String getBrokerUserName() {
        Map<String, String> properties = brokerDesc.getProperties();
        if (properties.containsKey("kerberos_principal")) {
            return properties.get("kerberos_principal");
        } else if (properties.containsKey("username")) {
            return properties.get("username");
        } else if (properties.containsKey("bos_accesskey")) {
            return properties.get("bos_accesskey");
        } else if (properties.containsKey("fs.s3a.access.key")) {
            return properties.get("fs.s3a.access.key");
        }
        return null;
    }
}
