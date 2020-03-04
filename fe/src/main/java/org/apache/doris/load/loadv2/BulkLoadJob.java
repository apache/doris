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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.doris.analysis.*;
import org.apache.doris.catalog.*;
import org.apache.doris.common.*;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.BrokerFileGroupAggInfo;
import org.apache.doris.load.FailMsg;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * parent of BrokerLoadJob and SparkLoadJob from load stmt
 */
public abstract class BulkLoadJob extends LoadJob {
    private static final Logger LOG = LogManager.getLogger(BulkLoadJob.class);

    // this param is used to persist the expr of columns
    // the origin stmt is persisted instead of columns expr
    // the expr of columns will be reanalyze when the log is replayed
    private String originStmt = "";

    // include broker desc and data desc
    protected BrokerFileGroupAggInfo fileGroupAggInfo = new BrokerFileGroupAggInfo();
    protected List<TabletCommitInfo> commitInfos = Lists.newArrayList();

    // sessionVariable's name -> sessionVariable's value
    // we persist these sessionVariables due to the session is not available when replaying the job.
    private Map<String, String> sessionVariables = Maps.newHashMap();

    // only for log replay
    public BulkLoadJob() {
        super();
    }

    public BulkLoadJob(long dbId, String label, String originStmt) throws MetaNotFoundException {
        super(dbId, label);
        this.originStmt = Strings.nullToEmpty(originStmt);
        this.authorizationInfo = gatherAuthInfo();

        if (ConnectContext.get() != null) {
            SessionVariable var = ConnectContext.get().getSessionVariable();
            sessionVariables.put(SessionVariable.SQL_MODE, Long.toString(var.getSqlMode()));
        } else {
            sessionVariables.put(SessionVariable.SQL_MODE, String.valueOf(SqlModeHelper.MODE_DEFAULT));
        }
    }

    public static BulkLoadJob fromLoadStmt(LoadStmt stmt, String originStmt) throws DdlException {
        // get db id
        String dbName = stmt.getLabel().getDbName();
        Database db = Catalog.getCurrentCatalog().getDb(stmt.getLabel().getDbName());
        if (db == null) {
            throw new DdlException("Database[" + dbName + "] does not exist");
        }

        // create job
        BulkLoadJob bulkLoadJob = null;
        try {
            switch (stmt.getDataProcessorDesc().getEtlJobType()) {
                case BROKER:
                    bulkLoadJob = new BrokerLoadJob(db.getId(), stmt.getLabel().getLabelName(),
                            stmt.getBrokerDesc(), originStmt);
                    break;
                case SPARK:
                    bulkLoadJob = new SparkLoadJob(db.getId(), stmt.getLabel().getLabelName(),
                            (EtlClusterDesc) stmt.getDataProcessorDesc(), originStmt);
                    break;
                case MINI:
                case DELETE:
                case HADOOP:
                case INSERT:
                    throw new DdlException("LoadManager only support the broker and spark load.");
                default:
                    throw new DdlException("Unknown load job type.");
            }

            bulkLoadJob.setJobProperties(stmt.getProperties());
            bulkLoadJob.checkAndSetDataSourceInfo(db, stmt.getDataDescriptions());
            return bulkLoadJob;
        } catch (MetaNotFoundException e) {
            throw new DdlException(e.getMessage());
        }
    }

    private void checkAndSetDataSourceInfo(Database db, List<DataDescription> dataDescriptions) throws DdlException {
        // check data source info
        db.readLock();
        try {
            for (DataDescription dataDescription : dataDescriptions) {
                BrokerFileGroup fileGroup = new BrokerFileGroup(dataDescription);
                fileGroup.parse(db, dataDescription);
                fileGroupAggInfo.addFileGroup(fileGroup);
            }
        } finally {
            db.readUnlock();
        }
    }

    private AuthorizationInfo gatherAuthInfo() throws MetaNotFoundException {
        Database database = Catalog.getCurrentCatalog().getDb(dbId);
        if (database == null) {
            throw new MetaNotFoundException("Database " + dbId + "has been deleted");
        }
        return new AuthorizationInfo(database.getFullName(), getTableNames());
    }

    @Override
    public Set<String> getTableNamesForShow() {
        Set<String> result = Sets.newHashSet();
        Database database = Catalog.getCurrentCatalog().getDb(dbId);
        if (database == null) {
            for (long tableId : fileGroupAggInfo.getAllTableIds()) {
                result.add(String.valueOf(tableId));
            }
            return result;
        }
        for (long tableId : fileGroupAggInfo.getAllTableIds()) {
            Table table = database.getTable(tableId);
            if (table == null) {
                result.add(String.valueOf(tableId));
            } else {
                result.add(table.getName());
            }
        }
        return result;
    }

    @Override
    public Set<String> getTableNames() throws MetaNotFoundException{
        Set<String> result = Sets.newHashSet();
        Database database = Catalog.getCurrentCatalog().getDb(dbId);
        if (database == null) {
            throw new MetaNotFoundException("Database " + dbId + "has been deleted");
        }
        // The database will not be locked in here.
        // The getTable is a thread-safe method called without read lock of database
        for (long tableId : fileGroupAggInfo.getAllTableIds()) {
            Table table = database.getTable(tableId);
            if (table == null) {
                throw new MetaNotFoundException("Failed to find table " + tableId + " in db " + dbId);
            } else {
                result.add(table.getName());
            }
        }
        return result;
    }

    @Override
    public void beginTxn()
            throws LabelAlreadyUsedException, BeginTransactionException, AnalysisException, DuplicatedRequestException {
        transactionId = Catalog.getCurrentGlobalTransactionMgr()
                .beginTransaction(dbId, label, null, "FE: " + FrontendOptions.getLocalHostAddress(),
                                  TransactionState.LoadJobSourceType.BATCH_LOAD_JOB, id,
                                  timeoutSecond);
    }

    @Override
    public void onTaskFailed(long taskId, FailMsg failMsg) {
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
                Catalog.getCurrentCatalog().getLoadTaskScheduler().submit(loadTask);
                return;
            }
        } finally {
            writeUnlock();
        }
    }

    /**
     * If the db or table could not be found, the Broker load job will be cancelled.
     */
    @Override
    public void analyze() {
        if (Strings.isNullOrEmpty(originStmt)) {
            return;
        }
        // Reset dataSourceInfo, it will be re-created in analyze
        fileGroupAggInfo = new BrokerFileGroupAggInfo();
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(originStmt),
                Long.valueOf(sessionVariables.get(SessionVariable.SQL_MODE))));
        LoadStmt stmt = null;
        try {
            stmt = (LoadStmt) parser.parse().value;
            for (DataDescription dataDescription : stmt.getDataDescriptions()) {
                dataDescription.analyzeWithoutCheckPriv();
            }
            Database db = Catalog.getCurrentCatalog().getDb(dbId);
            if (db == null) {
                throw new DdlException("Database[" + dbId + "] does not exist");
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
        Text.writeString(out, originStmt);

        out.writeInt(sessionVariables.size());
        for (Map.Entry<String, String> entry : sessionVariables.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }
    }

    public void readFields(DataInput in, BrokerDesc brokerDesc) throws IOException {
        super.readFields(in);

        if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_73) {
            brokerDesc = BrokerDesc.read(in);
        }

        if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_61) {
            fileGroupAggInfo.readFields(in);
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_58) {
            originStmt = Text.readString(in);
        } else {
            originStmt = "";
        }
        // The origin stmt does not be analyzed in here.
        // The reason is that it will thrown MetaNotFoundException when the tableId could not be found by tableName.
        // The origin stmt will be analyzed after the replay is completed.

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_66) {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String key = Text.readString(in);
                String value = Text.readString(in);
                sessionVariables.put(key, value);
            }
        } else {
            // old version of load does not have sqlmode, set it to default
            sessionVariables.put(SessionVariable.SQL_MODE, String.valueOf(SqlModeHelper.MODE_DEFAULT));
        }
    }

}
