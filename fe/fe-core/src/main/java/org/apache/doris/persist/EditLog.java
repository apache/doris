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

package org.apache.doris.persist;

import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.alter.AlterJobV2.JobState;
import org.apache.doris.alter.BatchAlterJobPersistInfo;
import org.apache.doris.alter.IndexChangeJob;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.backup.BackupJob;
import org.apache.doris.backup.Repository;
import org.apache.doris.backup.RestoreJob;
import org.apache.doris.binlog.AddPartitionRecord;
import org.apache.doris.binlog.CreateTableRecord;
import org.apache.doris.binlog.DropTableRecord;
import org.apache.doris.binlog.UpsertRecord;
import org.apache.doris.blockrule.SqlBlockRule;
import org.apache.doris.catalog.BrokerMgr;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.EncryptKey;
import org.apache.doris.catalog.EncryptKeyHelper;
import org.apache.doris.catalog.EncryptKeySearchDesc;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionSearchDesc;
import org.apache.doris.catalog.Resource;
import org.apache.doris.cloud.CloudWarmUpJob;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.persist.UpdateCloudReplicaInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.SmallFileMgr.SmallFile;
import org.apache.doris.cooldown.CooldownConfHandler;
import org.apache.doris.cooldown.CooldownConfList;
import org.apache.doris.cooldown.CooldownDelete;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogLog;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalObjectLog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.InitDatabaseLog;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.MetaIdMappingsLog;
import org.apache.doris.dictionary.Dictionary;
import org.apache.doris.ha.MasterInfo;
import org.apache.doris.indexpolicy.DropIndexPolicyLog;
import org.apache.doris.indexpolicy.IndexPolicy;
import org.apache.doris.insertoverwrite.InsertOverwriteLog;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.journal.Journal;
import org.apache.doris.journal.JournalBatch;
import org.apache.doris.journal.JournalCursor;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.journal.bdbje.BDBJEJournal;
import org.apache.doris.journal.bdbje.Timestamp;
import org.apache.doris.journal.local.LocalJournal;
import org.apache.doris.load.DeleteHandler;
import org.apache.doris.load.DeleteInfo;
import org.apache.doris.load.ExportJob;
import org.apache.doris.load.ExportJobState;
import org.apache.doris.load.ExportJobStateTransfer;
import org.apache.doris.load.ExportMgr;
import org.apache.doris.load.StreamLoadRecordMgr.FetchStreamLoadRecord;
import org.apache.doris.load.loadv2.LoadJob.LoadJobStateUpdateInfo;
import org.apache.doris.load.loadv2.LoadJobFinalOperation;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.privilege.UserPropertyInfo;
import org.apache.doris.plsql.metastore.PlsqlPackage;
import org.apache.doris.plsql.metastore.PlsqlProcedureKey;
import org.apache.doris.plsql.metastore.PlsqlStoredProcedure;
import org.apache.doris.plugin.PluginInfo;
import org.apache.doris.policy.DropPolicyLog;
import org.apache.doris.policy.Policy;
import org.apache.doris.policy.StoragePolicy;
import org.apache.doris.resource.workloadgroup.WorkloadGroup;
import org.apache.doris.resource.workloadschedpolicy.WorkloadSchedPolicy;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.AnalysisManager;
import org.apache.doris.statistics.NewPartitionLoadedEvent;
import org.apache.doris.statistics.TableStatsMeta;
import org.apache.doris.statistics.UpdateRowsEvent;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Frontend;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * EditLog maintains a log of the memory modifications.
 * Current we support only file editLog.
 */
public class EditLog {
    public static final Logger LOG = LogManager.getLogger(EditLog.class);

    // Helper class to hold log edit requests
    private static class EditLogItem {
        static AtomicLong nextUid = new AtomicLong(0);
        final short op;
        final Writable writable;
        final Object lock = new Object();
        boolean finished = false;
        long logId = -1;
        long uid = -1;

        EditLogItem(short op, Writable writable) {
            this.op = op;
            this.writable = writable;
            uid = nextUid.getAndIncrement();
        }
    }

    private final BlockingQueue<EditLogItem> logEditQueue = new LinkedBlockingQueue<>();
    private final Thread flushThread;

    private EditLogOutputStream editStream = null;

    private long txId = 0;


    private AtomicLong numTransactions = new AtomicLong(0);
    private AtomicLong totalTimeTransactions = new AtomicLong(0);
    private Journal journal;

    /**
     * The constructor.
     **/
    public EditLog(String nodeName) {
        String journalType = Config.edit_log_type;
        if (journalType.equalsIgnoreCase("bdb")) {
            journal = new BDBJEJournal(nodeName);
        } else if (journalType.equalsIgnoreCase("local")) {
            journal = new LocalJournal(Env.getCurrentEnv().getImageDir());
        } else {
            throw new IllegalArgumentException("Unknown edit log type: " + journalType);
        }

        // Flush thread initialization block
        flushThread = new Thread(() -> {
            while (true) {
                flushEditLog();
            }
        }, "EditLog-Flusher");
        flushThread.setDaemon(true);
        flushThread.start();
    }

    private void flushEditLog() {
        List<EditLogItem> batch = new ArrayList<>();
        try {
            batch.clear();
            EditLogItem first = logEditQueue.poll(100, TimeUnit.MILLISECONDS);
            if (first == null) {
                return;
            }
            batch.add(first);
            logEditQueue.drainTo(batch, Config.batch_edit_log_max_item_num - 1);

            int itemNum = Math.max(1, Math.min(Config.batch_edit_log_max_item_num, batch.size()));
            JournalBatch journalBatch = new JournalBatch(itemNum);

            if (DebugPointUtil.isEnable("EditLog.flushEditLog.exception")) {
                // For debug purpose, throw an exception to test the edit log flush
                throw new RuntimeException("EditLog.flushEditLog.exception");
            }
            // Array to record pairs of logId and num
            List<long[]> logIdNumPairs = new ArrayList<>();
            for (EditLogItem req : batch) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("try to flush editLog request: uid={}, op={}", req.uid, req.op);
                }
                journalBatch.addJournal(req.op, req.writable);
                if (journalBatch.shouldFlush()) {
                    long logId = journal.write(journalBatch);
                    logIdNumPairs.add(new long[]{logId, journalBatch.getJournalEntities().size()});
                    journalBatch = new JournalBatch(itemNum);
                }
            }
            // Write any remaining entries in the batch
            if (!journalBatch.getJournalEntities().isEmpty()) {
                long logId = journal.write(journalBatch);
                logIdNumPairs.add(new long[]{logId, journalBatch.getJournalEntities().size()});
            }

            // Notify all producers
            // For batch with index, assign logId to each request according to the batch flushes
            int reqIndex = 0;
            for (long[] pair : logIdNumPairs) {
                long logId = pair[0];
                int num = (int) pair[1];
                for (int i = 0; i < num && reqIndex < batch.size(); i++, reqIndex++) {
                    EditLogItem req = batch.get(reqIndex);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("notify editLog request: uid={}, op={}", req.uid, req.op);
                    }
                    req.logId = logId + i;
                    synchronized (req.lock) {
                        req.finished = true;
                        req.lock.notifyAll();
                    }
                }
            }
        } catch (Throwable t) {
            // Throwable contains all Exception and Error, such as IOException and
            // OutOfMemoryError
            if (journal instanceof BDBJEJournal) {
                LOG.error("BDBJE stats : {}", ((BDBJEJournal) journal).getBDBStats());
            }
            LOG.error("Fatal Error : write stream Exception", t);
            System.exit(-1);
        }

        txId += batch.size();
        // update statistics, etc. (optional, can be added as needed)
        if (txId >= Config.edit_log_roll_num) {
            LOG.info("txId {} is equal to or larger than edit_log_roll_num {}, will roll edit.", txId,
                    Config.edit_log_roll_num);
            rollEditLog();
            txId = 0;
        }
        if (MetricRepo.isInit) {
            MetricRepo.COUNTER_EDIT_LOG_WRITE.increase(Long.valueOf(batch.size()));
        }
    }

    public long getMaxJournalId() {
        return journal.getMaxJournalId();
    }

    public long getMinJournalId() {
        return journal.getMinJournalId();
    }

    public JournalCursor read(long fromId, long toId) {
        return journal.read(fromId, toId);
    }

    public long getFinalizedJournalId() {
        return journal.getFinalizedJournalId();
    }

    public void deleteJournals(long deleteToJournalId) {
        journal.deleteJournals(deleteToJournalId);
    }

    public List<Long> getDatabaseNames() {
        return journal.getDatabaseNames();
    }

    public synchronized int getNumEditStreams() {
        return journal == null ? 0 : 1;
    }

    /**
     * Load journal.
     **/
    public static void loadJournal(Env env, Long logId, JournalEntity journal) {
        short opCode = journal.getOpCode();
        if (opCode != OperationType.OP_SAVE_NEXTID && opCode != OperationType.OP_TIMESTAMP) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("replay journal op code: {}, log id: {}", opCode, logId);
            }
        }
        try {
            switch (opCode) {
                case OperationType.OP_SAVE_NEXTID: {
                    String idString = ((Text) journal.getData()).toString();
                    long id = Long.parseLong(idString);
                    env.setNextId(id + 1);
                    break;
                }
                case OperationType.OP_SAVE_TRANSACTION_ID: {
                    String idString = ((Text) journal.getData()).toString();
                    long id = Long.parseLong(idString);
                    Env.getCurrentGlobalTransactionMgr().getTransactionIDGenerator().initTransactionId(id + 1);
                    break;
                }
                case OperationType.OP_CREATE_DB: {
                    Database db = (Database) journal.getData();
                    CreateDbInfo info = new CreateDbInfo(db.getCatalog().getName(), db.getName(), db);
                    env.replayCreateDb(info);
                    break;
                }
                case OperationType.OP_NEW_CREATE_DB: {
                    CreateDbInfo info = (CreateDbInfo) journal.getData();
                    env.replayCreateDb(info);
                    break;
                }
                case OperationType.OP_DROP_DB: {
                    DropDbInfo dropDbInfo = (DropDbInfo) journal.getData();
                    env.replayDropDb(dropDbInfo);
                    break;
                }
                case OperationType.OP_ALTER_DB: {
                    DatabaseInfo dbInfo = (DatabaseInfo) journal.getData();
                    String dbName = dbInfo.getDbName();
                    LOG.info("Begin to unprotect alter db info {}", dbName);
                    env.replayAlterDatabaseQuota(dbName, dbInfo.getQuota(), dbInfo.getQuotaType());
                    break;
                }
                case OperationType.OP_ERASE_DB: {
                    Text dbId = (Text) journal.getData();
                    env.replayEraseDatabase(Long.parseLong(dbId.toString()));
                    break;
                }
                case OperationType.OP_RECOVER_DB: {
                    RecoverInfo info = (RecoverInfo) journal.getData();
                    env.replayRecoverDatabase(info);
                    break;
                }
                case OperationType.OP_RENAME_DB: {
                    DatabaseInfo dbInfo = (DatabaseInfo) journal.getData();
                    String dbName = dbInfo.getDbName();
                    LOG.info("Begin to unprotect rename db {}", dbName);
                    env.replayRenameDatabase(dbName, dbInfo.getNewDbName());
                    break;
                }
                case OperationType.OP_CREATE_TABLE: {
                    CreateTableInfo info = (CreateTableInfo) journal.getData();
                    LOG.info("Begin to unprotect create table. {}", info);
                    env.replayCreateTable(info);
                    if (Strings.isNullOrEmpty(info.getCtlName()) || info.getCtlName().equals(
                            InternalCatalog.INTERNAL_CATALOG_NAME)) {
                        CreateTableRecord record = new CreateTableRecord(logId, info);
                        env.getBinlogManager().addCreateTableRecord(record);
                    }
                    break;
                }
                case OperationType.OP_ALTER_EXTERNAL_TABLE_SCHEMA: {
                    RefreshExternalTableInfo info = (RefreshExternalTableInfo) journal.getData();
                    LOG.info("Begin to unprotect alter external table schema. db = {} table = {}", info.getDbName(),
                            info.getTableName());
                    env.replayAlterExternalTableSchema(info.getDbName(), info.getTableName(), info.getNewSchema());
                    break;
                }
                case OperationType.OP_DROP_TABLE: {
                    DropInfo info = (DropInfo) journal.getData();
                    LOG.info("Begin to unprotect drop table: {}", info);
                    if (Strings.isNullOrEmpty(info.getCtl()) || info.getCtl().equals(
                            InternalCatalog.INTERNAL_CATALOG_NAME)) {
                        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(info.getDbId());
                        env.replayDropTable(db, info.getTableId(), info.isForceDrop(), info.getRecycleTime());
                        DropTableRecord record = new DropTableRecord(logId, info);
                        env.getBinlogManager().addDropTableRecord(record);
                    } else {
                        ExternalCatalog ctl = (ExternalCatalog) Env.getCurrentEnv().getCatalogMgr()
                                .getCatalog(info.getCtl());
                        if (ctl != null) {
                            ctl.replayDropTable(info.getDb(), info.getTableName());
                        }
                    }
                    break;
                }
                case OperationType.OP_ADD_PARTITION: {
                    PartitionPersistInfo info = (PartitionPersistInfo) journal.getData();
                    LOG.info(
                            "Begin to unprotect add partition. db = " + info.getDbId() + " table = " + info.getTableId()
                                    + " partitionName = " + info.getPartition().getName());
                    AddPartitionRecord addPartitionRecord = new AddPartitionRecord(logId, info);
                    env.replayAddPartition(info);
                    env.getBinlogManager().addAddPartitionRecord(addPartitionRecord);
                    break;
                }
                case OperationType.OP_DROP_PARTITION: {
                    DropPartitionInfo info = (DropPartitionInfo) journal.getData();
                    LOG.info("Begin to unprotect drop partition. db = " + info.getDbId() + " table = "
                            + info.getTableId() + " partitionName = " + info.getPartitionName());
                    env.replayDropPartition(info);
                    env.getBinlogManager().addDropPartitionRecord(info, logId);
                    break;
                }
                case OperationType.OP_MODIFY_PARTITION: {
                    ModifyPartitionInfo info = (ModifyPartitionInfo) journal.getData();
                    LOG.info("Begin to unprotect modify partition. db = " + info.getDbId() + " table = "
                            + info.getTableId() + " partitionId = " + info.getPartitionId());
                    BatchModifyPartitionsInfo infos = new BatchModifyPartitionsInfo(info);
                    env.getAlterInstance().replayModifyPartition(info);
                    env.getBinlogManager().addModifyPartitions(infos, logId);
                    break;
                }
                case OperationType.OP_BATCH_MODIFY_PARTITION: {
                    BatchModifyPartitionsInfo info = (BatchModifyPartitionsInfo) journal.getData();
                    for (ModifyPartitionInfo modifyPartitionInfo : info.getModifyPartitionInfos()) {
                        env.getAlterInstance().replayModifyPartition(modifyPartitionInfo);
                    }
                    env.getBinlogManager().addModifyPartitions(info, logId);
                    break;
                }
                case OperationType.OP_ERASE_TABLE: {
                    Text tableId = (Text) journal.getData();
                    env.replayEraseTable(Long.parseLong(tableId.toString()));
                    break;
                }
                case OperationType.OP_ERASE_PARTITION: {
                    Text partitionId = (Text) journal.getData();
                    env.replayErasePartition(Long.parseLong(partitionId.toString()));
                    break;
                }
                case OperationType.OP_RECOVER_TABLE: {
                    RecoverInfo info = (RecoverInfo) journal.getData();
                    env.replayRecoverTable(info);
                    env.getBinlogManager().addRecoverTableRecord(info, logId);
                    break;
                }
                case OperationType.OP_RECOVER_PARTITION: {
                    RecoverInfo info = (RecoverInfo) journal.getData();
                    env.replayRecoverPartition(info);
                    env.getBinlogManager().addRecoverTableRecord(info, logId);
                    break;
                }
                case OperationType.OP_RENAME_TABLE: {
                    TableInfo info = (TableInfo) journal.getData();
                    env.replayRenameTable(info);
                    env.getBinlogManager().addTableRename(info, logId);
                    break;
                }
                case OperationType.OP_MODIFY_VIEW_DEF: {
                    AlterViewInfo info = (AlterViewInfo) journal.getData();
                    env.getAlterInstance().replayModifyViewDef(info);
                    env.getBinlogManager().addModifyViewDef(info, logId);
                    break;
                }
                case OperationType.OP_RENAME_PARTITION: {
                    TableInfo info = (TableInfo) journal.getData();
                    env.replayRenamePartition(info);
                    env.getBinlogManager().addPartitionRename(info, logId);
                    break;
                }
                case OperationType.OP_RENAME_COLUMN: {
                    TableRenameColumnInfo info = (TableRenameColumnInfo) journal.getData();
                    env.replayRenameColumn(info);
                    env.getBinlogManager().addColumnRename(info, logId);
                    break;
                }
                case OperationType.OP_BACKUP_JOB: {
                    BackupJob job = (BackupJob) journal.getData();
                    env.getBackupHandler().replayAddJob(job);
                    break;
                }
                case OperationType.OP_RESTORE_JOB: {
                    RestoreJob job = (RestoreJob) journal.getData();
                    job.setEnv(env);
                    env.getBackupHandler().replayAddJob(job);
                    break;
                }
                case OperationType.OP_DROP_ROLLUP: {
                    DropInfo info = (DropInfo) journal.getData();
                    env.getMaterializedViewHandler().replayDropRollup(info, env);
                    env.getBinlogManager().addDropRollup(info, logId);
                    break;
                }
                case OperationType.OP_BATCH_DROP_ROLLUP: {
                    BatchDropInfo batchDropInfo = (BatchDropInfo) journal.getData();
                    if (batchDropInfo.hasIndexNameMap()) {
                        for (Map.Entry<Long, String> entry : batchDropInfo.getIndexNameMap().entrySet()) {
                            long indexId = entry.getKey();
                            String indexName = entry.getValue();
                            DropInfo info = new DropInfo(batchDropInfo.getDbId(), batchDropInfo.getTableId(),
                                            batchDropInfo.getTableName(), indexId, indexName, false, false, 0);
                            env.getMaterializedViewHandler().replayDropRollup(info, env);
                            env.getBinlogManager().addDropRollup(info, logId);
                        }
                    } else {
                        for (Long indexId : batchDropInfo.getIndexIdSet()) {
                            DropInfo info = new DropInfo(batchDropInfo.getDbId(), batchDropInfo.getTableId(),
                                    batchDropInfo.getTableName(), indexId, "", false, false, 0);
                            env.getMaterializedViewHandler().replayDropRollup(info, env);
                        }
                    }
                    break;
                }
                case OperationType.OP_FINISH_CONSISTENCY_CHECK: {
                    ConsistencyCheckInfo info = (ConsistencyCheckInfo) journal.getData();
                    env.getConsistencyChecker().replayFinishConsistencyCheck(info, env);
                    break;
                }
                case OperationType.OP_RENAME_ROLLUP: {
                    TableInfo info = (TableInfo) journal.getData();
                    env.replayRenameRollup(info);
                    env.getBinlogManager().addRollupRename(info, logId);
                    break;
                }
                case OperationType.OP_LOAD_START:
                case OperationType.OP_LOAD_ETL:
                case OperationType.OP_LOAD_LOADING:
                case OperationType.OP_LOAD_QUORUM:
                case OperationType.OP_LOAD_DONE:
                case OperationType.OP_LOAD_CANCEL: {
                    LOG.warn("load job is deprecated");
                    break;
                }
                case OperationType.OP_EXPORT_CREATE: {
                    ExportJob job = (ExportJob) journal.getData();
                    ExportMgr exportMgr = env.getExportMgr();
                    job.cancelReplayedExportJob();
                    exportMgr.replayCreateExportJob(job);
                    break;
                }
                case OperationType.OP_EXPORT_UPDATE_STATE: {
                    ExportJobStateTransfer op = (ExportJobStateTransfer) journal.getData();
                    ExportMgr exportMgr = env.getExportMgr();
                    exportMgr.replayUpdateJobState(op);
                    break;
                }
                case OperationType.OP_FINISH_DELETE: {
                    DeleteInfo info = (DeleteInfo) journal.getData();
                    DeleteHandler deleteHandler = env.getDeleteHandler();
                    deleteHandler.replayDelete(info, env);
                    break;
                }
                case OperationType.OP_ADD_REPLICA: {
                    ReplicaPersistInfo info = (ReplicaPersistInfo) journal.getData();
                    env.replayAddReplica(info);
                    break;
                }
                case OperationType.OP_UPDATE_REPLICA: {
                    ReplicaPersistInfo info = (ReplicaPersistInfo) journal.getData();
                    env.replayUpdateReplica(info);
                    break;
                }
                case OperationType.OP_MODIFY_CLOUD_WARM_UP_JOB: {
                    CloudWarmUpJob cloudWarmUpJob = (CloudWarmUpJob) journal.getData();
                    ((CloudEnv) env).getCacheHotspotMgr().replayCloudWarmUpJob(cloudWarmUpJob);
                    break;
                }
                case OperationType.OP_DELETE_REPLICA: {
                    ReplicaPersistInfo info = (ReplicaPersistInfo) journal.getData();
                    env.replayDeleteReplica(info);
                    break;
                }
                case OperationType.OP_ADD_BACKEND: {
                    Backend be = (Backend) journal.getData();
                    Env.getCurrentSystemInfo().replayAddBackend(be);
                    break;
                }
                case OperationType.OP_DROP_BACKEND: {
                    Backend be = (Backend) journal.getData();
                    Env.getCurrentSystemInfo().replayDropBackend(be);
                    break;
                }
                case OperationType.OP_MODIFY_BACKEND: {
                    Backend be = (Backend) journal.getData();
                    Env.getCurrentSystemInfo().replayModifyBackend(be);
                    break;
                }
                case OperationType.OP_BACKEND_STATE_CHANGE: {
                    Backend be = (Backend) journal.getData();
                    Env.getCurrentSystemInfo().updateBackendState(be);
                    break;
                }
                case OperationType.OP_ADD_FIRST_FRONTEND:
                case OperationType.OP_ADD_FRONTEND: {
                    Frontend fe = (Frontend) journal.getData();
                    env.replayAddFrontend(fe);
                    break;
                }
                case OperationType.OP_MODIFY_FRONTEND: {
                    Frontend fe = (Frontend) journal.getData();
                    env.replayModifyFrontend(fe);
                    break;
                }
                case OperationType.OP_REMOVE_FRONTEND: {
                    Frontend fe = (Frontend) journal.getData();
                    env.replayDropFrontend(fe);
                    if (fe.getNodeName().equals(Env.getCurrentEnv().getNodeName())) {
                        LOG.warn("current fe {} is removed. will exit", fe);
                        System.exit(-1);
                    }
                    break;
                }
                case OperationType.OP_CREATE_USER: {
                    PrivInfo privInfo = (PrivInfo) journal.getData();
                    env.getAuth().replayCreateUser(privInfo);
                    break;
                }
                case OperationType.OP_NEW_DROP_USER: {
                    UserIdentity userIdent = (UserIdentity) journal.getData();
                    env.getAuth().replayDropUser(userIdent);
                    break;
                }
                case OperationType.OP_GRANT_PRIV: {
                    PrivInfo privInfo = (PrivInfo) journal.getData();
                    env.getAuth().replayGrant(privInfo);
                    break;
                }
                case OperationType.OP_REVOKE_PRIV: {
                    PrivInfo privInfo = (PrivInfo) journal.getData();
                    env.getAuth().replayRevoke(privInfo);
                    break;
                }
                case OperationType.OP_SET_PASSWORD: {
                    PrivInfo privInfo = (PrivInfo) journal.getData();
                    env.getAuth().replaySetPassword(privInfo);
                    break;
                }
                case OperationType.OP_SET_LDAP_PASSWORD: {
                    LdapInfo ldapInfo = (LdapInfo) journal.getData();
                    env.getAuth().replaySetLdapPassword(ldapInfo);
                    break;
                }
                case OperationType.OP_CREATE_ROLE: {
                    PrivInfo privInfo = (PrivInfo) journal.getData();
                    env.getAuth().replayCreateRole(privInfo);
                    break;
                }
                case OperationType.OP_ALTER_ROLE: {
                    PrivInfo privInfo = (PrivInfo) journal.getData();
                    env.getAuth().replayAlterRole(privInfo);
                    break;
                }
                case OperationType.OP_DROP_ROLE: {
                    PrivInfo privInfo = (PrivInfo) journal.getData();
                    env.getAuth().replayDropRole(privInfo);
                    break;
                }
                case OperationType.OP_UPDATE_USER_PROPERTY: {
                    UserPropertyInfo propertyInfo = (UserPropertyInfo) journal.getData();
                    env.getAuth().replayUpdateUserProperty(propertyInfo);
                    break;
                }
                case OperationType.OP_TIMESTAMP: {
                    Timestamp stamp = (Timestamp) journal.getData();
                    env.setSynchronizedTime(stamp.getTimestamp());
                    break;
                }
                case OperationType.OP_MASTER_INFO_CHANGE: {
                    MasterInfo info = (MasterInfo) journal.getData();
                    env.setMaster(info);
                    break;
                }
                case OperationType.OP_META_VERSION: {
                    String versionString = ((Text) journal.getData()).toString();
                    int version = Integer.parseInt(versionString);
                    if (version > FeConstants.meta_version) {
                        LOG.error("meta data version is out of date, image: {}. meta: {}."
                                        + "please update FeConstants.meta_version and restart.", version,
                                FeConstants.meta_version);
                        System.exit(-1);
                    }
                    MetaContext.get().setMetaVersion(version);
                    break;
                }
                case OperationType.OP_CREATE_CLUSTER: {
                    // Do nothing
                    break;
                }
                case OperationType.OP_ADD_BROKER: {
                    final BrokerMgr.ModifyBrokerInfo param = (BrokerMgr.ModifyBrokerInfo) journal.getData();
                    env.getBrokerMgr().replayAddBrokers(param.brokerName, param.brokerAddresses);
                    break;
                }
                case OperationType.OP_DROP_BROKER: {
                    final BrokerMgr.ModifyBrokerInfo param = (BrokerMgr.ModifyBrokerInfo) journal.getData();
                    env.getBrokerMgr().replayDropBrokers(param.brokerName, param.brokerAddresses);
                    break;
                }
                case OperationType.OP_DROP_ALL_BROKER: {
                    final String param = journal.getData().toString();
                    env.getBrokerMgr().replayDropAllBroker(param);
                    break;
                }
                case OperationType.OP_SET_LOAD_ERROR_HUB: {
                    // final LoadErrorHub.Param param = (LoadErrorHub.Param) journal.getData();
                    // ignore load error hub
                    break;
                }
                case OperationType.OP_UPSERT_TRANSACTION_STATE: {
                    final TransactionState state = (TransactionState) journal.getData();
                    Env.getCurrentGlobalTransactionMgr().replayUpsertTransactionState(state);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("logid: {}, opcode: {}, tid: {}", logId, opCode, state.getTransactionId());
                    }

                    // state.loadedTableIndexIds is updated after replay
                    if (state.getTransactionStatus() == TransactionStatus.VISIBLE) {
                        UpsertRecord upsertRecord = new UpsertRecord(logId, state);
                        Env.getCurrentEnv().getBinlogManager().addUpsertRecord(upsertRecord);
                    }
                    break;
                }
                case OperationType.OP_DELETE_TRANSACTION_STATE: {
                    final TransactionState state = (TransactionState) journal.getData();
                    Env.getCurrentGlobalTransactionMgr().replayDeleteTransactionState(state);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("opcode: {}, tid: {}", opCode, state.getTransactionId());
                    }
                    break;
                }
                case OperationType.OP_BATCH_REMOVE_TXNS: {
                    final BatchRemoveTransactionsOperation operation =
                            (BatchRemoveTransactionsOperation) journal.getData();
                    Env.getCurrentGlobalTransactionMgr().replayBatchRemoveTransactions(operation);
                    break;
                }
                case OperationType.OP_BATCH_REMOVE_TXNS_V2: {
                    final BatchRemoveTransactionsOperationV2 operation =
                            (BatchRemoveTransactionsOperationV2) journal.getData();
                    Env.getCurrentGlobalTransactionMgr().replayBatchRemoveTransactionV2(operation);
                    break;
                }
                case OperationType.OP_SET_TABLE_STATUS: {
                    final SetTableStatusOperationLog log = (SetTableStatusOperationLog) journal.getData();
                    env.replaySetTableStatus(log);
                    break;
                }
                case OperationType.OP_CREATE_REPOSITORY: {
                    Repository repository = (Repository) journal.getData();
                    env.getBackupHandler().getRepoMgr().addAndInitRepoIfNotExist(repository, true);
                    break;
                }
                case OperationType.OP_DROP_REPOSITORY: {
                    String repoName = ((Text) journal.getData()).toString();
                    env.getBackupHandler().getRepoMgr().removeRepo(repoName, true);
                    break;
                }
                case OperationType.OP_TRUNCATE_TABLE: {
                    TruncateTableInfo info = (TruncateTableInfo) journal.getData();
                    env.replayTruncateTable(info);
                    env.getBinlogManager().addTruncateTable(info, logId);
                    break;
                }
                case OperationType.OP_COLOCATE_ADD_TABLE: {
                    final ColocatePersistInfo info = (ColocatePersistInfo) journal.getData();
                    env.getColocateTableIndex().replayAddTableToGroup(info);
                    break;
                }
                case OperationType.OP_COLOCATE_REMOVE_TABLE: {
                    final ColocatePersistInfo info = (ColocatePersistInfo) journal.getData();
                    env.getColocateTableIndex().replayRemoveTable(info);
                    break;
                }
                case OperationType.OP_COLOCATE_BACKENDS_PER_BUCKETSEQ: {
                    final ColocatePersistInfo info = (ColocatePersistInfo) journal.getData();
                    env.getColocateTableIndex().replayAddBackendsPerBucketSeq(info);
                    break;
                }
                case OperationType.OP_COLOCATE_MARK_UNSTABLE: {
                    final ColocatePersistInfo info = (ColocatePersistInfo) journal.getData();
                    env.getColocateTableIndex().replayMarkGroupUnstable(info);
                    break;
                }
                case OperationType.OP_COLOCATE_MARK_STABLE: {
                    final ColocatePersistInfo info = (ColocatePersistInfo) journal.getData();
                    env.getColocateTableIndex().replayMarkGroupStable(info);
                    break;
                }
                case OperationType.OP_COLOCATE_MOD_REPLICA_ALLOC: {
                    final ColocatePersistInfo info = (ColocatePersistInfo) journal.getData();
                    env.getColocateTableIndex().replayModifyReplicaAlloc(info);
                    break;
                }
                case OperationType.OP_MODIFY_TABLE_COLOCATE: {
                    final TablePropertyInfo info = (TablePropertyInfo) journal.getData();
                    env.replayModifyTableColocate(info);
                    break;
                }
                case OperationType.OP_HEARTBEAT: {
                    final HbPackage hbPackage = (HbPackage) journal.getData();
                    Env.getCurrentHeartbeatMgr().replayHearbeat(hbPackage);
                    break;
                }
                case OperationType.OP_ADD_FUNCTION: {
                    final Function function = (Function) journal.getData();
                    Env.getCurrentEnv().replayCreateFunction(function);
                    break;
                }
                case OperationType.OP_DROP_FUNCTION: {
                    FunctionSearchDesc function = (FunctionSearchDesc) journal.getData();
                    Env.getCurrentEnv().replayDropFunction(function);
                    break;
                }
                case OperationType.OP_ADD_GLOBAL_FUNCTION: {
                    final Function function = (Function) journal.getData();
                    Env.getCurrentEnv().replayCreateGlobalFunction(function);
                    break;
                }
                case OperationType.OP_DROP_GLOBAL_FUNCTION: {
                    FunctionSearchDesc function = (FunctionSearchDesc) journal.getData();
                    Env.getCurrentEnv().replayDropGlobalFunction(function);
                    break;
                }
                case OperationType.OP_CREATE_ENCRYPTKEY: {
                    final EncryptKey encryptKey = (EncryptKey) journal.getData();
                    EncryptKeyHelper.replayCreateEncryptKey(encryptKey);
                    break;
                }
                case OperationType.OP_DROP_ENCRYPTKEY: {
                    EncryptKeySearchDesc encryptKeySearchDesc = (EncryptKeySearchDesc) journal.getData();
                    EncryptKeyHelper.replayDropEncryptKey(encryptKeySearchDesc);
                    break;
                }
                case OperationType.OP_BACKEND_TABLETS_INFO: {
                    BackendTabletsInfo backendTabletsInfo = (BackendTabletsInfo) journal.getData();
                    Env.getCurrentEnv().replayBackendTabletsInfo(backendTabletsInfo);
                    break;
                }
                case OperationType.OP_BACKEND_REPLICAS_INFO: {
                    BackendReplicasInfo backendReplicasInfo = (BackendReplicasInfo) journal.getData();
                    Env.getCurrentEnv().replayBackendReplicasInfo(backendReplicasInfo);
                    break;
                }
                case OperationType.OP_CREATE_ROUTINE_LOAD_JOB: {
                    RoutineLoadJob routineLoadJob = (RoutineLoadJob) journal.getData();
                    Env.getCurrentEnv().getRoutineLoadManager().replayCreateRoutineLoadJob(routineLoadJob);
                    break;
                }
                case OperationType.OP_CREATE_SCHEDULER_JOB: {
                    AbstractJob job = (AbstractJob) journal.getData();
                    Env.getCurrentEnv().getJobManager().replayCreateJob(job);
                    break;
                }
                case OperationType.OP_UPDATE_SCHEDULER_JOB: {
                    AbstractJob job = (AbstractJob) journal.getData();
                    Env.getCurrentEnv().getJobManager().replayUpdateJob(job);
                    break;
                }
                case OperationType.OP_DELETE_SCHEDULER_JOB: {
                    AbstractJob job = (AbstractJob) journal.getData();
                    Env.getCurrentEnv().getJobManager().replayDeleteJob(job);
                    break;
                }
                /*case OperationType.OP_CREATE_SCHEDULER_TASK: {
                    JobTask task = (JobTask) journal.getData();
                    Env.getCurrentEnv().getJobTaskManager().replayCreateTask(task);
                    break;
                }
                case OperationType.OP_DELETE_SCHEDULER_TASK: {
                    JobTask task = (JobTask) journal.getData();
                    Env.getCurrentEnv().getJobTaskManager().replayDeleteTask(task);
                    break;
                }*/
                case OperationType.OP_CHANGE_ROUTINE_LOAD_JOB: {
                    RoutineLoadOperation operation = (RoutineLoadOperation) journal.getData();
                    Env.getCurrentEnv().getRoutineLoadManager().replayChangeRoutineLoadJob(operation);
                    break;
                }
                case OperationType.OP_REMOVE_ROUTINE_LOAD_JOB: {
                    RoutineLoadOperation operation = (RoutineLoadOperation) journal.getData();
                    env.getRoutineLoadManager().replayRemoveOldRoutineLoad(operation);
                    break;
                }
                case OperationType.OP_CREATE_LOAD_JOB: {
                    org.apache.doris.load.loadv2.LoadJob loadJob =
                            (org.apache.doris.load.loadv2.LoadJob) journal.getData();
                    env.getLoadManager().replayCreateLoadJob(loadJob);
                    break;
                }
                case OperationType.OP_END_LOAD_JOB: {
                    LoadJobFinalOperation operation = (LoadJobFinalOperation) journal.getData();
                    env.getLoadManager().replayEndLoadJob(operation);
                    break;
                }
                case OperationType.OP_UPDATE_LOAD_JOB: {
                    LoadJobStateUpdateInfo info = (LoadJobStateUpdateInfo) journal.getData();
                    env.getLoadManager().replayUpdateLoadJobStateInfo(info);
                    break;
                }
                case OperationType.OP_CREATE_SYNC_JOB: {
                    break;
                }
                case OperationType.OP_UPDATE_SYNC_JOB_STATE: {
                    break;
                }
                case OperationType.OP_FETCH_STREAM_LOAD_RECORD: {
                    FetchStreamLoadRecord fetchStreamLoadRecord = (FetchStreamLoadRecord) journal.getData();
                    env.getStreamLoadRecordMgr().replayFetchStreamLoadRecord(fetchStreamLoadRecord);
                    break;
                }
                case OperationType.OP_CREATE_RESOURCE: {
                    final Resource resource = (Resource) journal.getData();
                    env.getResourceMgr().replayCreateResource(resource);
                    break;
                }
                case OperationType.OP_DROP_RESOURCE: {
                    final DropResourceOperationLog operationLog = (DropResourceOperationLog) journal.getData();
                    env.getResourceMgr().replayDropResource(operationLog);
                    break;
                }
                case OperationType.OP_ALTER_RESOURCE: {
                    final Resource resource = (Resource) journal.getData();
                    env.getResourceMgr().replayAlterResource(resource);
                    break;
                }
                case OperationType.OP_CREATE_SMALL_FILE: {
                    SmallFile smallFile = (SmallFile) journal.getData();
                    env.getSmallFileMgr().replayCreateFile(smallFile);
                    break;
                }
                case OperationType.OP_DROP_SMALL_FILE: {
                    SmallFile smallFile = (SmallFile) journal.getData();
                    env.getSmallFileMgr().replayRemoveFile(smallFile);
                    break;
                }
                case OperationType.OP_ALTER_JOB_V2: {
                    AlterJobV2 alterJob = (AlterJobV2) journal.getData();
                    switch (alterJob.getType()) {
                        case ROLLUP:
                            env.getMaterializedViewHandler().replayAlterJobV2(alterJob);
                            break;
                        case SCHEMA_CHANGE:
                            env.getSchemaChangeHandler().replayAlterJobV2(alterJob);
                            if (alterJob.getJobState().equals(JobState.FINISHED)) {
                                AnalysisManager manager = Env.getCurrentEnv().getAnalysisManager();
                                manager.removeTableStats(alterJob.getTableId());
                            }
                            break;
                        default:
                            break;
                    }
                    env.getBinlogManager().addAlterJobV2(alterJob, logId);
                    break;
                }
                case OperationType.OP_UPDATE_COOLDOWN_CONF:
                    CooldownConfList cooldownConfList = (CooldownConfList) journal.getData();
                    CooldownConfHandler.replayUpdateCooldownConf(cooldownConfList);
                    break;
                case OperationType.OP_COOLDOWN_DELETE:
                    // noop
                    break;
                case OperationType.OP_BATCH_ADD_ROLLUP: {
                    BatchAlterJobPersistInfo batchAlterJobV2 = (BatchAlterJobPersistInfo) journal.getData();
                    for (AlterJobV2 alterJobV2 : batchAlterJobV2.getAlterJobV2List()) {
                        env.getMaterializedViewHandler().replayAlterJobV2(alterJobV2);
                    }
                    break;
                }
                case OperationType.OP_MODIFY_DISTRIBUTION_TYPE: {
                    TableInfo tableInfo = (TableInfo) journal.getData();
                    env.replayConvertDistributionType(tableInfo);
                    env.getBinlogManager().addModifyDistributionType(tableInfo, logId);
                    break;
                }
                case OperationType.OP_DYNAMIC_PARTITION:
                case OperationType.OP_MODIFY_TABLE_PROPERTIES:
                case OperationType.OP_UPDATE_BINLOG_CONFIG:
                case OperationType.OP_MODIFY_REPLICATION_NUM: {
                    ModifyTablePropertyOperationLog log = (ModifyTablePropertyOperationLog) journal.getData();
                    env.replayModifyTableProperty(opCode, log);
                    env.getBinlogManager().addModifyTableProperty(log, logId);
                    break;
                }
                case OperationType.OP_MODIFY_DISTRIBUTION_BUCKET_NUM: {
                    ModifyTableDefaultDistributionBucketNumOperationLog log =
                            (ModifyTableDefaultDistributionBucketNumOperationLog) journal.getData();
                    env.replayModifyTableDefaultDistributionBucketNum(log);
                    env.getBinlogManager().addModifyDistributionNum(log, logId);
                    break;
                }
                case OperationType.OP_REPLACE_TEMP_PARTITION: {
                    ReplacePartitionOperationLog replaceTempPartitionLog =
                            (ReplacePartitionOperationLog) journal.getData();
                    env.replayReplaceTempPartition(replaceTempPartitionLog);
                    env.getBinlogManager().addReplacePartitions(replaceTempPartitionLog, logId);
                    break;
                }
                case OperationType.OP_INSTALL_PLUGIN: {
                    PluginInfo pluginInfo = (PluginInfo) journal.getData();
                    env.replayInstallPlugin(pluginInfo);
                    break;
                }
                case OperationType.OP_UNINSTALL_PLUGIN: {
                    PluginInfo pluginInfo = (PluginInfo) journal.getData();
                    env.replayUninstallPlugin(pluginInfo);
                    break;
                }
                case OperationType.OP_SET_REPLICA_STATUS: {
                    SetReplicaStatusOperationLog log = (SetReplicaStatusOperationLog) journal.getData();
                    env.replaySetReplicaStatus(log);
                    break;
                }
                case OperationType.OP_SET_REPLICA_VERSION: {
                    SetReplicaVersionOperationLog log = (SetReplicaVersionOperationLog) journal.getData();
                    env.replaySetReplicaVersion(log);
                    break;
                }
                case OperationType.OP_REMOVE_ALTER_JOB_V2: {
                    RemoveAlterJobV2OperationLog log = (RemoveAlterJobV2OperationLog) journal.getData();
                    switch (log.getType()) {
                        case ROLLUP:
                            env.getMaterializedViewHandler().replayRemoveAlterJobV2(log);
                            break;
                        case SCHEMA_CHANGE:
                            env.getSchemaChangeHandler().replayRemoveAlterJobV2(log);
                            break;
                        default:
                            break;
                    }
                    break;
                }
                case OperationType.OP_MODIFY_COMMENT: {
                    ModifyCommentOperationLog operation = (ModifyCommentOperationLog) journal.getData();
                    env.getAlterInstance().replayModifyComment(operation);
                    env.getBinlogManager().addModifyComment(operation, logId);
                    break;
                }
                case OperationType.OP_SET_PARTITION_VERSION: {
                    SetPartitionVersionOperationLog log = (SetPartitionVersionOperationLog) journal.getData();
                    env.replaySetPartitionVersion(log);
                    break;
                }
                case OperationType.OP_ALTER_ROUTINE_LOAD_JOB: {
                    AlterRoutineLoadJobOperationLog log = (AlterRoutineLoadJobOperationLog) journal.getData();
                    env.getRoutineLoadManager().replayAlterRoutineLoadJob(log);
                    break;
                }
                case OperationType.OP_GLOBAL_VARIABLE_V2: {
                    GlobalVarPersistInfo info = (GlobalVarPersistInfo) journal.getData();
                    env.replayGlobalVariableV2(info);
                    break;
                }
                case OperationType.OP_REPLACE_TABLE: {
                    ReplaceTableOperationLog log = (ReplaceTableOperationLog) journal.getData();
                    env.getAlterInstance().replayReplaceTable(log);
                    env.getBinlogManager().addReplaceTable(log, logId);
                    break;
                }
                case OperationType.OP_CREATE_SQL_BLOCK_RULE: {
                    SqlBlockRule rule = (SqlBlockRule) journal.getData();
                    env.getSqlBlockRuleMgr().replayCreate(rule);
                    break;
                }
                case OperationType.OP_ALTER_SQL_BLOCK_RULE: {
                    SqlBlockRule rule = (SqlBlockRule) journal.getData();
                    env.getSqlBlockRuleMgr().replayAlter(rule);
                    break;
                }
                case OperationType.OP_DROP_SQL_BLOCK_RULE: {
                    DropSqlBlockRuleOperationLog log = (DropSqlBlockRuleOperationLog) journal.getData();
                    env.getSqlBlockRuleMgr().replayDrop(log.getRuleNames());
                    break;
                }
                case OperationType.OP_MODIFY_TABLE_ENGINE: {
                    ModifyTableEngineOperationLog log = (ModifyTableEngineOperationLog) journal.getData();
                    env.getAlterInstance().replayProcessModifyEngine(log);
                    break;
                }
                case OperationType.OP_CREATE_POLICY: {
                    Policy log = (Policy) journal.getData();
                    env.getPolicyMgr().replayCreate(log);
                    break;
                }
                case OperationType.OP_DROP_POLICY: {
                    DropPolicyLog log = (DropPolicyLog) journal.getData();
                    env.getPolicyMgr().replayDrop(log);
                    break;
                }
                case OperationType.OP_ALTER_STORAGE_POLICY: {
                    StoragePolicy log = (StoragePolicy) journal.getData();
                    env.getPolicyMgr().replayStoragePolicyAlter(log);
                    break;
                }
                case OperationType.OP_CREATE_CATALOG: {
                    CatalogLog log = (CatalogLog) journal.getData();
                    env.getCatalogMgr().replayCreateCatalog(log);
                    break;
                }
                case OperationType.OP_DROP_CATALOG: {
                    CatalogLog log = (CatalogLog) journal.getData();
                    env.getCatalogMgr().replayDropCatalog(log);
                    break;
                }
                case OperationType.OP_ALTER_CATALOG_NAME: {
                    CatalogLog log = (CatalogLog) journal.getData();
                    env.getCatalogMgr().replayAlterCatalogName(log);
                    break;
                }
                case OperationType.OP_ALTER_CATALOG_COMMENT: {
                    CatalogLog log = (CatalogLog) journal.getData();
                    env.getCatalogMgr().replayAlterCatalogComment(log);
                    break;
                }
                case OperationType.OP_ALTER_CATALOG_PROPS: {
                    CatalogLog log = (CatalogLog) journal.getData();
                    env.getCatalogMgr().replayAlterCatalogProps(log, null, true);
                    break;
                }
                case OperationType.OP_REFRESH_CATALOG: {
                    CatalogLog log = (CatalogLog) journal.getData();
                    env.getRefreshManager().replayRefreshCatalog(log);
                    break;
                }
                case OperationType.OP_MODIFY_TABLE_LIGHT_SCHEMA_CHANGE: {
                    final TableAddOrDropColumnsInfo info = (TableAddOrDropColumnsInfo) journal.getData();
                    env.getSchemaChangeHandler().replayModifyTableLightSchemaChange(info);
                    env.getBinlogManager().addModifyTableAddOrDropColumns(info, logId);
                    break;
                }
                case OperationType.OP_ALTER_LIGHT_SCHEMA_CHANGE: {
                    final AlterLightSchemaChangeInfo info = (AlterLightSchemaChangeInfo) journal.getData();
                    env.getSchemaChangeHandler().replayAlterLightSchChange(info);
                    break;
                }
                case OperationType.OP_CLEAN_QUERY_STATS: {
                    final CleanQueryStatsInfo info = (CleanQueryStatsInfo) journal.getData();
                    env.getQueryStats().clear(info);
                    break;
                }
                case OperationType.OP_MODIFY_TABLE_ADD_OR_DROP_INVERTED_INDICES: {
                    final TableAddOrDropInvertedIndicesInfo info =
                            (TableAddOrDropInvertedIndicesInfo) journal.getData();
                    env.getSchemaChangeHandler().replayModifyTableAddOrDropInvertedIndices(info);
                    env.getBinlogManager().addModifyTableAddOrDropInvertedIndices(info, logId);
                    break;
                }
                case OperationType.OP_INVERTED_INDEX_JOB: {
                    IndexChangeJob indexChangeJob = (IndexChangeJob) journal.getData();
                    env.getSchemaChangeHandler().replayIndexChangeJob(indexChangeJob);
                    env.getBinlogManager().addIndexChangeJob(indexChangeJob, logId);
                    break;
                }
                case OperationType.OP_CLEAN_LABEL: {
                    final CleanLabelOperationLog log = (CleanLabelOperationLog) journal.getData();
                    env.getLoadManager().replayCleanLabel(log);
                    break;
                }
                case OperationType.OP_CREATE_MTMV_JOB:
                case OperationType.OP_CHANGE_MTMV_JOB:
                case OperationType.OP_DROP_MTMV_JOB:
                case OperationType.OP_CREATE_MTMV_TASK:
                case OperationType.OP_CHANGE_MTMV_TASK:
                case OperationType.OP_DROP_MTMV_TASK:
                case OperationType.OP_ALTER_MTMV_STMT: {
                    break;
                }
                case OperationType.OP_ADD_CONSTRAINT: {
                    final AlterConstraintLog log = (AlterConstraintLog) journal.getData();
                    try {
                        log.getTableIf().replayAddConstraint(log.getConstraint());
                    } catch (Exception e) {
                        LOG.error("Failed to replay add constraint", e);
                    }
                    break;
                }
                case OperationType.OP_DROP_CONSTRAINT: {
                    final AlterConstraintLog log = (AlterConstraintLog) journal.getData();
                    try {
                        log.getTableIf().replayDropConstraint(log.getConstraint().getName());
                    } catch (Exception e) {
                        LOG.error("Failed to replay drop constraint", e);
                    }
                    break;
                }
                case OperationType.OP_ALTER_USER: {
                    final AlterUserOperationLog log = (AlterUserOperationLog) journal.getData();
                    env.getAuth().replayAlterUser(log);
                    break;
                }
                case OperationType.OP_INIT_CATALOG:
                case OperationType.OP_INIT_CATALOG_COMP: {
                    final InitCatalogLog log = (InitCatalogLog) journal.getData();
                    env.getCatalogMgr().replayInitCatalog(log);
                    break;
                }
                case OperationType.OP_REFRESH_EXTERNAL_DB: {
                    final ExternalObjectLog log = (ExternalObjectLog) journal.getData();
                    env.getRefreshManager().replayRefreshDb(log);
                    break;
                }
                case OperationType.OP_INIT_EXTERNAL_DB: {
                    final InitDatabaseLog log = (InitDatabaseLog) journal.getData();
                    env.getCatalogMgr().replayInitExternalDb(log);
                    break;
                }
                case OperationType.OP_REFRESH_EXTERNAL_TABLE: {
                    final ExternalObjectLog log = (ExternalObjectLog) journal.getData();
                    env.getRefreshManager().replayRefreshTable(log);
                    break;
                }
                case OperationType.OP_DROP_EXTERNAL_TABLE: {
                    break;
                }
                case OperationType.OP_CREATE_EXTERNAL_TABLE: {
                    break;
                }
                case OperationType.OP_DROP_EXTERNAL_DB: {
                    break;
                }
                case OperationType.OP_CREATE_EXTERNAL_DB: {
                    break;
                }
                case OperationType.OP_ADD_EXTERNAL_PARTITIONS: {
                    break;
                }
                case OperationType.OP_DROP_EXTERNAL_PARTITIONS: {
                    break;
                }
                case OperationType.OP_REFRESH_EXTERNAL_PARTITIONS: {
                    break;
                }
                case OperationType.OP_CREATE_WORKLOAD_GROUP: {
                    final WorkloadGroup workloadGroup = (WorkloadGroup) journal.getData();
                    env.getWorkloadGroupMgr().replayCreateWorkloadGroup(workloadGroup);
                    break;
                }
                case OperationType.OP_DROP_WORKLOAD_GROUP: {
                    final DropWorkloadGroupOperationLog operationLog =
                            (DropWorkloadGroupOperationLog) journal.getData();
                    env.getWorkloadGroupMgr().replayDropWorkloadGroup(operationLog);
                    break;
                }
                case OperationType.OP_ALTER_WORKLOAD_GROUP: {
                    final WorkloadGroup resource = (WorkloadGroup) journal.getData();
                    env.getWorkloadGroupMgr().replayAlterWorkloadGroup(resource);
                    break;
                }
                case OperationType.OP_CREATE_WORKLOAD_SCHED_POLICY: {
                    final WorkloadSchedPolicy policy = (WorkloadSchedPolicy) journal.getData();
                    env.getWorkloadSchedPolicyMgr().replayCreateWorkloadSchedPolicy(policy);
                    break;
                }
                case OperationType.OP_ALTER_WORKLOAD_SCHED_POLICY: {
                    final WorkloadSchedPolicy policy = (WorkloadSchedPolicy) journal.getData();
                    env.getWorkloadSchedPolicyMgr().replayAlterWorkloadSchedPolicy(policy);
                    break;
                }
                case OperationType.OP_DROP_WORKLOAD_SCHED_POLICY: {
                    final DropWorkloadSchedPolicyOperatorLog dropLog
                            = (DropWorkloadSchedPolicyOperatorLog) journal.getData();
                    env.getWorkloadSchedPolicyMgr().replayDropWorkloadSchedPolicy(dropLog.getId());
                    break;
                }
                case OperationType.OP_INIT_EXTERNAL_TABLE: {
                    // Do nothing.
                    break;
                }
                case OperationType.OP_CREATE_ANALYSIS_JOB: {
                    AnalysisInfo info = (AnalysisInfo) journal.getData();
                    if (AnalysisManager.needAbandon(info)) {
                        break;
                    }
                    env.getAnalysisManager().replayCreateAnalysisJob(info);
                    break;
                }
                case OperationType.OP_CREATE_ANALYSIS_TASK: {
                    AnalysisInfo info = (AnalysisInfo) journal.getData();
                    if (AnalysisManager.needAbandon(info)) {
                        break;
                    }
                    env.getAnalysisManager().replayCreateAnalysisTask(info);
                    break;
                }
                case OperationType.OP_DELETE_ANALYSIS_JOB: {
                    env.getAnalysisManager().replayDeleteAnalysisJob((AnalyzeDeletionLog) journal.getData());
                    break;
                }
                case OperationType.OP_DELETE_ANALYSIS_TASK: {
                    env.getAnalysisManager().replayDeleteAnalysisTask((AnalyzeDeletionLog) journal.getData());
                    break;
                }
                case OperationType.OP_ADD_PLSQL_STORED_PROCEDURE: {
                    env.getPlsqlManager().replayAddPlsqlStoredProcedure((PlsqlStoredProcedure) journal.getData());
                    break;
                }
                case OperationType.OP_DROP_PLSQL_STORED_PROCEDURE: {
                    env.getPlsqlManager().replayDropPlsqlStoredProcedure((PlsqlProcedureKey) journal.getData());
                    break;
                }
                case OperationType.OP_ADD_PLSQL_PACKAGE: {
                    env.getPlsqlManager().replayAddPlsqlPackage((PlsqlPackage) journal.getData());
                    break;
                }
                case OperationType.OP_DROP_PLSQL_PACKAGE: {
                    env.getPlsqlManager().replayDropPlsqlPackage((PlsqlProcedureKey) journal.getData());
                    break;
                }
                case OperationType.OP_ALTER_DATABASE_PROPERTY: {
                    AlterDatabasePropertyInfo alterDatabasePropertyInfo = (AlterDatabasePropertyInfo) journal.getData();
                    LOG.info("replay alter database property: {}", alterDatabasePropertyInfo);
                    env.replayAlterDatabaseProperty(alterDatabasePropertyInfo.getDbName(),
                            alterDatabasePropertyInfo.getProperties());
                    env.getBinlogManager().addAlterDatabaseProperty(alterDatabasePropertyInfo, logId);
                    break;
                }
                case OperationType.OP_GC_BINLOG: {
                    BinlogGcInfo binlogGcInfo = (BinlogGcInfo) journal.getData();
                    LOG.info("replay gc binlog: {}", binlogGcInfo);
                    env.replayGcBinlog(binlogGcInfo);
                    break;
                }
                case OperationType.OP_BARRIER: {
                    BarrierLog log = (BarrierLog) journal.getData();
                    env.getBinlogManager().addBarrierLog(log, logId);
                    break;
                }
                case OperationType.OP_UPDATE_AUTO_INCREMENT_ID: {
                    env.replayAutoIncrementIdUpdateLog((AutoIncrementIdUpdateLog) journal.getData());
                    break;
                }
                case OperationType.OP_UPDATE_TABLE_STATS: {
                    env.getAnalysisManager().replayUpdateTableStatsStatus((TableStatsMeta) journal.getData());
                    break;
                }
                case OperationType.OP_PERSIST_AUTO_JOB: {
                    // Do nothing
                    break;
                }
                case OperationType.OP_DELETE_TABLE_STATS: {
                    long tableId = ((TableStatsDeletionLog) journal.getData()).id;
                    LOG.info("replay delete table stat tableId: {}", tableId);
                    Env.getCurrentEnv().getAnalysisManager().removeTableStats(tableId);
                    break;
                }
                case OperationType.OP_ALTER_MTMV: {
                    final AlterMTMV alterMtmv = (AlterMTMV) journal.getData();
                    env.getAlterInstance().processAlterMTMV(alterMtmv, true);
                    break;
                }
                case OperationType.OP_INSERT_OVERWRITE: {
                    final InsertOverwriteLog insertOverwriteLog = (InsertOverwriteLog) journal.getData();
                    env.getInsertOverwriteManager().replayInsertOverwriteLog(insertOverwriteLog);
                    break;
                }
                case OperationType.OP_ALTER_REPOSITORY: {
                    Repository repository = (Repository) journal.getData();
                    env.getBackupHandler().getRepoMgr().alterRepo(repository, true);
                    break;
                }
                case OperationType.OP_ADD_META_ID_MAPPINGS: {
                    env.getExternalMetaIdMgr().replayMetaIdMappingsLog((MetaIdMappingsLog) journal.getData());
                    break;
                }
                case OperationType.OP_LOG_UPDATE_ROWS: {
                    env.getAnalysisManager().replayUpdateRowsRecord((UpdateRowsEvent) journal.getData());
                    break;
                }
                case OperationType.OP_LOG_NEW_PARTITION_LOADED: {
                    env.getAnalysisManager().replayNewPartitionLoadedEvent((NewPartitionLoadedEvent) journal.getData());
                    break;
                }
                case OperationType.OP_LOG_ALTER_COLUMN_STATS: {
                    // TODO: implement this while statistics finished related work.
                    break;
                }
                case OperationType.OP_UPDATE_CLOUD_REPLICA: {
                    UpdateCloudReplicaInfo info = (UpdateCloudReplicaInfo) journal.getData();
                    ((CloudEnv) env).replayUpdateCloudReplica(info);
                    break;
                }
                case OperationType.OP_CREATE_DICTIONARY: {
                    CreateDictionaryPersistInfo info = (CreateDictionaryPersistInfo) journal.getData();
                    env.getDictionaryManager().replayCreateDictionary(info);
                    break;
                }
                case OperationType.OP_DROP_DICTIONARY: {
                    DropDictionaryPersistInfo info = (DropDictionaryPersistInfo) journal.getData();
                    env.getDictionaryManager().replayDropDictionary(info);
                    break;
                }
                case OperationType.OP_DICTIONARY_INC_VERSION: {
                    DictionaryIncreaseVersionInfo info = (DictionaryIncreaseVersionInfo) journal.getData();
                    env.getDictionaryManager().replayIncreaseVersion(info);
                    break;
                }
                case OperationType.OP_DICTIONARY_DEC_VERSION: {
                    DictionaryDecreaseVersionInfo info = (DictionaryDecreaseVersionInfo) journal.getData();
                    env.getDictionaryManager().replayDecreaseVersion(info);
                    break;
                }
                case OperationType.OP_CREATE_INDEX_POLICY: {
                    IndexPolicy log = (IndexPolicy) journal.getData();
                    env.getIndexPolicyMgr().replayCreateIndexPolicy(log);
                    break;
                }
                case OperationType.OP_DROP_INDEX_POLICY: {
                    DropIndexPolicyLog log = (DropIndexPolicyLog) journal.getData();
                    env.getIndexPolicyMgr().replayDropIndexPolicy(log);
                    break;
                }
                case OperationType.OP_BRANCH_OR_TAG: {
                    TableBranchOrTagInfo info = (TableBranchOrTagInfo) journal.getData();
                    CatalogIf ctl = Env.getCurrentEnv().getCatalogMgr().getCatalog(info.getCtlName());
                    if (ctl != null) {
                        ctl.replayOperateOnBranchOrTag(info.getDbName(), info.getTblName());
                    }
                    break;
                }
                case OperationType.OP_OPERATE_KEY: {
                    KeyOperationInfo info = (KeyOperationInfo) journal.getData();
                    env.getKeyManager().replayKeyOperation(info);
                    break;
                }
                default: {
                    IOException e = new IOException();
                    LOG.error("UNKNOWN Operation Type {}, log id: {}", opCode, logId, e);
                    throw e;
                }
            }
        } catch (MetaNotFoundException e) {
            /*
             * In the following cases, doris may record metadata modification information
             * for a table that no longer exists.
             * 1. Thread 1: get TableA object
             * 2. Thread 2: lock db and drop table and record edit log of the dropped TableA
             * 3. Thread 1: lock table, modify table and record edit log of the modified
             * TableA
             * **The modified edit log is after the dropped edit log**
             * Because the table has been dropped, the olapTable in here is null when the
             * modified edit log is replayed.
             * So in this case, we will ignore the edit log of the modified table after the
             * table is dropped.
             * This could make the meta inconsistent, for example, an edit log on a dropped
             * table is ignored, but
             * this table is restored later, so there may be an inconsistent situation
             * between master and followers. We
             * log a warning here to debug when happens. This could happen to other meta
             * like DB.
             */
            LOG.warn("[INCONSISTENT META] replay log {} failed, journal {}: {}", logId, journal, e.getMessage(), e);
        } catch (Exception e) {
            LOG.error("replay Operation Type {}, log id: {}", opCode, logId, e);
            System.exit(-1);
        }
    }

    /**
     * Shutdown the file store.
     */
    public synchronized void close() throws IOException {
        journal.close();
    }

    public void open() {
        journal.open();
    }

    /**
     * Close current journal and start a new journal
     */
    public void rollEditLog() {
        journal.rollJournal();
    }

    // NOTICE: No guarantee atomicity of entries
    private <T extends Writable> void logEdit(short op, List<T> entries) throws IOException {
        int itemNum = Math.max(1, Math.min(Config.batch_edit_log_max_item_num, entries.size()));
        JournalBatch batch = new JournalBatch(itemNum);
        long batchCount = 0;
        for (T entry : entries) {
            if (batch.shouldFlush()) {
                journal.write(batch);
                batch = new JournalBatch(itemNum);

                // take a rest
                batchCount++;
                if (batchCount >= Config.batch_edit_log_continuous_count_for_rest
                        && Config.batch_edit_log_rest_time_ms > 0) {
                    batchCount = 0;
                    try {
                        Thread.sleep(Config.batch_edit_log_rest_time_ms);
                    } catch (InterruptedException e) {
                        LOG.warn("sleep failed", e);
                    }
                }
            }
            batch.addJournal(op, entry);
        }
        if (!batch.getJournalEntities().isEmpty()) {
            journal.write(batch);
        }
        txId += entries.size();
    }

    /**
     * Asynchronously log an edit by putting it into a blocking queue and waiting for completion.
     * This method blocks until the log is written and returns the logId.
     */
    public long logEditWithQueue(short op, Writable writable) {
        EditLogItem req = new EditLogItem(op, writable);
        if (LOG.isDebugEnabled()) {
            LOG.debug("logEditWithQueue: op = {}, uid = {}", op, req.uid);
        }
        while (true) {
            try {
                logEditQueue.put(req);
                break;
            } catch (InterruptedException e) {
                LOG.warn("Interrupted during put, will sleep and retry.");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                    LOG.warn(" interrupted during sleep, will retry.", ex);
                }
            }
        }
        synchronized (req.lock) {
            while (!req.finished) {
                try {
                    req.lock.wait();
                } catch (InterruptedException e) {
                    LOG.error("Fatal Error : write stream Exception");
                    System.exit(-1);
                }
            }
        }

        return req.logId;
    }

    private synchronized long logEditDirectly(short op, Writable writable) {
        long logId = -1;
        try {
            logId = journal.write(op, writable);
        } catch (Throwable t) {
            // Throwable contains all Exception and Error, such as IOException and
            // OutOfMemoryError
            if (journal instanceof BDBJEJournal) {
                LOG.error("BDBJE stats : {}", ((BDBJEJournal) journal).getBDBStats());
            }
            LOG.error("Fatal Error : write stream Exception", t);
            System.exit(-1);
        }

        // get a new transactionId
        txId++;

        if (txId >= Config.edit_log_roll_num) {
            LOG.info("txId {} is equal to or larger than edit_log_roll_num {}, will roll edit.", txId,
                    Config.edit_log_roll_num);
            rollEditLog();
            txId = 0;
        }

        return logId;
    }

    /**
     * Write an operation to the edit log. Do not sync to persistent store yet.
     */
    private long logEdit(short op, Writable writable) {
        if (this.getNumEditStreams() == 0) {
            LOG.error("Fatal Error : no editLog stream", new Exception());
            throw new Error("Fatal Error : no editLog stream");
        }

        long start = System.currentTimeMillis();
        if (DebugPointUtil.isEnable("EditLog.logEdit.randomSleep")) {
            DebugPointUtil.DebugPoint debugPoint = DebugPointUtil.getDebugPoint("EditLog.logEdit.randomSleep");
            int upperLimit = debugPoint.param("upperLimit", 3000);
            long timestamp = System.currentTimeMillis();
            Random random = new Random(timestamp);
            try {
                int ms = Math.abs(random.nextInt()) % upperLimit;
                Thread.sleep(ms);
                LOG.info("logEdit in debug point op {} content {} sleep {} ms",
                        OperationType.getOpName(op), writable.toString(), ms);
            } catch (InterruptedException e) {
                LOG.warn("sleep exception op {} content {}", OperationType.getOpName(op), writable.toString(), e);
            }
        }
        long logId = -1;
        if (Config.enable_batch_editlog && op != OperationType.OP_TIMESTAMP) {
            logId = logEditWithQueue(op, writable);
        } else {
            logId = logEditDirectly(op, writable);
        }

        // update statistics
        long end = System.currentTimeMillis();
        numTransactions.incrementAndGet();
        totalTimeTransactions.addAndGet(end - start);
        if (MetricRepo.isInit) {
            MetricRepo.HISTO_EDIT_LOG_WRITE_LATENCY.update((end - start));
            MetricRepo.COUNTER_EDIT_LOG_CURRENT.increase(1L);
            MetricRepo.COUNTER_EDIT_LOG_WRITE.increase(1L);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("nextId = {}, numTransactions = {}, totalTimeTransactions = {}, op = {} delta = {}",
                    txId, numTransactions, totalTimeTransactions, op, end - start);
        }

        return logId;
    }

    /**
     * Return the size of the current EditLog
     */
    public synchronized long getEditLogSize() throws IOException {
        return editStream.length();
    }

    /**
     * Return the number of the current EditLog
     */
    public synchronized long getEditLogNum() throws IOException {
        return journal.getJournalNum();
    }

    public void logSaveNextId(long nextId) {
        logEdit(OperationType.OP_SAVE_NEXTID, new Text(Long.toString(nextId)));
    }

    public void logSaveTransactionId(long transactionId) {
        logEdit(OperationType.OP_SAVE_TRANSACTION_ID, new Text(Long.toString(transactionId)));
    }

    public void logCreateDb(CreateDbInfo info) {
        logEdit(OperationType.OP_NEW_CREATE_DB, info);
    }

    public void logDropDb(DropDbInfo dropDbInfo) {
        logEdit(OperationType.OP_DROP_DB, dropDbInfo);
    }

    public void logEraseDb(long dbId) {
        logEdit(OperationType.OP_ERASE_DB, new Text(Long.toString(dbId)));
    }

    public void logRecoverDb(RecoverInfo info) {
        logEdit(OperationType.OP_RECOVER_DB, info);
    }

    public void logAlterDb(DatabaseInfo dbInfo) {
        logEdit(OperationType.OP_ALTER_DB, dbInfo);
    }

    public void logCreateTable(CreateTableInfo info) {
        long logId = logEdit(OperationType.OP_CREATE_TABLE, info);
        if (Strings.isNullOrEmpty(info.getCtlName()) || info.getCtlName()
                .equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
            CreateTableRecord record = new CreateTableRecord(logId, info);
            Env.getCurrentEnv().getBinlogManager().addCreateTableRecord(record);
        }
    }

    public void logRefreshExternalTableSchema(RefreshExternalTableInfo info) {
        logEdit(OperationType.OP_ALTER_EXTERNAL_TABLE_SCHEMA, info);
    }

    public long logAddPartition(PartitionPersistInfo info) {
        long logId = logEdit(OperationType.OP_ADD_PARTITION, info);
        AddPartitionRecord record = new AddPartitionRecord(logId, info);
        Env.getCurrentEnv().getBinlogManager().addAddPartitionRecord(record);
        return logId;
    }

    public long logDropPartition(DropPartitionInfo info) {
        long logId = logEdit(OperationType.OP_DROP_PARTITION, info);
        Env.getCurrentEnv().getBinlogManager().addDropPartitionRecord(info, logId);
        return logId;
    }

    public void logErasePartition(long partitionId) {
        logEdit(OperationType.OP_ERASE_PARTITION, new Text(Long.toString(partitionId)));
    }

    public void logRecoverPartition(RecoverInfo info) {
        long logId = logEdit(OperationType.OP_RECOVER_PARTITION, info);
        Env.getCurrentEnv().getBinlogManager().addRecoverTableRecord(info, logId);
    }

    public void logModifyPartition(ModifyPartitionInfo info) {
        long logId = logEdit(OperationType.OP_MODIFY_PARTITION, info);
        BatchModifyPartitionsInfo infos = new BatchModifyPartitionsInfo(info);
        LOG.info("log modify partition, logId:{}, infos: {}", logId, infos.toJson());
        Env.getCurrentEnv().getBinlogManager().addModifyPartitions(infos, logId);
    }

    public void logBatchModifyPartition(BatchModifyPartitionsInfo info) {
        long logId = logEdit(OperationType.OP_BATCH_MODIFY_PARTITION, info);
        LOG.info("log modify partition, logId:{}, infos: {}", logId, info.toJson());
        Env.getCurrentEnv().getBinlogManager().addModifyPartitions(info, logId);
    }

    public void logDropTable(DropInfo info) {
        long logId = logEdit(OperationType.OP_DROP_TABLE, info);
        if (Strings.isNullOrEmpty(info.getCtl()) || info.getCtl().equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
            DropTableRecord record = new DropTableRecord(logId, info);
            Env.getCurrentEnv().getBinlogManager().addDropTableRecord(record);
        }
    }

    public void logEraseTable(long tableId) {
        logEdit(OperationType.OP_ERASE_TABLE, new Text(Long.toString(tableId)));
    }

    public void logRecoverTable(RecoverInfo info) {
        long logId = logEdit(OperationType.OP_RECOVER_TABLE, info);
        Env.getCurrentEnv().getBinlogManager().addRecoverTableRecord(info, logId);
    }

    public void logDropRollup(DropInfo info) {
        long logId = logEdit(OperationType.OP_DROP_ROLLUP, info);
        Env.getCurrentEnv().getBinlogManager().addDropRollup(info, logId);
    }

    public void logBatchDropRollup(BatchDropInfo batchDropInfo) {
        long logId = logEdit(OperationType.OP_BATCH_DROP_ROLLUP, batchDropInfo);
        for (Map.Entry<Long, String> entry : batchDropInfo.getIndexNameMap().entrySet()) {
            DropInfo info = new DropInfo(batchDropInfo.getDbId(),
                    batchDropInfo.getTableId(),
                    batchDropInfo.getTableName(),
                    entry.getKey(), entry.getValue(),
                    false, true, 0);
            Env.getCurrentEnv().getBinlogManager().addDropRollup(info, logId);
        }
    }

    public void logFinishConsistencyCheck(ConsistencyCheckInfo info) {
        logEdit(OperationType.OP_FINISH_CONSISTENCY_CHECK, info);
    }

    public void logAddBackend(Backend be) {
        logEdit(OperationType.OP_ADD_BACKEND, be);
    }

    public void logDropBackend(Backend be) {
        logEdit(OperationType.OP_DROP_BACKEND, be);
    }

    public void logModifyBackend(Backend be) {
        logEdit(OperationType.OP_MODIFY_BACKEND, be);
    }

    public void logAddFrontend(Frontend fe) {
        logEdit(OperationType.OP_ADD_FRONTEND, fe);
    }

    public void logAddFirstFrontend(Frontend fe) {
        logEdit(OperationType.OP_ADD_FIRST_FRONTEND, fe);
    }

    public void logModifyFrontend(Frontend fe) {
        logEdit(OperationType.OP_MODIFY_FRONTEND, fe);
    }

    public void logRemoveFrontend(Frontend fe) {
        logEdit(OperationType.OP_REMOVE_FRONTEND, fe);
    }

    public void logFinishDelete(DeleteInfo info) {
        logEdit(OperationType.OP_FINISH_DELETE, info);
    }

    public void logAddReplica(ReplicaPersistInfo info) {
        logEdit(OperationType.OP_ADD_REPLICA, info);
    }

    public void logUpdateReplica(ReplicaPersistInfo info) {
        logEdit(OperationType.OP_UPDATE_REPLICA, info);
    }

    public void logDeleteReplica(ReplicaPersistInfo info) {
        logEdit(OperationType.OP_DELETE_REPLICA, info);
    }

    public void logTimestamp(Timestamp stamp) {
        logEdit(OperationType.OP_TIMESTAMP, stamp);
    }

    public void logMasterInfo(MasterInfo info) {
        logEdit(OperationType.OP_MASTER_INFO_CHANGE, info);
    }

    public void logMetaVersion(int version) {
        logEdit(OperationType.OP_META_VERSION, new Text(Integer.toString(version)));
    }

    public void logBackendStateChange(Backend be) {
        logEdit(OperationType.OP_BACKEND_STATE_CHANGE, be);
    }

    public void logCreateUser(PrivInfo info) {
        logEdit(OperationType.OP_CREATE_USER, info);
    }

    public void logNewDropUser(UserIdentity userIdent) {
        logEdit(OperationType.OP_NEW_DROP_USER, userIdent);
    }

    public void logGrantPriv(PrivInfo info) {
        logEdit(OperationType.OP_GRANT_PRIV, info);
    }

    public void logRevokePriv(PrivInfo info) {
        logEdit(OperationType.OP_REVOKE_PRIV, info);
    }

    public void logSetPassword(PrivInfo info) {
        logEdit(OperationType.OP_SET_PASSWORD, info);
    }

    public void logSetLdapPassword(LdapInfo info) {
        logEdit(OperationType.OP_SET_LDAP_PASSWORD, info);
    }

    public void logCreateRole(PrivInfo info) {
        logEdit(OperationType.OP_CREATE_ROLE, info);
    }

    public void logAlterRole(PrivInfo info) {
        logEdit(OperationType.OP_ALTER_ROLE, info);
    }

    public void logDropRole(PrivInfo info) {
        logEdit(OperationType.OP_DROP_ROLE, info);
    }

    public void logDatabaseRename(DatabaseInfo databaseInfo) {
        logEdit(OperationType.OP_RENAME_DB, databaseInfo);
    }

    public void logTableRename(TableInfo tableInfo) {
        long logId = logEdit(OperationType.OP_RENAME_TABLE, tableInfo);
        LOG.info("log table rename, logId : {}, infos: {}", logId, tableInfo);
        Env.getCurrentEnv().getBinlogManager().addTableRename(tableInfo, logId);
    }

    public void logModifyViewDef(AlterViewInfo alterViewInfo) {
        long logId = logEdit(OperationType.OP_MODIFY_VIEW_DEF, alterViewInfo);
        if (LOG.isDebugEnabled()) {
            LOG.debug("log modify view, logId : {}, infos: {}", logId, alterViewInfo);
        }
        Env.getCurrentEnv().getBinlogManager().addModifyViewDef(alterViewInfo, logId);
    }

    public void logRollupRename(TableInfo tableInfo) {
        long logId = logEdit(OperationType.OP_RENAME_ROLLUP, tableInfo);
        LOG.info("log rollup rename, logId : {}, infos: {}", logId, tableInfo);
        Env.getCurrentEnv().getBinlogManager().addRollupRename(tableInfo, logId);
    }

    public void logPartitionRename(TableInfo tableInfo) {
        long logId = logEdit(OperationType.OP_RENAME_PARTITION, tableInfo);
        LOG.info("log partition rename, logId : {}, infos: {}", logId, tableInfo);
        Env.getCurrentEnv().getBinlogManager().addPartitionRename(tableInfo, logId);
    }

    public void logColumnRename(TableRenameColumnInfo info) {
        long logId = logEdit(OperationType.OP_RENAME_COLUMN, info);
        LOG.info("log column rename, logId : {}, infos: {}", logId, info);
        Env.getCurrentEnv().getBinlogManager().addColumnRename(info, logId);
    }

    public void logAddBroker(BrokerMgr.ModifyBrokerInfo info) {
        logEdit(OperationType.OP_ADD_BROKER, info);
    }

    public void logDropBroker(BrokerMgr.ModifyBrokerInfo info) {
        logEdit(OperationType.OP_DROP_BROKER, info);
    }

    public void logDropAllBroker(String brokerName) {
        logEdit(OperationType.OP_DROP_ALL_BROKER, new Text(brokerName));
    }

    public void logExportCreate(ExportJob job) {
        logEdit(OperationType.OP_EXPORT_CREATE, job);
    }

    public void logUpdateCloudReplicas(List<UpdateCloudReplicaInfo> infos) throws IOException {
        long start = System.currentTimeMillis();
        logEdit(OperationType.OP_UPDATE_CLOUD_REPLICA, infos);
        if (LOG.isDebugEnabled()) {
            LOG.debug("log update {} cloud replicas. cost: {} ms", infos.size(), (System.currentTimeMillis() - start));
        }
    }

    public void logExportUpdateState(ExportJob job, ExportJobState newState) {
        ExportJobStateTransfer transfer = new ExportJobStateTransfer(job, newState);
        logEdit(OperationType.OP_EXPORT_UPDATE_STATE, transfer);
    }

    // for TransactionState
    public void logInsertTransactionState(TransactionState transactionState) {
        long start = System.currentTimeMillis();
        long logId = logEdit(OperationType.OP_UPSERT_TRANSACTION_STATE, transactionState);
        long logEditEnd = System.currentTimeMillis();
        long end = logEditEnd;
        if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
            UpsertRecord record = new UpsertRecord(logId, transactionState);
            Env.getCurrentEnv().getBinlogManager().addUpsertRecord(record);
            end = System.currentTimeMillis();
        }
        if (end - start > Config.lock_reporting_threshold_ms) {
            LOG.warn("edit log insert transaction take a lot time, write bdb {} ms, write binlog {} ms",
                    logEditEnd - start, end - logEditEnd);
        }
    }

    public void logBackupJob(BackupJob job) {
        logEdit(OperationType.OP_BACKUP_JOB, job);
    }

    public void logCreateRepository(Repository repo) {
        logEdit(OperationType.OP_CREATE_REPOSITORY, repo);
    }

    public void logDropRepository(String repoName) {
        logEdit(OperationType.OP_DROP_REPOSITORY, new Text(repoName));
    }

    public void logAlterRepository(Repository repo) {
        logEdit(OperationType.OP_ALTER_REPOSITORY, repo);
    }

    public void logRestoreJob(RestoreJob job) {
        logEdit(OperationType.OP_RESTORE_JOB, job);
    }

    public void logUpdateUserProperty(UserPropertyInfo propertyInfo) {
        logEdit(OperationType.OP_UPDATE_USER_PROPERTY, propertyInfo);
    }

    public void logTruncateTable(TruncateTableInfo info) {
        long logId = logEdit(OperationType.OP_TRUNCATE_TABLE, info);
        LOG.info("log truncate table, logId:{}, infos: {}", logId, info);
        if (Strings.isNullOrEmpty(info.getCtl()) || info.getCtl().equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
            Env.getCurrentEnv().getBinlogManager().addTruncateTable(info, logId);
        }
    }

    public void logColocateModifyRepliaAlloc(ColocatePersistInfo info) {
        logEdit(OperationType.OP_COLOCATE_MOD_REPLICA_ALLOC, info);
    }

    public void logColocateAddTable(ColocatePersistInfo info) {
        logEdit(OperationType.OP_COLOCATE_ADD_TABLE, info);
    }

    public void logColocateRemoveTable(ColocatePersistInfo info) {
        logEdit(OperationType.OP_COLOCATE_REMOVE_TABLE, info);
    }

    public void logColocateBackendsPerBucketSeq(ColocatePersistInfo info) {
        logEdit(OperationType.OP_COLOCATE_BACKENDS_PER_BUCKETSEQ, info);
    }

    public void logColocateMarkUnstable(ColocatePersistInfo info) {
        logEdit(OperationType.OP_COLOCATE_MARK_UNSTABLE, info);
    }

    public void logColocateMarkStable(ColocatePersistInfo info) {
        logEdit(OperationType.OP_COLOCATE_MARK_STABLE, info);
    }

    public void logModifyTableColocate(TablePropertyInfo info) {
        logEdit(OperationType.OP_MODIFY_TABLE_COLOCATE, info);
    }

    public void logHeartbeat(HbPackage hbPackage) {
        logEdit(OperationType.OP_HEARTBEAT, hbPackage);
    }

    public void logAddFunction(Function function) {
        logEdit(OperationType.OP_ADD_FUNCTION, function);
    }

    public void logAddGlobalFunction(Function function) {
        logEdit(OperationType.OP_ADD_GLOBAL_FUNCTION, function);
    }

    public void logDropFunction(FunctionSearchDesc function) {
        logEdit(OperationType.OP_DROP_FUNCTION, function);
    }

    public void logDropGlobalFunction(FunctionSearchDesc function) {
        logEdit(OperationType.OP_DROP_GLOBAL_FUNCTION, function);
    }

    public void logAddEncryptKey(EncryptKey encryptKey) {
        logEdit(OperationType.OP_CREATE_ENCRYPTKEY, encryptKey);
    }

    public void logDropEncryptKey(EncryptKeySearchDesc desc) {
        logEdit(OperationType.OP_DROP_ENCRYPTKEY, desc);
    }

    @Deprecated
    public void logBackendTabletsInfo(BackendTabletsInfo backendTabletsInfo) {
        logEdit(OperationType.OP_BACKEND_TABLETS_INFO, backendTabletsInfo);
    }

    public void logBackendReplicasInfo(BackendReplicasInfo backendReplicasInfo) {
        logEdit(OperationType.OP_BACKEND_REPLICAS_INFO, backendReplicasInfo);
    }

    public void logCreateRoutineLoadJob(RoutineLoadJob routineLoadJob) {
        logEdit(OperationType.OP_CREATE_ROUTINE_LOAD_JOB, routineLoadJob);
    }

    public void logCreateJob(AbstractJob job) {
        logEdit(OperationType.OP_CREATE_SCHEDULER_JOB, job);
    }

    public void logUpdateJob(AbstractJob job) {
        logEdit(OperationType.OP_UPDATE_SCHEDULER_JOB, job);
    }

    public void logDeleteJob(AbstractJob job) {
        logEdit(OperationType.OP_DELETE_SCHEDULER_JOB, job);
    }

    public void logOpRoutineLoadJob(RoutineLoadOperation routineLoadOperation) {
        logEdit(OperationType.OP_CHANGE_ROUTINE_LOAD_JOB, routineLoadOperation);
    }

    public void logRemoveRoutineLoadJob(RoutineLoadOperation operation) {
        logEdit(OperationType.OP_REMOVE_ROUTINE_LOAD_JOB, operation);
    }

    public void logCreateLoadJob(org.apache.doris.load.loadv2.LoadJob loadJob) {
        logEdit(OperationType.OP_CREATE_LOAD_JOB, loadJob);
    }

    public void logEndLoadJob(LoadJobFinalOperation loadJobFinalOperation) {
        logEdit(OperationType.OP_END_LOAD_JOB, loadJobFinalOperation);
    }

    public void logUpdateLoadJob(LoadJobStateUpdateInfo info) {
        logEdit(OperationType.OP_UPDATE_LOAD_JOB, info);
    }

    public void logFetchStreamLoadRecord(FetchStreamLoadRecord fetchStreamLoadRecord) {
        logEdit(OperationType.OP_FETCH_STREAM_LOAD_RECORD, fetchStreamLoadRecord);
    }

    public void logCreateResource(Resource resource) {
        logEdit(OperationType.OP_CREATE_RESOURCE, resource);
    }

    public void logDropResource(DropResourceOperationLog operationLog) {
        logEdit(OperationType.OP_DROP_RESOURCE, operationLog);
    }

    public void logAlterResource(Resource resource) {
        logEdit(OperationType.OP_ALTER_RESOURCE, resource);
    }

    public void logAlterWorkloadGroup(WorkloadGroup workloadGroup) {
        logEdit(OperationType.OP_ALTER_WORKLOAD_GROUP, workloadGroup);
    }

    public void logCreateWorkloadGroup(WorkloadGroup workloadGroup) {
        logEdit(OperationType.OP_CREATE_WORKLOAD_GROUP, workloadGroup);
    }

    public void logDropWorkloadGroup(DropWorkloadGroupOperationLog operationLog) {
        logEdit(OperationType.OP_DROP_WORKLOAD_GROUP, operationLog);
    }

    public void logCreateWorkloadSchedPolicy(WorkloadSchedPolicy workloadSchedPolicy) {
        logEdit(OperationType.OP_CREATE_WORKLOAD_SCHED_POLICY, workloadSchedPolicy);
    }

    public void logAlterWorkloadSchedPolicy(WorkloadSchedPolicy workloadSchedPolicy) {
        logEdit(OperationType.OP_ALTER_WORKLOAD_SCHED_POLICY, workloadSchedPolicy);
    }

    public void dropWorkloadSchedPolicy(long policyId) {
        logEdit(OperationType.OP_DROP_WORKLOAD_SCHED_POLICY, new DropWorkloadSchedPolicyOperatorLog(policyId));
    }

    public void logAddPlsqlStoredProcedure(PlsqlStoredProcedure plsqlStoredProcedure) {
        logEdit(OperationType.OP_ADD_PLSQL_STORED_PROCEDURE, plsqlStoredProcedure);
    }

    public void logDropPlsqlStoredProcedure(PlsqlProcedureKey plsqlProcedureKey) {
        logEdit(OperationType.OP_DROP_PLSQL_STORED_PROCEDURE, plsqlProcedureKey);
    }

    public void logAddPlsqlPackage(PlsqlPackage pkg) {
        logEdit(OperationType.OP_ADD_PLSQL_PACKAGE, pkg);
    }

    public void logDropPlsqlPackage(PlsqlProcedureKey plsqlProcedureKey) {
        logEdit(OperationType.OP_DROP_PLSQL_PACKAGE, plsqlProcedureKey);
    }

    public void logAlterStoragePolicy(StoragePolicy storagePolicy) {
        logEdit(OperationType.OP_ALTER_STORAGE_POLICY, storagePolicy);
    }

    public void logCreateSmallFile(SmallFile info) {
        logEdit(OperationType.OP_CREATE_SMALL_FILE, info);
    }

    public void logDropSmallFile(SmallFile info) {
        logEdit(OperationType.OP_DROP_SMALL_FILE, info);
    }

    public void logAlterJob(AlterJobV2 alterJob) {
        long logId = logEdit(OperationType.OP_ALTER_JOB_V2, alterJob);
        Env.getCurrentEnv().getBinlogManager().addAlterJobV2(alterJob, logId);
    }

    public void logUpdateCooldownConf(CooldownConfList cooldownConf) {
        logEdit(OperationType.OP_UPDATE_COOLDOWN_CONF, cooldownConf);
    }

    public void logCooldownDelete(CooldownDelete cooldownDelete) {
        logEdit(OperationType.OP_COOLDOWN_DELETE, cooldownDelete);
    }

    public void logBatchAlterJob(BatchAlterJobPersistInfo batchAlterJobV2) {
        logEdit(OperationType.OP_BATCH_ADD_ROLLUP, batchAlterJobV2);
    }

    public void logModifyDistributionType(TableInfo tableInfo) {
        long logId = logEdit(OperationType.OP_MODIFY_DISTRIBUTION_TYPE, tableInfo);
        LOG.info("add modify distribution type binlog, logId: {}, infos: {}", logId, tableInfo);
        Env.getCurrentEnv().getBinlogManager().addModifyDistributionType(tableInfo, logId);
    }

    public void logModifyCloudWarmUpJob(CloudWarmUpJob cloudWarmUpJob) {
        logEdit(OperationType.OP_MODIFY_CLOUD_WARM_UP_JOB, cloudWarmUpJob);
    }

    private long logModifyTableProperty(short op, ModifyTablePropertyOperationLog info) {
        long logId = logEdit(op, info);
        Env.getCurrentEnv().getBinlogManager().addModifyTableProperty(info, logId);
        return logId;
    }

    public void logDynamicPartition(ModifyTablePropertyOperationLog info) {
        logModifyTableProperty(OperationType.OP_DYNAMIC_PARTITION, info);
    }

    public long logModifyReplicationNum(ModifyTablePropertyOperationLog info) {
        return logModifyTableProperty(OperationType.OP_MODIFY_REPLICATION_NUM, info);
    }

    public void logModifyDefaultDistributionBucketNum(ModifyTableDefaultDistributionBucketNumOperationLog log) {
        long logId = logEdit(OperationType.OP_MODIFY_DISTRIBUTION_BUCKET_NUM, log);
        LOG.info("add modify distribution bucket num binlog, logId: {}, infos: {}", logId, log);
        Env.getCurrentEnv().getBinlogManager().addModifyDistributionNum(log, logId);
    }

    public long logModifyTableProperties(ModifyTablePropertyOperationLog info) {
        return logModifyTableProperty(OperationType.OP_MODIFY_TABLE_PROPERTIES, info);
    }

    public long logUpdateBinlogConfig(ModifyTablePropertyOperationLog info) {
        return logModifyTableProperty(OperationType.OP_UPDATE_BINLOG_CONFIG, info);
    }

    public void logAlterLightSchemaChange(AlterLightSchemaChangeInfo info) {
        logEdit(OperationType.OP_ALTER_LIGHT_SCHEMA_CHANGE, info);
    }

    public void logReplaceTempPartition(ReplacePartitionOperationLog info) {
        long logId = logEdit(OperationType.OP_REPLACE_TEMP_PARTITION, info);
        LOG.info("log replace temp partition, logId: {}, info: {}", logId, info.toJson());
        Env.getCurrentEnv().getBinlogManager().addReplacePartitions(info, logId);
    }

    public void logInstallPlugin(PluginInfo plugin) {
        logEdit(OperationType.OP_INSTALL_PLUGIN, plugin);
    }

    public void logUninstallPlugin(PluginInfo plugin) {
        logEdit(OperationType.OP_UNINSTALL_PLUGIN, plugin);
    }

    public void logSetReplicaStatus(SetReplicaStatusOperationLog log) {
        logEdit(OperationType.OP_SET_REPLICA_STATUS, log);
    }

    public void logSetReplicaVersion(SetReplicaVersionOperationLog log) {
        logEdit(OperationType.OP_SET_REPLICA_VERSION, log);
    }

    public void logRemoveExpiredAlterJobV2(RemoveAlterJobV2OperationLog log) {
        logEdit(OperationType.OP_REMOVE_ALTER_JOB_V2, log);
    }

    public void logAlterRoutineLoadJob(AlterRoutineLoadJobOperationLog log) {
        logEdit(OperationType.OP_ALTER_ROUTINE_LOAD_JOB, log);
    }

    public void logSetPartitionVersion(SetPartitionVersionOperationLog log) {
        logEdit(OperationType.OP_SET_PARTITION_VERSION, log);
    }

    public void logGlobalVariableV2(GlobalVarPersistInfo info) {
        logEdit(OperationType.OP_GLOBAL_VARIABLE_V2, info);
    }

    public void logReplaceTable(ReplaceTableOperationLog log) {
        long logId = logEdit(OperationType.OP_REPLACE_TABLE, log);
        LOG.info("add replace table binlog, logId: {}, infos: {}", logId, log);
        Env.getCurrentEnv().getBinlogManager().addReplaceTable(log, logId);
    }

    public void logBatchRemoveTransactions(BatchRemoveTransactionsOperationV2 op) {
        logEdit(OperationType.OP_BATCH_REMOVE_TXNS_V2, op);
    }

    public void logSetTableStatus(SetTableStatusOperationLog log) {
        logEdit(OperationType.OP_SET_TABLE_STATUS, log);
    }

    public void logModifyComment(ModifyCommentOperationLog op) {
        long logId = logEdit(OperationType.OP_MODIFY_COMMENT, op);
        LOG.info("log modify comment, logId : {}, infos: {}", logId, op);
        Env.getCurrentEnv().getBinlogManager().addModifyComment(op, logId);
    }

    public void logCreateSqlBlockRule(SqlBlockRule rule) {
        logEdit(OperationType.OP_CREATE_SQL_BLOCK_RULE, rule);
    }

    public void logAlterSqlBlockRule(SqlBlockRule rule) {
        logEdit(OperationType.OP_ALTER_SQL_BLOCK_RULE, rule);
    }

    public void logDropSqlBlockRule(List<String> ruleNames) {
        logEdit(OperationType.OP_DROP_SQL_BLOCK_RULE, new DropSqlBlockRuleOperationLog(ruleNames));
    }

    public void logModifyTableEngine(ModifyTableEngineOperationLog log) {
        logEdit(OperationType.OP_MODIFY_TABLE_ENGINE, log);
    }

    public void logCreatePolicy(Policy policy) {
        logEdit(OperationType.OP_CREATE_POLICY, policy);
    }

    public void logDropPolicy(DropPolicyLog log) {
        logEdit(OperationType.OP_DROP_POLICY, log);
    }

    public void logCreateIndexPolicy(IndexPolicy policy) {
        logEdit(OperationType.OP_CREATE_INDEX_POLICY, policy);
    }

    public void logDropIndexPolicy(DropIndexPolicyLog policy) {
        logEdit(OperationType.OP_DROP_INDEX_POLICY, policy);
    }

    public void logCatalogLog(short id, CatalogLog log) {
        logEdit(id, log);
    }

    public void logInitCatalog(InitCatalogLog log) {
        logEdit(OperationType.OP_INIT_CATALOG, log);
    }

    public void logRefreshExternalDb(ExternalObjectLog log) {
        logEdit(OperationType.OP_REFRESH_EXTERNAL_DB, log);
    }

    public void logInitExternalDb(InitDatabaseLog log) {
        logEdit(OperationType.OP_INIT_EXTERNAL_DB, log);
    }

    public void logRefreshExternalTable(ExternalObjectLog log) {
        logEdit(OperationType.OP_REFRESH_EXTERNAL_TABLE, log);
    }

    @Deprecated
    public void logDropExternalTable(ExternalObjectLog log) {
        logEdit(OperationType.OP_DROP_EXTERNAL_TABLE, log);
    }

    @Deprecated
    public void logCreateExternalTable(ExternalObjectLog log) {
        logEdit(OperationType.OP_CREATE_EXTERNAL_TABLE, log);
    }

    @Deprecated
    public void logDropExternalDatabase(ExternalObjectLog log) {
        logEdit(OperationType.OP_DROP_EXTERNAL_DB, log);
    }

    @Deprecated
    public void logCreateExternalDatabase(ExternalObjectLog log) {
        logEdit(OperationType.OP_CREATE_EXTERNAL_DB, log);
    }

    @Deprecated
    public void logAddExternalPartitions(ExternalObjectLog log) {
        logEdit(OperationType.OP_ADD_EXTERNAL_PARTITIONS, log);
    }

    @Deprecated
    public void logDropExternalPartitions(ExternalObjectLog log) {
        logEdit(OperationType.OP_DROP_EXTERNAL_PARTITIONS, log);
    }

    public void logInvalidateExternalPartitions(ExternalObjectLog log) {
        logEdit(OperationType.OP_REFRESH_EXTERNAL_PARTITIONS, log);
    }

    public Journal getJournal() {
        return this.journal;
    }

    public void logModifyTableAddOrDropColumns(TableAddOrDropColumnsInfo info) {
        long logId = logEdit(OperationType.OP_MODIFY_TABLE_LIGHT_SCHEMA_CHANGE, info);
        Env.getCurrentEnv().getBinlogManager().addModifyTableAddOrDropColumns(info, logId);
    }

    public void logModifyTableAddOrDropInvertedIndices(TableAddOrDropInvertedIndicesInfo info) {
        long logId = logEdit(OperationType.OP_MODIFY_TABLE_ADD_OR_DROP_INVERTED_INDICES, info);
        Env.getCurrentEnv().getBinlogManager().addModifyTableAddOrDropInvertedIndices(info, logId);
    }

    public void logIndexChangeJob(IndexChangeJob indexChangeJob) {
        long logId = logEdit(OperationType.OP_INVERTED_INDEX_JOB, indexChangeJob);
        Env.getCurrentEnv().getBinlogManager().addIndexChangeJob(indexChangeJob, logId);
    }

    public void logCleanLabel(CleanLabelOperationLog log) {
        logEdit(OperationType.OP_CLEAN_LABEL, log);
    }

    public void logAlterUser(AlterUserOperationLog log) {
        logEdit(OperationType.OP_ALTER_USER, log);
    }

    public void logCleanQueryStats(CleanQueryStatsInfo log) {
        logEdit(OperationType.OP_CLEAN_QUERY_STATS, log);
    }

    public void logCreateAnalysisTasks(AnalysisInfo log) {
        logEdit(OperationType.OP_CREATE_ANALYSIS_TASK, log);
    }

    public void logCreateAnalysisJob(AnalysisInfo log) {
        logEdit(OperationType.OP_CREATE_ANALYSIS_JOB, log);
    }

    public void logDeleteAnalysisJob(AnalyzeDeletionLog log) {
        logEdit(OperationType.OP_DELETE_ANALYSIS_JOB, log);
    }

    public long logAlterDatabaseProperty(AlterDatabasePropertyInfo log) {
        long logId = logEdit(OperationType.OP_ALTER_DATABASE_PROPERTY, log);
        Env.getCurrentEnv().getBinlogManager().addAlterDatabaseProperty(log, logId);
        return logId;
    }

    public long logGcBinlog(BinlogGcInfo log) {
        return logEdit(OperationType.OP_GC_BINLOG, log);
    }

    public long logBarrier(BarrierLog log) {
        long logId = logEdit(OperationType.OP_BARRIER, log);
        Env.getCurrentEnv().getBinlogManager().addBarrierLog(log, logId);
        LOG.info("logId {}, barrier {}", logId, log);
        return logId;
    }

    public void logUpdateAutoIncrementId(AutoIncrementIdUpdateLog log) {
        logEdit(OperationType.OP_UPDATE_AUTO_INCREMENT_ID, log);
    }

    public void logCreateTableStats(TableStatsMeta tableStats) {
        logEdit(OperationType.OP_UPDATE_TABLE_STATS, tableStats);
    }

    public void logUpdateRowsRecord(UpdateRowsEvent record) {
        logEdit(OperationType.OP_LOG_UPDATE_ROWS, record);
    }

    public void logNewPartitionLoadedEvent(NewPartitionLoadedEvent event) {
        logEdit(OperationType.OP_LOG_NEW_PARTITION_LOADED, event);
    }

    public void logDeleteTableStats(TableStatsDeletionLog log) {
        logEdit(OperationType.OP_DELETE_TABLE_STATS, log);
    }

    public void logAlterMTMV(AlterMTMV log) {
        logEdit(OperationType.OP_ALTER_MTMV, log);

    }

    public void logAddConstraint(AlterConstraintLog log) {
        logEdit(OperationType.OP_ADD_CONSTRAINT, log);
    }

    public void logDropConstraint(AlterConstraintLog log) {
        logEdit(OperationType.OP_DROP_CONSTRAINT, log);
    }

    public void logInsertOverwrite(InsertOverwriteLog log) {
        logEdit(OperationType.OP_INSERT_OVERWRITE, log);
    }

    public void logMetaIdMappingsLog(MetaIdMappingsLog log) {
        logEdit(OperationType.OP_ADD_META_ID_MAPPINGS, log);
    }

    public String getNotReadyReason() {
        if (journal == null) {
            return "journal is null";
        }
        if (journal instanceof BDBJEJournal) {
            return ((BDBJEJournal) journal).getNotReadyReason();
        }
        return "";
    }

    public boolean exceedMaxJournalSize(BackupJob job) {
        try {
            return exceedMaxJournalSize(OperationType.OP_BACKUP_JOB, job);
        } catch (Exception e) {
            LOG.warn("exceedMaxJournalSize exception:", e);
        }
        return true;
    }

    private boolean exceedMaxJournalSize(short op, Writable writable) throws IOException {
        return journal.exceedMaxJournalSize(op, writable);
    }

    public void logCreateDictionary(Dictionary dictionary) {
        logEdit(OperationType.OP_CREATE_DICTIONARY, new CreateDictionaryPersistInfo(dictionary));
    }

    public void logDropDictionary(String dbName, String dictionaryName) {
        logEdit(OperationType.OP_DROP_DICTIONARY, new DropDictionaryPersistInfo(dbName, dictionaryName));
    }

    public void logDictionaryIncVersion(Dictionary dictionary) {
        logEdit(OperationType.OP_DICTIONARY_INC_VERSION, new DictionaryIncreaseVersionInfo(dictionary));
    }

    public void logDictionaryDecVersion(Dictionary dictionary) {
        logEdit(OperationType.OP_DICTIONARY_DEC_VERSION, new DictionaryDecreaseVersionInfo(dictionary));
    }

    public void logBranchOrTag(TableBranchOrTagInfo info) {
        logEdit(OperationType.OP_BRANCH_OR_TAG, info);
    }

    public void logOperateKey(KeyOperationInfo info) {
        logEdit(OperationType.OP_OPERATE_KEY, info);
    }
}
