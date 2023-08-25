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
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.SmallFileMgr.SmallFile;
import org.apache.doris.cooldown.CooldownConfHandler;
import org.apache.doris.cooldown.CooldownConfList;
import org.apache.doris.cooldown.CooldownDelete;
import org.apache.doris.datasource.CatalogLog;
import org.apache.doris.datasource.ExternalObjectLog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.InitDatabaseLog;
import org.apache.doris.ha.MasterInfo;
import org.apache.doris.journal.Journal;
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
import org.apache.doris.load.LoadJob;
import org.apache.doris.load.StreamLoadRecordMgr.FetchStreamLoadRecord;
import org.apache.doris.load.loadv2.LoadJob.LoadJobStateUpdateInfo;
import org.apache.doris.load.loadv2.LoadJobFinalOperation;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.load.sync.SyncJob;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mtmv.metadata.ChangeMTMVJob;
import org.apache.doris.mtmv.metadata.DropMTMVJob;
import org.apache.doris.mtmv.metadata.DropMTMVTask;
import org.apache.doris.mtmv.metadata.MTMVJob;
import org.apache.doris.mtmv.metadata.MTMVTask;
import org.apache.doris.mysql.privilege.UserPropertyInfo;
import org.apache.doris.plugin.PluginInfo;
import org.apache.doris.policy.DropPolicyLog;
import org.apache.doris.policy.Policy;
import org.apache.doris.policy.StoragePolicy;
import org.apache.doris.resource.workloadgroup.WorkloadGroup;
import org.apache.doris.scheduler.job.Job;
import org.apache.doris.scheduler.job.JobTask;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.TableStats;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Frontend;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * EditLog maintains a log of the memory modifications.
 * Current we support only file editLog.
 */
public class EditLog {
    public static final Logger LOG = LogManager.getLogger(EditLog.class);

    private EditLogOutputStream editStream = null;

    private long txId = 0;

    private long numTransactions;
    private long totalTimeTransactions;

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
            LOG.debug("replay journal op code: {}", opCode);
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
                    env.replayCreateDb(db);
                    break;
                }
                case OperationType.OP_DROP_DB: {
                    DropDbInfo dropDbInfo = (DropDbInfo) journal.getData();
                    env.replayDropDb(dropDbInfo.getDbName(), dropDbInfo.isForceDrop(), dropDbInfo.getRecycleTime());
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
                    LOG.info("Begin to unprotect create table. db = " + info.getDbName() + " table = " + info.getTable()
                            .getId());
                    CreateTableRecord record = new CreateTableRecord(logId, info);
                    env.replayCreateTable(info.getDbName(), info.getTable());
                    env.getBinlogManager().addCreateTableRecord(record);
                    break;
                }
                case OperationType.OP_ALTER_EXTERNAL_TABLE_SCHEMA: {
                    RefreshExternalTableInfo info = (RefreshExternalTableInfo) journal.getData();
                    LOG.info("Begin to unprotect alter external table schema. db = " + info.getDbName() + " table = "
                            + info.getTableName());
                    env.replayAlterExternalTableSchema(info.getDbName(), info.getTableName(), info.getNewSchema());
                    break;
                }
                case OperationType.OP_DROP_TABLE: {
                    DropInfo info = (DropInfo) journal.getData();
                    Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(info.getDbId());
                    LOG.info("Begin to unprotect drop table. db = " + db.getFullName() + " table = "
                            + info.getTableId());
                    DropTableRecord record = new DropTableRecord(logId, info);
                    env.replayDropTable(db, info.getTableId(), info.isForceDrop(), info.getRecycleTime());
                    env.getBinlogManager().addDropTableRecord(record);
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
                    env.getAlterInstance().replayModifyPartition(info);
                    break;
                }
                case OperationType.OP_BATCH_MODIFY_PARTITION: {
                    BatchModifyPartitionsInfo info = (BatchModifyPartitionsInfo) journal.getData();
                    for (ModifyPartitionInfo modifyPartitionInfo : info.getModifyPartitionInfos()) {
                        env.getAlterInstance().replayModifyPartition(modifyPartitionInfo);
                    }
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
                    break;
                }
                case OperationType.OP_RECOVER_PARTITION: {
                    RecoverInfo info = (RecoverInfo) journal.getData();
                    env.replayRecoverPartition(info);
                    break;
                }
                case OperationType.OP_RENAME_TABLE: {
                    TableInfo info = (TableInfo) journal.getData();
                    env.replayRenameTable(info);
                    break;
                }
                case OperationType.OP_MODIFY_VIEW_DEF: {
                    AlterViewInfo info = (AlterViewInfo) journal.getData();
                    env.getAlterInstance().replayModifyViewDef(info);
                    break;
                }
                case OperationType.OP_RENAME_PARTITION: {
                    TableInfo info = (TableInfo) journal.getData();
                    env.replayRenamePartition(info);
                    break;
                }
                case OperationType.OP_RENAME_COLUMN: {
                    TableRenameColumnInfo info = (TableRenameColumnInfo) journal.getData();
                    env.replayRenameColumn(info);
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
                    break;
                }
                case OperationType.OP_BATCH_DROP_ROLLUP: {
                    BatchDropInfo batchDropInfo = (BatchDropInfo) journal.getData();
                    for (long indexId : batchDropInfo.getIndexIdSet()) {
                        env.getMaterializedViewHandler().replayDropRollup(
                                new DropInfo(batchDropInfo.getDbId(), batchDropInfo.getTableId(),
                                        batchDropInfo.getTableName(), indexId, false, 0),
                                env);
                    }
                    break;
                }
                case OperationType.OP_FINISH_CONSISTENCY_CHECK: {
                    ConsistencyCheckInfo info = (ConsistencyCheckInfo) journal.getData();
                    env.getConsistencyChecker().replayFinishConsistencyCheck(info, env);
                    break;
                }
                case OperationType.OP_CLEAR_ROLLUP_INFO: {
                    ReplicaPersistInfo info = (ReplicaPersistInfo) journal.getData();
                    env.getLoadInstance().replayClearRollupInfo(info, env);
                    break;
                }
                case OperationType.OP_RENAME_ROLLUP: {
                    TableInfo info = (TableInfo) journal.getData();
                    env.replayRenameRollup(info);
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
                        System.out.println("current fe " + fe + " is removed. will exit");
                        LOG.info("current fe " + fe + " is removed. will exit");
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
                    LOG.debug("logid: {}, opcode: {}, tid: {}", logId, opCode, state.getTransactionId());

                    if (state.getTransactionStatus() == TransactionStatus.VISIBLE) {
                        UpsertRecord upsertRecord = new UpsertRecord(logId, state);
                        Env.getCurrentEnv().getBinlogManager().addUpsertRecord(upsertRecord);
                    }
                    break;
                }
                case OperationType.OP_DELETE_TRANSACTION_STATE: {
                    final TransactionState state = (TransactionState) journal.getData();
                    Env.getCurrentGlobalTransactionMgr().replayDeleteTransactionState(state);
                    LOG.debug("opcode: {}, tid: {}", opCode, state.getTransactionId());
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
                    Job job = (Job) journal.getData();
                    Env.getCurrentEnv().getAsyncJobManager().replayCreateJob(job);
                    break;
                }
                case OperationType.OP_UPDATE_SCHEDULER_JOB: {
                    Job job = (Job) journal.getData();
                    Env.getCurrentEnv().getAsyncJobManager().replayUpdateJob(job);
                    break;
                }
                case OperationType.OP_DELETE_SCHEDULER_JOB: {
                    Job job = (Job) journal.getData();
                    Env.getCurrentEnv().getAsyncJobManager().replayDeleteJob(job);
                    break;
                }
                case OperationType.OP_CREATE_SCHEDULER_TASK: {
                    JobTask task = (JobTask) journal.getData();
                    Env.getCurrentEnv().getJobTaskManager().replayCreateTask(task);
                    break;
                }
                case OperationType.OP_DELETE_SCHEDULER_TASK: {
                    JobTask task = (JobTask) journal.getData();
                    Env.getCurrentEnv().getJobTaskManager().replayDeleteTask(task);
                    break;
                }
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
                    SyncJob syncJob = (SyncJob) journal.getData();
                    env.getSyncJobManager().replayAddSyncJob(syncJob);
                    break;
                }
                case OperationType.OP_UPDATE_SYNC_JOB_STATE: {
                    SyncJob.SyncJobUpdateStateInfo info = (SyncJob.SyncJobUpdateStateInfo) journal.getData();
                    env.getSyncJobManager().replayUpdateSyncJobState(info);
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
                    break;
                }
                case OperationType.OP_DYNAMIC_PARTITION:
                case OperationType.OP_MODIFY_IN_MEMORY:
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
                    break;
                }
                case OperationType.OP_REPLACE_TEMP_PARTITION: {
                    ReplacePartitionOperationLog replaceTempPartitionLog =
                            (ReplacePartitionOperationLog) journal.getData();
                    env.replayReplaceTempPartition(replaceTempPartitionLog);
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
                case OperationType.OP_ALTER_CATALOG_PROPS: {
                    CatalogLog log = (CatalogLog) journal.getData();
                    env.getCatalogMgr().replayAlterCatalogProps(log, null, true);
                    break;
                }
                case OperationType.OP_REFRESH_CATALOG: {
                    CatalogLog log = (CatalogLog) journal.getData();
                    env.getCatalogMgr().replayRefreshCatalog(log);
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
                    break;
                }
                case OperationType.OP_INVERTED_INDEX_JOB: {
                    IndexChangeJob indexChangeJob = (IndexChangeJob) journal.getData();
                    env.getSchemaChangeHandler().replayIndexChangeJob(indexChangeJob);
                    break;
                }
                case OperationType.OP_CLEAN_LABEL: {
                    final CleanLabelOperationLog log = (CleanLabelOperationLog) journal.getData();
                    env.getLoadManager().replayCleanLabel(log);
                    break;
                }
                case OperationType.OP_CREATE_MTMV_JOB: {
                    final MTMVJob job = (MTMVJob) journal.getData();
                    env.getMTMVJobManager().replayCreateJob(job);
                    break;
                }
                case OperationType.OP_CHANGE_MTMV_JOB: {
                    final ChangeMTMVJob changeJob = (ChangeMTMVJob) journal.getData();
                    env.getMTMVJobManager().replayUpdateJob(changeJob);
                    break;
                }
                case OperationType.OP_DROP_MTMV_JOB: {
                    final DropMTMVJob dropJob = (DropMTMVJob) journal.getData();
                    env.getMTMVJobManager().replayDropJobs(dropJob.getJobIds());
                    break;
                }
                case OperationType.OP_CREATE_MTMV_TASK: {
                    final MTMVTask task = (MTMVTask) journal.getData();
                    env.getMTMVJobManager().replayCreateJobTask(task);
                    break;
                }
                case OperationType.OP_CHANGE_MTMV_TASK: {
                    break;
                }
                case OperationType.OP_DROP_MTMV_TASK: {
                    final DropMTMVTask dropTask = (DropMTMVTask) journal.getData();
                    env.getMTMVJobManager().replayDropJobTasks(dropTask.getTaskIds());
                    break;
                }
                case OperationType.OP_ALTER_MTMV_STMT: {
                    final AlterMultiMaterializedView alterView = (AlterMultiMaterializedView) journal.getData();
                    env.getAlterInstance().processAlterMaterializedView(alterView, true);
                    break;
                }
                case OperationType.OP_ALTER_USER: {
                    final AlterUserOperationLog log = (AlterUserOperationLog) journal.getData();
                    env.getAuth().replayAlterUser(log);
                    break;
                }
                case OperationType.OP_INIT_CATALOG: {
                    final InitCatalogLog log = (InitCatalogLog) journal.getData();
                    env.getCatalogMgr().replayInitCatalog(log);
                    break;
                }
                case OperationType.OP_REFRESH_EXTERNAL_DB: {
                    final ExternalObjectLog log = (ExternalObjectLog) journal.getData();
                    env.getCatalogMgr().replayRefreshExternalDb(log);
                    break;
                }
                case OperationType.OP_INIT_EXTERNAL_DB: {
                    final InitDatabaseLog log = (InitDatabaseLog) journal.getData();
                    env.getCatalogMgr().replayInitExternalDb(log);
                    break;
                }
                case OperationType.OP_REFRESH_EXTERNAL_TABLE: {
                    final ExternalObjectLog log = (ExternalObjectLog) journal.getData();
                    env.getCatalogMgr().replayRefreshExternalTable(log);
                    break;
                }
                case OperationType.OP_DROP_EXTERNAL_TABLE: {
                    final ExternalObjectLog log = (ExternalObjectLog) journal.getData();
                    env.getCatalogMgr().replayDropExternalTable(log);
                    break;
                }
                case OperationType.OP_CREATE_EXTERNAL_TABLE: {
                    final ExternalObjectLog log = (ExternalObjectLog) journal.getData();
                    env.getCatalogMgr().replayCreateExternalTableFromEvent(log);
                    break;
                }
                case OperationType.OP_DROP_EXTERNAL_DB: {
                    final ExternalObjectLog log = (ExternalObjectLog) journal.getData();
                    env.getCatalogMgr().replayDropExternalDatabase(log);
                    break;
                }
                case OperationType.OP_CREATE_EXTERNAL_DB: {
                    final ExternalObjectLog log = (ExternalObjectLog) journal.getData();
                    env.getCatalogMgr().replayCreateExternalDatabase(log);
                    break;
                }
                case OperationType.OP_ADD_EXTERNAL_PARTITIONS: {
                    final ExternalObjectLog log = (ExternalObjectLog) journal.getData();
                    env.getCatalogMgr().replayAddExternalPartitions(log);
                    break;
                }
                case OperationType.OP_DROP_EXTERNAL_PARTITIONS: {
                    final ExternalObjectLog log = (ExternalObjectLog) journal.getData();
                    env.getCatalogMgr().replayDropExternalPartitions(log);
                    break;
                }
                case OperationType.OP_REFRESH_EXTERNAL_PARTITIONS: {
                    final ExternalObjectLog log = (ExternalObjectLog) journal.getData();
                    env.getCatalogMgr().replayRefreshExternalPartitions(log);
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
                case OperationType.OP_INIT_EXTERNAL_TABLE: {
                    // Do nothing.
                    break;
                }
                case OperationType.OP_CREATE_ANALYSIS_JOB: {
                    env.getAnalysisManager().replayCreateAnalysisJob((AnalysisInfo) journal.getData());
                    break;
                }
                case OperationType.OP_CREATE_ANALYSIS_TASK: {
                    env.getAnalysisManager().replayCreateAnalysisTask((AnalysisInfo) journal.getData());
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
                    env.getAnalysisManager().replayUpdateTableStatsStatus((TableStats) journal.getData());
                    break;
                }
                default: {
                    IOException e = new IOException();
                    LOG.error("UNKNOWN Operation Type {}", opCode, e);
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
            LOG.warn("[INCONSISTENT META] replay failed {}: {}", journal, e.getMessage(), e);
        } catch (Exception e) {
            LOG.error("Operation Type {}", opCode, e);
            System.exit(-1);
        }
    }

    /**
     * Shutdown the file store.
     */
    public synchronized void close() throws IOException {
        journal.close();
    }

    public synchronized void createEditLogFile(File name) throws IOException {
        EditLogOutputStream editLogOutputStream = new EditLogFileOutputStream(name);
        editLogOutputStream.create();
        editLogOutputStream.close();
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

    /**
     * Write an operation to the edit log. Do not sync to persistent store yet.
     */
    private synchronized long logEdit(short op, Writable writable) {
        if (this.getNumEditStreams() == 0) {
            LOG.error("Fatal Error : no editLog stream", new Exception());
            throw new Error("Fatal Error : no editLog stream");
        }

        long start = System.currentTimeMillis();
        long logId = -1;
        try {
            logId = journal.write(op, writable);
        } catch (Throwable t) {
            // Throwable contains all Exception and Error, such as IOException and
            // OutOfMemoryError
            LOG.error("Fatal Error : write stream Exception", t);
            System.exit(-1);
        }

        // get a new transactionId
        txId++;

        // update statistics
        long end = System.currentTimeMillis();
        numTransactions++;
        totalTimeTransactions += (end - start);
        if (MetricRepo.isInit) {
            MetricRepo.HISTO_EDIT_LOG_WRITE_LATENCY.update((end - start));
            MetricRepo.COUNTER_EDIT_LOG_CURRENT.increase(1L);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("nextId = {}, numTransactions = {}, totalTimeTransactions = {}, op = {}", txId, numTransactions,
                    totalTimeTransactions, op);
        }

        if (txId >= Config.edit_log_roll_num) {
            LOG.info("txId {} is equal to or larger than edit_log_roll_num {}, will roll edit.", txId,
                    Config.edit_log_roll_num);
            rollEditLog();
            txId = 0;
        }

        if (MetricRepo.isInit) {
            MetricRepo.COUNTER_EDIT_LOG_WRITE.increase(1L);
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

    public synchronized long getTxId() {
        return txId;
    }

    public void logSaveNextId(long nextId) {
        logEdit(OperationType.OP_SAVE_NEXTID, new Text(Long.toString(nextId)));
    }

    public void logSaveTransactionId(long transactionId) {
        logEdit(OperationType.OP_SAVE_TRANSACTION_ID, new Text(Long.toString(transactionId)));
    }

    public void logCreateDb(Database db) {
        logEdit(OperationType.OP_CREATE_DB, db);
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
        CreateTableRecord record = new CreateTableRecord(logId, info);
        Env.getCurrentEnv().getBinlogManager().addCreateTableRecord(record);
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
        logEdit(OperationType.OP_RECOVER_PARTITION, info);
    }

    public void logModifyPartition(ModifyPartitionInfo info) {
        logEdit(OperationType.OP_MODIFY_PARTITION, info);
    }

    public void logBatchModifyPartition(BatchModifyPartitionsInfo info) {
        logEdit(OperationType.OP_BATCH_MODIFY_PARTITION, info);
    }

    public void logDropTable(DropInfo info) {
        long logId = logEdit(OperationType.OP_DROP_TABLE, info);
        DropTableRecord record = new DropTableRecord(logId, info);
        Env.getCurrentEnv().getBinlogManager().addDropTableRecord(record);
    }

    public void logEraseTable(long tableId) {
        logEdit(OperationType.OP_ERASE_TABLE, new Text(Long.toString(tableId)));
    }

    public void logRecoverTable(RecoverInfo info) {
        logEdit(OperationType.OP_RECOVER_TABLE, info);
    }

    public void logLoadStart(LoadJob job) {
        logEdit(OperationType.OP_LOAD_START, job);
    }

    public void logLoadEtl(LoadJob job) {
        logEdit(OperationType.OP_LOAD_ETL, job);
    }

    public void logLoadLoading(LoadJob job) {
        logEdit(OperationType.OP_LOAD_LOADING, job);
    }

    public void logLoadQuorum(LoadJob job) {
        logEdit(OperationType.OP_LOAD_QUORUM, job);
    }

    public void logLoadCancel(LoadJob job) {
        logEdit(OperationType.OP_LOAD_CANCEL, job);
    }

    public void logLoadDone(LoadJob job) {
        logEdit(OperationType.OP_LOAD_DONE, job);
    }

    public void logDropRollup(DropInfo info) {
        logEdit(OperationType.OP_DROP_ROLLUP, info);
    }

    public void logBatchDropRollup(BatchDropInfo batchDropInfo) {
        logEdit(OperationType.OP_BATCH_DROP_ROLLUP, batchDropInfo);
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

    public void logDropRole(PrivInfo info) {
        logEdit(OperationType.OP_DROP_ROLE, info);
    }

    public void logDatabaseRename(DatabaseInfo databaseInfo) {
        logEdit(OperationType.OP_RENAME_DB, databaseInfo);
    }

    public void logTableRename(TableInfo tableInfo) {
        logEdit(OperationType.OP_RENAME_TABLE, tableInfo);
    }

    public void logModifyViewDef(AlterViewInfo alterViewInfo) {
        logEdit(OperationType.OP_MODIFY_VIEW_DEF, alterViewInfo);
    }

    public void logRollupRename(TableInfo tableInfo) {
        logEdit(OperationType.OP_RENAME_ROLLUP, tableInfo);
    }

    public void logPartitionRename(TableInfo tableInfo) {
        logEdit(OperationType.OP_RENAME_PARTITION, tableInfo);
    }

    public void logColumnRename(TableRenameColumnInfo info) {
        logEdit(OperationType.OP_RENAME_COLUMN, info);
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

    public void logExportUpdateState(long jobId, ExportJobState newState) {
        ExportJobStateTransfer transfer = new ExportJobStateTransfer(jobId, newState);
        logEdit(OperationType.OP_EXPORT_UPDATE_STATE, transfer);
    }

    // for TransactionState
    public void logInsertTransactionState(TransactionState transactionState) {
        long logId = logEdit(OperationType.OP_UPSERT_TRANSACTION_STATE, transactionState);
        if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
            UpsertRecord record = new UpsertRecord(logId, transactionState);
            Env.getCurrentEnv().getBinlogManager().addUpsertRecord(record);
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

    public void logRestoreJob(RestoreJob job) {
        logEdit(OperationType.OP_RESTORE_JOB, job);
    }

    public void logUpdateUserProperty(UserPropertyInfo propertyInfo) {
        logEdit(OperationType.OP_UPDATE_USER_PROPERTY, propertyInfo);
    }

    public void logTruncateTable(TruncateTableInfo info) {
        logEdit(OperationType.OP_TRUNCATE_TABLE, info);
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

    public void logCreateJob(Job job) {
        logEdit(OperationType.OP_CREATE_SCHEDULER_JOB, job);
    }

    public void logUpdateJob(Job job) {
        logEdit(OperationType.OP_UPDATE_SCHEDULER_JOB, job);
    }

    public void logCreateJobTask(JobTask jobTask) {
        logEdit(OperationType.OP_CREATE_SCHEDULER_TASK, jobTask);
    }

    public void logDeleteJobTask(JobTask jobTask) {
        logEdit(OperationType.OP_DELETE_SCHEDULER_TASK, jobTask);
    }

    public void logDeleteJob(Job job) {
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

    public void logCreateSyncJob(SyncJob syncJob) {
        logEdit(OperationType.OP_CREATE_SYNC_JOB, syncJob);
    }

    public void logUpdateSyncJobState(SyncJob.SyncJobUpdateStateInfo info) {
        logEdit(OperationType.OP_UPDATE_SYNC_JOB_STATE, info);
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
        logEdit(OperationType.OP_MODIFY_DISTRIBUTION_TYPE, tableInfo);
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

    public void logModifyDefaultDistributionBucketNum(ModifyTableDefaultDistributionBucketNumOperationLog info) {
        logEdit(OperationType.OP_MODIFY_DISTRIBUTION_BUCKET_NUM, info);
    }

    public long logModifyInMemory(ModifyTablePropertyOperationLog info) {
        return logModifyTableProperty(OperationType.OP_MODIFY_IN_MEMORY, info);
    }

    public long logUpdateBinlogConfig(ModifyTablePropertyOperationLog info) {
        return logModifyTableProperty(OperationType.OP_UPDATE_BINLOG_CONFIG, info);
    }

    public void logAlterLightSchemaChange(AlterLightSchemaChangeInfo info) {
        logEdit(OperationType.OP_ALTER_LIGHT_SCHEMA_CHANGE, info);
    }

    public void logReplaceTempPartition(ReplacePartitionOperationLog info) {
        logEdit(OperationType.OP_REPLACE_TEMP_PARTITION, info);
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

    public void logRemoveExpiredAlterJobV2(RemoveAlterJobV2OperationLog log) {
        logEdit(OperationType.OP_REMOVE_ALTER_JOB_V2, log);
    }

    public void logAlterRoutineLoadJob(AlterRoutineLoadJobOperationLog log) {
        logEdit(OperationType.OP_ALTER_ROUTINE_LOAD_JOB, log);
    }

    public void logGlobalVariableV2(GlobalVarPersistInfo info) {
        logEdit(OperationType.OP_GLOBAL_VARIABLE_V2, info);
    }

    public void logReplaceTable(ReplaceTableOperationLog log) {
        logEdit(OperationType.OP_REPLACE_TABLE, log);
    }

    public void logBatchRemoveTransactions(BatchRemoveTransactionsOperationV2 op) {
        logEdit(OperationType.OP_BATCH_REMOVE_TXNS_V2, op);
    }

    public void logModifyComment(ModifyCommentOperationLog op) {
        logEdit(OperationType.OP_MODIFY_COMMENT, op);
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

    public void logCatalogLog(short id, CatalogLog log) {
        logEdit(id, log);
    }

    public void logCreateMTMVJob(MTMVJob job) {
        logEdit(OperationType.OP_CREATE_MTMV_JOB, job);
    }

    public void logDropMTMVJob(List<Long> jobIds) {
        logEdit(OperationType.OP_DROP_MTMV_JOB, new DropMTMVJob(jobIds));
    }

    public void logChangeMTMVJob(ChangeMTMVJob changeJob) {
        logEdit(OperationType.OP_CHANGE_MTMV_JOB, changeJob);
    }

    public void logCreateMTMVTask(MTMVTask task) {
        logEdit(OperationType.OP_CREATE_MTMV_TASK, task);
    }

    public void logDropMTMVTasks(List<String> taskIds) {
        logEdit(OperationType.OP_DROP_MTMV_TASK, new DropMTMVTask(taskIds));
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

    public void logDropExternalTable(ExternalObjectLog log) {
        logEdit(OperationType.OP_DROP_EXTERNAL_TABLE, log);
    }

    public void logCreateExternalTable(ExternalObjectLog log) {
        logEdit(OperationType.OP_CREATE_EXTERNAL_TABLE, log);
    }

    public void logDropExternalDatabase(ExternalObjectLog log) {
        logEdit(OperationType.OP_DROP_EXTERNAL_DB, log);
    }

    public void logCreateExternalDatabase(ExternalObjectLog log) {
        logEdit(OperationType.OP_CREATE_EXTERNAL_DB, log);
    }

    public void logAddExternalPartitions(ExternalObjectLog log) {
        logEdit(OperationType.OP_ADD_EXTERNAL_PARTITIONS, log);
    }

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
        logEdit(OperationType.OP_MODIFY_TABLE_ADD_OR_DROP_INVERTED_INDICES, info);
    }

    public void logIndexChangeJob(IndexChangeJob indexChangeJob) {
        logEdit(OperationType.OP_INVERTED_INDEX_JOB, indexChangeJob);
    }

    public void logCleanLabel(CleanLabelOperationLog log) {
        logEdit(OperationType.OP_CLEAN_LABEL, log);
    }

    public void logAlterUser(AlterUserOperationLog log) {
        logEdit(OperationType.OP_ALTER_USER, log);
    }

    public void logAlterMTMV(AlterMultiMaterializedView log) {
        logEdit(OperationType.OP_ALTER_MTMV_STMT, log);
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

    public void logDeleteAnalysisTask(AnalyzeDeletionLog log) {
        logEdit(OperationType.OP_DELETE_ANALYSIS_TASK, log);
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

    public void logCreateTableStats(TableStats tableStats) {
        logEdit(OperationType.OP_UPDATE_TABLE_STATS, tableStats);
    }
}
