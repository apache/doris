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
import org.apache.doris.alter.DecommissionBackendJob;
import org.apache.doris.alter.RollupJob;
import org.apache.doris.alter.SchemaChangeJob;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.backup.BackupJob;
import org.apache.doris.backup.Repository;
import org.apache.doris.backup.RestoreJob;
import org.apache.doris.catalog.BrokerMgr;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionSearchDesc;
import org.apache.doris.catalog.Resource;
import org.apache.doris.cluster.BaseParam;
import org.apache.doris.cluster.Cluster;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.SmallFileMgr.SmallFile;
import org.apache.doris.ha.MasterInfo;
import org.apache.doris.journal.Journal;
import org.apache.doris.journal.JournalCursor;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.journal.bdbje.BDBJEJournal;
import org.apache.doris.journal.bdbje.Timestamp;
import org.apache.doris.load.DeleteHandler;
import org.apache.doris.load.DeleteInfo;
import org.apache.doris.load.ExportJob;
import org.apache.doris.load.ExportMgr;
import org.apache.doris.load.Load;
import org.apache.doris.load.LoadErrorHub;
import org.apache.doris.load.LoadJob;
import org.apache.doris.load.StreamLoadRecordMgr.FetchStreamLoadRecord;
import org.apache.doris.load.loadv2.LoadJob.LoadJobStateUpdateInfo;
import org.apache.doris.load.loadv2.LoadJobFinalOperation;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.privilege.UserPropertyInfo;
import org.apache.doris.plugin.PluginInfo;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Frontend;
import org.apache.doris.transaction.TransactionState;

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

    public EditLog(String nodeName) {
        journal = new BDBJEJournal(nodeName);
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

    public static void loadJournal(Catalog catalog, JournalEntity journal) {
        short opCode = journal.getOpCode();
        if (opCode != OperationType.OP_SAVE_NEXTID && opCode != OperationType.OP_TIMESTAMP) {
            LOG.debug("replay journal op code: {}", opCode);
        }
        try {
            switch (opCode) {
                case OperationType.OP_SAVE_NEXTID: {
                    String idString = ((Text) journal.getData()).toString();
                    long id = Long.parseLong(idString);
                    catalog.setNextId(id + 1);
                    break;
                }
                case OperationType.OP_SAVE_TRANSACTION_ID: {
                    String idString = ((Text) journal.getData()).toString();
                    long id = Long.parseLong(idString);
                    Catalog.getCurrentGlobalTransactionMgr().getTransactionIDGenerator().initTransactionId(id + 1);
                    break;
                }
                case OperationType.OP_CREATE_DB: {
                    Database db = (Database) journal.getData();
                    catalog.replayCreateDb(db);
                    break;
                }
                case OperationType.OP_DROP_DB: {
                    DropDbInfo dropDbInfo = (DropDbInfo) journal.getData();
                    catalog.replayDropDb(dropDbInfo.getDbName(), dropDbInfo.isForceDrop());
                    break;
                }
                case OperationType.OP_ALTER_DB: {
                    DatabaseInfo dbInfo = (DatabaseInfo) journal.getData();
                    String dbName = dbInfo.getDbName();
                    LOG.info("Begin to unprotect alter db info {}", dbName);
                    catalog.replayAlterDatabaseQuota(dbName, dbInfo.getQuota(), dbInfo.getQuotaType());
                    break;
                }
                case OperationType.OP_ERASE_DB: {
                    Text dbId = (Text) journal.getData();
                    catalog.replayEraseDatabase(Long.parseLong(dbId.toString()));
                    break;
                }
                case OperationType.OP_RECOVER_DB: {
                    RecoverInfo info = (RecoverInfo) journal.getData();
                    catalog.replayRecoverDatabase(info);
                    break;
                }
                case OperationType.OP_RENAME_DB: {
                    DatabaseInfo dbInfo = (DatabaseInfo) journal.getData();
                    String dbName = dbInfo.getDbName();
                    LOG.info("Begin to unprotect rename db {}", dbName);
                    catalog.replayRenameDatabase(dbName, dbInfo.getNewDbName());
                    break;
                }
                case OperationType.OP_CREATE_TABLE: {
                    CreateTableInfo info = (CreateTableInfo) journal.getData();
                    LOG.info("Begin to unprotect create table. db = "
                            + info.getDbName() + " table = " + info.getTable().getId());
                    catalog.replayCreateTable(info.getDbName(), info.getTable());
                    break;
                }
                case OperationType.OP_ALTER_EXTERNAL_TABLE_SCHEMA: {
                    RefreshExternalTableInfo info = (RefreshExternalTableInfo) journal.getData();
                    LOG.info("Begin to unprotect alter external table schema. db = "
                            + info.getDbName() + " table = " + info.getTableName());
                    catalog.replayAlterExternalTableSchema(info.getDbName(), info.getTableName(), info.getNewSchema());
                    break;
                }
                case OperationType.OP_DROP_TABLE: {
                    DropInfo info = (DropInfo) journal.getData();
                    Database db = catalog.getDb(info.getDbId());
                    if (db == null) {
                        LOG.warn("failed to get db[{}]", info.getDbId());
                        break;
                    }
                    LOG.info("Begin to unprotect drop table. db = "
                            + db.getFullName() + " table = " + info.getTableId());
                    catalog.replayDropTable(db, info.getTableId(), info.isForceDrop());
                    break;
                }
                case OperationType.OP_ADD_PARTITION: {
                    PartitionPersistInfo info = (PartitionPersistInfo) journal.getData();
                    LOG.info("Begin to unprotect add partition. db = " + info.getDbId()
                            + " table = " + info.getTableId()
                            + " partitionName = " + info.getPartition().getName());
                    catalog.replayAddPartition(info);
                    break;
                }
                case OperationType.OP_DROP_PARTITION: {
                    DropPartitionInfo info = (DropPartitionInfo) journal.getData();
                    LOG.info("Begin to unprotect drop partition. db = " + info.getDbId()
                            + " table = " + info.getTableId()
                            + " partitionName = " + info.getPartitionName());
                    catalog.replayDropPartition(info);
                    break;
                }
                case OperationType.OP_MODIFY_PARTITION: {
                    ModifyPartitionInfo info = (ModifyPartitionInfo) journal.getData();
                    LOG.info("Begin to unprotect modify partition. db = " + info.getDbId()
                            + " table = " + info.getTableId() + " partitionId = " + info.getPartitionId());
                    catalog.getAlterInstance().replayModifyPartition(info);
                    break;
                }
                case OperationType.OP_BATCH_MODIFY_PARTITION: {
                    BatchModifyPartitionsInfo info = (BatchModifyPartitionsInfo) journal.getData();
                    for(ModifyPartitionInfo modifyPartitionInfo : info.getModifyPartitionInfos()) {
                        catalog.getAlterInstance().replayModifyPartition(modifyPartitionInfo);
                    }
                    break;
                }
                case OperationType.OP_ERASE_TABLE: {
                    Text tableId = (Text) journal.getData();
                    catalog.replayEraseTable(Long.parseLong(tableId.toString()));
                    break;
                }
                case OperationType.OP_ERASE_PARTITION: {
                    Text partitionId = (Text) journal.getData();
                    catalog.replayErasePartition(Long.parseLong(partitionId.toString()));
                    break;
                }
                case OperationType.OP_RECOVER_TABLE: {
                    RecoverInfo info = (RecoverInfo) journal.getData();
                    catalog.replayRecoverTable(info);
                    break;
                }
                case OperationType.OP_RECOVER_PARTITION: {
                    RecoverInfo info = (RecoverInfo) journal.getData();
                    catalog.replayRecoverPartition(info);
                    break;
                }
                case OperationType.OP_RENAME_TABLE: {
                    TableInfo info = (TableInfo) journal.getData();
                    catalog.replayRenameTable(info);
                    break;
                }
                case OperationType.OP_MODIFY_VIEW_DEF: {
                    AlterViewInfo info = (AlterViewInfo) journal.getData();
                    catalog.getAlterInstance().replayModifyViewDef(info);
                    break;
                }
                case OperationType.OP_RENAME_PARTITION: {
                    TableInfo info = (TableInfo) journal.getData();
                    catalog.replayRenamePartition(info);
                    break;
                }
                case OperationType.OP_BACKUP_JOB: {
                    BackupJob job = (BackupJob) journal.getData();
                    catalog.getBackupHandler().replayAddJob(job);
                    break;
                }
                case OperationType.OP_RESTORE_JOB: {
                    RestoreJob job = (RestoreJob) journal.getData();
                    job.setCatalog(catalog);
                    catalog.getBackupHandler().replayAddJob(job);
                    break;
                }
                case OperationType.OP_START_ROLLUP: {
                    RollupJob job = (RollupJob) journal.getData();
                    catalog.getRollupHandler().replayInitJob(job, catalog);
                    break;
                }
                case OperationType.OP_FINISHING_ROLLUP: {
                    RollupJob job = (RollupJob) journal.getData();
                    catalog.getRollupHandler().replayFinishing(job, catalog);
                    break;
                }
                case OperationType.OP_FINISH_ROLLUP: {
                    RollupJob job = (RollupJob) journal.getData();
                    catalog.getRollupHandler().replayFinish(job, catalog);
                    break;
                }
                case OperationType.OP_CANCEL_ROLLUP: {
                    RollupJob job = (RollupJob) journal.getData();
                    catalog.getRollupHandler().replayCancel(job, catalog);
                    break;
                }
                case OperationType.OP_DROP_ROLLUP: {
                    DropInfo info = (DropInfo) journal.getData();
                    catalog.getRollupHandler().replayDropRollup(info, catalog);
                    break;
                }
                case OperationType.OP_BATCH_DROP_ROLLUP: {
                    BatchDropInfo batchDropInfo = (BatchDropInfo) journal.getData();
                    for (long indexId : batchDropInfo.getIndexIdSet()) {
                        catalog.getRollupHandler().replayDropRollup(
                                new DropInfo(batchDropInfo.getDbId(), batchDropInfo.getTableId(), indexId, false), catalog);
                    }
                    break;
                }
                case OperationType.OP_START_SCHEMA_CHANGE: {
                    SchemaChangeJob job = (SchemaChangeJob) journal.getData();
                    LOG.info("Begin to unprotect create schema change job. db = " + job.getDbId()
                            + " table = " + job.getTableId());
                    catalog.getSchemaChangeHandler().replayInitJob(job, catalog);
                    break;
                }
                case OperationType.OP_FINISHING_SCHEMA_CHANGE: {
                    SchemaChangeJob job = (SchemaChangeJob) journal.getData();
                    LOG.info("Begin to unprotect replay finishing schema change job. db = " + job.getDbId()
                            + " table = " + job.getTableId());
                    catalog.getSchemaChangeHandler().replayFinishing(job, catalog);
                    break;
                }
                case OperationType.OP_FINISH_SCHEMA_CHANGE: {
                    SchemaChangeJob job = (SchemaChangeJob) journal.getData();
                    catalog.getSchemaChangeHandler().replayFinish(job, catalog);
                    break;
                }
                case OperationType.OP_CANCEL_SCHEMA_CHANGE: {
                    SchemaChangeJob job = (SchemaChangeJob) journal.getData();
                    LOG.debug("Begin to unprotect cancel schema change. db = " + job.getDbId()
                            + " table = " + job.getTableId());
                    catalog.getSchemaChangeHandler().replayCancel(job, catalog);
                    break;
                }
                case OperationType.OP_FINISH_CONSISTENCY_CHECK: {
                    ConsistencyCheckInfo info = (ConsistencyCheckInfo) journal.getData();
                    catalog.getConsistencyChecker().replayFinishConsistencyCheck(info, catalog);
                    break;
                }
                case OperationType.OP_CLEAR_ROLLUP_INFO: {
                    ReplicaPersistInfo info = (ReplicaPersistInfo) journal.getData();
                    catalog.getLoadInstance().replayClearRollupInfo(info, catalog);
                    break;
                }
                case OperationType.OP_RENAME_ROLLUP: {
                    TableInfo info = (TableInfo) journal.getData();
                    catalog.replayRenameRollup(info);
                    break;
                }
                case OperationType.OP_LOAD_START: {
                    LoadJob job = (LoadJob) journal.getData();
                    catalog.getLoadInstance().replayAddLoadJob(job);
                    break;
                }
                case OperationType.OP_LOAD_ETL: {
                    LoadJob job = (LoadJob) journal.getData();
                    catalog.getLoadInstance().replayEtlLoadJob(job);
                    break;
                }
                case OperationType.OP_LOAD_LOADING: {
                    LoadJob job = (LoadJob) journal.getData();
                    catalog.getLoadInstance().replayLoadingLoadJob(job);
                    break;
                }
                case OperationType.OP_LOAD_QUORUM: {
                    LoadJob job = (LoadJob) journal.getData();
                    Load load = catalog.getLoadInstance();
                    load.replayQuorumLoadJob(job, catalog);
                    break;
                }
                case OperationType.OP_LOAD_DONE: {
                    LoadJob job = (LoadJob) journal.getData();
                    Load load = catalog.getLoadInstance();
                    load.replayFinishLoadJob(job, catalog);
                    break;
                }
                case OperationType.OP_LOAD_CANCEL: {
                    LoadJob job = (LoadJob) journal.getData();
                    Load load = catalog.getLoadInstance();
                    load.replayCancelLoadJob(job);
                    break;
                }
                case OperationType.OP_EXPORT_CREATE: {
                    ExportJob job = (ExportJob) journal.getData();
                    ExportMgr exportMgr = catalog.getExportMgr();
                    exportMgr.replayCreateExportJob(job);
                    break;
                }
                case OperationType.OP_EXPORT_UPDATE_STATE:
                    ExportJob.StateTransfer op = (ExportJob.StateTransfer) journal.getData();
                    ExportMgr exportMgr = catalog.getExportMgr();
                    exportMgr.replayUpdateJobState(op.getJobId(), op.getState());
                    break;
                case OperationType.OP_FINISH_DELETE: {
                    DeleteInfo info = (DeleteInfo) journal.getData();
                    DeleteHandler deleteHandler = catalog.getDeleteHandler();
                    deleteHandler.replayDelete(info, catalog);
                    break;
                }
                case OperationType.OP_ADD_REPLICA: {
                    ReplicaPersistInfo info = (ReplicaPersistInfo) journal.getData();
                    catalog.replayAddReplica(info);
                    break;
                }
                case OperationType.OP_UPDATE_REPLICA: {
                    ReplicaPersistInfo info = (ReplicaPersistInfo) journal.getData();
                    catalog.replayUpdateReplica(info);
                    break;
                }
                case OperationType.OP_DELETE_REPLICA: {
                    ReplicaPersistInfo info = (ReplicaPersistInfo) journal.getData();
                    catalog.replayDeleteReplica(info);
                    break;
                }
                case OperationType.OP_ADD_BACKEND: {
                    Backend be = (Backend) journal.getData();
                    Catalog.getCurrentSystemInfo().replayAddBackend(be);
                    break;
                }
                case OperationType.OP_DROP_BACKEND: {
                    Backend be = (Backend) journal.getData();
                    Catalog.getCurrentSystemInfo().replayDropBackend(be);
                    break;
                }
                case OperationType.OP_MODIFY_BACKEND: {
                    Backend be = (Backend) journal.getData();
                    Catalog.getCurrentSystemInfo().replayModifyBackend(be);
                    break;
                }
                case OperationType.OP_BACKEND_STATE_CHANGE: {
                    Backend be = (Backend) journal.getData();
                    Catalog.getCurrentSystemInfo().updateBackendState(be);
                    break;
                }
                case OperationType.OP_START_DECOMMISSION_BACKEND: {
                    DecommissionBackendJob job = (DecommissionBackendJob) journal.getData();
                    LOG.debug("{}: {}", opCode, job.getTableId());
                    catalog.getClusterHandler().replayInitJob(job, catalog);
                    break;
                }
                case OperationType.OP_FINISH_DECOMMISSION_BACKEND: {
                    DecommissionBackendJob job = (DecommissionBackendJob) journal.getData();
                    LOG.debug("{}: {}", opCode, job.getTableId());
                    catalog.getClusterHandler().replayFinish(job, catalog);
                    break;
                }
                case OperationType.OP_ADD_FIRST_FRONTEND:
                case OperationType.OP_ADD_FRONTEND: {
                    Frontend fe = (Frontend) journal.getData();
                    catalog.replayAddFrontend(fe);
                    break;
                }
                case OperationType.OP_REMOVE_FRONTEND: {
                    Frontend fe = (Frontend) journal.getData();
                    catalog.replayDropFrontend(fe);
                    if (fe.getNodeName().equals(Catalog.getCurrentCatalog().getNodeName())) {
                        System.out.println("current fe " + fe + " is removed. will exit");
                        LOG.info("current fe " + fe + " is removed. will exit");
                        System.exit(-1);
                    }
                    break;
                }
                case OperationType.OP_CREATE_USER: {
                    PrivInfo privInfo = (PrivInfo) journal.getData();
                    catalog.getAuth().replayCreateUser(privInfo);
                    break;
                }
                case OperationType.OP_NEW_DROP_USER: {
                    UserIdentity userIdent = (UserIdentity) journal.getData();
                    catalog.getAuth().replayDropUser(userIdent);
                    break;
                }
                case OperationType.OP_GRANT_PRIV: {
                    PrivInfo privInfo = (PrivInfo) journal.getData();
                    catalog.getAuth().replayGrant(privInfo);
                    break;
                }
                case OperationType.OP_REVOKE_PRIV: {
                    PrivInfo privInfo = (PrivInfo) journal.getData();
                    catalog.getAuth().replayRevoke(privInfo);
                    break;
                }
                case OperationType.OP_SET_PASSWORD: {
                    PrivInfo privInfo = (PrivInfo) journal.getData();
                    catalog.getAuth().replaySetPassword(privInfo);
                    break;
                }
                case OperationType.OP_CREATE_ROLE: {
                    PrivInfo privInfo = (PrivInfo) journal.getData();
                    catalog.getAuth().replayCreateRole(privInfo);
                    break;
                }
                case OperationType.OP_DROP_ROLE: {
                    PrivInfo privInfo = (PrivInfo) journal.getData();
                    catalog.getAuth().replayDropRole(privInfo);
                    break;
                }
                case OperationType.OP_UPDATE_USER_PROPERTY: {
                    UserPropertyInfo propertyInfo = (UserPropertyInfo) journal.getData();
                    catalog.getAuth().replayUpdateUserProperty(propertyInfo);
                    break;
                }
                case OperationType.OP_TIMESTAMP: {
                    Timestamp stamp = (Timestamp) journal.getData();
                    catalog.setSynchronizedTime(stamp.getTimestamp());
                    break;
                }
                case OperationType.OP_MASTER_INFO_CHANGE: {
                    MasterInfo info = (MasterInfo) journal.getData();
                    catalog.setMaster(info);
                    break;
                }
                case OperationType.OP_META_VERSION: {
                    String versionString = ((Text) journal.getData()).toString();
                    int version = Integer.parseInt(versionString);
                    if (version > FeConstants.meta_version) {
                        LOG.error("meta data version is out of date, image: {}. meta: {}."
                                        + "please update FeConstants.meta_version and restart.",
                                MetaContext.get().getMetaVersion(), FeConstants.meta_version);
                        System.exit(-1);
                    }
                    MetaContext.get().setMetaVersion(version);
                    break;
                }
                case OperationType.OP_GLOBAL_VARIABLE: {
                    SessionVariable variable = (SessionVariable) journal.getData();
                    catalog.replayGlobalVariable(variable);
                    break;
                }
                case OperationType.OP_CREATE_CLUSTER: {
                    final Cluster value = (Cluster) journal.getData();
                    catalog.replayCreateCluster(value);
                    break;
                }
                case OperationType.OP_DROP_CLUSTER: {
                    final ClusterInfo value = (ClusterInfo) journal.getData();
                    catalog.replayDropCluster(value);
                    break;
                }
                case OperationType.OP_EXPAND_CLUSTER: {
                    final ClusterInfo info = (ClusterInfo) journal.getData();
                    catalog.replayExpandCluster(info);
                    break;
                }
                case OperationType.OP_LINK_CLUSTER: {
                    final BaseParam param = (BaseParam) journal.getData();
                    catalog.replayLinkDb(param);
                    break;
                }
                case OperationType.OP_MIGRATE_CLUSTER: {
                    final BaseParam param = (BaseParam) journal.getData();
                    catalog.replayMigrateDb(param);
                    break;
                }
                case OperationType.OP_UPDATE_DB: {
                    final DatabaseInfo param = (DatabaseInfo) journal.getData();
                    catalog.replayUpdateDb(param);
                    break;
                }
                case OperationType.OP_DROP_LINKDB: {
                    final DropLinkDbAndUpdateDbInfo param = (DropLinkDbAndUpdateDbInfo) journal.getData();
                    catalog.replayDropLinkDb(param);
                    break;
                }
                case OperationType.OP_ADD_BROKER: {
                    final BrokerMgr.ModifyBrokerInfo param = (BrokerMgr.ModifyBrokerInfo) journal.getData();
                    catalog.getBrokerMgr().replayAddBrokers(param.brokerName, param.brokerAddresses);
                    break;
                }
                case OperationType.OP_DROP_BROKER: {
                    final BrokerMgr.ModifyBrokerInfo param = (BrokerMgr.ModifyBrokerInfo) journal.getData();
                    catalog.getBrokerMgr().replayDropBrokers(param.brokerName, param.brokerAddresses);
                    break;
                }
                case OperationType.OP_DROP_ALL_BROKER: {
                    final String param = journal.getData().toString();
                    catalog.getBrokerMgr().replayDropAllBroker(param);
                    break;
                }
                case OperationType.OP_SET_LOAD_ERROR_HUB: {
                    final LoadErrorHub.Param param = (LoadErrorHub.Param) journal.getData();
                    catalog.getLoadInstance().setLoadErrorHubInfo(param);
                    break;
                }
                case OperationType.OP_UPDATE_CLUSTER_AND_BACKENDS: {
                    final BackendIdsUpdateInfo info = (BackendIdsUpdateInfo) journal.getData();
                    catalog.replayUpdateClusterAndBackends(info);
                    break;
                }
                case OperationType.OP_UPSERT_TRANSACTION_STATE: {
                    final TransactionState state = (TransactionState) journal.getData();
                    Catalog.getCurrentGlobalTransactionMgr().replayUpsertTransactionState(state);
                    LOG.debug("opcode: {}, tid: {}", opCode, state.getTransactionId());
                    break;
                }
                case OperationType.OP_DELETE_TRANSACTION_STATE: {
                    final TransactionState state = (TransactionState) journal.getData();
                    Catalog.getCurrentGlobalTransactionMgr().replayDeleteTransactionState(state);
                    LOG.debug("opcode: {}, tid: {}", opCode, state.getTransactionId());
                    break;
                }
                case OperationType.OP_BATCH_REMOVE_TXNS: {
                    final BatchRemoveTransactionsOperation operation = (BatchRemoveTransactionsOperation) journal.getData();
                    Catalog.getCurrentGlobalTransactionMgr().replayBatchRemoveTransactions(operation);
                    break;
                }
                case OperationType.OP_CREATE_REPOSITORY: {
                    Repository repository = (Repository) journal.getData();
                    catalog.getBackupHandler().getRepoMgr().addAndInitRepoIfNotExist(repository, true);
                    break;
                }
                case OperationType.OP_DROP_REPOSITORY: {
                    String repoName = ((Text) journal.getData()).toString();
                    catalog.getBackupHandler().getRepoMgr().removeRepo(repoName, true);
                    break;
                }
                case OperationType.OP_TRUNCATE_TABLE: {
                    TruncateTableInfo info = (TruncateTableInfo) journal.getData();
                    catalog.replayTruncateTable(info);
                    break;
                }
                case OperationType.OP_COLOCATE_ADD_TABLE: {
                    final ColocatePersistInfo info = (ColocatePersistInfo) journal.getData();
                    catalog.getColocateTableIndex().replayAddTableToGroup(info);
                    break;
                }
                case OperationType.OP_COLOCATE_REMOVE_TABLE: {
                    final ColocatePersistInfo info = (ColocatePersistInfo) journal.getData();
                    catalog.getColocateTableIndex().replayRemoveTable(info);
                    break;
                }
                case OperationType.OP_COLOCATE_BACKENDS_PER_BUCKETSEQ: {
                    final ColocatePersistInfo info = (ColocatePersistInfo) journal.getData();
                    catalog.getColocateTableIndex().replayAddBackendsPerBucketSeq(info);
                    break;
                }
                case OperationType.OP_COLOCATE_MARK_UNSTABLE: {
                    final ColocatePersistInfo info = (ColocatePersistInfo) journal.getData();
                    catalog.getColocateTableIndex().replayMarkGroupUnstable(info);
                    break;
                }
                case OperationType.OP_COLOCATE_MARK_STABLE: {
                    final ColocatePersistInfo info = (ColocatePersistInfo) journal.getData();
                    catalog.getColocateTableIndex().replayMarkGroupStable(info);
                    break;
                }
                case OperationType.OP_MODIFY_TABLE_COLOCATE: {
                    final TablePropertyInfo info = (TablePropertyInfo) journal.getData();
                    catalog.replayModifyTableColocate(info);
                    break;
                }
                case OperationType.OP_HEARTBEAT: {
                    final HbPackage hbPackage = (HbPackage) journal.getData();
                    Catalog.getCurrentHeartbeatMgr().replayHearbeat(hbPackage);
                    break;
                }
                case OperationType.OP_ADD_FUNCTION: {
                    final Function function = (Function) journal.getData();
                    Catalog.getCurrentCatalog().replayCreateFunction(function);
                    break;
                }
                case OperationType.OP_DROP_FUNCTION: {
                    FunctionSearchDesc function = (FunctionSearchDesc) journal.getData();
                    Catalog.getCurrentCatalog().replayDropFunction(function);
                    break;
                }
                case OperationType.OP_BACKEND_TABLETS_INFO: {
                    BackendTabletsInfo backendTabletsInfo = (BackendTabletsInfo) journal.getData();
                    Catalog.getCurrentCatalog().replayBackendTabletsInfo(backendTabletsInfo);
                    break;
                }
                case OperationType.OP_CREATE_ROUTINE_LOAD_JOB: {
                    RoutineLoadJob routineLoadJob = (RoutineLoadJob) journal.getData();
                    Catalog.getCurrentCatalog().getRoutineLoadManager().replayCreateRoutineLoadJob(routineLoadJob);
                    break;
                }
                case OperationType.OP_CHANGE_ROUTINE_LOAD_JOB: {
                    RoutineLoadOperation operation = (RoutineLoadOperation) journal.getData();
                    Catalog.getCurrentCatalog().getRoutineLoadManager().replayChangeRoutineLoadJob(operation);
                    break;
                }
                case OperationType.OP_REMOVE_ROUTINE_LOAD_JOB: {
                    RoutineLoadOperation operation = (RoutineLoadOperation) journal.getData();
                    catalog.getRoutineLoadManager().replayRemoveOldRoutineLoad(operation);
                    break;
                }
                case OperationType.OP_CREATE_LOAD_JOB: {
                    org.apache.doris.load.loadv2.LoadJob loadJob =
                            (org.apache.doris.load.loadv2.LoadJob) journal.getData();
                    catalog.getLoadManager().replayCreateLoadJob(loadJob);
                    break;
                }
                case OperationType.OP_END_LOAD_JOB: {
                    LoadJobFinalOperation operation = (LoadJobFinalOperation) journal.getData();
                    catalog.getLoadManager().replayEndLoadJob(operation);
                    break;
                }
                case OperationType.OP_UPDATE_LOAD_JOB: {
                    LoadJobStateUpdateInfo info = (LoadJobStateUpdateInfo) journal.getData();
                    catalog.getLoadManager().replayUpdateLoadJobStateInfo(info);
                    break;
                }
                case OperationType.OP_FETCH_STREAM_LOAD_RECORD: {
                    FetchStreamLoadRecord fetchStreamLoadRecord = (FetchStreamLoadRecord) journal.getData();
                    catalog.getStreamLoadRecordMgr().replayFetchStreamLoadRecord(fetchStreamLoadRecord);
                    break;
                }
                case OperationType.OP_CREATE_RESOURCE: {
                    final Resource resource = (Resource) journal.getData();
                    catalog.getResourceMgr().replayCreateResource(resource);
                    break;
                }
                case OperationType.OP_DROP_RESOURCE: {
                    final DropResourceOperationLog operationLog = (DropResourceOperationLog) journal.getData();
                    catalog.getResourceMgr().replayDropResource(operationLog);
                    break;
                }
                case OperationType.OP_CREATE_SMALL_FILE: {
                    SmallFile smallFile = (SmallFile) journal.getData();
                    catalog.getSmallFileMgr().replayCreateFile(smallFile);
                    break;
                }
                case OperationType.OP_DROP_SMALL_FILE: {
                    SmallFile smallFile = (SmallFile) journal.getData();
                    catalog.getSmallFileMgr().replayRemoveFile(smallFile);
                    break;
                }
                case OperationType.OP_ALTER_JOB_V2: {
                    AlterJobV2 alterJob = (AlterJobV2) journal.getData();
                    switch (alterJob.getType()) {
                        case ROLLUP:
                            catalog.getRollupHandler().replayAlterJobV2(alterJob);
                            break;
                        case SCHEMA_CHANGE:
                            catalog.getSchemaChangeHandler().replayAlterJobV2(alterJob);
                            break;
                        default:
                            break;
                    }
                    break;
                }
                case OperationType.OP_BATCH_ADD_ROLLUP: {
                    BatchAlterJobPersistInfo batchAlterJobV2 = (BatchAlterJobPersistInfo)journal.getData();
                    for (AlterJobV2 alterJobV2 : batchAlterJobV2.getAlterJobV2List()) {
                        catalog.getRollupHandler().replayAlterJobV2(alterJobV2);
                    }
                    break;
                }
                case OperationType.OP_MODIFY_DISTRIBUTION_TYPE: {
                    TableInfo tableInfo = (TableInfo)journal.getData();
                    catalog.replayConvertDistributionType(tableInfo);
                    break;
                }
                case OperationType.OP_DYNAMIC_PARTITION:
                case OperationType.OP_MODIFY_IN_MEMORY:
                case OperationType.OP_MODIFY_REPLICATION_NUM: {
                    ModifyTablePropertyOperationLog modifyTablePropertyOperationLog = (ModifyTablePropertyOperationLog) journal.getData();
                    catalog.replayModifyTableProperty(opCode, modifyTablePropertyOperationLog);
                    break;
                }
                case OperationType.OP_REPLACE_TEMP_PARTITION: {
                    ReplacePartitionOperationLog replaceTempPartitionLog = (ReplacePartitionOperationLog) journal.getData();
                    catalog.replayReplaceTempPartition(replaceTempPartitionLog);
                    break;
                }
                case OperationType.OP_INSTALL_PLUGIN: {
                    PluginInfo pluginInfo = (PluginInfo) journal.getData();
                    catalog.replayInstallPlugin(pluginInfo);
                    break;
                }
                case OperationType.OP_UNINSTALL_PLUGIN: {
                    PluginInfo pluginInfo = (PluginInfo) journal.getData();
                    catalog.replayUninstallPlugin(pluginInfo);
                    break;
                }
                case OperationType.OP_SET_REPLICA_STATUS: {
                    SetReplicaStatusOperationLog log = (SetReplicaStatusOperationLog) journal.getData();
                    catalog.replaySetReplicaStatus(log);
                    break;
                }
                case OperationType.OP_REMOVE_ALTER_JOB_V2: {
                    RemoveAlterJobV2OperationLog log = (RemoveAlterJobV2OperationLog) journal.getData();
                    switch (log.getType()) {
                        case ROLLUP:
                            catalog.getRollupHandler().replayRemoveAlterJobV2(log);
                            break;
                        case SCHEMA_CHANGE:
                            catalog.getSchemaChangeHandler().replayRemoveAlterJobV2(log);
                            break;
                        default:
                            break;
                    }
                    break;
                }
                case OperationType.OP_ALTER_ROUTINE_LOAD_JOB: {
                    AlterRoutineLoadJobOperationLog log = (AlterRoutineLoadJobOperationLog) journal.getData();
                    catalog.getRoutineLoadManager().replayAlterRoutineLoadJob(log);
                    break;
                }
                case OperationType.OP_GLOBAL_VARIABLE_V2: {
                    GlobalVarPersistInfo info = (GlobalVarPersistInfo) journal.getData();
                    catalog.replayGlobalVariableV2(info);
                    break;
                }
                case OperationType.OP_REPLACE_TABLE: {
                    ReplaceTableOperationLog log = (ReplaceTableOperationLog) journal.getData();
                    catalog.getAlterInstance().replayReplaceTable(log);
                    break;
                }
                default: {
                    IOException e = new IOException();
                    LOG.error("UNKNOWN Operation Type {}", opCode, e);
                    throw e;
                }
            }
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
    private synchronized void logEdit(short op, Writable writable) {
        if (this.getNumEditStreams() == 0) {
            LOG.error("Fatal Error : no editLog stream", new Exception());
            throw new Error("Fatal Error : no editLog stream");
        }

        long start = System.currentTimeMillis();

        try {
            journal.write(op, writable);
        } catch (Throwable t) {
            // Throwable contains all Exception and Error, such as IOException and OutOfMemoryError
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
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("nextId = {}, numTransactions = {}, totalTimeTransactions = {}, op = {}",
                    txId, numTransactions, totalTimeTransactions, op);
        }

        if (txId >= Config.edit_log_roll_num) {
            LOG.info("txId {} is equal to or larger than edit_log_roll_num {}, will roll edit.",
                    txId, Config.edit_log_roll_num);
            rollEditLog();
            txId = 0;
        }

        if (MetricRepo.isInit) {
            MetricRepo.COUNTER_EDIT_LOG_WRITE.increase(1L);
        }
    }

    /**
     * Return the size of the current EditLog
     */
    synchronized long getEditLogSize() throws IOException {
        return editStream.length();
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
        logEdit(OperationType.OP_CREATE_TABLE, info);
    }

    public void logRefreshExternalTableSchema(RefreshExternalTableInfo info) {
        logEdit(OperationType.OP_ALTER_EXTERNAL_TABLE_SCHEMA, info);
    }

    public void logAddPartition(PartitionPersistInfo info) {
        logEdit(OperationType.OP_ADD_PARTITION, info);
    }

    public void logDropPartition(DropPartitionInfo info) {
        logEdit(OperationType.OP_DROP_PARTITION, info);
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
        logEdit(OperationType.OP_DROP_TABLE, info);
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

    public void logStartRollup(RollupJob rollupJob) {
        logEdit(OperationType.OP_START_ROLLUP, rollupJob);
    }

    public void logFinishingRollup(RollupJob rollupJob) {
        logEdit(OperationType.OP_FINISHING_ROLLUP, rollupJob);
    }

    public void logFinishRollup(RollupJob rollupJob) {
        logEdit(OperationType.OP_FINISH_ROLLUP, rollupJob);
    }

    public void logCancelRollup(RollupJob rollupJob) {
        logEdit(OperationType.OP_CANCEL_ROLLUP, rollupJob);
    }

    public void logDropRollup(DropInfo info) {
        logEdit(OperationType.OP_DROP_ROLLUP, info);
    }

    public void logBatchDropRollup (BatchDropInfo batchDropInfo) {
        logEdit(OperationType.OP_BATCH_DROP_ROLLUP, batchDropInfo);
    }

    public void logStartSchemaChange(SchemaChangeJob schemaChangeJob) {
        logEdit(OperationType.OP_START_SCHEMA_CHANGE, schemaChangeJob);
    }

    public void logFinishingSchemaChange(SchemaChangeJob schemaChangeJob) {
        logEdit(OperationType.OP_FINISHING_SCHEMA_CHANGE, schemaChangeJob);
    }

    public void logFinishSchemaChange(SchemaChangeJob schemaChangeJob) {
        logEdit(OperationType.OP_FINISH_SCHEMA_CHANGE, schemaChangeJob);
    }

    public void logCancelSchemaChange(SchemaChangeJob schemaChangeJob) {
        logEdit(OperationType.OP_CANCEL_SCHEMA_CHANGE, schemaChangeJob);
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

    public void logCreateRole(PrivInfo info) {
        logEdit(OperationType.OP_CREATE_ROLE, info);
    }

    public void logDropRole(PrivInfo info) {
        logEdit(OperationType.OP_DROP_ROLE, info);
    }

    public void logStartDecommissionBackend(DecommissionBackendJob job) {
        logEdit(OperationType.OP_START_DECOMMISSION_BACKEND, job);
    }

    public void logFinishDecommissionBackend(DecommissionBackendJob job) {
        logEdit(OperationType.OP_FINISH_DECOMMISSION_BACKEND, job);
    }

    public void logDatabaseRename(DatabaseInfo databaseInfo) {
        logEdit(OperationType.OP_RENAME_DB, databaseInfo);
    }

    public void logUpdateDatabase(DatabaseInfo databaseInfo) {
        logEdit(OperationType.OP_UPDATE_DB, databaseInfo);
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

    public void logGlobalVariable(SessionVariable variable) {
        logEdit(OperationType.OP_GLOBAL_VARIABLE, variable);
    }

    public void logCreateCluster(Cluster cluster) {
        logEdit(OperationType.OP_CREATE_CLUSTER, cluster);
    }

    public void logDropCluster(ClusterInfo info) {
        logEdit(OperationType.OP_DROP_CLUSTER, info);
    }

    public void logExpandCluster(ClusterInfo ci) {
        logEdit(OperationType.OP_EXPAND_CLUSTER, ci);
    }

    public void logLinkCluster(BaseParam param) {
        logEdit(OperationType.OP_LINK_CLUSTER, param);
    }

    public void logMigrateCluster(BaseParam param) {
        logEdit(OperationType.OP_MIGRATE_CLUSTER, param);
    }

    public void logDropLinkDb(DropLinkDbAndUpdateDbInfo info) {
        logEdit(OperationType.OP_DROP_LINKDB, info);
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

    public void logSetLoadErrorHub(LoadErrorHub.Param param) {
        logEdit(OperationType.OP_SET_LOAD_ERROR_HUB, param);
    }

    public void logExportCreate(ExportJob job) {
        logEdit(OperationType.OP_EXPORT_CREATE, job);
    }

    public void logExportUpdateState(long jobId, ExportJob.JobState newState) {
        ExportJob.StateTransfer transfer = new ExportJob.StateTransfer(jobId, newState);
        logEdit(OperationType.OP_EXPORT_UPDATE_STATE, transfer);
    }

    public void logUpdateClusterAndBackendState(BackendIdsUpdateInfo info) {
        logEdit(OperationType.OP_UPDATE_CLUSTER_AND_BACKENDS, info);
    }

    // for TransactionState
    public void logInsertTransactionState(TransactionState transactionState) {
        logEdit(OperationType.OP_UPSERT_TRANSACTION_STATE, transactionState);
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

    public void logDropFunction(FunctionSearchDesc function) {
        logEdit(OperationType.OP_DROP_FUNCTION, function);
    }

    public void logBackendTabletsInfo(BackendTabletsInfo backendTabletsInfo) {
        logEdit(OperationType.OP_BACKEND_TABLETS_INFO, backendTabletsInfo);
    }

    public void logCreateRoutineLoadJob(RoutineLoadJob routineLoadJob) {
        logEdit(OperationType.OP_CREATE_ROUTINE_LOAD_JOB, routineLoadJob);
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

    public void logCreateSmallFile(SmallFile info) {
        logEdit(OperationType.OP_CREATE_SMALL_FILE, info);
    }

    public void logDropSmallFile(SmallFile info) {
        logEdit(OperationType.OP_DROP_SMALL_FILE, info);
    }

    public void logAlterJob(AlterJobV2 alterJob) {
        logEdit(OperationType.OP_ALTER_JOB_V2, alterJob);
    }

    public void logBatchAlterJob(BatchAlterJobPersistInfo batchAlterJobV2) {
        logEdit(OperationType.OP_BATCH_ADD_ROLLUP, batchAlterJobV2);
    }

    public void logModifyDistributionType(TableInfo tableInfo) {
        logEdit(OperationType.OP_MODIFY_DISTRIBUTION_TYPE, tableInfo);
    }

    public void logDynamicPartition(ModifyTablePropertyOperationLog info) {
        logEdit(OperationType.OP_DYNAMIC_PARTITION, info);
    }

    public void logModifyReplicationNum(ModifyTablePropertyOperationLog info) {
        logEdit(OperationType.OP_MODIFY_REPLICATION_NUM, info);
    }

    public void logModifyInMemory(ModifyTablePropertyOperationLog info) {
        logEdit(OperationType.OP_MODIFY_IN_MEMORY, info);
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

    public void logBatchRemoveTransactions(BatchRemoveTransactionsOperation op) {
        logEdit(OperationType.OP_BATCH_REMOVE_TXNS, op);
    }
}
