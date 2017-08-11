// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

package com.baidu.palo.persist;

import com.baidu.palo.alter.DecommissionBackendJob;
import com.baidu.palo.alter.RollupJob;
import com.baidu.palo.alter.SchemaChangeJob;
import com.baidu.palo.backup.BackupJob;
import com.baidu.palo.backup.RestoreJob;
import com.baidu.palo.catalog.BrokerMgr;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.UserProperty;
import com.baidu.palo.cluster.BaseParam;
import com.baidu.palo.cluster.Cluster;
import com.baidu.palo.common.Config;
import com.baidu.palo.common.FeConstants;
import com.baidu.palo.common.io.Text;
import com.baidu.palo.common.io.Writable;
import com.baidu.palo.ha.MasterInfo;
import com.baidu.palo.journal.Journal;
import com.baidu.palo.journal.JournalCursor;
import com.baidu.palo.journal.JournalEntity;
import com.baidu.palo.journal.bdbje.BDBJEJournal;
import com.baidu.palo.journal.bdbje.Timestamp;
import com.baidu.palo.journal.local.LocalJournal;
import com.baidu.palo.load.AsyncDeleteJob;
import com.baidu.palo.load.DeleteInfo;
import com.baidu.palo.load.ExportJob;
import com.baidu.palo.load.ExportMgr;
import com.baidu.palo.load.Load;
import com.baidu.palo.load.LoadErrorHub;
import com.baidu.palo.load.LoadJob;
import com.baidu.palo.qe.SessionVariable;
import com.baidu.palo.system.Backend;
import com.baidu.palo.system.Frontend;

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

    public EditLog() {
        String journalType = Config.edit_log_type;
        if (journalType.equalsIgnoreCase("bdb")) {
            journal = new BDBJEJournal();
        } else if (journalType.equalsIgnoreCase("local")) {
            journal = new LocalJournal(Catalog.IMAGE_DIR);
            Catalog.getInstance().setIsMaster(true);
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
                case OperationType.OP_CREATE_DB: {
                    Database db = (Database) journal.getData();
                    catalog.replayCreateDb(db);
                    break;
                }
                case OperationType.OP_DROP_DB: {
                    String dbName = ((Text) journal.getData()).toString();
                    catalog.replayDropDb(dbName);
                    break;
                }
                case OperationType.OP_ALTER_DB: {
                    DatabaseInfo dbInfo = (DatabaseInfo) journal.getData();
                    String dbName = dbInfo.getDbName();
                    LOG.info("Begin to unprotect alter db info {}", dbName);
                    catalog.replayAlterDatabaseQuota(dbName, dbInfo.getQuota());
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
                case OperationType.OP_DROP_TABLE: {
                    DropInfo info = (DropInfo) journal.getData();
                    Database db = catalog.getDb(info.getDbId());
                    if (db == null) {
                        LOG.warn("failed to get db[{}]", info.getDbId());
                        break;
                    }
                    LOG.info("Begin to unprotect drop table. db = "
                            + db.getName() + " table = " + info.getTableId());
                    catalog.replayDropTable(db, info.getTableId());
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
                    catalog.replayModifyPartition(info);
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
                case OperationType.OP_RENAME_PARTITION: {
                    TableInfo info = (TableInfo) journal.getData();
                    catalog.replayRenamePartition(info);
                    break;
                }
                case OperationType.OP_BACKUP_START: {
                    BackupJob job = (BackupJob) journal.getData();
                    catalog.getBackupHandler().replayBackupStart(catalog, job);
                    break;
                }
                case OperationType.OP_BACKUP_FINISH_SNAPSHOT: {
                    BackupJob job = (BackupJob) journal.getData();
                    catalog.getBackupHandler().replayBackupFinishSnapshot(job);
                    break;
                }
                case OperationType.OP_BACKUP_FINISH: {
                    BackupJob job = (BackupJob) journal.getData();
                    catalog.getBackupHandler().replayBackupFinish(catalog, job);
                    break;
                }
                case OperationType.OP_RESTORE_START: {
                    RestoreJob job = (RestoreJob) journal.getData();
                    catalog.getBackupHandler().replayRestoreStart(catalog, job);
                    break;
                }
                case OperationType.OP_RESTORE_FINISH: {
                    RestoreJob job = (RestoreJob) journal.getData();
                    catalog.getBackupHandler().replayRestoreFinish(catalog, job);
                    break;
                }
                case OperationType.OP_START_ROLLUP: {
                    RollupJob job = (RollupJob) journal.getData();
                    catalog.getRollupHandler().replayInitJob(job, catalog);
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
                case OperationType.OP_START_SCHEMA_CHANGE: {
                    SchemaChangeJob job = (SchemaChangeJob) journal.getData();
                    LOG.info("Begin to unprotect create schema change job. db = " + job.getDbId()
                            + " table = " + job.getTableId());
                    catalog.getSchemaChangeHandler().replayInitJob(job, catalog);
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
                case OperationType.OP_FINISH_SYNC_DELETE: {
                    DeleteInfo info = (DeleteInfo) journal.getData();
                    Load load = catalog.getLoadInstance();
                    load.replayDelete(info, catalog);
                    break;
                }
                case OperationType.OP_FINISH_ASYNC_DELETE: {
                    AsyncDeleteJob deleteJob = (AsyncDeleteJob) journal.getData();
                    Load load = catalog.getLoadInstance();
                    load.replayFinishAsyncDeleteJob(deleteJob, catalog);
                    break;
                }
                case OperationType.OP_ADD_REPLICA: {
                    ReplicaPersistInfo info = (ReplicaPersistInfo) journal.getData();
                    catalog.replayAddReplica(info);
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
                    if (fe.getHost().equals(Catalog.getInstance().getSelfNode().first)
                            && fe.getPort() == Catalog.getInstance().getSelfNode().second
                            && fe.getRole().equals(Catalog.getInstance().getRole())) {
                        LOG.info("current fe " + fe + " is removed. will exit");
                        System.exit(-1);
                    }
                    break;
                }
                case OperationType.OP_ALTER_ACCESS_RESOURCE: {
                    UserProperty resource = (UserProperty) journal.getData();
                    catalog.getUserMgr().replayAlterAccess(resource);
                    break;
                }
                case OperationType.OP_DROP_USER: {
                    String userName = ((Text) journal.getData()).toString();
                    catalog.getUserMgr().replayDropUser(userName);
                    break;
                }
                case OperationType.OP_TIMESTAMP: {
                    Timestamp stamp = (Timestamp) journal.getData();
                    catalog.setSynchronizedTime(stamp.getTimestamp());
                    break;
                }
                case OperationType.OP_MASTER_INFO_CHANGE: {
                    MasterInfo info = (MasterInfo) journal.getData();
                    catalog.setMasterIp(info.getIp());
                    catalog.setMasterHttpPort(info.getHttpPort());
                    catalog.setMasterRpcPort(info.getRpcPort());
                    break;
                }
                case OperationType.OP_META_VERSION: {
                    String versionString = ((Text) journal.getData()).toString();
                    int version = Integer.parseInt(versionString);
                    if (catalog.getJournalVersion() > FeConstants.meta_version) {
                        LOG.error("meta data version is out of date, image: {}. meta: {}."
                                + "please update FeConstants.meta_version and restart.",
                                  catalog.getJournalVersion(), FeConstants.meta_version);
                        System.exit(-1);
                    }
                    catalog.setJournalVersion(version);
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
                case OperationType.OP_UPDATE_CLUSTER: {
                    final ClusterInfo value = (ClusterInfo) journal.getData();
                    catalog.replayUpdateCluster(value);
                    break;
                }
                case OperationType.OP_MODIFY_CLUSTER: {
                    final ClusterInfo value = (ClusterInfo) journal.getData();
                    catalog.replayUpdateCluster(value);
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
                case OperationType.OP_SET_LOAD_ERROR_URL: {
                    final LoadErrorHub.Param param = (LoadErrorHub.Param) journal.getData();
                    catalog.getLoadInstance().setLoadErrorHubInfo(param);
                    break;
                }
                default: {
                    IOException e = new IOException();
                    LOG.error("UNKNOWN Operation Type {}", opCode, e);
                }
            }
        } catch (Exception e) {
            LOG.error("Operation Type {}", opCode, e);
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
            LOG.error("Fatal Error : no editLog stream");
            throw new Error("Fatal Error : no editLog stream");
        }

        long start = System.currentTimeMillis();

        try {
            journal.write(op, writable);
        } catch (Exception e) {
            LOG.error("Fatal Error : write stream Exception", e);
            Runtime.getRuntime().exit(-1);
        }

        // get a new transactionId
        txId++;

        // update statistics
        long end = System.currentTimeMillis();
        numTransactions++;
        totalTimeTransactions += (end - start);

        if (LOG.isDebugEnabled()) {
            LOG.debug("nextId = {}, numTransactions = {}, totalTimeTransactions = {}, op = {}",
                      txId, numTransactions, totalTimeTransactions, op);
        }

        if (txId == Config.edit_log_roll_num) {
            LOG.info("txId is equal to edit_log_roll_num {}, will roll edit.", txId);
            rollEditLog();
            txId = 0;
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

    public void logCreateDb(Database db) {
        logEdit(OperationType.OP_CREATE_DB, db);
    }

    public void logDropDb(String dbName) {
        logEdit(OperationType.OP_DROP_DB, new Text(dbName));
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

    public void logFinishRollup(RollupJob rollupJob) {
        logEdit(OperationType.OP_FINISH_ROLLUP, rollupJob);
    }

    public void logCancelRollup(RollupJob rollupJob) {
        logEdit(OperationType.OP_CANCEL_ROLLUP, rollupJob);
    }

    public void logClearRollupIndexInfo(ReplicaPersistInfo info) {
        logEdit(OperationType.OP_CLEAR_ROLLUP_INFO, info);
    }

    public void logDropRollup(DropInfo info) {
        logEdit(OperationType.OP_DROP_ROLLUP, info);
    }

    public void logStartSchemaChange(SchemaChangeJob schemaChangeJob) {
        logEdit(OperationType.OP_START_SCHEMA_CHANGE, schemaChangeJob);
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

    public void logAddFrontend(Frontend fe) {
        logEdit(OperationType.OP_ADD_FRONTEND, fe);
    }

    public void logAddFirstFrontend(Frontend fe) {
        logEdit(OperationType.OP_ADD_FIRST_FRONTEND, fe);
    }

    public void logRemoveFrontend(Frontend fe) {
        logEdit(OperationType.OP_REMOVE_FRONTEND, fe);
    }

    public void logFinishSyncDelete(DeleteInfo info) {
        logEdit(OperationType.OP_FINISH_SYNC_DELETE, info);
    }

    public void logFinishAsyncDelete(AsyncDeleteJob job) {
        logEdit(OperationType.OP_FINISH_ASYNC_DELETE, job);
    }

    public void logAddReplica(ReplicaPersistInfo info) {
        logEdit(OperationType.OP_ADD_REPLICA, info);
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

    public void logAlterAccess(UserProperty userProperty) {
        logEdit(OperationType.OP_ALTER_ACCESS_RESOURCE, userProperty);
    }

    public void logDropUser(String userName) {
        logEdit(OperationType.OP_DROP_USER, new Text(userName));
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

    public void logRollupRename(TableInfo tableInfo) {
        logEdit(OperationType.OP_RENAME_ROLLUP, tableInfo);
    }

    public void logPartitionRename(TableInfo tableInfo) {
        logEdit(OperationType.OP_RENAME_PARTITION, tableInfo);
    }

    public void logBackupStart(BackupJob backupJob) {
        logEdit(OperationType.OP_BACKUP_START, backupJob);
    }

    public void logBackupFinishSnapshot(BackupJob backupJob) {
        logEdit(OperationType.OP_BACKUP_FINISH_SNAPSHOT, backupJob);
    }

    public void logBackupFinish(BackupJob backupJob) {
        logEdit(OperationType.OP_BACKUP_FINISH, backupJob);
    }

    public void logRestoreJobStart(RestoreJob restoreJob) {
        logEdit(OperationType.OP_RESTORE_START, restoreJob);
    }

    public void logRestoreFinish(RestoreJob restoreJob) {
        logEdit(OperationType.OP_RESTORE_FINISH, restoreJob);
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

    public void logUpdateDbClusterName(String info) {
        logEdit(OperationType.OP_UPDATE_DB, new Text(info));
    }

    public void logModifyCluster(ClusterInfo ci) {
        logEdit(OperationType.OP_MODIFY_CLUSTER, ci);
    }

    public void logLinkCluster(BaseParam param) {
        logEdit(OperationType.OP_LINK_CLUSTER, param);
    }

    public void logMigrateCluster(BaseParam param) {
        logEdit(OperationType.OP_MIGRATE_CLUSTER, param);
    }

    public void logUpdateCluster(ClusterInfo info) {
        logEdit(OperationType.OP_UPDATE_CLUSTER, info);
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
        logEdit(OperationType.OP_SET_LOAD_ERROR_URL, param);
    }

    public void logExportCreate(ExportJob job) {
        logEdit(OperationType.OP_EXPORT_CREATE, job);
    }

    public void logExportUpdateState(long jobId, ExportJob.JobState newState) {
        ExportJob.StateTransfer transfer = new ExportJob.StateTransfer(jobId, newState);
        logEdit(OperationType.OP_EXPORT_UPDATE_STATE, transfer);
    }
}
