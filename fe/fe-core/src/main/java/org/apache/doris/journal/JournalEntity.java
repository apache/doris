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

package org.apache.doris.journal;

import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.alter.BatchAlterJobPersistInfo;
import org.apache.doris.alter.IndexChangeJob;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.backup.BackupJob;
import org.apache.doris.backup.Repository;
import org.apache.doris.backup.RestoreJob;
import org.apache.doris.blockrule.SqlBlockRule;
import org.apache.doris.catalog.BrokerMgr;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.EncryptKey;
import org.apache.doris.catalog.EncryptKeySearchDesc;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionSearchDesc;
import org.apache.doris.catalog.Resource;
import org.apache.doris.cluster.Cluster;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.SmallFileMgr.SmallFile;
import org.apache.doris.cooldown.CooldownConfList;
import org.apache.doris.cooldown.CooldownDelete;
import org.apache.doris.datasource.CatalogLog;
import org.apache.doris.datasource.ExternalObjectLog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.InitDatabaseLog;
import org.apache.doris.datasource.InitTableLog;
import org.apache.doris.ha.MasterInfo;
import org.apache.doris.journal.bdbje.Timestamp;
import org.apache.doris.load.DeleteInfo;
import org.apache.doris.load.ExportJob;
import org.apache.doris.load.LoadErrorHub;
import org.apache.doris.load.LoadJob;
import org.apache.doris.load.StreamLoadRecordMgr.FetchStreamLoadRecord;
import org.apache.doris.load.loadv2.LoadJob.LoadJobStateUpdateInfo;
import org.apache.doris.load.loadv2.LoadJobFinalOperation;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.load.sync.SyncJob;
import org.apache.doris.mtmv.metadata.ChangeMTMVJob;
import org.apache.doris.mtmv.metadata.DropMTMVJob;
import org.apache.doris.mtmv.metadata.DropMTMVTask;
import org.apache.doris.mtmv.metadata.MTMVJob;
import org.apache.doris.mtmv.metadata.MTMVTask;
import org.apache.doris.mysql.privilege.UserPropertyInfo;
import org.apache.doris.persist.AlterDatabasePropertyInfo;
import org.apache.doris.persist.AlterLightSchemaChangeInfo;
import org.apache.doris.persist.AlterMultiMaterializedView;
import org.apache.doris.persist.AlterRoutineLoadJobOperationLog;
import org.apache.doris.persist.AlterUserOperationLog;
import org.apache.doris.persist.AlterViewInfo;
import org.apache.doris.persist.AnalyzeDeletionLog;
import org.apache.doris.persist.BackendReplicasInfo;
import org.apache.doris.persist.BackendTabletsInfo;
import org.apache.doris.persist.BarrierLog;
import org.apache.doris.persist.BatchDropInfo;
import org.apache.doris.persist.BatchModifyPartitionsInfo;
import org.apache.doris.persist.BatchRemoveTransactionsOperation;
import org.apache.doris.persist.BatchRemoveTransactionsOperationV2;
import org.apache.doris.persist.BinlogGcInfo;
import org.apache.doris.persist.CleanLabelOperationLog;
import org.apache.doris.persist.CleanQueryStatsInfo;
import org.apache.doris.persist.ColocatePersistInfo;
import org.apache.doris.persist.ConsistencyCheckInfo;
import org.apache.doris.persist.CreateTableInfo;
import org.apache.doris.persist.DatabaseInfo;
import org.apache.doris.persist.DropDbInfo;
import org.apache.doris.persist.DropInfo;
import org.apache.doris.persist.DropPartitionInfo;
import org.apache.doris.persist.DropResourceOperationLog;
import org.apache.doris.persist.DropSqlBlockRuleOperationLog;
import org.apache.doris.persist.DropWorkloadGroupOperationLog;
import org.apache.doris.persist.GlobalVarPersistInfo;
import org.apache.doris.persist.HbPackage;
import org.apache.doris.persist.LdapInfo;
import org.apache.doris.persist.ModifyCommentOperationLog;
import org.apache.doris.persist.ModifyPartitionInfo;
import org.apache.doris.persist.ModifyTableDefaultDistributionBucketNumOperationLog;
import org.apache.doris.persist.ModifyTableEngineOperationLog;
import org.apache.doris.persist.ModifyTablePropertyOperationLog;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.PartitionPersistInfo;
import org.apache.doris.persist.PrivInfo;
import org.apache.doris.persist.RecoverInfo;
import org.apache.doris.persist.RefreshExternalTableInfo;
import org.apache.doris.persist.RemoveAlterJobV2OperationLog;
import org.apache.doris.persist.ReplacePartitionOperationLog;
import org.apache.doris.persist.ReplaceTableOperationLog;
import org.apache.doris.persist.ReplicaPersistInfo;
import org.apache.doris.persist.RoutineLoadOperation;
import org.apache.doris.persist.SetReplicaStatusOperationLog;
import org.apache.doris.persist.TableAddOrDropColumnsInfo;
import org.apache.doris.persist.TableAddOrDropInvertedIndicesInfo;
import org.apache.doris.persist.TableInfo;
import org.apache.doris.persist.TablePropertyInfo;
import org.apache.doris.persist.TableRenameColumnInfo;
import org.apache.doris.persist.TruncateTableInfo;
import org.apache.doris.plugin.PluginInfo;
import org.apache.doris.policy.DropPolicyLog;
import org.apache.doris.policy.Policy;
import org.apache.doris.policy.StoragePolicy;
import org.apache.doris.resource.workloadgroup.WorkloadGroup;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Frontend;
import org.apache.doris.transaction.TransactionState;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// this is the value written to bdb or local edit files. key is an auto-increasing long.
public class JournalEntity implements Writable {
    public static final Logger LOG = LogManager.getLogger(JournalEntity.class);

    private short opCode;
    private Writable data;
    private long dataSize;

    public short getOpCode() {
        return this.opCode;
    }

    public void setOpCode(short opCode) {
        this.opCode = opCode;
    }

    public Writable getData() {
        return this.data;
    }

    public void setData(Writable data) {
        this.data = data;
    }

    public String toString() {
        return " opCode=" + opCode + " " + data;
    }

    public void setDataSize(long dataSize) {
        this.dataSize = dataSize;
    }

    public long getDataSize() {
        return this.dataSize;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeShort(opCode);
        data.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        opCode = in.readShort();
        // set it to true after the entity is truly read,
        // to avoid someone forget to call read method.
        boolean isRead = false;
        LOG.debug("get opcode: {}", opCode);
        switch (opCode) {
            case OperationType.OP_LOCAL_EOF: {
                data = null;
                isRead = true;
                break;
            }
            case OperationType.OP_SAVE_NEXTID: {
                data = new Text();
                ((Text) data).readFields(in);
                isRead = true;
                break;
            }
            case OperationType.OP_SAVE_TRANSACTION_ID: {
                data = new Text();
                ((Text) data).readFields(in);
                isRead = true;
                break;
            }
            case OperationType.OP_CREATE_DB: {
                data = Database.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_DROP_DB: {
                data = DropDbInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_ALTER_DB:
            case OperationType.OP_RENAME_DB: {
                data = new DatabaseInfo();
                ((DatabaseInfo) data).readFields(in);
                isRead = true;
                break;
            }
            case OperationType.OP_CREATE_TABLE: {
                data = new CreateTableInfo();
                ((CreateTableInfo) data).readFields(in);
                isRead = true;
                break;
            }
            case OperationType.OP_DROP_TABLE: {
                data = DropInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_ALTER_EXTERNAL_TABLE_SCHEMA: {
                data = RefreshExternalTableInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_ADD_PARTITION: {
                data = new PartitionPersistInfo();
                ((PartitionPersistInfo) data).readFields(in);
                isRead = true;
                break;
            }
            case OperationType.OP_DROP_PARTITION: {
                data = DropPartitionInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_MODIFY_PARTITION: {
                data = ModifyPartitionInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_BATCH_MODIFY_PARTITION: {
                data = BatchModifyPartitionsInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_ERASE_DB:
            case OperationType.OP_ERASE_TABLE:
            case OperationType.OP_ERASE_PARTITION: {
                data = new Text();
                ((Text) data).readFields(in);
                isRead = true;
                break;
            }
            case OperationType.OP_RECOVER_DB:
            case OperationType.OP_RECOVER_TABLE:
            case OperationType.OP_RECOVER_PARTITION: {
                data = RecoverInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_DROP_ROLLUP: {
                data = DropInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_BATCH_DROP_ROLLUP: {
                data = BatchDropInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_RENAME_TABLE:
            case OperationType.OP_RENAME_ROLLUP:
            case OperationType.OP_RENAME_PARTITION: {
                data = new TableInfo();
                ((TableInfo) data).readFields(in);
                isRead = true;
                break;
            }
            case OperationType.OP_RENAME_COLUMN: {
                data = TableRenameColumnInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_MODIFY_VIEW_DEF: {
                data = AlterViewInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_BACKUP_JOB: {
                data = BackupJob.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_RESTORE_JOB: {
                data = RestoreJob.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_FINISH_CONSISTENCY_CHECK: {
                data = new ConsistencyCheckInfo();
                ((ConsistencyCheckInfo) data).readFields(in);
                isRead = true;
                break;
            }
            case OperationType.OP_LOAD_START:
            case OperationType.OP_LOAD_ETL:
            case OperationType.OP_LOAD_LOADING:
            case OperationType.OP_LOAD_QUORUM:
            case OperationType.OP_LOAD_DONE:
            case OperationType.OP_LOAD_CANCEL: {
                data = new LoadJob();
                ((LoadJob) data).readFields(in);
                isRead = true;
                break;
            }
            case OperationType.OP_EXPORT_CREATE:
                data = ExportJob.read(in);
                isRead = true;
                break;
            case OperationType.OP_EXPORT_UPDATE_STATE:
                data = ExportJob.StateTransfer.read(in);
                isRead = true;
                break;
            case OperationType.OP_FINISH_DELETE: {
                data = DeleteInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_ADD_REPLICA:
            case OperationType.OP_UPDATE_REPLICA:
            case OperationType.OP_DELETE_REPLICA:
            case OperationType.OP_CLEAR_ROLLUP_INFO: {
                data = ReplicaPersistInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_ADD_BACKEND:
            case OperationType.OP_DROP_BACKEND:
            case OperationType.OP_MODIFY_BACKEND:
            case OperationType.OP_BACKEND_STATE_CHANGE: {
                data = Backend.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_ADD_FRONTEND:
            case OperationType.OP_ADD_FIRST_FRONTEND:
            case OperationType.OP_MODIFY_FRONTEND:
            case OperationType.OP_REMOVE_FRONTEND: {
                data = Frontend.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_SET_LOAD_ERROR_HUB: {
                data = new LoadErrorHub.Param();
                ((LoadErrorHub.Param) data).readFields(in);
                isRead = true;
                break;
            }
            case OperationType.OP_NEW_DROP_USER: {
                data = UserIdentity.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_CREATE_USER:
            case OperationType.OP_GRANT_PRIV:
            case OperationType.OP_REVOKE_PRIV:
            case OperationType.OP_SET_PASSWORD:
            case OperationType.OP_CREATE_ROLE:
            case OperationType.OP_DROP_ROLE: {
                data = PrivInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_SET_LDAP_PASSWORD: {
                data = LdapInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_UPDATE_USER_PROPERTY: {
                data = UserPropertyInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_MASTER_INFO_CHANGE: {
                data = MasterInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_TIMESTAMP: {
                data = new Timestamp();
                ((Timestamp) data).readFields(in);
                isRead = true;
                break;
            }
            case OperationType.OP_META_VERSION: {
                data = new Text();
                ((Text) data).readFields(in);
                isRead = true;
                break;
            }
            case OperationType.OP_CREATE_CLUSTER: {
                data = Cluster.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_ADD_BROKER:
            case OperationType.OP_DROP_BROKER: {
                data = new BrokerMgr.ModifyBrokerInfo();
                ((BrokerMgr.ModifyBrokerInfo) data).readFields(in);
                isRead = true;
                break;
            }
            case OperationType.OP_DROP_ALL_BROKER: {
                data = new Text();
                ((Text) data).readFields(in);
                isRead = true;
                break;
            }
            case OperationType.OP_UPSERT_TRANSACTION_STATE:
            case OperationType.OP_DELETE_TRANSACTION_STATE: {
                data = new TransactionState();
                ((TransactionState) data).readFields(in);
                isRead = true;
                break;
            }
            case OperationType.OP_BATCH_REMOVE_TXNS: {
                data = BatchRemoveTransactionsOperation.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_BATCH_REMOVE_TXNS_V2: {
                data = BatchRemoveTransactionsOperationV2.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_CREATE_REPOSITORY: {
                data = Repository.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_DROP_REPOSITORY: {
                data = new Text();
                ((Text) data).readFields(in);
                isRead = true;
                break;
            }
            case OperationType.OP_TRUNCATE_TABLE: {
                data = TruncateTableInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_COLOCATE_ADD_TABLE:
            case OperationType.OP_COLOCATE_REMOVE_TABLE:
            case OperationType.OP_COLOCATE_BACKENDS_PER_BUCKETSEQ:
            case OperationType.OP_COLOCATE_MARK_UNSTABLE:
            case OperationType.OP_COLOCATE_MARK_STABLE: {
                data = ColocatePersistInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_MODIFY_TABLE_COLOCATE: {
                data = TablePropertyInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_HEARTBEAT: {
                data = HbPackage.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_ADD_FUNCTION: {
                data = Function.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_DROP_FUNCTION: {
                data = FunctionSearchDesc.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_ADD_GLOBAL_FUNCTION: {
                data = Function.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_DROP_GLOBAL_FUNCTION: {
                data = FunctionSearchDesc.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_CREATE_ENCRYPTKEY: {
                data = EncryptKey.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_DROP_ENCRYPTKEY: {
                data = EncryptKeySearchDesc.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_BACKEND_TABLETS_INFO: {
                data = BackendTabletsInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_BACKEND_REPLICAS_INFO: {
                data = BackendReplicasInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_CREATE_ROUTINE_LOAD_JOB: {
                data = RoutineLoadJob.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_CHANGE_ROUTINE_LOAD_JOB:
            case OperationType.OP_REMOVE_ROUTINE_LOAD_JOB: {
                data = RoutineLoadOperation.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_CREATE_LOAD_JOB: {
                data = org.apache.doris.load.loadv2.LoadJob.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_END_LOAD_JOB: {
                data = LoadJobFinalOperation.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_UPDATE_LOAD_JOB: {
                data = LoadJobStateUpdateInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_CREATE_SYNC_JOB: {
                data = SyncJob.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_UPDATE_SYNC_JOB_STATE: {
                data = SyncJob.SyncJobUpdateStateInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_FETCH_STREAM_LOAD_RECORD: {
                data = FetchStreamLoadRecord.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_CREATE_RESOURCE:
            case OperationType.OP_ALTER_RESOURCE: {
                data = Resource.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_DROP_RESOURCE: {
                data = DropResourceOperationLog.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_CREATE_SMALL_FILE:
            case OperationType.OP_DROP_SMALL_FILE: {
                data = SmallFile.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_ALTER_JOB_V2: {
                data = AlterJobV2.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_UPDATE_COOLDOWN_CONF: {
                data = CooldownConfList.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_COOLDOWN_DELETE: {
                data = CooldownDelete.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_BATCH_ADD_ROLLUP: {
                data = BatchAlterJobPersistInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_MODIFY_DISTRIBUTION_TYPE: {
                data = TableInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_SET_REPLICA_STATUS: {
                data = SetReplicaStatusOperationLog.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_DYNAMIC_PARTITION:
            case OperationType.OP_MODIFY_IN_MEMORY:
            case OperationType.OP_MODIFY_REPLICATION_NUM:
            case OperationType.OP_UPDATE_BINLOG_CONFIG: {
                data = ModifyTablePropertyOperationLog.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_MODIFY_DISTRIBUTION_BUCKET_NUM: {
                data = ModifyTableDefaultDistributionBucketNumOperationLog.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_REPLACE_TEMP_PARTITION: {
                data = ReplacePartitionOperationLog.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_INSTALL_PLUGIN: {
                data = PluginInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_UNINSTALL_PLUGIN: {
                data = PluginInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_REMOVE_ALTER_JOB_V2: {
                data = RemoveAlterJobV2OperationLog.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_MODIFY_COMMENT: {
                data = ModifyCommentOperationLog.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_ALTER_ROUTINE_LOAD_JOB: {
                data = AlterRoutineLoadJobOperationLog.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_GLOBAL_VARIABLE_V2: {
                data = GlobalVarPersistInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_REPLACE_TABLE: {
                data = ReplaceTableOperationLog.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_CREATE_SQL_BLOCK_RULE: {
                data = SqlBlockRule.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_ALTER_SQL_BLOCK_RULE: {
                data = SqlBlockRule.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_DROP_SQL_BLOCK_RULE: {
                data = DropSqlBlockRuleOperationLog.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_MODIFY_TABLE_ENGINE: {
                data = ModifyTableEngineOperationLog.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_CREATE_POLICY: {
                data = Policy.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_DROP_POLICY: {
                data = DropPolicyLog.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_ALTER_STORAGE_POLICY: {
                data = StoragePolicy.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_CREATE_CATALOG:
            case OperationType.OP_DROP_CATALOG:
            case OperationType.OP_ALTER_CATALOG_NAME:
            case OperationType.OP_ALTER_CATALOG_PROPS:
            case OperationType.OP_REFRESH_CATALOG: {
                data = CatalogLog.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_INIT_CATALOG: {
                data = InitCatalogLog.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_INIT_EXTERNAL_DB: {
                data = InitDatabaseLog.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_INIT_EXTERNAL_TABLE: {
                data = InitTableLog.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_REFRESH_EXTERNAL_DB:
            case OperationType.OP_DROP_EXTERNAL_TABLE:
            case OperationType.OP_CREATE_EXTERNAL_TABLE:
            case OperationType.OP_DROP_EXTERNAL_DB:
            case OperationType.OP_CREATE_EXTERNAL_DB:
            case OperationType.OP_ADD_EXTERNAL_PARTITIONS:
            case OperationType.OP_DROP_EXTERNAL_PARTITIONS:
            case OperationType.OP_REFRESH_EXTERNAL_PARTITIONS:
            case OperationType.OP_REFRESH_EXTERNAL_TABLE: {
                data = ExternalObjectLog.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_MODIFY_TABLE_LIGHT_SCHEMA_CHANGE: {
                data = TableAddOrDropColumnsInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_MODIFY_TABLE_ADD_OR_DROP_INVERTED_INDICES: {
                data = TableAddOrDropInvertedIndicesInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_INVERTED_INDEX_JOB: {
                data = IndexChangeJob.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_CLEAN_LABEL: {
                data = CleanLabelOperationLog.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_CREATE_MTMV_JOB: {
                data = MTMVJob.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_DROP_MTMV_JOB: {
                data = DropMTMVJob.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_CHANGE_MTMV_JOB: {
                data = ChangeMTMVJob.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_CREATE_MTMV_TASK: {
                data = MTMVTask.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_DROP_MTMV_TASK: {
                data = DropMTMVTask.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_CHANGE_MTMV_TASK: {
                Text.readString(in);
                isRead = true;
                break;
            }
            case OperationType.OP_ALTER_MTMV_STMT: {
                data = AlterMultiMaterializedView.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_ALTER_USER: {
                data = AlterUserOperationLog.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_CREATE_WORKLOAD_GROUP:
            case OperationType.OP_ALTER_WORKLOAD_GROUP: {
                data = WorkloadGroup.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_DROP_WORKLOAD_GROUP: {
                data = DropWorkloadGroupOperationLog.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_ALTER_LIGHT_SCHEMA_CHANGE: {
                data = AlterLightSchemaChangeInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_CLEAN_QUERY_STATS: {
                data = CleanQueryStatsInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_CREATE_ANALYSIS_JOB: {
                data = AnalysisInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_CREATE_ANALYSIS_TASK: {
                data = AnalysisInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_DELETE_ANALYSIS_JOB: {
                data = AnalyzeDeletionLog.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_DELETE_ANALYSIS_TASK: {
                data = AnalyzeDeletionLog.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_ALTER_DATABASE_PROPERTY: {
                data = AlterDatabasePropertyInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_GC_BINLOG: {
                data = BinlogGcInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_BARRIER: {
                data = BarrierLog.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_UPDATE_TABLE_STATS:
            case OperationType.OP_PERSIST_AUTO_JOB:
            case OperationType.OP_DELETE_TABLE_STATS: {
                isRead = true;
                break;
            }
            default: {
                IOException e = new IOException();
                LOG.error("UNKNOWN Operation Type {}", opCode, e);
                throw e;
            }
        } // end switch
        Preconditions.checkState(isRead);
    }
}
