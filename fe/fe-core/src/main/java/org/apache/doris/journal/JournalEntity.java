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

import org.apache.doris.alter.AlterJob;
import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.alter.BatchAlterJobPersistInfo;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.backup.BackupJob;
import org.apache.doris.backup.Repository;
import org.apache.doris.backup.RestoreJob;
import org.apache.doris.catalog.BrokerMgr;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionSearchDesc;
import org.apache.doris.catalog.Resource;
import org.apache.doris.cluster.BaseParam;
import org.apache.doris.cluster.Cluster;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.SmallFileMgr.SmallFile;
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
import org.apache.doris.master.Checkpoint;
import org.apache.doris.mysql.privilege.UserPropertyInfo;
import org.apache.doris.persist.AlterRoutineLoadJobOperationLog;
import org.apache.doris.persist.AlterViewInfo;
import org.apache.doris.persist.BackendIdsUpdateInfo;
import org.apache.doris.persist.BackendTabletsInfo;
import org.apache.doris.persist.BatchDropInfo;
import org.apache.doris.persist.BatchModifyPartitionsInfo;
import org.apache.doris.persist.BatchRemoveTransactionsOperation;
import org.apache.doris.persist.ClusterInfo;
import org.apache.doris.persist.ColocatePersistInfo;
import org.apache.doris.persist.ConsistencyCheckInfo;
import org.apache.doris.persist.CreateTableInfo;
import org.apache.doris.persist.DatabaseInfo;
import org.apache.doris.persist.DropDbInfo;
import org.apache.doris.persist.DropInfo;
import org.apache.doris.persist.DropLinkDbAndUpdateDbInfo;
import org.apache.doris.persist.DropPartitionInfo;
import org.apache.doris.persist.DropResourceOperationLog;
import org.apache.doris.persist.GlobalVarPersistInfo;
import org.apache.doris.persist.HbPackage;
import org.apache.doris.persist.ModifyPartitionInfo;
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
import org.apache.doris.persist.TableInfo;
import org.apache.doris.persist.TablePropertyInfo;
import org.apache.doris.persist.TruncateTableInfo;
import org.apache.doris.plugin.PluginInfo;
import org.apache.doris.qe.SessionVariable;
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
    public static final Logger LOG = LogManager.getLogger(Checkpoint.class);

    private short opCode;
    private Writable data;

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
                data = new Database();
                ((Database) data).readFields(in);
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
                data = new DropInfo();
                ((DropInfo) data).readFields(in);
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
                data = new RecoverInfo();
                ((RecoverInfo) data).readFields(in);
                isRead = true;
                break;
            }
            case OperationType.OP_START_ROLLUP:
            case OperationType.OP_FINISHING_ROLLUP:
            case OperationType.OP_FINISHING_SCHEMA_CHANGE:
            case OperationType.OP_FINISH_ROLLUP:
            case OperationType.OP_CANCEL_ROLLUP:
            case OperationType.OP_START_SCHEMA_CHANGE:
            case OperationType.OP_FINISH_SCHEMA_CHANGE:
            case OperationType.OP_CANCEL_SCHEMA_CHANGE:
            case OperationType.OP_START_DECOMMISSION_BACKEND:
            case OperationType.OP_FINISH_DECOMMISSION_BACKEND: {
                data = AlterJob.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_DROP_ROLLUP: {
                data = new DropInfo();
                ((DropInfo) data).readFields(in);
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
                data = new ExportJob();
                ((ExportJob) data).readFields(in);
                isRead = true;
                break;
            case OperationType.OP_EXPORT_UPDATE_STATE:
                data = new ExportJob.StateTransfer();
                ((ExportJob.StateTransfer) data).readFields(in);
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
            case OperationType.OP_BACKEND_STATE_CHANGE: {
                data = Backend.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_ADD_FRONTEND:
            case OperationType.OP_ADD_FIRST_FRONTEND:
            case OperationType.OP_REMOVE_FRONTEND: {
                data = new Frontend();
                ((Frontend) data).readFields(in);
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
            case OperationType.OP_UPDATE_USER_PROPERTY: {
                data = UserPropertyInfo.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_MASTER_INFO_CHANGE: {
                data = new MasterInfo();
                ((MasterInfo) data).readFields(in);
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
            case OperationType.OP_GLOBAL_VARIABLE: {
                data = new SessionVariable();
                ((SessionVariable) data).readFields(in);
                isRead = true;
                break;
            }
            case OperationType.OP_CREATE_CLUSTER: {
                data = Cluster.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_DROP_CLUSTER:
            case OperationType.OP_EXPAND_CLUSTER: {
                data = new ClusterInfo();
                ((ClusterInfo) data).readFields(in);
                isRead = true;
                break;
            }
            case OperationType.OP_LINK_CLUSTER:
            case OperationType.OP_MIGRATE_CLUSTER: {
                data = new BaseParam();
                ((BaseParam) data).readFields(in);
                isRead = true;
                break;
            }
            case OperationType.OP_UPDATE_DB: {
                data = new DatabaseInfo();
                ((DatabaseInfo) data).readFields(in);
                isRead = true;
                break;
            }
            case OperationType.OP_DROP_LINKDB: {
                data = new DropLinkDbAndUpdateDbInfo();
                ((DropLinkDbAndUpdateDbInfo) data).readFields(in);
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
            case OperationType.OP_UPDATE_CLUSTER_AND_BACKENDS: {
                data = new BackendIdsUpdateInfo();
                ((BackendIdsUpdateInfo) data).readFields(in);
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
                data = new TablePropertyInfo();
                ((TablePropertyInfo) data).readFields(in);
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
            case OperationType.OP_BACKEND_TABLETS_INFO: {
                data = BackendTabletsInfo.read(in);
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
            case OperationType.OP_FETCH_STREAM_LOAD_RECORD: {
                data = FetchStreamLoadRecord.read(in);
                isRead = true;
                break;
            }
            case OperationType.OP_CREATE_RESOURCE: {
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
            case OperationType.OP_MODIFY_REPLICATION_NUM: {
                data = ModifyTablePropertyOperationLog.read(in);
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
            default: {
                IOException e = new IOException();
                LOG.error("UNKNOWN Operation Type {}", opCode, e);
                throw e;
            }
        } // end switch
        Preconditions.checkState(isRead);
    }
}

