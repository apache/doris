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
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.backup.BackupJob;
import org.apache.doris.backup.BackupJob_D;
import org.apache.doris.backup.Repository;
import org.apache.doris.backup.RestoreJob;
import org.apache.doris.backup.RestoreJob_D;
import org.apache.doris.catalog.BrokerMgr;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionSearchDesc;
import org.apache.doris.cluster.BaseParam;
import org.apache.doris.cluster.Cluster;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.SmallFileMgr.SmallFile;
import org.apache.doris.ha.MasterInfo;
import org.apache.doris.journal.bdbje.Timestamp;
import org.apache.doris.load.AsyncDeleteJob;
import org.apache.doris.load.DeleteInfo;
import org.apache.doris.load.ExportJob;
import org.apache.doris.load.LoadErrorHub;
import org.apache.doris.load.LoadJob;
import org.apache.doris.load.loadv2.LoadJobFinalOperation;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.master.Checkpoint;
import org.apache.doris.mysql.privilege.UserProperty;
import org.apache.doris.mysql.privilege.UserPropertyInfo;
import org.apache.doris.persist.BackendIdsUpdateInfo;
import org.apache.doris.persist.BackendTabletsInfo;
import org.apache.doris.persist.CloneInfo;
import org.apache.doris.persist.ClusterInfo;
import org.apache.doris.persist.ColocatePersistInfo;
import org.apache.doris.persist.ConsistencyCheckInfo;
import org.apache.doris.persist.CreateTableInfo;
import org.apache.doris.persist.DatabaseInfo;
import org.apache.doris.persist.DropInfo;
import org.apache.doris.persist.DropLinkDbAndUpdateDbInfo;
import org.apache.doris.persist.DropPartitionInfo;
import org.apache.doris.persist.HbPackage;
import org.apache.doris.persist.ModifyPartitionInfo;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.PartitionPersistInfo;
import org.apache.doris.persist.PrivInfo;
import org.apache.doris.persist.RecoverInfo;
import org.apache.doris.persist.ReplicaPersistInfo;
import org.apache.doris.persist.RoutineLoadOperation;
import org.apache.doris.persist.TableInfo;
import org.apache.doris.persist.TablePropertyInfo;
import org.apache.doris.persist.TruncateTableInfo;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Frontend;
import org.apache.doris.transaction.TransactionState;

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

    @Override
    public void readFields(DataInput in) throws IOException {
        boolean needRead = true;
        opCode = in.readShort();
        LOG.debug("get opcode: {}", opCode);
        switch (opCode) {
            case OperationType.OP_SAVE_NEXTID: {
                data = new Text();
                break;
            }
            case OperationType.OP_SAVE_TRANSACTION_ID: {
                data = new Text();
                break;
            }
            case OperationType.OP_CREATE_DB: {
                data = new Database();
                break;
            }
            case OperationType.OP_DROP_DB: {
                data = new Text();
                break;
            }
            case OperationType.OP_ALTER_DB:
            case OperationType.OP_RENAME_DB: {
                data = new DatabaseInfo();
                break;
            }
            case OperationType.OP_CREATE_TABLE: {
                data = new CreateTableInfo();
                break;
            }
            case OperationType.OP_DROP_TABLE: {
                data = new DropInfo();
                break;
            }
            case OperationType.OP_ADD_PARTITION: {
                data = new PartitionPersistInfo();
                break;
            }
            case OperationType.OP_DROP_PARTITION: {
                data = new DropPartitionInfo();
                break;
            }
            case OperationType.OP_MODIFY_PARTITION: {
                data = new ModifyPartitionInfo();
                break;
            }
            case OperationType.OP_ERASE_DB:
            case OperationType.OP_ERASE_TABLE:
            case OperationType.OP_ERASE_PARTITION: {
                data = new Text();
                break;
            }
            case OperationType.OP_RECOVER_DB:
            case OperationType.OP_RECOVER_TABLE:
            case OperationType.OP_RECOVER_PARTITION: {
                data = new RecoverInfo();
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
                needRead = false;
                break;
            }
            case OperationType.OP_DROP_ROLLUP: {
                data = new DropInfo();
                break;
            }
            case OperationType.OP_RENAME_TABLE:
            case OperationType.OP_RENAME_ROLLUP:
            case OperationType.OP_RENAME_PARTITION: {
                data = new TableInfo();
                break;
            }
            case OperationType.OP_BACKUP_START:
            case OperationType.OP_BACKUP_FINISH_SNAPSHOT:
            case OperationType.OP_BACKUP_FINISH: {
                data = new BackupJob_D();
                break;
            }
            case OperationType.OP_RESTORE_START:
            case OperationType.OP_RESTORE_FINISH: {
                data = new RestoreJob_D();
                break;
            }
            case OperationType.OP_BACKUP_JOB: {
                data = BackupJob.read(in);
                needRead = false;
                break;
            }
            case OperationType.OP_RESTORE_JOB: {
                data = RestoreJob.read(in);
                needRead = false;
                break;
            }
            case OperationType.OP_FINISH_CONSISTENCY_CHECK: {
                data = new ConsistencyCheckInfo();
                break;
            }
            case OperationType.OP_LOAD_START:
            case OperationType.OP_LOAD_ETL:
            case OperationType.OP_LOAD_LOADING:
            case OperationType.OP_LOAD_QUORUM:
            case OperationType.OP_LOAD_DONE:
            case OperationType.OP_LOAD_CANCEL: {
                data = new LoadJob();
                break;
            }
            case OperationType.OP_EXPORT_CREATE:
                data = new ExportJob();
                break;
            case OperationType.OP_EXPORT_UPDATE_STATE:
                data = new ExportJob.StateTransfer();
                break;
            case OperationType.OP_FINISH_SYNC_DELETE: {
                data = new DeleteInfo();
                break;
            }
            case OperationType.OP_FINISH_ASYNC_DELETE: {
                data = AsyncDeleteJob.read(in);
                needRead = false;
                break;
            }
            case OperationType.OP_CLONE_DONE: {
                data = new CloneInfo();
                break;
            }
            case OperationType.OP_ADD_REPLICA:
            case OperationType.OP_UPDATE_REPLICA:
            case OperationType.OP_DELETE_REPLICA:
            case OperationType.OP_CLEAR_ROLLUP_INFO: {
                data = ReplicaPersistInfo.read(in);
                needRead = false;
                break;
            }
            case OperationType.OP_ADD_BACKEND:
            case OperationType.OP_DROP_BACKEND:
            case OperationType.OP_BACKEND_STATE_CHANGE: {
                data = new Backend();
                break;
            }
            case OperationType.OP_ADD_FRONTEND:
            case OperationType.OP_ADD_FIRST_FRONTEND:
            case OperationType.OP_REMOVE_FRONTEND: {
                data = new Frontend();
                break;
            }
            case OperationType.OP_SET_LOAD_ERROR_HUB: {
                data = new LoadErrorHub.Param();
                break;
            }
            case OperationType.OP_ALTER_ACCESS_RESOURCE: {
                data = new UserProperty();
                break;
            }
            case OperationType.OP_DROP_USER: {
                data = new Text();
                break;
            }
            case OperationType.OP_NEW_DROP_USER: {
                data = UserIdentity.read(in);
                needRead = false;
                break;
            }
            case OperationType.OP_CREATE_USER:
            case OperationType.OP_GRANT_PRIV:
            case OperationType.OP_REVOKE_PRIV:
            case OperationType.OP_SET_PASSWORD:
            case OperationType.OP_CREATE_ROLE:
            case OperationType.OP_DROP_ROLE: {
                data = PrivInfo.read(in);
                needRead = false;
                break;
            }
            case OperationType.OP_UPDATE_USER_PROPERTY: {
                data = UserPropertyInfo.read(in);
                needRead = false;
                break;
            }
            case OperationType.OP_MASTER_INFO_CHANGE: {
                data = new MasterInfo();
                break;
            }
            case OperationType.OP_TIMESTAMP: {
                data = new Timestamp();
                break;
            }
            case OperationType.OP_META_VERSION: {
                data = new Text();
                break;
            }
            case OperationType.OP_GLOBAL_VARIABLE: {
                data = new SessionVariable();
                break;
            }
            case OperationType.OP_CREATE_CLUSTER: {
                data = Cluster.read(in);
                needRead = false;
                break;
            }
            case OperationType.OP_DROP_CLUSTER: {
                data = new ClusterInfo();
                break;
            }
            case OperationType.OP_UPDATE_CLUSTER: {
                data = new ClusterInfo();
                break;
            }
            case OperationType.OP_EXPAND_CLUSTER: {
                data = new ClusterInfo();
                break;
            }
            case OperationType.OP_LINK_CLUSTER: {
                data = new BaseParam();
                break;
            }
            case OperationType.OP_MIGRATE_CLUSTER: {
                data = new BaseParam();
                break;
            }
            case OperationType.OP_UPDATE_DB: {
                data = new DatabaseInfo();
                break;
            }
            case OperationType.OP_DROP_LINKDB: {
                data = new DropLinkDbAndUpdateDbInfo();
                break;
            }
            case OperationType.OP_ADD_BROKER:
            case OperationType.OP_DROP_BROKER: {
                data = new BrokerMgr.ModifyBrokerInfo();
                break;
            }
            case OperationType.OP_DROP_ALL_BROKER: {
                data = new Text();
                break;
            }
            case OperationType.OP_UPDATE_CLUSTER_AND_BACKENDS: {
                data = new BackendIdsUpdateInfo();
                break;
            }
            case OperationType.OP_UPSERT_TRANSACTION_STATE:
            case OperationType.OP_DELETE_TRANSACTION_STATE: {
                data = new TransactionState();
                break;
            }
            case OperationType.OP_CREATE_REPOSITORY: {
                data = Repository.read(in);
                needRead = false;
                break;
            }
            case OperationType.OP_DROP_REPOSITORY: {
                data = new Text();
                break;
            }

            case OperationType.OP_TRUNCATE_TABLE: {
                data = TruncateTableInfo.read(in);
                needRead = false;
                break;
            }

            case OperationType.OP_COLOCATE_ADD_TABLE:
            case OperationType.OP_COLOCATE_REMOVE_TABLE:
            case OperationType.OP_COLOCATE_BACKENDS_PER_BUCKETSEQ:
            case OperationType.OP_COLOCATE_MARK_UNSTABLE:
            case OperationType.OP_COLOCATE_MARK_STABLE: {
                data = new ColocatePersistInfo();
                break;
            }
            case OperationType.OP_MODIFY_TABLE_COLOCATE: {
                data = new TablePropertyInfo();
                break;
            }
            case OperationType.OP_HEARTBEAT: {
                data = HbPackage.read(in);
                needRead = false;
                break;
            }
            case OperationType.OP_ADD_FUNCTION: {
                data = Function.read(in);
                needRead = false;
                break;
            }
            case OperationType.OP_DROP_FUNCTION: {
                data = FunctionSearchDesc.read(in);
                needRead = false;
                break;
            }
            case OperationType.OP_BACKEND_TABLETS_INFO: {
                data = BackendTabletsInfo.read(in);
                needRead = false;
                break;
            }
            case OperationType.OP_CREATE_ROUTINE_LOAD_JOB: {
                data = RoutineLoadJob.read(in);
                needRead = false;
                break;
            }
            case OperationType.OP_CHANGE_ROUTINE_LOAD_JOB:
            case OperationType.OP_REMOVE_ROUTINE_LOAD_JOB: {
                data = RoutineLoadOperation.read(in);
                needRead = false;
                break;
            }
            case OperationType.OP_CREATE_LOAD_JOB: {
                data = org.apache.doris.load.loadv2.LoadJob.read(in);
                needRead = false;
                break;
            }
            case OperationType.OP_END_LOAD_JOB: {
                data = LoadJobFinalOperation.read(in);
                needRead = false;
                break;
            }
            case OperationType.OP_CREATE_SMALL_FILE:
            case OperationType.OP_DROP_SMALL_FILE: {
                data = SmallFile.read(in);
                needRead = false;
                break;
            }
            default: {
                IOException e = new IOException();
                LOG.error("UNKNOWN Operation Type {}", opCode, e);
                throw e;
            }
        }

        if (needRead) {
            data.readFields(in);
        }
    }
}
