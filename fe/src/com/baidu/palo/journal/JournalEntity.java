// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.journal;

import com.baidu.palo.alter.AlterJob;
import com.baidu.palo.backup.BackupJob;
import com.baidu.palo.backup.RestoreJob;
import com.baidu.palo.catalog.BrokerMgr;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.UserProperty;
import com.baidu.palo.cluster.BaseParam;
import com.baidu.palo.cluster.Cluster;
import com.baidu.palo.common.io.Text;
import com.baidu.palo.common.io.Writable;
import com.baidu.palo.ha.MasterInfo;
import com.baidu.palo.journal.bdbje.Timestamp;
import com.baidu.palo.load.AsyncDeleteJob;
import com.baidu.palo.load.DeleteInfo;
import com.baidu.palo.load.ExportJob;
import com.baidu.palo.load.LoadErrorHub;
import com.baidu.palo.load.LoadJob;
import com.baidu.palo.master.Checkpoint;
import com.baidu.palo.persist.CloneInfo;
import com.baidu.palo.persist.ClusterInfo;
import com.baidu.palo.persist.ConsistencyCheckInfo;
import com.baidu.palo.persist.CreateTableInfo;
import com.baidu.palo.persist.DatabaseInfo;
import com.baidu.palo.persist.DropInfo;
import com.baidu.palo.persist.DropLinkDbAndUpdateDbInfo;
import com.baidu.palo.persist.DropPartitionInfo;
import com.baidu.palo.persist.LinkDbInfo;
import com.baidu.palo.persist.ModifyPartitionInfo;
import com.baidu.palo.persist.OperationType;
import com.baidu.palo.persist.PartitionPersistInfo;
import com.baidu.palo.persist.RecoverInfo;
import com.baidu.palo.persist.ReplicaPersistInfo;
import com.baidu.palo.persist.TableInfo;
import com.baidu.palo.persist.UpdateClusterAndBackends;
import com.baidu.palo.qe.SessionVariable;
import com.baidu.palo.system.Backend;
import com.baidu.palo.system.Frontend;

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
            case OperationType.OP_CLEAR_ROLLUP_INFO: {
                data = new ReplicaPersistInfo();
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
                data = new BackupJob();
                break;
            }
            case OperationType.OP_RESTORE_START:
            case OperationType.OP_RESTORE_FINISH: {
                data = new RestoreJob();
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
            case OperationType.OP_DELETE_REPLICA: {
                data = new ReplicaPersistInfo();
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
            case OperationType.OP_SET_LOAD_ERROR_URL: {
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
                data = new Cluster();
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
            case OperationType.OP_LINK_CLUSTER: {
                data = new BaseParam();
                break;
            }
            case OperationType.OP_MIGRATE_CLUSTER: {
                data = new BaseParam();
                break;
            }
            case OperationType.OP_MODIFY_CLUSTER: {
                data = new ClusterInfo();
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
                data = new UpdateClusterAndBackends();
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
