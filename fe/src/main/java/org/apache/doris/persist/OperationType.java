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

public class OperationType {
    public static final short OP_INVALID = -1;
    public static final short OP_SAVE_NEXTID = 0;
    public static final short OP_CREATE_DB = 1;
    public static final short OP_DROP_DB = 2;
    public static final short OP_ALTER_DB = 3;
    public static final short OP_ERASE_DB = 4;
    public static final short OP_RECOVER_DB = 5;
    public static final short OP_RENAME_DB = 6;

    // 10~19 110~119 210~219 ...
    public static final short OP_CREATE_TABLE = 10;
    public static final short OP_DROP_TABLE = 11;
    public static final short OP_ADD_PARTITION = 12;
    public static final short OP_DROP_PARTITION = 13;
    public static final short OP_MODIFY_PARTITION = 14;
    public static final short OP_ERASE_TABLE = 15;
    public static final short OP_ERASE_PARTITION = 16;
    public static final short OP_RECOVER_TABLE = 17;
    public static final short OP_RECOVER_PARTITION = 18;
    public static final short OP_RENAME_TABLE = 19;
    public static final short OP_RENAME_PARTITION = 110;
    @Deprecated
    public static final short OP_BACKUP_START = 111;
    @Deprecated
    public static final short OP_BACKUP_FINISH_SNAPSHOT = 112;
    @Deprecated
    public static final short OP_BACKUP_FINISH = 113;
    @Deprecated
    public static final short OP_RESTORE_START = 114;
    @Deprecated
    public static final short OP_RESTORE_FINISH = 115;
    public static final short OP_BACKUP_JOB = 116;
    public static final short OP_RESTORE_JOB = 117;
    public static final short OP_TRUNCATE_TABLE = 118;

    // 20~29 120~129 220~229 ...
    public static final short OP_START_ROLLUP = 20;
    public static final short OP_FINISH_ROLLUP = 21;
    public static final short OP_CANCEL_ROLLUP = 23;
    public static final short OP_DROP_ROLLUP = 24;
    public static final short OP_START_SCHEMA_CHANGE = 25;
    public static final short OP_FINISH_SCHEMA_CHANGE = 26;
    public static final short OP_CANCEL_SCHEMA_CHANGE = 27;
    public static final short OP_CLEAR_ROLLUP_INFO = 28;
    public static final short OP_FINISH_CONSISTENCY_CHECK = 29;
    public static final short OP_RENAME_ROLLUP = 120;

    // 30~39 130~139 230~239 ...
    public static final short OP_LOAD_START = 30;
    public static final short OP_LOAD_ETL = 31;
    public static final short OP_LOAD_LOADING = 32;
    public static final short OP_LOAD_QUORUM = 33;
    public static final short OP_LOAD_DONE = 34;
    public static final short OP_LOAD_CANCEL = 35;
    public static final short OP_EXPORT_CREATE = 36;
    public static final short OP_EXPORT_UPDATE_STATE = 37;

    public static final short OP_FINISH_SYNC_DELETE = 40;
    @Deprecated
    // (cmy 2015-07-22)
    // do not use it anymore,use OP_ADD_REPLICA and OP_DELETE_REPLICA instead.
    // remove later
    public static final short OP_CLONE_DONE = 41;
    public static final short OP_ADD_REPLICA = 42;
    public static final short OP_DELETE_REPLICA = 43;
    public static final short OP_FINISH_ASYNC_DELETE = 44;

    public static final short OP_ADD_BACKEND = 50;
    public static final short OP_DROP_BACKEND = 51;
    public static final short OP_BACKEND_STATE_CHANGE = 52;
    public static final short OP_START_DECOMMISSION_BACKEND = 53;
    public static final short OP_FINISH_DECOMMISSION_BACKEND = 54;
    public static final short OP_ADD_FRONTEND = 55;
    public static final short OP_ADD_FIRST_FRONTEND = 56;
    public static final short OP_REMOVE_FRONTEND = 57;
    public static final short OP_SET_LOAD_ERROR_URL = 58;

    public static final short OP_ALTER_ACCESS_RESOURCE = 60;
    @Deprecated
    public static final short OP_DROP_USER = 61;
    public static final short OP_CREATE_USER = 62;
    public static final short OP_NEW_DROP_USER = 63;
    public static final short OP_GRANT_PRIV = 64;
    public static final short OP_REVOKE_PRIV = 65;
    public static final short OP_SET_PASSWORD = 66;
    public static final short OP_CREATE_ROLE = 67;
    public static final short OP_DROP_ROLE = 68;
    public static final short OP_UPDATE_USER_PROPERTY = 69;

    public static final short OP_TIMESTAMP = 70;
    public static final short OP_MASTER_INFO_CHANGE = 71;
    public static final short OP_META_VERSION = 72;

    public static final short OP_GLOBAL_VARIABLE = 73;

    public static final short OP_CREATE_CLUSTER = 74;
    public static final short OP_DROP_CLUSTER = 75;
    public static final short OP_EXPAND_CLUSTER = 76;
    public static final short OP_MIGRATE_CLUSTER = 77;
    public static final short OP_LINK_CLUSTER = 78;
    public static final short OP_ENTER_CLUSTER = 79;
    public static final short OP_SHOW_CLUSTERS = 80;
    @Deprecated
    public static final short OP_UPDATE_CLUSTER = 81;
    public static final short OP_UPDATE_DB = 82;
    public static final short OP_DROP_LINKDB = 83;

    public static final short OP_ADD_BROKER = 85;
    public static final short OP_DROP_BROKER = 86;
    public static final short OP_DROP_ALL_BROKER = 87;
    public static final short OP_UPDATE_CLUSTER_AND_BACKENDS = 88;
    public static final short OP_CREATE_REPOSITORY = 89;
    public static final short OP_DROP_REPOSITORY = 90;

    //real time load 100 -108
    public static final short OP_UPSERT_TRANSACTION_STATE = 100;
    public static final short OP_DELETE_TRANSACTION_STATE = 101;
    public static final short OP_FINISHING_ROLLUP = 102;
    public static final short OP_FINISHING_SCHEMA_CHANGE = 103;
    public static final short OP_SAVE_TRANSACTION_ID = 104;

    // routine load 110~120
    public static final short OP_ROUTINE_LOAD_JOB = 110;

}
