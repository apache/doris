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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * Operation name and code mapping.
 **/
public class OperationType {
    // OP_LOCAL_EOF is only for local edit log, to indicate the end of a edit log run.
    public static final short OP_LOCAL_EOF = -1;
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
    public static final short OP_RENAME_COLUMN = 115;
    public static final short OP_BACKUP_JOB = 116;
    public static final short OP_RESTORE_JOB = 117;
    public static final short OP_TRUNCATE_TABLE = 118;
    public static final short OP_MODIFY_VIEW_DEF = 119;
    public static final short OP_REPLACE_TEMP_PARTITION = 210;
    public static final short OP_BATCH_MODIFY_PARTITION = 211;
    public static final short OP_REPLACE_TABLE = 212;

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
    public static final short OP_ALTER_JOB_V2 = 121;
    public static final short OP_MODIFY_DISTRIBUTION_TYPE = 122;
    public static final short OP_BATCH_ADD_ROLLUP = 123;
    public static final short OP_BATCH_DROP_ROLLUP = 124;
    public static final short OP_REMOVE_ALTER_JOB_V2 = 125;
    public static final short OP_MODIFY_COMMENT = 126;
    public static final short OP_MODIFY_TABLE_ENGINE = 127;

    //schema change for add and drop columns
    public static final short OP_MODIFY_TABLE_LIGHT_SCHEMA_CHANGE = 128;

    //schema change for add and drop inverted indices
    public static final short OP_MODIFY_TABLE_ADD_OR_DROP_INVERTED_INDICES = 220;
    public static final short OP_INVERTED_INDEX_JOB = 221;

    // 30~39 130~139 230~239 ...
    // load job for only hadoop load
    public static final short OP_LOAD_START = 30;
    public static final short OP_LOAD_ETL = 31;
    public static final short OP_LOAD_LOADING = 32;
    public static final short OP_LOAD_QUORUM = 33;
    public static final short OP_LOAD_DONE = 34;
    public static final short OP_LOAD_CANCEL = 35;
    public static final short OP_EXPORT_CREATE = 36;
    public static final short OP_EXPORT_UPDATE_STATE = 37;
    public static final short OP_CLEAN_LABEL = 38;

    @Deprecated
    public static final short OP_FINISH_SYNC_DELETE = 40;
    public static final short OP_FINISH_DELETE = 41;
    public static final short OP_ADD_REPLICA = 42;
    public static final short OP_DELETE_REPLICA = 43;
    @Deprecated
    public static final short OP_FINISH_ASYNC_DELETE = 44;
    public static final short OP_UPDATE_REPLICA = 45;
    @Deprecated
    public static final short OP_BACKEND_TABLETS_INFO = 46;
    public static final short OP_SET_REPLICA_STATUS = 47;
    public static final short OP_BACKEND_REPLICAS_INFO = 48;

    public static final short OP_ADD_BACKEND = 50;
    public static final short OP_DROP_BACKEND = 51;
    public static final short OP_BACKEND_STATE_CHANGE = 52;
    public static final short OP_START_DECOMMISSION_BACKEND = 53;
    public static final short OP_FINISH_DECOMMISSION_BACKEND = 54;
    public static final short OP_ADD_FRONTEND = 55;
    public static final short OP_ADD_FIRST_FRONTEND = 56;
    public static final short OP_REMOVE_FRONTEND = 57;
    public static final short OP_SET_LOAD_ERROR_HUB = 58;
    public static final short OP_HEARTBEAT = 59;
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
    @Deprecated
    // replaced by OP_GLOBAL_VARIABLE_V2
    public static final short OP_GLOBAL_VARIABLE = 73;

    @Deprecated
    // remove be removed in 3.0
    public static final short OP_CREATE_CLUSTER = 74;
    public static final short OP_DROP_CLUSTER = 75;
    public static final short OP_EXPAND_CLUSTER = 76;
    public static final short OP_MIGRATE_CLUSTER = 77;
    public static final short OP_LINK_CLUSTER = 78;
    public static final short OP_ENTER_CLUSTER = 79;
    public static final short OP_SHOW_CLUSTERS = 80;
    public static final short OP_UPDATE_DB = 82;
    public static final short OP_DROP_LINKDB = 83;
    public static final short OP_GLOBAL_VARIABLE_V2 = 84;

    public static final short OP_ADD_BROKER = 85;
    public static final short OP_DROP_BROKER = 86;
    public static final short OP_DROP_ALL_BROKER = 87;
    public static final short OP_UPDATE_CLUSTER_AND_BACKENDS = 88;
    public static final short OP_CREATE_REPOSITORY = 89;
    public static final short OP_DROP_REPOSITORY = 90;
    public static final short OP_MODIFY_BACKEND = 91;

    public static final short OP_MODIFY_FRONTEND = 92;

    //colocate table
    public static final short OP_COLOCATE_ADD_TABLE = 94;
    public static final short OP_COLOCATE_REMOVE_TABLE = 95;
    public static final short OP_COLOCATE_BACKENDS_PER_BUCKETSEQ = 96;
    public static final short OP_COLOCATE_MARK_UNSTABLE = 97;
    public static final short OP_COLOCATE_MARK_STABLE = 98;
    public static final short OP_MODIFY_TABLE_COLOCATE = 99;

    //real time load 100 -108
    public static final short OP_UPSERT_TRANSACTION_STATE = 100;
    @Deprecated
    // use OP_BATCH_REMOVE_TXNS_V2 instead
    public static final short OP_DELETE_TRANSACTION_STATE = 101;
    public static final short OP_FINISHING_ROLLUP = 102;
    public static final short OP_FINISHING_SCHEMA_CHANGE = 103;
    public static final short OP_SAVE_TRANSACTION_ID = 104;
    @Deprecated
    // use OP_BATCH_REMOVE_TXNS_V2 instead
    public static final short OP_BATCH_REMOVE_TXNS = 105;
    public static final short OP_BATCH_REMOVE_TXNS_V2 = 106;

    // routine load 110~120
    public static final short OP_ROUTINE_LOAD_JOB = 110;
    public static final short OP_ALTER_ROUTINE_LOAD_JOB = 111;

    // UDF 130-140
    public static final short OP_ADD_FUNCTION = 130;
    public static final short OP_DROP_FUNCTION = 131;
    public static final short OP_ADD_GLOBAL_FUNCTION = 132;
    public static final short OP_DROP_GLOBAL_FUNCTION = 133;

    // routine load 200
    public static final short OP_CREATE_ROUTINE_LOAD_JOB = 200;
    public static final short OP_CHANGE_ROUTINE_LOAD_JOB = 201;
    public static final short OP_REMOVE_ROUTINE_LOAD_JOB = 202;

    // load job v2 for broker load 230~250
    public static final short OP_CREATE_LOAD_JOB = 230;
    // this finish op include finished and cancelled
    public static final short OP_END_LOAD_JOB = 231;
    // update job info, used by spark load
    public static final short OP_UPDATE_LOAD_JOB = 232;
    // fetch stream load record
    public static final short OP_FETCH_STREAM_LOAD_RECORD = 233;
    // create sync job
    public static final short OP_CREATE_SYNC_JOB = 234;
    // update sync job state
    public static final short OP_UPDATE_SYNC_JOB_STATE = 235;

    // small files 251~260
    public static final short OP_CREATE_SMALL_FILE = 251;
    public static final short OP_DROP_SMALL_FILE = 252;
    public static final short OP_CREATE_ENCRYPTKEY = 253;
    public static final short OP_DROP_ENCRYPTKEY = 254;

    // dynamic partition 261~265
    public static final short OP_DYNAMIC_PARTITION = 261;

    // set table replication_num config 266
    public static final short OP_MODIFY_REPLICATION_NUM = 266;
    // set table in memory (confusion)
    // The actual opcode now represents
    // modify table properties: inMemory, StoragePolicy, IsBeingSynced, CompactionPolicy,
    // TimeSeriesCompactionFileCountThreshold, SeriesCompactionTimeThresholdSeconds,
    // SkipWriteIndexOnLoad, EnableSingleReplicaCompaction.
    public static final short OP_MODIFY_IN_MEMORY = 267;

    // set table default distribution bucket num
    public static final short OP_MODIFY_DISTRIBUTION_BUCKET_NUM = 268;

    // plugin 270~275
    public static final short OP_INSTALL_PLUGIN = 270;

    public static final short OP_UNINSTALL_PLUGIN = 271;

    // resource 276~290
    public static final short OP_CREATE_RESOURCE = 276;
    public static final short OP_DROP_RESOURCE = 277;
    public static final short OP_ALTER_RESOURCE = 278;

    // alter external table
    public static final short OP_ALTER_EXTERNAL_TABLE_SCHEMA = 280;

    public static final short OP_SET_LDAP_PASSWORD = 290;

    // sql block rule 300-310
    public static final short OP_CREATE_SQL_BLOCK_RULE = 300;
    public static final short OP_ALTER_SQL_BLOCK_RULE = 301;
    public static final short OP_DROP_SQL_BLOCK_RULE = 302;

    // policy 310-320
    public static final short OP_CREATE_POLICY = 310;
    public static final short OP_DROP_POLICY = 311;
    public static final short OP_ALTER_STORAGE_POLICY = 312;

    // catalog 320-330
    public static final short OP_CREATE_CATALOG = 320;
    public static final short OP_DROP_CATALOG = 321;
    public static final short OP_ALTER_CATALOG_NAME = 322;
    public static final short OP_ALTER_CATALOG_PROPS = 323;
    public static final short OP_REFRESH_CATALOG = 324;
    public static final short OP_INIT_CATALOG = 325;
    public static final short OP_REFRESH_EXTERNAL_DB = 326;
    public static final short OP_INIT_EXTERNAL_DB = 327;
    public static final short OP_REFRESH_EXTERNAL_TABLE = 328;
    @Deprecated
    public static final short OP_INIT_EXTERNAL_TABLE = 329;

    // scheduler job and task 330-350
    public static final short OP_CREATE_MTMV_JOB = 330;
    public static final short OP_DROP_MTMV_JOB = 331;
    public static final short OP_CHANGE_MTMV_JOB = 332;

    public static final short OP_CREATE_MTMV_TASK = 340;
    public static final short OP_DROP_MTMV_TASK = 341;
    @Deprecated
    public static final short OP_CHANGE_MTMV_TASK = 342;

    public static final short OP_ALTER_MTMV_STMT = 345;

    public static final short OP_DROP_EXTERNAL_TABLE = 350;
    public static final short OP_DROP_EXTERNAL_DB = 351;
    public static final short OP_CREATE_EXTERNAL_TABLE = 352;
    public static final short OP_CREATE_EXTERNAL_DB = 353;
    public static final short OP_ADD_EXTERNAL_PARTITIONS = 354;
    public static final short OP_DROP_EXTERNAL_PARTITIONS = 355;
    public static final short OP_REFRESH_EXTERNAL_PARTITIONS = 356;

    public static final short OP_ALTER_USER = 400;
    // cooldown related
    public static final short OP_UPDATE_COOLDOWN_CONF = 401;
    public static final short OP_COOLDOWN_DELETE = 402;
    public static final short OP_ALTER_LIGHT_SCHEMA_CHANGE = 403;

    // workload group 410 ~ 419
    public static final short OP_CREATE_WORKLOAD_GROUP = 410;
    public static final short OP_DROP_WORKLOAD_GROUP = 411;
    public static final short OP_ALTER_WORKLOAD_GROUP = 412;

    // query stats 440 ~ 424
    public static final short OP_CLEAN_QUERY_STATS = 420;

    // update binlog config
    public static final short OP_UPDATE_BINLOG_CONFIG = 425;

    public static final short OP_CREATE_ANALYSIS_TASK = 430;

    public static final short OP_DELETE_ANALYSIS_TASK = 431;

    public static final short OP_CREATE_ANALYSIS_JOB = 432;

    public static final short OP_DELETE_ANALYSIS_JOB = 433;

    public static final short OP_ALTER_DATABASE_PROPERTY = 434;

    public static final short OP_GC_BINLOG = 435;

    public static final short OP_BARRIER = 436;

    // For backward compatible with 2.0.3
    public static final short OP_UPDATE_TABLE_STATS = 455;

    // For backward compatible with 2.0.3
    public static final short OP_PERSIST_AUTO_JOB = 456;

    // For backward compatible with 2.0.3
    public static final short OP_DELETE_TABLE_STATS = 457;

    public static final short OP_ALTER_CATALOG_COMMENT = 458;

    /**
     * Get opcode name by op code.
     **/
    public static String getOpName(short opCode) {
        try {
            Field[] fields = OperationType.class.getDeclaredFields();
            for (Field field : fields) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                short s = field.getShort(null);
                if (s != opCode) {
                    continue;
                }
                return field.getName();
            }
        } catch (Exception e) {
            return "Not Found: " + e.getMessage();
        }
        return "Not Found";
    }
}
