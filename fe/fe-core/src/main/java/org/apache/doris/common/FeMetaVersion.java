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

package org.apache.doris.common;

public final class FeMetaVersion {

    public static final int VERSION_1 = 1;

    public static final int VERSION_2 = 2;

    public static final int VERSION_3 = 3;

    public static final int VERSION_4 = 4;

    public static final int VERSION_5 = 5;

    public static final int VERSION_6 = 6;

    public static final int VERSION_7 = 7;
    // jira 1902
    public static final int VERSION_8 = 8;
    // jira 529
    public static final int VERSION_9 = 9;
    // jira 1772 1792 1834
    public static final int VERSION_10 = 10;
    // jira 1978
    public static final int VERSION_11 = 11;
    // jira 1981
    public static final int VERSION_12 = 12;
    // jira 2245 load priority
    public static final int VERSION_15 = 15;
    // jira 1635 dynamic fe
    public static final int VERSION_16 = 16;
    // jira 1988 backup and restore
    public static final int VERSION_17 = 17;
    // jira 2190 support null
    public static final int VERSION_18 = 18;
    // jira 2288 async delete
    public static final int VERSION_19 = 19;
    // jira 2399 global session variables persistence
    public static final int VERSION_20 = 20;
    // ca 2288
    public static final int VERSION_21 = 21;
    // for compatible
    public static final int VERSION_22 = 22;

    // LoadJob timestamp
    public static final int VERSION_23 = 23;

    // load job error hub
    public static final int VERSION_24 = 24;

    // Palo3.0 (general model/multi tenant)
    public static final int VERSION_30 = 30;

    // Palo3.1 image version
    public static final int VERSION_31 = 31;

    // Palo3.2
    public static final int VERSION_32 = 32;

    // persist decommission type
    public static final int VERSION_33 = 33;

    // persist LoadJob's execMemLimit
    public static final int VERSION_34 = 34;

    // update the BE in cluster, because of forgetting
    // to remove backend in cluster when drop backend or 
    // decommission in latest versions.
    public static final int VERSION_35 = 35;

    // persist diskAvailableCapacity
    public static final int VERSION_36 = 36;

    // added collation_server to variables (palo-3059)
    public static final int VERSION_37 = 37;

    // parallel exec param and batch size
    public static final int VERSION_38 = 38;

    // schema change support row to column
    public static final int VERSION_39 = 39;

    // persistent brpc port in Backend
    public static final int VERSION_40 = 40;

    // change the way to name Frontend
    public static final int VERSION_41 = 41;

    // new backup and restore
    public static final int VERSION_42 = 42;

    // new privilege management
    public static final int VERSION_43 = 43;

    // streaming load
    public static final int VERSION_45 = 45;

    // colocate join
    public static final int VERSION_46 = 46;

    // UDF
    public static final int VERSION_47 = 47;

    // replica schema hash
    public static final int VERSION_48 = 48;
    // routine load job
    public static final int VERSION_49 = 49;
    // load job v2 for broker load
    public static final int VERSION_50 = 50;
    // kafka custom properties
    public static final int VERSION_51 = 51;
    // small files
    public static final int VERSION_52 = 52;
    // Support exec_mem_limit in ExportJob
    public static final int VERSION_53 = 53;
    // support strict mode, change timeout to long, and record txn id in load job
    public static final int VERSION_54 = 54;
    // modify colocation join
    public static final int VERSION_55 = 55;
    // persist auth info in load job
    public static final int VERSION_56 = 56;
    // for base index using different id
    public static final int VERSION_57 = 57;
    // broker load support function, persist origin stmt in broker load
    public static final int VERSION_58 = 58;
    // support strict mode in routine load and stream load
    public static final int VERSION_59 = 59;
    // refactor date literal
    public static final int VERSION_60 = 60;
    // for alter job v2
    public static final int VERSION_61 = 61;
    // add param: doris_shuffle_partitions
    public static final int VERSION_62 = 62;
    // for table comment
    public static final int VERSION_63 = 63;
    // for table create time
    public static final int VERSION_64 = 64;

    // support sql mode, change sql_mode from string to long
    public static final int VERSION_65 = 65;
    // routine load/stream load persist session variables
    public static final int VERSION_66 = 66;
    // load_mem_limit session variable
    public static final int VERSION_67 = 67;
    // for es table context
    public static final int VERSION_68 = 68;
    // modify password checking logic
    public static final int VERSION_69 = 69;
    // for indexes
    public static final int VERSION_70 = 70;
    // dynamic partition
    public static final int VERSION_71 = 71;
    // in memory table
    public static final int VERSION_72 = 72;
    // broker persist isAlive
    public static final int VERSION_73 = 73;
    // temp partitions
    public static final int VERSION_74 = 74;
    // support materialized index meta while there is different keys type in different materialized index
    public static final int VERSION_75 = 75;
    // multi statement
    public static final int VERSION_76 = 76;
    // load to temp partitions
    public static final int VERSION_77 = 77;
    // plugin support
    public static final int VERSION_78 = 78;
    // for transaction state in table level
    public static final int VERSION_79 = 79;
    // optimize alterJobV2 memory consumption
    public static final int VERSION_80 = 80;
    // replica quota support
    public static final int VERSION_81 = 81;
    // optimize delete job
    public static final int VERSION_82 = 82;
    // modify TransactionState Field
    public static final int VERSION_83 = 83;
    // add storage format in schema change job
    public static final int VERSION_84 = 84;
    // add storage format in rollup job
    public static final int VERSION_85 = 85;
    // serialize origStmt in rollupJob and mv meta
    public static final int VERSION_86 = 86;
    // spark resource, resource privilege, broker file group for hive table
    public static final int VERSION_87 = 87;
    // add partition visibleVersionTime
    public static final int VERSION_88 = 88;
    // force drop db, force drop table, force drop partition
    // make force drop operation do not recycle meta
    public static final int VERSION_89 = 89;
    // for global variable persist
    public static final int VERSION_90 = 90;
    // sparkLoadAppHandle
    public static final int VERSION_91 = 91;
    // for mysql external table support resource
    public static final int VERSION_92 = 92;
    //jira: 4863 for load job support udf
    public static final int VERSION_93 = 93;
    // refactor load job property persist method
    public static final int VERSION_94 = 94;
    // serialize resources in restore job
    public static final int VERSION_95 = 95;
    // support delete without partition
    public static final int VERSION_96 = 96;
    // persist orig stmt of export job
    public static final int VERSION_97 = 97;
    // add list partition
    public static final int VERSION_98 = 98;
    // add audit steam load and change the serialization backend method to json
    public static final int VERSION_99 = 99;
    // for max query instance
    public static final int VERSION_100 = 100;
    // add errorRowsAfterResumed to distinguish totalErrorRows and currentErrorRows even if the job is paused.
    public static final int VERSION_101 = 101;
    // add data encrypt
    public static final int VERSION_102 = 102;
    // support sync job
    public static final int VERSION_103 = 103;
    // add sql block rule to deny specified sql
    public static final int VERSION_104 = 104;
    // change replica to replica allocation
    public static final int VERSION_105 = 105;
    // add ldap info
    public static final int VERSION_106 = 106;
    // add query stats
    public static final int VERSION_107 = 107;

    // note: when increment meta version, should assign the latest version to VERSION_CURRENT
    public static final int VERSION_CURRENT = VERSION_107;
}
