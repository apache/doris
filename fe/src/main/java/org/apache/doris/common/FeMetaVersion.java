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
    // jira 1635 dynamice fe
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

    // paralle exec param and batch size
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
}
