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
suite("one_phase") {
    sql """
    drop table if exists users;
    
    CREATE TABLE `users` (
        `UserID` bigint NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`UserID`)
    DISTRIBUTED BY HASH(`UserID`) BUCKETS 48
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "min_load_replica_num" = "-1",
    "is_being_synced" = "false",
    "storage_medium" = "hdd",
    "storage_format" = "V2",
    "inverted_index_storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false",
    "group_commit_interval_ms" = "10000",
    "group_commit_data_bytes" = "134217728"
    ); 
    
    insert into users values (11111),(11112),(11113);
 
    """
    
    sql "set sort_phase_num=1;"
    qt_1 "select userid from users order by userid limit 2, 109000000;"

    sql "set sort_phase_num=2;"
    qt_2 "select userid from users order by userid limit 2, 109000000;"

}