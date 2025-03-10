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
suite("cse") {
    sql """
    drop table if exists cse;
    CREATE TABLE `cse` (
    `k1` int NOT NULL,
    d   datev2,
    i  int
    ) ENGINE=OLAP
    DUPLICATE KEY(`k1`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 3
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

    insert into cse values (1, '20240101', 100);
    """

    explain {
        sql """
            physical plan
            select sum(    
                case when k1 between i and 
                    cast(from_unixtime(
                    unix_timestamp(date_add(
                                from_unixtime(unix_timestamp(cast(d as string), 'yyyyMMdd')),
                                INTERVAL 5 DAY)), 'yyyyMMdd') as int) 
                THEN 1
                ELSE 0
                end) as c1,
                sum(
                        case when k1 between i and 
                                    cast(from_unixtime(
                                    unix_timestamp(date_add(
                                            from_unixtime(unix_timestamp(cast(d as string), 'yyyyMMdd')),
                                            INTERVAL 2 DAY)), 'yyyyMMdd') as int) 
                            THEN 1
                            ELSE 0
                            end) as c2
            from cse
            group by d;
            """
        contains("l1([k1#0, d#1, i#2, (k1 >= i)#9, cast(d as TEXT)#10, unix_timestamp(cast(d as TEXT)#10, '%Y%m%d') AS `unix_timestamp(cast(d as TEXT), '%Y%m%d')`#11])")
    }
    
}

