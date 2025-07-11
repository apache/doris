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


suite("variant_rqg_fix1", "p0") {

    sql """ drop table if exists table_200_undef_partitions2_keys3_properties4_distributed_by5 """
    sql """
        CREATE TABLE `table_200_undef_partitions2_keys3_properties4_distributed_by5` (
        `pk` int NULL,
        `var` variant NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`pk`)
        DISTRIBUTED BY HASH(`pk`) BUCKETS 10
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
        """

     sql """ insert into table_200_undef_partitions2_keys3_properties4_distributed_by5 values(20, '{"k1" : 1, "k2" : "str", "k3" : 3, "k4" : "str2"}') """

     qt_sql """ select count() from table_200_undef_partitions2_keys3_properties4_distributed_by5 where cast(var['k2'] as string) > 'bea' """
}
    

