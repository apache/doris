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

suite("group_bit") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    def table = "group_bit_and_or_xor"
    sql """ CREATE TABLE if not exists ${table} (
        `k` int(11) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k`) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        )"""
    
    sql "insert into ${table} values (44), (28)"

    qt_select "select group_bit_and(k) from ${table}"
    qt_select "select group_bit_or(k) from ${table}"
    qt_select "select group_bit_xor(k) from ${table}"
    sql "drop table ${table}"

}