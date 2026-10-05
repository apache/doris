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

suite("test_dup_table_auto_inc_start_value_10000") {
    
    // auto-increment column is key
    def table1 = "test_dup_tab_auto_inc_start_value_10000_key"
    sql "drop table if exists ${table1}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table1}` (
          `id` BIGINT NOT NULL AUTO_INCREMENT(10000) COMMENT "用户 ID",
          `x` int(11) NOT NULL COMMENT "",
          `y` int(11) NOT NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        )
    """
    streamLoad {
        table "${table1}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'x, y'

        file 'auto_inc_10000.csv'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_count_max_min "select count(distinct id), max(id), min(id) from ${table1};"
    sql "drop table if exists ${table1};"

    // auto-increment column is value
    def table2 = "test_dup_tab_auto_inc_start_value_10000_value"
    sql "drop table if exists ${table2}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table2}` (
          `x` int(11) NOT NULL COMMENT "",
          `y` int(11) NOT NULL COMMENT "",
          `id` BIGINT NOT NULL AUTO_INCREMENT(10000) COMMENT "用户 ID"
        ) ENGINE=OLAP
        DUPLICATE KEY(`x`, `y`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`x`, `y`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        )
    """
    streamLoad {
        table "${table2}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'x, y'

        file 'auto_inc_10000.csv'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_count_max_min "select count(distinct id), max(id), min(id) from ${table2};"
    sql "drop table if exists ${table2};"

    sql "set batch_size = 4096;"
    def table3 = "test_dup_tab_auto_inc_start_value_10000_key_2"
    sql "drop table if exists ${table3}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table3}` (
          `id` BIGINT NOT NULL AUTO_INCREMENT(10000) COMMENT "用户 ID",
          `x` int(11) NOT NULL COMMENT "",
          `y` int(11) NOT NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        )
    """
    streamLoad {
        table "${table3}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'x, y'

        file 'auto_inc_10000.csv'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_count_max_min "select count(distinct id), max(id), min(id) from ${table3};"
    sql "drop table if exists ${table3};"
}
