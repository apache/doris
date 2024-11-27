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

suite("test_dup_table_auto_inc_start_value_basic") {
    
    // auto-increment column is key
    def table1 = "test_dup_tab_auto_inc_col_start_value_basic_key"
    sql "drop table if exists ${table1}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table1}` (
          `id` BIGINT NOT NULL AUTO_INCREMENT(10000) COMMENT "用户 ID",
          `name` varchar(65533) NOT NULL COMMENT "用户姓名",
          `value` int(11) NOT NULL COMMENT "用户得分"
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
        set 'columns', 'name, value'

        file 'auto_inc_basic.csv'
        time 10000 // limit inflight 10s
    }
    sql " sync "
    qt_auto_inc_ids "select * from ${table1};"
    sql "drop table if exists ${table1};"


    // auto-increment column is value
    def table2 = "test_dup_tab_auto_inc_col_start_value_basic_value"
    sql "drop table if exists ${table2}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table2}` (
          `name` varchar(65533) NOT NULL COMMENT "用户姓名",
          `value` int(11) NOT NULL COMMENT "用户得分",
          `id` BIGINT NOT NULL AUTO_INCREMENT(10000) COMMENT "用户 ID"
        ) ENGINE=OLAP
        DUPLICATE KEY(`name`, `value`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`name`, `value`) BUCKETS 1
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
        set 'columns', 'name, value'

        file 'auto_inc_basic.csv'
        time 10000 // limit inflight 10s
    }
    sql " sync "
    qt_auto_inc_ids "select * from ${table2} order by id;"
    sql "drop table if exists ${table2};"

    // auto-increment start value can be 0
    def table3 = "test_dup_tab_auto_inc_col_start_value_0_basic_key"
    sql "drop table if exists ${table3}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table3}` (
          `id` BIGINT NOT NULL AUTO_INCREMENT(0) COMMENT "用户 ID",
          `name` varchar(65533) NOT NULL COMMENT "用户姓名",
          `value` int(11) NOT NULL COMMENT "用户得分"
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
        set 'columns', 'name, value'

        file 'auto_inc_basic.csv'
        time 10000 // limit inflight 10s
    }
    sql " sync "
    qt_auto_inc_ids "select * from ${table3};"
    sql "drop table if exists ${table3};"

}