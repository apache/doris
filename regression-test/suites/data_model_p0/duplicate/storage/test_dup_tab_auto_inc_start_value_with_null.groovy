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

suite("test_dup_table_auto_inc_start_value_with_null") {

    // auto-increment column is key, don't specify auto-inc column in stream load
    def table1 = "test_dup_table_auto_inc_start_value_basic_key_with_null"
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
        set 'columns', 'id, name, value'

        file 'auto_inc_basic_with_null.csv'
        time 10000 // limit inflight 10s
    }
    sql """sync"""
    qt_auto_inc_ids "select * from ${table1};"
    sql "drop table if exists ${table1};"


    // auto-increment column is key, some of the values is null, some is valid value
    def table2 = "test_dup_table_auto_inc_start_value_basic_key_with_null_2"
    sql "drop table if exists ${table2}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table2}` (
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
        table "${table2}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'id, name, value'

        file 'auto_inc_basic_with_null_2.csv'
        time 10000 // limit inflight 10s
    }
    sql """sync"""
    qt_auto_inc_ids "select * from ${table2};"
    sql "drop table if exists ${table2};"


    // auto-increment column is value, don't specify auto-inc column in stream load
    def table3 = "test_dup_table_auto_inc_start_value_basic_value_with_null"
    sql "drop table if exists ${table3}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table3}` (
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
        table "${table3}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'id, name, value'

        file 'auto_inc_basic_with_null.csv'
        time 10000 // limit inflight 10s
    }
    sql """sync"""
    qt_auto_inc_ids "select * from ${table3} order by id;"
    sql "drop table if exists ${table3};"


    // auto-increment column is value, some of the values is null, some is valid value
    def table4 = "test_dup_table_auto_inc_start_value_basic_value_with_null_2"
    sql "drop table if exists ${table4}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table4}` (
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
        table "${table4}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'id, name, value'

        file 'auto_inc_basic_with_null_2.csv'
        time 10000 // limit inflight 10s
    }
    sql """sync"""
    qt_auto_inc_ids "select * from ${table4} order by id;"
    sql "drop table if exists ${table4};"

    // insert stmt
    def table5 = "test_dup_table_auto_inc_start_value_basic_insert_stmt"
    sql "drop table if exists ${table5}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table5}` (
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
    sql """insert into ${table5} values
    (null, "Bob", 100),
    (null, "Alice", 200),
    (null, "Tom", 300),
    (null, "Test", 400),
    (null, "Carter", 500),
    (null, "Smith", 600),
    (null, "Beata", 700),
    (null, "Doris", 800),
    (null, "Nereids", 900);"""
    qt_sql "select * from ${table5} order by id;"

    def table6 = "test_dup_table_auto_inc_start_value_basic_insert_stmt2"
    def table7 = "test_dup_table_auto_inc_start_value_basic_insert_stmt3"
    sql "drop table if exists ${table6}"
    sql """
          CREATE TABLE IF NOT EXISTS `${table6}` (  
            `id` BIGINT NOT NULL AUTO_INCREMENT(10000) COMMENT "",
            `value` int(11) NOT NULL COMMENT ""
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
    sql "drop table if exists ${table7}"
    sql """
          CREATE TABLE IF NOT EXISTS `${table7}` (  
            `x` BIGINT NOT NULL,
            `y` BIGINT NOT NULL
          ) ENGINE=OLAP
          DUPLICATE KEY(`x`)
          COMMENT "OLAP"
          DISTRIBUTED BY HASH(`x`) BUCKETS 1
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "in_memory" = "false",
          "storage_format" = "V2"
          )
      """
    sql "insert into ${table7} values(0,0),(1,1),(2,2),(3,3),(4,4),(5,5);"
    sql "insert into ${table6} select null, y from ${table7};"
    qt_sql "select * from ${table6} order by id"

    def table8 = "test_dup_table_auto_inc_start_value_basic_insert_stmt4"
    sql "drop table if exists ${table8}"
    sql """
          CREATE TABLE IF NOT EXISTS `${table8}` (  
            `id` BIGINT NOT NULL COMMENT "",
            `value` BIGINT NOT NULL AUTO_INCREMENT(10000) COMMENT ""
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
    sql "insert into ${table8} select x, null from ${table7};"
    qt_sql "select * from ${table8} order by id"
}
