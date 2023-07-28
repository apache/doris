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

suite("test_unique_table_auto_inc") {
    
    // auto-increment column is key
    def table1 = "test_unique_tab_auto_inc_col_basic_key"
    sql "drop table if exists ${table1}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table1}` (
          `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT "用户 ID",
          `name` varchar(65533) NOT NULL COMMENT "用户姓名",
          `value` int(11) NOT NULL COMMENT "用户得分"
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "enable_unique_key_merge_on_write" = "true"
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
    qt_sql "select * from ${table1};"
    sql """ insert into ${table1} values(0, "Bob", 123), (2, "Tom", 323), (4, "Carter", 523);"""
    qt_sql "select * from ${table1} order by id"
    sql "drop table if exists ${table1};"

    // auto-increment column is value
    def table2 = "test_unique_tab_auto_inc_col_basic_value"
    sql "drop table if exists ${table2}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table2}` (
          `name` varchar(65533) NOT NULL COMMENT "用户姓名",
          `value` int(11) NOT NULL COMMENT "用户得分",
          `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT "用户 ID"
        ) ENGINE=OLAP
        UNIQUE KEY(`name`, `value`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`name`, `value`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "enable_unique_key_merge_on_write" = "true"
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
    qt_sql "select id, name, value from ${table2} order by id;"
    sql """ insert into ${table2} values("Bob", 100, 1230), ("Tom", 300, 1232), ("Carter", 500, 1234);"""
    qt_sql "select id, name, value from ${table2} order by id;"
    sql "drop table if exists ${table2};"

    // auto inc key with null values
    def table3 = "test_unique_tab_auto_inc_col_key_with_null"
    sql "drop table if exists ${table3}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table3}` (
          `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT "用户 ID",
          `name` varchar(65533) NOT NULL COMMENT "用户姓名",
          `value` int(11) NOT NULL COMMENT "用户得分"
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "enable_unique_key_merge_on_write" = "true"
        )
    """
    streamLoad {
        table "${table3}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'id, name, value'

        file 'auto_inc_with_null.csv'
        time 10000 // limit inflight 10s
    }
    qt_sql "select * from ${table3};"
    sql """ insert into ${table3} values(0, "Bob", 123), (2, "Tom", 323), (4, "Carter", 523);"""
    qt_sql "select * from ${table3} order by id"
    sql "drop table if exists ${table3};"

    // dircetly update rows in one batch
    def table4 = "test_unique_tab_auto_inc_col_key_with_null"
    sql "drop table if exists ${table4}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table4}` (
          `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT "用户 ID",
          `name` varchar(65533) NOT NULL COMMENT "用户姓名",
          `value` int(11) NOT NULL COMMENT "用户得分"
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "enable_unique_key_merge_on_write" = "true"
        )
    """
    streamLoad {
        table "${table4}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'id, name, value'

        file 'auto_inc_update_inplace.csv'
        time 10000 // limit inflight 10s
    }
    qt_update_inplace "select * from ${table4};"
    sql "drop table if exists ${table4};"

    // test for partial update, auto inc col is key
    def table5 = "test_unique_tab_auto_inc_col_key_partial_update"
    sql "drop table if exists ${table5}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table5}` (
          `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT "用户 ID",
          `name` varchar(65533) NOT NULL COMMENT "用户姓名",
          `value` int(11) NOT NULL COMMENT "用户得分"
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "enable_unique_key_merge_on_write" = "true"
        )
    """
    streamLoad {
        table "${table5}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'name, value'

        file 'auto_inc_basic.csv'
        time 10000 // limit inflight 10s
    }
    qt_partial_update_key "select * from ${table5} order by id;"

    streamLoad {
        table "${table5}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'id, value'
        set 'partial_columns', 'true'

        file 'auto_inc_partial_update1.csv'
        time 10000
    }
    qt_partial_update_key "select * from ${table5} order by id;"
    sql "drop table if exists ${table5};"

    // test for partial update, auto inc col is value, update auto inc col
    def table6 = "test_unique_tab_auto_inc_col_value_partial_update"
    sql "drop table if exists ${table6}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table6}` (
          `name` varchar(65533) NOT NULL COMMENT "用户姓名",
          `value` int(11) NOT NULL COMMENT "用户得分",
          `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT "用户 ID"
        ) ENGINE=OLAP
        UNIQUE KEY(`name`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`name`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "enable_unique_key_merge_on_write" = "true"
        )
    """
    streamLoad {
        table "${table6}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'name, value'

        file 'auto_inc_basic.csv'
        time 10000 // limit inflight 10s
    }
    qt_partial_update_value "select * from ${table6} order by id;"

    streamLoad {
        table "${table6}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'name, id'
        set 'partial_columns', 'true'

        file 'auto_inc_partial_update2.csv'
        time 10000
    }
    qt_partial_update_value "select * from ${table6} order by id;"
    sql "drop table if exists ${table6};"

    // test for partial update, auto inc col is value, update other col
    def table7 = "test_unique_tab_auto_inc_col_value_partial_update"
    sql "drop table if exists ${table7}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table7}` (
          `name` varchar(65533) NOT NULL COMMENT "用户姓名",
          `value` int(11) NOT NULL COMMENT "用户得分",
          `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT "用户 ID"
        ) ENGINE=OLAP
        UNIQUE KEY(`name`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`name`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "enable_unique_key_merge_on_write" = "true"
        )
    """
    streamLoad {
        table "${table7}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'name, value'

        file 'auto_inc_basic.csv'
        time 10000 // limit inflight 10s
    }
    qt_partial_update_value "select * from ${table7} order by id;"

    streamLoad {
        table "${table7}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'name, value'
        set 'partial_columns', 'true'

        file 'auto_inc_partial_update2.csv'
        time 10000
    }
    qt_partial_update_value "select * from ${table7} order by id;"
    sql "drop table if exists ${table7};"
}

