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

suite("test_dup_table_auto_inc_start_value_col") {

    def table1 = "test_dup_tab_auto_inc_start_value_col1"
    sql "drop table if exists ${table1}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table1}` (
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
    qt_desc "desc ${table1}"
    def res = sql "show create table ${table1};"
    assertTrue(res.size() != 0)

    // duplicate table with a auto-increment value column
    def table2 = "test_dup_tab_auto_inc_start_value_col2"
    sql "drop table if exists ${table2}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table2}` (
          `id` int(11) NOT NULL COMMENT "",
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
    qt_desc "desc ${table2}"
    res = sql "show create table ${table2};"
    assertTrue(res.size() != 0)

    // duplicate table with two auto-increment columns
    def table3 = "test_dup_tab_auto_inc_start_value_col3"
    sql "drop table if exists ${table3}"
    test {
        sql """
            CREATE TABLE IF NOT EXISTS `${table3}` (
              `id` BIGINT NOT NULL AUTO_INCREMENT(10000) COMMENT "",
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
        exception "there can be at most one auto increment column in OlapTable."
    }

    // duplicate table with a auto-increment key column
    // insert values and check query
    def table4 = "test_dup_tab_start_value_basic_int_tab1"
    sql "drop table if exists ${table4}"
    sql """
CREATE TABLE IF NOT EXISTS `${table4}` (
  `siteid` BIGINT NOT NULL AUTO_INCREMENT(10000) COMMENT "",
  `citycode` int(11) NOT NULL COMMENT "",
  `userid` int(11) NOT NULL COMMENT "",
  `pv` int(11) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`siteid`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`siteid`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2"
)

"""
    sql """insert into ${table4} values
        (9,10,11,12),
        (9,10,11,12),
        (1,2,3,4),
        (13,21,22,16),
        (13,14,15,16),
        (17,18,19,20),
        (1,2,3,4),
        (13,21,22,16),
        (13,14,15,16),
        (17,18,19,20),
        (5,6,7,8),
        (5,6,7,8)
"""
    // read key column
    qt_sql """select siteid from ${table4} order by siteid"""    
    // read non-key column
    sql """select citycode from ${table4} order by citycode"""
    sql "drop table if exists ${table4}"

    // duplicate table with a auto-increment value column
    // insert values and check query
    def table5 = "test_dup_tab_start_value_basic_int_tab2"
    sql "drop table if exists ${table5}"
    sql """
CREATE TABLE IF NOT EXISTS `${table5}` (
  `siteid` int(11) NOT NULL COMMENT "",
  `citycode` BIGINT NOT NULL AUTO_INCREMENT(10000) COMMENT "",
  `userid` int(11) NOT NULL COMMENT "",
  `pv` int(11) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`siteid`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`siteid`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2"
) """
    sql """insert into ${table5} values
        (9,10,11,12),
        (9,10,11,12),
        (1,2,3,4),
        (13,21,22,16),
        (13,14,15,16),
        (17,18,19,20),
        (1,2,3,4),
        (13,21,22,16),
        (13,14,15,16),
        (17,18,19,20),
        (5,6,7,8),
        (5,6,7,8) """

    // read key column
    sql """select siteid from ${table5} order by siteid"""

    // read non-key column
    sql """select citycode from ${table5} order by citycode"""
    sql "drop table if exists ${table5}"

    def table_check = "test_dup_tab_auto_inc_start_value_col_check"
    sql "drop table if exists ${table_check}"

    test {
        sql """
            CREATE TABLE IF NOT EXISTS `${table_check}` (
              `id` BIGINT AUTO_INCREMENT(10000) COMMENT "",
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
        exception "the auto increment column should be NOT NULL."
    }

    sql "drop table if exists ${table_check}"
    test {
        sql """
            CREATE TABLE IF NOT EXISTS `${table_check}` (
              `id` VARCHAR NOT NULL AUTO_INCREMENT(10000) COMMENT "",
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
        exception "the auto increment must be BIGINT type."
    }

    sql "drop table if exists ${table_check}"
    test {
        sql """
            CREATE TABLE IF NOT EXISTS `${table_check}` (
              `id` BIGINT NOT NULL AUTO_INCREMENT DEFAULT "0" COMMENT "",
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
        exception "the auto increment column can't have default value."
    }

    sql "drop table if exists ${table_check}"
    test {
        sql """
            CREATE TABLE IF NOT EXISTS `${table_check}` (
              `id` BIGINT NOT NULL AUTO_INCREMENT(-1)  COMMENT "",
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
        exception "AUTO_INCREMENT start value can not be negative."
    }

    sql "drop table if exists ${table_check}"
    try {
        sql """
            CREATE TABLE IF NOT EXISTS `${table_check}` (
              AUTO_INCREMENT BIGINT NOT NULL AUTO_INCREMENT(100) COMMENT "",
              VALUE int(11) NOT NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(AUTO_INCREMENT)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(AUTO_INCREMENT) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            )
        """
    } catch (Exception e){
        // no exception
        assertTrue(false)
    } finally{

    }
}