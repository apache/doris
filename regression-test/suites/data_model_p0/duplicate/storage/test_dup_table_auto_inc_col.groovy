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

suite("test_dup_table_auto_inc_col") {

    // duplicate table with a auto-increment key column
    def table1 = "test_dup_tab_auto_inc_col1"
    sql "drop table if exists ${table1}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table1}` (
          `id` int(11) NOT NULL AUTO_INCREMENT COMMENT "",
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
    qt_sql "show create table ${table1};"

    // duplicate table with a auto-increment value column
    def table2 = "test_dup_tab_auto_inc_col2"
    sql "drop table if exists ${table2}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table2}` (
          `id` int(11) NOT NULL COMMENT "",
          `value` int(11) NOT NULL AUTO_INCREMENT COMMENT ""
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
    qt_sql "show create table ${table2};"

    // duplicate table with two auto-increment columns
    def table3 = "test_dup_tab_auto_inc_col3"
    sql "drop table if exists ${table3}"
    test {
        sql """
            CREATE TABLE IF NOT EXISTS `${table3}` (
              `id` int(11) NOT NULL AUTO_INCREMENT COMMENT "",
              `value` int(11) NOT NULL AUTO_INCREMENT COMMENT ""
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
    def table4 = "test_dup_tab_basic_int_tab1"
    sql "drop table if exists ${table4}"
    sql """
CREATE TABLE IF NOT EXISTS `${table4}` (
  `siteid` int(11) NOT NULL AUTO_INCREMENT COMMENT "",
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
    test {
        sql """
            select siteid from ${table4} order by siteid
        """
        result([[1],[1],[5],[5],[9],[9],[13],[13],[13],[13],[17],[17]])
    }

    // read non-key column
    test {
        sql """
            select citycode from ${table4} order by citycode
        """
        result([[2], [2], [6], [6], [10], [10], [14], [14], [18], [18], [21], [21]])
    }

    // read key, key is predicate
    test {
        sql """
            select siteid from ${table4} where siteid = 13
        """
        result([[13], [13], [13], [13]])
    }

    test {
        sql """
            select siteid from ${table4} where siteid != 13 order by siteid
        """
        result([[1],[1],[5],[5],[9],[9],[17],[17]])
    }

    // read non-key, non-key is predicate
    test {
        sql """
            select citycode from ${table4} where citycode = 14
        """
        result([[14], [14]])
    }

    test {
        sql """
            select citycode from ${table4} where citycode != 14 order by citycode
        """
        result([[2],[2],[6],[6],[10],[10],[18],[18],[21],[21]])
    }

    // read key, predicate is non-key
    test {
        sql """
            select siteid from ${table4} where citycode = 14
        """
        result([[13], [13]])
    }

    test{
        sql """
            select siteid from ${table4} where citycode != 14 order by siteid
        """
        result([[1],[1],[5],[5],[9],[9],[13],[13],[17],[17]])
    }

    // read non-key, predicate is key
    test {
        sql """
            select citycode from ${table4} where siteid = 13 order by citycode
        """
        result([[14], [14], [21], [21]])
    }

    test {
        sql """
            select citycode from ${table4} where siteid != 13 order by citycode
        """
        result([[2], [2], [6], [6], [10], [10], [18], [18]])
    }

    test {
        sql """
            select siteid,citycode from ${table4} where siteid = 13 order by siteid,citycode
        """
        result([[13, 14], [13, 14], [13, 21], [13, 21]])
    }

    test {
        sql """
            select siteid,citycode from ${table4} where siteid != 13 order by siteid,citycode
        """
        result([[1,2],[1,2],[5,6],[5,6],[9,10],[9,10],[17,18],[17,18]])
    }


    // common read
    test {
        sql """
            select siteid,citycode,userid,pv from ${table4} order by siteid,citycode,pv limit 1
        """
        result([[1,2,3,4]])
    }

    // predicate in with two values
    test {
        sql """
            select distinct userid as col from ${table4} where userid in (3,7) order by col;
        """
        result([[3],[7]])
    }

    // predicate not with two values
    test {
        sql """
            select distinct userid as col from ${table4} where userid not in (3,7) order by col;
        """
        result([[11],[15],[19],[22]])
    }

    sql "drop table if exists ${table4}"

    // duplicate table with a auto-increment value column
    // insert values and check query
    def table5 = "test_dup_tab_basic_int_tab2"
    sql "drop table if exists ${table5}"
    sql """
CREATE TABLE IF NOT EXISTS `${table5}` (
  `siteid` int(11) NOT NULL COMMENT "",
  `citycode` int(11) NOT NULL AUTO_INCREMENT COMMENT "",
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
    test {
        sql """
            select siteid from ${table5} order by siteid
        """
        result([[1],[1],[5],[5],[9],[9],[13],[13],[13],[13],[17],[17]])
    }

    // read non-key column
    test {
        sql """
            select citycode from ${table5} order by citycode
        """
        result([[2], [2], [6], [6], [10], [10], [14], [14], [18], [18], [21], [21]])
    }

    // read key, key is predicate
    test {
        sql """
            select siteid from ${table5} where siteid = 13
        """
        result([[13], [13], [13], [13]])
    }

    test {
        sql """
            select siteid from ${table5} where siteid != 13 order by siteid
        """
        result([[1],[1],[5],[5],[9],[9],[17],[17]])
    }

    // read non-key, non-key is predicate
    test {
        sql """
            select citycode from ${table5} where citycode = 14
        """
        result([[14], [14]])
    }

    test {
        sql """
            select citycode from ${table5} where citycode != 14 order by citycode
        """
        result([[2],[2],[6],[6],[10],[10],[18],[18],[21],[21]])
    }

    // read key, predicate is non-key
    test {
        sql """
            select siteid from ${table5} where citycode = 14
        """
        result([[13], [13]])
    }

    test{
        sql """
            select siteid from ${table5} where citycode != 14 order by siteid
        """
        result([[1],[1],[5],[5],[9],[9],[13],[13],[17],[17]])
    }

    // read non-key, predicate is key
    test {
        sql """
            select citycode from ${table5} where siteid = 13 order by citycode
        """
        result([[14], [14], [21], [21]])
    }

    test {
        sql """
            select citycode from ${table5} where siteid != 13 order by citycode
        """
        result([[2], [2], [6], [6], [10], [10], [18], [18]])
    }

    test {
        sql """
            select siteid,citycode from ${table5} where siteid = 13 order by siteid,citycode
        """
        result([[13, 14], [13, 14], [13, 21], [13, 21]])
    }

    test {
        sql """
            select siteid,citycode from ${table5} where siteid != 13 order by siteid,citycode
        """
        result([[1,2],[1,2],[5,6],[5,6],[9,10],[9,10],[17,18],[17,18]])
    }


    // common read
    test {
        sql """
            select siteid,citycode,userid,pv from ${table5} order by siteid,citycode,pv limit 1
        """
        result([[1,2,3,4]])
    }

    // predicate in with two values
    test {
        sql """
            select distinct userid as col from ${table5} where userid in (3,7) order by col;
        """
        result([[3],[7]])
    }

    // predicate not with two values
    test {
        sql """
            select distinct userid as col from ${table5} where userid not in (3,7) order by col;
        """
        result([[11],[15],[19],[22]])
    }

    sql "drop table if exists ${table5}"

    def table_check = "test_dup_tab_auto_inc_col_check"
    sql "drop table if exists ${table_check}"
    test {
        sql """
            CREATE TABLE IF NOT EXISTS `${table_check}` (
              `id` int(11) AUTO_INCREMENT COMMENT "",
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
              `id` VARCHAR NOT NULL AUTO_INCREMENT COMMENT "",
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
        exception "the auto increment should be integer type."
    }

    sql "drop table if exists ${table_check}"
    test {
        sql """
            CREATE TABLE IF NOT EXISTS `${table_check}` (
              `id` VARCHAR NOT NULL AUTO_INCREMENT DEFAULT "0" COMMENT "",
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
}