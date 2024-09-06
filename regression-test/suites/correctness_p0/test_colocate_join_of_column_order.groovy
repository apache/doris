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

suite("test_colocate_join_of_column_order") {
    sql """ DROP TABLE IF EXISTS `test_colocate_join_of_column_order_t1`; """
    // distributed by k1,k2
    sql """
        CREATE TABLE IF NOT EXISTS `test_colocate_join_of_column_order_t1` (
        `k1` varchar(64) NULL,
        `k2` varchar(64) NULL,
        `v` int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`,`k2`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`,`k2`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "colocate_with" = "group_column_order"
        );
    """
    sql """ DROP TABLE IF EXISTS `test_colocate_join_of_column_order_t2`; """
    // distributed by k2,k1
    sql """
        CREATE TABLE IF NOT EXISTS `test_colocate_join_of_column_order_t2` (
        `k1` varchar(64) NULL,
        `k2` varchar(64) NULL,
        `v` int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`,`k2`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k2`,`k1`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "colocate_with" = "group_column_order"
        );
    """
    sql """insert into test_colocate_join_of_column_order_t1 values('k1','k2',11);"""
    sql """insert into test_colocate_join_of_column_order_t2 values('k1','k2',11);"""

    sql """set enable_nereids_planner=true; """
    explain {
        sql("select * from test_colocate_join_of_column_order_t1 a join test_colocate_join_of_column_order_t2 b on a.k1=b.k1 and a.k2=b.k2;")
        notContains "COLOCATE"
    }
    explain {
        sql("select * from test_colocate_join_of_column_order_t1 a join test_colocate_join_of_column_order_t2 b on a.k1=b.k2;")
        notContains "COLOCATE"
    }
    explain {
        sql("select * from test_colocate_join_of_column_order_t1 a join test_colocate_join_of_column_order_t2 b on a.k1=b.k1;")
        notContains "COLOCATE"
    }
    explain {
        sql("select * from test_colocate_join_of_column_order_t1 a join test_colocate_join_of_column_order_t2 b on a.k1=b.k2 and a.v=b.v;")
        notContains "COLOCATE"
    }
    explain {
        sql("select * from test_colocate_join_of_column_order_t1 a join test_colocate_join_of_column_order_t2 b on a.k1=b.k2 and a.k2=b.k1;")
        contains "COLOCATE"
    }
    explain {
        sql("select * from test_colocate_join_of_column_order_t1 a join test_colocate_join_of_column_order_t2 b on a.k1=b.k2 and a.k2=b.k1 and a.v=b.v;")
        contains "COLOCATE"
    }

    sql """ DROP TABLE IF EXISTS `test_colocate_join_of_column_order_t1`; """
    sql """ DROP TABLE IF EXISTS `test_colocate_join_of_column_order_t2`; """

    // multi tables
    sql """ DROP TABLE IF EXISTS `test_colocate_join_of_column_order_ta`; """
    sql """
        CREATE TABLE IF NOT EXISTS `test_colocate_join_of_column_order_ta` ( `c1` bigint NULL, `c2` bigint NULL)
            DISTRIBUTED BY HASH(c1) PROPERTIES ( "replication_num" = "1", "colocate_with" = "group_column_order3");
    """
    sql """ DROP TABLE IF EXISTS `test_colocate_join_of_column_order_tb`; """
    sql """
        CREATE TABLE IF NOT EXISTS `test_colocate_join_of_column_order_tb` ( `c1` bigint NULL, `c2` bigint NULL)
            DISTRIBUTED BY HASH(c1) PROPERTIES ( "replication_num" = "1", "colocate_with" = "group_column_order3");
    """
    sql """ DROP TABLE IF EXISTS `test_colocate_join_of_column_order_tc`; """
    sql """
        CREATE TABLE IF NOT EXISTS `test_colocate_join_of_column_order_tc` ( `c1` bigint NULL, `c2` bigint NULL)
            DISTRIBUTED BY HASH(c1) PROPERTIES ( "replication_num" = "1", "colocate_with" = "group_column_order3");
    """
    sql """insert into test_colocate_join_of_column_order_ta values(1,1);"""
    sql """insert into test_colocate_join_of_column_order_tb values(1,1);"""
    sql """insert into test_colocate_join_of_column_order_tc values(1,1);"""

    explain {
        sql("""select /*+ set_var(disable_join_reorder=true) */ * from test_colocate_join_of_column_order_ta join [shuffle] (select cast((c2 + 1) as bigint) c2 from test_colocate_join_of_column_order_tb) test_colocate_join_of_column_order_tb  on test_colocate_join_of_column_order_ta.c1 = test_colocate_join_of_column_order_tb.c2 join [shuffle] test_colocate_join_of_column_order_tc on test_colocate_join_of_column_order_tb.c2 = test_colocate_join_of_column_order_tc.c1;""");
        contains "COLOCATE"
    }

    sql """ DROP TABLE IF EXISTS `test_colocate_join_of_column_order_ta`; """
    sql """ DROP TABLE IF EXISTS `test_colocate_join_of_column_order_tb`; """
    sql """ DROP TABLE IF EXISTS `test_colocate_join_of_column_order_tc`; """
}
