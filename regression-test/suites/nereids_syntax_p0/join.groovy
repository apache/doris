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

suite("join") {
    sql """
        SET enable_nereids_planner=true
    """

    sql "SET enable_fallback_to_original_planner=false"

    sql """
        drop table if exists test_table_a;
    """

    sql """
        drop table if exists test_table_b;
    """

    sql """
        CREATE TABLE `test_table_a`
        (
            `wtid`                varchar(30)    NOT NULL ,
            `wfid`           varchar(30) NOT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`wtid`,`wfid`)
        DISTRIBUTED BY HASH(`wfid`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        CREATE TABLE `test_table_b`
        (
            `wtid`           varchar(100) NOT NULL ,
            `wfid`           varchar(100) NOT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`wtid`,`wfid`)
        DISTRIBUTED BY HASH(`wfid`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "storage_format" = "V2"
        );
    """

    sql """
        insert into test_table_b values( '1', '1'),('2','2'),('3','3'),('1','2'),('1','3'),('2','3'); 
    """

    sql """
        insert into test_table_a values( '1', '1'),('2','2'),('3','3'),('1','2'),('1','3'),('2','3'); 
    """

    // must analyze before explain, because empty table may generate different plan
    sql """
        analyze table test_table_b with sync;
    """

    sql """
        analyze table test_table_a with sync;
    """

    order_qt_inner_join_1 """
        SELECT * FROM lineorder JOIN supplier ON lineorder.lo_suppkey = supplier.s_suppkey
    """

    order_qt_inner_join_2 """
        SELECT * FROM lineorder INNER JOIN supplier ON lineorder.lo_suppkey = supplier.s_suppkey
    """

    order_qt_left_outer_join_1 """
        SELECT * FROM lineorder LEFT JOIN supplier ON lineorder.lo_suppkey = supplier.s_suppkey
    """

    order_qt_left_outer_join_2 """
        SELECT * FROM lineorder LEFT OUTER JOIN supplier ON lineorder.lo_suppkey = supplier.s_suppkey
    """

    order_qt_right_outer_join_1 """
        SELECT * FROM lineorder RIGHT JOIN supplier ON lineorder.lo_suppkey = supplier.s_suppkey
    """

    order_qt_right_outer_join_2 """
        SELECT * FROM lineorder RIGHT OUTER JOIN supplier ON lineorder.lo_suppkey = supplier.s_suppkey
    """

    order_qt_left_semi_join """
        SELECT * FROM lineorder LEFT SEMI JOIN supplier ON lineorder.lo_suppkey = supplier.s_suppkey
    """

    order_qt_right_semi_join """
        SELECT * FROM lineorder RIGHT SEMI JOIN supplier ON lineorder.lo_suppkey = supplier.s_suppkey
    """

    order_qt_left_anti_join """
        SELECT * FROM lineorder LEFT ANTI JOIN supplier ON lineorder.lo_suppkey = supplier.s_suppkey
    """

    order_qt_right_anti_join """
        SELECT * FROM lineorder RIGHT ANTI JOIN supplier ON lineorder.lo_suppkey = supplier.s_suppkey
    """

    order_qt_cross_join """
        SELECT * FROM lineorder CROSS JOIN supplier;
    """

    order_qt_inner_join_with_other_condition """
        select lo_orderkey, lo_partkey, p_partkey, p_size from lineorder inner join part on lo_partkey = p_partkey where lo_orderkey - 1310000 > p_size;
    """

    order_qt_outer_join_with_filter """
        select lo_orderkey, lo_partkey, p_partkey, p_size from lineorder inner join part on lo_partkey = p_partkey where lo_orderkey - 1310000 > p_size;
    """

    sql """
        drop table if exists outerjoin_A_join;
    """

    sql """
        drop table if exists outerjoin_B_join;
    """

    sql """
        drop table if exists outerjoin_C_join;
    """

    sql """
        drop table if exists outerjoin_D;
    """
    
    sql """
        create table if not exists outerjoin_A_join ( a int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        create table if not exists outerjoin_B_join ( a int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        create table if not exists outerjoin_C_join ( a int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        create table if not exists outerjoin_D ( a int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        insert into outerjoin_A_join values( 1 );
    """

    sql """
        insert into outerjoin_B_join values( 1 );
    """

    sql """
        insert into outerjoin_C_join values( 1 );
    """

    sql """
        insert into outerjoin_D values( 1 );
    """

    test {
        sql"""select * from test_table_a a cross join test_table_b b on a.wtid > b.wtid"""
        check{result, exception, startTime, endTime ->
            assertTrue(exception != null)
            logger.info(exception.message)
        }
    }

    sql """drop table if exists test_memo_1"""
    sql """drop table if exists test_memo_2"""
    sql """drop table if exists test_memo_3"""

    sql """ CREATE TABLE `test_memo_1` (
    `c_bigint` bigint(20) NULL,
    `c_long_decimal` decimal(27, 9) NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`c_bigint`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`c_bigint`) BUCKETS 1
    PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false"
    );
    """

    sql """ CREATE TABLE `test_memo_2` (
    `sk` bigint(20) NULL,
    `id` int(11) NULL
    ) ENGINE=OLAP
    UNIQUE KEY(`sk`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`sk`) BUCKETS 1
    PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false"
    );
    """

    sql """ CREATE TABLE `test_memo_3` (
    `id` bigint(20) NOT NULL,
    `c1` varchar(150) NULL
    ) ENGINE=OLAP
    UNIQUE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false"
    );
    """

    order_qt_test """
        select 
         ref_1.`c_long_decimal` as c0,
         ref_3.`c1` as c1
        from
          test_memo_1 as ref_1
          inner join test_memo_2 as ref_2 on (case when true then 5 else 5 end is not NULL)
          inner join test_memo_3 as ref_3 on (version() is not NULL)
        where
          ref_2.`id` is not NULL
    """

    order_qt_test "SELECT * FROM lineorder RIGHT SEMI JOIN supplier ON lineorder.lo_suppkey = supplier.s_suppkey and s_name='Supplier#000000029';"
}
