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

suite("test_pushdown_explain") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "use nereids_test_query_db"
    
    explain {
        sql("select k1 from baseall where k1 = 1")
        contains "PREDICATES:"
    }
    qt_select "select k1 from baseall where k1 = 1"

    sql "DROP TABLE IF EXISTS test_lineorder"
    sql """ CREATE TABLE `test_lineorder` (
        `lo_orderkey` INT NOT NULL COMMENT '\"\"',
        `lo_linenumber` INT NOT NULL COMMENT '\"\"',
        `lo_shipmode` VARCHAR(11) NOT NULL COMMENT '\"\"'
    ) ENGINE=OLAP
    DUPLICATE KEY(`lo_orderkey`)
    DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 48
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    ); """
    sql """ insert into test_lineorder values(1,2,"asd"); """
    explain {
        sql("select count(1) from test_lineorder;")
        contains "pushAggOp=COUNT"
    }
    explain {
        sql("select count(*) from test_lineorder;")
        contains "pushAggOp=COUNT"
    }
    explain {
        sql("select count(1) - count(lo_shipmode) from test_lineorder;")
        contains "pushAggOp=COUNT"
    }
    explain {
        sql("select count(lo_orderkey) from test_lineorder;")
        contains "pushAggOp=COUNT"
    }
    explain {
        sql("select count(cast(lo_orderkey as bigint)) from test_lineorder;")
        contains "pushAggOp=COUNT"
    }

    sql "DROP TABLE IF EXISTS table_unique0"
    sql """ 
        CREATE TABLE `table_unique0` (
            `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
            `username` VARCHAR(50) NOT NULL COMMENT '\"用户昵称\"'
        ) ENGINE=OLAP
        UNIQUE KEY(`user_id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "disable_auto_compaction" = "true"
        );
    """

    // set seession variables
    sql "set enable_pushdown_minmax_on_unique = true;"

    sql """ insert into table_unique0 values(1,"a"); """
    sql """ insert into table_unique0 values(1,"b"); """
    sql """ insert into table_unique0 values(1,"c"); """
    qt_select_table_unique0 "select * from table_unique0 order by user_id;" // 1, c
    qt_select_table_unique0_min "select min(username) from table_unique0;"  // a is read from zone map

    sql """ insert into table_unique0 values(2,"g"); """
    sql """ insert into table_unique0 values(2,"f"); """
    sql """ insert into table_unique0 values(2,"e"); """
    qt_select_table_unique1 "select * from table_unique0 order by user_id;" // 2, e
    qt_select_table_unique1_max "select max(username) from table_unique0;"  // g is read from zone map

    sql """ insert into table_unique0 values(3,"h"); """   
    sql """ insert into table_unique0(user_id,username,__DORIS_DELETE_SIGN__) values(3,'h',1); """ // delete id = 3
    qt_select_table_unique2 "select * from table_unique0 order by user_id;" // no user_id = 3
    qt_select_table_unique2_max "select max(username) from table_unique0;"  // h is read from zone map

    sql """ insert into table_unique0 values(4,"l"); """  
    sql """ update table_unique0 set username = "k" where user_id = 4; """ 
    qt_select_table_unique3 "select * from table_unique0 order by user_id;" // 4 ,k 
    qt_select_table_unique3_max "select max(username) from table_unique0;"  // l is read from zone map

    sql "DROP TABLE IF EXISTS table_unique"
    sql """ 
        CREATE TABLE `table_unique` (
            `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
            `username` VARCHAR(50) NOT NULL COMMENT '\"用户昵称\"',
            `val` VARCHAR(50) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`user_id`, `username`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "disable_auto_compaction" = "true"
        );
    """
    sql """ 
        insert into table_unique values(1,"asd","cc"),(2,"qwe","vvx"),(3,"ffsd","mnm"),(4,"qdf","ll"),(5,"cvfv","vff");
    """

    sql "set enable_pushdown_minmax_on_unique = false;"
    explain {
        sql("select min(user_id) from table_unique;")
        contains "pushAggOp=NONE"
    }
    explain {
        sql("select max(user_id) from table_unique;")
        contains "pushAggOp=NONE"
    }
    explain {
        sql("select min(username) from table_unique;")
        contains "pushAggOp=NONE"
    }
    explain {
        sql("select max(username) from table_unique;")
        contains "pushAggOp=NONE"
    }


    // set seession variables
    sql "set enable_pushdown_minmax_on_unique = true;"
    explain {
        sql("select min(user_id) from table_unique;")
        contains "pushAggOp=MINMAX"
    }
    explain {
        sql("select max(user_id) from table_unique;")
        contains "pushAggOp=MINMAX"
    }
    explain {
        sql("select min(username) from table_unique;")
        contains "pushAggOp=MINMAX"
    }
    explain {
        sql("select max(username) from table_unique;")
        contains "pushAggOp=MINMAX"
    }
    qt_select_0 "select * from table_unique order by user_id;"
    qt_select_1 "select min(user_id) from table_unique;"
    qt_select_2 "select max(user_id) from table_unique;"
    qt_select_3 "select min(username) from table_unique;"
    qt_select_4 "select max(username) from table_unique;"
    qt_select_5 "select min(val) from table_unique;"
    qt_select_6 "select max(val) from table_unique;"
    sql """
        update table_unique set val = "zzz" where user_id = 1;
    """
    qt_select_00 "select * from table_unique order by user_id;"
    qt_select_7 "select min(user_id) from table_unique;"
    qt_select_8 "select max(user_id) from table_unique;"
    qt_select_9 "select min(username) from table_unique;"
    qt_select_10 "select max(username) from table_unique;"
    qt_select_11 "select min(val) from table_unique;"
    qt_select_12 "select max(val) from table_unique;"

    sql """
        delete from table_unique where user_id = 2;
    """
    qt_select_000 "select * from table_unique order by user_id;"
    qt_select_13 "select min(user_id) from table_unique;"
    qt_select_14 "select max(user_id) from table_unique;"
    qt_select_15 "select min(username) from table_unique;"
    qt_select_16 "select max(username) from table_unique;"
    qt_select_18 "select max(val) from table_unique;"


    sql "DROP TABLE IF EXISTS table_agg"
    sql """ 
        CREATE TABLE `table_agg` (
            `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
            `username` VARCHAR(50) NOT NULL COMMENT '\"用户昵称\"',
            `val` VARCHAR(50) max NULL 
        ) ENGINE=OLAP
        AGGREGATE KEY(`user_id`, `username`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "disable_auto_compaction" = "true"
        );
    """

    sql """ 
        insert into table_agg values(1,"asd","cc"),(2,"qwe","vvx"),(3,"ffsd","mnm"),(4,"qdf","ll"),(5,"cvfv","vff");
    """

    explain {
        sql("select min(user_id) from table_agg;")
        contains "pushAggOp=MINMAX"
    }
    explain {
        sql("select max(user_id) from table_agg;")
        contains "pushAggOp=MINMAX"
    }
    explain {
        sql("select min(username) from table_agg;")
        contains "pushAggOp=MINMAX"
    }
    explain {
        sql("select max(username) from table_agg;")
        contains "pushAggOp=MINMAX"
    }

    qt_select_19 "select min(user_id) from table_agg;"
    qt_select_20 "select max(user_id) from table_agg;"
    qt_select_21 "select min(username) from table_agg;"
    qt_select_22 "select max(username) from table_agg;"
    qt_select_23 "select min(val) from table_agg;"
    qt_select_24 "select max(val) from table_agg;"
}
