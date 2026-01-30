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

suite("test_group_concat") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    qt_select """
                SELECT group_concat(k6) FROM nereids_test_query_db.test where k6='false'
              """

    qt_select """
                SELECT group_concat(DISTINCT k6) FROM nereids_test_query_db.test where k6='false'
              """

    qt_select """
                SELECT abs(k3), group_concat(cast(abs(k2) as varchar) order by abs(k2), k1) FROM nereids_test_query_db.baseall group by abs(k3) order by abs(k3)
              """
              
    qt_select """
                SELECT abs(k3), group_concat(cast(abs(k2) as varchar), ":" order by abs(k2), k1) FROM nereids_test_query_db.baseall group by abs(k3) order by abs(k3)
              """

    sql"""SELECT abs(k3), group_concat(distinct cast(abs(k2) as char) order by abs(k1), k2) FROM nereids_test_query_db.baseall group by abs(k3) order by abs(k3);"""

    sql"""SELECT abs(k3), group_concat(distinct cast(abs(k2) as char), ":" order by abs(k1), k2) FROM nereids_test_query_db.baseall group by abs(k3) order by abs(k3);"""

    qt_select """
                SELECT count(distinct k7), group_concat(k6 order by k6) FROM nereids_test_query_db.baseall;
              """

    sql """drop table if exists table_group_concat;"""

    sql """create table table_group_concat ( b1 varchar(10) not null, b2 int not null, b3 varchar(10) not null )
            ENGINE=OLAP
            DISTRIBUTED BY HASH(b3) BUCKETS 4
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
        """

    sql """insert into table_group_concat values('1', 1, '2'),('1', 1, '2'),('1', 2, '2');"""

    qt_select_10 """
                select
                group_concat( distinct b1 ), group_concat( distinct b3 )
                from
                table_group_concat;
              """

    qt_select_11 """
                select
                group_concat( distinct b1 ), group_concat( distinct b3 )
                from
                table_group_concat
                group by 
                b2;
              """

    sql """ drop table table_group_concat """
    sql """create table table_group_concat ( b1 varchar(10) not null, b2 int not null, b3 varchar(10) not null )
            ENGINE=OLAP
            DISTRIBUTED BY HASH(b3) BUCKETS 4
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
        """

    sql """insert into table_group_concat values('1', 1, '1'),('1', 2, '2'),('1', 3, '3');"""
    sql """insert into table_group_concat values('1', 1, '11'),('1', 2, '21');"""
    sql """insert into table_group_concat values('2', 21, '21'),('2', 22, '22'),('2', 23, '23');"""
    sql """insert into table_group_concat values('2', 21, '211'),('2', 22, '222');"""

    qt_select_group_concat_order_by_all_data """
      select * from table_group_concat order by b1, b2, b3;
    """
    qt_select_group_concat_order_by_desc1 """
                SELECT b1, group_concat(cast(abs(b2) as varchar) order by abs(b2) desc) FROM table_group_concat  group by b1 order by b1
              """

    qt_select_group_concat_order_by_desc2 """
                SELECT b1, group_concat(cast(abs(b3) as varchar) order by abs(b2) desc, b3) FROM table_group_concat  group by b1 order by b1
              """
    qt_select_group_concat_order_by_desc3 """
                SELECT b1, group_concat(cast(abs(b3) as varchar) order by abs(b2) desc, b3 desc) FROM table_group_concat  group by b1 order by b1
              """

    sql """create view if not exists test_view as SELECT b1, group_concat(cast(abs(b3) as varchar) order by abs(b2) desc, b3 desc) FROM table_group_concat  group by b1 order by b1;"""
    order_qt_select_group_concat_order_by_desc4 """
                select * from test_view;
    """
    sql """drop view if exists test_view"""

    // Constant Folding Correctness Test
    sql """ DROP TABLE IF EXISTS test_group_concat_fold_const_t1 """
    sql """ DROP TABLE IF EXISTS test_group_concat_fold_const_t2 """
    sql """ CREATE TABLE test_group_concat_fold_const_t1 (
                pk INT NOT NULL
            ) DISTRIBUTED BY HASH(pk) BUCKETS 1
            PROPERTIES ("replication_num" = "1");
    """
    sql """ CREATE TABLE test_group_concat_fold_const_t2 (
                pk INT NOT NULL,
                val VARCHAR(100) NOT NULL,
                filter_col DATETIME NOT NULL
            ) DISTRIBUTED BY HASH(pk) BUCKETS 1
            PROPERTIES ("replication_num" = "1");
    """
    sql """ INSERT INTO test_group_concat_fold_const_t1 VALUES (1), (2), (3); """
    sql """ INSERT INTO test_group_concat_fold_const_t2 VALUES
        (1, 'aaa', '2020-01-01 00:00:00'),
        (2, 'bbb', '2020-01-01 00:00:00');
    """
    // `SELECT group_concat(...) FROM t1 LEFT JOIN t2 ON ... WHERE t2.col = '...';`
    // In constant folding, LEFT JOIN is rewritten as INNER JOIN. 
    // If there are no matching rows, GROUP_CONCAT is not executed and returns NULL.
    // In `debug_skip_fold_constant=true`, normal execution of LEFT OUTER JOIN. 
    // After being filtered by WHERE, there are no matching rows, test if `add_batch` can correctly handle the case of null values
    // should return NULL instead of empty values
    testFoldConst("SELECT group_concat(t2.val ORDER BY t1.pk) AS result FROM test_group_concat_fold_const_t1 AS t1 LEFT JOIN test_group_concat_fold_const_t2 AS t2 ON t1.pk = t2.pk WHERE t2.filter_col = '2077-01-01 00:00:00';")
}
