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

suite("test_subquery") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    qt_sql1 """
        select c1, c3, m2 from 
            (select c1, c3, max(c2) m2 from 
                (select c1, c2, c3 from 
                    (select k3 c1, k2 c2, max(k1) c3 from nereids_test_query_db.test 
                     group by 1, 2 order by 1 desc, 2 desc limit 5) x 
                ) x2 group by c1, c3 limit 10
            ) t 
        where c1>0 order by 2 , 1 limit 3
    """

    qt_sql2 """
        with base as (select k1, k2 from nereids_test_query_db.test as t where k1 in (select k1 from nereids_test_query_db.baseall
        where k7 = 'wangjuoo4' group by 1 having count(distinct k7) > 0)) select * from base limit 10;
    """

    qt_sql3 """
        SELECT k1 FROM nereids_test_query_db.test GROUP BY k1 HAVING k1 IN (SELECT k1 FROM nereids_test_query_db.baseall WHERE
        k2 >= (SELECT min(k3) FROM nereids_test_query_db.bigtable WHERE k2 = baseall.k2)) order by k1;
    """

    qt_sql4 """
        select count() from (select k2, k1 from nereids_test_query_db.baseall order by k1 limit 1) a;
    """

    qt_uncorrelated_exists_with_limit_0 """
        select * from nereids_test_query_db.baseall where exists (select * from nereids_test_query_db.baseall limit 0)
    """

    // test uncorrelated scalar subquery with limit <= 1
    sql """
        select * from nereids_test_query_db.baseall where k1 = (select k1 from nereids_test_query_db.baseall limit 1)
    """

    // test uncorrelated subquery in having
    sql """
        select count(*) from nereids_test_query_db.baseall
        group by k0 
        having min(k0) in (select k0 from nereids_test_query_db.baseall)
    """

    // test uncorrelated scalar subquery with more than one return rows
    test {
        sql """
            select * from nereids_test_query_db.baseall where k1 = (select k1 from nereids_test_query_db.baseall limit 2)
        """
        exception("Expected LE 1 to be returned by expression")
    }

    // test uncorrelated scalar subquery with order by and limit
    qt_uncorrelated_scalar_with_sort_and_limit """
            select * from nereids_test_query_db.baseall where k1 = (select k1 from nereids_test_query_db.baseall order by k1 desc limit 1)
        """

    sql """DROP TABLE IF EXISTS table_1000_undef_undef"""
    sql """DROP TABLE IF EXISTS table_1000_undef_undef2"""
    sql """CREATE TABLE `table_1000_undef_undef` (
            `pk` int(11) NULL,
            `col_bigint_undef_signed` bigint(20) NULL,
            `col_bigint_undef_signed2` bigint(20) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`pk`, `col_bigint_undef_signed`, `col_bigint_undef_signed2`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`pk`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "is_being_synced" = "false",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false"
            );  """

    sql """ CREATE TABLE `table_1000_undef_undef2` (
            `pk` int(11) NULL,
            `col_bigint_undef_signed` bigint(20) NULL,
            `col_bigint_undef_signed2` bigint(20) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`pk`, `col_bigint_undef_signed`, `col_bigint_undef_signed2`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`pk`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "is_being_synced" = "false",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false"
            );"""
    explain {
        sql """
            SELECT `col_bigint_undef_signed` '00:39:36' , `col_bigint_undef_signed` '11:19:45', `col_bigint_undef_signed` '11:55:37', `col_bigint_undef_signed2` '19:01:23'
                FROM table_1000_undef_undef2
                WHERE EXISTS 
                    (SELECT `col_bigint_undef_signed` '17:38:13' , `col_bigint_undef_signed2` '17:36:21'
                    FROM table_1000_undef_undef2
                    WHERE `col_bigint_undef_signed2` NOT IN 
                        (SELECT `col_bigint_undef_signed`
                        FROM table_1000_undef_undef2
                        WHERE `col_bigint_undef_signed2` < 
                            (SELECT AVG(`col_bigint_undef_signed`)
                            FROM table_1000_undef_undef2
                            WHERE `col_bigint_undef_signed2` < 2)) ) ; 
        """
        contains("VAGGREGATE")
    }

    explain {
        sql """SELECT * FROM table_1000_undef_undef t1 WHERE t1.pk <= (SELECT COUNT(t2.pk) FROM table_1000_undef_undef2 t2 WHERE (t1.col_bigint_undef_signed = t2.col_bigint_undef_signed)); """
        contains("ifnull")
    }

    sql """DROP TABLE IF EXISTS table_1000_undef_undef"""
    sql """DROP TABLE IF EXISTS table_1000_undef_undef2"""

    sql """drop table if exists test_one_row_relation;"""
    sql """
        CREATE TABLE `test_one_row_relation` (
        `user_id` int(11) NULL 
        )
        UNIQUE KEY(`user_id`)
        COMMENT 'test'
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ set enable_nereids_dml=true; """
    
    sql """insert into test_one_row_relation select (select 1);"""

    qt_sql_subquery_one_row_relation """select * from test_one_row_relation;"""

    qt_sql_mark_join """with A as (select count(*) n1 from test_one_row_relation where exists (select 1 from test_one_row_relation t where t.user_id = test_one_row_relation.user_id) or 1 = 1) select * from A;"""

    sql """drop table if exists test_one_row_relation;"""
}
