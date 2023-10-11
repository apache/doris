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

suite("test_subquery_in_disjunction") {
    sql """ DROP TABLE IF EXISTS test_sq_dj1 """
    sql """ DROP TABLE IF EXISTS test_sq_dj2 """
    sql """
    CREATE TABLE `test_sq_dj1` (
        `c1` int(11) NULL,
        `c2` int(11) NULL,
        `c3` int(11) NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`c1`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`c1`) BUCKETS 3
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );
    """
    sql """
    CREATE TABLE `test_sq_dj2` (
        `c1` int(11) NULL,
        `c2` int(11) NULL,
        `c3` int(11) NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`c1`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`c1`) BUCKETS 3
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );
    """
    sql """
        insert into test_sq_dj1 values(1, 2, 3), (10, 20, 30), (100, 200, 300)
    """
    sql """
        insert into test_sq_dj2 values(10, 20, 30)
    """

    order_qt_in """
        SELECT * FROM test_sq_dj1 WHERE c1 IN (SELECT c1 FROM test_sq_dj2) OR c1 < 10;
    """

    order_qt_scalar """
        SELECT * FROM test_sq_dj1 WHERE c1 > (SELECT AVG(c1) FROM test_sq_dj2) OR c1 < 10;
    """

    order_qt_exists_true """
        SELECT * FROM test_sq_dj1 WHERE EXISTS (SELECT c1 FROM test_sq_dj2 WHERE c1 = 10) OR c1 < 10;
    """

    order_qt_in_exists_false """
        SELECT * FROM test_sq_dj1 WHERE EXISTS (SELECT c1 FROM test_sq_dj2 WHERE c1 > 10) OR c1 < 10;
    """

    order_qt_not_in """
        SELECT * FROM test_sq_dj1 WHERE c1 NOT IN (SELECT c1 FROM test_sq_dj2) OR c1 = 10;
    """

    order_qt_not_in_covered """
        SELECT * FROM test_sq_dj1 WHERE c1 NOT IN (SELECT c1 FROM test_sq_dj2) OR c1 = 100;
    """

    qt_hash_join_with_other_conjuncts1 """
        SELECT * FROM test_sq_dj1 WHERE c1 IN (SELECT c1 FROM test_sq_dj2 WHERE test_sq_dj1.c1 > test_sq_dj2.c2) OR c1 < 10 ORDER BY c1;
    """

    qt_hash_join_with_other_conjuncts2 """
        SELECT * FROM test_sq_dj1 WHERE c1 IN (SELECT c1 FROM test_sq_dj2 WHERE test_sq_dj1.c1 < test_sq_dj2.c2) OR c1 < 10 ORDER BY c1;
    """

    qt_hash_join_with_other_conjuncts3 """
        SELECT * FROM test_sq_dj1 WHERE c1 IN (SELECT c1 FROM test_sq_dj2 WHERE test_sq_dj1.c1 > test_sq_dj2.c2) OR c1 < 11 ORDER BY c1;
    """

    qt_hash_join_with_other_conjuncts4 """
        SELECT * FROM test_sq_dj1 WHERE c1 IN (SELECT c1 FROM test_sq_dj2 WHERE test_sq_dj1.c1 < test_sq_dj2.c2) OR c1 < 11 ORDER BY c1;
    """

    // TODO: enable this after DORIS-7051 and DORIS-7052 is fixed
    // qt_hash_join_with_other_conjuncts5 """
    //     SELECT * FROM test_sq_dj1 WHERE c1 NOT IN (SELECT c1 FROM test_sq_dj2 WHERE test_sq_dj1.c1 > test_sq_dj2.c2) OR c1 < 10 ORDER BY c1;
    // """

    // qt_hash_join_with_other_conjuncts6 """
    //     SELECT * FROM test_sq_dj1 WHERE c1 NOT IN (SELECT c1 FROM test_sq_dj2 WHERE test_sq_dj1.c1 < test_sq_dj2.c2) OR c1 < 10 ORDER BY c1;
    // """

    // qt_hash_join_with_other_conjuncts7 """
    //     SELECT * FROM test_sq_dj1 WHERE c1 NOT IN (SELECT c1 FROM test_sq_dj2 WHERE test_sq_dj1.c1 > test_sq_dj2.c2) OR c1 < 11 ORDER BY c1;
    // """

    // qt_hash_join_with_other_conjuncts8 """
    //     SELECT * FROM test_sq_dj1 WHERE c1 NOT IN (SELECT c1 FROM test_sq_dj2 WHERE test_sq_dj1.c1 < test_sq_dj2.c2) OR c1 < 11 ORDER BY c1;
    // """

    qt_same_subquery_in_conjuncts """
        SELECT * FROM test_sq_dj1 WHERE c1 IN (SELECT c1 FROM test_sq_dj2) OR c1 IN (SELECT c1 FROM test_sq_dj2) OR c1 < 10 ORDER BY c1;
    """

    qt_two_subquery_in_one_conjuncts """
        SELECT * FROM test_sq_dj1 WHERE c1 IN (SELECT c1 FROM test_sq_dj2) OR c1 IN (SELECT c2 FROM test_sq_dj2) OR c1 < 10 ORDER BY c1;
    """

    // test mark join that one probe row matches multiple build rows
    sql """drop table if exists sub_query_correlated_subquery1;"""
    sql """create table if not exists sub_query_correlated_subquery1
        (k1 bigint, k2 bigint)
        duplicate key(k1)
        distributed by hash(k2) buckets 1
        properties('replication_num' = '1');"""

    sql """drop table if exists sub_query_correlated_subquery3;"""
    sql """create table if not exists sub_query_correlated_subquery3
        (kk1 int not null, k2 varchar(128), k3 bigint, v1 bigint, v2 bigint)
        distributed by hash(k2) buckets 1
        properties('replication_num' = '1');"""

    sql """
        insert into
            sub_query_correlated_subquery1
        values
            (1, 2),
            (1, 3),
            (2, 4),
            (2, 5),
            (3, 3),
            (3, 4),
            (20, 2),
            (22, 3),
            (24, 4);
    """
    sql """
        insert into
            sub_query_correlated_subquery3
        values
            (1, "abc", 2, 3, 4),
            (1, "abcd", 3, 3, 4),
            (2, "xyz", 2, 4, 2),
            (2, "uvw", 3, 4, 2),
            (2, "uvw", 3, 4, 2),
            (3, "abc", 4, 5, 3),
            (3, "abc", 4, 5, 3);
    """

    qt_mark_join_with_other_conjuncts1 """
        SELECT
            *
        FROM
            sub_query_correlated_subquery1
        WHERE
            k1 IN (
                SELECT
                    kk1
                FROM
                    sub_query_correlated_subquery3
                WHERE
                    sub_query_correlated_subquery1.k1 > sub_query_correlated_subquery3.k3
            )
            OR k1 < 10
        ORDER BY
            k1, k2;
    """

    qt_mark_join_with_other_conjuncts2 """
        SELECT
            *
        FROM
            sub_query_correlated_subquery1
        WHERE
            k1 IN (
                SELECT
                    kk1
                FROM
                    sub_query_correlated_subquery3
                WHERE
                    sub_query_correlated_subquery1.k1 != sub_query_correlated_subquery3.k3
            )
            OR k1 < 10
        ORDER BY
            k1, k2;
    """
}
