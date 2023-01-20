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
        SELECT * FROM test_sq_dj1 WHERE c1 IN (SELECT c1 FROM test_sq_dj2 WHERE test_sq_dj1.c1 > test_sq_dj2.c2) OR c1 < 10;
    """

    qt_hash_join_with_other_conjuncts2 """
        SELECT * FROM test_sq_dj1 WHERE c1 IN (SELECT c1 FROM test_sq_dj2 WHERE test_sq_dj1.c1 < test_sq_dj2.c2) OR c1 < 10;
    """

    qt_hash_join_with_other_conjuncts3 """
        SELECT * FROM test_sq_dj1 WHERE c1 IN (SELECT c1 FROM test_sq_dj2 WHERE test_sq_dj1.c1 > test_sq_dj2.c2) OR c1 < 11;
    """

    qt_hash_join_with_other_conjuncts4 """
        SELECT * FROM test_sq_dj1 WHERE c1 IN (SELECT c1 FROM test_sq_dj2 WHERE test_sq_dj1.c1 < test_sq_dj2.c2) OR c1 < 11;
    """
}
