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

suite("test_update_order_by_limit") {
    sql "DROP TABLE IF EXISTS test_update_obl"
    sql """
        CREATE TABLE IF NOT EXISTS test_update_obl (
            id int,
            c1 int,
            c2 varchar(32)
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            'replication_num' = '1'
        );
    """

    // insert test data: 10 rows with id 1..10
    sql """
        INSERT INTO test_update_obl VALUES
            (1, 100, 'a'), (2, 90, 'b'), (3, 80, 'c'), (4, 70, 'd'), (5, 60, 'e'),
            (6, 50, 'f'), (7, 40, 'g'), (8, 30, 'h'), (9, 20, 'i'), (10, 10, 'j');
    """
    order_qt_before_update """SELECT * FROM test_update_obl ORDER BY id;"""

    // test UPDATE with ORDER BY and LIMIT
    // c1 ascending: 10(id=10), 20(id=9), 30(id=8)
    // LIMIT 3 means update the first 3 rows: set c2='updated' for id=10, id=9, id=8
    sql "UPDATE test_update_obl SET c2 = 'updated' ORDER BY c1 ASC LIMIT 3;"
    order_qt_update_order_limit """SELECT * FROM test_update_obl ORDER BY id;"""

    // reset data
    sql "DROP TABLE IF EXISTS test_update_obl"
    sql """
        CREATE TABLE IF NOT EXISTS test_update_obl (
            id int,
            c1 int,
            c2 varchar(32)
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            'replication_num' = '1'
        );
    """
    sql """
        INSERT INTO test_update_obl VALUES
            (1, 100, 'a'), (2, 90, 'b'), (3, 80, 'c'), (4, 70, 'd'), (5, 60, 'e'),
            (6, 50, 'f'), (7, 40, 'g'), (8, 30, 'h'), (9, 20, 'i'), (10, 10, 'j');
    """

    // test UPDATE with ORDER BY, LIMIT and OFFSET
    // c1 ascending: 10(id=10), 20(id=9), 30(id=8), 40(id=7), 50(id=6)
    // LIMIT 2, 3 means offset=2, limit=3: skip 2 rows (id=10, id=9), update next 3: id=8, id=7, id=6
    sql "UPDATE test_update_obl SET c2 = 'updated' ORDER BY c1 ASC LIMIT 2, 3;"
    order_qt_update_order_limit_offset """SELECT * FROM test_update_obl ORDER BY id;"""

    // reset data
    sql "DROP TABLE IF EXISTS test_update_obl"
    sql """
        CREATE TABLE IF NOT EXISTS test_update_obl (
            id int,
            c1 int,
            c2 varchar(32)
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            'replication_num' = '1'
        );
    """
    sql """
        INSERT INTO test_update_obl VALUES
            (1, 100, 'a'), (2, 90, 'b'), (3, 80, 'c'), (4, 70, 'd'), (5, 60, 'e'),
            (6, 50, 'f'), (7, 40, 'g'), (8, 30, 'h'), (9, 20, 'i'), (10, 10, 'j');
    """

    // test UPDATE with WHERE, ORDER BY and LIMIT
    // filter: c1 > 30 leaves: 100(id=1), 90(id=2), 80(id=3), 70(id=4), 60(id=5), 50(id=6), 40(id=7)
    // order by c1 ASC: 40(id=7), 50(id=6), 60(id=5), 70(id=4), 80(id=3), 90(id=2), 100(id=1)
    // LIMIT 2: update id=7, id=6
    sql "UPDATE test_update_obl SET c2 = 'updated' WHERE c1 > 30 ORDER BY c1 ASC LIMIT 2;"
    order_qt_update_where_order_limit """SELECT * FROM test_update_obl ORDER BY id;"""

    // reset data
    sql "DROP TABLE IF EXISTS test_update_obl"
    sql """
        CREATE TABLE IF NOT EXISTS test_update_obl (
            id int,
            c1 int,
            c2 varchar(32)
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            'replication_num' = '1'
        );
    """
    sql """
        INSERT INTO test_update_obl VALUES
            (1, 100, 'a'), (2, 90, 'b'), (3, 80, 'c'), (4, 70, 'd'), (5, 60, 'e'),
            (6, 50, 'f'), (7, 40, 'g'), (8, 30, 'h'), (9, 20, 'i'), (10, 10, 'j');
    """

    // test UPDATE with ORDER BY DESC and LIMIT
    // c1 descending: 100(id=1), 90(id=2), 80(id=3)
    // LIMIT 2: update id=1, id=2
    sql "UPDATE test_update_obl SET c2 = 'updated' ORDER BY c1 DESC LIMIT 2;"
    order_qt_update_order_desc_limit """SELECT * FROM test_update_obl ORDER BY id;"""

    // test UPDATE with LIMIT OFFSET syntax
    sql "DROP TABLE IF EXISTS test_update_obl"
    sql """
        CREATE TABLE IF NOT EXISTS test_update_obl (
            id int,
            c1 int,
            c2 varchar(32)
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            'replication_num' = '1'
        );
    """
    sql """
        INSERT INTO test_update_obl VALUES
            (1, 100, 'a'), (2, 90, 'b'), (3, 80, 'c'), (4, 70, 'd'), (5, 60, 'e'),
            (6, 50, 'f'), (7, 40, 'g'), (8, 30, 'h'), (9, 20, 'i'), (10, 10, 'j');
    """

    // LIMIT 2 OFFSET 1 means skip 1 then update next 2
    // c1 ascending: 10(id=10), 20(id=9), 30(id=8)
    // skip 1 (id=10), update 2 (id=9, id=8)
    sql "UPDATE test_update_obl SET c2 = 'updated' ORDER BY c1 ASC LIMIT 2 OFFSET 1;"
    order_qt_update_limit_offset_syntax """SELECT * FROM test_update_obl ORDER BY id;"""

    // test UPDATE with multiple SET assignments and ORDER BY LIMIT
    sql "DROP TABLE IF EXISTS test_update_obl"
    sql """
        CREATE TABLE IF NOT EXISTS test_update_obl (
            id int,
            c1 int,
            c2 varchar(32)
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            'replication_num' = '1'
        );
    """
    sql """
        INSERT INTO test_update_obl VALUES
            (1, 100, 'a'), (2, 90, 'b'), (3, 80, 'c'), (4, 70, 'd'), (5, 60, 'e'),
            (6, 50, 'f'), (7, 40, 'g'), (8, 30, 'h'), (9, 20, 'i'), (10, 10, 'j');
    """

    // update both c1 and c2 for the 3 rows with largest c1
    sql "UPDATE test_update_obl SET c1 = 999, c2 = 'top3' ORDER BY c1 DESC LIMIT 3;"
    order_qt_update_multi_set """SELECT * FROM test_update_obl ORDER BY id;"""
}
