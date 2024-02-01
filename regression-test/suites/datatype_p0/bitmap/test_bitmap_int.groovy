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

suite("test_bitmap_int") {
    sql "DROP TABLE IF EXISTS test_int_bitmap"
    sql """
        CREATE TABLE IF NOT EXISTS test_int_bitmap (`id` int, `bitmap_set` bitmap bitmap_union) 
        ENGINE=OLAP DISTRIBUTED BY HASH(`id`) BUCKETS 5 properties("replication_num" = "1");
        """
    sql "insert into test_int_bitmap values(1, bitmap_hash(1)), (2, bitmap_hash(2)), (3, bitmap_hash(3))"

    qt_sql1 "select bitmap_union_count(bitmap_set) from test_int_bitmap"
    qt_sql2 "select id,bitmap_union_count(bitmap_set) from test_int_bitmap group by id order by id"
    order_qt_sql3 "select * from test_int_bitmap"
    qt_desc "desc test_int_bitmap"

    sql "DROP TABLE test_int_bitmap"

    // bitmap_hash64
    sql """
        CREATE TABLE IF NOT EXISTS test_int_bitmap (`id` int, `bitmap_set` bitmap bitmap_union)
        ENGINE=OLAP DISTRIBUTED BY HASH(`id`) BUCKETS 5 properties("replication_num" = "1");
        """
    sql "insert into test_int_bitmap values(1, bitmap_hash64(1)), (2, bitmap_hash64(2)), (3, bitmap_hash64(3))"
    sql "insert into test_int_bitmap values(1, bitmap_hash64(11)), (2, bitmap_hash64(22))"

    qt_sql64_1 "select bitmap_union_count(bitmap_set) from test_int_bitmap"
    qt_sql64_2 "select id,bitmap_union_count(bitmap_set) from test_int_bitmap group by id order by id"
    order_qt_sql64_3 "select * from test_int_bitmap"

    sql "DROP TABLE test_int_bitmap"

    sql "DROP TABLE IF EXISTS test_bitmap"
    sql """CREATE TABLE
    `test_bitmap` (
    `id` varchar(16) NOT NULL,
    `id_bitmap` bitmap BITMAP_UNION NOT NULL
    ) DISTRIBUTED BY HASH (`id`) BUCKETS 1 PROPERTIES ("replication_num" = "1");"""

    test {
        sql """SELECT id_bitmap  FROM test_bitmap  WHERE (id_bitmap NOT IN (NULL) OR (1 = 1)) AND id_bitmap IS NOT NULL   LIMIT 20;"""
        exception "errCode"
    }

    test {
        sql """SELECT id_bitmap  FROM test_bitmap  WHERE id_bitmap IN (NULL) LIMIT 20;"""
        exception "errCode"
    }

    test {
        sql """SELECT id_bitmap  FROM test_bitmap  WHERE id_bitmap = 1 LIMIT 20;"""
        exception "errCode"
    }

    test {
        sql """SELECT case id_bitmap when 1 then 1 else 0 FROM test_bitmap;"""
        exception "ParseException"
    }

    qt_sql64_4 """SELECT id_bitmap  FROM test_bitmap  WHERE id_bitmap is null LIMIT 20;"""

    qt_sql64_5 """select case when 1 = 0 then bitmap_from_string('0') else bitmap_from_string('0') end as new_bitmap;"""

    sql "DROP TABLE IF EXISTS test_bitmap"

}


