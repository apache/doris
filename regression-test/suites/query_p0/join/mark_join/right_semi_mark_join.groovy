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


suite("right_semi_mark_join") {
    String suiteName = "right_semi_mark_join"
    String table_tbl1 = "${suiteName}_table_tbl1"
    String table_tbl2 = "${suiteName}_table_tbl2"
    String table_tbl3 = "${suiteName}_table_tbl3"
    
    sql "drop table if exists ${table_tbl1};"
    sql "drop table if exists ${table_tbl2};"
    sql "drop table if exists ${table_tbl3};"

    sql """
        create table ${table_tbl1} (pk int, col1 bigint, col2 bigint) engine = olap DUPLICATE KEY(pk) distributed by hash(pk) buckets 10 properties("replication_num" = "1");
    """

    sql """
        insert into
            ${table_tbl1}(pk, col1, col2)
        values
            (0, null, 18332),  (1, 788547, null), (2, 4644959, -56),  (3, 8364628, 72),  (4, null, -5581),
            (5, 2344024, -62), (6, -2689177, 22979),  (7, 1320, -41), (8, null, -54),  (9, 12, -6236),
            (10, -8321648, null), (11, 153691, null), (12, -8056, null), (13, -12, -2343514), (14, -35, -3361960);
    """

    sql """
        create table ${table_tbl2} (
            pk int, col1 bigint, col2 bigint
        ) engine = olap 
        distributed by hash(pk) buckets 4
        properties("replication_num" = "1");
    """

    sql """
        insert into
            ${table_tbl2}(pk, col1, col2)
        values
            (0, 108, 31161), (1, 1479175, 6764263), (2, 110, 25), (3, 110, -18656), (4, null, -51),
            (5, 21, 27), (6, -6950217, 1585978), (7, null, null), (8, null, 3453467),  (9, null, -6701140);
    """
    
    sql """
        create table ${table_tbl3} (
            pk int, col1 bigint, col2 bigint, col3 bigint
        ) engine = olap 
        DUPLICATE KEY(pk) distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """

    sql """
        insert into
            ${table_tbl3}(pk, col1, col2)
        values
            (0, 55, -58), (1, 49, 29792), (2, 95, 32361),  (3, 31243, -27428), (4, -27400, null),
            (5, 31243, null), (6, null, -27428), (7, null, 7), (8, 31243, -21951), (9, 13186, 24466),
            (10, null, -8), (11, null, null), (12, -18, 32361), (13, null, -18), (14, 21681, 14079),
            (15, 31241, -17653), (16, 5825, 13559), (17, null, -10508), (18, null, 20682), (19, 31243, -98),
            (73, -32480, 24424), (74, 31, -27428), (75, 31243, -718), (76, null, 20822), (77, 31243, -27428),
            (78, -15934, null), (79, 78, -27428), (80, 8572, -27428), (81, 31243, 4077), (82, null, 114),
            (83, 10, -71), (84, -32489, 32361), (85, null, null), (86, -22984, 32361), (87, 26607, -27428),
            (5, 31243, null), (6, null, -27428), (7, null, 7), (8, 31243, -21951), (9, 13186, 24466),
            (10, null, -8), (11, null, null), (12, -18, 32361), (13, null, -18), (14, 21681, 14079),
            (15, 31241, -17653), (16, 5825, 13559), (17, null, -10508), (18, null, 20682), (19, 31243, -98),
            (73, -32480, 24424), (74, 31, -27428), (75, 31243, -718), (76, null, 20822), (77, 31243, -27428),
            (78, -15934, null), (79, 78, -27428), (80, 8572, -27428), (81, 31243, 4077), (82, null, 114),
            (83, 10, -71), (84, -32489, 32361), (85, null, null), (86, -22984, 32361), (87, 26607, -27428),
            (10, null, -8), (11, null, null), (12, -18, 32361), (13, null, -18), (14, 21681, 14079),
            (15, 31241, -17653), (16, 5825, 13559), (17, null, -10508), (18, null, 20682), (19, 31243, -98),
            (73, -32480, 24424), (74, 31, -27428), (75, 31243, -718), (76, null, 20822), (77, 31243, -27428),
            (78, -15934, null), (79, 78, -27428), (80, 8572, -27428), (81, 31243, 4077), (82, null, 114),
            (83, 10, -71), (84, -32489, 32361), (85, null, null), (86, -22984, 32361), (87, 26607, -27428);
    """

    qt_test """
        SELECT
            T1.pk AS C1,
            T1.col2 AS C2
        FROM
            ${table_tbl1} AS T1 FULL
            OUTER JOIN ${table_tbl2} AS T2 ON T1.col1 <= T2.col2
            OR T2.col1 IN (
                SELECT
                    T3.col2
                FROM
                    ${table_tbl3} AS T3
                WHERE
                    T2.col2 = T3.col1
            )
        ORDER BY
            C1,
            C2 DESC;
    """
}