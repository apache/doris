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

suite("test_mark_join", "nereids_p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "drop table if exists `test_mark_join_t1`;"
    sql "drop table if exists `test_mark_join_t2`;"

    sql """
        CREATE TABLE IF NOT EXISTS `test_mark_join_t1` (
          k1 int not null,
          k2 int,
          k3 bigint,
          v1 varchar(255) not null,
          v2 varchar(255),
          v3 varchar(255)
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`, `k2`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "in_memory" = "false",
          "storage_format" = "V2"
        );
    """

    sql """
        CREATE TABLE IF NOT EXISTS `test_mark_join_t2` (
          k1 int not null,
          k2 int,
          k3 bigint,
          v1 varchar(255) not null,
          v2 varchar(255),
          v3 varchar(255)
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`, `k2`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "in_memory" = "false",
          "storage_format" = "V2"
        );
    """

    sql """
        insert into `test_mark_join_t1` values
            (1,     1,      1,      'abc',      'efg',      'hjk'),
            (2,     2,      2,      'aabb',     'eeff',     'ccdd'),
            (3,     null,   3,      'iii',      null,       null),
            (3,     null,   null,   'hhhh',     null,       null),
            (4,     null,   4,      'dddd',     'ooooo',    'kkkkk'
        );
    """

    sql """
        insert into `test_mark_join_t2` values
            (1,     1,      1,      'abc',      'efg',      'hjk'),
            (2,     2,      2,      'aabb',     'eeff',     'ccdd'),
            (3,     null,   null,   'diid',     null,       null),
            (3,     null,   3,      'ooekd',    null,       null),
            (4,     4,   null,   'oepeld',   null,       'kkkkk'
        );
    """

    qt_mark_join1 """
        select
            k1, k2
            , k1 not in (select test_mark_join_t2.k2 from test_mark_join_t2 where test_mark_join_t2.k3 < test_mark_join_t1.k3) vv
        from test_mark_join_t1 order by 1, 2, 3;
    """

    qt_mark_join2 """
        select
            k1, k2
            , k2 not in (select test_mark_join_t2.k3 from test_mark_join_t2 where test_mark_join_t2.k2 > test_mark_join_t1.k3) vv
        from test_mark_join_t1 order by 1, 2, 3;
    """

    qt_mark_join3 """
        select
            k1, k2
            , k1 in (select test_mark_join_t2.k1 from test_mark_join_t2 where test_mark_join_t2.k3 < test_mark_join_t1.k3) vv
        from test_mark_join_t1 order by 1, 2, 3;
    """

    qt_mark_join4 """
        select
            k1, k2
            , k1 not in (select test_mark_join_t2.k2 from test_mark_join_t2 where test_mark_join_t2.k3 = test_mark_join_t1.k3) vv
        from test_mark_join_t1 order by 1, 2, 3;
    """

    qt_mark_join5 """
        select
            k1, k2
            , k2 not in (select test_mark_join_t2.k3 from test_mark_join_t2 where test_mark_join_t2.k2 = test_mark_join_t1.k3) vv
        from test_mark_join_t1 order by 1, 2, 3;
    """

    qt_mark_join6 """
        select
            k1, k2
            , k1 in (select test_mark_join_t2.k1 from test_mark_join_t2 where test_mark_join_t2.k3 = test_mark_join_t1.k3) vv
        from test_mark_join_t1 order by 1, 2, 3;
    """


}
