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

suite("test_select_mv") {

    def wait_mv_finish = { db ->
        while(true) {
            Thread.sleep(1000)
            boolean finished = true;
            def result = sql """SHOW ALTER TABLE MATERIALIZED VIEW FROM ${db};"""
            for (int i = 0; i < result.size(); i++) {
                if (result[i][8] != 'FINISHED') {
                    finished = false;
                    break;
                }
            }
            if (finished) {
                break;
            }
        }
    }

    def dup_sql1 = """select count(*) from test_dup;"""
    def dup_sql2 = """select mv_key2 from test_dup index dup1 order by mv_key2;"""
    def dup_sql3 = """select count(mv_key2) from test_dup index dup1;"""
    def dup_sql4 = """select min(mv_key2), max(mv_key2), count(mv_key2), sum(mv_key2) from test_dup index dup1;"""
    def dup_sql5 = """select `mva_SUM__CAST(``value`` AS BIGINT)` as a from test_dup index dup1 order by a;"""
    def dup_sql6 = """select count(`mva_SUM__CAST(``value`` AS BIGINT)`) from test_dup index dup1;"""
    def dup_sql7 = """select min(`mva_SUM__CAST(``value`` AS BIGINT)`), max(`mva_SUM__CAST(``value`` AS BIGINT)`), ndv(`mva_SUM__CAST(``value`` AS BIGINT)`), sum(`mva_SUM__CAST(``value`` AS BIGINT)`) from test_dup index dup1;"""

    def agg_sql1 = """select count(*) from test_agg;"""
    def agg_sql2 = """select mv_key2 from test_agg index agg1 order by mv_key2;"""
    def agg_sql3 = """select count(mv_key2) from test_agg index agg1;"""
    def agg_sql4 = """select min(mv_key2), max(mv_key2), count(mv_key2), sum(mv_key2) from test_agg index agg1;"""
    def agg_sql5 = """select `mva_SUM__CAST(``value`` AS BIGINT)` as a from test_agg index agg1 order by a;"""
    def agg_sql6 = """select count(`mva_SUM__CAST(``value`` AS BIGINT)`) from test_agg index agg1;"""
    def agg_sql7 = """select min(`mva_SUM__CAST(``value`` AS BIGINT)`), max(`mva_SUM__CAST(``value`` AS BIGINT)`), ndv(`mva_SUM__CAST(``value`` AS BIGINT)`), sum(`mva_SUM__CAST(``value`` AS BIGINT)`) from test_agg index agg1;"""


    sql """drop database if exists test_select_mv"""
    sql """create database test_select_mv"""
    sql """use test_select_mv"""

    sql """CREATE TABLE test_dup (
            key1 int NOT NULL,
            key2 int NOT NULL,
            value int NOT NULL
        )ENGINE=OLAP
        DUPLICATE KEY(`key1`, `key2`)
        DISTRIBUTED BY HASH(`key1`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1" 
        );
    """

    createMV("create materialized view dup1 as select key2, sum(value) from test_dup group by key2;")

    sql """CREATE TABLE test_agg (
            key1 int NOT NULL,
            key2 int NOT NULL,
            value int SUM NOT NULL
        )ENGINE=OLAP
        AGGREGATE KEY(`key1`, `key2`)
        DISTRIBUTED BY HASH(`key1`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1" 
        );
    """

    createMV("create materialized view agg1 as select key2, sum(value) from test_agg group by key2;")

    sql """insert into test_dup values (1, 1, 1), (2, 2, 2)"""
    sql """insert into test_dup values (1, 1, 1), (2, 2, 2)"""
    sql """insert into test_agg values (1, 1, 1), (2, 2, 2)"""
    sql """insert into test_agg values (1, 1, 1), (2, 2, 2)"""

    qt_dup_sql1 dup_sql1
    qt_dup_sql2 dup_sql2
    qt_dup_sql3 dup_sql3
    qt_dup_sql4 dup_sql4
    qt_dup_sql5 dup_sql5
    qt_dup_sql6 dup_sql6
    qt_dup_sql7 dup_sql7

    qt_agg_sql1 agg_sql1
    qt_agg_sql2 agg_sql2
    qt_agg_sql3 agg_sql3
    qt_agg_sql4 agg_sql4
    qt_agg_sql5 agg_sql5
    qt_agg_sql6 agg_sql6
    qt_agg_sql7 agg_sql7

    sql """drop database if exists test_select_mv"""
}

