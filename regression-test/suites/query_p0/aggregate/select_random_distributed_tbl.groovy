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

suite("select_random_distributed_tbl") {
    def tableName = "random_distributed_tbl_test"

    sql "drop table if exists ${tableName};"
    sql "set enable_agg_state=true;"
    sql """ admin set frontend config("enable_quantile_state_type"="true"); """
    sql """
    CREATE TABLE ${tableName}
    (
        `k1` LARGEINT NOT NULL,
        `k2` VARCHAR(20) NULL,
        `v_sum` BIGINT SUM NULL DEFAULT "0",
        `v_max` INT MAX NULL DEFAULT "0",
        `v_min` INT MIN NULL DEFAULT "99999",
        `v_generic` AGG_STATE<avg(int NULL)> GENERIC,
        `v_hll` HLL HLL_UNION NOT NULL,
        `v_bitmap` BITMAP BITMAP_UNION NOT NULL,
        `v_quantile_union` QUANTILE_STATE QUANTILE_UNION NOT NULL
    ) ENGINE=OLAP
    AGGREGATE KEY(`k1`, `k2`)
    COMMENT 'OLAP'
    DISTRIBUTED BY RANDOM BUCKETS 10
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """
    
    sql """ insert into ${tableName} values(1,"a",1,1,1,avg_state(1),hll_hash(1),bitmap_hash(1),to_quantile_state(1, 2048)) """
    sql """ insert into ${tableName} values(1,"a",2,2,2,avg_state(2),hll_hash(2),bitmap_hash(2),to_quantile_state(2, 2048)) """
    sql """ insert into ${tableName} values(1,"a",3,3,3,avg_state(3),hll_hash(3),bitmap_hash(3),to_quantile_state(3, 2048)) """
    sql """ insert into ${tableName} values(2,"b",4,4,4,avg_state(4),hll_hash(4),bitmap_hash(4),to_quantile_state(4, 2048)) """
    sql """ insert into ${tableName} values(2,"b",5,5,5,avg_state(5),hll_hash(5),bitmap_hash(5),to_quantile_state(5, 2048)) """
    sql """ insert into ${tableName} values(2,"b",6,6,6,avg_state(6),hll_hash(6),bitmap_hash(6),to_quantile_state(6, 2048)) """

    for (int i = 0; i < 2; ++i) {
        if (i == 0) {
            // test legacy planner
            sql "set enable_nereids_planner = false;"
        } else if (i == 1) {
            // test nereids planner
            sql "set enable_nereids_planner = true;"
        }

        def whereStr = ""
        for (int j = 0; j < 2; ++j) {
            if (j == 1) {
                // test with filter
                whereStr = "where k1 > 0"
            }
            def sql1 = "select * except (v_generic) from ${tableName} ${whereStr} order by k1, k2"
            qt_sql_1 "${sql1}"
            def res1 = sql """ explain ${sql1} """
            assertTrue(res1.toString().contains("VAGGREGATE"))

            def sql2 = "select k1 ,k2 ,v_sum ,v_max ,v_min ,v_hll ,v_bitmap ,v_quantile_union from ${tableName} ${whereStr} order by k1, k2"
            qt_sql_2 "${sql2}"
            def res2 = sql """ explain ${sql2} """
            assertTrue(res2.toString().contains("VAGGREGATE"))

            def sql3 = "select k1+1, k2, v_sum from ${tableName} ${whereStr} order by k1, k2"
            qt_sql_3 "${sql3}"
            def res3 = sql """ explain ${sql3} """
            assertTrue(res3.toString().contains("VAGGREGATE"))

            def sql4 = "select k1, k2, v_sum+1 from ${tableName} ${whereStr} order by k1, k2"
            qt_sql_4 "${sql4}"
            def res4 = sql """ explain ${sql4} """
            assertTrue(res4.toString().contains("VAGGREGATE"))

            def sql5 =  """ select k1, sum(v_sum), max(v_max), min(v_min), avg_merge(v_generic), 
                hll_union_agg(v_hll), bitmap_union_count(v_bitmap), quantile_percent(quantile_union(v_quantile_union),0.5) 
                from ${tableName} ${whereStr} group by k1 order by k1 """
            qt_sql_5 "${sql5}"

            def sql6 = "select count(1) from ${tableName} ${whereStr}"
            qt_sql_6 "${sql6}"

            def sql7 = "select count(*) from ${tableName} ${whereStr}"
            qt_sql_7 "${sql7}"

            def sql8 = "select max(k1) from ${tableName} ${whereStr}"
            qt_sql_8 "${sql8}"
            def res8 = sql """ explain ${sql8} """
            // no pre agg
            assertFalse(res8.toString().contains("sum"))

            def sql9 = "select max(v_sum) from ${tableName} ${whereStr}"
            qt_sql_9 "${sql9}"
            def res9 = sql """ explain ${sql9} """
            assertTrue(res9.toString().contains("sum"))

            def sql10 = "select sum(v_max) from ${tableName} ${whereStr}"
            qt_sql_10 "${sql10}"

            def sql11 = "select sum(v_min) from ${tableName} ${whereStr}"
            qt_sql_11 "${sql11}"

            // test group by value
            def sql12 = "select v_min, sum(v_sum) from ${tableName} ${whereStr} group by v_min order by v_min"
            qt_sql_12 "${sql12}"

            def sql13 = "select count(k1) from ${tableName} ${whereStr}"
            qt_sql_13 "${sql13}"

            def sql14 = "select count(distinct k1) from ${tableName} ${whereStr}"
            qt_sql_14 "${sql14}"

            def sql15 = "select count(v_sum) from ${tableName} ${whereStr}"
            qt_sql_15 "${sql15}"

            def sql16 = "select count(distinct v_sum) from ${tableName} ${whereStr}"
            qt_sql_16 "${sql16}"
        }
    }

    sql "drop table ${tableName};"

    // test all keys are NOT NULL for AGG table
    sql "drop table if exists random_distributed_tbl_test_2;"
    sql """ CREATE TABLE random_distributed_tbl_test_2 (
        `k1` LARGEINT NOT NULL
    ) ENGINE=OLAP
    AGGREGATE KEY(`k1`)
    COMMENT 'OLAP'
    DISTRIBUTED BY RANDOM BUCKETS 10
    PROPERTIES (
        "replication_num" = "1"
    );
    """

    sql """ insert into random_distributed_tbl_test_2 values(1); """
    sql """ insert into random_distributed_tbl_test_2 values(1); """
    sql """ insert into random_distributed_tbl_test_2 values(1); """

    sql "set enable_nereids_planner = false;"
    qt_sql_17 "select k1 from random_distributed_tbl_test_2;"
    qt_sql_18 "select distinct k1 from random_distributed_tbl_test_2;"

    sql "set enable_nereids_planner = true;"
    qt_sql_19 "select k1 from random_distributed_tbl_test_2;"
    qt_sql_20 "select distinct k1 from random_distributed_tbl_test_2;"

    sql "drop table random_distributed_tbl_test_2;"
}
