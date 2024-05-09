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
    sql """
    CREATE TABLE ${tableName}
    (
        `user_id` LARGEINT NOT NULL COMMENT "用户id",
        `date` DATE NOT NULL COMMENT "数据灌入日期时间",
        `city` VARCHAR(20) COMMENT "用户所在城市",
        `age` SMALLINT COMMENT "用户年龄",
        `sex` TINYINT COMMENT "用户性别",
        `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
        `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
        `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间"
    )
    AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`)
    DISTRIBUTED BY RANDOM BUCKETS 10
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """
    
    sql "insert into ${tableName} values (1,'2024-01-01','beijing',10,1,10,50,200);"
    sql "insert into ${tableName} values (1,'2024-01-01','beijing',10,1,20,200,80);"
    sql "insert into ${tableName} values (2,'2024-01-02','shanghai',10,1,5,40,20);"
    sql "insert into ${tableName} values (2,'2024-01-02','shanghai',10,1,30,90,120);"

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
                whereStr = "where user_id > 0"
            }
            def sql1 = "select * from ${tableName} ${whereStr} order by user_id, date, city, age, sex;"
            qt_sql_1 "${sql1}"
            def res1 = sql """ explain ${sql1} """
            assertTrue(res1.toString().contains("VAGGREGATE"))

            def sql2 = "select user_id, date, city, age, sex, cost, max_dwell_time, min_dwell_time from ${tableName} ${whereStr} order by user_id, date, city, age, sex;"
            qt_sql_2 "${sql2}"
            def res2 = sql """ explain ${sql2} """
            assertTrue(res2.toString().contains("VAGGREGATE"))

            def sql3 = "select user_id+1, date, city, age, sex, cost from ${tableName} ${whereStr} order by user_id, date, city, age, sex;"
            qt_sql_3 "${sql3}"
            def res3 = sql """ explain ${sql3} """
            assertTrue(res3.toString().contains("VAGGREGATE"))

            def sql4 = "select user_id, date, city, age, sex, cost+1 from ${tableName} ${whereStr} order by user_id, date, city, age, sex;"
            qt_sql_4 "${sql4}"
            def res4 = sql """ explain ${sql4} """
            assertTrue(res4.toString().contains("VAGGREGATE"))

            def sql5 =  "select user_id, sum(cost), max(max_dwell_time), min(min_dwell_time) from ${tableName} ${whereStr} group by user_id order by user_id;"
            qt_sql_5 "${sql5}"

            def sql6 = "select count(1) from ${tableName} ${whereStr}"
            qt_sql_6 "${sql6}"

            def sql7 = "select count(*) from ${tableName} ${whereStr}"
            qt_sql_7 "${sql7}"

            def sql8 = "select max(user_id) from ${tableName} ${whereStr}"
            qt_sql_8 "${sql8}"
            def res8 = sql """ explain ${sql8} """
            // no pre agg
            assertFalse(res8.toString().contains("sum"))

            def sql9 = "select max(cost) from ${tableName} ${whereStr}"
            qt_sql_9 "${sql9}"
            def res9 = sql """ explain ${sql9} """
            assertTrue(res9.toString().contains("sum"))

            def sql10 = "select sum(max_dwell_time) from ${tableName} ${whereStr}"
            qt_sql_10 "${sql10}"

            def sql11 = "select sum(min_dwell_time) from ${tableName} ${whereStr}"
            qt_sql_11 "${sql11}"

            // test group by value
            def sql12 = "select min_dwell_time, sum(cost) from ${tableName} ${whereStr} group by min_dwell_time order by min_dwell_time"
            qt_sql_12 "${sql12}"

            def sql13 = "select count(user_id) from ${tableName} ${whereStr}"
            qt_sql_13 "${sql13}"

            def sql14 = "select count(distinct user_id) from ${tableName} ${whereStr}"
            qt_sql_14 "${sql14}"

            def sql15 = "select count(cost) from ${tableName} ${whereStr}"
            qt_sql_15 "${sql15}"

            def sql16 = "select count(distinct cost) from ${tableName} ${whereStr}"
            qt_sql_16 "${sql16}"
        }
    }

    sql "drop table ${tableName};"
}