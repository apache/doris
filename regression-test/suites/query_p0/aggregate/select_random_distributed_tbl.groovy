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

    // test legacy planner
    sql "set enable_nereids_planner = false;"

    def sql1 = "select * from ${tableName} order by user_id, date, city, age, sex;"
    qt_sql "${sql1}"
    def res1 = sql """ explain ${sql1} """
    assertTrue(res1.toString().contains("VAGGREGATE"))

    def sql2 = "select user_id, date, city, age, sex, cost, max_dwell_time, min_dwell_time from ${tableName} order by user_id, date, city, age, sex;"
    qt_sql "${sql2}"
    def res2 = sql """ explain ${sql2} """
    assertTrue(res2.toString().contains("VAGGREGATE"))

    def sql3 = "select user_id+1, date, city, age, sex, cost from ${tableName} order by user_id, date, city, age, sex;"
    qt_sql "${sql3}"
    def res3 = sql """ explain ${sql3} """
    assertTrue(res3.toString().contains("VAGGREGATE"))

    def sql4 = "select user_id, date, city, age, sex, cost+1 from ${tableName} order by user_id, date, city, age, sex;"
    qt_sql "${sql4}"
    def res4 = sql """ explain ${sql4} """
    assertTrue(res4.toString().contains("VAGGREGATE"))

    def sql5 =  "select user_id, sum(cost), max(max_dwell_time), min(min_dwell_time) from ${tableName} group by user_id order by user_id;"
    qt_sql "${sql5}"

    def sql6 = "select count(1) from ${tableName}"
    qt_sql "${sql6}"

    def sql7 = "select count(*) from ${tableName}"
    qt_sql "${sql7}"

    def sql8 = "select max(user_id) from ${tableName}"
    qt_sql "${sql8}"
    def res8 = sql """ explain ${sql8} """
    // no pre agg
    assertFalse(res8.toString().contains("sum"))

    def sql9 = "select max(cost) from ${tableName}"
    qt_sql "${sql9}"
    def res9 = sql """ explain ${sql9} """
    assertTrue(res9.toString().contains("sum"))

    def sql10 = "select sum(max_dwell_time) from ${tableName}"
    qt_sql "${sql10}"

    def sql11 = "select sum(min_dwell_time) from ${tableName}"
    qt_sql "${sql11}"

    sql "drop table ${tableName};"
}