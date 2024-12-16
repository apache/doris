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

// nereids_testJoinOnLeftProjectToJoin
suite ("joinOnLeftPToJoin") {
    sql "SET experimental_enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql """ DROP TABLE IF EXISTS joinOnLeftPToJoin; """
    sql """
            create table joinOnLeftPToJoin (
                time_col dateV2, 
                empid int, 
                name varchar, 
                deptno int, 
                salary int, 
                commission int)
            partition by range (time_col) (partition p1 values less than MAXVALUE) distributed by hash(time_col) buckets 3 properties('replication_num' = '1');
        """

    sql """alter table joinOnLeftPToJoin modify column time_col set stats ('row_count'='3');"""

    sql """insert into joinOnLeftPToJoin values("2020-01-02",2,"b",2,2,2);"""
    sql """insert into joinOnLeftPToJoin values("2020-01-03",3,"c",3,3,3);"""
    sql """insert into joinOnLeftPToJoin values("2020-01-02",2,"b",2,7,2);"""

    sql """ DROP TABLE IF EXISTS joinOnLeftPToJoin_1; """
    sql """
        create table joinOnLeftPToJoin_1 (
            time_col dateV2, 
            deptno int, 
            name varchar, 
            cost int) 
        partition by range (time_col) (partition p1 values less than MAXVALUE) distributed by hash(time_col) buckets 3 properties('replication_num' = '1');
        """


    sql """insert into joinOnLeftPToJoin_1 values("2020-01-02",2,"b",2);"""
    sql """insert into joinOnLeftPToJoin_1 values("2020-01-03",3,"c",3);"""
    sql """insert into joinOnLeftPToJoin_1 values("2020-01-02",2,"b",1);"""

    createMV("create materialized view joinOnLeftPToJoin_mv as select deptno, sum(salary), sum(commission) from joinOnLeftPToJoin group by deptno;")
    sleep(3000)
    createMV("create materialized view joinOnLeftPToJoin_1_mv as select deptno, max(cost) from joinOnLeftPToJoin_1 group by deptno;")
    sleep(3000)

    sql "analyze table joinOnLeftPToJoin with sync;"
    sql "analyze table joinOnLeftPToJoin_1 with sync;"
    sql """set enable_stats=false;"""

    mv_rewrite_all_success("select * from (select deptno , sum(salary) from joinOnLeftPToJoin group by deptno) A join (select deptno, max(cost) from joinOnLeftPToJoin_1 group by deptno ) B on A.deptno = B.deptno;",
    ["joinOnLeftPToJoin_mv", "joinOnLeftPToJoin_1_mv"])

    order_qt_select_mv "select * from (select deptno , sum(salary) from joinOnLeftPToJoin group by deptno) A join (select deptno, max(cost) from joinOnLeftPToJoin_1 group by deptno ) B on A.deptno = B.deptno order by A.deptno;"

    sql """set enable_stats=true;"""
    sql """alter table joinOnLeftPToJoin_1 modify column time_col set stats ('row_count'='3');"""

    mv_rewrite_all_success("select * from (select deptno , sum(salary) from joinOnLeftPToJoin group by deptno) A join (select deptno, max(cost) from joinOnLeftPToJoin_1 group by deptno ) B on A.deptno = B.deptno;",
            ["joinOnLeftPToJoin_mv", "joinOnLeftPToJoin_1_mv"])
}
