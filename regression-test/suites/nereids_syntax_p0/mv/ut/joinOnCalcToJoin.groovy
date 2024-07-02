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
suite ("joinOnCalcToJoin") {
    sql "SET experimental_enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql """ DROP TABLE IF EXISTS joinOnCalcToJoin; """
    sql """
            create table joinOnCalcToJoin (
                time_col dateV2, 
                empid int, 
                name varchar, 
                deptno int, 
                salary int, 
                commission int)
            partition by range (time_col) (partition p1 values less than MAXVALUE) distributed by hash(time_col) buckets 3 properties('replication_num' = '1');
        """

    sql """insert into joinOnCalcToJoin values("2020-01-02",2,"b",2,2,2);"""
    sql """insert into joinOnCalcToJoin values("2020-01-03",3,"c",3,3,3);"""
    sql """insert into joinOnCalcToJoin values("2020-01-02",2,"b",2,7,2);"""

    sql """ DROP TABLE IF EXISTS joinOnCalcToJoin_1; """
    sql """
        create table joinOnCalcToJoin_1 (
            time_col dateV2, 
            deptno int, 
            name varchar, 
            cost int) 
        partition by range (time_col) (partition p1 values less than MAXVALUE) distributed by hash(time_col) buckets 3 properties('replication_num' = '1');
        """

    sql """insert into joinOnCalcToJoin_1 values("2020-01-02",2,"b",2);"""
    sql """insert into joinOnCalcToJoin_1 values("2020-01-03",3,"c",3);"""
    sql """insert into joinOnCalcToJoin_1 values("2020-01-02",2,"b",1);"""

    createMV("create materialized view joinOnLeftPToJoin_mv as select empid, deptno from joinOnCalcToJoin;")
    sleep(3000)
    createMV("create materialized view joinOnLeftPToJoin_1_mv as select deptno, cost from joinOnCalcToJoin_1;")
    sleep(3000)

    sql "analyze table joinOnCalcToJoin with sync;"
    sql "analyze table joinOnCalcToJoin_1 with sync;"
    sql """set enable_stats=false;"""

    explain {
        sql("select * from (select empid, deptno from joinOnCalcToJoin where empid = 0) A join (select deptno, cost from joinOnCalcToJoin_1 where deptno > 0) B on A.deptno = B.deptno;")
        contains "(joinOnLeftPToJoin_mv)"
        contains "(joinOnLeftPToJoin_1_mv)"
    }

    sql """set enable_stats=true;"""
    explain {
        sql("select * from (select empid, deptno from joinOnCalcToJoin where empid = 0) A join (select deptno, cost from joinOnCalcToJoin_1 where deptno > 0) B on A.deptno = B.deptno;")
        contains "(joinOnLeftPToJoin_mv)"
        contains "(joinOnLeftPToJoin_1_mv)"
    }
}
