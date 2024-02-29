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

suite("test_join", "nereids_p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql 'set parallel_fragment_exec_instance_num = 2;'
    sql"use nereids_test_query_db"

    def tbName1 = "test"
    def tbName2 = "baseall"
    def tbName3 = "bigtable"

    order_sql """select j.*, d.* from ${tbName2} j full outer join ${tbName1} d on (j.k1=d.k1) order by j.k1, j.k2, j.k3, j.k4, d.k1, d.k2
            limit 100"""
    order_sql """select * from (select j.k1 j1, j.k2 j2, j.k3 j3, j.k4 j4, j.k5 j5, j.k6 j6, j.k10 j10, j.k11 j11, 
             j.k7 j7, j.k8 j8, j.k9 j9, d.k1 d1, d.k2 d2, d.k3 d3, d.k4 d4, d.k5 d5, d.k6 d6, d.k10 d10, 
             d.k11 d11, d.k7 d7, d.k8 d8, d.k9 d9 from ${tbName2} j left join ${tbName1} d on (j.k1=d.k1) 
             union select j.k1 j1, j.k2 j2, j.k3 j3, j.k4 j4, j.k5 j5, j.k6 j6, j.k10 j10, j.k11 j11, 
             j.k7 j7, j.k8 j8, j.k9 j9, d.k1 d1, d.k2 d2, d.k3 d3, d.k4 d4, d.k5 d5, d.k6 d6, d.k10 d10, d.k11 d11, 
             d.k7 d7, d.k8 d8, d.k9 d9 from ${tbName2} j right join ${tbName1} d on (j.k1=d.k1) ) a order by j1, j2, j3, j4, d1, d2 
             limit 100"""
    qt_join1 """select sum(t1.k1), sum(t1.k3), max(t1.k5), max(t2.k4) from ${tbName1} t1 inner join ${tbName2} t2 on t1.k1 = t2.k1 and 
		    t1.k6 is not null and t2.k6 is not null"""
    qt_join2 """select k1, k2, k3 from ${tbName1} where k7 is not null order by 1 desc, 2 desc, 3 desc limit 10"""
    qt_join3 """select c.k1, c.k8 from ${tbName2} d join (select a.k1 as k1, a.k8 from ${tbName1} a join ${tbName2} b on (a.k1=b.k1)) c
		    on c.k1 = d.k1 order by 1, 2"""
    qt_join4 """select a.k1, b.k1 from ${tbName2} a join (select k1, k2 from ${tbName1} order by k1 limit 10) b 
		    on a.k2=b.k2 order by 1, 2"""
    qt_join5 """select a.k1, b.k2 from ${tbName2} as a join (select k1, k2 from ${tbName1}) as b
		    where a.k1 = b.k1 order by a.k1, b.k2"""
    qt_join6 """select A.k1,B.k1 from ${tbName2} as A join ${tbName1} as B where A.k1=B.k1+1 
		    order by A.k1, A.k2, A.k3, A.k4"""
    qt_join7 """select A.k1, B.k2 from ${tbName2} as A join ${tbName1} as B 
    		    order by A.k1, B.k2 limit 10"""
    qt_join8 """select A.k1 from ${tbName2} as A join ${tbName1} as B order by A.k1 limit 10"""
    qt_join9 """select a.k4 from ${tbName1} a inner join ${tbName2} b on (a.k1=b.k1) 
		    where a.k2>0 and b.k1=1 and a.k1=1 order by 1"""
    qt_join10 """select j.*, d.* from ${tbName1} j inner join ${tbName2} d on (j.k1=d.k1) 
		    order by j.k1, j.k2, j.k3, j.k4"""
    qt_join11 """select a.k1, b.k2, c.k3 
		    from ${tbName1} a join ${tbName2} b on (a.k1=b.k1) join ${tbName1} c on (a.k1 = c.k1)
		    where a.k2>0 and a.k1+50<0"""
    qt_join12 """select t1.k1, t2.k1 from ${tbName1} t1 join ${tbName2} t2 where (t1.k1<3 and t2.k1<3)
		    order by t1.k1, t2.k1 limit 100"""
    qt_join13 """select a.k1, b.k1, a.k2, b.k2 from
            (select k1, k2 from ${tbName1} where k9>0 and k6="false" union all
	        select k1, k2 from ${tbName2} where k6="true" union all
	        select 0, 0) a inner join
	        ${tbName1} b on a.k1=b.k1 and b.k1<5 order by 1, 2, 3, 4"""
    qt_join14 """select a.k1, b.k1, a.k2, b.k2 from
             ${tbName1} b left outer join
            (select k1, k2 from ${tbName2} where k9>0 and k6="false" union all
	        select k1, k2 from ${tbName1} where k6="true" union all
	        select 0, 0) a on a.k1=b.k1 where b.k1<5 and a.k1 is not NULL order by 1, 2, 3, 4"""
    qt_join15 """select a.k1, b.k1, a.k2, b.k2 from
            (select k1, k2 from ${tbName1} where k1=1 and lower(k6) like "%w%" union all
	        select k1, k2 from ${tbName2} where k1=2  union all
	        select 0, 1) a  join
            (select k1, k2 from ${tbName2} where k1=1 and lower(k6) like "%w%" union all
	        select k1, k2 from ${tbName1} where k1>0  union all
	        select 1, 2) b on a.k1 = b.k1 where b.k1<5 order by 1, 2, 3, 4"""
    qt_join16 """select count(*) from 
	        (select k1 from ${tbName1} union distinct
	        select k1 from ${tbName2}) a inner join
	        (select k2 from ${tbName2} union distinct
	        select k2 from ${tbName1}) b on a.k1+1000=b.k2 inner join
	        (select distinct k1 from ${tbName1}) c on a.k1=c.k1"""
    qt_join17 """select count(t1.k1) as wj from ${tbName2} t1 left join
		    ${tbName2} t2 on t1.k10=t2.k10 left join
		    ${tbName2} t3 on t2.k1 = t3.k1"""
    qt_join18 """select j.*, d.* from ${tbName1} j left join ${tbName2} d on (lower(j.k6) = lower(d.k6)) 
		    order by j.k1, j.k2, j.k3, j.k4, d.k2, d.k3, d.k4"""
    qt_join19 """select j.*, d.* from ${tbName1} j right join ${tbName2} d on (lower(j.k6) = lower(d.k6)) 
		    order by j.k1, j.k2, j.k3, j.k4, d.k2, d.k3, d.k4"""
    qt_join20 """select k1, v.k2 from ${tbName2} c, (select k2 from ${tbName1} order by k2 limit 2) v 
		    where k1 in (1, 2, 3) order by 1, 2"""
    qt_join21 """select k1, v.k2 from ${tbName2} c, (select k2 from ${tbName1} order by k2 limit 2) v 
    	    where k1 in (1, 2, 3) and v.k2%2=0 order by 1, 2"""
    qt_join22 """select k1, k2, cnt, avp from ${tbName1} c, (select count(k1) cnt, avg(k2) avp from ${tbName2}) v where k1 <3 
		    order by 1, 2, 3, 4"""
    qt_join23 """select k1, avg(maxp) from ${tbName1} c, (select max(k8) maxp from ${tbName1} group by k1) v where k1<3 group by k1
		    order by 1, 2"""
    qt_join24 """select k1, v.k3, cnt, avp from ${tbName2} c, (select count(k1) cnt, avg(k9) avp, k3 from ${tbName2} group by k3) v
		    where k1<0 order by 1, 2, 3, 4"""
    qt_join25 """select count(k5), k2 from ${tbName1} c, (select ca.k1 okey, cb.k2 opr from ${tbName2} ca, ${tbName1} cb where 
		    ca.k1=cb.k1 and ca.k2+cb.k2>2) v group by k2 order by 1, 2"""
    qt_join26 """select count(k6), k1 from ${tbName1} c, 
		    (select ca.k1 wj, ca.k2 opr from ${tbName2} ca left outer join ${tbName1} cb on ca.k1 = cb.k1) v
		    group by k1 order by 1, 2"""
    qt_join27 """select count(k6), k1 from ${tbName1} c, 
		    (select ca.k1 wj, ca.k2 opr from ${tbName2} ca right outer join ${tbName1} cb 
		    on ca.k1 = cb.k1 and ca.k2+cb.k2>2) v
		    group by k1 order by 1, 2"""
    // Ocurrs time out with specified time 299969 MILLISECONDS

    List selected =  ["a.k1, b.k1, a.k2, b.k2, a.k3, b.k3", "count(a.k1), count(b.k1), count(a.k2), count(b.k2), count(*)"]
    List join_types = ["inner", "left outer", "right outer", ""]

    for (i in selected) {
        qt_join28"""select ${i} from ${tbName1} a join ${tbName2} b 
                on a.k1 = b.k1 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join29"""select ${i} from ${tbName1} a join ${tbName2} b 
                on a.k1 > b.k1 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join30"""select ${i} from ${tbName1} a join ${tbName2} b 
                on a.k1 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join31"""select ${i} from ${tbName1} a join ${tbName2} b 
                on a.k1 = b.k1 and a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join32"""select ${i} from ${tbName1} a join ${tbName2} b 
                on a.k1 = b.k1 and a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join33"""select ${i} from ${tbName1} a join ${tbName2} b 
                order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join34"""select ${i} from ${tbName1} a join ${tbName2} b 
                on a.k1 = b.k1 or a.k2 = b.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join35"""select ${i} from ${tbName1} a join ${tbName2} b 
                on a.k1 < b.k1 or a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join36"""select ${i} from ${tbName1} a join ${tbName2} b 
                on a.k1 = b.k1 or a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join37"""select ${i} from ${tbName1} a join ${tbName2} b 
                on a.k1 = b.k1 or a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join38"""select ${i} from ${tbName1} a join ${tbName2} b 
                on a.k1 < b.k1 or a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join39"""select ${i} from ${tbName1} a join ${tbName2} b on a.k1 = b.k1 
                join ${tbName3} c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join40"""select ${i} from ${tbName1} a join ${tbName2} b on a.k2 = b.k2 and a.k1 > 0 
                join ${tbName3} c on a.k3 = c.k3 and b.k1 = c.k1 + 1 and c.k3 > 0 
                order by 1, 2, 3, 4, 5 limit 65535"""


        // test_inner_join
        qt_inner_join1"""select ${i} from ${tbName1} a inner join ${tbName2} b 
                on a.k1 = b.k1 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_inner_join2"""select ${i} from ${tbName1} a inner join ${tbName2} b 
                on a.k1 > b.k1 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_inner_join3"""select ${i} from ${tbName1} a inner join ${tbName2} b 
                on a.k1 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_inner_join4"""select ${i} from ${tbName1} a inner join ${tbName2} b 
                on a.k1 = b.k1 and a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_inner_join5"""select ${i} from ${tbName1} a inner join ${tbName2} b 
                on a.k1 = b.k1 and a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_inner_join6"""select ${i} from ${tbName1} a inner join ${tbName2} b 
                order by 1, 2, 3, 4, 5 limit 65535"""
        qt_inner_join7"""select ${i} from ${tbName1} a inner join ${tbName2} b 
                on a.k1 = b.k1 or a.k2 = b.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_inner_join8"""select ${i} from ${tbName1} a inner join ${tbName2} b 
                on a.k1 < b.k1 or a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_inner_join9"""select ${i} from ${tbName1} a inner join ${tbName2} b 
                on a.k1 = b.k1 or a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_inner_join10"""select ${i} from ${tbName1} a inner join ${tbName2} b 
                on a.k1 = b.k1 or a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_inner_join11"""select ${i} from ${tbName1} a inner join ${tbName2} b 
                on a.k1 < b.k1 or a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_inner_join12"""select ${i} from ${tbName1} a inner join ${tbName2} b on a.k1 = b.k1 
                inner join ${tbName3} c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_inner_join13"""select ${i} from ${tbName1} a inner join ${tbName2} b on a.k2 = b.k2 and a.k1 > 0 
                inner join ${tbName3} c on a.k3 = c.k3 and b.k1 = c.k1 + 1 and c.k3 > 0 
                order by 1, 2, 3, 4, 5 limit 65535"""
    }

    // join null value
    def table_1 = "join_null_value_left_table"
    def table_2 = "join_null_value_right_table"
    sql"""drop table if exists join_null_value_left_table"""
    sql"""drop table if exists join_null_value_right_table"""
    sql"""create table if not exists join_null_value_left_table (k1 tinyint, k2 decimal(9,3) NULL, k3 char(5) NULL,
                    k4 date NULL, k5 datetime NULL, 
                    k6 double sum) engine=olap 
                    distributed by hash(k1) buckets 2 properties("storage_type"="column", "replication_num" = "1")"""
    sql"""create table if not exists join_null_value_right_table (k1 tinyint, k2 decimal(9,3) NULL, k3 char(5) NULL,
                    k4 date NULL, k5 datetime NULL, 
                    k6 double sum) engine=olap 
                    distributed by hash(k1) buckets 2 properties("storage_type"="column", "replication_num" = "1")"""
    sql"""insert into join_null_value_left_table values (1, NULL,'null', NULL, NULL, 8.9),
                    (2, NULL,'2', NULL, NULL, 8.9),
                    (3, NULL,'null', '2019-09-09', NULL, 8.9);"""
    sql"""insert into join_null_value_right_table values (1, NULL,'null', NULL, NULL, 8.9),
                    (2, NULL,'2', NULL, NULL, 8.9),
                    (3, NULL,'null', '2019-09-09', NULL, 8.9);"""
    sql"""insert into join_null_value_left_table values (5, 2.2,"null", NULL, "2019-09-09 00:00:00", 8.9)"""
    for (type in join_types) {
        for (index in range(1, 7)) {
            qt_join_null_value1"""select * from join_null_value_left_table a ${type} join join_null_value_right_table b on a.k${index} = b.k${index} and 
                a.k2 = b.k2 and a.k${index} != b.k2 order by a.k1, b.k1"""
            qt_join_null_value2"""select * from join_null_value_left_table a ${type} join join_null_value_right_table b on a.k${index} = b.k${index} and 
                a.k2 = b.k2 and a.k${index} != b.k2 order by a.k1, b.k1"""
        }
    }
    //  <=>, =, is NULL, ifnull
    qt_join_null1"""select * from join_null_value_left_table a  left join join_null_value_right_table b on a.k2 <=> b.k2 and 
        a.k3 is NULL order by a.k1, b.k1"""
    qt_join_null2"""select * from join_null_value_left_table a join join_null_value_right_table b on a.k2<=> b.k2 and 
        a.k4<=>NULL order by a.k1,b.k1"""
    qt_join_null3"""select * from join_null_value_left_table a join join_null_value_right_table b on a.k2<=> b.k2
        and a.k4<=>NULL  and b.k4 is not NULL order by a.k1,b.k1"""
    qt_join_null4"""select * from join_null_value_left_table a join join_null_value_right_table b on a.k2<=> b.k2 and 
       a.k4<=>NULL  and b.k4 is not NULL and a.k3=2 order by a.k1,b.k1"""
    qt_join_null5"""select * from join_null_value_left_table a join join_null_value_right_table b on ifnull(a.k4,null)
       <=> ifnull(b.k5,null) order by a.k1, a.k2, a.k3, b.k1, b.k2"""
    sql"drop table join_null_value_left_table"
    sql"drop table join_null_value_right_table"



    // join null string
    def table_3 = "table_join_null_string_1"
    def table_4 = "table_join_null_string_2"
    sql"""drop table if exists ${table_3}"""
    sql"""drop table if exists ${table_4}"""
    sql"""create table if not exists ${table_3} (a int, b varchar(11)) distributed by hash(a) buckets 3 properties("replication_num" = "1")"""
    sql"""create table if not exists ${table_4} (a int, b varchar(11)) distributed by hash(a) buckets 3 properties("replication_num" = "1")"""
    sql"""insert into ${table_3} values (1,"a"),(2,"b"),(3,"c"),(4,NULL)"""
    sql"""insert into ${table_4} values (1,"a"),(2,"b"),(3,"c"),(4,NULL)"""
    def res99 = sql"""select count(*) from ${table_3} join ${table_4} where ${table_3}.b = ${table_4}.b"""
    def res100 = sql"""select 3"""
    check2_doris(res99, res100)
    sql"""drop table ${table_3}"""
    sql"""drop table ${table_4}"""

    qt_sql """select k1 from baseall left semi join test on true order by k1;"""
    qt_sql """select k1 from baseall left semi join test on false order by k1;"""
    qt_sql """select k1 from baseall left anti join test on true order by k1;"""
    qt_sql """select k1 from baseall left anti join test on false order by k1;"""

    qt_sql """select k1 from test right semi join baseall on true order by k1;"""
    qt_sql """select k1 from test right semi join baseall on false order by k1;"""
    qt_sql """select k1 from test right anti join baseall on true order by k1;"""
    qt_sql """select k1 from test right anti join baseall on false order by k1;"""
}
