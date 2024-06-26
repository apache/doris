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

suite("order_group", "query,p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql"use nereids_test_query_db"
    def tableName1 ="test"
    def tableName2 ="baseall"

    // order by
    qt_orderBy1 "select k1, k10 from ${tableName1} order by 1, 2 limit 1000"
    qt_orderBy2 "select k1, k8 from ${tableName1} order by 1, 2 desc limit 1000"
    qt_orderBy3 "select k4, k10 from (select k4, k10 from ${tableName1} order by 1, 2 limit 1000000) as i \
		    order by 1, 2 limit 1000"
    qt_orderBy4 "select * from ${tableName1} where k1<-1000 order by k1"
    for (i in range(1, 12)){
        for (j in range(1, 12)) {
            if (i != j & j != 7 & i != 7 & i != 6 & j != 6) {
                qt_orderBy5 "select k${i}, k${j} from ${tableName1} order by k${i}, k${j}"
                qt_orderBy6 "select k${i}, k${j} from ${tableName1} order by k${i}, k${j} asc"
                qt_orderBy7 "select k${i}, k${j} from ${tableName1} order by k${i}, k${j} desc"
            }
        }
    }

    // group
    qt_group1 "select min(k5) from ${tableName1}"
    qt_group2 "select max(k5) from ${tableName1}"
    qt_group3 "select avg(k5) from ${tableName1}"
    qt_group4 "select sum(k5) from ${tableName1}"
    qt_group5 "select count(k5) from ${tableName1}"
    qt_group6 "select min(k5) from ${tableName1} group by k2 order by min(k5)"
    qt_group7 "select max(k5) from ${tableName1} group by k1 order by max(k5)"
    qt_group8 "select avg(k5) from ${tableName1} group by k1 order by avg(k5)"
    qt_group9 "select sum(k5) from ${tableName1} group by k1 order by sum(k5)"
    qt_group10 "select count(k5) from ${tableName1} group by k1 order by count(k5)"
    qt_group11 "select lower(k6), avg(k8), sum(k8),count(k8),  min(k8), max(k8)\
		    from ${tableName1} group by lower(k6) \
		    order by avg(k8), sum(k8),count(k8),  min(k8), max(k8)" 
    qt_group12 "select k2, avg(k8) from ${tableName1} group by k2 \
		    order by k2, avg(k8)" 
    qt_group13 "select k2, sum(k8) from ${tableName1} group by k2 \
		    order by k2, sum(k8)" 
    qt_group14 "select k2, count(k8) from ${tableName1} group by k2 \
		    order by k2, count(k8)" 
    qt_group15 "select k2, min(k8) from ${tableName1} group by k2 \
		    order by k2, min(k8)" 
    qt_group16 "select k2, max(k8) from ${tableName1} group by k2 \
		    order by k2, max(k8)" 
    qt_group17 "select k6, avg(k8) from ${tableName1} group by k6 having k6=\"true\"\
		    order by k6, avg(k8)" 
    qt_group18 "select k6, sum(k8) from ${tableName1} group by k6 having k6=\"true\" \
		    order by k6, sum(k8)" 
    qt_group19 "select k6, count(k8) from ${tableName1} group by k6 having k6=\"true\" \
		    order by k6, count(k8)" 
    qt_group20 "select k6, min(k8) from ${tableName1} group by k6 having k6=\"true\" \
		    order by k6, min(k8)" 
    qt_group21 "select k6, max(k8) from ${tableName1} group by k6 having k6=\"true\" \
		    order by k6, max(k8)" 
    qt_group22 "select k2, avg(k8) from ${tableName1} group by k2 having k2<=1989 \
		    order by k2, avg(k8)" 
    qt_group23 "select k2, sum(k8) from ${tableName1} group by k2 having k2<=1989 \
		    order by k2, sum(k8)" 
    qt_group24 "select k2, count(k8) from ${tableName1} group by k2 having k2<=1989 \
		    order by k2, count(k8)" 
    qt_group25 "select k2, min(k8) from ${tableName1} group by k2 having k2<=1989 \
		    order by k2, min(k8)" 
    qt_group26 "select k2, max(k8) from ${tableName1} group by k2 having k2<=1989 \
		    order by k2, max(k8)" 
    qt_group27 "select count(ALL *) from ${tableName1} where k5 is not null group by k1%10 order by 1"
    qt_group28 "select k5, k5*2, count(*) from ${tableName1} group by 1, 2 order by 1, 2,3"
    qt_group29 "select k1%3, k2%3, count(*) from ${tableName1} where k4>0 group by 2, 1 order by 1, 2 ,3"
    qt_group30 "select k1%2, k2%2, k3%3, k4%3, k11, count(*) from ${tableName1} \
		    where (k11='2015-03-13 12:36:38' or k11 = '2000-01-01 00:00:00')\
		    and k5 is not null group by 1, 2, 3, 4, 5 order by 1, 2, 3, 4, 5" 
    qt_group31 "select count(*) from ${tableName1} where (k11='2015-03-13 12:36:38' or k11 = '2000-01-01 00:00:00')\
		    and k5 is not null group by k1%2, k2%2, k3%3, k4%3, k11%2 order by 1"
    qt_group32 "select count(*), min(k1), max(k1), sum(k1), avg(k1) from ${tableName1} where k1=10000 order by 1"
    qt_group33 "select k1 % 7, count(*), avg(k1) from ${tableName1} where k4>0 group by 1 having avg(k1) > 2 or count(*)>5\
		    order by 1, 2, 3"
    qt_group34 "select k10, count(*) from ${tableName1} where k5 is not null group by k10 \
		    having k10<cast('2010-01-01 01:05:20' as datetime) order by 1, 2"
    qt_group35 "select k1*k1, k1+k1 as c from ${tableName1} group by k1*k1, k1+k1, k1*k1 having (c)<5\
		    order by 1, 2 limit 10"
    qt_group36 "select 1 from (select count(k4) c from ${tableName1} having min(k1) is not null) as t \
		    where c is not null"
    qt_group37 "select count(k1), sum(k1*k2) from ${tableName1} order by 1, 2"
    qt_group38 "select k1%2, k2+1, k3 from ${tableName1} where k3>10000 group by 1,2,3 order by 1,2,3" \

    qt_group39 "select extract(year from k10) as wj, extract(month from k10) as dyk, sum(k1)\
		    from ${tableName1} group by 1, 2 order by 1,2,3"

    // with having
    qt_group40 "select avg(k1) as a from ${tableName1} group by k2 having a > 10 order by a"
    qt_group41 "select avg(k5) as a from ${tableName1} group by k1 having a > 100 order by a"
    qt_group42 "select sum(k5) as a from ${tableName1} group by k1 having a < 100.0 order by a"
    qt_group43 "select sum(k8) as a from ${tableName1} group by k1 having a > 100 order by a"
    qt_group44 "select avg(k9) as a from ${tableName1} group by k1 having a < 100.0 order by a"

    // order 2
    qt_order8 "select k1, k2 from (select k1, max(k2) as k2 from ${tableName1} where k1 > 0 group by k1 \
            order by k1)a where k1 > 0 and k1 < 10 order by k1"
    qt_order9 "select k1, k2 from (select k1, max(k2) as k2 from ${tableName1} where k1 > 0 group by k1 \
            order by k1)a left join (select k1 as k3, k2 as k4 from ${tableName2}) b on a.k1 = b.k3 \
            where k1 > 0 and k1 < 10 order by k1, k2"
    qt_order10 "select k1, count(*) from ${tableName1} group by 1 order by 1 limit 10"
    qt_order11 "select a.k1, b.k1, a.k6 from ${tableName2} a join ${tableName1} b on a.k1 = b.k1 where a.k2 > 0 \
            and a.k1 + b.k1 > 20 and b.k6 = \"false\" order by a.k1"
    qt_order12 "select k1 from baseall order by k1 % 5, k1"
    qt_order13 "select k1 from (select k1, k2 from ${tableName2} order by k1 limit 10) a where k1 > 5 \
            order by k1 limit 10"
    qt_order14 "select k1 from (select k1, k2 from ${tableName2} order by k1) a where k1 > 5 \
            order by k1 limit 10"
    qt_order15 "select k1 from (select k1, k2 from ${tableName2} order by k1 limit 10 offset 3) a \
            where k1 > 5 order by k1 limit 5 offset 2"
    //qt_order16 "select a.k1, a.k2, b.k1 from ${tableName2} a join (select * from ${tableName1} where k6 = \"false\" \
      //      order by k1 limit 3 offset 2) b on a.k1 = b.k1 where a.k2 > 0 order by 1"


    // with NULL values
    try_sql "select k4 + k5 from ${tableName1} nulls first"
    def res1 = sql"select k4 + k5 as sum, k5, k5 + k6 as nu from ${tableName1} where k6 not like 'na%' and\
       k6 not like 'INf%' order by sum nulls first"
    def res2 = sql"select k4 + k5 as sum, k5,  k5 + k7 as nu from ${tableName1} where k6 not like 'na%' and\
        k6 not like 'INf%' order by sum nulls first"
    check2_doris(res1, res2)

    qt_orderBy_withNull_1 "select k4 + k5 from ${tableName1} order by 1 nulls first"
    // line2 = "select k4 + k5 from ${tableName1} order by 1"

    // NULL结果
    qt_orderBy_withNull_2 "select k5, k5 + k6 from ${tableName1} where lower(k6) not like 'na%' and\
        upper(k6) not like 'INF%' order by k5 nulls first"
    // line2 = "select k5, NULL from ${tableName1} where lower(k6) not like 'na%' and\
    //     upper(k6) not like 'INF%' order by k5"

    // null 和非null
    qt_orderBy_withNull_3 " select a.k1 ak1, b.k1 bk1 from ${tableName1} a \
       right join ${tableName2} b on a.k1=b.k1 and b.k1>10 \
       order by ak1 desc nulls first, bk1"
    // line2 = " select a.k1 ak1, b.k1 bk1 from ${tableName1} a \
    //        right join ${tableName2} b on a.k1=b.k1 and b.k1>10 \
    //        order by isnull(ak1) desc, ak1 desc, bk1"

    // NULL列group by
    qt_orderBy_withNull_4 "select k5 + k4 as nu, sum(k1) from ${tableName1} group by nu order by nu\
        nulls first"
    // line2 = "select k4 + k5 as nu, sum(k1) from ${tableName1} group by nu order by nu"
    qt_orderBy_withNull_5 "select k6 + k5 as nu from test group by nu"
    // line4 = "select NULL"
    qt_orderBy_withNull_6 "select k6 + k5 as nu, sum(1) from test  group by nu order by nu  desc limit 5"
    // line2 = "select NULL, count(1) from test"
    qt_orderBy_withNull_7 "select k6 + k5 as nu, sum(1) from test  group by nu order by nu limit 5"

    // 窗口函数对NULL的处理
    def res3 = sql"select k1, k2, nu from (select k1, k2, k5, k5 + k6 as nu,\
          sum(k2) over (partition by k5 + k6)\
         as ss from ${tableName2})s  where s.k5 > 2000 order by k1 nulls first"
    def res4 = sql"select k1, k2, nu from (select k1, k2, k5, k5 + k6 as nu,\
         sum(k2) over (partition by k5 + k6)\
         as ss from ${tableName2}  where k5 > 2000 )s order by k1"
    check2_doris(res3, res4)

    // 2
    // 非NULL结果
    try_sql"select k4 + k5 from ${tableName1} nulls last"
    qt_orderBy_withNull_8 "select k4 + k5 as sum, k5 + k6 as nu from ${tableName1}  where lower(k6) not like 'na%' and\
       upper(k6) not like 'INF%' order by sum nulls last"
    // line2 = "select k4 + k5 as sum, NULL as nu from ${tableName1} where lower(k6) not like 'na%' and\
    //    upper(k6) not like 'INF%' order by sum"
    qt_orderBy_withNull_9 "select k4 + k5 as nu from ${tableName1} order by nu nulls last"
    // line2 = "select k4 + k5 as nu from ${tableName1} order by nu"

    //null 和非null
    qt_orderBy_withNull_10 " select a.k1 ak1, b.k1 bk1 from ${tableName1} a \
       right join ${tableName2} b on a.k1=b.k1 and b.k1>10 \
       order by ak1 nulls last, bk1"
    //    line2 = " select a.k1 ak1, b.k1 bk1 from ${tableName1} a \
    //           right join ${tableName2} b on a.k1=b.k1 and b.k1>10 \
    //           order by isnull(ak1), ak1, bk1"

    // NULL列group by
    def res5 = order_sql"""select k5 + k6 as nu, sum(k1) from ${tableName1} group by nu order by nu,
        sum(k1) nulls last"""
    def res6 = order_sql"""select k6 + k5 as nu, sum(k1) from ${tableName1} group by nu order by nu, sum(k1)"""
    check2_doris(res5, res6)
    //issue https://github.com/apache/doris/issues/2142
    def res7 = sql "select k1, k2, nu from (select k1, k2, k5, k5 + k6 as nu,\
          sum(k2) over (partition by k5 + k6)\
         as ss from ${tableName2})s  where s.k5 > 2000 order by k1,k2 nulls last"
    def res8 = sql "select k1, k2, nu from (select k1, k2, k5, k5 + k6 as nu,\
         sum(k2) over (partition by k5 + k6)\
         as ss from ${tableName2}  where k5 > 2000 )s order by k1,k2 "
    check2_doris(res7, res8)

    qt_group31 "select count(*) from ${tableName1} where (k11='2015-03-13 12:36:38' or k11 = '2000-01-01 00:00:00')\
		    and k5 is not null group by k1%2, k2%2, k3%3, k4%3, k11%2 order by count(*)"
    qt_group1 "select min(k5) from ${tableName1}"
    qt_orderBy_withNull_3 " select a.k1 ak1, b.k1 bk1 from ${tableName1} a \
       right join ${tableName2} b on a.k1=b.k1 and b.k1>10 \
       order by bk1 desc nulls first, ak1"
    qt_orderBy_withNull_3 " select a.k1 ak1, b.k1 bk1 from ${tableName1} a \
       right join ${tableName2} b on a.k1=b.k1 and b.k1>10 \
       order by bk1 desc nulls last, ak1"
    qt_orderBy_withNull_3 " select a.k1 ak1, b.k1 bk1 from ${tableName1} a \
       right join ${tableName2} b on a.k1=b.k1 and b.k1>10 \
       order by bk1 desc, ak1"
}
