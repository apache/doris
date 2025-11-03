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

suite("test_keyword", "query,p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql("use nereids_test_query_db")

    def tableName1 = "test"
    def tableName2 = "baseall"

    // distinct
    qt_distinct1 "select distinct k1 from ${tableName1} order by k1"
    qt_distinct2 "select distinct k2 from ${tableName1} order by k2"
    qt_distinct3 "select distinct k3 from ${tableName1} order by k3"
    qt_distinct5 "select distinct k4 from ${tableName1} order by k4"
    qt_distinct6 "select distinct k5 from ${tableName1} order by k5"
    qt_distinct8 "select distinct upper(k6) from ${tableName1} order by upper(k6)"
    qt_distinct9 "select distinct k8 from ${tableName1} order by k8"
    qt_distinct10 "select distinct k9 from ${tableName1} order by k9"
    qt_distinct11 "select distinct k10 from ${tableName1} order by k10"
    qt_distinct12 "select distinct k11 from ${tableName1} order by k11"
    qt_distinct13 "select distinct k1, upper(k6), k9 from ${tableName1} order by k1, upper(k6), k9"
    qt_distinct14 "select count(distinct k1, k5) from ${tableName1}"
    qt_distinct15 "select k1, count(distinct k3), sum(distinct k2), count(k6) from ${tableName1} \
		    group by 1 order by 1, 2, 3"
    qt_distinct16 "select count(distinct k1) from ${tableName1} order by max(distinct k1) limit 100"
    qt_distinct17 "select distinct * from ${tableName1} where k1<20 order by k1, k2, k3, k4"
    qt_distinct18 "select distinct * from ${tableName1} order by k1, k2, k3, k4"
    qt_distinct19 "select count(distinct NULL) from ${tableName1}"
//     qt_distinct20 "select count(distinct k1, NULL) from ${tableName1}"
    qt_distinct21 "select t1.c, t2.c from (select count(distinct k1) as c from ${tableName1}) t1 join \
		                     (select count(distinct k1) as c from ${tableName2}) t2 on\
				     (t1.c = t2.c) order by t1.c, t2.c"
    qt_distinct22 "select count(distinct k1) from ${tableName1} having count(k1)>60000"
    qt_distinct23 "select count(distinct k1) from ${tableName1} having count(k1)>70000"
    qt_distinct24 "select count(*), COUNT(distinct 1) from ${tableName1} where false"
    qt_distinct25 "select avg(distinct k1), avg(k1) from ${tableName1}"
    qt_distinct26 "select count(*) from (select count(distinct k1) from ${tableName1} group by k2) v \
		    order by count(*)"

    // as -> alias
    qt_alias1 "select * from ${tableName2} as a, ${tableName1} as b where a.k1 = b.k1 \
		    order by a.k1, a.k2, a.k3, a.k4, b.k1, b.k2, b.k3, b.k4"
    qt_alias2 "select k1 as k2 from ${tableName1} order by k1, k2, k3, k4"
    qt_alias3 "select date_format(b.k10, '%%Y%%m%%d') as k10 from ${tableName1} \
               a left join (select k10 from ${tableName2}) b \
               on a.k10 = b.k10 group by k10 order by k10"

    // having
    qt_having1 "select k1, k2 from ((select * from ${tableName2}) union all (select * from bigtable)) a \
             having k1 = 1 order by 1, 2"
    qt_having2 "select k1, k2 from ${tableName2} having k1 % 3 = 0 order by k1, k2"
    qt_having3 "select count(k1) b from ${tableName2} where k2 = 1989 having b >= 2 order by b"
    qt_having4 "select count(k1) b from ${tableName2} where k2 = 1989 having b > 2 order by b"
    qt_having5 "select k2, 0 as x from ${tableName2} group by k2 having k2 > 0 and x > 1 order by k2"
    qt_having6 "select k2, 0 as x from ${tableName2} group by k2 having k2 > 0 order by k2"
    qt_having7 "select k2, count(k1) b from ${tableName2} group by k2 having max(k1) > 2 order by k2"
    // PALO-2961
    qt_having8 "select a.k1, a.k2, a.k3, b.k2 from ${tableName2} a left join ${tableName2} b on a.k1 = b.k1 + 5 \
            having b.k2 < 0 order by a.k1"
    qt_having9 "select a.k1, a.k2, a.k3, b.k2 from ${tableName2} a left outer join ${tableName2} b on a.k1 = b.k1 + 5 \
            having b.k2 is not null order by a.k1"
    qt_having10 "select a.k1, a.k2, a.k3, b.k2 from ${tableName2} a join ${tableName2} b on a.k1 = b.k1 + 5 \
            having b.k2 < 0 order by a.k1"
    qt_having11 " select k2, count(*) from ${tableName2} group by k2 having k2 > 1000 order by k2"

    // as and derived
    qt_alias4 "select * from (select k1 from baseall) b order by 1"
    try_sql "select * from (select k1 from baseall) order by 1"
    qt_alias5 "select baseall.k1, t3.t from baseall, (select k2 as t from test where k2 = 1989) as t3 where \
            baseall.k1 > 0 and t3.t > 0 order by 1, 2"
    qt_alias6 "select baseall.k1, t3.k1 from baseall, (select k1 from test where k2 = 1989) as t3 where \
            baseall.k1 > 0 and t3.k1 > 0 order by 1, 2;"
    try_sql "SELECT a FROM (SELECT 1 FROM (SELECT 1) a HAVING a=1) b"
    try_sql "SELECT a,b as a FROM (SELECT '1' as a,'2' as b) b  HAVING a=1;"
    try_sql "SELECT a,2 as a FROM (SELECT '1' as a) b HAVING a=1;"
    try_sql "SELECT 1 FROM (SELECT 1) a WHERE a=2;"
    order_qt_alias7 "select * from baseall as x1, bigtable as x2;"
    qt_alias8 "select * from (select 1) as a;"
    qt_alias9 "select a from (select 1 as a) as b;"
    qt_alias10 "select 1 from (select 1) as a;"
    qt_alias11 "select * from (select * from baseall union select * from baseall) a order by k1;"
    qt_alias12 "select * from (select * from baseall union all select * from baseall) a order by k1;"
    qt_alias13 "select * from (select * from baseall union all \
            (select * from baseall order by k1 limit 2)) a order by k1"
    qt_alias14 "SELECT * FROM (SELECT k1 FROM test) as b ORDER BY k1  ASC LIMIT 0,20;"
    qt_alias15 "select * from (select 1 as a) b  left join (select 2 as a) c using(a);"
    try_sql "select 1 from  (select 2) a order by 0;"
    qt_alias16 "select * from (select k1 from test group by k1) bar order by k1;"
    qt_alias17 "SELECT a.x FROM (SELECT 1 AS x) AS a HAVING a.x = 1;"
    try_sql "select k1 as a, k2 as b, k3 as c from baseall t where a > 0;"
    qt_alias18 "select k1 as a, k2 as b, k3 as c from baseall t group by a, b, c order by a, b, c;"
    qt_alias19 "select k1 as a, k2 as b, k3 as c from baseall t group by a, b, c having a > 5 order by a, b, c;"
    qt_alias20 "select * from (select 1 as a) b  right join (select 2 as a) c using(a);"
    qt_alias21 "select * from (select 1 as a) b  full join (select 2 as a) c using(a) order by 1, 2;"
    try_sql "select k1 as k7, k2 as k8, k3 as k9 from baseall t group by k7, k8, k9 having k7 > 5 \
            order by k7;"
    try_sql "select k1 as k7, k2 as k8, k3 as k9 from baseall t where k8 > 0 group by k7, k8, k9 having k7 > 5 order by k7;"


    qt_distinct "select distinct upper(k6) from ${tableName1} order by upper(k6)"
    qt_distinct "select distinct * from ${tableName1} where k1<20 order by k1, k2, k3, k4"
    qt_having2 "select k1, k2 from ${tableName2} having k1 % 3 = 0 order by k1, k2"
    qt_distinct25 "select avg(distinct k1), avg(k1) from ${tableName1}"
    qt_distinct26 "select count(*) from (select count(distinct k1) from ${tableName1} group by k2) v \
		    order by count(*)"
}
