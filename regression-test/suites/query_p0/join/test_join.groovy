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

suite("test_join", "query,p0") {
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"
    sql 'set be_number_for_test=1'
    sql"use test_query_db"


    sql"drop view if exists test_join_empty_view"
    sql"create view test_join_empty_view as select * from baseall where k1 = 0"

    order_sql """select j.*, d.* from baseall j full outer join test d on (j.k1=d.k1) order by j.k1, j.k2, j.k3, j.k4, d.k1, d.k2
            limit 100"""
    order_sql """select * from (select j.k1 j1, j.k2 j2, j.k3 j3, j.k4 j4, j.k5 j5, j.k6 j6, j.k10 j10, j.k11 j11, 
             j.k7 j7, j.k8 j8, j.k9 j9, d.k1 d1, d.k2 d2, d.k3 d3, d.k4 d4, d.k5 d5, d.k6 d6, d.k10 d10, 
             d.k11 d11, d.k7 d7, d.k8 d8, d.k9 d9 from baseall j left join test d on (j.k1=d.k1) 
             union select j.k1 j1, j.k2 j2, j.k3 j3, j.k4 j4, j.k5 j5, j.k6 j6, j.k10 j10, j.k11 j11, 
             j.k7 j7, j.k8 j8, j.k9 j9, d.k1 d1, d.k2 d2, d.k3 d3, d.k4 d4, d.k5 d5, d.k6 d6, d.k10 d10, d.k11 d11, 
             d.k7 d7, d.k8 d8, d.k9 d9 from baseall j right join test d on (j.k1=d.k1) ) a order by j1, j2, j3, j4, d1, d2 
             limit 100"""
    qt_join1 """select sum(t1.k1), sum(t1.k3), max(t1.k5), max(t2.k4) from test t1 inner join baseall t2 on t1.k1 = t2.k1 and 
		    t1.k6 is not null and t2.k6 is not null"""
    qt_join2 """select k1, k2, k3 from test where k7 is not null order by 1 desc, 2 desc, 3 desc limit 10"""
    qt_join3 """select c.k1, c.k8 from baseall d join (select a.k1 as k1, a.k8 from test a join baseall b on (a.k1=b.k1)) c
		    on c.k1 = d.k1 order by 1, 2"""
    qt_join4 """select a.k1, b.k1 from baseall a join (select k1, k2 from test order by k1 limit 10) b 
		    on a.k2=b.k2 order by 1, 2"""
    qt_join5 """select a.k1, b.k2 from baseall as a join (select k1, k2 from test) as b
		    where a.k1 = b.k1 order by a.k1, b.k2"""
    qt_join6 """select A.k1,B.k1 from baseall as A join test as B where A.k1=B.k1+1 
		    order by A.k1, A.k2, A.k3, A.k4"""
    qt_join7 """select A.k1, B.k2 from baseall as A join test as B 
    		    order by A.k1, B.k2 limit 10"""
    qt_join8 """select A.k1 from baseall as A join test as B order by A.k1 limit 10"""
    qt_join9 """select a.k4 from test a inner join baseall b on (a.k1=b.k1) 
		    where a.k2>0 and b.k1=1 and a.k1=1 order by 1"""
    qt_join10 """select j.*, d.* from test j inner join baseall d on (j.k1=d.k1) 
		    order by j.k1, j.k2, j.k3, j.k4"""
    qt_join11 """select a.k1, b.k2, c.k3 
		    from test a join baseall b on (a.k1=b.k1) join test c on (a.k1 = c.k1)
		    where a.k2>0 and a.k1+50<0"""
    qt_join12 """select t1.k1, t2.k1 from test t1 join baseall t2 where (t1.k1<3 and t2.k1<3)
		    order by t1.k1, t2.k1 limit 100"""
    qt_join13 """select a.k1, b.k1, a.k2, b.k2 from
            (select k1, k2 from test where k9>0 and k6="false" union all
	        select k1, k2 from baseall where k6="true" union all
	        select 0, 0) a inner join
	        test b on a.k1=b.k1 and b.k1<5 order by 1, 2, 3, 4"""
    qt_join14 """select a.k1, b.k1, a.k2, b.k2 from
             test b left outer join
            (select k1, k2 from baseall where k9>0 and k6="false" union all
	        select k1, k2 from test where k6="true" union all
	        select 0, 0) a on a.k1=b.k1 where b.k1<5 and a.k1 is not NULL order by 1, 2, 3, 4"""
    qt_join15 """select a.k1, b.k1, a.k2, b.k2 from
            (select k1, k2 from test where k1=1 and lower(k6) like "%w%" union all
	        select k1, k2 from baseall where k1=2  union all
	        select 0, 1) a  join
            (select k1, k2 from baseall where k1=1 and lower(k6) like "%w%" union all
	        select k1, k2 from test where k1>0  union all
	        select 1, 2) b on a.k1 = b.k1 where b.k1<5 order by 1, 2, 3, 4"""
    qt_join16 """select count(*) from 
	        (select k1 from test union distinct
	        select k1 from baseall) a inner join
	        (select k2 from baseall union distinct
	        select k2 from test) b on a.k1+1000=b.k2 inner join
	        (select distinct k1 from test) c on a.k1=c.k1"""
    qt_join17 """select count(t1.k1) as wj from baseall t1 left join
		    baseall t2 on t1.k10=t2.k10 left join
		    baseall t3 on t2.k1 = t3.k1"""
    qt_join18 """select j.*, d.* from test j left join baseall d on (lower(j.k6) = lower(d.k6)) 
		    order by j.k1, j.k2, j.k3, j.k4, d.k2, d.k3, d.k4"""
    qt_join19 """select j.*, d.* from test j right join baseall d on (lower(j.k6) = lower(d.k6)) 
		    order by j.k1, j.k2, j.k3, j.k4, d.k2, d.k3, d.k4"""
    qt_join20 """select k1, v.k2 from baseall c, (select k2 from test order by k2 limit 2) v 
		    where k1 in (1, 2, 3) order by 1, 2"""
    qt_join21 """select k1, v.k2 from baseall c, (select k2 from test order by k2 limit 2) v 
    	    where k1 in (1, 2, 3) and v.k2%2=0 order by 1, 2"""
    qt_join22 """select k1, k2, cnt, avp from test c, (select count(k1) cnt, avg(k2) avp from baseall) v where k1 <3 
		    order by 1, 2, 3, 4"""
    qt_join23 """select k1, avg(maxp) from test c, (select max(k8) maxp from test group by k1) v where k1<3 group by k1
		    order by 1, 2"""
    qt_join24 """select k1, v.k3, cnt, avp from baseall c, (select count(k1) cnt, avg(k9) avp, k3 from baseall group by k3) v
		    where k1<0 order by 1, 2, 3, 4"""
    qt_join25 """select count(k5), k2 from test c, (select ca.k1 okey, cb.k2 opr from baseall ca, test cb where 
		    ca.k1=cb.k1 and ca.k2+cb.k2>2) v group by k2 order by 1, 2"""
    qt_join26 """select count(k6), k1 from test c, 
		    (select ca.k1 wj, ca.k2 opr from baseall ca left outer join test cb on ca.k1 = cb.k1) v
		    group by k1 order by 1, 2"""
    qt_join27 """select count(k6), k1 from test c, 
		    (select ca.k1 wj, ca.k2 opr from baseall ca right outer join test cb 
		    on ca.k1 = cb.k1 and ca.k2+cb.k2>2) v
		    group by k1 order by 1, 2"""
    // Ocurrs time out with specified time 299969 MILLISECONDS

    List selected =  ["a.k1, b.k1, a.k2, b.k2, a.k3, b.k3", "count(a.k1), count(b.k1), count(a.k2), count(b.k2), count(*)"]

    for( i in selected) {
        qt_join28"""select ${i} from test a join baseall b 
                on a.k1 = b.k1 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join29"""select ${i} from test a join baseall b 
                on a.k1 > b.k1 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join30"""select ${i} from test a join baseall b 
                on a.k1 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join31"""select ${i} from test a join baseall b 
                on a.k1 = b.k1 and a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join32"""select ${i} from test a join baseall b 
                on a.k1 = b.k1 and a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join33"""select ${i} from test a join baseall b 
                order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join34"""select ${i} from test a join baseall b 
                on a.k1 = b.k1 or a.k2 = b.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join35"""select ${i} from test a join baseall b 
                on a.k1 < b.k1 or a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join36"""select ${i} from test a join baseall b 
                on a.k1 = b.k1 or a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join37"""select ${i} from test a join baseall b 
                on a.k1 = b.k1 or a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join38"""select ${i} from test a join baseall b 
                on a.k1 < b.k1 or a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join39"""select ${i} from test a join baseall b on a.k1 = b.k1 
                join bigtable c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join40"""select ${i} from test a join baseall b on a.k2 = b.k2 and a.k1 > 0 
                join bigtable c on a.k3 = c.k3 and b.k1 = c.k1 + 1 and c.k3 > 0 
                order by 1, 2, 3, 4, 5 limit 65535"""


        // test_inner_join
        qt_inner_join1"""select ${i} from test a inner join baseall b 
                on a.k1 = b.k1 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_inner_join2"""select ${i} from test a inner join baseall b 
                on a.k1 > b.k1 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_inner_join3"""select ${i} from test a inner join baseall b 
                on a.k1 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_inner_join4"""select ${i} from test a inner join baseall b 
                on a.k1 = b.k1 and a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_inner_join5"""select ${i} from test a inner join baseall b 
                on a.k1 = b.k1 and a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_inner_join6"""select ${i} from test a inner join baseall b 
                order by 1, 2, 3, 4, 5 limit 65535"""
        qt_inner_join7"""select ${i} from test a inner join baseall b 
                on a.k1 = b.k1 or a.k2 = b.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_inner_join8"""select ${i} from test a inner join baseall b 
                on a.k1 < b.k1 or a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_inner_join9"""select ${i} from test a inner join baseall b 
                on a.k1 = b.k1 or a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_inner_join10"""select ${i} from test a inner join baseall b 
                on a.k1 = b.k1 or a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_inner_join11"""select ${i} from test a inner join baseall b 
                on a.k1 < b.k1 or a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_inner_join12"""select ${i} from test a inner join baseall b on a.k1 = b.k1 
                inner join bigtable c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_inner_join13"""select ${i} from test a inner join baseall b on a.k2 = b.k2 and a.k1 > 0 
                inner join bigtable c on a.k3 = c.k3 and b.k1 = c.k1 + 1 and c.k3 > 0 
                order by 1, 2, 3, 4, 5 limit 65535"""
    }

    // test_left_join
    String i = "a.k1, b.k1, a.k2, b.k2, a.k3, b.k3"
    qt_left_join1"""select ${i} from test a left join baseall b 
            on a.k1 = b.k1 and a.k2 > 0 order by 1, isnull(b.k1), 2, 3, 4, 5 limit 65535"""
    qt_left_join2"""select ${i} from test a left join baseall b 
            on a.k1 = b.k1 and a.k2 > b.k2 order by 1, isnull(b.k1), 2, 3, 4, 5 limit 65535"""
    qt_left_join3"""select ${i} from test a left join baseall b on a.k2 = b.k2 and a.k1 > 0 
            left join bigtable c on a.k3 = c.k3 and b.k1 = c.k1 + 1 and c.k3 > 0 
            order by 1, isnull(b.k1), 2, 3, 4, 5 limit 65535"""

    for (s in selected) {
        qt_left_join4"""select ${s} from test a left join baseall b 
                    on a.k1 = b.k1 order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a left join baseall b
                on a.k1 > b.k1 order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a left join baseall b
                on a.k1 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        test {
            sql"""select ${s} from test a left join baseall b 
               order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
            sql"""select ${s} from test a left join baseall b
               on a.k1 = b.k1 or a.k2 = b.k2 order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a left join baseall b
               on a.k1 < b.k1 or a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a left join baseall b
               on a.k1 = b.k1 or a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a left join baseall b
               on a.k1 = b.k1 or a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a left join baseall b
                    on a.k1 < b.k1 or a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_left_join5"""select ${s} from test a left join baseall b on a.k1 = b.k1
                left join bigtable c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535"""
    }

    // left_outer_join
    qt_left_outer_join1"""select ${i} from test a left outer join baseall b 
            on a.k1 = b.k1 and a.k2 > 0 order by 1, isnull(b.k1), 2, 3, 4, 5 limit 65535"""
    qt_left_outer_join2"""select ${i} from test a left outer join baseall b 
            on a.k1 = b.k1 and a.k2 > b.k2 order by 1, isnull(b.k1), 2, 3, 4, 5 limit 65535"""
    qt_left_outer_join3"""select ${i} from test a left outer join baseall b on a.k2 = b.k2 and a.k1 > 0 
            left join bigtable c on a.k3 = c.k3 and b.k1 = c.k1 + 1 and c.k3 > 0 
            order by 1, isnull(b.k1), 2, 3, 4, 5 limit 65535"""

    for (s in selected) {
        qt_left_outer_join4"""select ${s} from test a left outer join baseall b 
                on a.k1 = b.k1 order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a left outer join baseall b
                on a.k1 > b.k1 order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a left outer join baseall b
                on a.k1 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        test {
            sql"""select ${s} from test a left outer join baseall b 
               where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
            sql"""select ${s} from test a left outer join baseall b
                    on a.k1 = b.k1 or a.k2 = b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a left outer join baseall b
                    on a.k1 < b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a left outer join baseall b
                    on a.k1 = b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a left outer join baseall b
                    on a.k1 = b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a left outer join baseall b
                    on a.k1 < b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_left_outer_join5"""select ${s} from test a left outer join baseall b on a.k1 = b.k1
                left outer join bigtable c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535"""

    }

    // right join
    qt_right_join1"""select ${i} from test a right join baseall b 
               on a.k1 = b.k1 and a.k2 > b.k2 order by isnull(a.k1), 1, 2, 3, 4, 5 limit 65535"""

    for (s in selected) {
        qt_right_join2"""select ${s} from test a right join baseall b 
               on a.k1 = b.k1 order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a right join baseall b
               on a.k1 > b.k1 order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a right join baseall b
                    on a.k1 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_right_join3"""select ${s} from test a right join baseall b
                on a.k1 = b.k1 and a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        test {
            sql"""select ${s} from test a right join baseall b 
               where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                logger.info(exception.message)
                assertTrue(exception != null)
            }
        }
            sql"""select ${s} from test a right join baseall b
                    on a.k1 = b.k1 or a.k2 = b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a right join baseall b
                    on a.k1 < b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a right join baseall b
                    on a.k1 = b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a right join baseall b
                    on a.k1 = b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a right join baseall b
                    on a.k1 < b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_right_join4"""select ${s} from test a right join baseall b on a.k1 = b.k1
                right join bigtable c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535"""
    }
    qt_right_join5"""select ${i} from test a right join baseall b on a.k2 = b.k2 and a.k1 > 0 
            right join bigtable c on a.k3 = c.k3 and b.k1 = c.k1 + 1 and c.k3 > 0 
            order by isnull(a.k1), 1, 2, 3, 4, 5 limit 65535"""

    // right outer join
    qt_right_outer_join1"""select ${i} from test a right outer join baseall b 
               on a.k1 = b.k1 and a.k2 > b.k2 order by isnull(a.k1), 1, 2, 3, 4, 5 limit 65535"""

    for (s in selected) {
        qt_right_outer_join2"""select ${s} from test a right  outer join baseall b 
               on a.k1 = b.k1 order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a right  outer join baseall b
                    on a.k1 > b.k1 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a right  outer join baseall b
                    on a.k1 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_right_outer_join3"""select ${s} from test a right  outer join baseall b
               on a.k1 = b.k1 and a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        test {
            sql"""select ${s} from test a right  outer join baseall b 
               where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
            sql"""select ${s} from test a right  outer join baseall b
                    on a.k1 = b.k1 or a.k2 = b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a right  outer join baseall b
                    on a.k1 < b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a right  outer join baseall b
                    on a.k1 = b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a right  outer join baseall b
                    on a.k1 = b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a right  outer join baseall b
                    on a.k1 < b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_right_outer_join4"""select ${s} from test a right outer join baseall b on a.k1 = b.k1
                right outer join bigtable c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535"""

    }
    qt_right_outer_join5"""select ${i} from test a right outer join baseall b on a.k2 = b.k2 and a.k1 > 0 
            right outer join bigtable c on a.k3 = c.k3 and b.k1 = c.k1 + 1 and c.k3 > 0 
            order by isnull(a.k1), 1, 2, 3, 4, 5 limit 65535"""

    // right outer join with other join predicates
    qt_right_outer_join_wih_other_pred """
        select a.k2, b.k2, c.k2 from test a left join test b on a.k2 = b.k2 right join baseall c on b.k2 = c.k1 and 1 = 2 order by 1, 2, 3;
    """

    // full outer join
    for (s in selected) {
        sql"""select ${s} from test a full outer join baseall b on a.k1 = b.k1 
                 order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from test a left outer join baseall b on a.k1 = b.k1 
                 order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a full outer join baseall b on a.k1 > b.k1
                where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a full outer join baseall b on a.k1 > 0
                order by 1, 2, 3, 4, 5 limit 65535"""
        test {
            sql"""select ${s} from test a full outer join baseall b 
                where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
            sql"""select ${s} from test a full outer join baseall b
                on a.k1 = b.k1 or a.k2 = b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a full outer join baseall b
                on a.k1 < b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a full outer join baseall b
                on a.k1 = b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a full outer join baseall b
                on a.k1 = b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from test a full outer join baseall b
                on a.k1 < b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from test a full outer join baseall b on a.k1 = b.k1
                full outer join bigtable c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from test a left outer join baseall b on a.k1 = b.k1 
                 left outer join bigtable c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535"""
    }
    sql"""select a.k1 k1, a.k2, a.k3, b.k1, b.k2, b.k3 from test a full outer join baseall b 
             on a.k1 = b.k1 and a.k2 > b.k2 order by isnull(k1), 1, 2, 3, 4, 5 limit 65535"""
    sql"""select a.k1 ak1, a.k2 ak2, a.k3 ak3, b.k1 bk1, b.k2 bk2, b.k3 bk3 from test a left outer join baseall b 
             on a.k1 = b.k1 and a.k2 > b.k2 union (select a.k1 ak1, a.k2 ak2, a.k3 ak3, b.k1 bk1, b.k2 bk2, b.k3 bk3 
             from test a right outer join baseall b on a.k1 = b.k1 and a.k2 > b.k2) 
             order by isnull(ak1), 1, 2, 3, 4, 5 limit 65535"""
    sql"""select count(*) from test a full outer join baseall b on a.k2 = b.k2 and a.k1 > 0 
            full outer join bigtable c on a.k3 = c.k3 and b.k1 = c.k1 and c.k3 > 0"""
    sql"""select count(*) from ((select a.k1 as k1, b.k1 as k2, a.k2 as k3, b.k2 as k4, a.k3 as k5, b.k3 as k6, c.k1 as k7, c.k2 as k8, c.k3 as k9 from test a 
            left outer join baseall b on a.k2 = b.k2 and a.k1 > 0 
            left outer join bigtable c on a.k3 = c.k3 and b.k1 = c.k1 and c.k3 > 0) union 
            (select a.k1, b.k1, a.k2, b.k2, a.k3, b.k3, c.k1, c.k2, c.k3 from test a 
            left outer join baseall b on a.k2 = b.k2 and a.k1 > 0 
            right outer join bigtable c on a.k3 = c.k3 and b.k1 = c.k1 and c.k3 > 0) union 
            (select a.k1, b.k1, a.k2, b.k2, a.k3, b.k3, c.k1, c.k2, c.k3 from test a 
            right outer join baseall b on a.k2 = b.k2 and a.k1 > 0 
            left outer join bigtable c on a.k3 = c.k3 and b.k1 = c.k1 and c.k3 > 0) 
            union (select a.k1, b.k1, a.k2, b.k2, a.k3, b.k3, c.k1, c.k2, c.k3 from test a 
            right outer join baseall b on a.k2 = b.k2 and a.k1 > 0 
            right outer join bigtable c on a.k3 = c.k3 and b.k1 = c.k1 and c.k3 > 0))a"""
    sql"""select ${i} from test a full outer join baseall b on a.k1 = b.k1 
             and a.k2 > 0 order by 1, isnull(b.k1), 2, 3, 4, 5  limit 65535"""
    sql"""select ${i} from test a left outer join baseall b on a.k1 = b.k1 and a.k2 > 0 
            order by 1, isnull(b.k1), 2, 3, 4, 5 limit 65535"""

    // cross join
    for (s in selected){
        test {
            sql"""select ${s} from test a cross join baseall b 
                    on a.k1 = b.k1 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        test {
            sql"""select ${s} from test a cross join baseall b 
                    on a.k1 > b.k1 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        test {
            sql"""select ${s} from test a cross join baseall b 
                    on a.k1 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        test {
            sql"""select ${s} from test a cross join baseall b 
                    on a.k1 = b.k1 and a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        test {
            sql"""select ${s} from test a cross join baseall b 
                    on a.k1 = b.k1 and a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        qt_cross_join1"""select ${s} from test a cross join baseall b 
               where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535"""
        test {
            sql"""select ${s} from test a cross join baseall b 
                    on a.k1 = b.k1 or a.k2 = b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        test {
            sql"""select ${s} from test a cross join baseall b 
                    on a.k1 < b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        test {
            sql"""select ${s} from test a cross join baseall b 
                    on a.k1 = b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        test {
            sql"""select ${s} from test a cross join baseall b 
                    on a.k1 = b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        test {
            sql"""select ${s} from test a cross join baseall b 
                    on a.k1 < b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        test {
            sql"""select ${s} from test a cross join baseall b on a.k1 = b.k1 
                cross join bigtable c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        test {
            sql"""select ${s} from test a cross join baseall b on a.k2 = b.k2 and a.k1 > 0 
                cross join bigtable c on a.k3 = c.k3 and b.k1 = c.k1 + 1 and c.k3 > 0 
                order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
    }


    // left_semi_join
    List left_selected = ["a.k1, a.k2, a.k3, a.k4, a.k5", "count(a.k1), count(a.k2), count(a.k4), count(a.k3), count(*)"]
    for (s in left_selected){
        sql"""select ${s} from test a left semi join baseall b on a.k1 = b.k1 
                 order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from test a left outer join baseall b on a.k1 = b.k1 
                 where b.k3 is not null order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from test a left semi join baseall b on a.k1 > b.k1
            where a.k2 > 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from test a left semi join baseall b on a.k1 > 0
            where a.k2 > 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535"""
        def res15 = sql"""select ${s} from test a left semi join baseall b
                on a.k1 = b.k1 and a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        def res16 = sql"""select ${s} from test a left outer join baseall b 
                on a.k1 = b.k1 and a.k2 > 0 where b.k3 is not null 
                order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res15, res16)
        def res17 = sql"""select ${s} from test a left semi join baseall b 
                    on a.k1 = b.k1 and a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        def res18 = sql"""select ${s} from test a left outer join baseall b 
                 on a.k1 = b.k1 and a.k2 > b.k2 where b.k3 is not null 
                 order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res17, res18)
        test {
            sql"""select ${s} from test a left semi join baseall b 
                where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        sql"""select ${s} from test a left semi join baseall b
                on a.k1 = b.k1 or a.k2 = b.k2 where a.k2 > 0 and a.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from test a left semi join baseall b
                on a.k1 < b.k1 or a.k2 > b.k2 where a.k2 > 0 and a.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from test a left semi join baseall b
            on a.k1 = b.k1 or a.k2 > b.k2 where a.k2 > 0 and a.k6 > "000"
            order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from test a left semi join baseall b
            on a.k1 = b.k1 or a.k2 > 0 where a.k2 > 0 and a.k6 > "000"
            order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from test a left semi join baseall b
            on a.k1 < b.k1 or a.k2 > 0 where a.k2 > 0 and a.k6 > "000"
            order by 1, 2, 3, 4, 5 limit 65535"""
        def res19 = sql"""select ${s} from test a left semi join baseall b on a.k1 = b.k1
                left semi join bigtable c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        def res20 = sql"""select ${s} from (select distinct a.* from test a left outer join baseall b on a.k1 = b.k1 
                left outer join bigtable c on a.k2 = c.k2 where a.k1 is not null 
                and b.k1 is not null and c.k1 is not null) a order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res19, res20)
        def res21 = sql"""select ${s} from test a left semi join baseall b on a.k2 = b.k2 and a.k1 > 0 
                left semi join bigtable c on a.k3 = c.k3 and a.k1 = c.k1 + 1 and c.k3 > 0 
                order by 1, 2, 3, 4, 5 limit 65535"""
        def res22 = sql"""select ${s} from (select distinct a.* from test a left outer join baseall b on a.k2 = b.k2 and a.k1 > 0 
                left outer join bigtable c on a.k3 = c.k3 and a.k1 = c.k1 + 1 and c.k3 > 0 
                where a.k1 is not null and b.k1 is not null and c.k1 is not null and a.k1 > 0 and c.k3 > 0) a
                order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res21, res22)
    }

    qt_cross_join2 """
        select t1.k1 from test t1 cross join test t2 where t1.k1 = t2.k1 + 1 group by t1.k1 order by t1.k1;
    """

    // right semi join
    List right_selected = ["b.k1, b.k2, b.k3, b.k4, b.k5", "count(b.k1), count(b.k2), count(b.k4), count(b.k3), count(*)"]
    for (s in right_selected){
        def res23 = sql"""select ${s} from test a right semi join test b 
                on a.k1 = b.k1 order by 1, 2, 3, 4, 5 limit 65535"""
        def res24 = sql"""select ${s} from test a right outer join test b 
                on a.k1 = b.k1 where a.k2 is not null order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res23, res24)
        sql"""select ${s} from test a right semi join baseall b
                on a.k1 > b.k1 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from test a right semi join baseall b
                on a.k1 > 0 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        def res25 = sql"""select ${s} from baseall a right semi join test b
                 on a.k1 = b.k1 and a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        def res26 = sql"""select ${s} from baseall a right outer join test b on a.k1 = b.k1 and 
                 a.k2 > 0 where a.k2 is not null order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res25, res26)
        def res27 = sql"""select ${s} from baseall a right semi join test b 
                    on a.k1 = b.k1 and a.k2 > b.k2  order by 1, 2, 3, 4, 5 limit 65535"""
        def res28 = sql"""select ${s} from baseall a right outer join test b 
                    on a.k1 = b.k1 and a.k2 > b.k2 where a.k2 is not null 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res27, res28)
        test {
            sql"""select ${s} from test a right semi join baseall b 
               where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        sql"""select ${s} from test a right semi join baseall b
                on a.k1 = b.k1 or a.k2 = b.k2 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from test a right semi join baseall b
                on a.k1 < b.k1 or a.k2 > b.k2 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from test a right semi join baseall b
                on a.k1 = b.k1 or a.k2 > b.k2 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from test a right semi join baseall b
                on a.k1 = b.k1 or a.k2 > 0 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from test a right semi join baseall b
                on a.k1 < b.k1 or a.k2 > 0 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        def res29 = sql"""select ${s} from bigtable a right semi join test c on a.k1 = c.k1
                right semi join baseall b on b.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535"""

        def res30 = sql"""select ${s} from (select distinct b.* from bigtable a right outer join test c on a.k1 = c.k1 
                right outer join baseall b on b.k2 = c.k2 where a.k1 is not null 
                and b.k1 is not null and c.k1 is not null) b order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res29, res30)
        def res31 = sql"""select ${s} from baseall c right semi join test a on c.k2 = a.k2 and c.k1 > 0 
                right semi join bigtable b on a.k3 = b.k3 and b.k1 = a.k1 + 1 and a.k3 > 0 
                order by 1, 2, 3, 4, 5 limit 65535"""
        def res32 = sql"""select ${s} from (select distinct a.* from baseall c right outer join test b1 on c.k2 = b1.k2 and c.k1 > 0 
                right outer join bigtable a on c.k3 = a.k3 and a.k1 = c.k1 + 1 and a.k3 > 0 
                where a.k1 is not null and b1.k1 is not null and a.k1 is not null and a.k1 > 0 and c.k3 > 0) b
                order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res31, res32)
    }

    // left anti join
    for (s in left_selected){
        def res33 = sql"""select ${s} from test a left anti join baseall b 
                    on a.k1 = b.k1  order by 1, 2, 3, 4, 5 limit 65535"""
        def res34 = sql"""select ${s} from test a left outer join baseall b 
                    on a.k1 = b.k1 where b.k3 is null order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res33, res34)
        sql"""select ${s} from test a left anti join baseall b
                on a.k1 > b.k1 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from test a left anti join baseall b
                on a.k1 > 0 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        def res35 = sql"""select ${s} from test a left anti join baseall b
                    on a.k1 = b.k1 and a.k2 > 0 order by 1, 2, 3, 4, 5 limit 50000"""
        def res36 = sql"""select ${s} from test a left outer join baseall b 
                    on a.k1 = b.k1 and a.k2 > 0 where b.k3 is null 
                    order by 1, 2, 3, 4, 5 limit 50000"""
        check2_doris(res35, res36)
        def res37 = sql"""select ${s} from test a left anti join baseall b 
                 on a.k1 = b.k1 and a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        def res38 = sql"""select ${s} from test a left outer join baseall b 
                 on a.k1 = b.k1 and a.k2 > b.k2 where b.k3 is null 
                 order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res37, res38)
        test {
            sql"""select ${s} from test a left anti join baseall b
               where a.k2 > 0 and a.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        sql"""select ${s} from test a left anti join baseall b
                on a.k1 = b.k1 or a.k2 = b.k2 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from test a left anti join baseall b
                on a.k1 < b.k1 or a.k2 > b.k2 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from test a left anti join baseall b
                on a.k1 = b.k1 or a.k2 > b.k2 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from test a left anti join baseall b
                on a.k1 = b.k1 or a.k2 > 0 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from test a left anti join baseall b
                on a.k1 < b.k1 or a.k2 > 0 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        def res39 = sql"""select ${s} from test a left anti join baseall b on a.k1 = b.k1
                left anti join bigtable c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        def res40 = sql"""select ${s} from test a left outer join baseall b on a.k1 = b.k1 
                left outer join bigtable c on a.k2 = c.k2 where 
                b.k1 is null and c.k1 is null order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res39, res40)
        def res41 = sql"""select ${s} from test a left anti join baseall b on a.k2 = b.k2 and a.k1 > 0 
                left anti join bigtable c on a.k3 = c.k3 and a.k1 = c.k1 + 1 and c.k3 > 0 
                order by 1, 2, 3, 4, 5 limit 65535"""
        def res42 = sql"""select ${s} from (select distinct a.* from test a left outer join baseall b on a.k2 = b.k2 and a.k1 > 0 
                left outer join bigtable c on a.k3 = c.k3 and a.k1 = c.k1 + 1 and c.k3 > 0 
                where b.k1 is null and c.k1 is null) a
                order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res41, res42)
    }

    qt_left_anti_join_with_other_pred "select b.k1 from baseall b left anti join test t on b.k1 = t.k1 and 1 = 2 order by b.k1"

    qt_left_anti_join_null_1 "select b.k1 from baseall b left anti join test t on b.k1 = t.k1 order by b.k1"

    qt_left_anti_join_null_2 "select b.k1 from baseall b left anti join test_join_empty_view t on b.k1 = t.k1 order by b.k1"

    qt_left_anti_join_null_3 "select b.k1 from baseall b left anti join test t on b.k1 > t.k2 order by b.k1"

    qt_left_anti_join_null_4 "select b.k1 from baseall b left anti join test_join_empty_view t on b.k1 > t.k2 order by b.k1"

    // right anti join
    for (s in right_selected){
        def res43 = sql"""select ${s} from baseall a right anti join test b 
                    on a.k1 = b.k1 order by 1, 2, 3, 4, 5 limit 65535"""
        def res44 = sql"""select ${s} from baseall a right outer join test b 
                on a.k1 = b.k1 where a.k2 is null 
                order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res43, res44)
        sql"""select ${s} from test a right anti join baseall b
                on a.k1 > b.k1 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from test a right anti join baseall b
                on a.k1 > 0 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        def res45 = sql"""select ${s} from baseall a right anti join test b
                    on a.k1 = b.k1 and a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        def res46 = sql"""select ${s} from baseall a right outer join test b 
                    on a.k1 = b.k1 and a.k2 > 0 where a.k2 is null 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res45 , res46)
        def res47 = sql"""select ${s} from baseall a right anti join test b 
                    on a.k1 = b.k1 and a.k2 > b.k2  order by 1, 2, 3, 4, 5 limit 65535"""
        def res48 = sql"""select ${s} from baseall a right outer join test b 
                    on a.k1 = b.k1 and a.k2 > b.k2 where a.k2 is null 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res47, res48)
        test {
            sql"""select ${s} from test a right anti join baseall b 
               where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        sql"""select ${s} from test a right anti join baseall b
                on a.k1 = b.k1 or a.k2 = b.k2 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from test a right anti join baseall b
                on a.k1 < b.k1 or a.k2 > b.k2 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from test a right anti join baseall b
                on a.k1 = b.k1 or a.k2 > b.k2 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from test a right anti join baseall b
                on a.k1 = b.k1 or a.k2 > 0 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from test a right anti join baseall b
                on a.k1 < b.k1 or a.k2 > 0 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from test a right anti join baseall c on a.k1 = c.k1
                right anti join bigtable b on c.k2 = b.k2 order by 1, 2, 3, 4, 5 limit 65535"""

        sql"""select ${s} from (select distinct b.k1, b.k2, b.k3, b.k4, b.k5 from 
                test a right outer join baseall c on a.k1 = c.k1 right outer join 
                bigtable b on c.k2=b.k2) b order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from test a right anti join baseall c on a.k2 = c.k2 and a.k1 > 0 
                right anti join bigtable b on c.k3 = b.k3 and b.k1 = c.k1 + 1 and c.k3 > 0 
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from (select distinct c.* from test a right outer join baseall b on a.k2 = b.k2 and a.k1 > 0 
                right outer join bigtable c on b.k3 = c.k3 and c.k1 = b.k1 + 1 and c.k3 > 0 
                where b.k1 is null and a.k1 is null and a.k1 > 0) b
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from (select distinct c.k1 k1, c.k2 k2, c.k3 k3, c.k4 k4, c.k5 k5 from 
                (select b2.* from test a right outer join baseall b2 on a.k2 = b2.k2 and a.k1 > 0 
                where a.k1 is null and a.k1 > 0) b1 right outer join bigtable c 
                on b1.k3 = c.k3 and c.k1 = b1.k1 + 1 and c.k3 > 0 where b1.k1 is null) b 
                order by 1, 2, 3, 4, 5 limit 65535"""
    }

    qt_right_anti_join_with_other_pred "select t.k1 from baseall b right anti join test t on b.k1 = t.k1 and 1 = 2 order by t.k1"

    qt_right_anti_join_null_1 "select b.k1 from test t right anti join baseall b on b.k1 > t.k1 order by b.k1"

    qt_right_anti_join_null_2 "select /*+SET_VAR(batch_size=3) */ b.k1 from test_join_empty_view t right anti join baseall b on b.k1 > t.k1 order by b.k1"

    // join with no join keyword
    for (s in selected){
        qt_join_without_keyword1"""select ${s} from test a , baseall b 
                    where a.k1 = b.k1 and a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join_without_keyword2"""select ${s} from test a , baseall b 
                    where a.k1 > b.k1 and a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join_without_keyword3"""select ${s} from test a , baseall b 
                    where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join_without_keyword4"""select ${s} from test a , baseall b 
                    where a.k1 = b.k1 and a.k2 > 0 and a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join_without_keyword5"""select ${s} from test a , baseall b 
                    where a.k1 = b.k1 and a.k2 > b.k2 and a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join_without_keyword6"""select ${s} from test a , baseall b 
               where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join_without_keyword7"""select ${s} from test a , baseall b 
                    where (a.k1 = b.k1 or a.k2 = b.k2) and a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join_without_keyword8"""select ${s} from test a , baseall b 
                    where (a.k1 < b.k1 or a.k2 > b.k2) and a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join_without_keyword9"""select ${s} from test a , baseall b 
                    where (a.k1 = b.k1 or a.k2 > b.k2) and a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join_without_keyword10"""select ${s} from test a , baseall b 
                    where (a.k1 = b.k1 or a.k2 > 0) and a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join_without_keyword11"""select ${s} from test a , baseall b 
                    where (a.k1 < b.k1 or a.k2 > 0) and a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join_without_keyword12"""select ${s} from test a, baseall b, bigtable c where a.k1 = b.k1 
                and a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join_without_keyword13"""select ${s} from test a, baseall b, bigtable c where a.k2 = b.k2 and a.k1 > 0 
                and a.k3 = c.k3 and b.k1 = c.k1 + 1 and c.k3 > 0 
                order by 1, 2, 3, 4, 5 limit 65535"""
    }

    // join with empty table
    qt_join_with_emptyTable1"""select a.k1, a.k2, a.k3, b.k1, b.k2, b.k3 from baseall a join test_join_empty_view b on a.k1 = b.k1 
            order by 1, 2, 3, 4, 5"""
    qt_join_with_emptyTable2"""select a.k1, a.k2, a.k3, b.k1, b.k2, b.k3 from baseall a inner join test_join_empty_view b on a.k1 = b.k1 
            order by 1, 2, 3, 4, 5"""
    qt_join_with_emptyTable3"""select a.k1, a.k2, a.k3, b.k1, b.k2, b.k3 from baseall a left join test_join_empty_view b on a.k1 = b.k1 
            order by 1, 2, 3, 4, 5"""
    qt_join_with_emptyTable4"""select a.k1, a.k2, a.k3, b.k1, b.k2, b.k3 from baseall a right join test_join_empty_view b on a.k1 = b.k1 
            order by 1, 2, 3, 4, 5"""
    def res53 = sql"""select a.k1, a.k2, a.k3, b.k1, b.k2, b.k3 from baseall a full outer join test_join_empty_view b on a.k1 = b.k1 
            order by 1, 2, 3, 4, 5"""
    def res54 = sql"""select a.k1, a.k2, a.k3, b.k1, b.k2, b.k3 from baseall a left join test_join_empty_view b on a.k1 = b.k1 
            order by 1, 2, 3, 4, 5"""
    check2_doris(res53, res54)
    // qt_join_with_emptyTable5"""select a.k1, a.k2, a.k3, b.k1, b.k2, b.k3 from baseall a cross join test_join_empty_view b on a.k1 = b.k1
    //         order by 1, 2, 3, 4, 5"""
    test {
        sql"""select a.k1, a.k2, a.k3 from baseall a left semi join test_join_empty_view b on a.k1 = b.k1 
            order by 1, 2, 3"""
        check{result, exception, startTime, endTime ->
            logger.info(result.toString())
            assertTrue(result.isEmpty())
        }
    }
    test {
        sql"""select b.k1, b.k2, b.k3 from baseall a right semi join test_join_empty_view b on a.k1 = b.k1 
            order by 1, 2, 3"""
        check{result, exception, startTime, endTime ->
            assertTrue(result.isEmpty())
        }
    }
    def res55 = sql"""select a.k1, a.k2, a.k3 from baseall a left anti join test_join_empty_view b on a.k1 = b.k1 
            order by 1, 2, 3"""
    def res56 = sql"""select k1, k2, k3 from baseall order by 1, 2, 3"""
    check2_doris(res55, res56)
    test {
        sql"""select b.k1, b.k2, b.k3 from baseall a right anti join test_join_empty_view b on a.k1 = b.k1 
            order by 1, 2, 3"""
        check{result, exception, startTime, endTime ->
            assertTrue(result.isEmpty())
        }
    }



    // cases for bug
    def res57 = sql"""select count(b.k1), count(b.k2), count(b.k4), count(*) from baseall a 
           right semi join test b on a.k1 = b.k1 and a.k2 > b.k2 order by 1, 2, 3, 4 limit 65535"""
    def res58 = sql"""select count(b.k1), count(b.k2), count(b.k4), count(*) from baseall a 
           right semi join test b on a.k1 = b.k1 and a.k2 < b.k2 order by 1, 2, 3, 4 limit 65535"""
    assertTrue(res57 == res58)


    def res59 = sql"""select count(b.k1), count(b.k2), count(b.k4), count(*) from baseall a 
           right semi join test b on a.k1 = b.k1 order by 1, 2, 3, 4 limit 65535"""
    def res60 = sql"""select count(b.k1), count(b.k2), count(b.k4), count(*) from baseall a 
          right semi join test b on a.k1 = b.k1 order by 1, 2, 3, 4 limit 65535"""
    for (j in range(0, 100)) {
        def res61 = sql"""select count(b.k1), count(b.k2), count(b.k4), count(*) from baseall a 
                   right semi join test b on a.k1 = b.k1 order by 1, 2, 3, 4 limit 65535"""
        def res62 = sql"""select count(b.k1), count(b.k2), count(b.k4), count(*) from baseall a 
                  right semi join test b on a.k1 = b.k1 order by 1, 2, 3, 4 limit 65535"""
        check2_doris(res61, res59)
        check2_doris(res62, res60)
    }


    def res63 = sql"""select count(*) from test a full outer join baseall b on a.k2 = b.k2 and a.k1 > 0 
            full outer join bigtable c on a.k3 = c.k3 and b.k1 = c.k1 and a.k3 > 0 
            order by 1 limit 65535"""
    def res64 = sql"""select count(*) from test a full outer join baseall b on a.k2 = b.k2 and a.k1 > 0 
           full outer join bigtable c on a.k3 = c.k3 and b.k1 = c.k1 and c.k3 > 0 
           order by 1 limit 65535"""
    check2_doris(res63, res64)

    sql"drop view if exists nullable"
    sql"""create view nullable(n1, n2) as select a.k1, b.k2 from baseall 
            a left join bigtable b on a.k1 = b.k1 + 10 where b.k2 is null"""
    qt_join_bug1"""select k1, n1 from baseall a right outer join nullable b on a.k1 % 2 = b.n1 % 2 
           order by a.k1, b.n1"""
    qt_join_bug2"""select n.k1, m.k1, m.k2, n.k2 from (select a.k1, a.k2, a.k3 from 
           baseall a join baseall b on (a.k1 = b.k1 and a.k2 = b.k2 and a.k3 = b.k3)) m 
           left join test n on m.k1 = n.k1 order by 1, 2, 3, 4"""
    // https://github.com/apache/doris/issues/4210
    qt_join_bug3"""select * from baseall t1 where k1 = (select min(k1) from test t2 where t2.k1 = t1.k1 and t2.k2=t1.k2)
           order by k1"""
    qt_join_bug4"""select b.k1 from baseall b where b.k1 not in( select k1 from baseall where k1 is not null )"""


    // basic join
    List columns = ["k1", "k2", "k3", "k4", "k5", "k6", "k10", "k11"]
    List join_types = ["inner", "left outer", "right outer", ""]
    for (type in join_types) {
        for (c in columns) {
            qt_join_basic1"""select * from baseall a ${type} join test b on (a.${c} = b.${c}) 
                   order by isnull(a.k1), a.k1, a.k2, a.k3, isnull(b.k1), b.k1, b.k2, b.k3 
                   limit 60015"""
        }
    }
    for (c in columns){
        sql"""select * from baseall a full outer join test b on (a.${c} = b.${c}) 
                order by isnull(a.k1), a.k1, a.k2, a.k3, a.k4, isnull(b.k1), b.k1, b.k2, b.k3, 
                b.k4 limit 65535"""
        sql"""select a.k1 ak1, a.k2 ak2, a.k3 ak3, a.k4 ak4, a.k5 ak5, a.k6 ak7, a.k10 ak10, a.k11 ak11, 
                 a.k7 ak7, a.k8 ak8, a.k9 ak9, b.k1 bk1, b.k2 bk2, b.k3 bk3, b.k4 bk4, b.k5 bk5, 
                 b.k6 bk6, b.k10 bk10, b.k11 bk11, b.k7 bk7, b.k8 bk8, b.k9 bk9 
                 from baseall a left outer join test b on (a.${c} = b.${c}) 
                 union select a.k1 ak1, a.k2 ak2, a.k3 ak3, a.k4 ak4, a.k5 ak5, a.k6 ak6, a.k10 ak10, 
                 a.k11 ak11, a.k7 ak7, a.k8 ak8, a.k9 ak9, b.k1 bk1, b.k2 bk2, b.k3 bk3, b.k4 bk4, 
                 b.k5 bk5, b.k6 bk6, b.k10 bk10, b.k11 bk11, b.k7 bk7, b.k8 bk8, b.k9 bk9 from 
                 baseall a right outer join test b on (a.${c} = b.${c}) order by 
                 isnull(ak1), 1, 2, 3, 4, isnull(bk1), 12, 13, 14, 15 limit 65535"""

        def res67 = sql"""select * from baseall a left semi join test b on (a.${c} = b.${c}) 
                order by a.k1, a.k2, a.k3"""
        def res68 = sql"""select distinct a.* from baseall a left outer join test b on (a.${c} = b.${c}) 
                where b.k1 is not null order by a.k1, a.k2, a.k3"""
        check2_doris(res67, res68)

        def res69 = sql"""select * from baseall a right semi join test b on (a.${c} = b.${c}) 
                order by b.k1, b.k2, b.k3"""
        def res70 = sql"""select distinct b.* from baseall a right outer join test b on (a.${c} = b.${c}) 
                where a.k1 is not null order by b.k1, b.k2, b.k3"""
        check2_doris(res69, res70)

        def res71 = sql"""select * from baseall a left anti join test b on (a.${c} = b.${c}) 
                order by a.k1, a.k2, a.k3"""
        def res72 = sql"""select distinct a.* from baseall a left outer join test b on (a.${c} = b.${c}) 
                where b.k1 is null order by a.k1, a.k2, a.k3"""
        check2_doris(res71, res72)

        def res73 = sql"""select * from baseall a right anti join test b on (a.${c} = b.${c}) 
                order by b.k1, b.k2, b.k3"""
        def res74 = sql"""select distinct b.* from baseall a right outer join test b on (a.${c} = b.${c}) 
                where a.k1 is null order by b.k1, b.k2, b.k3"""
        check2_doris(res73, res74)
    }



    // complex join
    String col = "k1"
    for (t in join_types){
        qt_complex_join1"""select count(a.k1), count(b.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from baseall where k1 < 5 
                    order by k1) a ${t} join (select k1, k2, k6 from baseall where k1 < 5 
                    order by k1) b on (a.${col} = b.${col})"""
    }

    def res75 = sql"""select count(a.k1), count(b.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from baseall where k1 < 5 
            order by k1) a full outer join (select k1, k2, k6 from baseall where k1 < 5 
            order by k1) b on (a.k1 = b.k1)"""
    def res76 = sql"""select count(c.k1), count(c.m1), count(*) from 
            (select distinct a.*, b.* from (select k1 + 2 as m1, k2 + 1000 as m2, k6 as m6 
            from baseall where k1 < 5 order by k1) a left outer join 
            (select k1, k2, k6 from baseall where k1 < 5 order by k1) b on (a.m1 = b.k1) 
            union (select distinct a.*, b.* from 
            (select k1 + 2 as m1, k2 + 1000 as m2, k6 as m6 from baseall where k1 < 5 
            order by k1) a right outer join (select k1, k2, k6 from baseall where k1 < 5 
            order by k1) b on (a.m1 = b.k1))) c"""
    check2_doris(res75, res76)

    def res77 = sql"""select count(a.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from baseall where k1 < 5 
            order by k1) a left semi join (select k1, k2, k6 from baseall where k1 < 5 
            order by k1) b on (a.k1 = b.k1)"""
    def res78 = sql"""select count(a.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from baseall where k1 < 5 
            order by k1) a left outer join (select k1, k2, k6 from baseall where k1 < 5 
            order by k1) b on (a.k1 = b.k1) where b.k1 is not null """
    check2_doris(res77, res78)

    def res79 = sql"""select count(b.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from baseall where k1 < 5 
            order by k1) a right semi join (select k1, k2, k6 from baseall where k1 < 5 
            order by k1) b on (a.k1 = b.k1) """
    def res80 = sql"""select count(b.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from baseall where k1 < 5 
            order by k1) a right outer join (select k1, k2, k6 from baseall where k1 < 5 
            order by k1) b on (a.k1 = b.k1) where a.k1 is not null"""
    check2_doris(res79, res80)

    def res81 = sql"""select count(a.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from baseall where k1 < 5 
            order by k1) a left anti join (select k1, k2, k6 from baseall where k1 < 5 
            order by k1) b on (a.k1 = b.k1)"""
    def res82 = sql"""select count(a.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from baseall where k1 < 5 
            order by k1) a left outer join (select k1, k2, k6 from baseall where k1 < 5 
            order by k1) b on (a.k1 = b.k1) where b.k1 is null"""
    check2_doris(res81, res82)

    def res83 = sql"""select count(b.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from baseall where k1 < 5 
            order by k1) a right anti join (select k1, k2, k6 from baseall where k1 < 5 
            order by k1) b on (a.k1 = b.k1) """
    def res84 = sql"""select count(b.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from baseall where k1 < 5 
            order by k1) a right outer join (select k1, k2, k6 from baseall where k1 < 5 
            order by k1) b on (a.k1 = b.k1) where a.k1 is null"""
    check2_doris(res83, res84)

    // join multi table
    sql"drop view if exists nullable"
    sql"""create view nullable(n1, n2) as select a.k1, b.k2 from baseall 
            a left join bigtable b on a.k1 = b.k1 + 10 where b.k2 is null"""
    String null_name = "nullable"

    for (t in join_types){
        qt_join_multi_table1"""select * from baseall a ${t} join ${null_name} b on a.k1 = b.n1 order by 
                    a.k1, b.n1"""
        qt_join_multi_table2"""select * from baseall a ${t} join ${null_name} b on a.k1 = b.n2 order by 
                    a.k1, b.n1"""
    }
    test {
        sql"""select a.k1, a.k2 from baseall a left semi join ${null_name} b on a.k1 = b.n2 
            order by a.k1"""
        check{result, exception, startTime, endTime ->
            assertTrue(result.isEmpty())
        }
    }
    test {
        sql"""select b.n1, b.n2 from baseall a right semi join ${null_name} b on a.k1 = b.n2 
           order by b.n1"""
        check{result, exception, startTime, endTime ->
            assertTrue(result.isEmpty())
        }
    }
    test {
        sql"""select b.k1, b.k2 from ${null_name} a right semi join baseall b on b.k1 = a.n2 
           order by b.k1"""
        check{result, exception, startTime, endTime ->
            assertTrue(result.isEmpty())
        }
    }
    test {
        sql"""select a.n1, a.n2 from ${null_name} a left semi join baseall b on b.k1 = a.n2 
           order by 1, 2"""
        check{result, exception, startTime, endTime ->
            assertTrue(result.isEmpty())
        }
    }

    def res85 = sql"""select a.k1, a.k2 from baseall a left anti join ${null_name} b on a.k1 = b.n2 
           order by 1, 2"""
    def res86 = sql"""select k1, k2 from baseall order by k1, k2"""
    check2_doris(res85, res86)

    def res87 = sql"""select b.n1, b.n2 from baseall a right anti join ${null_name} b on a.k1 = b.n2 
            order by 1, 2"""
    def res88 = sql"""select n1, n2 from ${null_name} order by n1, n2"""
    check2_doris(res87, res88)

    def res89 = sql"""select b.k1, b.k2 from ${null_name} a right anti join baseall b on b.k1 = a.n2 
           order by 1, 2"""
    def res90 = sql"""select k1, k2 from baseall order by k1, k2"""
    check2_doris(res89, res90)

    // join on predicate
    qt_join_on_predicate1"""select c.k1 from baseall a join test b on a.k2 between 0 and 1000 
            join bigtable c on a.k10 = c.k10 order by k1 limit 65535"""
    qt_join_on_predicate2"""select a.k1 from baseall a join test b on b.k2 between 0 and 1000 and a.k1 = b.k1 order by k1;"""
    qt_join_on_predicate3"""select a.k1 from baseall a join test b on b.k2 between 0 and 1000 order by k1;"""
    qt_join_on_predicate4"""select a.k1 from baseall a join test b on b.k2 in (49, 60, 85) order by k1;"""
    qt_join_on_predicate5"""select a.k1 from baseall a join test b on b.k2 in (49, 60, 85) and a.k1 = b.k1 order by k1"""
    qt_join_on_predicate6"""select count(a.k1) from baseall a join test b on a.k1 < 10 and a.k1 = b.k1"""
    qt_join_on_predicate7"""SELECT t2.k1,t2.k2,t3.k1,t3.k2 FROM baseall t2 LEFT JOIN test t3 ON t2.k2=t3.k2 WHERE t2.k1 = 4 OR (t2.k1 > 4 AND t3.k1 IS NULL) order by 1, 2, 3, 4"""

    // <=> test cases
    qt_join41"""select 1 <=> 2, 1 <=> 1, "a"= \"a\""""
    qt_join42"""select 1 <=> null, null <=> null,  not("1" <=> NULL)"""
    def res93 = sql"""select  cast("2019-09-09" as int) <=> NULL, cast("2019" as int) <=> NULL"""
    def res94 = sql"""select  NULL <=> NULL, 2019 <=> NULL """
    check2_doris(res93, res94)

    def res95 = sql"""select (2019+10) <=> NULL, not (2019+10) <=> NULL, ("1"+"2") <=> NULL"""
    def res96 = sql"""select  2029 <=> NULL, not 2029 <=> NULL, 3 <=> NULL"""
    check2_doris(res95, res96)

    qt_join43"""select 2019 <=> NULL and NULL <=> NULL, NULL <=> NULL and NULL <=> NULL, 
       2019 <=> NULL or NULL <=> NULL"""


    // <=> in join test case
    String null_table_1 = "join_null_safe_equal_1"
    String null_table_2 = "join_null_safe_equal_2"
    sql"""drop table if exists ${null_table_1}"""
    sql"""drop table if exists ${null_table_2}"""
    sql"""create table if not exists ${null_table_1} (k1 tinyint, k2 decimal(9,3) NULL, k3 char(5) NULL,
                    k4 date NULL, k5 datetime NULL, 
                    k6 double sum) engine=olap 
                    distributed by hash(k1) buckets 2 properties("storage_type"="column", "replication_num" = "1")"""
    sql"""create table if not exists ${null_table_2} (k1 tinyint, k2 decimal(9,3) NULL, k3 char(5) NULL,
                    k4 date NULL, k5 datetime NULL, 
                    k6 double sum) engine=olap 
                    distributed by hash(k1) buckets 2 properties("storage_type"="column", "replication_num" = "1")"""
    sql"""insert into ${null_table_1} values (1, NULL,'null', NULL, NULL, 8.9),
                    (2, NULL,'2', NULL, NULL, 8.9),
                    (3, NULL,'null', '2019-09-09', NULL, 8.9);"""
    sql"""insert into ${null_table_2} values (1, NULL,'null', NULL, NULL, 8.9),
                    (2, NULL,'2', NULL, NULL, 8.9),
                    (3, NULL,'null', '2019-09-09', NULL, 8.9);"""
    sql"""insert into ${null_table_1} values (5, NULL,"null", NULL, "2019-09-09 00:00:00", 8.9)"""
    qt_join44"""select k1<=>NULL, k2<=>NULL, k4<=>NULL, k5<=>NULL, k6<=>NULL
      from ${null_table_1} order by k1, k2, k4, k5, k6"""
    for (index in range(1, 7)) {
        qt_left_join"""select * from ${null_table_1} a left join ${null_table_1} b on  a.k${index}<=>b.k${index} 
            order by a.k1, b.k1"""
        qt_right_join"""select * from ${null_table_1} a right join ${null_table_1} b on  a.k${index}<=>b.k${index}
            order by a.k1, b.k1"""
        qt_hash_join"""select * from ${null_table_1} a right join ${null_table_1} b on  a.k${index}<=>b.k${index} and a.k2=b.k2
            order by a.k1, b.k1"""
        qt_cross_join"""select * from ${null_table_1} a right join ${null_table_1} b on  a.k${index}<=>b.k${index} and a.k2 !=b.k2
            order by a.k1, b.k1"""
        qt_cross_join"""select * from ${null_table_1} a right join ${null_table_1} b on  a.k${index}<=>b.k${index} and a.k1 > b.k1
            order by a.k1, b.k1"""
    }
    
    // test null <=> runtime filter
    sql"set enable_runtime_filter_prune = false"
    for (index in range(1, 7)) {
        qt_left_join"""select * from ${null_table_1} a join ${null_table_1} b on  a.k${index}<=>b.k${index} 
            order by a.k1, b.k1"""
        qt_left_join"""select * from ${null_table_1} a left join ${null_table_1} b on  a.k${index}<=>b.k${index} 
            order by a.k1, b.k1"""
        qt_right_join"""select * from ${null_table_1} a right join ${null_table_1} b on  a.k${index}<=>b.k${index}
            order by a.k1, b.k1"""
        qt_hash_join"""select * from ${null_table_1} a right join ${null_table_1} b on  a.k${index}<=>b.k${index} and a.k2=b.k2
            order by a.k1, b.k1"""
        qt_cross_join"""select * from ${null_table_1} a right join ${null_table_1} b on  a.k${index}<=>b.k${index} and a.k2 !=b.k2
            order by a.k1, b.k1"""
        qt_cross_join"""select * from ${null_table_1} a right join ${null_table_1} b on  a.k${index}<=>b.k${index} and a.k1 > b.k1
            order by a.k1, b.k1"""
    }
    sql"set enable_runtime_filter_prune = true"
    
    //  windows
    def res97 = sql"""select * from (select k1, k2, sum(k2) over (partition by k1) as ss from ${null_table_2})a
       left join ${null_table_1} b on  a.k2=b.k2 and a.k1 >b.k1 order by a.k1, b.k1"""
    def res98 = sql"""select * from (select k1, k2, k5 from ${null_table_2}) a left join ${null_table_1} b
      on  a.k2=b.k2 and a.k1 >b.k1 order by a.k1, b.k1"""
    check2_doris(res97, res98)
    sql"drop table ${null_table_1}"
    sql"drop table ${null_table_2}"



    // join null value
    def table_1 = "join_null_value_left_table"
    def table_2 = "join_null_value_right_table"
    sql"""drop table if exists ${table_1}"""
    sql"""drop table if exists ${table_2}"""
    sql"""create table if not exists ${table_1} (k1 tinyint, k2 decimal(9,3) NULL, k3 char(5) NULL,
                    k4 date NULL, k5 datetime NULL, 
                    k6 double sum) engine=olap 
                    distributed by hash(k1) buckets 2 properties("storage_type"="column", "replication_num" = "1")"""
    sql"""create table if not exists ${table_2} (k1 tinyint, k2 decimal(9,3) NULL, k3 char(5) NULL,
                    k4 date NULL, k5 datetime NULL, 
                    k6 double sum) engine=olap 
                    distributed by hash(k1) buckets 2 properties("storage_type"="column", "replication_num" = "1")"""
    sql"""insert into ${table_1} values (1, NULL,'null', NULL, NULL, 8.9),
                    (2, NULL,'2', NULL, NULL, 8.9),
                    (3, NULL,'null', '2019-09-09', NULL, 8.9);"""
    sql"""insert into ${table_2} values (1, NULL,'null', NULL, NULL, 8.9),
                    (2, NULL,'2', NULL, NULL, 8.9),
                    (3, NULL,'null', '2019-09-09', NULL, 8.9);"""
    sql"""insert into ${table_1} values (5, 2.2,"null", NULL, "2019-09-09 00:00:00", 8.9)"""
    for (type in join_types) {
        for (index in range(1, 7)) {
            qt_join_null_value1"""select * from ${table_1} a ${type} join ${table_2} b on a.k${index} = b.k${index} and 
                a.k2 = b.k2 and a.k${index} != b.k2 order by a.k1, b.k1"""
            qt_join_null_value2"""select * from ${table_1} a ${type} join ${table_2} b on a.k${index} = b.k${index} and 
                a.k2 = b.k2 and a.k${index} != b.k2 order by a.k1, b.k1"""
        }
    }
    //  <=>, =, is NULL, ifnull
    qt_join_null1"""select * from ${table_1} a  left join ${table_2} b on a.k2 <=> b.k2 and 
        a.k3 is NULL order by a.k1, b.k1"""
    qt_join_null2"""select * from ${table_1} a join ${table_2} b on a.k2<=> b.k2 and 
        a.k4<=>NULL order by a.k1,b.k1"""
    qt_join_null3"""select * from ${table_1} a join ${table_2} b on a.k2<=> b.k2
        and a.k4<=>NULL  and b.k4 is not NULL order by a.k1,b.k1"""
    qt_join_null4"""select * from ${table_1} a join ${table_2} b on a.k2<=> b.k2 and 
       a.k4<=>NULL  and b.k4 is not NULL and a.k3=2 order by a.k1,b.k1"""
    qt_join_null5"""select * from ${table_1} a join ${table_2} b on ifnull(a.k4,null)
       <=> ifnull(b.k5,null) order by a.k1, a.k2, a.k3, b.k1, b.k2"""
    sql"drop table ${table_1}"
    sql"drop table ${table_2}"



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

    // test bucket shuffle join, github issue #6171
    List table_list = ["T_DORIS_A", "T_DORIS_B", "T_DORIS_C", "T_DORIS_D", "T_DORIS_E"]
    List column_list = [",APPLY_CRCL bigint(19)",
                   ",FACTOR_FIN_VALUE decimal(19,2),PRJT_ID bigint(19)",
                   "",
                   ",LIMIT_ID bigint(19),CORE_ID bigint(19)",
                   ",SHARE_ID bigint,SPONSOR_ID bigint"]
    table_list.eachWithIndex {tb, idx ->
        sql"""drop table if exists ${tb}"""
        sql"""create table if not exists ${tb} (ID bigint not null ${column_list[idx]}) 
                UNIQUE KEY(`ID`) 
                DISTRIBUTED BY HASH(`ID`) BUCKETS 32 
                PROPERTIES("replication_num"="1");"""
    }
    sql "SET disable_join_reorder=true"
    def ret = sql"""desc SELECT B.FACTOR_FIN_VALUE, D.limit_id FROM T_DORIS_A A LEFT JOIN T_DORIS_B B ON B.PRJT_ID = A.ID 
            LEFT JOIN T_DORIS_C C ON A.apply_crcl = C.id JOIN T_DORIS_D D ON C.ID = D.CORE_ID order by 
            B.FACTOR_FIN_VALUE, D.limit_id desc;"""
    logger.info(ret.toString())
    assertTrue(ret.toString().contains("  |  join op: INNER JOIN(BROADCAST)"))
    sql "SET disable_join_reorder=false"

    sql "drop table if exists `t0`"
    sql "drop table if exists `t1`"

    sql """
    CREATE TABLE IF NOT EXISTS t0(c0 BOOLEAN NOT NULL) DISTRIBUTED BY HASH (c0) BUCKETS 8 PROPERTIES ("replication_num" = "1");
    """
    sql """
    CREATE TABLE IF NOT EXISTS t1(c0 DATETIME NOT NULL) DISTRIBUTED BY HASH (c0) BUCKETS 9 PROPERTIES ("replication_num" = "1");
    """
    sql """INSERT INTO t1 (c0) VALUES (DATE '1970-02-15'), (DATE '1970-11-05'), (DATE '1970-07-10');"""
    sql """INSERT INTO t1 (c0) VALUES (DATE '1970-04-04');"""
    sql """INSERT INTO t1 (c0) VALUES (DATE '1970-09-06');"""
    sql """INSERT INTO t0 (c0) VALUES (true);"""
    sql """INSERT INTO t0 (c0) VALUES (false);"""
    qt_test """SELECT t1.c0 FROM  t1 RIGHT JOIN t0 ON true WHERE  (abs(1)=0) GROUP BY  t1.c0;"""

    sql """ DROP TABLE IF EXISTS tbl2; """
    sql """ DROP TABLE IF EXISTS tbl1; """
    sql """ CREATE TABLE tbl1 (
            data_dt DATE NULL COMMENT '数据日期',
            engineer VARCHAR(100) NULL COMMENT '工程师'
            ) ENGINE=OLAP
            UNIQUE KEY(data_dt, engineer)
            PARTITION BY RANGE(data_dt)
            (
              FROM ('2022-11-05') TO ('2024-03-20') INTERVAL 1 DAY
            )
            DISTRIBUTED BY HASH(data_dt) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "group_commit_interval_ms" = "10000",
            "group_commit_data_bytes" = "134217728"
            ); """
    sql """ CREATE TABLE tbl2 (
            data_dt DATE NULL COMMENT '数据日期'
            ) ENGINE=OLAP
            UNIQUE KEY(data_dt)
            PARTITION BY RANGE(data_dt)
            (
              FROM ('2022-11-05') TO ('2024-03-20') INTERVAL 1 DAY
            )
            DISTRIBUTED BY HASH(data_dt) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "min_load_replica_num" = "-1",
            "is_being_synced" = "false",
            "storage_format" = "V2",
            "group_commit_interval_ms" = "10000",
            "group_commit_data_bytes" = "134217728"
            ); """

    sql """ insert into tbl1 values('2023-01-01', 'engineer1'),('2023-01-01', 'engineer2'),('2023-01-02', 'engineer3'),('2023-01-02', 'enginee4'); """
    sql """ insert into tbl2 values('2023-01-01'); """
    qt_sql """ select /*+SET_VAR(batch_size=1, disable_join_reorder=true)*/ count(DISTINCT dcqewrt.engineer)  as active_person_count from tbl1 dcqewrt left join [broadcast] tbl2 dd on dd.data_dt = dcqewrt.data_dt; """
    sql """ DROP TABLE IF EXISTS tbl2; """
    sql """ DROP TABLE IF EXISTS tbl1; """
}
