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

suite("test_union") {
    def db = "test_query_db"
    sql "use ${db}"

//    order_qt_select "select k1, k2 from baseall union select k2, k3 from test"
//    order_qt_select "select k2, count(k1) from ((select k2, avg(k1) k1 from baseall group by k2) union all (select k2, count(k1) k1 from test group by k2) )b group by k2 having k2 > 0 order by k2;"

    def tbName1 = "test"
    def tbName2 = "baseall"

    qt_union1 """(select A.k2 as wj1,count(*) as wj2, case A.k2 when 1989 then "wj" 
		    when 1992 then "dyk" when 1985 then "wcx" else "mlx" end 
		    from ${tbName1} as A join ${tbName1} as B where A.k1=B.k1+1 
		    group by A.k2 having sum(A.k3)> 1989) union all 
		    (select C.k5, C.k8, C.k6 from ${tbName1} as C where lower(C.k6) like "tr%")
		    order by wj1,wj2"""
    qt_union2 """(select A.k2 as wj1,count(*) as wj2, case A.k2 when 1989 then "wj" 
		    when 1992 then "dyk" when 1985 then "wcx" else "mlx" end,
		    if (A.k2<>255,"hello","world") 
		    from ${tbName1} as A join ${tbName1} as B where A.k1=B.k1+1 
		    group by A.k2 having sum(A.k3)> 1989) union all 
		    (select C.k5, C.k8, C.k6, if (C.k8<0,"hello","world") 
		    from ${tbName1} as C where lower(C.k6) like "tr%")
		    order by wj1,wj2"""
    qt_union3 """ select A.k2,count(*) from ${tbName1} as A join ${tbName1} as B 
		    where A.k1=B.k1+1 group by A.k2 having sum(A.k3)> 1989 order by A.k2 desc"""
    qt_union4 """(select A.k2 as wj1,count(*) as wj2 from ${tbName1} as A join ${tbName1} as B 
		    where A.k1=B.k1+1 group by A.k2 having sum(A.k3)> 1989)
		    union all (select C.k5, C.k8 from ${tbName2} as C where C.k6 like "tr%")
		    order by wj1,wj2"""
    qt_union5 """(select * from ${tbName1}) union (select * from ${tbName1}) order by k1, k2, k3, k4 limit 4"""
    qt_union6 """(select * from ${tbName1}) union all (select * from ${tbName1}) 
		    order by k1, k2, k3, k4 limit 4"""
    qt_union7 """(select * from ${tbName1} where k1<10) union all 
		    (select * from ${tbName1} where k5<0) order by k1,k2,k3 limit 40"""
    qt_union8 """(select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from ${tbName1} where k1>0)
      union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from ${tbName2} where k2>0)
      union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from ${tbName2} where k3>0)
		      order by k1, k2, k3, k4"""
    qt_union9 """(select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from ${tbName1} 
		    where k1>0 order by k1, k2, k3, k4 limit 1)
      union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from ${tbName2} 
                    where k2>0 order by k1, k2, k3, k4 limit 1)
      union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from ${tbName2} 
		    where k3>0  order by k1, k2, k3, k4 limit 1)
      order by k1, k2, k3, k4"""
    qt_union10 """(select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from ${tbName1} where k1>0)
      union distinct (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from ${tbName2} where k2>0)
      union distinct (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from ${tbName2} where k3>0)
		      order by k1, k2, k3, k4"""
    qt_union11 """(select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from ${tbName1} where k1>0)
      union distinct (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from ${tbName2} where k2>0)
      union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from ${tbName2} where k3>0)
		      order by k1, k2, k3, k4"""
    qt_union12 """(select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from ${tbName1} 
		    where k1>0 order by k1, k2, k3, k4 limit 1)
      union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from ${tbName2} 
                    where k2>0 order by k1, k2, k3, k4 limit 1)
      union distinct (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from ${tbName2} 
		    where k3>0  order by k1, k2, k3, k4 limit 1)
      order by k1, k2, k3, k4"""
//    qt_union13 """(select count(k1), sum(k2) from ${tbName1})
//            union all (select k1, k2 from ${tbName1} order by k1, k2 limit 10)
//	    union all (select sum(k1), max(k3) from ${tbName1} group by k2)
//	    union all (select k1, k2 from ${tbName2})
//	    union all (select a.k1, b.k2 from ${tbName1} a join ${tbName2} b on (a.k1=b.k1))
//	    union all (select 1000, 2000) order by k1, k2"""
    qt_union14 """select * from (select 1 a, 2 b 
		    union all select 3, 4 
		    union all select 10, 20) t where a<b order by a, b"""
    qt_union15 """select count(*) from (select 1 from ${tbName1} as t1 join ${tbName2} as t2 on t1.k1 = t2.k1
		        union all select 1 from ${tbName1} as t1) as t3"""
    qt_union16 """(select k1, count(*) from ${tbName1} where k1=1 group by k1)
		    union distinct (select 2,3) order by 1,2"""
    qt_union17 """(select 1, 'a', NULL, 10.0)
            union all (select 2, 'b', NULL, 20.0)
	    union all (select 1, 'a', NULL, 10.0) order by 1, 2"""
    qt_union18 """select count(*) from (
             (select k1, k2, k3, k4, k5, k6, k7, k8, k9, 10, k11 from ${tbName1} where k1>0)
   union distinct (select k1, k2, k3, k4, k5, k6, k7, k8, k9, 10, k11 from ${tbName1} where k2>0)
   union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, 10, k11 from ${tbName2} where k3>0)) x"""
    qt_union19 """(select 10, 10.0, 'hello', 'world') union all
            (select k1, k5, k6, k7 from ${tbName2} where k1=1) union all
	    (select 20, 20.0, 'wangjuoo4', 'beautiful') union all
	    (select k2, k8, k6, k7 from ${tbName2} where k2>0) order by 1, 2, 3, 4"""
    qt_union20 """select x.k1, k2, k3, k4, k5 from 
             ((select k1, k2, k3, k4, k5 from ${tbName1} where k1>0) union distinct
	     (select k1, k2, k3, k4, k5 from ${tbName2} where k2>0)) x 
	     where x.k1<5 and x.k3>0 order by 1, 2, 3, 4"""
    qt_union21 """select x.k1, k2, k3, k4, k5 from 
             ((select k1, k2, k3, k4, k5 from ${tbName1} where k1>0) union all
	     (select k1, k2, k3, k4, k5 from ${tbName2} where k2>0)) x 
	     where x.k1<5 and x.k3>0 order by 1, 2, 3, 4"""


    // test_query_union_2
    qt_union22 """select x.k1, k6, k7, k8, k9, k10 from 
          ((select k1, k6, k7, k8, k9, k10 from ${tbName1} where k1=1) union distinct
	   (select k1, k6, k7, k8, k9, k10 from ${tbName1} where k9>0)) x union distinct
	  (select k1, k6, k7, k8, k9, k10 from ${tbName2}) order by 1, 4, 5, 6 limit 10"""
    qt_union23 """select x.k1, k6, k7, k8, k9, k10 from 
          ((select k1, k6, k7, k8, k9, k10 from ${tbName1} where k1=1) union all
	   (select k1, k6, k7, k8, k9, k10 from ${tbName1} where k9>0)) x union all
	  (select k1, k6, k7, k8, k9, k10 from ${tbName2}) order by 1, 4, 5, 6 limit 10"""
    qt_union24 """(select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from ${tbName1} where k1>0)
           union all (select 1, 2, 3, 4, 3.14, 'hello', 'world', 0.0, 1.1, cast('1989-03-21' as date), 
           cast('1989-03-21 13:00:00' as datetime))
           union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from ${tbName2} where k3>0)
	       order by k1, k2, k3, k4"""
    qt_union25 """(select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from ${tbName1} where k1>0)
            union distinct (select 1, 2, 3, 4, 3.14, 'hello', 'world', 0.0, 1.1, cast('1989-03-21' as date), 
            cast('1989-03-21 13:00:00' as datetime))
            union distinct (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from ${tbName2} where k3>0)
	        order by k1, k2, k3, k4"""


    // test_union_basic
    qt_union26 """select 1, 2  union select 1.01, 2.0 union (select 0.0001, 0.0000001) order by 1, 2"""
    qt_union27 """select 1, 2 union (select "hell0", "") order by 1, 2"""
    qt_union28 """select 1, 2  union select 1.0, 2.0 union (select 1.00000000, 2.00000) order by 1, 2"""
    qt_union29 """select 1, 2  union all select 1.0, 2.0 union (select 1.00000000, 2.00000) order by 1, 2"""
    qt_union30 """select 1, 2  union all select 1.0, 2.0 union all (select 1.00000000, 2.00000) order by 1, 2"""
    qt_union31 """select 1, 2  union select 1.0, 2.0 union all (select 1.00000000, 2.00000) order by 1, 2"""
    qt_union32 """select 1, 2  union distinct select 1.0, 2.0 union distinct (select 1.00000000, 2.00000) order by 1, 2"""
    qt_union33 """select cast("2016-07-01" as date) union (select "2016-07-02") order by 1"""
    qt_union34 """select "2016-07-01" union (select "2016-07-02") order by 1"""
    qt_union35 """select cast("2016-07-01" as date) union (select cast("2016-07-02 1:10:0" as date)) order by 1"""
    def res1 = sql"""select cast(1 as decimal), cast(2 as double) union distinct select 1.0, 2.0 
             union distinct (select 1.00000000, 2.00000) order by 1, 2"""
    def res2 = sql"""select cast(1 as decimal), cast(2 as decimal) union distinct select 1.0, 2.0 
             union distinct (select 1.00000000, 2.00000) order by 1, 2"""

    // test_union_multi
    List sub_sql = ["(select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from baseall where k1 % 3 = 0)"] * 10
    String sql1 = String.join(" union ", sub_sql) + " order by 1, 2, 3, 4"
    String sql2 = String.join(" union all ", sub_sql) + " order by 1, 2, 3, 4"
    String sql3 = String.join(" union distinct ", sub_sql) + " order by 1, 2, 3, 4"
    sql(sql1)
    sql(sql1)
    sql(sql1)
    sql(sql1)
    sql(sql1)
    sql(sql2)
    sql(sql2)
    sql(sql2)
    sql(sql2)
    sql(sql2)
    sql(sql3)
    sql(sql3)
    sql(sql3)
    sql(sql3)
    sql(sql3)


    // test_union_bug
    // PALO-3617
    qt_union36 """select * from (select 1 as a, 2 as b union select 3, 3) c where a = 1"""
    sql """drop view if exists nullable"""
    sql """CREATE VIEW `nullable` AS SELECT `a`.`k1` AS `n1`, `b`.`k2` AS `n2` 
           FROM `${db}`.`baseall` a LEFT OUTER JOIN 
           `${db}`.`bigtable` b ON `a`.`k1` = `b`.`k1` + 10
           WHERE `b`.`k2` IS NULL"""
    order_qt_union37 """select n1 from nullable union all select n2 from nullable"""
    qt_union38 """(select n1 from nullable) union all (select n2 from nullable order by n1) order by n1"""
    qt_union39 """(select n1 from nullable) union all (select n2 from nullable) order by n1"""


    // test_union_different_column
    // 2个select 的列个数 或 字段类型不相同
    // 列个数会报错；大类型（数值或字符或日期）不同的会报错，大类型相同的成功
    test {
        sql "select k1, k2 from ${tbName2} union select k2 from ${tbName1} order by k1, k2"
        check{result, exception, startTime, endTime ->
            assertTrue(exception != null)
            logger.info(exception.message)
        }
    }
    test {
        sql "select k1, k1 from ${tbName2} union select k2 from ${tbName1} limit 3"
        check { result, exception, startTime, endTime ->
            assertTrue(exception != null)
            logger.info(exception.message)
        }
    }
    test {
        sql "(select k1, k1 from ${tbName2}) union (select k2, 1 from ${tbName1}) order by k1"
        check{result, exception, startTime, endTime ->
            assertTrue(exception != null)
            logger.info(exception.message)
        }
    }
    test {
        sql "(select k1+k1 from ${tbName2}) union (select k2 from ${tbName1}) order by k1+k1"
        check{result, exception, startTime, endTime ->
            assertTrue(exception != null)
            logger.info(exception.message)
        }
    }


    // 不同类型的列
    def index = 2..10
    index.each {
        if (![6, 7, 10].contains(it.toInteger())) {
            def res3 = sql"""(select k1 from ${tbName2}) union all (select k${it} from ${tbName1} 
                    order by k${it}) order by k1 limit 30"""
            def res4 = sql"""select k1 from ${tbName2} union all (select k${it} from ${tbName1} 
                    order by k${it})order by k1 limit 30"""
            check2_doris(res3, res4)
        }
    }
    test {
        sql """(select k1, k2 from ${tbName2}) union (select k2, k10 from ${tbName1} order by k10)
            order by k1, k2"""
        check {result, exception, startTime, endTime ->
            assertTrue(exception != null)
            logger.info(exception.message)
        }
    }
    // cast类型
    def res5 = sql"""(select k1, k2 from ${tbName2}) union (select k2, cast(k11 as int) from ${tbName1})
       order by k1, k2"""
    def res6 = sql"""(select k1, k2 from ${tbName2}) union (select k2, cast(k11 as int) from ${tbName1} order by k2)
       order by k1, k2"""
    check2_doris(res5, res6)
    def res7 = sql"""(select k1, k2 from ${tbName2}) union (select k2, cast(k10 as int) from ${tbName1}) order by k1, k2"""

    def res8 = sql"""(select k1, k2 from ${tbName2}) union (select k2, cast(k10 as int) from ${tbName1} order by k2) order
        by k1, k2"""
    check2_doris(res7, res8)
    // 不同类型不同个数
    test {
        sql """select k1, k2 from ${tbName2} union select k11, k10, k9  from ${tbName1} order by k1, k2"""
        check {result, exception, startTime, endTime ->
            assertTrue(exception != null)
            logger.info(exception.message)
        }
    }

    // test_union_different_schema
    def new_union_table = "union_different_schema_table"
    sql"""drop table if exists ${new_union_table}"""
    sql"""create table if not exists ${new_union_table}(k1 tinyint, k2 decimal(9,3) NULL, k3 char(5) NULL,
        k4 date NULL, k5 datetime NULL, 
        k6 double sum) engine=olap 
        distributed by hash(k1) buckets 2 properties("storage_type"="column", "replication_num" = "1")"""
    //不同schema 不同列报错
    test{
        sql "select * from ${new_union_table} union select * from ${tbName1} order by k1, k2"
        check{result, exception, startTime, endTime ->
            assertTrue(exception != null)
            logger.info(exception.message)
        }
    }
    for (idx in range(1, 5)) {
        qt_union40 """(select k1 from ${new_union_table}) union (select k${idx} from ${tbName1}) order by k1"""
    }
    sql"""drop table ${new_union_table}"""

    sql 'set enable_fallback_to_original_planner=false'
    sql 'set enable_nereids_planner=true'
    qt_union35 """select cast("2016-07-01" as date) union (select cast("2016-07-02 1:10:0" as date)) order by 1"""

    qt_union36 """SELECT a,2 as a FROM (SELECT '1' as a) b where a=1;"""
    
    test {
        sql 'select * from (values (1, 2, 3), (4, 5, 6)) a'
        result([[1, 2, 3], [4, 5, 6]])
    }
}
