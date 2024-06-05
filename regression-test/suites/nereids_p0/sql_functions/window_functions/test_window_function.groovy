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

suite("test_window_function") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    def windowFunctionTable1 = "test_window_function1"
    sql """ DROP TABLE IF EXISTS ${windowFunctionTable1} """
    sql """ create table if not exists ${windowFunctionTable1} (stock_symbol varchar(64), closing_price decimal(8,2), closing_date datetime not null, closing_date1 datetimev2 not null, closing_date2 datetimev2(3) not null, closing_date3 datetimev2(6) not null) duplicate key (stock_symbol) distributed by hash (stock_symbol) PROPERTIES("replication_num" = "1") """

    sql """ INSERT INTO ${windowFunctionTable1} VALUES ('JDR',12.86,'2014-10-02 00:00:00','2014-10-02 00:00:00.111111','2014-10-02 00:00:00.111111','2014-10-02 00:00:00.111111'),('JDR',12.89,'2014-10-03 00:00:00','2014-10-03 00:00:00.111111','2014-10-03 00:00:00.111111','2014-10-03 00:00:00.111111'),('JDR',12.94,'2014-10-04 00:00:00','2014-10-04 00:00:00.111111','2014-10-04 00:00:00.111111','2014-10-04 00:00:00.111111'),('JDR',12.55,'2014-10-05 00:00:00','2014-10-05 00:00:00.111111','2014-10-05 00:00:00.111111','2014-10-05 00:00:00.111111'),('JDR',14.03,'2014-10-06 00:00:00','2014-10-06 00:00:00.111111','2014-10-06 00:00:00.111111','2014-10-06 00:00:00.111111'),('JDR',14.75,'2014-10-07 00:00:00','2014-10-07 00:00:00.111111','2014-10-07 00:00:00.111111','2014-10-07 00:00:00.111111'),('JDR',13.98,'2014-10-08 00:00:00','2014-10-08 00:00:00.111111','2014-10-08 00:00:00.111111','2014-10-08 00:00:00.111111') """

    qt_sql """
            SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=1) */
                stock_symbol,
                closing_date,
                closing_price,
                avg( closing_price ) over ( PARTITION BY stock_symbol ORDER BY closing_date rows BETWEEN 1 preceding AND 1 following ) AS moving_average 
            FROM
                ${windowFunctionTable1}   
            ORDER BY
                stock_symbol,
                closing_date,
                closing_price
           """
    // LEAD
    qt_sql """ 
             SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=1) */
                stock_symbol,
                closing_date,
                closing_price,
                CASE ( lead( closing_price, 1, 0 ) over ( PARTITION BY stock_symbol ORDER BY closing_date )- closing_price ) > 0 
                WHEN TRUE THEN  "higher" 
                WHEN FALSE THEN "flat or lower" END AS "trending" 
             FROM
                ${windowFunctionTable1}  
             ORDER BY
                closing_date;
            """

    // LEAD not nullable coredump
    qt_sql """
           select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ t1.new_time from (select closing_date, lead(closing_date, 1, '2014-10-02 00:00:00') over () as new_time from ${windowFunctionTable1}) as t1 left join ${windowFunctionTable1} t2 on t2.closing_date = t1.closing_date order by t1.new_time desc;
           """

    // LAG
    qt_sql """ 
             SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=1) */
                stock_symbol,
                closing_date,
                closing_price,
                lag( closing_price, 1, 0 ) over ( PARTITION BY stock_symbol ORDER BY closing_date ) AS "yesterday closing" 
             FROM
                ${windowFunctionTable1}  
             ORDER BY
                closing_date;
            """

    qt_sql """
            SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=1) */
                stock_symbol,
                closing_date1,
                closing_price,
                avg( closing_price ) over ( PARTITION BY stock_symbol ORDER BY closing_date1 rows BETWEEN 1 preceding AND 1 following ) AS moving_average
            FROM
                ${windowFunctionTable1}
            ORDER BY
                stock_symbol,
                closing_date1,
                closing_price
           """
    // LEAD
    qt_sql """
             SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=1) */
                stock_symbol,
                closing_date1,
                closing_price,
                CASE ( lead( closing_price, 1, 0 ) over ( PARTITION BY stock_symbol ORDER BY closing_date1 )- closing_price ) > 0
                WHEN TRUE THEN  "higher"
                WHEN FALSE THEN "flat or lower" END AS "trending"
             FROM
                ${windowFunctionTable1}
             ORDER BY
                closing_date1;
            """

    // LEAD not nullable coredump
    qt_sql """  
           select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ t1.new_time from (select closing_date1, lead(closing_date1, 1, '2014-10-02 00:00:00') over () as new_time from ${windowFunctionTable1}) as t1 left join ${windowFunctionTable1} t2 on t2.closing_date1 = t1.closing_date1 order by t1.new_time desc;
           """

    // LAG
    qt_sql """
             SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=1) */
                stock_symbol,
                closing_date1,
                closing_price,
                lag( closing_price, 1, 0 ) over ( PARTITION BY stock_symbol ORDER BY closing_date1 ) AS "yesterday closing"
             FROM
                ${windowFunctionTable1}
             ORDER BY
                closing_date1;
            """

    qt_sql """
            SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=1) */
                stock_symbol,
                closing_date2,
                closing_price,
                avg( closing_price ) over ( PARTITION BY stock_symbol ORDER BY closing_date2 rows BETWEEN 1 preceding AND 1 following ) AS moving_average
            FROM
                ${windowFunctionTable1}
            ORDER BY
                stock_symbol,
                closing_date2,
                closing_price
           """
    // LEAD
    qt_sql """
             SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=1) */
                stock_symbol,
                closing_date2,
                closing_price,
                CASE ( lead( closing_price, 1, 0 ) over ( PARTITION BY stock_symbol ORDER BY closing_date2 )- closing_price ) > 0
                WHEN TRUE THEN  "higher"
                WHEN FALSE THEN "flat or lower" END AS "trending"
             FROM
                ${windowFunctionTable1}
             ORDER BY
                closing_date2;
            """

    // LEAD not nullable coredump
    qt_sql """
           select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ t1.new_time from (select closing_date2, lead(closing_date2, 1, '2014-10-02 00:00:00') over () as new_time from ${windowFunctionTable1}) as t1 left join ${windowFunctionTable1} t2 on t2.closing_date2 = t1.closing_date2 order by t1.new_time desc;
           """

    // LAG
    qt_sql """
             SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=1) */
                stock_symbol,
                closing_date2,
                closing_price,
                lag( closing_price, 1, 0 ) over ( PARTITION BY stock_symbol ORDER BY closing_date2 ) AS "yesterday closing"
             FROM
                ${windowFunctionTable1}
             ORDER BY
                closing_date2;
            """

    qt_sql """
            SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=1) */
                stock_symbol,
                closing_date3,
                closing_price,
                avg( closing_price ) over ( PARTITION BY stock_symbol ORDER BY closing_date3 rows BETWEEN 1 preceding AND 1 following ) AS moving_average
            FROM
                ${windowFunctionTable1}
            ORDER BY
                stock_symbol,
                closing_date3,
                closing_price
           """
    // LEAD
    qt_sql """
             SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=1) */
                stock_symbol,
                closing_date3,
                closing_price,
                CASE ( lead( closing_price, 1, 0 ) over ( PARTITION BY stock_symbol ORDER BY closing_date3 )- closing_price ) > 0
                WHEN TRUE THEN  "higher"
                WHEN FALSE THEN "flat or lower" END AS "trending"
             FROM
                ${windowFunctionTable1}
             ORDER BY
                closing_date3;
            """

    // LEAD not nullable coredump
    qt_sql """
           select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ t1.new_time from (select closing_date3, lead(closing_date3, 1, '2014-10-02 00:00:00') over () as new_time from ${windowFunctionTable1}) as t1 left join ${windowFunctionTable1} t2 on t2.closing_date3 = t1.closing_date3 order by t1.new_time desc;
           """

    // LAG
    qt_sql """
             SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=1) */
                stock_symbol,
                closing_date3,
                closing_price,
                lag( closing_price, 1, 0 ) over ( PARTITION BY stock_symbol ORDER BY closing_date3 ) AS "yesterday closing"
             FROM
                ${windowFunctionTable1}
             ORDER BY
                closing_date3;
            """
    sql """ drop table   ${windowFunctionTable1} """


    def windowFunctionTable2 = "test_window_function2"
    sql """ DROP TABLE IF EXISTS ${windowFunctionTable2} """
    sql """ create table if not exists ${windowFunctionTable2} (x int, property  varchar(64)) duplicate key (x) distributed by hash (x) PROPERTIES("replication_num" = "1") """
    sql """ insert into  ${windowFunctionTable2} values (2,'even'),(4,'even'),(6,'even'),(8,'even'),(10,'even'),(1,'odd'),(3,'odd'),(5,'odd'),(7,'odd'),(9,'odd'); """

    // SUM
    qt_sql """
               SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=1) */
                    x,
                    property,
                    sum( x ) over ( PARTITION BY property ORDER BY x rows BETWEEN 1 preceding AND 1 following ) AS 'moving total' 
                FROM
                    ${windowFunctionTable2} 
                WHERE
                    property IN ( 'odd', 'even' )
                ORDER BY
                    property, x;
           """
    // AVG
    qt_sql """
               SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=1) */
                    x,
                    property,
                    avg( x ) over ( PARTITION BY property ORDER BY x rows BETWEEN 1 preceding AND 1 following ) AS 'moving average' 
               FROM
                    ${windowFunctionTable2} 
               WHERE
                    property IN ( 'odd', 'even' )
               ORDER BY
                    property, x;
           """
    // COUNT
    qt_sql """
               SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=1) */
                    x,
                    property,
                    count( x ) over ( PARTITION BY property ORDER BY x rows BETWEEN unbounded preceding AND current ROW ) AS 'cumulative total' 
               FROM
                    ${windowFunctionTable2}  
               WHERE
                    property IN ( 'odd', 'even' )
               ORDER BY
                    property, x;
           """
    sql """ truncate table ${windowFunctionTable2} """
    sql """ insert into  ${windowFunctionTable2} values (2,'even'),(4,'even'),(6,'even'),(8,'even'),(10,'even'),(1,'odd'),(3,'odd'),(5,'odd'),(7,'odd'),(9,'odd'); """

    // MIN
    qt_sql """ 
              SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=1) */
                   x,
                   property,
                   min( x ) over ( ORDER BY property, x DESC rows BETWEEN unbounded preceding AND 1 following ) AS 'local minimum' 
              FROM
                   ${windowFunctionTable2} 
              WHERE
                   property IN ( 'prime', 'square' );
           """
    // MAX
    qt_sql """
              SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=1) */
                   x,
                   property,
                   max( x ) over ( ORDER BY property, x rows BETWEEN unbounded preceding AND 1 following ) AS 'local maximum' 
              FROM
                   ${windowFunctionTable2}  
              WHERE
                   property IN ( 'prime', 'square' );
           """
    sql """ drop table   ${windowFunctionTable2}  """


    def windowFunctionTable3 = "test_window_function3"
    sql """ DROP TABLE IF EXISTS ${windowFunctionTable3} """
    sql """ create table if not exists ${windowFunctionTable3} (x int, y  int) duplicate key (x) distributed by hash (x) PROPERTIES("replication_num" = "1") """
    sql """ insert into  ${windowFunctionTable3} values (1,1),(1,2),(1,2),(2,1),(2,2),(2,3),(3,1),(3,1),(3,2); """

    // RANK
    qt_sql """ select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ x, y, rank() over(partition by x order by y) as rank from ${windowFunctionTable3} order by x, y; """
    // DENSE_RANK
    qt_sql """ select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ x, y, dense_rank() over(partition by x order by y) as rank from ${windowFunctionTable3} order by x, y; """
    // PERCENT_RANK
    qt_sql """ select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ x, y, percent_rank() over(partition by x order by y) as rank from ${windowFunctionTable3} order by x, y; """
    // CUME_DIST
    qt_sql """ select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ x, y, cume_dist() over(partition by x order by y) as rank from ${windowFunctionTable3} order by x, y; """
    // ROW_NUMBER
    qt_sql """ select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ x, y, row_number() over(partition by x order by y) as rank from ${windowFunctionTable3} order by x, y; """

    sql """ drop table   ${windowFunctionTable3}  """


    def windowFunctionTable4 = "test_window_function4"
    sql """ DROP TABLE IF EXISTS ${windowFunctionTable4} """
    sql """ create table if not exists ${windowFunctionTable4} (name varchar(64),country varchar(64),greeting varchar(64)) duplicate key (name) distributed by hash (name) PROPERTIES("replication_num" = "1") """
    sql """ insert into ${windowFunctionTable4} VALUES ('Pete','USA','Hello'),('John','USA','Hi'),('Boris','Germany','Guten tag'),('Michael','Germany','Guten morgen'),('Bjorn','Sweden','Hej'),('Mats','Sweden','Tja')"""

    // first_value
    qt_sql """ select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ country, name,first_value(greeting) over (partition by country order by name, greeting) as greeting from ${windowFunctionTable4} order by country, name; """
    // last_value
    qt_sql """ select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ country, name,last_value(greeting)  over (partition by country order by name, greeting) as greeting from ${windowFunctionTable4} order by country, name; """

    sql """ drop table   ${windowFunctionTable4}  """

    sql "use nereids_test_query_db"
    List<String> fields = ["k1", "k2", "k3", "k4", "k5", "k6", "k10", "k11", "k7", "k8", "k9"]

    // test_query_first_value
    String k1 = fields[3]
    String k2 = fields[5]
    String k3 = fields[3]
    qt_first_value1"""select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ ${k1}, first_value(${k2}) over (partition by ${k1} order by ${k3}, ${k2})
             as wj from baseall  order by ${k1}, wj"""
    qt_first_value2"""select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ ${k1}, first_value(${k2}) over (partition by ${k1} order by ${k3}, ${k2}
             range between unbounded preceding and current row)
             as wj from baseall  order by ${k1}, wj"""
    qt_first_value3"""select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ ${k1}, first_value(${k2}) over (partition by ${k1} order by ${k3}, ${k2} 
             rows between unbounded preceding and current row)
             as wj from baseall  order by ${k1}, wj"""
    qt_first_value4"""select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ a, min(d) as wjj from 
             (select t1.k1 as k1, t1.k2 as k2, t1.k3 as k3,
             t1.k4 as k4, t1.k5 as k5,t1.k6 as k6,
             t1.k7 as k7, t1.k8 as k8, t1.k9 as k9,
             t1.k10 as k10, t1.k11 as k11, 
             t1.${k1} as a, t1.${k2} as b, t2.${k2} as c, t2.${k3} as d 
             from baseall t1 join baseall  t2 
             where t1.${k1}=t2.${k1} and t1.${k3}>=t2.${k3}) T 
             group by k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, a, b
             order by a, wjj"""

    // test_query_last_value
    qt_last_value1"""select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ ${k1}, last_value(${k2}) over (partition by ${k1} order by ${k3},${k2})
                 as wj from baseall  order by ${k1}, wj"""
    qt_last_value2"""select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ ${k1}, last_value(${k2}) over (partition by ${k1} order by ${k3},${k2}
             range between unbounded preceding and current row)
             as wj from baseall  order by ${k1}, wj"""
    qt_last_value3"""select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ ${k1}, last_value(${k2}) over (partition by ${k1} order by ${k3},${k2}
             rows between unbounded preceding and current row)
             as wj from baseall  order by ${k1}, wj"""
    qt_last_value4"""select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ a, max(d) as wjj from 
             (select t1.k1 as k1, t1.k2 as k2, t1.k3 as k3,
             t1.k4 as k4, t1.k5 as k5,t1.k6 as k6,
             t1.k7 as k7, t1.k8 as k8, t1.k9 as k9,
             t1.k10 as k10, t1.k11 as k11, 
             t1.${k1} as a, t1.${k2} as b, t2.${k2} as c, t2.${k3} as d 
             from baseall t1 join baseall  t2 
             where t1.${k1}=t2.${k1} and t1.${k3}>=t2.${k3}) T 
             group by k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, a, b 
             order by a, wjj"""

    // test_query_row_number
    qt_row_number1"""select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ ${k1}, row_number() over (partition by ${k1} order by ${k3}) 
                    as wj from baseall order by ${k1}, wj"""
    qt_row_number2"""select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ ${k1}, count(k1) over (partition by ${k1} order by ${k3}
                    rows between unbounded preceding and current row)
                    as wj from baseall order by ${k1}, wj"""

    // test error
    test {
        sql("select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ ${k1}, lag(${k2}) over (partition by ${k1} order by ${k3}) from baseall")
        exception ""
    }
    test {
        sql"select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ ${k1}, lag(${k2}, -1, 1) over (partition by ${k1} order by ${k3}) from baseall"
        exception ""
    }
    test {
        sql"select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ ${k1}, lag(${k2}, 1) over (partition by ${k1} order by ${k3}) from baseall"
        exception ""
    }
    test {
        sql"select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ ${k1}, lead(${k2}) over (partition by ${k1} order by ${k3}) from baseall"
        exception ""
    }
    test {
        sql"select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ ${k1}, lead(${k2}, -1, 1) over (partition by ${k1} order by ${k3}) from baseall"
        exception ""
    }
    test {
        sql"select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ ${k1}, lead(${k2}, 1) over (partition by ${k1} order by ${k3}) from baseall"
        exception ""
    }
    qt_window_error1"""select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ ${k1}, first_value(${k2}) over (partition by ${k1}) from baseall order by ${k1}"""
    qt_window_error2"""select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ ${k1}, first_value(${k2}) over (order by ${k3}) from baseall"""
    qt_window_error3"""select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ ${k1}, max(${k2}) over (order by ${k3}) from baseall"""
    test {
        sql"""select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ ${k1}, sum(${k2}) over (partition by ${k1} order by ${k3} rows
            between current row and unbounded preceding) as wj
            from baseall order by ${k1}, wj"""
        exception ""
    }
    test {
        sql"""select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ ${k1}, sum(${k2}) over (partition by ${k1} order by ${k3} rows 
            between 0 preceding and 1 following) as wj 
            from baseall order by ${k1}, wj"""
        exception ""
    }
    test {
        sql"""select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ ${k1}, sum(${k2}) over (partition by ${k1} order by ${k3} rows 
            between unbounded following and current row) as wj 
            from baseall order by ${k1}, wj"""
        exception ""
    }
    test {
        sql"""select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ ${k1}, rank(${k2}) over (partition by ${k1} order by ${k3}) as wj
            from baseall order by ${k1}, wj"""
        exception ""
    }
    test {
        sql"""select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ ${k1}, max() over (partition by ${k1} order by ${k3}) as wj
            from baseall order by ${k1}, wj"""
        exception ""
    }
    test {
        sql"""select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ ${k1}, count(${k2}) over (order by ${k1} rows partition by ${k3}) as wj
            from baseall order by ${k1}, wj"""
        exception ""
    }

    // test_query_rank
    k3 = fields[7]
    qt_rank1"""select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ ${k1}, rank() over (partition by ${k1} order by ${k3}) as wj 
             from baseall order by ${k1}, wj"""
    qt_rank2"""select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ F2.${k1}, (F1.wj - F2.basewj + 1) as wj from
             (select a, c, count(*) as wj from 
             (select t1.k1 as k1, t1.k2 as k2, t1.k3 as k3,
             t1.k4 as k4, t1.k5 as k5,t1.k6 as k6,
             t1.k7 as k7, t1.k8 as k8, t1.k9 as k9,
             t1.k10 as k10, t1.k11 as k11, 
             t1.${k1} as a,  t1.${k3} as c 
             from baseall t1 join baseall  t2 
             where t1.${k1}=t2.${k1} and t1.${k3}>=t2.${k3}) T 
             group by k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, a, c) as F1 join
             (select ${k1}, ${k3}, count(*) as basewj from baseall group by ${k1}, ${k3}) as F2
             where F1.a=F2.${k1} and F1.c = F2.${k3} order by F2.${k1}, wj"""

    //test_hang
    qt_window_hang1"""select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ ${k1}, row_number() over (partition by ${k1} order by ${k3}) as wj from
             baseall order by ${k1}, wj"""
    String line = "("
    String cur
    for (p in range(0, 829)) {
        if (p == 0) {
            cur = "(select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ ${k1}, 1 as wj from baseall order by ${k1}, ${k3} limit 1)".toString()
        }
        else {
            cur = """(select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ ${k1}, ${p+1} as wj from baseall order by ${k1} , ${k3}
                    limit ${p}, 1 ) """.toString()

        }
        if (p < 828) {
            line = line + cur + " union all "
        }
        else {
            line = line + cur + ")"
        }
    }

    sql """ admin set frontend config("remote_fragment_exec_timeout_ms"="300000"); """

    //qt_window_hang2"""select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ A.${k1}, A.wj - B.dyk + 1 as num from
    //    (select ${k1}, wj from ${line} as W1) as A join
    //    (select ${k1}, min(wj) as dyk from ${line} as W2 group by ${k1}) as B
    //    where A.${k1}=B.${k1}  order by A.${k1}, num"""

    //test_hujie
    line = "("
    for (p in range(0, 829)) {
        if (p == 0 ) {
            cur = "(select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ * from baseall order by k1, k6 limit 1)"
        } else {
            cur = "(select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ * from baseall order by k1, k6 limit ${p}, 1)"
        }
        if (p < 828) {
            line = line + cur + " union all "
        } else {
            line = line + cur + ")"
        }
    }
    // qt_hujie1"select T.k1, T.k6 from ${line} as T order by T.k1, T.k6"
    qt_hujie2"select /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ k1, k6 from baseall order by k1, k6"

    // test_bug
    order_qt_window_bug1"""SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ wj FROM (SELECT row_number() over (PARTITION BY k6 ORDER BY k1) AS wj 
            FROM baseall ) AS A where wj = 2"""
    order_qt_window_bug2"""SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=1) */ A.k2 AS a, A.k1 as b, B.k1 as c, B.k2 as d FROM  
           ( SELECT k2, k1, row_number () over (PARTITION BY k2 ORDER BY k3) AS wj  
           FROM baseall ) AS A JOIN ( SELECT k2, k1, row_number () over  
           (PARTITION BY k2 ORDER BY k3) AS wj FROM baseall ) AS B WHERE A.k2=B.k2"""
}

