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

suite("test_window_function", "query") {
    sql """ SET enable_vectorized_engine = TRUE; """

    def windowFunctionTable1 = "test_window_function1"
    sql """ DROP TABLE IF EXISTS ${windowFunctionTable1} """
    sql """ create table ${windowFunctionTable1} (stock_symbol varchar(64), closing_price decimal(8,2), closing_date datetime not null) duplicate key (stock_symbol) distributed by hash (stock_symbol) PROPERTIES("replication_num" = "1") """

    sql """ INSERT INTO ${windowFunctionTable1} VALUES ('JDR',12.86,'2014-10-02 00:00:00'),('JDR',12.89,'2014-10-03 00:00:00'),('JDR',12.94,'2014-10-04 00:00:00'),('JDR',12.55,'2014-10-05 00:00:00'),('JDR',14.03,'2014-10-06 00:00:00'),('JDR',14.75,'2014-10-07 00:00:00'),('JDR',13.98,'2014-10-08 00:00:00') """

    qt_sql """
            SELECT
                stock_symbol,
                closing_date,
                closing_price,
                avg( closing_price ) over ( PARTITION BY stock_symbol ORDER BY closing_date rows BETWEEN 1 preceding AND 1 following ) AS moving_average 
            FROM
                ${windowFunctionTable1}   
           """
    // LEAD
    qt_sql """ 
             SELECT
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
           select t1.new_time from (select closing_date, lead(closing_date, 1, '2014-10-02 00:00:00') over () as new_time from ${windowFunctionTable1}) as t1 left join ${windowFunctionTable1} t2 on t2.closing_date = t1.closing_date;
           """

    // LAG
    qt_sql """ 
             SELECT
                stock_symbol,
                closing_date,
                closing_price,
                lag( closing_price, 1, 0 ) over ( PARTITION BY stock_symbol ORDER BY closing_date ) AS "yesterday closing" 
             FROM
                ${windowFunctionTable1}  
             ORDER BY
                closing_date;
            """
    sql """ drop table   ${windowFunctionTable1} """


    def windowFunctionTable2 = "test_window_function2"
    sql """ DROP TABLE IF EXISTS ${windowFunctionTable2} """
    sql """ create table ${windowFunctionTable2} (x int, property  varchar(64)) duplicate key (x) distributed by hash (x) PROPERTIES("replication_num" = "1") """
    sql """ insert into  ${windowFunctionTable2} values (2,'even'),(4,'even'),(6,'even'),(8,'even'),(10,'even'),(1,'odd'),(3,'odd'),(5,'odd'),(7,'odd'),(9,'odd'); """

    // SUM
    qt_sql """
               SELECT
                    x,
                    property,
                    sum( x ) over ( PARTITION BY property ORDER BY x rows BETWEEN 1 preceding AND 1 following ) AS 'moving total' 
                FROM
                    ${windowFunctionTable2} 
                WHERE
                    property IN ( 'odd', 'even' );
           """
    // AVG
    qt_sql """
               SELECT
                    x,
                    property,
                    avg( x ) over ( PARTITION BY property ORDER BY x rows BETWEEN 1 preceding AND 1 following ) AS 'moving average' 
               FROM
                    ${windowFunctionTable2} 
               WHERE
                    property IN ( 'odd', 'even' );
           """
    // COUNT
    qt_sql """
               SELECT
                    x,
                    property,
                    count( x ) over ( PARTITION BY property ORDER BY x rows BETWEEN unbounded preceding AND current ROW ) AS 'cumulative total' 
               FROM
                    ${windowFunctionTable2}  
               WHERE
                    property IN ( 'odd', 'even' );
           """
    sql """ truncate table ${windowFunctionTable2} """
    sql """ insert into  ${windowFunctionTable2} values (2,'even'),(4,'even'),(6,'even'),(8,'even'),(10,'even'),(1,'odd'),(3,'odd'),(5,'odd'),(7,'odd'),(9,'odd'); """

    // MIN
    qt_sql """ 
              SELECT
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
              SELECT
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
    sql """ create table ${windowFunctionTable3} (x int, y  int) duplicate key (x) distributed by hash (x) PROPERTIES("replication_num" = "1") """
    sql """ insert into  ${windowFunctionTable3} values (1,1),(1,2),(1,2),(2,1),(2,2),(2,3),(3,1),(3,1),(3,2); """

    // RANK
    qt_sql """ select x, y, rank() over(partition by x order by y) as rank from ${windowFunctionTable3} ; """
    // DENSE_RANK
    qt_sql """ select x, y, dense_rank() over(partition by x order by y) as rank from ${windowFunctionTable3} ; """
    // ROW_NUMBER
    qt_sql """ select x, y, row_number() over(partition by x order by y) as rank from ${windowFunctionTable3} ; """

    sql """ drop table   ${windowFunctionTable3}  """


    def windowFunctionTable4 = "test_window_function4"
    sql """ DROP TABLE IF EXISTS ${windowFunctionTable4} """
    sql """ create table ${windowFunctionTable4} (name varchar(64),country varchar(64),greeting varchar(64)) duplicate key (name) distributed by hash (name) PROPERTIES("replication_num" = "1") """
    sql """ insert into ${windowFunctionTable4} VALUES ('Pete','USA','Hello'),('John','USA','Hi'),('Boris','Germany','Guten tag'),('Michael','Germany','Guten morgen'),('Bjorn','Sweden','Hej'),('Mats','Sweden','Tja')"""

    // first_value
    qt_sql """ select country, name,first_value(greeting) over (partition by country order by name, greeting) as greeting from ${windowFunctionTable4}; """
    // last_value
    qt_sql """ select country, name,last_value(greeting)  over (partition by country order by name, greeting) as greeting from ${windowFunctionTable4} ; """

    sql """ drop table   ${windowFunctionTable4}  """
}

