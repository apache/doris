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
suite("test_qualify_query") {
    sql "create database if not exists qualify_test"
    sql "use qualify_test"
    sql "DROP TABLE IF EXISTS sales"
    sql """
           CREATE TABLE sales (
               year INT,
               country STRING,
               product STRING,
               profit INT
            ) 
            DISTRIBUTED BY HASH(`year`) BUCKETS 1
            PROPERTIES (
            "replication_num" = "1"
            )
        """
    sql """
        INSERT INTO sales VALUES
        (2000,'Finland','Computer',1501),
        (2000,'Finland','Phone',100),
        (2001,'Finland','Phone',10),
        (2000,'India','Calculator',75),
        (2000,'India','Calculator',76),
        (2000,'India','Computer',1201),
        (2000,'USA','Calculator',77),
        (2000,'USA','Computer',1502),
        (2001,'USA','Calculator',50),
        (2001,'USA','Computer',1503),
        (2001,'USA','Computer',1202),
        (2001,'USA','TV',150),
        (2001,'USA','TV',101);
        """

    qt_select_1 "select year + 1 as year, country from sales where year >= 2000 qualify row_number() over (order by year) > 1 order by year,country;"

    qt_select_4 "select year, country, profit, row_number() over (order by year) as rk from (select * from sales) a where year = 2000 qualify rk = 1;"

    qt_select_5 "select year, country, product, profit, row_number() over (partition by year, country order by profit desc) as rk from sales where year = 2000 qualify rk = 1 order by year, country, product, profit;"

    qt_select_6 "select year, country, profit, row_number() over (partition by year, country order by profit desc) as rk from (select * from sales) a where year >= 2000 having profit > 200 qualify rk = 1 order by year, country;"

    qt_select_7 "select year, country, profit from (select year, country, profit from (select year, country, profit, row_number() over (partition by year, country order by profit desc) as rk from (select * from sales) a where year >= 2000 having profit > 200) t where rk = 1) a where year >= 2000 qualify row_number() over (order by profit) = 1;"

    qt_select_8 "select year, country, profit from (select year, country, profit from (select * from sales) a where year >= 2000 having profit > 200 qualify row_number() over (partition by year, country order by profit desc) = 1) a qualify row_number() over (order by profit) = 1;"

    qt_select_9 "select * except(year) replace(profit+1 as profit), row_number() over (order by profit) as rk from sales where year >= 2000 qualify rk = 1;"

    qt_select_10 "select * except(year) replace(profit+1 as profit) from sales where year >= 2000 qualify row_number() over (order by year) > profit;"

    qt_select_12 "select year + 1, if(country = 'USA', 'usa' , country), case when profit < 200 then 200 else profit end as new_profit, row_number() over (partition by year, country order by profit desc) as rk from (select * from sales) a where year >= 2000 having profit > 200 qualify rk = 1 order by new_profit;"

    qt_select_13 "select year + 1, if(country = 'USA', 'usa' , country), case when profit < 200 then 200 else profit end as new_profit from (select * from sales) a where year >= 2000 having profit > 200 qualify row_number() over (partition by year, country order by profit desc)  = 1 order by new_profit;"

    qt_select_14 "select * from sales where year >= 2000 qualify row_number() over (partition by year order by profit desc, country) = 1 order by country,profit;"

    qt_select_15 "select *,row_number() over (partition by year order by profit desc, country) as rk from sales where year >= 2000 qualify rk = 1 order by country,profit;"

    qt_select_16 "select * from sales where year >= 2000 qualify row_number() over (partition by year order by if(profit > 200, profit, profit+200) desc, country) = profit order by country;"

    qt_select_17 "select * from sales where year >= 2000 qualify row_number() over (partition by year order by case when profit > 200 then profit else profit+200 end desc, country) = profit order by country;"

    qt_select_18 "select distinct x.year, x.country, x.product from sales x left join sales y on x.year = y.year left join sales z on x.year = z.year where x.year >= 2000 qualify row_number() over (partition by x.year order by x.profit desc) = x.profit order by year;"

    qt_select_19 "select year, country, profit, row_number() over (order by profit) as rk1, row_number() over (order by country) as rk2 from (select * from sales) a where year >= 2000 qualify rk1 = 1 and rk2 > 2;"

    qt_select_20 "select year, country, profit, row_number() over (order by year) as rk from (select * from sales) a where year >= 2000 qualify rk + 1 > 1 * 100;"

    qt_select_21 "select year, country, profit, row_number() over (order by profit) as rk from (select * from sales) a where year >= 2000 qualify rk in (1,2,3);"

    qt_select_22 "select year, country, profit, row_number() over (order by profit) as rk from (select * from sales) a where year >= 2000 qualify rk = (select 1);"

    qt_select_23 "select year, country, profit, row_number() over (order by year) as rk from (select * from sales) a where year >= 2000 qualify rk = (select max(year) from sales);"

    qt_select_24 "select year+1, country, sum(profit) as total from sales where year >= 2000 and country = 'Finland' group by year,country having sum(profit) > 100 qualify row_number() over (order by year) = 1;"

    qt_select_25 "select year, country, profit from (select * from sales) a where year >= 2000 qualify row_number() over (partition by year, country order by profit desc) = 1 order by year, country, profit;"

    qt_select_26 "select year + 1, country from sales where year >= 2000 and country = 'Finland' group by year,country qualify row_number() over (order by year) > 1;"

    qt_select_27 "select year + 1, country, row_number() over (order by year) as rk from sales where year >= 2000 and country = 'Finland' group by year,country qualify rk > 1;"

    qt_select_28 "select year + 1, country, sum(profit) as total from sales where year >= 2000 group by year,country having sum(profit) > 1700 qualify row_number() over (order by year) = 1;"

    qt_select_29 "select distinct year + 1,country from sales qualify row_number() over (order by profit + 1) = 1;"

    qt_select_30 "select distinct year,country, row_number() over (order by profit + 1) as rk from sales qualify row_number() over (order by profit + 1) = 1;"

    qt_select_31 "select distinct year + 1 as year,country from sales where country = 'Finland' group by year, country qualify row_number() over (order by year) = 1;"

    qt_select_32 "select distinct year,country from sales having sum(profit) > 100 qualify row_number() over (order by year) > 100;"

    qt_select_33 "select distinct year,country,rank() over (order by year) from sales where country = 'USA' having sum(profit) > 100 qualify row_number() over (order by year) > 1;"

    qt_select_34 "select distinct year,country,rank() over (order by year) from sales where country = 'India' having sum(profit) > 100;"

    qt_select_35 "select year + 1, country from sales having profit >= 100 qualify row_number() over (order by profit) = 6;"

    qt_select_36 "select year + 1, country, row_number() over (order by profit) rk from sales having profit >= 100 qualify rk = 6;"
}





