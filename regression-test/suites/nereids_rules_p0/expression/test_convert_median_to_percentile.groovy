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

suite("test_convert_median_to_percentile") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "create database if not exists test_convert_median_to_percentile"
    sql "use test_convert_median_to_percentile"

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

    def sql1 = "select median(profit) from sales"
    def sql2 = "select percentile(profit, 0.5) from sales"
    def explainStr1 = sql """ explain  ${sql1} """
    assertTrue(explainStr1.toString().contains("percentile(profit, 0.5)"))
    qt_select_1 "${sql1}"
    qt_select_2 "${sql2}"

    def sql3 = "select year, median(profit) from sales group by year order by year"
    def sql4 = "select year, percentile(profit, 0.5) from sales group by year order by year"
    def explainStr3 = sql """ explain  ${sql3} """
    assertTrue(explainStr3.toString().contains("percentile(profit"))
    qt_select_3 "${sql3}"
    qt_select_4 "${sql4}"

    def sql5 = "select year, median(profit) from sales group by year having median(profit) > 100"
    def sql6 = "select year, percentile(profit, 0.5) from sales group by year having percentile(profit, 0.5) > 100"
    def explainStr5 = sql """ explain  ${sql5} """
    assertTrue(explainStr5.toString().contains("percentile(profit"))
    qt_select_5 "${sql5}"
    qt_select_6 "${sql6}"

    sql "DROP TABLE if exists sales"
    sql "DROP DATABASE if exists test_convert_median_to_percentile"
}
