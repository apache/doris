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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite ("multiple_ssb") {
    sql """ DROP TABLE IF EXISTS lineorder_flat; """
    sql """set enable_nereids_planner=true"""
    sql """SET enable_fallback_to_original_planner=false"""

    sql """
        CREATE TABLE IF NOT EXISTS `lineorder_flat` (
        `LO_ORDERDATE` int(11) NOT NULL COMMENT "",
        `LO_ORDERKEY` int(11) NOT NULL COMMENT "",
        `LO_LINENUMBER` tinyint(4) NOT NULL COMMENT "",
        `LO_CUSTKEY` int(11) NOT NULL COMMENT "",
        `LO_PARTKEY` int(11) NOT NULL COMMENT "",
        `LO_SUPPKEY` int(11) NOT NULL COMMENT "",
        `LO_ORDERPRIORITY` varchar(100) NOT NULL COMMENT "",
        `LO_SHIPPRIORITY` tinyint(4) NOT NULL COMMENT "",
        `LO_QUANTITY` tinyint(4) NOT NULL COMMENT "",
        `LO_EXTENDEDPRICE` int(11) NOT NULL COMMENT "",
        `LO_ORDTOTALPRICE` int(11) NOT NULL COMMENT "",
        `LO_DISCOUNT` tinyint(4) NOT NULL COMMENT "",
        `LO_REVENUE` int(11) NOT NULL COMMENT "",
        `LO_SUPPLYCOST` int(11) NOT NULL COMMENT "",
        `LO_TAX` tinyint(4) NOT NULL COMMENT "",
        `LO_COMMITDATE` date NOT NULL COMMENT "",
        `LO_SHIPMODE` varchar(100) NOT NULL COMMENT "",
        `C_NAME` varchar(100) NOT NULL COMMENT "",
        `C_ADDRESS` varchar(100) NOT NULL COMMENT "",
        `C_CITY` varchar(100) NOT NULL COMMENT "",
        `C_NATION` varchar(100) NOT NULL COMMENT "",
        `C_REGION` varchar(100) NOT NULL COMMENT "",
        `C_PHONE` varchar(100) NOT NULL COMMENT "",
        `C_MKTSEGMENT` varchar(100) NOT NULL COMMENT "",
        `S_NAME` varchar(100) NOT NULL COMMENT "",
        `S_ADDRESS` varchar(100) NOT NULL COMMENT "",
        `S_CITY` varchar(100) NOT NULL COMMENT "",
        `S_NATION` varchar(100) NOT NULL COMMENT "",
        `S_REGION` varchar(100) NOT NULL COMMENT "",
        `S_PHONE` varchar(100) NOT NULL COMMENT "",
        `P_NAME` varchar(100) NOT NULL COMMENT "",
        `P_MFGR` varchar(100) NOT NULL COMMENT "",
        `P_CATEGORY` varchar(100) NOT NULL COMMENT "",
        `P_BRAND` varchar(100) NOT NULL COMMENT "",
        `P_COLOR` varchar(100) NOT NULL COMMENT "",
        `P_TYPE` varchar(100) NOT NULL COMMENT "",
        `P_SIZE` tinyint(4) NOT NULL COMMENT "",
        `P_CONTAINER` varchar(100) NOT NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`LO_ORDERDATE`, `LO_ORDERKEY`)
        COMMENT "OLAP"
        PARTITION BY RANGE(`LO_ORDERDATE`)
        (PARTITION p1992 VALUES [("-2147483648"), ("19930101")),
        PARTITION p1993 VALUES [("19930101"), ("19940101")),
        PARTITION p1994 VALUES [("19940101"), ("19950101")),
        PARTITION p1995 VALUES [("19950101"), ("19960101")),
        PARTITION p1996 VALUES [("19960101"), ("19970101")),
        PARTITION p1997 VALUES [("19970101"), ("19980101")),
        PARTITION p1998 VALUES [("19980101"), ("19990101")))
        DISTRIBUTED BY HASH(`LO_ORDERKEY`) BUCKETS 48
        PROPERTIES (
        "replication_num" = "1",
        "colocate_with" = "groupxx1",
        "in_memory" = "false",
        "storage_format" = "DEFAULT"
        );
        """

    sql """INSERT INTO lineorder_flat (LO_ORDERDATE, LO_ORDERKEY, LO_LINENUMBER, LO_CUSTKEY, LO_PARTKEY, LO_SUPPKEY, LO_ORDERPRIORITY, LO_SHIPPRIORITY, LO_QUANTITY, LO_EXTENDEDPRICE, LO_ORDTOTALPRICE, LO_DISCOUNT, LO_REVENUE, LO_SUPPLYCOST, LO_TAX, LO_COMMITDATE, LO_SHIPMODE, C_NAME, C_ADDRESS, C_CITY, C_NATION, C_REGION, C_PHONE, C_MKTSEGMENT, S_NAME, S_ADDRESS, S_CITY, S_NATION, S_REGION, S_PHONE, P_NAME, P_MFGR, P_CATEGORY, P_BRAND, P_COLOR,P_TYPE,P_SIZE,P_CONTAINER) VALUES (19930101 , 1 , 1 , 1 , 1 , 1 , '1' , 1 , 1 , 1 , 1 , 100 , 1 , 1 , 1 , '2023-06-09' , 'shipmode' , 'name' , 'address' , 'city' , 'nation' , 'AMERICA' , 'phone' , 'mktsegment' , 'name' , 'address' , 'city' , 'nation' , 'AMERICA' ,'phone', 'name', 'MFGR#1', 'category', 'brand', 'color', 'type', 4 ,'container');"""

    test {
        sql """create materialized view lineorder_q_1_1 as 
                SELECT LO_ORDERKEY, SUM(LO_EXTENDEDPRICE * LO_DISCOUNT)
                FROM lineorder_flat GROUP BY
                    LO_ORDERKEY, LO_ORDERDATE, LO_DISCOUNT, LO_QUANTITY;"""
        exception "not in select list"
    }

    createMV ("""create materialized view lineorder_q_1_1 as 
                SELECT LO_ORDERKEY, SUM(LO_EXTENDEDPRICE * LO_DISCOUNT) AS revenue
                FROM lineorder_flat
                WHERE
                    LO_ORDERDATE >= 19930101
                    AND LO_ORDERDATE <= 19931231
                    AND LO_DISCOUNT >= 1 AND LO_DISCOUNT <= 3
                    AND LO_QUANTITY < 25
                GROUP BY
                    LO_ORDERKEY;""")

    createMV ("""create materialized view lineorder_q_2_1 as 
                SELECT
                    (LO_ORDERDATE DIV 10000) AS YEAR,
                    P_BRAND,
                    SUM(LO_REVENUE)
                FROM lineorder_flat
                WHERE P_CATEGORY = 'MFGR#12' AND S_REGION = 'AMERICA'
                GROUP BY YEAR, P_BRAND
                ORDER BY YEAR, P_BRAND;""")

    createMV ("""create materialized view lineorder_q_3_1 as 
                SELECT
                    C_NATION,
                    S_NATION, (LO_ORDERDATE DIV 10000) AS YEAR,
                    SUM(LO_REVENUE) AS revenue
                FROM lineorder_flat
                WHERE
                    C_REGION = 'ASIA'
                    AND S_REGION = 'ASIA'
                    AND LO_ORDERDATE >= 19920101
                    AND LO_ORDERDATE <= 19971231
                GROUP BY C_NATION, S_NATION, YEAR;""")

    createMV ("""create materialized view lineorder_q_4_1 as 
                SELECT (LO_ORDERDATE DIV 10000) AS YEAR,
                C_NATION,
                SUM(LO_REVENUE - LO_SUPPLYCOST) AS profit
                FROM lineorder_flat
                WHERE
                C_REGION = 'AMERICA'
                AND S_REGION = 'AMERICA'
                AND P_MFGR IN ('MFGR#1', 'MFGR#2')
                GROUP BY YEAR, C_NATION
                ORDER BY YEAR ASC, C_NATION ASC;""")

    sql """INSERT INTO lineorder_flat (LO_ORDERDATE, LO_ORDERKEY, LO_LINENUMBER, LO_CUSTKEY, LO_PARTKEY, LO_SUPPKEY, LO_ORDERPRIORITY, LO_SHIPPRIORITY, LO_QUANTITY, LO_EXTENDEDPRICE, LO_ORDTOTALPRICE, LO_DISCOUNT, LO_REVENUE, LO_SUPPLYCOST, LO_TAX, LO_COMMITDATE, LO_SHIPMODE,C_NAME,C_ADDRESS,C_CITY,C_NATION,C_REGION,C_PHONE,C_MKTSEGMENT,S_NAME,S_ADDRESS,S_CITY,S_NATION,S_REGION,S_PHONE,P_NAME,P_MFGR,P_CATEGORY,P_BRAND,P_COLOR,P_TYPE,P_SIZE,P_CONTAINER) VALUES (19930101 , 2 , 2 , 2 , 2 , 2 ,'2',2 ,2 ,2 ,2 ,2 ,2 ,2 ,2 ,'2023-06-09','shipmode','name','address','city','nation','region','phone','mktsegment','name','address','city','nation','region','phone','name','mfgr','category','brand','color','type',4,'container');"""

    sql """INSERT INTO lineorder_flat (LO_ORDERDATE, LO_ORDERKEY, LO_LINENUMBER, LO_CUSTKEY, LO_PARTKEY, LO_SUPPKEY, LO_ORDERPRIORITY, LO_SHIPPRIORITY, LO_QUANTITY, LO_EXTENDEDPRICE, LO_ORDTOTALPRICE, LO_DISCOUNT, LO_REVENUE, LO_SUPPLYCOST, LO_TAX, LO_COMMITDATE, LO_SHIPMODE, C_NAME, C_ADDRESS, C_CITY, C_NATION, C_REGION, C_PHONE, C_MKTSEGMENT, S_NAME, S_ADDRESS, S_CITY, S_NATION, S_REGION, S_PHONE, P_NAME, P_MFGR, P_CATEGORY, P_BRAND, P_COLOR,P_TYPE,P_SIZE,P_CONTAINER) VALUES (19930101 , 1 , 1 , 1 , 1 , 1 , '1' , 1 , 1 , 1 , 1 , 100 , 1 , 1 , 1 , '2023-06-09' , 'shipmode' , 'name' , 'address' , 'city' , 'nation' , 'AMERICA' , 'phone' , 'mktsegment' , 'name' , 'address' , 'city' , 'nation' , 'AMERICA' ,'phone', 'name', 'MFGR#12', 'MFGR#12', 'brand', 'color', 'type', 4 ,'container');"""

    sql """INSERT INTO lineorder_flat (LO_ORDERDATE, LO_ORDERKEY, LO_LINENUMBER, LO_CUSTKEY, LO_PARTKEY, LO_SUPPKEY, LO_ORDERPRIORITY, LO_SHIPPRIORITY, LO_QUANTITY, LO_EXTENDEDPRICE, LO_ORDTOTALPRICE, LO_DISCOUNT, LO_REVENUE, LO_SUPPLYCOST, LO_TAX, LO_COMMITDATE, LO_SHIPMODE,C_NAME,C_ADDRESS,C_CITY,C_NATION,C_REGION,C_PHONE,C_MKTSEGMENT,S_NAME,S_ADDRESS,S_CITY,S_NATION,S_REGION,S_PHONE,P_NAME,P_MFGR,P_CATEGORY,P_BRAND,P_COLOR,P_TYPE,P_SIZE,P_CONTAINER) VALUES (19930101 , 2 , 2 , 2 , 2 , 2 ,'2',2 ,2 ,2 ,2 ,2 ,2 ,2 ,2 ,'2023-06-09','shipmode','name','address','city','nation','region','phone','mktsegment','name','address','city','nation','region','phone','name','mfgr','category','brand','color','type',4,'container');"""

    sql """INSERT INTO lineorder_flat (LO_ORDERDATE, LO_ORDERKEY, LO_LINENUMBER, LO_CUSTKEY, LO_PARTKEY, LO_SUPPKEY, LO_ORDERPRIORITY, LO_SHIPPRIORITY, LO_QUANTITY, LO_EXTENDEDPRICE, LO_ORDTOTALPRICE, LO_DISCOUNT, LO_REVENUE, LO_SUPPLYCOST, LO_TAX, LO_COMMITDATE, LO_SHIPMODE, C_NAME, C_ADDRESS, C_CITY, C_NATION, C_REGION, C_PHONE, C_MKTSEGMENT, S_NAME, S_ADDRESS, S_CITY, S_NATION, S_REGION, S_PHONE, P_NAME, P_MFGR, P_CATEGORY, P_BRAND, P_COLOR,P_TYPE,P_SIZE,P_CONTAINER) VALUES (19920101 , 1 , 1 , 1 , 1 , 1 , '1' , 1 , 1 , 1 , 1 , 100 , 1 , 1 , 1 , '2023-06-09' , 'ASIA' , 'ASIA' , 'ASIA' , 'ASIA' , 'ASIA' , 'ASIA' , 'ASIA' , 'ASIA' , 'ASIA' , 'ASIA' , 'ASIA' , 'ASIA' , 'ASIA' ,'ASIA', 'ASIA', 'MFGR#12', 'MFGR#12', 'brand', 'color', 'type', 4 ,'container');"""

    sql """INSERT INTO lineorder_flat (LO_ORDERDATE, LO_ORDERKEY, LO_LINENUMBER, LO_CUSTKEY, LO_PARTKEY, LO_SUPPKEY, LO_ORDERPRIORITY, LO_SHIPPRIORITY, LO_QUANTITY, LO_EXTENDEDPRICE, LO_ORDTOTALPRICE, LO_DISCOUNT, LO_REVENUE, LO_SUPPLYCOST, LO_TAX, LO_COMMITDATE, LO_SHIPMODE,C_NAME,C_ADDRESS,C_CITY,C_NATION,C_REGION,C_PHONE,C_MKTSEGMENT,S_NAME,S_ADDRESS,S_CITY,S_NATION,S_REGION,S_PHONE,P_NAME,P_MFGR,P_CATEGORY,P_BRAND,P_COLOR,P_TYPE,P_SIZE,P_CONTAINER) VALUES (19930101 , 2 , 2 , 2 , 2 , 2 ,'2',2 ,2 ,2 ,2 ,2 ,2 ,2 ,2 ,'2023-06-09','shipmode','name','address','city','nation','region','phone','mktsegment','name','address','city','nation','region','phone','name','mfgr','category','brand','color','type',4,'container');"""

    sql """INSERT INTO lineorder_flat (LO_ORDERDATE, LO_ORDERKEY, LO_LINENUMBER, LO_CUSTKEY, LO_PARTKEY, LO_SUPPKEY, LO_ORDERPRIORITY, LO_SHIPPRIORITY, LO_QUANTITY, LO_EXTENDEDPRICE, LO_ORDTOTALPRICE, LO_DISCOUNT, LO_REVENUE, LO_SUPPLYCOST, LO_TAX, LO_COMMITDATE, LO_SHIPMODE,C_NAME,C_ADDRESS,C_CITY,C_NATION,C_REGION,C_PHONE,C_MKTSEGMENT,S_NAME,S_ADDRESS,S_CITY,S_NATION,S_REGION,S_PHONE,P_NAME,P_MFGR,P_CATEGORY,P_BRAND,P_COLOR,P_TYPE,P_SIZE,P_CONTAINER) VALUES (2 , 2 , 2 , 2 , 2 , 2 ,'2',2 ,2 ,2 ,2 ,2 ,2 ,2 ,2 ,'2023-06-09','shipmode','name','address','city','nation','region','phone','mktsegment','name','address','city','nation','region','phone','name','mfgr','category','brand','color','type',4,'container');"""

    sql """INSERT INTO lineorder_flat (LO_ORDERDATE, LO_ORDERKEY, LO_LINENUMBER, LO_CUSTKEY, LO_PARTKEY, LO_SUPPKEY, LO_ORDERPRIORITY, LO_SHIPPRIORITY, LO_QUANTITY, LO_EXTENDEDPRICE, LO_ORDTOTALPRICE, LO_DISCOUNT, LO_REVENUE, LO_SUPPLYCOST, LO_TAX, LO_COMMITDATE, LO_SHIPMODE, C_NAME, C_ADDRESS, C_CITY, C_NATION, C_REGION, C_PHONE, C_MKTSEGMENT, S_NAME, S_ADDRESS, S_CITY, S_NATION, S_REGION, S_PHONE, P_NAME, P_MFGR, P_CATEGORY, P_BRAND, P_COLOR,P_TYPE,P_SIZE,P_CONTAINER) VALUES (1 , 1 , 1 , 1 , 1 , 1 , '1' , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , '2023-06-09' , 'shipmode' , 'name' , 'address' , 'city' , 'nation' , 'AMERICA' , 'phone' , 'mktsegment' , 'name' , 'address' , 'city' , 'nation' , 'AMERICA' ,'phone', 'name', 'MFGR#1', 'category', 'brand', 'color', 'type', 4 ,'container');"""

    qt_select_star "select * from lineorder_flat order by 1,2;"
    
    explain {
        sql("""SELECT SUM(LO_EXTENDEDPRICE * LO_DISCOUNT) AS revenue
                FROM lineorder_flat
                WHERE
                    LO_ORDERDATE >= 19930101
                    AND LO_ORDERDATE <= 19931231
                    AND LO_DISCOUNT >= 1 AND LO_DISCOUNT <= 3
                    AND LO_QUANTITY < 25;""")
        contains "(lineorder_q_1_1)"
    }
    qt_select_q_1_1 """SELECT SUM(LO_EXTENDEDPRICE * LO_DISCOUNT) AS revenue
                FROM lineorder_flat
                WHERE
                    LO_ORDERDATE >= 19930101
                    AND LO_ORDERDATE <= 19931231
                    AND LO_DISCOUNT >= 1 AND LO_DISCOUNT <= 3
                    AND LO_QUANTITY < 25;"""

    explain {
        sql("""SELECT
                SUM(LO_REVENUE), (LO_ORDERDATE DIV 10000) AS YEAR,
                P_BRAND
            FROM lineorder_flat
            WHERE P_CATEGORY = 'MFGR#12' AND S_REGION = 'AMERICA'
            GROUP BY (LO_ORDERDATE DIV 10000), P_BRAND
            ORDER BY YEAR, P_BRAND;""")
        contains "(lineorder_q_2_1)"
    }
    qt_select_q_2_1 """SELECT
                    SUM(LO_REVENUE), (LO_ORDERDATE DIV 10000) AS YEAR,
                    P_BRAND
                FROM lineorder_flat
                WHERE P_CATEGORY = 'MFGR#12' AND S_REGION = 'AMERICA'
                GROUP BY YEAR, P_BRAND
                ORDER BY YEAR, P_BRAND;"""

    explain {
        sql("""SELECT
                C_NATION,
                S_NATION, (LO_ORDERDATE DIV 10000) AS YEAR,
                SUM(LO_REVENUE) AS revenue
            FROM lineorder_flat
            WHERE
                C_REGION = 'ASIA'
                AND S_REGION = 'ASIA'
                AND LO_ORDERDATE >= 19920101
                AND LO_ORDERDATE <= 19971231
            GROUP BY C_NATION, S_NATION, YEAR
            ORDER BY YEAR ASC, revenue DESC;""")
        contains "(lineorder_q_3_1)"
    }
    qt_select_q_3_1 """SELECT
                        C_NATION,
                        S_NATION, (LO_ORDERDATE DIV 10000) AS YEAR,
                        SUM(LO_REVENUE) AS revenue
                    FROM lineorder_flat
                    WHERE
                        C_REGION = 'ASIA'
                        AND S_REGION = 'ASIA'
                        AND LO_ORDERDATE >= 19920101
                        AND LO_ORDERDATE <= 19971231
                    GROUP BY C_NATION, S_NATION, YEAR
                    ORDER BY YEAR ASC, revenue DESC;"""

    explain {
        sql("""SELECT (LO_ORDERDATE DIV 10000) AS YEAR,
                C_NATION,
                SUM(LO_REVENUE - LO_SUPPLYCOST) AS profit
                FROM lineorder_flat
                WHERE
                C_REGION = 'AMERICA'
                AND S_REGION = 'AMERICA'
                AND P_MFGR IN ('MFGR#1', 'MFGR#2')
                GROUP BY YEAR, C_NATION
                ORDER BY YEAR ASC, C_NATION ASC;""")
        contains "(lineorder_q_4_1)"
    }
    qt_select_q_4_1 """SELECT (LO_ORDERDATE DIV 10000) AS YEAR,
                C_NATION,
                SUM(LO_REVENUE - LO_SUPPLYCOST) AS profit
                FROM lineorder_flat
                WHERE
                C_REGION = 'AMERICA'
                AND S_REGION = 'AMERICA'
                AND P_MFGR IN ('MFGR#1', 'MFGR#2')
                GROUP BY YEAR, C_NATION
                ORDER BY YEAR ASC, C_NATION ASC;"""
}
