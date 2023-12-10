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

suite("cast") {
    def tableName1 ="test"
    def tableName2 ="baseall"

    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    sql """
    drop table if exists test
    """

    sql """
    drop table if exists baseall
    """

    sql """
    CREATE TABLE IF NOT EXISTS `test` (                                                      
        `k0` boolean NULL,                                                        
        `k1` tinyint(4) NULL,                                                     
        `k2` smallint(6) NULL,                                                   
        `k3` int(11) NULL,                                                        
        `k4` bigint(20) NULL,                                                     
        `k5` decimal(9, 3) NULL,                                                  
        `k6` char(5) NULL,                                                        
        `k10` date NULL,                                                          
        `k11` datetime NULL,                                                      
        `k7` varchar(20) NULL,                                                    
        `k8` double MAX NULL,                                                     
        `k9` float SUM NULL,                                                      
        `k12` text REPLACE_IF_NOT_NULL NULL,                                      
        `k13` largeint(40) REPLACE NULL                                           
    ) ENGINE=OLAP                                                               
    AGGREGATE KEY(`k0`, `k1`, `k2`, `k3`, `k4`, `k5`, `k6`, `k10`, `k11`, `k7`) 
    COMMENT 'OLAP'                                                              
    DISTRIBUTED BY HASH(`k1`) BUCKETS 5                                         
    PROPERTIES (                                                                
        "replication_allocation" = "tag.location.default: 1",                       
        "in_memory" = "false",                                                      
        "storage_format" = "V2",                                                    
        "disable_auto_compaction" = "false"                                         
    );
    """

    sql """
    CREATE TABLE IF NOT EXISTS `baseall` (                                                    
        `k0` boolean NULL,                                                        
        `k1` tinyint(4) NULL,                                                     
        `k2` smallint(6) NULL,                                                    
        `k3` int(11) NULL,                                                        
        `k4` bigint(20) NULL,                                                     
        `k5` decimal(9, 3) NULL,                                                  
        `k6` char(5) NULL,                                                        
        `k10` date NULL,                                                          
        `k11` datetime NULL,                                                      
        `k7` varchar(20) NULL,                                                    
        `k8` double MAX NULL,                                                     
        `k9` float SUM NULL,                                                      
        `k12` text REPLACE NULL,                                                  
        `k13` largeint(40) REPLACE NULL                                           
        ) ENGINE=OLAP                                                               
        AGGREGATE KEY(`k0`, `k1`, `k2`, `k3`, `k4`, `k5`, `k6`, `k10`, `k11`, `k7`) 
        COMMENT 'OLAP'                                                              
        DISTRIBUTED BY HASH(`k1`) BUCKETS 5                                         
        PROPERTIES (                                                                
            "replication_allocation" = "tag.location.default: 1",                       
            "in_memory" = "false",                                                      
            "storage_format" = "V2",                                                    
            "disable_auto_compaction" = "false"                                         
        );
    """

    sql """
        insert into test values
        (0, 1, 1989, 1001, 11011902, 123.123, 'true', '1989-03-21', '1989-03-21 13:00:00', 'wangjuoo4', 0.1, 6.333, 'string12345', 170141183460469231731687303715884105727),
        (0, 2, 1986, 1001, 11011903, 1243.500, 'false', '1901-12-31', '1989-03-21 13:00:00', 'wangynnsf', 20.268, 789.25, 'string12345', -170141183460469231731687303715884105727),
        (0, 3, 1989, 1002, 11011905, 24453.325, 'false', '2012-03-14', '2000-01-01 00:00:00', 'yunlj8@nk', 78945.0, 3654.0, 'string12345', 0);
    """

    sql """
        insert into baseall values 
        (1, 10, 1991, 5014, 9223372036854775807, -258.369, 'false', '2015-04-02', '2013-04-02 15:16:52', 'wangynnsf', -123456.54, 0.235, 'string12345', -11011903),
        (1, 12, 32767, -2147483647, 9223372036854775807, 243.325, 'false', '1991-08-11', '2013-04-02 15:16:52', 'lifsno', -564.898, 3.1415927, 'string12345', 1701604692317316873037158),  
        (0, 6, 32767, 3021, 123456, 604587.000, 'true', '2014-11-11', '2015-03-13 12:36:38', 'yanavnd', 0.1, 80699.0, 'string12345', 20220104),    
        (null, null, null, null, null, null, null, null, null, null, null, null, null, null),
        (0, 3, 1989, 1002, 11011905, 24453.325, 'false', '2012-03-14', '2000-01-01 00:00:00', 'yunlj8@nk', 78945.0, 3654.0, 'string12345', 0);
    """

    // order by
    qt_orderBy1 "select k1, k10 from test order by 1, 2 limit 1000"
    qt_orderBy2 "select k1, k8 from test order by 1, 2 desc limit 1000"
    qt_orderBy3 "select k4, k10 from (select k4, k10 from test order by 1, 2 limit 1000000) as i order by 1, 2 limit 1000"
    qt_orderBy4 "select * from test where k1<-1000 order by k1"

    // group
    qt_group1 "select min(k5) from test"
    qt_group2 "select max(k5) from test"
    qt_group3 "select avg(k5) from test"
    qt_group4 "select sum(k5) from test"
    qt_group5 "select count(k5) from test"
    qt_group6 "select min(k5) from test group by k2 order by min(k5)"
    qt_group7 "select max(k5) from test group by k1 order by max(k5)"
    qt_group8 "select avg(k5) from test group by k1 order by avg(k5)"
    qt_group9 "select sum(k5) from test group by k1 order by sum(k5)"
    qt_group10 "select count(k5) from test group by k1 order by count(k5)"
    qt_group11 "select lower(k6), avg(k8), sum(k8),count(k8),  min(k8), max(k8) from test group by lower(k6) order by avg(k8), sum(k8),count(k8),  min(k8), max(k8)" 
    qt_group12 "select k2, avg(k8) from test group by k2 order by k2, avg(k8)" 
    qt_group13 "select k2, sum(k8) from test group by k2 order by k2, sum(k8)" 
    qt_group14 "select k2, count(k8) from test group by k2 order by k2, count(k8)" 
    qt_group15 "select k2, min(k8) from test group by k2 order by k2, min(k8)" 
    qt_group16 "select k2, max(k8) from test group by k2 order by k2, max(k8)" 
    qt_group17 "select k6, avg(k8) from test group by k6 having k6='true' order by k6, avg(k8)" 
    qt_group18 "select k6, sum(k8) from test group by k6 having k6='true' order by k6, sum(k8)" 
    qt_group19 "select k6, count(k8) from test group by k6 having k6='true' order by k6, count(k8)" 
    qt_group20 "select k6, min(k8) from test group by k6 having k6='true' order by k6, min(k8)" 
    qt_group21 "select k6, max(k8) from test group by k6 having k6='true' order by k6, max(k8)" 
    qt_group22 "select k2, avg(k8) from test group by k2 having k2<=1989 order by k2, avg(k8)" 
    qt_group23 "select k2, sum(k8) from test group by k2 having k2<=1989 order by k2, sum(k8)" 
    qt_group24 "select k2, count(k8) from test group by k2 having k2<=1989 order by k2, count(k8)" 
    qt_group25 "select k2, min(k8) from test group by k2 having k2<=1989 order by k2, min(k8)" 
    qt_group26 "select k2, max(k8) from test group by k2 having k2<=1989 order by k2, max(k8)" 
    qt_group27 "select count(ALL *) from test where k5 is not null group by k1%10 order by 1"
    qt_group28 "select k5, k5 * 2, count(*) from test group by 1, 2 order by 1, 2,3"
    qt_group29 "select k1 % 3, k2 % 3, count(*) from test where k4 > 0 group by 2, 1 order by 1, 2, 3"
    qt_group30 "select k1 % 2, k2 % 2, k3 % 3, k4 % 3, k11, count(*) from test where (k11 = '2015-03-13 12:36:38' or k11 = '2000-01-01 00:00:00') and k5 is not null group by 1, 2, 3, 4, 5 order by 1, 2, 3, 4, 5" 
    qt_group31 "select count(*) from test where (k11='2015-03-13 12:36:38' or k11 = '2000-01-01 00:00:00') and k5 is not null group by k1%2, k2%2, k3%3, k4%3, k11%2 order by 1"
    qt_group32 "select count(*), min(k1), max(k1), sum(k1), avg(k1) from test where k1=10000 order by 1"
    qt_group33 "select k1 % 7, count(*), avg(k1) from test where k4 > 0 group by 1 having avg(k1) > 2 or count(*) > 5 order by 1, 2, 3"
    qt_group34 "select k10, count(*) from test where k5 is not null group by k10 having k10 < cast('2010-01-01 01:05:20' as datetime) order by 1, 2"
    qt_group35 "select k1 * k1, k1 + k1 as c from test group by k1 * k1, k1 + k1, k1 * k1 having (c) < 5 order by 1, 2 limit 10"
    qt_group36 "select 1 from (select count(k4) c from test having min(k1) is not null) as t where c is not null"
    qt_group37 "select count(k1), sum(k1 * k2) from test order by 1, 2"
    qt_group38 "select k1 % 2, k2 + 1, k3 from test where k3 > 10000 group by 1,2,3 order by 1,2,3" 
    qt_group39 "select extract(year from k10) as wj, extract(month from k10) as dyk, sum(k1) from test group by 1, 2 order by 1, 2, 3"

    // with having
    qt_group40 "select avg(k1) as a from test group by k2 having a > 10 order by a"
    qt_group41 "select avg(k5) as a from test group by k1 having a > 100 order by a"
    qt_group42 "select sum(k5) as a from test group by k1 having a < 100.0 order by a"
    qt_group43 "select sum(k8) as a from test group by k1 having a > 100 order by a"
    qt_group44 "select avg(k9) as a from test group by k1 having a < 100.0 order by a"

    // order 2
    qt_order8 "select k1, k2 from (select k1, max(k2) as k2 from test where k1 > 0 group by k1 order by k1)a where k1 > 0 and k1 < 10 order by k1"
    qt_order9 "select k1, k2 from (select k1, max(k2) as k2 from test where k1 > 0 group by k1 order by k1)a left join (select k1 as k3, k2 as k4 from baseall) b on a.k1 = b.k3 where k1 > 0 and k1 < 10 order by k1, k2"
    qt_order10 "select k1, count(*) from test group by 1 order by 1 limit 10"
    qt_order11 "select a.k1, b.k1, a.k6 from baseall a join test b on a.k1 = b.k1 where a.k2 > 0 and a.k1 + b.k1 > 20 and b.k6 = 'false' order by a.k1"
    qt_order12 "select k1 from baseall order by k1 % 5, k1"
    qt_order13 "select k1 from (select k1, k2 from baseall order by k1 limit 10) a where k1 > 5 order by k1 limit 10"
    qt_order14 "select k1 from (select k1, k2 from baseall order by k1) a where k1 > 5 order by k1 limit 10"
    qt_order15 "select k1 from (select k1, k2 from baseall order by k1 limit 10 offset 3) a where k1 > 5 order by k1 limit 5 offset 2"
    qt_order16 "select a.k1, a.k2, b.k1 from baseall a join (select * from test where k6 = 'false' order by k1 limit 3 offset 2) b on a.k1 = b.k1 where a.k2 > 0 order by 1"

    qt_orderBy_withNull_1 "select k4 + k5 from test order by 1 nulls first"

    // NULL结果
    qt_orderBy_withNull_2 "select k5, k5 + k6 from test where lower(k6) not like 'na%' and upper(k6) not like 'INF%' order by k5 nulls first"

    // null 和非null
    qt_orderBy_withNull_3 " select a.k1 ak1, b.k1 bk1 from test a right join baseall b on a.k1=b.k1 and b.k1>10 order by ak1 desc nulls first, bk1"

    // NULL列group by
    qt_orderBy_withNull_4 "select k5 + k4 as nu, sum(k1) from test group by nu order by nu nulls first"
    qt_orderBy_withNull_5 "select k6 + k5 as nu from test group by nu"
    qt_orderBy_withNull_6 "select k6 + k5 as nu, sum(1) from test  group by nu order by nu  desc limit 5"
    qt_orderBy_withNull_7 "select k6 + k5 as nu, sum(1) from test  group by nu order by nu limit 5"

    qt_orderBy_withNull_8 "select k4 + k5 as sum, k5 + k6 as nu from test  where lower(k6) not like 'na%' and upper(k6) not like 'INF%' order by sum nulls last"
    qt_orderBy_withNull_9 "select k4 + k5 as nu from test order by nu nulls last"

    //null 和非null
    qt_orderBy_withNull_10 " select a.k1 ak1, b.k1 bk1 from test a right join baseall b on a.k1=b.k1 and b.k1 > 10 order by ak1 nulls last, bk1"

    qt_group31 "select count(*) from test where (k11='2015-03-13 12:36:38' or k11 = '2000-01-01 00:00:00') and k5 is not null group by k1%2, k2%2, k3%3, k4%3, k11%2 order by count(*)"

    test {
        sql "select true + 1 + 'x'"
        exception "string literal 'x' cannot be cast to double"
    }

    qt_sql_test_DecimalV3_mode """select cast(1 as DECIMALV3(1, 0)) % 2.1;""";

    // test cast to time
    qt_tinyint """select cast(k1 as time) ct from test order by ct;"""
    qt_smallint """select cast(k2 as time) ct from test order by ct;"""
    qt_int """select cast(k3 as time) ct from test order by ct;"""
    qt_bigint """select cast(k4 as time) ct from test order by ct;"""
    qt_largeint """select cast(k13 as time) ct from test order by ct;"""
    qt_float """select cast(k9 as time) ct from test order by ct;"""
    qt_double """select cast(k8 as time) ct from test order by ct;"""
    qt_char """select cast(k6 as time) ct from test order by ct;"""
    qt_varchar """select cast(k7 as time) ct from test order by ct;"""
    qt_string """select cast(k12 as time) ct from test order by ct;"""

    qt_tinyint """select cast(cast(1 as tinyint) as time)"""
    qt_smallint """select cast(cast(1 as smallint) as time)"""
    qt_int """select cast(cast(1 as int) as time)"""
    qt_bigint """select cast(cast(1 as bigint) as time)"""
    qt_largeint """select cast(cast(1 as largeint) as time)"""
    qt_float """select cast(cast(0 as float) as time)"""
    qt_double """select cast(cast(0 as double) as time)"""
    qt_char """select cast(cast("1" as char(1)) as time)"""
    qt_varchar """select cast(cast("1" as varchar(1)) as time)"""
    qt_string """select cast(cast("1" as string) as time)"""

    qt_string_to_array "select cast('[1, 2, 3]' as array<int>)"
    qt_string_to_map "select cast('{1:1,2:2,3:3}' as map<int, int>)"
    qt_string_to_struct "select cast('{1,2,3}' as struct<a:int, b:int, c:int>)"

    // boolean
    test {
        sql """select cast(k0 as time) ct from test order by ct;"""
        exception "cannot cast"
    }
    // decimal
    test {
        sql """select cast(k5 as time) ct from test order by ct;"""
        exception "cannot cast"
    }
    // date
    test {
        sql """select cast(k10 as time) ct from test order by ct;"""
        exception "cannot cast"
    }
    // datetime
    test {
        sql """select cast(k11 as time) ct from test order by ct;"""
        exception "cannot cast"
    }
}
