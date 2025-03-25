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

suite("test_to_iso8601") {

    def dbName = "test_iso8601"
    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "CREATE DATABASE ${dbName}"
    sql "USE $dbName"
    
    sql """
        CREATE TABLE IF NOT EXISTS `tb` (
            `id` int null comment "",
            
            `k1`  date   null comment "",
            `k2` DATETIME null comment "",
            `k3` DATETIME(1)  null comment "",
            `k4` DATETIME(2) null comment "",
            `k5` DATETIME(3) null comment "",
            `k6` DATETIME(4) null comment "",
            `k7` DATETIME(6) null comment "",

            `k11` date  not null comment "",
            `k22` DATETIME not  null comment "",
            `k33` DATETIME(1) not  null comment "",
            `k44` DATETIME(2) not null comment "",
            `k55` DATETIME(3) not null comment "",
            `k66` DATETIME(4) not null comment "",
            `k77` DATETIME(6) not null comment ""    
        ) engine=olap
        DISTRIBUTED BY HASH(`k1`) BUCKETS 5 properties("replication_num" = "1")
        """

    sql """ insert into tb values (1,        cast( '2023-04-05' as date ),
        cast( '2023-04-05 03:04:05' as DATETIME),           cast( '2023-04-05 03:04:05' as DATETIME(1) ),cast( '2023-04-05 03:04:05' as DATETIME(2) ),cast( '2023-04-05 03:04:05' as DATETIME(3) ),
        cast( '2023-04-05 03:04:05' as DATETIME(4) ),cast( '2023-04-05 03:04:05' as DATETIME(6) ),
        cast( '2023-04-05' as date ),
        cast( '2023-04-05 03:04:05' as DATETIME),           cast( '2023-04-05 03:04:05' as DATETIME(1) ),cast( '2023-04-05 03:04:05' as DATETIME(2) ),cast( '2023-04-05 03:04:05' as DATETIME(3) ),
        cast( '2023-04-05 03:04:05' as DATETIME(4) ),cast( '2023-04-05 03:04:05' as DATETIME(6) )
        );
        """

    sql """
        insert into tb values (2,cast( '2023-04-05' as date ),cast( '2023-04-05 03:04:05' as DATETIME ),cast( '2023-04-05 03:04:05.1' as DATETIME(1) ),cast( '2023-04-05 03:04:05.12' as DATETIME(2) ),
        cast( '2023-04-05 03:04:05.123' as DATETIME(3) ),cast( '2023-04-05 03:04:05.1234' as DATETIME(4) ),cast( '2023-04-05 03:04:05.123456' as DATETIME(6) ),cast( '2023-04-05' as date ),cast( '2023-04-05 03:04:05' as DATETIME ),
        cast( '2023-04-05 03:04:05.1' as DATETIME(1) ),cast( '2023-04-05 03:04:05.12' as DATETIME(2) ),cast( '2023-04-05 03:04:05.123' as DATETIME(3) ),cast( '2023-04-05 03:04:05.1234' as DATETIME(4) ),
        cast( '2023-04-05 03:04:05.123456' as DATETIME(6) )    
        ); """ 

    
    sql  """  
        insert into tb values   (3,cast( '2023-04-05' as date ),cast( '2023-04-05 03:04:05' as DATETIME ),
        cast( '2023-04-05 03:04:05.1' as DATETIME(1) ),cast( '2023-04-05 03:04:05.1' as DATETIME(2) ),cast( '2023-04-05 03:04:05.1' as DATETIME(3) ),
        cast( '2023-04-05 03:04:05.1' as DATETIME(4) ),cast( '2023-04-05 03:04:05.1' as DATETIME(6) ),cast( '2023-04-05' as date ),cast( '2023-04-05 03:04:05' as DATETIME ),
        cast( '2023-04-05 03:04:05.1' as DATETIME(1) ),cast( '2023-04-05 03:04:05.1' as DATETIME(2) ),cast( '2023-04-05 03:04:05.1' as DATETIME(3) ),
        cast( '2023-04-05 03:04:05.1' as DATETIME(4) ),cast( '2023-04-05 03:04:05.1' as DATETIME(6) )    
        );"""
    
    sql """ 
        insert into tb values (4,CAST('0000-01-03' AS DATE),CAST('0000-01-03 00:00:00' AS DATETIME),CAST('0000-01-03 00:00:00' AS DATETIME(1)),CAST('0000-01-03 00:00:00' AS DATETIME(2)),
        CAST('0000-01-03 00:00:00' AS DATETIME(3)),CAST('0000-01-03 00:00:00' AS DATETIME(4)),CAST('0000-01-03 00:00:00' AS DATETIME(6)),CAST('0000-01-03' AS DATE),CAST('0000-01-03 00:00:00' AS DATETIME),
        CAST('0000-01-03 00:00:00' AS DATETIME(1)),CAST('0000-01-03 00:00:00' AS DATETIME(2)),CAST('0000-01-03 00:00:00' AS DATETIME(3)),CAST('0000-01-03 00:00:00' AS DATETIME(4)),CAST('0000-01-03 00:00:00' AS DATETIME(6))
        );""" 
    
    sql """ 
        insert into tb values  (5,CAST('9999-12-31' AS DATE),CAST('9999-12-31 23:59:59' AS DATETIME),CAST('9999-12-31 23:59:59.9' AS DATETIME(1)),
        CAST('9999-12-31 23:59:59.99' AS DATETIME(2)),CAST('9999-12-31 23:59:59.999' AS DATETIME(3)),CAST('9999-12-31 23:59:59.9999' AS DATETIME(4)),
        CAST('9999-12-31 23:59:59.999999' AS DATETIME(6)),CAST('9999-12-31' AS DATE),CAST('9999-12-31 23:59:59' AS DATETIME),CAST('9999-12-31 23:59:59.9' AS DATETIME(1)),
        CAST('9999-12-31 23:59:59.99' AS DATETIME(2)),CAST('9999-12-31 23:59:59.999' AS DATETIME(3)),CAST('9999-12-31 23:59:59.9999' AS DATETIME(4)),CAST('9999-12-31 23:59:59.999999' AS DATETIME(6))
        ); """
    
    sql """  
    insert into tb values  (6,NULL,NULL,NULL,NULL,NULL,NULL,NULL,CAST('9999-12-31' AS DATE),CAST('9999-12-31 23:59:59' AS DATETIME),CAST('9999-12-31 23:59:59.9' AS DATETIME(1)),
    CAST('9999-12-31 23:59:59.99' AS DATETIME(2)),CAST('9999-12-31 23:59:59.999' AS DATETIME(3)),CAST('9999-12-31 23:59:59.9999' AS DATETIME(4)),CAST('9999-12-31 23:59:59.999999' AS DATETIME(6))
    );
    """


    qt_test_1 """select to_iso8601(k1) from tb order by id;"""
    qt_test_2 """select to_iso8601(k2) from tb order by id;"""
    qt_test_3 """select to_iso8601(k3) from tb order by id;"""
    qt_test_4 """select to_iso8601(k4) from tb order by id;"""
    qt_test_5 """select to_iso8601(k5) from tb order by id;"""
    qt_test_6 """select to_iso8601(k6) from tb order by id;"""
    qt_test_7 """select to_iso8601(k7) from tb order by id;"""

    qt_test_8 """select to_iso8601(k11) from tb order by id;"""
    qt_test_9  """select to_iso8601(k22) from tb order by id;"""
    qt_test_10 """select to_iso8601(k33) from tb order by id;"""
    qt_test_11 """select to_iso8601(k44) from tb order by id;"""
    qt_test_12 """select to_iso8601(k55) from tb order by id;"""
    qt_test_13 """select to_iso8601(k66) from tb order by id;"""
    qt_test_14 """select to_iso8601(k77) from tb order by id;"""

    qt_test_7_2 """select to_iso8601(nullable(k7)) from tb order by id;"""
    qt_test_14_2 """select to_iso8601(nullable(k77)) from tb order by id;"""
    qt_test_14_2 """select to_iso8601(NULL) from tb order by id;"""



    sql """ drop table tb """ 



    qt_test_15 """SELECT to_iso8601(CAST('2023-01-03' AS DATE));"""
    qt_test_16 """SELECT to_iso8601(CAST('2023-01-03 00:00:00' AS DATETIME));"""

    qt_test_17 """SELECT to_iso8601(CAST('0000-01-03' AS DATE));"""
    qt_test_18 """SELECT to_iso8601(CAST('0000-01-03 00:00:00' AS DATETIME));"""

    qt_test_19 """SELECT to_iso8601(CAST('0000-12-31' AS DATE));"""
    qt_test_20 """SELECT to_iso8601(CAST('0000-12-31 23:59:59' AS DATETIME));"""

    qt_test_21 """SELECT to_iso8601(CAST('0000-02-28' AS DATE));"""
    qt_test_22 """SELECT to_iso8601(CAST('0000-02-28 00:00:00' AS DATETIME));"""

    qt_test_23 """SELECT to_iso8601(CAST('0000-02-29' AS DATE));"""
    qt_test_24 """SELECT to_iso8601(CAST('0000-02-29 00:00:00' AS DATETIME));"""

    qt_test_25 """SELECT to_iso8601(CAST('1900-02-28' AS DATE));"""
    qt_test_26 """SELECT to_iso8601(CAST('1900-02-28 00:00:00' AS DATETIME));"""

    qt_test_27 """SELECT to_iso8601(CAST('9999-12-31' AS DATE));"""
    qt_test_28 """SELECT to_iso8601(CAST('9999-12-31 23:59:59' AS DATETIME));"""

    qt_test_29 """SELECT to_iso8601(CAST('1970-01-01' AS DATE));"""
    qt_test_30 """SELECT to_iso8601(CAST('1970-01-01 00:00:00' AS DATETIME));"""

    qt_test_31 """ SELECT to_iso8601(nullable(CAST('1970-01-01' AS DATE))); """
    qt_test_32 """ SELECT to_iso8601(nullable(NULL)); """
    qt_test_33 """ SELECT to_iso8601(NULL); """


}