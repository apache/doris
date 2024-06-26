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

suite("test_in_expr", "query") {
    def nullTableName = "in_expr_test_null"
    def notNullTableName = "in_expr_test_not_null"

    sql """DROP TABLE IF EXISTS ${nullTableName}"""
    sql """
            CREATE TABLE IF NOT EXISTS ${nullTableName} (
              `cid` int(11) NULL,
              `number` int(11) NULL,
              `addr` varchar(256) NULL,
              `fnum` float NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`cid`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`cid`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            )
        """
    sql """ insert into ${nullTableName} values(100,1,'a', 1.1),(101,2,'b',1.2),(102,3,'c',1.3),(103,4,'d',1.4),(104,null,'e',1.5),(105,6, null,null) """


    sql """DROP TABLE IF EXISTS ${notNullTableName}"""
    sql """
            CREATE TABLE IF NOT EXISTS ${notNullTableName} (
              `cid` int(11) not NULL,
              `number` int(11) not NULL,
              `addr` varchar(256) not NULL,
              `fnum` float not NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`cid`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`cid`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            )
        """

    sql """ insert into ${notNullTableName} values(100,1,'a', 1.1),(101,2,'b', 1.2),(102,3,'c',1.3),(103,4,'d',1.4) """

    sql """ set enable_vectorized_engine = true """

    // 1 in expr
    // 1.1 nullable
    // 1.1.1 string + set_not_null
    qt_select "select t1.number from ${nullTableName} t1 left join ${nullTableName} t2 on t1.cid=t2.cid where t2.addr in ('d')"

    // 1.1.2 string + null_in_set
    qt_select "select t1.number from ${nullTableName} t1 left join ${nullTableName} t2 on t1.cid=t2.cid where t2.addr in ('d', null)"

    qt_select "select * from ${nullTableName} where addr not in ('d', null)"

    qt_select "select * from ${nullTableName} where not(addr not in ('d', null))"

    qt_select "select * from ${nullTableName} where addr in ('d', null)"

    qt_select "select * from ${nullTableName} where not(addr in ('d', null))"

    qt_select_float_in """ select fnum,  fnum in (1.1, null) from ${nullTableName} order by cid"""
    qt_select_float_in2 """ select fnum,  not (fnum in (1.1, null)) from ${nullTableName} order by cid"""
    qt_select_float_in3 """ select fnum,  fnum not in (1.1, null) from ${nullTableName} order by cid"""
    qt_select_float_in4 """ select fnum,  fnum in (null) from ${nullTableName} order by cid"""
    qt_select_float_in5 """ select fnum,  not(fnum in (null)) from ${nullTableName} order by cid"""
    qt_select_float_in6 """ select fnum,  fnum not in (null) from ${nullTableName} order by cid"""

    // 1.1.3 non-string
    qt_select "select t1.addr from ${nullTableName} t1 left join ${nullTableName} t2 on t1.cid=t2.cid where t2.number in (3)"

    // 1.2 not null
    // 1.2.1 string + set_not_null
    qt_select "select t1.number from ${notNullTableName} t1 left join ${notNullTableName} t2 on t1.cid=t2.cid where t2.addr in ('d')"

    // 1.1.2 string + null_in_set
    qt_select "select t1.number from ${notNullTableName} t1 left join ${notNullTableName} t2 on t1.cid=t2.cid where t2.addr in ('d', null)"

    // 1.1.3 non-string
    qt_select "select t1.addr from ${notNullTableName} t1 left join ${notNullTableName} t2 on t1.cid=t2.cid where t2.number in (3)"




    // 2 not in expr
    // 2.1 nullable
    // 2.1.1 string + set_not_null
    qt_select "select t1.number from ${nullTableName} t1 left join ${nullTableName} t2 on t1.cid=t2.cid where t2.addr not in ('d') order by t1.number"

    // 2.1.2 string + null_in_set
    qt_select "select t1.number from ${nullTableName} t1 left join ${nullTableName} t2 on t1.cid=t2.cid where t2.addr not in ('d', null) "

    // 2.1.3 non-string
    qt_select "select t1.addr from ${nullTableName} t1 left join ${nullTableName} t2 on t1.cid=t2.cid where t2.number not in (3) order by t1.addr "

    // 2.2 not null
    // 2.2.1 string + set_not_null
    qt_select "select t1.number from ${notNullTableName} t1 left join ${notNullTableName} t2 on t1.cid=t2.cid where t2.addr not in ('d') order by t1.number "

    // 2.1.2 string + null_in_set
    qt_select "select t1.number from ${notNullTableName} t1 left join ${notNullTableName} t2 on t1.cid=t2.cid where t2.addr not in ('d', null)"

    // 2.1.3 non-string
    qt_select "select t1.addr from ${notNullTableName} t1 left join ${notNullTableName} t2 on t1.cid=t2.cid where t2.number not in (3) order by t1.addr "

    qt_select_not_null_in_null """ select cid, cid in (100, null) from ${notNullTableName} order by cid"""
    qt_select_not_null_in_null2 """ select cid, cid in (null) from ${notNullTableName} order by cid"""
    qt_select_not_null_in_null3 """ select addr, addr in ('a', null) from ${notNullTableName} order by addr"""
    qt_select_not_null_in_null4 """ select addr, addr in (null) from ${notNullTableName} order by addr"""

    qt_select_not_null_not_in_null """ select cid, cid not in (100, null) from ${notNullTableName} order by cid"""
    qt_select_not_null_not_in_null2 """ select cid, cid not in (null) from ${notNullTableName} order by cid"""
    qt_select_not_null_not_in_null3 """ select addr, addr not in ('a', null) from ${notNullTableName} order by addr"""
    qt_select_not_null_not_in_null4 """ select addr, addr not in (null) from ${notNullTableName} order by addr"""

    qt_select_not_null_float_in """ select fnum,  fnum in (1.1, null) from ${notNullTableName} order by cid"""
    qt_select_not_null_float_in2 """ select fnum,  not (fnum in (1.1, null)) from ${notNullTableName} order by cid"""
    qt_select_not_null_float_in3 """ select fnum,  fnum not in (1.1, null) from ${notNullTableName} order by cid"""
    qt_select_not_null_float_in4 """ select fnum,  fnum in (null) from ${notNullTableName} order by cid"""
    qt_select_not_null_float_in5 """ select fnum,  not(fnum in (null)) from ${notNullTableName} order by cid"""
    qt_select_not_null_float_in6 """ select fnum,  fnum not in (null) from ${notNullTableName} order by cid"""

    sql """DROP TABLE IF EXISTS ${nullTableName}"""
    sql """DROP TABLE IF EXISTS ${notNullTableName}"""

    // from https://github.com/apache/doris/issues/19374
    sql """DROP TABLE IF EXISTS t11"""
    sql """
            CREATE TABLE t11(c0 CHAR(109) NOT NULL) DISTRIBUTED BY HASH (c0) BUCKETS 13 PROPERTIES ("replication_num" = "1");
        """
    sql """
        insert into t11 values ('1'), ('1'), ('2'), ('2'), ('3'), ('4');
        """
    
    qt_select "select t11.c0 from t11 group by t11.c0 having not ('1' in (t11.c0)) order by t11.c0;"
    qt_select "select t11.c0 from t11 group by t11.c0 having ('1' not in (t11.c0)) order by t11.c0;"
}
