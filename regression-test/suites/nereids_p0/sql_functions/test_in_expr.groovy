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
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    def nullTableName = "in_expr_test_null"
    def notNullTableName = "in_expr_test_not_null"

    sql """DROP TABLE IF EXISTS ${nullTableName}"""
    sql """
            CREATE TABLE IF NOT EXISTS ${nullTableName} (
              `cid` int(11) NULL,
              `number` int(11) NULL,
              `addr` varchar(256) NULL
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
    sql """ insert into ${nullTableName} values(100,1,'a'),(101,2,'b'),(102,3,'c'),(103,4,'d'),(104,null,'e'),(105,6, null) """


    sql """DROP TABLE IF EXISTS ${notNullTableName}"""
    sql """
            CREATE TABLE IF NOT EXISTS ${notNullTableName} (
              `cid` int(11) not NULL,
              `number` int(11) not NULL,
              `addr` varchar(256) not NULL
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

    sql """ insert into ${notNullTableName} values(100,1,'a'),(101,2,'b'),(102,3,'c'),(103,4,'d') """

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

    sql """DROP TABLE IF EXISTS ${nullTableName}"""
    sql """DROP TABLE IF EXISTS ${notNullTableName}"""

    sql """DROP TABLE IF EXISTS table_with_null"""

    sql """
          CREATE TABLE IF NOT EXISTS table_with_null (
              `id` INT ,
              `c1` INT
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            );
    """

    sql """ insert into table_with_null values(1, null); """

    qt_select """ select 0 in (c1, null) from table_with_null;"""





}
