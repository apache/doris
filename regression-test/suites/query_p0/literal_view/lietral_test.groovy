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

suite("literal_view_test", "arrow_flight_sql") {

    sql """DROP TABLE IF EXISTS table1"""

    sql """
    CREATE table table1(
            `a` varchar(150) NULL COMMENT "",
            `b` varchar(60) NULL COMMENT ""
    )ENGINE=OLAP
    UNIQUE KEY(`a`, `b`)
    DISTRIBUTED BY HASH(`b`) BUCKETS 1
    PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
    );
    """

    sql """
        INSERT into table1
        values('org1','code1');
    """

    sql """DROP TABLE IF EXISTS table2"""

    sql """
    CREATE table table2(
            `c` varchar(40) NOT NULL COMMENT "c"
    )ENGINE=OLAP
    UNIQUE KEY(`c`)
    DISTRIBUTED BY HASH(`c`) BUCKETS 1
    PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
    );

    """

    sql """DROP TABLE IF EXISTS table3"""

    sql """
    CREATE table table3 (
            `c` varchar(40) NOT NULL COMMENT "c"
    )ENGINE=OLAP
    UNIQUE KEY(`c`)
    DISTRIBUTED BY HASH(`c`) BUCKETS 1
    PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
    );
    """

    sql """DROP VIEW IF EXISTS `test_v`"""

    sql """
    CREATE view `test_v` as
    select t1.b
    from table1 as t1
    left outer JOIN table2 as org ON  t1.a = org.c
    left outer join table3 as doi  on t1.a = doi.c
    ;
    """

    qt_sql """
    SELECT b
    FROM test_v
    WHERE substring('2022-12',6,2)='01';
    """

    sql """DROP TABLE IF EXISTS `test_insert`"""

    sql """
        CREATE TABLE `test_insert` (
            `id` varchar(11) NULL COMMENT '唯一标识',
            `name` varchar(10) NULL COMMENT '采集时间',
            `age` int(11) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT 'test'
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        insert into test_insert values (1,'doris',10),(2,'spark',2),(3,'flink',20);
    """

    order_qt_left """select * 
        from test_insert 
        left join (select 1 as v1) t1 
        on false 
        where t1.v1 is null
    """

    qt_sql1 """
        select id, name
        from (
        select '123' as id,
        '1234' as name,
        age
        from test_insert
        ) a
        where name != '1234';
    """

    test {
        sql "select * from (select null as top) t where top is not null"
        result ([])
    }

    test {
        sql "select * from (select null as top) t where top is null"
        result ([[null]])
    }

    test {
        sql "select * from (select null as top) t where top = 5"
        result ([])
    }
}
