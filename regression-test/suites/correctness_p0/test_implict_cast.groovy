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

suite("test_implicit_cast") {
    sql """
        drop table if exists cast_test_table;
    """
    
    sql """
        create table cast_test_table ( a decimal(18, 3) not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        insert into cast_test_table values( 1 );
    """

    qt_select """
        select
        round(
            sum(
            ifnull(
                case
                when a = 0 then a
                else 0
                end,
                0
            )
            ),
            2
        ) as a
        from
        cast_test_table;
    """

    sql """
        drop table if exists cast_test_table;
    """

    sql """drop table if exists test_orders_t"""
    sql """drop table if exists test_servers_t"""

    sql """CREATE TABLE `test_orders_t` (
            `a1` date NOT NULL,
            `a2` varchar(50) NOT NULL,
            `a3` int(11) NULL,
            `a4` int(11) NULL,
            `a5` varchar(128) NULL,
            `a6` int(11) NULL,
            `a7` varchar(50) NULL,
            `a8` DECIMAL(8, 2) NULL,
            `a9` int(11) NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`a1`, `a2`, `a3`)
            DISTRIBUTED BY HASH(`a2`) BUCKETS 2
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );"""

    sql """CREATE TABLE `test_servers_t` (
            `b1` bigint(20) NULL,
            `b2` text NULL,
            `b3` text NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`b1`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`b1`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            ); """

    sql """SELECT  a5 AS account_id
                ,a4 AS a4
                ,CASE WHEN a7 = 'MC' THEN a8*0.034
                        ELSE a8 END AS total_top_up
                ,from_unixtime(cast(a9 AS BIGINT) - 5*3600) AS pay_date
            FROM test_orders_t
            WHERE a6 IN ( 2 , 5)
            AND a4 IN ( SELECT b1 FROM test_servers_t WHERE b2 = 'yes' AND (b1 = 22101 or b3 = 'UTC-5'))
            AND a9 >= 1672930800
            AND a1 = current_date();"""

    sql """drop table if exists test_orders_t"""
    sql """drop table if exists test_servers_t"""
}
