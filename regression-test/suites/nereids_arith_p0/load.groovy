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

suite("load") {

    // ddl begin
    sql "drop table if exists expr_test"
    sql "drop table if exists expr_test_not_nullable"

    sql  "ADMIN SET FRONTEND CONFIG ('disable_decimalv2' = 'false')"

    sql """
        CREATE TABLE IF NOT EXISTS `expr_test` (
            `id` int null,
            `kbool` boolean null,
            `ktint` tinyint(4) null,
            `ksint` smallint(6) null,
            `kint` int(11) null,
            `kbint` bigint(20) null,
            `klint` largeint(40) null,
            `kfloat` float null,
            `kdbl` double null,
            `kdcml` decimalv2(9, 3) null,
            `kchr` char(10) null,
            `kvchr` varchar(10) null,
            `kstr` string null,
            `kdt` date null,
            `kdtv2` datev2 null,
            `kdtm` datetime null,
            `kdtmv2` datetimev2(0) null,
            `kdcml32v3` decimalv3(7, 3) null,
            `kdcml64v3` decimalv3(10, 5) null,
            `kdcml128v3` decimalv3(20, 8) null
        ) engine=olap
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        properties("replication_num" = "1")
    """

    sql """
        CREATE TABLE IF NOT EXISTS `expr_test_not_nullable` (
            `id` int not null,
            `kbool` boolean not null,
            `ktint` tinyint(4) not null,
            `ksint` smallint(6) not null,
            `kint` int(11) not null,
            `kbint` bigint(20) not null,
            `klint` largeint(40) not null,
            `kfloat` float not null,
            `kdbl` double not null,
            `kdcml` decimalv2(9, 3) not null,
            `kchr` char(10) not null,
            `kvchr` varchar(10) not null,
            `kstr` string not null,
            `kdt` date not null,
            `kdtv2` datev2 not null,
            `kdtm` datetime not null,
            `kdtmv2` datetimev2(0) not null,
            `kdcml32v3` decimalv3(7, 3) not null,
            `kdcml64v3` decimalv3(10, 5) not null,
            `kdcml128v3` decimalv3(20, 8) not null
        ) engine=olap
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        properties("replication_num" = "1")
    """
    // ddl end

    streamLoad {
        table "expr_test"
        db "regression_test_nereids_arith_p0"
        set 'column_separator', ';'
        set 'columns', '''
            id, kbool, ktint, ksint, kint, kbint, klint, kfloat, kdbl, kdcml, kchr, kvchr, kstr,
            kdt, kdtv2, kdtm, kdtmv2, kdcml32v3, kdcml64v3, kdcml128v3
            '''
        file "expr_test.dat"
    }

    sql """
        insert into expr_test_not_nullable select * from expr_test where id is not null
    """

    sql "set enable_decimal256=true;"
    sql """
        CREATE TABLE IF NOT EXISTS `expr_test2` (
            `id` int null,
            `kipv4` ipv4 null,
            `kipv6` ipv6 null,
            `kdcml256v3` decimalv3(76, 66) null
        ) engine=olap
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        properties("replication_num" = "1")
    """

    sql """
        CREATE TABLE IF NOT EXISTS `expr_test_not_nullable2` (
            `id` int null,
            `kipv4` ipv4 not null,
            `kipv6` ipv6 not null,
            `kdcml256v3` decimalv3(76, 66) not null
        ) engine=olap
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        properties("replication_num" = "1")
    """

    sql"""
    insert into expr_test2 values(1,"192.168.1.1","ffff:0000:0000:0000:0000:0000:0000:0001",1.1),(2,"192.168.1.2","ffff:0000:0000:0000:0000:0000:0000:0002",2.2),(3,"192.168.1.3","ffff:0000:0000:0000:0000:0000:0000:0003",3.3),(4,"192.168.1.4","ffff:0000:0000:0000:0000:0000:0000:0004",4.4),(5,"192.168.1.5","ffff:0000:0000:0000:0000:0000:0000:0005",5.5)
    """

    sql"""
    insert into expr_test2 values(1,"192.168.11.1","ffff:0000:0000:0000:0000:0000:0100:0001",11.1),(2,"192.168.11.2","ffff:0000:0000:0000:0000:0000:0010:0002",22.2),(3,"192.168.11.3","ffff:0000:0000:0000:0000:0000:0001:0003",33.3),(4,"192.168.11.4","ffff:0000:0000:0000:0000:0100:0000:0004",44.4),(5,"192.168.11.5","ffff:0000:0000:0000:0000:0000:0100:0005",55.5)
    """

    sql"""
    insert into expr_test2 values(1,null,null,null),(2,null,null,null),(3,null,null,null),(4,"192.168.1.4","ffff:0000:0000:0000:0000:0000:0000:0004",null),(5,"192.168.1.5","ffff:0000:0000:0000:0000:0000:0000:0005",null)
    """

    sql"""
    insert into expr_test_not_nullable2 values(1,"192.168.11.1","ffff:0000:0000:0000:0000:0000:0100:0001",11.1),(2,"192.168.11.2","ffff:0000:0000:0000:0000:0000:0010:0002",22.2),(3,"192.168.11.3","ffff:0000:0000:0000:0000:0000:0001:0003",33.3),(4,"192.168.11.4","ffff:0000:0000:0000:0000:0100:0000:0004",44.4),(5,"192.168.11.5","ffff:0000:0000:0000:0000:0000:0100:0005",55.5)
    """

    sql"""
    insert into expr_test_not_nullable2 values(1,"192.168.11.1","ffff:0000:0000:0000:0000:0000:0100:0001",32.1),(2,"192.168.11.2","ffff:0000:0000:0000:0000:0000:0010:0002",21.3),(3,"192.168.11.3","ffff:0000:0000:0000:0000:0000:0001:0003",12.4),(4,"192.168.11.4","ffff:0000:0000:0000:0000:0100:0000:0004",5.5),(5,"192.168.11.5","ffff:0000:0000:0000:0000:0000:0100:0005",6.7)
    """
}