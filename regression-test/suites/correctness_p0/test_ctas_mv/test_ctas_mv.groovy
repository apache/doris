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

suite("test_ctas_mv") {
    sql """
        drop table if exists test_table_t1;
    """

    sql """
        drop table if exists test_table_t2;
    """

    sql """
        drop table if exists szjdf_zjb;
    """

    sql """
        CREATE TABLE test_table_t1 (
        a1 varchar(65533) NULL default '123',
        a2 varchar(64) NULL default '',
        a3 varchar(65533) NULL default '',
        a4 varchar(65533) NULL default '',
        a5 varchar(64) default '2023-01-31',
        a6 varchar(64) default ''
        ) ENGINE = OLAP
        DUPLICATE KEY(a1)
        DISTRIBUTED BY HASH(a1) BUCKETS 3
        PROPERTIES (
        "replication_allocation"="tag.location.default:1",
        "is_being_synced"="false",
        "storage_format"="V2",
        "disable_auto_compaction"="false",
        "enable_single_replica_compaction"="false"
        );
    """

    sql """ 
        CREATE TABLE test_table_t2 (
        a1 varchar(64) NULL default '123'
        ) ENGINE = OLAP
        DUPLICATE KEY(a1)
        DISTRIBUTED BY HASH(a1) BUCKETS AUTO
        PROPERTIES (
        "replication_allocation"="tag.location.default:1",
        "is_being_synced"="false",
        "storage_format"="V2",
        "disable_auto_compaction"="false",
        "enable_single_replica_compaction"="false"
        );
    """

    sql """ insert into test_table_t1 values(); """
    sql """ insert into test_table_t2 values(); """

    createMV(""" CREATE MATERIALIZED VIEW test_table_view As
            select a1,a3,a4,DATE_FORMAT(a5, 'yyyyMMdd') QUERY_TIME,DATE_FORMAT(a6 ,'yyyyMMdd') CREATE_TIME
            from test_table_t1 where DATE_FORMAT(a5, 'yyyyMMdd') =20230131; """)

    sql """ create table szjdf_zjb PROPERTIES("replication_num"="1") as
            select disf.a2, disf.a1, disf.a3, disf.a4,
            substr(date_format(disf.a5,"yyyyMMdd"), 1, 6),
            date_format(disf.a5,"yyyyMMdd"),
            date_format(disf.a6,"yyyyMMdd")
            from
            (
            select * from test_table_t1
            where date_format(a5,"yyyyMMdd") >=20230131 and date_format(a5,"yyyyMMdd") <=20230131
            and a3 is not NULL
            ) disf
            left join
            test_table_t2 cmif
            on disf.a1 = cmif.a1; """

    qt_select """
        select * from szjdf_zjb;
    """

    sql """
        drop table if exists test_table_t1;
    """

    sql """
        drop table if exists test_table_t2;
    """

    sql """
        drop table if exists szjdf_zjb;
    """
}
