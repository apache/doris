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

suite("test_outer_join_with_inline_view") {
    sql """
        drop table if exists ojwiv_t1;
    """

    sql """
        drop table if exists ojwiv_t2;
    """
    
    sql """
        create table if not exists ojwiv_t1(
          k1 int not null,
          v1 int not null
        )
        distributed by hash(k1)
        properties(
          'replication_num' = '1'
        );
    """

    sql """
        create table if not exists ojwiv_t2(
          k1 int not null,
          c1 varchar(255) not null
        )
        distributed by hash(k1)
        properties('replication_num' = '1');
    """

    sql """
        insert into ojwiv_t1 values(1, 1), (2, 2), (3, 3), (4, 4);
    """

    sql """
        insert into ojwiv_t2 values(1, '1'), (2, '2');
    """

    qt_select_with_order_by """
        select * from
          (select * from ojwiv_t1) a
        left outer join
          (select * from ojwiv_t2) b
        on a.k1 = b.k1
        order by a.v1; 
    """

    qt_select_with_agg_in_inline_view """
        select * from
          (select * from ojwiv_t1) a
        left outer join
          (select k1, count(distinct c1) from ojwiv_t2 group by k1) b
        on a.k1 = b.k1
        order by a.v1; 
    """

    sql """
        drop table if exists subquery_table_1;
    """

    sql """
        drop table if exists subquery_table_2;
    """

    sql """
        drop table if exists subquery_table_3;
    """

    sql """
        CREATE TABLE `subquery_table_1` (
        `org_code` varchar(96) NULL 
        ) ENGINE=OLAP
        DUPLICATE KEY(`org_code`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`org_code`) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        CREATE TABLE `subquery_table_2` (
        `SKU_CODE` varchar(96) NULL 
        ) ENGINE=OLAP
        DUPLICATE KEY(`SKU_CODE`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`SKU_CODE`) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        CREATE TABLE `subquery_table_3` (
        `org_code` varchar(96) NULL, 
        `bo_ql_in_advance` decimal(24, 6) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`org_code`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`org_code`) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        insert into subquery_table_1 values('1');
    """

    sql """
        insert into subquery_table_2 values('1');
    """

    sql """
        insert into subquery_table_3 values('1',1);
    """

    qt_select_with_agg_in_inline_view_and_outer_join """
        select
            count(cc.qlnm) as qlnm
        FROM
            subquery_table_1 aa
            left join (
                SELECT
                    `s`.`SKU_CODE` AS `org_code`,
                    coalesce(`t3`.`bo_ql_in_advance`, 0) AS `qlnm`
                FROM
                    `subquery_table_2` s
                inner JOIN 
                    `subquery_table_3` t3 
                            ON `s`.`SKU_CODE` = `t3`.`org_code`
            ) cc on aa.org_code = cc.org_code
        group by
            aa.org_code;
    """

    sql """drop table if exists tableau_trans_wide_day_month_year;"""
    sql """CREATE TABLE
            `tableau_trans_wide_day_month_year` (
                `business_type` varchar(200) NULL 
            ) ENGINE = OLAP 
            DUPLICATE KEY(`business_type`) 
            DISTRIBUTED BY HASH(`business_type`) BUCKETS 15 
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );"""
    sql """INSERT INTO `tableau_trans_wide_day_month_year` VALUES (NULL);"""
    qt_select_with_outerjoin_nullable """ SELECT '2023-10-07' a
                                            FROM 
                                                (SELECT t.business_type
                                                FROM tableau_trans_wide_day_month_year t ) a full
                                            JOIN 
                                                (SELECT t.business_type
                                                FROM tableau_trans_wide_day_month_year t
                                                WHERE false ) c
                                                ON nvl(a.business_type,'0')=nvl(c.business_type,'0'); """
    sql """drop table if exists tableau_trans_wide_day_month_year;"""
}
