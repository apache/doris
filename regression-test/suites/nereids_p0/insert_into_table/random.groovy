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

suite('nereids_insert_random') {
    sql 'use nereids_insert_into_table_test'
    sql 'clean label from nereids_insert_into_table_test'

    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set enable_nereids_dml=true'
    sql 'set enable_strict_consistency_dml=true'

    sql '''insert into dup_t_type_cast_rd
            select id, ktint, ksint, kint, kbint, kdtv2, kdtm, kdbl from src'''
    sql 'sync'
    qt_11 'select * from dup_t_type_cast_rd order by id, kint'

    sql '''insert into dup_t_type_cast_rd with label label_dup_type_cast_cte_rd
            with cte as (select id, ktint, ksint, kint, kbint, kdtv2, kdtm, kdbl from src)
            select * from cte'''
    sql 'sync'
    qt_12 'select * from dup_t_type_cast_rd order by id, kint'

    sql '''insert into dup_t_type_cast_rd partition (p1, p2) with label label_dup_type_cast_rd
            select id, ktint, ksint, kint, kbint, kdtv2, kdtm, kdbl from src where id < 4'''
    sql 'sync'
    qt_13 'select * from dup_t_type_cast_rd order by id, kint'

    sql 'set delete_without_partition=true'
    sql '''delete from dup_t_type_cast_rd where id is not null'''
    sql '''delete from dup_t_type_cast_rd where id is null'''

    sql 'set enable_strict_consistency_dml=true'
    sql 'drop table if exists tbl_1'
    sql 'drop table if exists tbl_4'
    sql """CREATE TABLE tbl_1 (k1 INT, k2 INT) DISTRIBUTED BY HASH(k1) BUCKETS 10 PROPERTIES ( "light_schema_change" = "false", "replication_num" = "1");"""
    sql """INSERT INTO tbl_1 VALUES (1, 11);"""
    sql 'sync'
    sql """CREATE TABLE tbl_4 (k1 INT, k2 INT, v INT SUM) AGGREGATE KEY (k1, k2) DISTRIBUTED BY HASH(k1) BUCKETS 10  PROPERTIES ( "replication_num" = "1"); """ 
    sql """INSERT INTO tbl_4 SELECT k1, k2, k2 FROM tbl_1;"""
    sql 'sync'
    qt_sql_select """ select * from tbl_4; """;


    sql 'drop table if exists tbl_5'
    sql 'drop table if exists tbl_6'
    sql 'drop table if exists tbl_7'

    sql """    
        CREATE TABLE `tbl_5` (
        `orderId` varchar(96) NOT NULL,
        `updated_at` datetime NOT NULL,
        `userLabel` varchar(255) NULL,
        `userTag` variant NULL
        ) ENGINE=OLAP
        duplicate KEY(`orderId`, `updated_at`)
        DISTRIBUTED BY HASH(`orderId`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
     """  
     sql """   
        CREATE TABLE tbl_6 
        (
        order_id VARCHAR(96) NOT NULL,
        updated_at DATETIMEV2  NOT NULL
        ) ENGINE=OLAP
        duplicate KEY(`order_id`)
        DISTRIBUTED BY HASH(`order_id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """ 

        sql """ INSERT INTO `tbl_6` values('601022201389484209', '2024-04-09 20:58:49');""" 

        sql """
        CREATE TABLE tbl_7 
        (
        orderId VARCHAR(96) NOT NULL,
        userLabel VARIANT NULL
        )ENGINE=OLAP
        UNIQUE KEY(`orderId`)
        DISTRIBUTED BY HASH(orderId) BUCKETS AUTO
        PROPERTIES (
        "replication_num" = "1"
        );
        """ 
        sql """INSERT INTO `tbl_7` values('601022201389484209','{\"is_poi_first_order\":0}');""" 

        sql 'sync'
        qt_sql_select2 """ INSERT INTO
                        tbl_5
                        SELECT
                        A.order_id as orderId,
                        A.updated_at,
                        CASE
                                WHEN LOCATE('下单1次', CAST(B.userLabel AS STRING)) > 0
                                OR LOCATE('买买', CAST(B.userLabel AS STRING)) > 0 then '买买'
                                when B.userLabel ["is_poi_first_order"] = 1 then '买买'
                                else '卖卖'
                        end as userLabel,
                        B.userLabel AS `userTag`
                        FROM
                        (
                                select
                                order_id,updated_at
                                from
                                tbl_6
                        ) AS A
                        LEFT JOIN (
                                select
                                orderId,userLabel
                                from
                                tbl_7
                        ) AS B ON A.order_id = B.orderId; """;
        qt_sql_select3 """ select * from tbl_5; """;

}
