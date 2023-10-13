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

suite("test_inlineview_with_project") {
    sql "set enable_nereids_planner=false"
    sql """
        drop table if exists cir_1756_t1;
    """

    sql """
        drop table if exists cir_1756_t2;
    """
    
    sql """
        create table cir_1756_t1 (`date` date not null)
        ENGINE=OLAP
        DISTRIBUTED BY HASH(`date`) BUCKETS 5
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        create table cir_1756_t2 ( `date` date not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(`date`) BUCKETS 5
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        insert into cir_1756_t1 values("2020-02-02");
    """

    sql """
        insert into cir_1756_t2 values("2020-02-02");
    """

    qt_select """
        WITH t0 AS(
            SELECT report.date1 AS date2 FROM(
                SELECT DATE_FORMAT(date, '%Y%m%d') AS date1 FROM cir_1756_t1
            ) report GROUP BY report.date1
            ),
            t3 AS(
                SELECT date_format(date, '%Y%m%d') AS `date3`
                FROM `cir_1756_t2`
            )
        SELECT row_number() OVER(ORDER BY date2)
        FROM(
            SELECT t0.date2 FROM t0 LEFT JOIN t3 ON t0.date2 = t3.date3
        ) tx;
    """

    qt_select2 """
        SELECT count(*) AS count
        FROM (with t0 AS 
            (SELECT report.date AS date,
                max(date)- min(date) imp_price
            FROM 
                (SELECT DATE_FORMAT(date,
                '%Y%m%d') AS date
                FROM cir_1756_t1 ) report
                GROUP BY  date ), t3 AS 
                    (SELECT date_format(date,
                '%Y%m%d') AS `date`
                    FROM cir_1756_t2 )
                    SELECT date 1account_id_num
                    FROM 
                        (SELECT date,
                dense_rank() over(partition by date
                        ORDER BY  date desc) 1account_id_num
                        FROM 
                            (SELECT t0.date
                            FROM t0
                            LEFT JOIN t3
                                ON t0.date=t3.date )t0 )tb
                            WHERE 1account_id_num <= 3 ) t;
    """

    sql """
        drop table if exists cir_1756_t1;
    """

    sql """
        drop table if exists cir_1756_t2;
    """

    sql """
        drop table if exists ods_table1;
    """

    sql """
        drop table if exists ods_table2;
    """

    sql """
        drop table if exists ods_table3;
    """

    sql """
        drop table if exists ods_table4;
    """

    sql """
        CREATE TABLE `ods_table1` (
        `dt` datev2 NOT NULL,
        `id` int(11) NULL,
        `server_id` int(11) NULL,
        `uid` varchar(128) NULL,
        `status` int(11) NULL,
        `price` decimal(8, 2) NULL,
        `pay_time` int(11) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`dt`, `id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        CREATE TABLE `ods_table2` (
        `openid` bigint(20) NULL,
        `attribution` varchar(2048) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`openid`,  `attribution`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`openid`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        CREATE TABLE `ods_table3` (
        `gamesvrid` bigint(20) NULL,
        `region` text NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`gamesvrid`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`gamesvrid`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        CREATE TABLE `ods_table4` (
        `event_name` varchar(64) NULL,
        `serverid` varchar(64) NULL,
        `playerid` varchar(64) NULL,
        `region` varchar(64) NULL,
        `uid` varchar(64) NULL,
        `_json` jsonb NULL,
        `dt` datev2 NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`event_name`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`event_name`) BUCKETS 16
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false"
        );
    """

    qt_select3 """
        with 
        table_orders as (
            select
                a.uid as vopenid,
                a.server_id as gamesvrid,
                a.price,
                case
                when c.region = 'TH' then 
                to_date(from_unixtime(CAST(a.pay_time AS BIGINT) + 7 * 3600))
                else to_date(from_unixtime(CAST(a.pay_time AS BIGINT) - 5 * 3600))
                end as paydate,
                b.attribution,
                c.region
            FROM
                ods_table1 a
                left join ods_table2 b on cast(a.uid as bigint) = cast(b.openid as bigint)
                join ods_table3 c on a.server_id = cast(c.gamesvrid as int)
            where
                a.status = 2
                and a.pay_time >= 1672930800
        ),
        playerregistertotal as (
            SELECT
                cast(jsonb_extract_string(a._json, '\$.created_dt') as datetimev2) as dt,
                a.serverid as gamesvrid,
                b.attribution,
                c.region,
                a.uid as vopenid,
                a.playerid as vroleid
            FROM
                ods_table4 a
                left join ods_table2 b on cast(a.uid as bigint) = cast(b.openid as bigint)
                join ods_table3 c on a.serverid = cast(c.gamesvrid as int)
            WHERE
                event_name = 'dd'
        ),
        tab_join_login as (
            SELECT
                tab_rolereg.vopenid,
                tab_rolereg.注册日期,
                tab_rolereg.gamesvrid,
                tab_rolereg.attribution,
                tab_rolereg.region,
                table_orders.price,
                table_orders.paydate,
                table_orders.gamesvrid as payer_gamesvrid,
                table_orders.attribution as payer_attribution,
                table_orders.region as payer_region
            FROM
                (
                    SELECT
                        gamesvrid,
                        vopenid,
                        case
                        when
                    region
                        = 'TH' then to_date(date_add(dt, INTERVAL +7 HOUR))
                        else to_date(date_add(dt, INTERVAL -5 HOUR)) end as 注册日期,
                        attribution,
                    region
                    FROM
                        (
                            SELECT
                                gamesvrid,
                                vopenid,
                                dt,
                                attribution,
                                region,
                                row_number() over(
                                    partition BY vopenid
                                    ORDER BY
                                        dt ASC
                                ) AS reg_times
                            FROM
                                playerregistertotal
                        ) as tab_rolereg2
                    where
                        tab_rolereg2.reg_times = 1
                ) as tab_rolereg
                left JOIN table_orders ON cast(tab_rolereg.vopenid as bigint)  = cast(table_orders.vopenid as bigint) 
        ),
        table_registerpay AS(
            SELECT
                case when bitand(grouping_id(paydate,payer_gamesvrid,payer_attribution,payer_region),8) = 8 then 'All' else paydate end as paydate,
                SUM(ddd) AS ddd
            FROM
                (
                select
                    paydate,
                    payer_gamesvrid,
                    payer_attribution,
                    payer_region,
                    CASE
                    WHEN 注册日期 is not null
                    and paydate = 注册日期 THEN price
                    ELSE 0 END AS ddd
                from 
                    tab_join_login
                ) cte
            GROUP BY
                CUBE(paydate,
                payer_gamesvrid,
                payer_attribution,
                payer_region)
        )
        select paydate  from table_registerpay;
    """

    sql """
        drop table if exists ods_table1;
    """

    sql """
        drop table if exists ods_table2;
    """

    sql """
        drop table if exists ods_table3;
    """

    sql """
        drop table if exists ods_table4;
    """

    sql """
        drop table if exists cir2824_table;
    """

    sql """
        CREATE TABLE `cir2824_table` (
        `id` BIGINT(20) NULL,
        `create_user` BIGINT(20) NULL,
        `event_content` TEXT NULL,
        `dest_farm_id` BIGINT(20) NULL,
        `weight` DOUBLE NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 48
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "function_column.sequence_type" = "BIGINT",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        drop view if exists cir2824_view;
    """

    sql """
        CREATE VIEW `cir2824_view` COMMENT 'VIEW' AS
        select `ev`.`id` AS `id`,
                CAST(`ev`.`create_user` AS BIGINT) AS `create_user`,
                `ev`.`event_content` AS `event_content`,
                `ev`.`dest_farm_id` AS `dest_farm_id`
        FROM `cir2824_table` ev;
    """

    explain {
        sql("""
            WITH cir2824_temp1 AS( SELECT
                    CASE
                    WHEN dest_farm_id IS NULL
                        AND get_json_string(t.event_content,'\$.destFarmId') != '' THEN
                    0
                    ELSE 1
                    END AS is_trans
                FROM cir2824_view t )
            SELECT 1
            FROM cir2824_temp1;
        """)
    }

    sql """
        drop view if exists cir2824_view;
    """

    sql """
        drop table if exists cir2824_table;
    """

    sql """
        drop table if exists dws_mf_wms_t1;
    """

    sql """
        drop table if exists dws_mf_wms_t2;
    """

    sql """
        drop table if exists dws_mf_wms_t3;
    """

    sql """
        CREATE TABLE `dws_mf_wms_t1` (
        `id` varchar(20) NOT NULL COMMENT '',
        `final_weight` double NULL COMMENT ''
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT ''
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        CREATE TABLE `dws_mf_wms_t2` (
        `plate_id` varchar(32) NULL COMMENT '',
        `entry_time` datetime NULL COMMENT ''
        ) ENGINE=OLAP
        UNIQUE KEY(`plate_id`)
        COMMENT ''
        DISTRIBUTED BY HASH(`plate_id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        CREATE TABLE `dws_mf_wms_t3` (
        `material_id` varchar(50) NULL,
        `out_time` datetime NULL COMMENT ''
        ) ENGINE=OLAP
        UNIQUE KEY(`material_id`)
        COMMENT ' '
        DISTRIBUTED BY HASH(`material_id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """insert into dws_mf_wms_t1 values( '1', 1.0);"""
    sql """insert into dws_mf_wms_t2 values( '1', '2020-02-02 22:22:22');"""
    sql """insert into dws_mf_wms_t3 values( '1', '2020-02-02 22:22:22');"""

    qt_select4 """select cur_final_weight from (
                    SELECT
                        round(`t1`.`final_weight` / 1000 , 2) AS `cur_final_weight`,
                        coalesce(`t5`.`avg_inv_hours`, 0) AS `avg_inv_hours`,
                        coalesce(`t5`.`max_inv_hours`, 0) AS `max_inv_hours`
                    FROM
                        `dws_mf_wms_t1` t1
                    LEFT OUTER JOIN (
                        SELECT
                            round(avg(timestampdiff(SECOND, `t1`.`entry_time`, `t2`.`out_time`)) / 3600.0, 1) AS `avg_inv_hours`,
                            round(max(timestampdiff(SECOND, `t1`.`entry_time`, `t2`.`out_time`)) / 3600.0, 1) AS `max_inv_hours`
                        FROM
                            `dws_mf_wms_t2` t1
                        LEFT OUTER JOIN `dws_mf_wms_t3` t2 ON
                            `t1`.`plate_id` = `t2`.`material_id`) t5 ON
                        1 = 1
                        )res;"""
}
