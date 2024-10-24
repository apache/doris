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

suite("test_outer_join_with_null_value") {
    sql """
        drop table if exists outer_table_a;
    """

    sql """
        drop table if exists outer_table_b;
    """
    
    sql """
        create table outer_table_a
        (
            PROJECT_ID VARCHAR(32) not null,
            SO_NO VARCHAR(32) not null,
            ORG_ID VARCHAR(32) not null
        )ENGINE = OLAP
        DUPLICATE KEY(PROJECT_ID)
        DISTRIBUTED BY HASH(PROJECT_ID) BUCKETS 30
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        create table outer_table_b
        (
            PROJECT_ID VARCHAR(32) not null,
            SO_NO VARCHAR(32),
            ORG_ID VARCHAR(32) not null
        )ENGINE = OLAP
        DUPLICATE KEY(PROJECT_ID)
        DISTRIBUTED BY HASH(PROJECT_ID) BUCKETS 30
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        insert into outer_table_a values('1','1','1');
    """

    sql """
        insert into outer_table_b values('1','1','1'),('1',null,'1');
    """

    qt_select1 """
        select
        count(*)
        FROM
        outer_table_b WSA
        LEFT JOIN outer_table_a WBWD ON WBWD.ORG_ID = WSA.ORG_ID
        AND WBWD.PROJECT_ID = WSA.PROJECT_ID
        AND WBWD.SO_NO = WSA.SO_NO;
    """

    qt_select2 """
        select
        count(*)
        FROM
        outer_table_b WSA
        LEFT JOIN outer_table_a WBWD ON WBWD.ORG_ID = WSA.ORG_ID
        AND WBWD.PROJECT_ID = WSA.PROJECT_ID
        AND WBWD.SO_NO = WSA.SO_NO
        AND WBWD.SO_NO >= WSA.SO_NO;
    """

    sql """
        drop table if exists outer_table_a;
    """

    sql """
        drop table if exists outer_table_b;
    """

    sql """drop TABLE IF EXISTS `ISE_xxx_t`;"""
    sql """CREATE TABLE IF NOT EXISTS `ISE_xxx_t` (
            `DATE_CD_PC` datev2 NULL COMMENT "", 
            `AREA_ID` bigint(20) NULL COMMENT "", 
            `CHANNEL_NAME` varchar(50) NULL COMMENT "", 
            `DATE_CD` datetimev2 NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`DATE_CD_PC`,`AREA_ID`,`CHANNEL_NAME`)
        DISTRIBUTED BY HASH(`DATE_CD_PC`, `AREA_ID`, `CHANNEL_NAME`) BUCKETS 16
        PROPERTIES(
            "replication_num" = "1"
        );"""
    sql """insert into ISE_xxx_t(DATE_CD_PC, AREA_ID, CHANNEL_NAME, DATE_CD) values
    ("2023-12-27", 1, "xx", "2023-12-27"), ("2023-12-27", 1, "xx", "2023-12-27"),
    ("2023-12-27", 1, "xx", "2023-12-27"), ("2023-12-27", 1, "xx", "2023-12-27"),
    ("2023-12-27", 1, "xx", "2023-12-27");"""

    sql """drop TABLE IF EXISTS `ISE_xxx_t2`;"""
    sql """CREATE TABLE IF NOT EXISTS `ISE_xxx_t2` (
            `DATE_CD` datev2 NULL COMMENT "", 
            `AREA_ID` int(11) NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`DATE_CD`,`AREA_ID`)
        DISTRIBUTED BY HASH(`DATE_CD`, `AREA_ID`) BUCKETS 6
        PROPERTIES(
            "replication_num" = "1"
        );"""
    sql """insert into ISE_xxx_t2(AREA_ID, DATE_CD) values
    (1, "2023-12-27"), (1, "2023-12-27"),
    (1, "2023-12-27"), (1, "2023-12-27");"""

    sql """drop TABLE IF EXISTS `ISE_xxx_t3`;"""
    sql """CREATE TABLE IF NOT EXISTS `ISE_xxx_t3` (
            `AREA_ID` int(11) NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`AREA_ID`)
        DISTRIBUTED BY HASH(`AREA_ID`) BUCKETS 6
        PROPERTIES(
            "compression" = "LZ4",
            "in_memory" = "false",
            "replication_num" = "1"
        );"""
    sql """insert into ISE_xxx_t3(AREA_ID) values
    (1), (1),(1),
    (1), (1),(1),
    (1), (1),(1);"""

    sql """drop TABLE IF EXISTS `ISE_xxx_t4`;"""
    sql """CREATE TABLE IF NOT EXISTS `ISE_xxx_t4` (
            `AREA_ID` decimalv3(18, 0) NOT NULL , 
            `COMM_LVL3_ID` decimalv3(18, 0) NULL , 
            `AREA_NAME` varchar(400) NULL , 
            `AREA_LEVEL` bigint(20) NULL , 
            `LATN_ID` bigint(20) NULL 
        ) ENGINE=OLAP
        DUPLICATE KEY(`AREA_ID`)
        DISTRIBUTED BY HASH(`AREA_ID`) BUCKETS 4
        PROPERTIES(
            "replication_num" = "1"
        );"""
    sql """insert into ISE_xxx_t4(AREA_ID, AREA_NAME, comm_lvl3_id, AREA_LEVEL, LATN_ID) values
    (1, "xx", 2, 3, 15 ),
    (1, "xx", 3, 3, 15 ),
    (1, "xx", 4, 3, 15 ),
    (1, "xx", 1, 3, 15 ),
    (1, "xx", 32518, 3, 15);"""

    sql """drop TABLE if EXISTS `ISE_xxx_t5`;"""
    sql """CREATE TABLE `ISE_xxx_t5` (
            `date_cd` datev2 NULL COMMENT "", 
            `order_id` varchar(20) NULL COMMENT "", 
            `area_id` decimalv3(18, 0) NULL COMMENT "",
        ) 
        DUPLICATE KEY(`date_cd`,`order_id`,`area_id`)
        DISTRIBUTED BY HASH(`date_cd`, `order_id`, `area_id`) BUCKETS 6
        PROPERTIES(
            "replication_num" = "1"
        );"""
    sql """insert into ISE_xxx_t5(AREA_ID, date_cd) values
    (1, "2023-12-27" ),(1, "2023-12-27" ),(1, "2023-12-27" ),
    (1, "2023-12-27" ),(1, "2023-12-27" ),(1, "2023-12-27" );"""

    sql """drop TABLE IF EXISTS `ISE_xxx_t6`;"""
    sql """CREATE TABLE `ISE_xxx_t6` (
            `DATE_CD` datev2 NULL COMMENT "", 
            `AREA_ID` decimalv3(16, 0) NULL COMMENT "", 
            `CHANNEL_TYPE` varchar(64) NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`DATE_CD`,`AREA_ID`)
        DISTRIBUTED BY HASH(`DATE_CD`, `AREA_ID`) BUCKETS 6
        PROPERTIES(
            "replication_num" = "1"
        );"""
    sql """insert into ISE_xxx_t6(DATE_CD, AREA_ID, CHANNEL_TYPE) values
    ("2023-12-27", 1, "xx"),
    ("2023-12-27", 1, "xx"),
    ("2023-12-27", 1, "xx"),
    ("2023-12-27", 1, "xx"),
    ("2023-12-27", 1, "xx");"""

    sql """drop VIEW if EXISTS `ISE_xxx_t7`;"""
    sql """CREATE VIEW `ISE_xxx_t7` (
            `AREA_ID_LV3`,
            `AREA_ID`,
            `AREA_NAME`,
            `LATN_ID`,
            `AREA_LEVEL`
        ) AS
        SELECT
            `P`.`COMM_LVL3_ID` AS `AREA_ID_LV3`,
            `P`.`AREA_ID` AS `AREA_ID`,
            `P`.`AREA_NAME` AS `AREA_NAME`,
            `P`.`LATN_ID` AS `LATN_ID`,
            `P`.`AREA_LEVEL` AS `AREA_LEVEL`
        FROM
            `ISE_xxx_t4` AS `P`
        WHERE
            `P`.`comm_lvl3_id` IN (2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 32518)
        UNION
        ALL
        SELECT
            NULL AS `AREA_ID_LV3`,
            1 AS `AREA_ID`,
            'xxx' AS `AREA_NAME`,
            NULL AS `LATN_ID`,
            1 AS `AREA_LEVEL`
        FROM
            `ISE_xxx_t4` AS `P`
        WHERE
            `P`.`AREA_ID` = 2;"""

    qt_select_xx """SELECT
                        COUNT(1) AS `m0`
                    FROM
                        (
                            SELECT
                                `t2`.`AREA_ID` AS `d0`
                                
                            FROM
                                (
                                    SELECT
                                        `t1`.`CHANNEL_NAME` AS `CHANNEL_NAME`,
                                        `t1`.`DATE_CD` AS `DATE_CD`,
                                        `t1`.`DATE_CD_PC` AS `DATE_CD_PC`,
                                        `t1`.`AREA_ID` AS `AREA_ID`
                                    FROM
                                        `ISE_xxx_t` AS `t1`
                                    WHERE
                                        (
                                            (`t1`.`CHANNEL_NAME` IN ('xx'))
                                            AND (
                                                (`t1`.`DATE_CD_PC` >= DATE('2023-12-27'))
                                                AND (`t1`.`DATE_CD_PC` < DATE('2023-12-28'))
                                            )
                                        )
                                ) AS `t1`
                                LEFT JOIN `ISE_xxx_t7` AS `t2` ON (`t1`.`AREA_ID` <=> `t2`.`AREA_ID`)
                                LEFT JOIN `ISE_xxx_t3` AS `t3` ON (`t1`.`AREA_ID` <=> `t3`.`AREA_ID`)
                                LEFT JOIN (
                                    SELECT
                                        `t4`.`DATE_CD` AS `DATE_CD`,
                                        `t4`.`AREA_ID` AS `AREA_ID`
                                    FROM
                                        `ISE_xxx_t2` AS `t4`
                                    WHERE
                                        (
                                            (`t4`.`DATE_CD` >= DATE('2023-12-27'))
                                            AND (`t4`.`DATE_CD` < DATE('2023-12-28'))
                                        )
                                ) AS `t4` ON (
                                    (`t1`.`AREA_ID` <=> `t4`.`AREA_ID`)
                                    AND (`t1`.`DATE_CD_PC` <=> `t4`.`DATE_CD`)
                                )
                                LEFT JOIN `ISE_xxx_t5` AS `t5` ON (
                                    (`t1`.`AREA_ID` <=> `t5`.`area_id`)
                                    AND (
                                        `t1`.`DATE_CD` <=> (`t5`.`date_cd` + INTERVAL 0 SECOND)
                                    )
                                )
                                LEFT JOIN (
                                    SELECT
                                        `t6`.`CHANNEL_TYPE` AS `CHANNEL_TYPE`,
                                        `t6`.`AREA_ID` AS `AREA_ID`,
                                        `t6`.`DATE_CD` AS `DATE_CD`
                                    FROM
                                        `ISE_xxx_t6` AS `t6`
                                    WHERE
                                        (
                                            (
                                                (`t6`.`DATE_CD` >= DATE('2023-12-27'))
                                                AND (`t6`.`DATE_CD` < DATE('2023-12-28'))
                                            )
                                            AND (`t6`.`CHANNEL_TYPE` IN ('xx'))
                                        )
                                ) AS `t6` ON (
                                    (`t1`.`DATE_CD_PC` <=> `t6`.`DATE_CD`)
                                    AND (`t1`.`AREA_ID` <=> `t6`.`AREA_ID`)
                                    AND (`t1`.`CHANNEL_NAME` <=> `t6`.`CHANNEL_TYPE`)
                                )
                            WHERE
                                (
                                    (`t2`.`AREA_LEVEL` IN (3))
                                    AND (`t2`.`LATN_ID` IN (15))
                                )
                            GROUP BY
                                `t2`.`AREA_ID`
                                
                        ) AS `T_COUNT_`;"""

}
