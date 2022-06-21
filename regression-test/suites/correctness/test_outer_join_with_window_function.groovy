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

suite("test_outer_join_with_with_window_function") {
    sql """
        drop table if exists dwd_online_detail;
    """

    sql """
        CREATE TABLE `dwd_online_detail` (
        `logout_time` datetime NOT NULL DEFAULT "9999-12-30 00:00:00",
        `login_time` datetime NOT NULL DEFAULT "9999-12-30 00:00:00",
        `game_code` varchar(50) NOT NULL DEFAULT "-",
        `plat_code` varchar(50) NOT NULL DEFAULT "-",
        `account` varchar(255) NOT NULL DEFAULT "-",
        `playerid` varchar(255) NOT NULL DEFAULT "-",
        `userid` varchar(255) NOT NULL DEFAULT "-",
        `pid_code` varchar(50) NOT NULL DEFAULT "-",
        `gid_code` varchar(50) NOT NULL DEFAULT "-",
        `org_sid` int(11) NOT NULL DEFAULT "0",
        `ct_sid` int(11) NOT NULL DEFAULT "0",
        `next_login_time` datetime NOT NULL DEFAULT "9999-12-30 00:00:00"
        ) ENGINE=OLAP
        DUPLICATE KEY(`logout_time`, `login_time`, `game_code`, `plat_code`, `account`, `playerid`, `userid`)
        PARTITION BY RANGE(`logout_time`)
        (PARTITION p99991230 VALUES [('9999-12-30 00:00:00'), ('9999-12-31 00:00:00')))
        DISTRIBUTED BY HASH(`game_code`, `plat_code`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "colocate_with" = "gp_group"
        );
    """
    
    sql """
        drop table if exists ods_logout;
    """

    sql """
        CREATE TABLE `ods_logout` (
        `day` date NULL COMMENT "",
        `game` varchar(500) NULL COMMENT "",
        `plat` varchar(500) NULL COMMENT "",
        `dt` datetime NULL COMMENT "",
        `time` bigint(20) NULL COMMENT "",
        `sid` int(11) NULL COMMENT "",
        `pid` varchar(500) NULL COMMENT "",
        `gid` varchar(500) NULL COMMENT "",
        `account` varchar(500) NULL COMMENT "",
        `playerid` varchar(500) NULL COMMENT "",
        `prop` varchar(500) NULL COMMENT "",
        `p01` varchar(500) NULL COMMENT "",
        `p02` varchar(500) NULL COMMENT "",
        `p03` varchar(500) NULL COMMENT "",
        `p04` varchar(500) NULL COMMENT "",
        `p05` varchar(500) NULL COMMENT "",
        `p06` varchar(500) NULL COMMENT "",
        `p07` varchar(500) NULL COMMENT "",
        `p08` varchar(500) NULL COMMENT "",
        `p09` varchar(500) NULL COMMENT "",
        `p10` varchar(500) NULL COMMENT "",
        `p11` varchar(500) NULL COMMENT "",
        `p12` varchar(500) NULL COMMENT "",
        `p13` varchar(500) NULL COMMENT "",
        `p14` varchar(500) NULL COMMENT "",
        `p15` varchar(500) NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`day`, `game`, `plat`)
        PARTITION BY RANGE(`day`)
        (PARTITION p201907 VALUES [('2019-07-01'), ('2019-08-01')))
        DISTRIBUTED BY HASH(`game`, `plat`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        drop table if exists dim_account_userid_mapping;
    """

    sql """
        CREATE TABLE `dim_account_userid_mapping` (
        `end_time` datetime NOT NULL DEFAULT "9999-12-30 00:00:00",
        `start_time` datetime NOT NULL DEFAULT "9999-12-30 00:00:00",
        `game_code` varchar(50) NOT NULL,
        `plat_code` varchar(50) NOT NULL,
        `userkey` varchar(255) NOT NULL,
        `userid` varchar(255) NOT NULL,
        `account` varchar(255) NOT NULL,
        `pid_code` varchar(50) NOT NULL DEFAULT "-",
        `gid_code` varchar(50) NOT NULL DEFAULT "-",
        `region` varchar(50) NOT NULL DEFAULT "-"
        ) ENGINE=OLAP
        DUPLICATE KEY(`end_time`, `start_time`, `game_code`, `plat_code`, `userkey`)
        PARTITION BY RANGE(`end_time`)
        (PARTITION p20190705 VALUES [('2019-07-05 00:00:00'), ('2019-07-06 00:00:00')))
        DISTRIBUTED BY HASH(`game_code`, `plat_code`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "colocate_with" = "gp_group"
        );
    """

    sql """
        insert into ods_logout(day, game, plat, playerid, dt) values('2019-07-05', 'mus', '37wan', '1136638398824557', '2019-07-05 00:00:00');
    """

    sql """
        insert into dwd_online_detail(game_code, plat_code, playerid, account, org_sid, ct_sid, login_time, logout_time, pid_code,gid_code)
        values('mus', '37wan', '1577946288488507', '1492704224', '421001', '421001', '2020-01-19 11:15:21', '9999-12-30 00:00:00', '-', '-');
    """

    qt_select_with_order_by """
        SELECT online_detail.game_code, next_login_time
        FROM (
        select dwd_online_detail.game_code, LEAD(dwd_online_detail.login_time, 1, '9999-12-30 00:00:00') OVER () AS next_login_time
        from dim_account_userid_mapping right join dwd_online_detail
        on dim_account_userid_mapping.game_code = dwd_online_detail.game_code
        ) online_detail
        LEFT JOIN (
            SELECT day, game AS game_code, dt
            FROM ods_logout
        ) logout
        ON online_detail.game_code = logout.game_code;
    """
}
