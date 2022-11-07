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

suite("test_outer_join_with_window_function_datev2") {
    def tableName = "datev2_dwd_online_detail"
    def tableName2 = "datev2_ods_logout"
    def tableName3 = "datev2_dim_account_userid_mapping"
    def tableName4 = "datev2_ods_login"
    sql """
        drop table if exists ${tableName};
    """

    sql """
        CREATE TABLE IF NOT EXISTS `${tableName}` (
        `logout_time` datetimev2 NOT NULL DEFAULT "9999-12-30 00:00:00",
        `login_time` datetimev2 NOT NULL DEFAULT "9999-12-30 00:00:00",
        `game_code` varchar(50) NOT NULL DEFAULT "-",
        `plat_code` varchar(50) NOT NULL DEFAULT "-",
        `account` varchar(255) NOT NULL DEFAULT "-",
        `playerid` varchar(255) NOT NULL DEFAULT "-",
        `userid` varchar(255) NOT NULL DEFAULT "-",
        `pid_code` varchar(50) NOT NULL DEFAULT "-",
        `gid_code` varchar(50) NOT NULL DEFAULT "-",
        `org_sid` int(11) NOT NULL DEFAULT "0",
        `ct_sid` int(11) NOT NULL DEFAULT "0",
        `next_login_time` datetimev2 NOT NULL DEFAULT "9999-12-30 00:00:00"
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
        drop table if exists ${tableName2};
    """

    sql """
        CREATE TABLE IF NOT EXISTS `${tableName2}` (
        `day` datev2 NULL COMMENT "",
        `game` varchar(500) NULL COMMENT "",
        `plat` varchar(500) NULL COMMENT "",
        `dt` datetimev2 NULL COMMENT "",
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
        drop table if exists ${tableName3};
    """

    sql """
        CREATE TABLE IF NOT EXISTS `${tableName3}` (
        `end_time` datetimev2 NOT NULL DEFAULT "9999-12-30 00:00:00",
        `start_time` datetimev2 NOT NULL DEFAULT "9999-12-30 00:00:00",
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
        drop table if exists ${tableName4};
    """

    sql """
        CREATE TABLE IF NOT EXISTS `${tableName4}` (
        `day` datev2 NULL COMMENT "",
        `game` varchar(500) NULL COMMENT "",
        `plat` varchar(500) NULL COMMENT "",
        `dt` datetimev2 NULL COMMENT "",
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
        COMMENT "登录ods"
        PARTITION BY RANGE(`day`)
        (PARTITION p201803 VALUES [('2018-03-01'), ('2018-04-01')),
        PARTITION p201804 VALUES [('2018-04-01'), ('2018-05-01')),
        PARTITION p201805 VALUES [('2018-05-01'), ('2018-06-01')),
        PARTITION p201806 VALUES [('2018-06-01'), ('2018-07-01')),
        PARTITION p201807 VALUES [('2018-07-01'), ('2018-08-01')),
        PARTITION p201808 VALUES [('2018-08-01'), ('2018-09-01')),
        PARTITION p201809 VALUES [('2018-09-01'), ('2018-10-01')),
        PARTITION p201810 VALUES [('2018-10-01'), ('2018-11-01')),
        PARTITION p201811 VALUES [('2018-11-01'), ('2018-12-01')),
        PARTITION p201812 VALUES [('2018-12-01'), ('2019-01-01')),
        PARTITION p201901 VALUES [('2019-01-01'), ('2019-02-01')),
        PARTITION p201902 VALUES [('2019-02-01'), ('2019-03-01')),
        PARTITION p201903 VALUES [('2019-03-01'), ('2019-04-01')),
        PARTITION p201904 VALUES [('2019-04-01'), ('2019-05-01')),
        PARTITION p201905 VALUES [('2019-05-01'), ('2019-06-01')),
        PARTITION p201906 VALUES [('2019-06-01'), ('2019-07-01')),
        PARTITION p201907 VALUES [('2019-07-01'), ('2019-08-01')),
        PARTITION p201908 VALUES [('2019-08-01'), ('2019-09-01')),
        PARTITION p201909 VALUES [('2019-09-01'), ('2019-10-01')),
        PARTITION p201910 VALUES [('2019-10-01'), ('2019-11-01')),
        PARTITION p201911 VALUES [('2019-11-01'), ('2019-12-01')),
        PARTITION p201912 VALUES [('2019-12-01'), ('2020-01-01')),
        PARTITION p202001 VALUES [('2020-01-01'), ('2020-02-01')),
        PARTITION p202002 VALUES [('2020-02-01'), ('2020-03-01')),
        PARTITION p202003 VALUES [('2020-03-01'), ('2020-04-01')),
        PARTITION p202004 VALUES [('2020-04-01'), ('2020-05-01')),
        PARTITION p202005 VALUES [('2020-05-01'), ('2020-06-01')),
        PARTITION p202006 VALUES [('2020-06-01'), ('2020-07-01')),
        PARTITION p202007 VALUES [('2020-07-01'), ('2020-08-01')),
        PARTITION p202008 VALUES [('2020-08-01'), ('2020-09-01')),
        PARTITION p202009 VALUES [('2020-09-01'), ('2020-10-01')),
        PARTITION p202010 VALUES [('2020-10-01'), ('2020-11-01')),
        PARTITION p202011 VALUES [('2020-11-01'), ('2020-12-01')),
        PARTITION p202012 VALUES [('2020-12-01'), ('2021-01-01')),
        PARTITION p202101 VALUES [('2021-01-01'), ('2021-02-01')),
        PARTITION p202102 VALUES [('2021-02-01'), ('2021-03-01')),
        PARTITION p202103 VALUES [('2021-03-01'), ('2021-04-01')),
        PARTITION p202104 VALUES [('2021-04-01'), ('2021-05-01')),
        PARTITION p202105 VALUES [('2021-05-01'), ('2021-06-01')),
        PARTITION p202106 VALUES [('2021-06-01'), ('2021-07-01')),
        PARTITION p202107 VALUES [('2021-07-01'), ('2021-08-01')),
        PARTITION p202108 VALUES [('2021-08-01'), ('2021-09-01')),
        PARTITION p202109 VALUES [('2021-09-01'), ('2021-10-01')),
        PARTITION p202110 VALUES [('2021-10-01'), ('2021-11-01')),
        PARTITION p202111 VALUES [('2021-11-01'), ('2021-12-01')),
        PARTITION p202112 VALUES [('2021-12-01'), ('2022-01-01')),
        PARTITION p202201 VALUES [('2022-01-01'), ('2022-02-01')),
        PARTITION p202202 VALUES [('2022-02-01'), ('2022-03-01')),
        PARTITION p202203 VALUES [('2022-03-01'), ('2022-04-01')),
        PARTITION p202204 VALUES [('2022-04-01'), ('2022-05-01')),
        PARTITION p202205 VALUES [('2022-05-01'), ('2022-06-01')),
        PARTITION p202206 VALUES [('2022-06-01'), ('2022-07-01')),
        PARTITION p202207 VALUES [('2022-07-01'), ('2022-08-01')),
        PARTITION p202208 VALUES [('2022-08-01'), ('2022-09-01')),
        PARTITION p202209 VALUES [('2022-09-01'), ('2022-10-01')))
        DISTRIBUTED BY HASH(`game`, `plat`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "dynamic_partition.enable" = "true",
        "dynamic_partition.time_unit" = "MONTH",
        "dynamic_partition.time_zone" = "Asia/Shanghai",
        "dynamic_partition.start" = "-2147483648",
        "dynamic_partition.end" = "3",
        "dynamic_partition.prefix" = "p",
        "dynamic_partition.replication_allocation" = "tag.location.default: 1",
        "dynamic_partition.buckets" = "4",
        "dynamic_partition.create_history_partition" = "true",
        "dynamic_partition.history_partition_num" = "50",
        "dynamic_partition.hot_partition_num" = "2",
        "dynamic_partition.reserved_history_periods" = "NULL",
        "dynamic_partition.start_day_of_month" = "1",
        "in_memory" = "false",
        "storage_format" = "V2");
    """

    sql """
        insert into ${tableName2}(day, game, plat, playerid, dt) values('2019-07-05', 'abc', 'xyz', '1136638398824557', '2019-07-05 00:00:00');
    """

    sql """
        insert into ${tableName}(game_code, plat_code, playerid, account, org_sid, ct_sid, login_time, logout_time, pid_code,gid_code)
        values('abc', 'xyz', '1577946288488507', '1492704224', '421001', '421001', '2020-01-19 11:15:21', '9999-12-30 00:00:00', '-', '-');
    """

    sql "sync"

    qt_select """
        SELECT online_detail.game_code,online_detail.plat_code,online_detail.playerid,online_detail.account,online_detail.org_sid , online_detail.ct_sid ,
        online_detail.login_time,if(online_detail.logout_time='9999-12-30 00:00:00',coalesce(logout.dt,online_detail.next_login_time),online_detail.logout_time) logout_time ,online_detail.next_login_time,online_detail.userid
        ,online_detail.pid_code,online_detail.gid_code
        from
                (select
                        tmp.game_code,tmp.plat_code,tmp.playerid,tmp.account,tmp.org_sid,tmp.ct_sid,tmp.login_time,tmp.logout_time,
                        LEAD(tmp.login_time,1, '9999-12-30 00:00:00') over (partition by tmp.game_code,tmp.plat_code,tmp.playerid order by tmp.login_time) next_login_time,
                        COALESCE (mp.userid,'-') userid,COALESCE (mp.pid_code,'-') pid_code,COALESCE (mp.gid_code,'-') gid_code
                from
                        (select * from ${tableName3}
                        where   start_time < convert_tz(date_add('2019-07-05 00:00:00',INTERVAL 1 day),'Asia/Shanghai','Asia/Shanghai')
                        and end_time >= convert_tz('2019-07-05 00:00:00','Asia/Shanghai','Asia/Shanghai')
                        and game_code ='abc'  and plat_code='xyz'
                        ) mp
                right join
                    (
                    select *,concat_ws('_',pid_code,gid_code,account) userkey from
                    (select game_code,plat_code,playerid,account,org_sid,ct_sid,login_time,logout_time,pid_code,gid_code
                    from ${tableName} where logout_time='9999-12-30 00:00:00' and game_code='abc' and plat_code ='xyz'
                    union all
                    select game game_code,plat plat_code,playerid,account,sid org_sid,cast(p08 as int) ct_sid,dt login_time,'9999-12-30 00:00:00' logout_time,pid pid_code,gid gid_code
                    from ${tableName4}
                    where game='abc' and `plat` = 'xyz'
                    AND  dt BETWEEN convert_tz('2019-07-05 00:00:00','Asia/Shanghai','Asia/Shanghai')
                        and convert_tz('2019-07-05 23:59:59','Asia/Shanghai','Asia/Shanghai')
                        and day BETWEEN date_sub('2019-07-05',INTERVAL 1 DAY ) and date_add('2019-07-05',INTERVAL 1 DAY )
                    group by 1,2,3,4,5,6,7,8,9,10
                    ) t
                    ) tmp
                    on mp.game_code=tmp.game_code and mp.plat_code = tmp.plat_code and mp.userkey = tmp.userkey
                        and tmp.login_time >= mp.start_time and tmp.login_time < mp.end_time
                ) online_detail
                left JOIN
                        (select  day,game game_code,plat plat_code,playerid, dt
                        from  ${tableName2} dlt
                        where game='abc' and `plat` = 'xyz'
                        and dt BETWEEN convert_tz('2019-07-05 00:00:00','Asia/Shanghai','Asia/Shanghai')
                                and convert_tz('2019-07-05 23:59:59','Asia/Shanghai','Asia/Shanghai')
                                and day BETWEEN date_sub('2019-07-05',INTERVAL 1 DAY ) and date_add('2019-07-05',INTERVAL 1 DAY )
                        group by 1,2,3,4,5
                        ) logout
                on  online_detail.game_code=logout.game_code and  online_detail.plat_code=logout.plat_code
                and online_detail.playerid=logout.playerid
                and logout.dt>online_detail.login_time and logout.dt < online_detail.next_login_time
                union all
                select game_code,plat_code,playerid,account,org_sid,ct_sid,login_time,logout_time,next_login_time,userid,pid_code,gid_code
                from ${tableName}
                where logout_time BETWEEN convert_tz('2019-07-05 00:00:00','Asia/Shanghai','Asia/Shanghai')
                                and convert_tz('2019-07-05 23:59:59','Asia/Shanghai','Asia/Shanghai')
                        and not (game_code='abc' and `plat_code` = 'xyz'  );
    """
}
