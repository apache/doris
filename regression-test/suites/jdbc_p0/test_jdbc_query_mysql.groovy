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

suite("test_jdbc_query_mysql", "p0") {

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_57_port = context.config.otherConfigs.get("mysql_57_port")
        String jdbcResourceMysql57 = "jdbc_resource_mysql_57"
        String jdbcMysql57Table1 = "jdbc_mysql_57_table1"
        String exMysqlTable = "doris_ex_tb";
        String exMysqlTable1 = "doris_ex_tb1";
        String exMysqlTable2 = "doris_ex_tb2";
        String inDorisTable = "doris_in_tb";
        String inDorisTable1 = "doris_in_tb1";
        String inDorisTable2 = "doris_in_tb2";

        sql """drop resource if exists $jdbcResourceMysql57;"""
        sql """
            create external resource $jdbcResourceMysql57
            properties (
                "type"="jdbc",
                "user"="root",
                "password"="123456",
                "jdbc_url"="jdbc:mysql://127.0.0.1:$mysql_57_port/doris_test",
                "driver_url"="https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/mysql-connector-java-8.0.25.jar",
                "driver_class"="com.mysql.cj.jdbc.Driver"
            );
            """

        sql """drop table if exists $jdbcMysql57Table1"""
        sql """
            CREATE EXTERNAL TABLE `$jdbcMysql57Table1` (
                k1 boolean,
                k2 char(100),
                k3 varchar(128),
                k4 date,
                k5 float,
                k6 tinyint,
                k7 smallint,
                k8 int,
                k9 bigint,
                k10 double,
                k11 datetime,
                k12 decimal(10, 3)
            ) ENGINE=JDBC
            PROPERTIES (
            "resource" = "$jdbcResourceMysql57",
            "table" = "test1",
            "table_type"="mysql"
            );
            """
        order_qt_sql1 """select count(*) from $jdbcMysql57Table1"""
        order_qt_sql2 """select * from $jdbcMysql57Table1"""

        // test for 'insert into inner_table from ex_table'
        sql  """ drop table if exists ${exMysqlTable} """
        sql  """ drop table if exists ${inDorisTable} """
        sql  """
              CREATE EXTERNAL TABLE ${exMysqlTable} (
              `id` int(11) NOT NULL COMMENT "主键id",
              `name` string NULL COMMENT "名字"
              ) ENGINE=JDBC
              COMMENT "JDBC Mysql 外部表"
              PROPERTIES (
                "resource" = "$jdbcResourceMysql57",
                "table" = "ex_tb0",
                "table_type"="mysql"
              );
        """
        sql  """
              CREATE TABLE ${inDorisTable} (
                `id` int(11) NOT NULL COMMENT "主键id",
                `name` string REPLACE_IF_NOT_NULL NULL COMMENT "名字"
              ) ENGINE=OLAP
              AGGREGATE KEY(`id`)
              COMMENT "OLAP"
              DISTRIBUTED BY HASH(`id`) BUCKETS 10
              PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "in_memory" = "false",
                "storage_format" = "V2"
              );
        """
        sql  """ insert into ${inDorisTable} select id, name from ${exMysqlTable}; """
        order_qt_sql  """ select id, name from ${inDorisTable} order by id; """


        // test for value column which type is string
        sql  """ drop table if exists ${exMysqlTable2} """
        sql  """ drop table if exists ${inDorisTable2} """
        sql  """
               CREATE EXTERNAL TABLE ${exMysqlTable2} (
               `id` int(11) NOT NULL,
               `count_value` varchar(20) NULL
               ) ENGINE=JDBC
               COMMENT "JDBC Mysql 外部表"
               PROPERTIES (
                "resource" = "$jdbcResourceMysql57",
                "table" = "ex_tb2",
                "table_type"="mysql"
               ); 
        """
        sql """
                CREATE TABLE ${inDorisTable2} (
                `id` int(11) NOT NULL,
                `count_value` string REPLACE_IF_NOT_NULL NULL
                ) ENGINE=OLAP
                AGGREGATE KEY(`id`)
                COMMENT "OLAP"
                DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "in_memory" = "true",
                "storage_format" = "V2"
                );
        """
        sql """ insert into ${inDorisTable2} select id, count_value from ${exMysqlTable2}; """
        order_qt_sql """ select id,count_value  from ${inDorisTable2} order by id; """


        // test for ex_table join in_table
        sql  """ drop table if exists ${exMysqlTable} """
        sql  """ drop table if exists ${inDorisTable} """
        sql  """ drop table if exists ${inDorisTable1} """
        sql  """ 
                CREATE EXTERNAL TABLE `${exMysqlTable}` (
                  `game_code` varchar(20) NOT NULL COMMENT "",
                  `plat_code` varchar(20) NOT NULL COMMENT "",
                  `account` varchar(100) NOT NULL COMMENT "",
                  `login_time` bigint(20) NOT NULL COMMENT "",
                  `register_time` bigint(20) NULL COMMENT "",
                  `pid` varchar(20) NULL COMMENT "",
                  `gid` varchar(20) NULL COMMENT "",
                  `region` varchar(100) NULL COMMENT ""
                ) ENGINE=JDBC
                COMMENT "JDBC Mysql 外部表"
                PROPERTIES (
                "resource" = "$jdbcResourceMysql57",
                "table" = "ex_tb3",
                "table_type"="mysql"
                ); 
        """
        sql  """
                CREATE TABLE `${inDorisTable}`
                (
                    `day`      date         NULL COMMENT "",
                    `game`     varchar(500) NULL COMMENT "",
                    `plat`     varchar(500) NULL COMMENT "",
                    `dt`       datetime     NULL COMMENT "",
                    `time`     bigint(20)   NULL COMMENT "",
                    `sid`      int(11)      NULL COMMENT "",
                    `pid`      varchar(500) NULL COMMENT "",
                    `gid`      varchar(500) NULL COMMENT "",
                    `account`  varchar(500) NULL COMMENT "",
                    `playerid` varchar(500) NULL COMMENT "",
                    `prop`     varchar(500) NULL COMMENT "",
                    `p01`      varchar(500) NULL COMMENT "",
                    `p02`      varchar(500) NULL COMMENT "",
                    `p03`      varchar(500) NULL COMMENT "",
                    `p04`      varchar(500) NULL COMMENT "",
                    `p05`      varchar(500) NULL COMMENT "",
                    `p06`      varchar(500) NULL COMMENT "",
                    `p07`      varchar(500) NULL COMMENT "",
                    `p08`      varchar(500) NULL COMMENT "",
                    `p09`      varchar(500) NULL COMMENT "",
                    `p10`      varchar(500) NULL COMMENT "",
                    `p11`      varchar(500) NULL COMMENT "",
                    `p12`      varchar(500) NULL COMMENT "",
                    `p13`      varchar(500) NULL COMMENT "",
                    `p14`      varchar(500) NULL COMMENT "",
                    `p15`      varchar(500) NULL COMMENT ""
                ) ENGINE = OLAP DUPLICATE KEY(`day`, `game`, `plat`)
                COMMENT "doris olap table"
                DISTRIBUTED BY HASH(`game`, `plat`) BUCKETS 4
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
                );
        """
        sql  """ insert into ${inDorisTable} VALUES
            ('2020-05-25','mus','plat_code','2020-05-25 23:34:32',1590420872639,300292,'11','1006061','1001169339','1448335986680242','{}','17001','223.104.103.102','a','808398','0.0','290','258','300292','','','','','','',''),
            ('2020-05-25','mus','plat_code','2020-05-25 18:29:54',1590402594411,300292,'11','1006061','1001169339','1448335986660352','{}','17001','27.186.139.141','a','96855737','8173.0','290','290','300292','','','','','','',''),
            ('2020-05-25','mus','plat_code','2020-05-25 12:37:13',1590381433914,300292,'11','1006061','1001169339','1448335986660352','{}','17001','36.98.31.111','a','96942383','8173.0','290','290','300292','','','','','','',''),
            ('2020-05-25','mus','plat_code','2020-05-25 19:39:50',1590406790026,300292,'11','1006061','1001169339','1448335986660352','{}','17001','36.98.131.232','a','96855737','8173.0','290','290','300292','','','','','','',''),
            ('2020-05-25','mus','plat_code','2020-05-25 23:28:02',1590420482288,300292,'11','1006061','1001169339','1448335986660317','{}','17001','223.104.103.102','a','15584051','116.0','290','290','300292','','','','','','','');
        """
        sql  """
                CREATE TABLE `${inDorisTable1}`
                (
                    `game_code`          varchar(50)  NOT NULL DEFAULT "-",
                    `plat_code`          varchar(50)  NOT NULL DEFAULT "-",
                    `sid`                int(11)      NULL,
                    `name`               varchar(50)  NULL,
                    `day`                varchar(32)  NULL,
                    `merged_to`          int(11)      NULL,
                    `merge_count`        int(11)      NULL,
                    `merge_path`         varchar(255) NULL,
                    `merge_time`         bigint(20)   NULL,
                    `merge_history_time` bigint(20)   NULL,
                    `open_time`          bigint(20)   NULL,
                    `open_day`           int(11)      NULL,
                    `time_zone`          varchar(32)  NULL,
                    `state`              smallint(6)  NULL
                ) ENGINE = OLAP 
                DUPLICATE KEY(`game_code`, `plat_code`, `sid`, `name`)
                COMMENT "维度表"
                DISTRIBUTED BY HASH(`game_code`, `plat_code`, `sid`, `name`) BUCKETS 4
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "in_memory" = "true",
                "storage_format" = "V2"
                );
        """
        sql  """ 
              INSERT INTO ${inDorisTable1} (game_code,plat_code,sid,name,`day`,merged_to,merge_count,merge_path,merge_time,merge_history_time,open_time,open_day,time_zone,state) VALUES
                ('mus','plat_code',310132,'aa','2020-05-25',310200,NULL,NULL,1609726391000,1609726391000,1590406370000,606,'GMT+8',2),
                ('mus','plat_code',310078,'aa','2020-05-05',310140,NULL,NULL,1620008473000,1604284571000,1588690010001,626,'GMT+8',2),
                ('mus','plat_code',310118,'aa','2020-05-19',310016,NULL,NULL,1641178695000,1614565485000,1589871140001,612,'GMT+8',2),
                ('mus','plat_code',421110,'aa','2020-05-24',421116,NULL,NULL,1641178695000,1635732967000,1590285600000,607,'GMT+8',2),
                ('mus','plat_code',300417,'aa','2019-08-31',300499,NULL,NULL,1617590476000,1617590476000,1567243760000,874,'GMT+8',2),
                ('mus','plat_code',310030,'aa','2020-04-25',310140,NULL,NULL,1620008473000,1604284571000,1587780830000,636,'GMT+8',2),
                ('mus','plat_code',310129,'aa','2020-05-24',310033,NULL,NULL,1641178695000,1604284571000,1590274340000,607,'GMT+8',2),
                ('mus','plat_code',310131,'aa','2020-05-25',310016,NULL,NULL,1604284571000,1604284571000,1590378830000,606,'GMT+8',2),
                ('mus','plat_code',410083,'aa','2020-02-04',410114,NULL,NULL,1627872240000,1627872240000,1580749850000,717,'GMT+8',2),
                ('mus','plat_code',310128,'aa','2020-05-23',310128,2,'310180,310114,310112,310107,310080,310076,310065,310066,310054,310038,310036,310018,310011,310012,310032,310031',1630895172000,NULL,1590226280000,608,'GMT+8',1),
                ('mus','plat_code',410052,'aa','2019-12-17',410111,2,'410038,410028',1641178752000,1641178752000,1576517330000, 766,'GMT+8',2);
        """
        order_qt_sql  """
                select l.game_code, l.plat_code, l.org_sid, l.account, l.playerid, l.gid gid_code, l.pid pid_code, 
                coalesce(cast(l.ct_sid as int), l.org_sid, 0) ct_sid, coalesce(l.ip, '-') ip, l.dt dt , 
                coalesce(l.player_name, '-') player_name, coalesce(mp.userid, '-') userid, uniqueKey, 
                coalesce(from_unixtime(s.open_time / 1000, '%Y-%m-%d %H:%i:%s'), '9999-12-30 00:00:00') open_server_time, 
                '-' country, '-' province, '-' city from ( select * from ( select game_code, plat_code, account, 
                FROM_UNIXTIME( login_time / 1000) start_time, account userKey, concat('_', account, register_time) userid, 
                FROM_UNIXTIME( LEAD (login_time , 1, 253402099200000) over (partition by game_code, plat_code, account order by login_time) / 1000 ) end_time 
                from ${exMysqlTable} where game_code = 'mus' and plat_code = 'plat_code' and login_time < unix_timestamp(convert_tz(date_add('2020-05-25 00:00:00', 
                INTERVAL 1 day), 'Asia/Shanghai', 'Asia/Shanghai'))* 1000 ) dim_account_userid_mapping where start_time < convert_tz(date_add('2020-05-25 00:00:00', INTERVAL 1 day), 
                'Asia/Shanghai', 'Asia/Shanghai') and end_time >= convert_tz('2020-05-25 00:00:00', 'Asia/Shanghai', 'Asia/Shanghai') 
                and game_code = 'mus' and plat_code = 'plat_code' ) mp right join ( select game game_code, `day`, `plat` plat_code, `playerid`, 
                dt, `sid` ct_sid, `pid`, `gid`, `account`, p07 org_sid, p11 ip , get_json_string(prop, '\$.player_name') player_name, 
                account userKey, CONCAT_WS('_', 'custom', game, plat, sid, day, ROW_NUMBER() over (partition by game, plat, sid, day order by `time`)) uniqueKey 
                from ${inDorisTable} where dt BETWEEN convert_tz('2020-05-25 00:00:00', 'Asia/Shanghai', 'Asia/Shanghai') and convert_tz('22020-05-25 23:59:59', 
                'Asia/Shanghai', 'Asia/Shanghai') and day BETWEEN date_sub('2020-05-25', INTERVAL 1 DAY ) and date_add('2020-05-25', INTERVAL 1 DAY ) 
                and game = 'mus' and plat = 'plat_code' ) l on l.userKey = mp.userKey and l.game_code = mp.game_code and l.plat_code = mp.plat_code 
                and l.dt >= mp.start_time and l.dt < mp.end_time left join ${inDorisTable1} s on l.org_sid = s.sid and l.game_code = s.game_code 
                and l.plat_code = s.plat_code;
        """


        // test for insert null to string type column
        sql  """ drop table if exists ${exMysqlTable} """
        sql  """ drop table if exists ${inDorisTable} """
        sql  """ CREATE EXTERNAL TABLE `${exMysqlTable}` (
                  `id` int(11) not NULL,
                  `apply_id` varchar(32) NULL,
                  `begin_value` string NULL,
                  `operator` varchar(32) NULL,
                  `operator_name` varchar(32) NULL,
                  `state` varchar(8) NULL,
                  `sub_state` varchar(8) NULL,
                  `state_count` smallint(5) NULL,
                  `create_time` datetime NULL
                ) ENGINE=JDBC
                COMMENT "JDBC Mysql 外部表"
                PROPERTIES (
                "resource" = "$jdbcResourceMysql57",
                "table" = "ex_tb5",
                "table_type"="mysql"
                );
        """
        sql  """
                CREATE TABLE `${inDorisTable}` (
                  `id` int(11) NULL,
                  `apply_id` varchar(96) NULL,
                  `begin_value` string,
                  `operator` varchar(96),
                  `operator_name` varchar(96) NULL,
                  `state` varchar(24) NULL,
                  `sub_state` varchar(24) NULL,
                  `state_count` smallint(6) NULL,
                  `create_time` datetime NULL,
                  `binlog_file` varchar(64) NULL,
                  `binlog_position` largeint(40) NULL,
                  `os_ts_ms` bigint(20) NULL,
                  `os_data_flg` varchar(4) NULL
                ) ENGINE=OLAP
                UNIQUE KEY(`id`)
                COMMENT "OLAP"
                DISTRIBUTED BY HASH(`id`) BUCKETS 5
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "in_memory" = "false",
                "storage_format" = "V2"
                );
        """
        sql  """  
                insert into ${inDorisTable} (id, apply_id, begin_value, operator, operator_name, state, sub_state, 
                                            create_time, binlog_file, binlog_position, os_ts_ms, os_data_flg)
                select id,apply_id,begin_value,operator, operator_name, state, sub_state, create_time, 'init', 100, 100, 'd'
                from ${exMysqlTable} where begin_value is null; 
        """
        order_qt_sql """
                SELECT  min(LENGTH(begin_value)), max(LENGTH(begin_value)), sum(case when begin_value is null then 1 else 0 end)
                from $exMysqlTable ;
        """
    }
}


