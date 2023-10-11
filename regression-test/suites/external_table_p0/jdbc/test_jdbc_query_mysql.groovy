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

suite("test_jdbc_query_mysql", "p0,external,mysql,external_docker,external_docker_mysql") {

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"

    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_57_port = context.config.otherConfigs.get("mysql_57_port")
        String jdbcResourceMysql57 = "jdbc_resource_mysql_57_x"
        String jdbcMysql57Table1 = "jdbc_mysql_57_table1"
        String exMysqlTable = "doris_ex_tb";
        String exMysqlTable1 = "doris_ex_tb1";
        String exMysqlTable2 = "doris_ex_tb2";
	    String exMysqlTypeTable = "doris_ex_type_tb";
        String inDorisTable = "test_jdbc_mysql_doris_in_tb";
        String inDorisTable1 = "test_jdbc_mysql_doris_in_tb1";
        String inDorisTable2 = "test_jdbc_mysql_doris_in_tb2";

        sql """drop resource if exists $jdbcResourceMysql57;"""
        sql """
            create external resource $jdbcResourceMysql57
            properties (
                "type"="jdbc",
                "user"="root",
                "password"="123456",
                "jdbc_url"="jdbc:mysql://${externalEnvIp}:$mysql_57_port/doris_test",
                "driver_url"="${driver_url}",
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
                "in_memory" = "false",
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
                "in_memory" = "false",
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
                  `id` bigint not NULL,
                  `apply_id` varchar(32) NULL,
                  `begin_value` string NULL,
                  `operator` varchar(32) NULL,
                  `operator_name` varchar(32) NULL,
                  `state` varchar(8) NULL,
                  `sub_state` varchar(8) NULL,
                  `state_count` int NULL,
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


        // test for quotation marks in int
        sql """ drop table if exists ${exMysqlTable1} """
        sql  """ CREATE EXTERNAL TABLE `${exMysqlTable1}` (
                    `id` bigint(20) NULL,
                    `t_id` bigint(20) NULL,
                    `name` text NULL
                ) ENGINE=JDBC
                COMMENT "JDBC Mysql 外部表"
                PROPERTIES (
                "resource" = "$jdbcResourceMysql57",
                "table" = "ex_tb6",
                "table_type"="mysql"
                );
        """
        order_qt_sql """ select * from $exMysqlTable1 where id in ('639215401565159424') and  id='639215401565159424';  """
        order_qt_sql """ select * from $exMysqlTable1 where id in (639215401565159424) and  id=639215401565159424; """
        order_qt_sql """ select * from $exMysqlTable1 where id in ('639215401565159424') ; """


        // test for decimal
        sql """ drop table if exists ${exMysqlTable2} """
        sql  """ CREATE EXTERNAL TABLE `${exMysqlTable2}` (
                    `id` varchar(32) NULL DEFAULT "",
                    `user_name` varchar(32) NULL DEFAULT "",
                    `member_list` DECIMAL(10,3)
                ) ENGINE=JDBC
                COMMENT "JDBC Mysql 外部表"
                PROPERTIES (
                "resource" = "$jdbcResourceMysql57",
                "table" = "ex_tb7",
                "table_type"="mysql"
                );
        """
        order_qt_sql """ select  * from ${exMysqlTable2} order by member_list; """


        // test for 'With in in select smt and group by will cause missing from GROUP BY'
        sql """ drop table if exists ${exMysqlTable} """
        sql  """ CREATE EXTERNAL TABLE `${exMysqlTable}` (
                    `date` date NOT NULL COMMENT "",
                    `uid` varchar(64) NOT NULL,
                    `stat_type` int(11) NOT NULL COMMENT "",
                    `price` varchar(255) NULL COMMENT "price"
                ) ENGINE=JDBC
                COMMENT "JDBC Mysql 外部表"
                PROPERTIES (
                "resource" = "$jdbcResourceMysql57",
                "table" = "ex_tb8",
                "table_type"="mysql"
                );
        """
        order_qt_sql """ select date, sum(if(stat_type in (1), 1, 0)) from ${exMysqlTable} group by date; """


        // test for DATE_ADD
        sql """ drop table if exists ${exMysqlTable1} """
        sql  """ CREATE EXTERNAL TABLE `${exMysqlTable1}` (
                    c_date date NULL
                ) ENGINE=JDBC
                COMMENT "JDBC Mysql 外部表"
                PROPERTIES (
                "resource" = "$jdbcResourceMysql57",
                "table" = "ex_tb9",
                "table_type"="mysql"
                );
        """
        order_qt_sql """ select DATE_ADD(c_date, INTERVAL 1 month) as c from ${exMysqlTable1} order by c; """


        // test for count(1) of subquery
        // this external table will use doris_test.ex_tb2
        sql """ drop table if exists ${exMysqlTable2} """
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
        order_qt_sql """ select count(1) from (select '2022' as dt, sum(id) from ${exMysqlTable2}) a; """



        // test for 'select * from (select 1 as a) b  full outer join (select 2 as a) c using(a)'
        // this external table will use doris_test.ex_tb0
        sql """ drop table if exists ${exMysqlTable} """
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
        order_qt_sql """ 
            select * from 
            (select id as a from ${exMysqlTable} where id = 111) b  
            full outer join 
            (select id as a from ${exMysqlTable} where id = 112) c 
            using(a); 
        """


        // test for 'select CAST(NULL AS CHAR(1))'
        // this external table will use doris_test.ex_tb9
        sql """ drop table if exists ${exMysqlTable1} """
        sql  """                
               CREATE EXTERNAL TABLE ${exMysqlTable1} (
                c_date date NULL
               ) ENGINE=JDBC
               COMMENT "JDBC Mysql 外部表"
               PROPERTIES (
                "resource" = "$jdbcResourceMysql57",
                "table" = "ex_tb9",
                "table_type"="mysql"
               ); 
        """
        order_qt_sql """ 
            select CAST(c_date AS CHAR(1)) as a from ${exMysqlTable1} order by a;
        """


        // test for string sort
        // this external table will use doris_test.ex_tb7
        sql """ drop table if exists ${exMysqlTable2} """
        sql  """                
               CREATE EXTERNAL TABLE ${exMysqlTable2} (
                    `date` date NOT NULL COMMENT "",
                    `uid` varchar(64) NOT NULL,
                    `stat_type` int(11) NOT NULL COMMENT "",
                    `price` varchar(255) NULL COMMENT "price"
               ) ENGINE=JDBC
               COMMENT "JDBC Mysql 外部表"
               PROPERTIES (
                "resource" = "$jdbcResourceMysql57",
                "table" = "ex_tb8",
                "table_type"="mysql"
               ); 
        """
        order_qt_sql """ 
            select * from
            (select uid as a, uid as b, uid as c, 6 from ${exMysqlTable2} where stat_type = 2
            union all
            select '汇总' as a, '汇总' as b, '汇总' as c, 6) a
            order by 1,2,3,4;
        """


        // test for query int without quotation marks
        sql """ drop table if exists ${exMysqlTable} """
        sql  """                
               CREATE EXTERNAL TABLE ${exMysqlTable} (
                `aa` varchar(200) NULL COMMENT "",
                `bb` int NULL COMMENT "",
                `cc` bigint NULL COMMENT ""
               ) ENGINE=JDBC
               COMMENT "JDBC Mysql 外部表"
               PROPERTIES (
                "resource" = "$jdbcResourceMysql57",
                "table" = "ex_tb10",
                "table_type"="mysql"
               ); 
        """
        order_qt_sql """ 
            select t.aa, count(if(t.bb in (1,2) ,true ,null)) as c from ${exMysqlTable} t group by t.aa order by c;
        """


        // test for wrong result
        sql """ drop table if exists ${exMysqlTable1} """
        sql """ drop table if exists ${exMysqlTable2} """
        sql  """                
               CREATE EXTERNAL TABLE ${exMysqlTable1} (
                `aa` varchar(200) NULL COMMENT "",
                `bb` int NULL COMMENT ""
               ) ENGINE=JDBC
               COMMENT "JDBC Mysql 外部表"
               PROPERTIES (
                "resource" = "$jdbcResourceMysql57",
                "table" = "ex_tb11",
                "table_type"="mysql"
               ); 
        """
        sql  """                
               CREATE EXTERNAL TABLE ${exMysqlTable2} (
                `cc` varchar(200) NULL COMMENT "",
                `dd` int NULL COMMENT ""
               ) ENGINE=JDBC
               COMMENT "JDBC Mysql 外部表"
               PROPERTIES (
                "resource" = "$jdbcResourceMysql57",
                "table" = "ex_tb12",
                "table_type"="mysql"
               ); 
        """
        order_qt_sql """ 
            select t.* from ( select * from ${exMysqlTable1} t1 left join ${exMysqlTable2} t2 on t1.aa=t2.cc ) t order by aa; 
        """


        // test for crash be sql
        sql """ drop table if exists ${exMysqlTable} """
        sql  """                
               CREATE EXTERNAL TABLE ${exMysqlTable} (
                 name varchar(128),
                 age INT,
                 idCode  varchar(128),
                 cardNo varchar(128),
                 number varchar(128),
                 birthday DATETIME,
                 country varchar(128),
                 gender varchar(128),
                 covid boolean
               ) ENGINE=JDBC
               COMMENT "JDBC Mysql 外部表"
               PROPERTIES (
                "resource" = "$jdbcResourceMysql57",
                "table" = "ex_tb13",
                "table_type"="mysql"
               ); 
        """
        order_qt_sql """ 
            SELECT count(1) FROM (WITH t1 AS ( WITH t AS ( SELECT * FROM ${exMysqlTable}) 
                SELECT idCode, COUNT(1) as dataAmount,ROUND(COUNT(1) / tableWithSum.sumResult,4) as proportion,                  
                MD5(idCode) as virtuleUniqKey FROM t,(SELECT COUNT(1) as sumResult from t) tableWithSum  
                GROUP BY idCode ,tableWithSum.sumResult  ) 
                SELECT  idCode,dataAmount, (CASE WHEN t1.virtuleUniqKey = tableWithMaxId.max_virtuleUniqKey THEN
                ROUND(proportion + calcTheTail, 4)  ELSE  proportion END) proportion  FROM t1, 
                (SELECT (1 - sum(t1.proportion)) as calcTheTail FROM t1 ) tableWithTail,          
                (SELECT virtuleUniqKey as  max_virtuleUniqKey FROM t1 ORDER BY proportion DESC LIMIT 1 ) tableWithMaxId  
                ORDER BY idCode) t_aa;
        """


        // test for query like
        sql """ drop table if exists ${exMysqlTable1} """
        sql  """                
               CREATE EXTERNAL TABLE ${exMysqlTable1} (
                 tid varchar(128),
                 log_time date,
                 dt  date,
                 cmd varchar(128),
                 dp_from varchar(128)
               ) ENGINE=JDBC
               COMMENT "JDBC Mysql 外部表"
               PROPERTIES (
                "resource" = "$jdbcResourceMysql57",
                "table" = "ex_tb14",
                "table_type"="mysql"
               ); 
        """
        order_qt_sql """
               select APPROX_COUNT_DISTINCT(tid) as counts 
               from ${exMysqlTable1} 
               where log_time >= '2022-11-02 20:00:00' AND log_time < '2022-11-02 21:00:00'
               and dt = '2022-11-02'
               and cmd = '8011' and tid is not null and tid != '' 
               and (dp_from like '%gdt%' or dp_from like '%vivo%' or dp_from like '%oppo%');
        """


        // test for IFNULL, IFNULL and get_json_str
        // this external table will use doris_test.ex_tb1
        sql """ drop table if exists ${exMysqlTable} """
        sql """
            CREATE EXTERNAL TABLE ${exMysqlTable} (
                  id varchar(128)
               ) ENGINE=JDBC
               COMMENT "JDBC Mysql 外部表"
               PROPERTIES (
                "resource" = "$jdbcResourceMysql57",
                "table" = "ex_tb1",
                "table_type"="mysql"
             );
        """
        order_qt_sql """ select IFNULL(get_json_string(id, "\$.k1"), 'SUCCESS')= 'FAIL' from ${exMysqlTable}; """
        order_qt_sql """ select CONCAT(SPLIT_PART(reverse(id),'.',1),".",IFNULL(SPLIT_PART(reverse(id),'.',2),' ')) from ${exMysqlTable}; """


        // test for complex query cause be core
        sql """ drop table if exists ${exMysqlTable1} """
        sql """ drop table if exists ${exMysqlTable2} """
        sql """
            CREATE EXTERNAL TABLE ${exMysqlTable1} (
                `id` bigint(20) NOT NULL COMMENT '',
                `name` varchar(192) NOT NULL COMMENT '',
                `is_delete` tinyint(4) NULL,
                `create_uid` bigint(20) NULL,
                `modify_uid` bigint(20) NULL,
                `ctime` bigint(20) NULL,
                `mtime` bigint(20) NULL
               ) ENGINE=JDBC
               COMMENT "JDBC Mysql 外部表"
               PROPERTIES (
                "resource" = "$jdbcResourceMysql57",
                "table" = "ex_tb16",
                "table_type"="mysql"
             );
        """
        sql """
            CREATE EXTERNAL TABLE ${exMysqlTable2} (
                `id` bigint(20) NULL,
                `media_order_id` int(11) NULL,
                `supplier_id` int(11) NULL,
                `agent_policy_type` tinyint(4) NULL,
                `agent_policy` decimal(6, 2) NULL,
                `capital_type` bigint(20) NULL,
                `petty_cash_type` tinyint(4) NULL,
                `recharge_amount` decimal(10, 2) NULL,
                `need_actual_amount` decimal(10, 2) NULL,
                `voucher_url` varchar(765) NULL,
                `ctime` bigint(20) NULL,
                `mtime` bigint(20) NULL,
                `is_delete` tinyint(4) NULL,
                `media_remark` text NULL,
                `account_number` varchar(765) NULL,
                `currency_type` tinyint(4) NULL,
                `order_source` tinyint(4) NULL
               ) ENGINE=JDBC
               COMMENT "JDBC Mysql 外部表"
               PROPERTIES (
                "resource" = "$jdbcResourceMysql57",
                "table" = "ex_tb17",
                "table_type"="mysql"
             );
        """
        order_qt_sql """ 
        with tmp_media_purchase as (
            select media_order_id, supplier_id, agent_policy_type, agent_policy, capital_type, petty_cash_type, 
                recharge_amount, need_actual_amount, voucher_url, m.`ctime`, m.`mtime`, m.`is_delete`, media_remark, 
                account_number, currency_type, order_source, `name`
        from ${exMysqlTable2} m left join ${exMysqlTable1} s on s.id = m.supplier_id where m.is_delete = 0),
        t1 as (select media_order_id, from_unixtime(MIN(ctime), '%Y-%m-%d') AS first_payment_date,
              from_unixtime(max(ctime), '%Y-%m-%d') AS last_payment_date,
              sum(IFNULL(recharge_amount, 0.00)) recharge_total_amount,
              sum(case when capital_type = '2' then IFNULL(recharge_amount, 0.00) else 0.00 end) as petty_amount,
              sum(case when capital_type = '2' and petty_cash_type = '1' then IFNULL(recharge_amount, 0.00) else 0.00 end) as petty_change_amount,
              sum(case when capital_type = '2' and petty_cash_type = '2' then IFNULL(recharge_amount, 0.00) else 0.00 end) as petty_recharge_amount,
              sum(case when capital_type = '2' and petty_cash_type = '3' then IFNULL(recharge_amount, 0.00) else 0.00 end) as petty_return_amount,
              sum(case when capital_type = '3' then IFNULL(need_actual_amount, 0.00) else 0.00 end) as return_goods_amount,
              GROUP_CONCAT(distinct cast(supplier_id as varchar (12))) supplier_id_list
       from tmp_media_purchase group by media_order_id),
        t2 as (select media_order_id, GROUP_CONCAT(distinct (case agent_policy_type 
        when '1' then 'A' when '2' then 'B' when '3' then 'C' when '4' then 'D' when '5' then 'E' when '6' then 'F'
        when '7' then 'G' when '8' then 'H' when '9' then 'I' when '10' then 'J' when '11' then 'K' when '12' then 'L'
        when '13' then 'M'  else agent_policy_type end)) agent_policy_type_list
       from tmp_media_purchase group by media_order_id),
       t3 as (select media_order_id, GROUP_CONCAT(distinct cast(agent_policy as varchar (12))) agent_policy_list
       from tmp_media_purchase group by media_order_id),
       t4 as (select media_order_id, GROUP_CONCAT(distinct (case capital_type
        when '1' then 'A' when '2' then 'B' when '3' then 'C' else capital_type end)) capital_type_list
       from tmp_media_purchase group by media_order_id),
       t5 as (select media_order_id, GROUP_CONCAT(distinct (case petty_cash_type
        when '1' then 'A' when '2' then 'B' when '3' then 'C' else petty_cash_type end)) petty_cash_type_list
       from tmp_media_purchase group by media_order_id),
       t6 as (select media_order_id, GROUP_CONCAT(distinct `name`) company_name_list
            from tmp_media_purchase group by media_order_id)
        select distinct tmp_media_purchase.`media_order_id`,
                first_payment_date,
                last_payment_date,
                recharge_total_amount,
                petty_amount,
                petty_change_amount,
                petty_recharge_amount,
                petty_return_amount,
                return_goods_amount,
                supplier_id_list,
                agent_policy_type_list,
                agent_policy_list,
                capital_type_list,
                petty_cash_type_list,
                company_name_list
        from tmp_media_purchase
         left join t1 on tmp_media_purchase.media_order_id = t1.media_order_id
         left join t2 on tmp_media_purchase.media_order_id = t2.media_order_id
         left join t3 on tmp_media_purchase.media_order_id = t3.media_order_id
         left join t4 on tmp_media_purchase.media_order_id = t4.media_order_id
         left join t5 on tmp_media_purchase.media_order_id = t5.media_order_id
         left join t6 on tmp_media_purchase.media_order_id = t6.media_order_id
        order by tmp_media_purchase.media_order_id
        """


        // test for aggregate
        order_qt_sql1 """ SELECT COUNT(true) FROM $jdbcMysql57Table1 """
        order_qt_sql2 """ SELECT COUNT(*) FROM $jdbcMysql57Table1 WHERE k7 < k8 """
        order_qt_sql3 """ SELECT COUNT(*) FROM $jdbcMysql57Table1 WHERE NOT k7 < k8 """
        order_qt_sql4 """ SELECT COUNT(*) FROM $jdbcMysql57Table1 WHERE NULL """
        order_qt_sql5 """ SELECT COUNT(*) FROM $jdbcMysql57Table1 WHERE NULLIF(k2, 'F') IS NULL """
        order_qt_sql6 """ SELECT COUNT(*) FROM $jdbcMysql57Table1 WHERE NULLIF(k2, 'F') IS NOT NULL """
        order_qt_sql7 """ SELECT COUNT(*) FROM $jdbcMysql57Table1 WHERE NULLIF(k2, 'F') = k2 """
        order_qt_sql8 """ SELECT COUNT(*) FROM $jdbcMysql57Table1 WHERE COALESCE(NULLIF(k2, 'abc'), 'abc') = 'abc' """
        order_qt_sql9 """ SELECT COUNT(*) FROM $jdbcMysql57Table1 WHERE k7 < k8 AND k8 > 30 AND k8 < 40 """
        order_qt_sql10 """ SELECT COUNT(*) FROM (SELECT k1 FROM $jdbcMysql57Table1) x """
        order_qt_sql11 """ SELECT COUNT(*) FROM (SELECT k1, COUNT(*) FROM $jdbcMysql57Table1 GROUP BY k1) x """
        order_qt_sql12 """ SELECT k1, c, count(*) FROM (SELECT k1, count(*) c FROM $jdbcMysql57Table1 GROUP BY k1) as a GROUP BY k1, c """
        order_qt_sql13 """ SELECT k2, sum(CAST(NULL AS BIGINT)) FROM $jdbcMysql57Table1 GROUP BY k2 """
        order_qt_sql14 """ SELECT `key`, COUNT(*) as c FROM (
                            SELECT CASE WHEN k8 % 3 = 0 THEN NULL WHEN k8 % 5 = 0 THEN 0 ELSE k8 END AS `key`
                            FROM $jdbcMysql57Table1) as a GROUP BY `key` order by c desc key asc limit 10"""
        order_qt_sql15 """ SELECT lines, COUNT(*) as c FROM (SELECT k7, COUNT(*) lines FROM $jdbcMysql57Table1 GROUP BY k7) U GROUP BY lines order by c"""
        order_qt_sql16 """ SELECT COUNT(DISTINCT k8 + 1) FROM $jdbcMysql57Table1 """
        order_qt_sql17 """ SELECT COUNT(*) FROM (SELECT DISTINCT k8 + 1 FROM $jdbcMysql57Table1) t """
        order_qt_sql18 """ SELECT COUNT(DISTINCT k8), COUNT(*) from $jdbcMysql57Table1 where k8 > 40 """
        order_qt_sql19 """ SELECT COUNT(DISTINCT k8) AS count, k7 FROM $jdbcMysql57Table1 GROUP BY k7 ORDER BY count, k7 """
        order_qt_sql20 """ SELECT k2, k3, COUNT(DISTINCT k5), SUM(DISTINCT k8) FROM $jdbcMysql57Table1 GROUP BY k2, k3 order by k2, k3 """
        order_qt_sql21 """ SELECT k2, COUNT(DISTINCT k7), COUNT(DISTINCT k8) FROM $jdbcMysql57Table1 GROUP BY k2 """
        order_qt_sql22 """ SELECT SUM(DISTINCT x) FROM (SELECT k7, COUNT(DISTINCT k8) x FROM $jdbcMysql57Table1 GROUP BY k7) t """
        order_qt_sql23 """ SELECT max(k8), COUNT(k7), sum(DISTINCT k6) FROM $jdbcMysql57Table1 """
        order_qt_sql24 """ SELECT s, MAX(k6), SUM(a) FROM (SELECT k6, avg(k8) AS a, SUM(DISTINCT k7) AS s FROM $jdbcMysql57Table1 GROUP BY k6) as b  group by s"""
        order_qt_sql25 """ SELECT COUNT(DISTINCT k8) FROM $jdbcMysql57Table1 WHERE LENGTH(k2) > 2 """
        sql  """ drop table if exists ${exMysqlTable} """
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
        order_qt_sql26 """ 
                        SELECT max(id), min(id), count(id) + 1, count(id)
                        FROM (SELECT DISTINCT k8 FROM $jdbcMysql57Table1) AS r1
                        LEFT JOIN ${exMysqlTable} as a ON r1.k8 = a.id GROUP BY r1.k8 
                        HAVING sum(id) < 110 """
        order_qt_sql27 """ SELECT id BETWEEN 110 AND 115 from $exMysqlTable GROUP BY id BETWEEN 110 AND 115;  """
        order_qt_sql28 """ SELECT CAST(id BETWEEN 1 AND 120 AS BIGINT) FROM $exMysqlTable GROUP BY id """
        order_qt_sql29 """ SELECT CAST(50 BETWEEN id AND 120 AS BIGINT) FROM $exMysqlTable GROUP BY id """
        order_qt_sql30 """ SELECT CAST(50 BETWEEN 1 AND id AS BIGINT) FROM $exMysqlTable GROUP BY id """
        order_qt_sql31 """ SELECT CAST(id AS VARCHAR) as a, count(*) FROM $exMysqlTable GROUP BY CAST(id AS VARCHAR) order by a """
        order_qt_sql32 """ SELECT NULLIF(k7, k8), count(*) as c FROM $jdbcMysql57Table1 GROUP BY NULLIF(k7, k8) order by c desc"""
        order_qt_sql33 """ SELECT id + 1, id + 2, id + 3, id + 4, id + 5, id + 6, id + 7,id + 8, id + 9, id + 10, COUNT(*) AS c
                            FROM $exMysqlTable GROUP BY id + 1, id + 2, id + 3, id + 4, id + 5, id + 6, id + 7,id + 8, id + 9, id + 10
                            ORDER BY c desc """
        order_qt_sql35 """ 
                        SELECT name,SUM(CAST(id AS BIGINT))
                        FROM $exMysqlTable
                        WHERE name = 'abc'
                        GROUP BY name
                        UNION
                        SELECT NULL, SUM(CAST(id AS BIGINT))
                        FROM $exMysqlTable
                        WHERE name = 'abd' """


        // test for distribute queries
        order_qt_sql38 """ SELECT count(*) FROM ${exMysqlTable} WHERE id IN (SELECT k8 FROM $jdbcMysql57Table1 WHERE k8 > 111); """
        sql """ create view if not exists aview as select k7, k8 from $jdbcMysql57Table1; """
        order_qt_sql39 """ SELECT * FROM aview a JOIN aview b on a.k8 = b.k8 order by a.k8 desc limit 5 """
        order_qt_sql42 """ SELECT * FROM (SELECT * FROM $jdbcMysql57Table1 WHERE k8 % 8 = 0) l JOIN ${exMysqlTable} o ON l.k8 = o.id """
        order_qt_sql43 """ SELECT * FROM (SELECT * FROM $jdbcMysql57Table1 WHERE k8 % 8 = 0) l LEFT JOIN ${exMysqlTable} o ON l.k8 = o.id order by k8 limit 5"""
        order_qt_sql44 """ SELECT * FROM (SELECT * FROM $jdbcMysql57Table1 WHERE k8 % 8 = 0) l RIGHT JOIN ${exMysqlTable} o ON l.k8 = o.id"""
        order_qt_sql45 """ SELECT * FROM (SELECT * FROM $jdbcMysql57Table1 WHERE k8 % 8 = 0) l JOIN 
                            (SELECT id, COUNT(*) FROM ${exMysqlTable} WHERE id > 111 GROUP BY id ORDER BY id) o ON l.k8 = o.id """
        order_qt_sql46 """ SELECT * FROM (SELECT * FROM $jdbcMysql57Table1 WHERE k8 % 8 = 0) l JOIN ${exMysqlTable} o ON l.k8 = o.id + 1"""
        order_qt_sql47 """ SELECT * FROM (
                            SELECT k8 % 120 AS a, k8 % 3 AS b
                            FROM $jdbcMysql57Table1) l JOIN 
                            (SELECT t1.a AS a, SUM(t1.b) AS b, SUM(LENGTH(t2.name)) % 3 AS d
                                FROM ( SELECT id AS a, id % 3 AS b FROM ${exMysqlTable}) t1
                            JOIN ${exMysqlTable} t2 ON t1.a = t2.id GROUP BY t1.a) o
                            ON l.b = o.d AND l.a = o.a order by l.a desc limit 3"""
        // this pr fixed, wait for merge: https://github.com/apache/doris/pull/16442                    
        order_qt_sql48 """ SELECT x, y, COUNT(*) as c FROM (SELECT k8, 0 AS x FROM $jdbcMysql57Table1) a
                            JOIN (SELECT k8, 1 AS y FROM $jdbcMysql57Table1) b ON a.k8 = b.k8 group by x, y order by c desc limit 3 """
        order_qt_sql49 """ SELECT * FROM (SELECT * FROM $jdbcMysql57Table1 WHERE k8 % 120 > 110) l
                            JOIN (SELECT *, COUNT(1) OVER (PARTITION BY id ORDER BY id) FROM ${exMysqlTable}) o ON l.k8 = o.id """
        order_qt_sql50 """ SELECT COUNT(*) FROM $jdbcMysql57Table1 as a LEFT OUTER JOIN ${exMysqlTable} as b ON a.k8 = b.id AND a.k8 > 111 WHERE a.k8 < 114 """
        order_qt_sql51 """ SELECT count(*) > 0 FROM $jdbcMysql57Table1 JOIN ${exMysqlTable} ON (cast(1.2 AS FLOAT) = CAST(1.2 AS decimal(2,1))) """
        order_qt_sql52 """ SELECT count(*) > 0 FROM $jdbcMysql57Table1 JOIN ${exMysqlTable} ON CAST((CASE WHEN (TRUE IS NOT NULL) THEN '1.2' ELSE '1.2' END) AS FLOAT) = CAST(1.2 AS decimal(2,1)) """
        order_qt_sql53 """ SELECT SUM(k8) FROM $jdbcMysql57Table1 as a JOIN ${exMysqlTable} as b ON a.k8 = CASE WHEN b.id % 2 = 0 and b.name = 'abc' THEN b.id ELSE NULL END """
        order_qt_sql54 """ SELECT COUNT(*) FROM $jdbcMysql57Table1 a JOIN ${exMysqlTable} b on not (a.k8 <> b.id) """
        order_qt_sql55 """ SELECT COUNT(*) FROM $jdbcMysql57Table1 a JOIN ${exMysqlTable} b on not not not (a.k8 = b.id)  """
        order_qt_sql56 """ SELECT x + y FROM (
                           SELECT id, COUNT(*) x FROM ${exMysqlTable} GROUP BY id) a JOIN 
                            (SELECT k8, COUNT(*) y FROM $jdbcMysql57Table1 GROUP BY k8) b ON a.id = b.k8 """
        order_qt_sql57 """ SELECT COUNT(*) FROM ${exMysqlTable} as a JOIN $jdbcMysql57Table1 as b ON a.id = b.k8 AND a.name LIKE '%ab%' """
        order_qt_sql58 """ 
                        SELECT COUNT(*) FROM
                        (SELECT a.k8 AS o1, b.id AS o2 FROM $jdbcMysql57Table1 as a LEFT OUTER JOIN ${exMysqlTable} as b 
                        ON a.k8 = b.id AND b.id < 114
                            UNION ALL
                        SELECT a.k8 AS o1, b.id AS o2 FROM $jdbcMysql57Table1 as a RIGHT OUTER JOIN ${exMysqlTable} as b 
                        ON a.k8 = b.id AND b.id < 114 WHERE a.k8 IS NULL) as t1
                         WHERE o1 IS NULL OR o2 IS NULL """
        order_qt_sql59 """ SELECT COUNT(*) FROM $jdbcMysql57Table1 as a JOIN ${exMysqlTable} as b ON a.k8 = 112 AND b.id = 112 """
        order_qt_sql60 """ WITH x AS (SELECT DISTINCT k8 FROM $jdbcMysql57Table1 ORDER BY k8 LIMIT 10)
                            SELECT count(*) FROM x a JOIN x b on a.k8 = b.k8 """
        order_qt_sql61 """ SELECT COUNT(*) FROM (SELECT * FROM $jdbcMysql57Table1 ORDER BY k8 desc LIMIT 5) a 
                            CROSS JOIN (SELECT * FROM $jdbcMysql57Table1 ORDER BY k8 desc LIMIT 5) b """
        order_qt_sql62 """ SELECT a.k8 FROM (SELECT * FROM $jdbcMysql57Table1 WHERE k8 < 119) a
                            CROSS JOIN (SELECT * FROM $jdbcMysql57Table1 WHERE k8 > 100) b order by a.k8 desc limit 3"""
        order_qt_sql63 """ SELECT * FROM (SELECT 1 a) x CROSS JOIN (SELECT 2 b) y """
        order_qt_sql65 """ SELECT t.c FROM (SELECT 1) as t1 CROSS JOIN (SELECT 0 AS c UNION ALL SELECT 1) t """
        order_qt_sql66 """ SELECT t.c FROM (SELECT 1) as a CROSS JOIN (SELECT 0 AS c UNION ALL SELECT 1) t """
        order_qt_sql67 """ SELECT * FROM (SELECT * FROM $jdbcMysql57Table1 ORDER BY k8 LIMIT 5) a
                            JOIN (SELECT * FROM $jdbcMysql57Table1 ORDER BY k8 LIMIT 5) b ON 123 = 123
                            order by a.k8 desc limit 5"""
        sql  """ drop table if exists ${exMysqlTable1} """
        sql  """ 
                CREATE EXTERNAL TABLE `${exMysqlTable1}` (
                   `products_id` int(11) NOT NULL,
                   `orders_id` int(11) NOT NULL,
                   `sales_add_time` datetime NOT NULL,
                   `sales_update_time` datetime NOT NULL,
                   `finance_admin` int(11) NOT NULL
                ) ENGINE=JDBC
                COMMENT "JDBC Mysql 外部表"
                PROPERTIES (
                "resource" = "$jdbcResourceMysql57",
                "table" = "ex_tb4",
                "table_type"="mysql"
                ); 
        """
        order_qt_sql68 """ SELECT finance_admin, count(1) as c FROM $exMysqlTable1 GROUP BY finance_admin
                            HAVING c IN (select k8 from $jdbcMysql57Table1 where k8 = 2) """


        // test for order by
        order_qt_sql70 """ WITH t AS (SELECT 1 x, 2 y) SELECT x, y FROM t ORDER BY x, y """
        order_qt_sql71 """ WITH t AS (SELECT k8 x, k7 y FROM $jdbcMysql57Table1) SELECT x, y FROM t ORDER BY x, y LIMIT 1 """
        order_qt_sql72 """ SELECT finance_admin X FROM ${exMysqlTable1} ORDER BY x """


        // test for queries
        order_qt_sql73 """ SELECT k7, k8 FROM $jdbcMysql57Table1 LIMIT 0 """
        order_qt_sql74 """ SELECT COUNT(k8) FROM $jdbcMysql57Table1 """
        order_qt_sql75 """ SELECT COUNT(CAST(NULL AS BIGINT)) FROM $jdbcMysql57Table1 """
        order_qt_sql76 """ SELECT k8 FROM $jdbcMysql57Table1 WHERE k8 < 120 INTERSECT SELECT id as k8 FROM ${exMysqlTable}  """
        order_qt_sql77 """ SELECT k8 FROM $jdbcMysql57Table1 WHERE k8 < 120 INTERSECT DISTINCT SELECT id as k8 FROM ${exMysqlTable}  """
        order_qt_sql78 """ WITH wnation AS (SELECT k7, k8 FROM $jdbcMysql57Table1) 
                            SELECT k8 FROM wnation WHERE k8 < 100
                            INTERSECT SELECT k8 FROM wnation WHERE k8 > 98 """
        order_qt_sql79 """ SELECT num FROM (SELECT 1 AS num FROM $jdbcMysql57Table1 WHERE k8=10 
                            INTERSECT SELECT 1 FROM $jdbcMysql57Table1 WHERE k8=20) T """
        order_qt_sql80 """ SELECT k8 FROM (SELECT k8 FROM $jdbcMysql57Table1 WHERE k8 < 100
                            INTERSECT SELECT k8 FROM $jdbcMysql57Table1 WHERE k8 > 95) as t1
                            UNION SELECT 4 """
        order_qt_sql81 """ SELECT k8, k8 / 2 FROM (SELECT k8 FROM $jdbcMysql57Table1 WHERE k8 < 10
                            INTERSECT SELECT k8 FROM $jdbcMysql57Table1 WHERE k8 > 4) T WHERE k8 % 2 = 0 order by k8 limit 3 """
        order_qt_sql82 """ SELECT k8 FROM (SELECT k8 FROM $jdbcMysql57Table1 WHERE k8 < 7
                            UNION SELECT k8 FROM $jdbcMysql57Table1 WHERE k8 > 21) as t1
                            INTERSECT SELECT 1 """
        order_qt_sql83 """ SELECT k8 FROM (SELECT k8 FROM $jdbcMysql57Table1 WHERE k8 < 100
                            INTERSECT SELECT k8 FROM $jdbcMysql57Table1 WHERE k8 > 95) as t1
                            UNION ALL SELECT 4 """
        order_qt_sql84 """ SELECT NULL, NULL INTERSECT SELECT NULL, NULL FROM $jdbcMysql57Table1 """
        order_qt_sql85 """ SELECT COUNT(*) FROM $jdbcMysql57Table1 INTERSECT SELECT COUNT(k8) FROM $jdbcMysql57Table1 HAVING SUM(k7) IS NOT NULL """
        order_qt_sql86 """ SELECT k8 FROM $jdbcMysql57Table1 WHERE k8 < 7 EXCEPT SELECT k8 FROM $jdbcMysql57Table1 WHERE k8 > 21 """
        order_qt_sql87 """ SELECT row_number() OVER (PARTITION BY k7) rn, k8 FROM $jdbcMysql57Table1 LIMIT 3 """
        order_qt_sql88 """ SELECT row_number() OVER (PARTITION BY k7 ORDER BY k8) rn FROM $jdbcMysql57Table1 LIMIT 3 """
        order_qt_sql89 """ SELECT row_number() OVER (ORDER BY k8) rn FROM $jdbcMysql57Table1 LIMIT 3 """
        order_qt_sql90 """ SELECT row_number() OVER () FROM $jdbcMysql57Table1 as a JOIN ${exMysqlTable} as b ON a.k8 = b.id WHERE a.k8 > 111 LIMIT 2 """
        order_qt_sql91 """ SELECT k7, k8, SUM(rn) OVER (PARTITION BY k8) c
                            FROM ( SELECT k7, k8, row_number() OVER (PARTITION BY k8) rn
                             FROM (SELECT * FROM $jdbcMysql57Table1 ORDER BY k8 desc LIMIT 10) as t1) as t2 limit 3 """


        // test for create external table use different type with original type
        sql  """ drop table if exists ${exMysqlTypeTable} """
        sql  """
               CREATE EXTERNAL TABLE ${exMysqlTypeTable} (
               `id` int NOT NULL,
               `count_value` varchar(100) NULL
               ) ENGINE=JDBC
               COMMENT "JDBC Mysql 外部表"
               PROPERTIES (
                "resource" = "$jdbcResourceMysql57",
                "table" = "ex_tb2",
                "table_type"="mysql"
               ); 
        """
        order_qt_sql """ select * from ${exMysqlTypeTable} order by id """


        order_qt_sql92 """ WITH a AS (SELECT k8 from $jdbcMysql57Table1), b AS (WITH a AS (SELECT k8 from $jdbcMysql57Table1) SELECT * FROM a) 
                            SELECT * FROM b order by k8 desc limit 5 """
        order_qt_sql93 """ SELECT CASE k8 WHEN 1 THEN CAST(1 AS decimal(4,1)) WHEN 2 THEN CAST(1 AS decimal(4,2)) 
                            ELSE CAST(1 AS decimal(4,3)) END FROM $jdbcMysql57Table1 limit 3"""
        order_qt_sql95 """ SELECT * from (SELECT k8 FROM $jdbcMysql57Table1 UNION (SELECT id as k8 FROM ${exMysqlTable}  UNION SELECT k7 as k8 FROM $jdbcMysql57Table1) 
                            UNION ALL SELECT products_id as k8 FROM $exMysqlTable1 ORDER BY k8 limit 3) as a  order by k8 limit 3"""
        order_qt_sql100 """ SELECT COUNT(*) FROM $jdbcMysql57Table1 WHERE EXISTS(SELECT max(id) FROM ${exMysqlTable}) """
        order_qt_sql103 """ SELECT count(*) FROM $jdbcMysql57Table1 n WHERE (SELECT count(*) FROM ${exMysqlTable} r WHERE n.k8 = r.id) > 1 """
        order_qt_sql105 """ SELECT count(*) AS numwait FROM $jdbcMysql57Table1 l1 WHERE
                            EXISTS(SELECT * FROM $jdbcMysql57Table1 l2 WHERE l2.k8 = l1.k8 )
                            AND NOT EXISTS(SELECT * FROM $jdbcMysql57Table1 l3 WHERE l3.k8= l1.k8) """
        order_qt_sql106 """ SELECT AVG(x) FROM (SELECT 1 AS x, k7 FROM $jdbcMysql57Table1) as a GROUP BY x, k7 """
        order_qt_sql107 """ WITH lineitem_ex AS (
                                SELECT k8,CAST(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CAST((k8 % 255) AS VARCHAR), '.'),
                                    CAST((k8 % 255) AS VARCHAR)), '.'),CAST(k8 AS VARCHAR)), '.' ),
                                    CAST(k8 AS VARCHAR)) as varchar) AS ip FROM $jdbcMysql57Table1)
                            SELECT SUM(length(l.ip)) as s FROM lineitem_ex l, ${exMysqlTable} p WHERE l.k8 = p.id order by s limit 3 """
        order_qt_sql108 """ SELECT RANK() OVER (PARTITION BY k7 ORDER BY COUNT(DISTINCT k8)) rnk
                            FROM $jdbcMysql57Table1 GROUP BY k7, k8 ORDER BY rnk limit 3"""
        order_qt_sql109 """ SELECT sum(k7) OVER(PARTITION BY k7 ORDER BY k8), count(k7) OVER(PARTITION BY k7 ORDER BY k6),
                            min(k8) OVER(PARTITION BY k11, k12 ORDER BY k8) FROM $jdbcMysql57Table1 ORDER BY 1, 2 limit 3 """
        order_qt_sql110 """ WITH t1 AS (SELECT k8 FROM $jdbcMysql57Table1 ORDER BY k7, k8 desc LIMIT 2),
                                    t2 AS (SELECT k8, sum(k8) OVER() AS x FROM t1),
                                    t3 AS (SELECT max(x) OVER() FROM t2) SELECT * FROM t3 limit 3"""
        order_qt_sql111 """ SELECT rank() OVER () FROM (SELECT k8 FROM $jdbcMysql57Table1 LIMIT 10) as t LIMIT 3 """
        order_qt_sql112 """ SELECT k7, count(DISTINCT k8) FROM $jdbcMysql57Table1 WHERE k8 > 110 GROUP BY GROUPING SETS ((), (k7)) """

        // TODO: check this, maybe caused by datasource in JDBC
        // test alter resource
        sql """alter resource $jdbcResourceMysql57 properties("password" = "1234567")"""
        test {
            sql """select count(*) from $jdbcMysql57Table1"""
            exception "Access denied for user"
        }
        sql """alter resource $jdbcResourceMysql57 properties("password" = "123456")"""

        // test for type check
        sql  """ drop table if exists ${exMysqlTypeTable} """
        sql  """
               CREATE EXTERNAL TABLE ${exMysqlTypeTable} (
               `id` bigint NOT NULL,
               `count_value` varchar(100) NULL
               ) ENGINE=JDBC
               COMMENT "JDBC Mysql 外部表"
               PROPERTIES (
                "resource" = "$jdbcResourceMysql57",
                "table" = "ex_tb2",
                "table_type"="mysql"
               ); 
        """

        test {
            sql """select * from ${exMysqlTypeTable} order by id"""
            exception "Fail to convert jdbc type of java.lang.Integer to doris type BIGINT on column: id"
        }

    }
}







