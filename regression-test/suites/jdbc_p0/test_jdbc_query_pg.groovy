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

suite("test_jdbc_query_pg", "p0") {

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_14_port = context.config.otherConfigs.get("pg_14_port")
        String jdbcResourcePg14 = "jdbc_resource_pg_14"
        String jdbcPg14Table1 = "jdbc_pg_14_table1"
        String dorisExTable1 = "doris_ex_table1";
        String dorisExTable2 = "doris_ex_table2";
        String dorisInTable1 = "doris_in_table1";
        String dorisInTable2 = "doris_in_table2";
        String dorisInTable3 = "doris_in_table3";
        String dorisInTable4 = "doris_in_table4";
        String dorisViewName = "doris_view_name";

        sql """drop resource if exists $jdbcResourcePg14;"""
        sql """
            create external resource $jdbcResourcePg14
            properties (
                "type"="jdbc",
                "user"="postgres",
                "password"="123456",
                "jdbc_url"="jdbc:postgresql://127.0.0.1:$pg_14_port/postgres?currentSchema=doris_test",
                "driver_url"="https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/postgresql-42.5.0.jar",
                "driver_class"="org.postgresql.Driver"
            );
            """

        sql """drop table if exists $jdbcPg14Table1"""
        sql """
            CREATE EXTERNAL TABLE `$jdbcPg14Table1` (
                k1 boolean,
                k2 char(100),
                k3 varchar(128),
                k4 date,
                k5 double,
                k6 smallint,
                k7 int,
                k8 bigint,
                k9 datetime,
                k10 decimal(10, 3)
            ) ENGINE=JDBC
            PROPERTIES (
            "resource" = "$jdbcResourcePg14",
            "table" = "test1",
            "table_type"="postgresql"
            );
            """
        order_qt_sql1 """select count(*) from $jdbcPg14Table1"""
        order_qt_sql2 """select * from $jdbcPg14Table1"""


        // test for : doris query external table which is pg table's view
        sql  """ drop table if exists $dorisExTable1 """
        sql """
            CREATE EXTERNAL TABLE `$dorisExTable1` (
                `id` bigint(20) NULL COMMENT "",
                `code` varchar(100) NULL COMMENT "",
                `value` text NULL COMMENT "",
                `label` text NULL COMMENT "",
                `deleted` int(11) NULL COMMENT "",
                `o_idx` int(11) NULL COMMENT ""
            ) ENGINE=JDBC
            PROPERTIES (
            "resource" = "$jdbcResourcePg14",
            "table" = "test2_view",
            "table_type"="postgresql"
            );
        """
        order_qt_sql """select * from $dorisExTable1"""


        // test for doris inner table join doris external table of pg table
        sql  """ drop table if exists $dorisExTable2 """
        sql  """ drop table if exists $dorisInTable2 """
        sql  """
            CREATE EXTERNAL TABLE `$dorisExTable2` (
                `id` int NULL COMMENT "",
                `name` varchar(20) NULL COMMENT ""
            ) ENGINE=JDBC
            PROPERTIES (
            "resource" = "$jdbcResourcePg14",
            "table" = "test3", 
            "table_type"="postgresql"
            );
        """
        sql """
            CREATE TABLE $dorisInTable2 (
                `id1` int NOT NULL,
                `name1` varchar(25) NOT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id1`, `name1`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`id1`) BUCKETS 2
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "in_memory" = "false",
                "storage_format" = "V2"
            );
        """
        sql """ insert into $dorisInTable2 values (123, 'zhangsan'), (124, 'lisi'); """
        order_qt_sql """ 
            select exter.*, inne.* 
            from ${dorisExTable2} exter
            join 
            ${dorisInTable2} inne
            on (exter.id = inne.id1) 
            order by exter.id;
        """


        // test for quotation marks in external table properties
        sql """ drop table if exists $dorisExTable1 """
        sql """
            CREATE EXTERNAL TABLE `$dorisExTable1` (
                `id` int NULL COMMENT ""
            ) ENGINE=JDBC
            PROPERTIES (
            "resource" = "$jdbcResourcePg14",
            "table" = "doris_test.test4",
            "table_type"="postgresql"
            );
        """
        order_qt_sql """ select * from $dorisExTable1 order by id; """


        // test for jsonb type
        sql """ drop table if exists $dorisExTable2 """
        sql """
            CREATE EXTERNAL TABLE `$dorisExTable2` (
                id int null comment '',
                `result` string null comment ''
            ) ENGINE=JDBC
            PROPERTIES (
            "resource" = "$jdbcResourcePg14",
            "table" = "test5",
            "table_type"="postgresql"
            );
        """
        order_qt_sql """ select * from $dorisExTable2 where id = 2; """


        // test for key word in doris external table
        sql """ drop table if exists $dorisExTable1 """
        sql """
            CREATE EXTERNAL TABLE `$dorisExTable1` (
                id int null comment '',
                `result` string null comment '',
                `limit` string null comment ''
            ) ENGINE=JDBC
            PROPERTIES (
            "resource" = "$jdbcResourcePg14",
            "table" = "test6",
            "table_type"="postgresql"
            );
        """
        order_qt_sql """ select id,result,`limit`  from $dorisExTable1 order by id; """


        // test for camel case
        sql """ drop table if exists $dorisExTable2 """
        sql """
            CREATE EXTERNAL TABLE `$dorisExTable2` (
                 id int null comment '',
                `QueryResult` string null comment ''
            ) ENGINE=JDBC
            PROPERTIES (
            "resource" = "$jdbcResourcePg14",
            "table" = "test7",
            "table_type"="postgresql"
            );
        """
        order_qt_sql """ select * from $dorisExTable1 order by id; """


        // test for insert more than 1024 to external view
        sql """ drop table if exists $dorisExTable1 """
        sql """ drop table if exists $dorisInTable1 """
        sql """ drop table if exists $dorisInTable3 """
        sql """ drop table if exists $dorisInTable4 """
        sql """
            CREATE EXTERNAL TABLE `$dorisExTable1` (
                `id` BIGINT(20) NULL,
                `c_user` TEXT NULL,
                `c_time` DATETIME NULL,
                `m_user` TEXT NULL,
                `m_time` DATETIME NULL,
                `app_id` BIGINT(20) NULL,
                `t_id` BIGINT(20) NULL,
                `deleted` TEXT NULL,
                `w_t_s` DATETIME NULL,
                `rf_id` TEXT NULL,
                `e_info` TEXT NULL,
                `f_id` BIGINT(20) NULL,
                `id_code` TEXT NULL
            ) ENGINE=JDBC
            PROPERTIES (
            "resource" = "$jdbcResourcePg14",
            "table" = "test8",
            "table_type"="postgresql"
            );
        """
        sql """
            CREATE TABLE $dorisInTable1 (
            `org_id` bigint(20) NULL,
            `id` bigint(20) NULL,
            `c_user` text NULL,
            `c_time` datetime NULL,
            `m_user` text NULL,
            `m_time` datetime NULL,
            `app_id` bigint(20) NULL,
            `t_id` bigint(20) NULL,
            `w_t_s` datetime NULL,
            `rf_id` text NULL,
            `e_info` text NULL,
            `id_code` text NULL,
            `weight` decimal(10, 2) NULL,
            `remark` text NULL,
            `herd_code` text NULL,
            `e_no` text NULL,
            `gbcode` text NULL,
            `c_u_n` text NULL,
            `compute_time` datetime NULL,
            `dt_str` text NULL
            ) ENGINE=OLAP
              DUPLICATE KEY(`org_id`)
              COMMENT "OLAP"
              DISTRIBUTED BY HASH(`org_id`) BUCKETS 4
              PROPERTIES (
              "replication_allocation" = "tag.location.default: 1",
              "in_memory" = "false",
              "storage_format" = "V2"
              );
        """
        sql """
            CREATE TABLE $dorisInTable3 (
            `id` bigint(20) NULL,
            `t_id` bigint(20) NULL,
            `org_id` bigint(20) NULL,
            `herd_code` text NULL,
            `herd_name` text NULL,
            `yz_id` text NULL,
            `gbcode` text NULL,
            `status` text NULL,
            `phystatus` text NULL,
            `mngstatus` text NULL,
            `statusDate` datetime NULL,
            `phystatusDate` datetime NULL,
            `mngstatusDate` datetime NULL,
            `b_code` text NULL,
            `b_name` text NULL,
            `b_type_code` text NULL,
            `b_type_name` text NULL,
            `s_type_code` text NULL,
            `s_type_name` text NULL,
            `sex` text NULL,
            `birthDate` datetime NULL,
            `fId` bigint(20) NULL,
            `mId` bigint(20) NULL,
            `bW` decimal(10, 2) NULL,
            `eno` text NULL,
            `sType` int(11) NULL,
            `sTypeName` text NULL,
            `iniParity` int(11) NULL,
            `curParity` int(11) NULL,
            `entryDate` datetime NULL,
            `entryW` decimal(10, 2) NULL,
            `enterFDate` datetime NULL,
            `enterFW` decimal(10, 2) NULL,
            `weight` decimal(10, 2) NULL,
            `c_id` bigint(20) NULL,
            `groupCode` text NULL,
            `fstMatingDate` datetime NULL,
            `life_fstMatingDate` datetime NULL,
            `fstHeatDate` datetime NULL,
            `life_fstHeatDate` datetime NULL,
            `dataCalDate` datetime NULL,
            `remark` text NULL,
            `compute_time` datetime NULL,
            `tType` text NULL,
            `data_source_type` int(11) NULL,
            `ltid` text NULL,
            `removeDate` datetime NULL,
            `enterFDesc` int(11) NULL,
            `o_l_eFarmDesc` int(11) NULL,
            `left_n` int(11) NULL,
            `right_n` int(11) NULL,
            `location_id` bigint(20) NULL,
            `invent_start_date` date NULL,
            `invent_end_date` date NULL,
            `supplier_name` text NULL,
            `source_f` text NULL
          ) ENGINE=OLAP
          UNIQUE KEY(`id`)
          COMMENT "OLAP"
          DISTRIBUTED BY HASH(`id`) BUCKETS 2
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "in_memory" = "false",
          "storage_format" = "V2"
          );
        """
        sql """
            CREATE TABLE $dorisInTable4 (
            `id` BIGINT(20) NULL COMMENT "",
            `app_id` BIGINT(20) NULL COMMENT "",
            `type` TINYINT(4) NULL COMMENT "",
            `name` VARCHAR(255) NULL COMMENT "",
            `nname` VARCHAR(255) NULL COMMENT "",
            `mobile` VARCHAR(255) NULL COMMENT "",
            `email` VARCHAR(255) NULL COMMENT "",
            `img_url` VARCHAR(1000) NULL COMMENT "",
            `password` VARCHAR(64) NULL COMMENT "",
            `expired_time` DATETIME NULL COMMENT "",
            `ac_type` TINYINT(4) NULL COMMENT "",
            `status` TINYINT(4) NULL COMMENT "",
            `f_time` DATETIME NULL COMMENT "",
            `salt` VARCHAR(64) NULL COMMENT "",
            `deleted` BOOLEAN NULL COMMENT "",
            `c_user` VARCHAR(50) NULL COMMENT "",
            `c_time` DATETIME NULL COMMENT "",
            `m_user` VARCHAR(50) NULL COMMENT "",
            `m_time` DATETIME NULL COMMENT "",
            `remark` VARCHAR(250) NULL COMMENT "",
            `s_c_r` VARCHAR(500) NULL COMMENT "",
            `r_type` TINYINT(4) NULL COMMENT "",
            `yz_id` VARCHAR(50) NULL COMMENT "",
            `yz_id_status` VARCHAR(10) NULL COMMENT "",
            `yz_id_apply_status` VARCHAR(10) NULL COMMENT "",
            `sex` VARCHAR(20) NULL COMMENT "",
            `signature` VARCHAR(1000) NULL COMMENT "",
            `username` VARCHAR(255) NULL COMMENT "",
            `idcard` VARCHAR(40) NULL COMMENT "",
            `username_update_time` DATETIME NULL COMMENT "",
            `p256password` VARCHAR(256) NULL COMMENT "",
            `p256salt` VARCHAR(64) NULL COMMENT "",
            `e_password` VARCHAR(255) NULL COMMENT "",
            `id_f_image_id` BIGINT(20) NULL COMMENT "",
            `id_b_image_id` BIGINT(20) NULL COMMENT "",
            `id_c_status` TINYINT(4) NULL COMMENT "",
            `n_search` VARCHAR(500) NULL COMMENT "",
            `has_open_protect` BOOLEAN NULL COMMENT "",
            `has_c` BOOLEAN NULL COMMENT "",
            `province_code` VARCHAR(255) NULL COMMENT "",
            `city_code` VARCHAR(255) NULL COMMENT "",
            `area_code` VARCHAR(255) NULL COMMENT "",
            `date_of_birth` DATE NULL COMMENT "",
            `ts_ms` BIGINT(20) NULL COMMENT "",
            `lsn` BIGINT(20) NULL COMMENT ""
          ) ENGINE=OLAP
          UNIQUE KEY(`id`)
          COMMENT "OLAP"
          DISTRIBUTED BY HASH(`id`) BUCKETS 2
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "in_memory" = "false",
          "storage_format" = "V2"
          ); 
        """
        sql """
            CREATE VIEW $dorisViewName as select `ID` AS `ID`, `c_user` AS `c_user`, 
            `c_time` AS `c_time`, `m_user` AS `m_user`, `m_time` AS `m_time`, 
            `app_id` AS `app_id`, `t_id` AS `t_id`, `w_t_s` AS `w_t_s`, 
            `rf_id` AS `rf_id`, `e_info` AS `e_info`, `f_id` AS `org_id`, `id_code` AS `id_code`,
            ROUND( CAST ( get_json_string ( `e_info`, '\$.weight' ) AS DECIMAL ( 10, 2 )), 2 ) AS `weight`,
            get_json_string ( `e_info`, '\$.remark' ) AS `remark`,to_date( `w_t_s` ) AS `dt_str`   
            from $dorisExTable1 
        """
        sql """
            insert into $dorisInTable1 
            (id, c_user, c_time, m_user, m_time, app_id, t_id, w_t_s, rf_id, e_info, org_id, id_code, weight, remark, 
            herd_code, e_no, gbcode, c_u_n, compute_time, dt_str)
            select w.id, w.c_user, w.c_time, w.m_user, w.m_time,w.app_id, w.t_id, w.w_t_s, w.rf_id, w.e_info, w.org_id,
            w.id_code, w.weight, w.remark, f.herd_code, f.eno, f.gbcode, p.name as c_u_n, NOW() as compute_time,
            w.dt_str from $dorisViewName w 
            left join $dorisInTable3 f on f.ltid = w.rf_id and f.org_id = w.org_id and f.o_l_eFarmDesc = 1
            left join $dorisInTable4 p on p.id = cast(w.c_user as bigint) limit 1025;
        """
        order_qt_sql """ select count(*) from $dorisInTable1;"""
        order_qt_sql """ select * from $dorisInTable1 order by id limit 5; """

    }
}


