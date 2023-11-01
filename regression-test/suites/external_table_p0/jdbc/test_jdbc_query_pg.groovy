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

import java.nio.charset.Charset;

suite("test_jdbc_query_pg", "p0,external,pg,external_docker,external_docker_pg") {

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

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
        String exMysqlTypeTable = "doris_type_tb";
        String exMysqlTypeTable2 = "doris_type_tb2";

        println "yyy default charset: " + Charset.defaultCharset()

        sql """drop resource if exists $jdbcResourcePg14;"""
        sql """
            create external resource $jdbcResourcePg14
            properties (
                "type"="jdbc",
                "user"="postgres",
                "password"="123456",
                "jdbc_url"="jdbc:postgresql://${externalEnvIp}:$pg_14_port/postgres?currentSchema=doris_test",
                "driver_url"="${driver_url}",
                "driver_class"="org.postgresql.Driver"
            );
            """

        sql """drop table if exists $jdbcPg14Table1"""
        sql """
            CREATE EXTERNAL TABLE `$jdbcPg14Table1` (
                k1 boolean comment "中国",
                k2 char(100),
                k3 varchar(128),
                k4 date,
                k5 float,
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
        order_qt_sql2 """select * from $jdbcPg14Table1 order by k1,k2,k3,k4,k5,k6,k7,k8,k9,k10;"""


        // test for : doris query external table which is pg table's view
        sql  """ drop table if exists $dorisExTable1 """
        sql """
            CREATE EXTERNAL TABLE `$dorisExTable1` (
                `id` bigint(20) NULL COMMENT "",
                `code` varchar(100) NULL COMMENT "",
                `value` text NULL COMMENT "",
                `label` text NULL COMMENT "",
                `deleted` boolean NULL COMMENT "",
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
                `deleted` boolean NULL,
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
        sql """ drop view if exists $dorisViewName """
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
            w.id_code, w.weight, w.remark, f.herd_code, f.eno, f.gbcode, p.name as c_u_n, '2022-11-19 14:43:54' as compute_time,
            w.dt_str from $dorisViewName w 
            left join $dorisInTable3 f on f.ltid = w.rf_id and f.org_id = w.org_id and f.o_l_eFarmDesc = 1
            left join $dorisInTable4 p on p.id = cast(w.c_user as bigint) limit 1025;
        """
        order_qt_sql """ select count(*) from $dorisInTable1;"""
        order_qt_sql """ select * from $dorisInTable1 order by id limit 5; """


        // test for aggregate
        order_qt_sql1 """ SELECT COUNT(true) FROM $jdbcPg14Table1 """
        order_qt_sql2 """ SELECT COUNT(*) FROM $jdbcPg14Table1 WHERE k7 < k8 """
        order_qt_sql3 """ SELECT COUNT(*) FROM $jdbcPg14Table1 WHERE NOT k7 < k8 """
        order_qt_sql4 """ SELECT COUNT(*) FROM $jdbcPg14Table1 WHERE NULL """
        order_qt_sql5 """ SELECT COUNT(*) FROM $jdbcPg14Table1 WHERE NULLIF(k2, 'F') IS NULL """
        order_qt_sql6 """ SELECT COUNT(*) FROM $jdbcPg14Table1 WHERE NULLIF(k2, 'F') IS NOT NULL """
        order_qt_sql7 """ SELECT COUNT(*) FROM $jdbcPg14Table1 WHERE NULLIF(k2, 'F') = k2 """
        order_qt_sql8 """ SELECT COUNT(*) FROM $jdbcPg14Table1 WHERE COALESCE(NULLIF(k2, 'abc'), 'abc') = 'abc' """
        order_qt_sql9 """ SELECT COUNT(*) FROM $jdbcPg14Table1 WHERE k7 < k8 AND k8 > 30 AND k8 < 40 """
        order_qt_sql10 """ SELECT COUNT(*) FROM (SELECT k1 FROM $jdbcPg14Table1) x """
        order_qt_sql11 """ SELECT COUNT(*) FROM (SELECT k1, COUNT(*) FROM $jdbcPg14Table1 GROUP BY k1) x """
        order_qt_sql12 """ SELECT k1, c, count(*) FROM (SELECT k1, count(*) c FROM $jdbcPg14Table1 GROUP BY k1) as a GROUP BY k1, c """
        order_qt_sql13 """ SELECT k2, sum(CAST(NULL AS BIGINT)) FROM $jdbcPg14Table1 GROUP BY k2 """
        order_qt_sql14 """ SELECT `key`, COUNT(*) as c FROM (
                            SELECT CASE WHEN k8 % 3 = 0 THEN NULL WHEN k8 % 5 = 0 THEN 0 ELSE k8 END AS `key`
                            FROM $jdbcPg14Table1) as a GROUP BY `key` order by c desc limit 2"""
        order_qt_sql15 """ SELECT lines, COUNT(*) as c FROM (SELECT k7, COUNT(*) lines FROM $jdbcPg14Table1 GROUP BY k7) U GROUP BY lines order by c"""
        order_qt_sql16 """ SELECT COUNT(DISTINCT k8 + 1) FROM $jdbcPg14Table1 """
        order_qt_sql17 """ SELECT COUNT(*) FROM (SELECT DISTINCT k8 + 1 FROM $jdbcPg14Table1) t """
        order_qt_sql18 """ SELECT COUNT(DISTINCT k8), COUNT(*) from $jdbcPg14Table1 where k8 > 40 """
        order_qt_sql19 """ SELECT COUNT(DISTINCT k8) AS count, k7 FROM $jdbcPg14Table1 GROUP BY k7 ORDER BY count, k7 """
        order_qt_sql20 """ SELECT k2, k3, COUNT(DISTINCT k5), SUM(DISTINCT k8) FROM $jdbcPg14Table1 GROUP BY k2, k3 order by k2, k3 """
        order_qt_sql21 """ SELECT k2, COUNT(DISTINCT k7), COUNT(DISTINCT k8) FROM $jdbcPg14Table1 GROUP BY k2 """
        order_qt_sql22 """ SELECT SUM(DISTINCT x) FROM (SELECT k7, COUNT(DISTINCT k8) x FROM $jdbcPg14Table1 GROUP BY k7) t """
        order_qt_sql23 """ SELECT max(k8), COUNT(k7), sum(DISTINCT k6) FROM $jdbcPg14Table1 """
        order_qt_sql24 """ SELECT s, MAX(k6), SUM(a) FROM (SELECT k6, avg(k8) AS a, SUM(DISTINCT k7) AS s FROM $jdbcPg14Table1 GROUP BY k6) as b  group by s"""
        order_qt_sql25 """ SELECT COUNT(DISTINCT k8) FROM $jdbcPg14Table1 WHERE LENGTH(k2) > 2 """
        sql  """ drop table if exists $dorisExTable2 """
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
        order_qt_sql26 """ 
                        SELECT max(id), min(id), count(id) + 1, count(id)
                        FROM (SELECT DISTINCT k8 FROM $jdbcPg14Table1) AS r1
                        LEFT JOIN ${dorisExTable2} as a ON r1.k8 = a.id GROUP BY r1.k8 
                        HAVING sum(id) < 110 """
        order_qt_sql27 """ SELECT id BETWEEN 110 AND 115 from $dorisExTable2 GROUP BY id BETWEEN 110 AND 115;  """
        order_qt_sql28 """ SELECT CAST(id BETWEEN 1 AND 120 AS BIGINT) FROM $dorisExTable2 GROUP BY id """
        order_qt_sql29 """ SELECT CAST(50 BETWEEN id AND 120 AS BIGINT) FROM $dorisExTable2 GROUP BY id """
        order_qt_sql30 """ SELECT CAST(50 BETWEEN 1 AND id AS BIGINT) FROM $dorisExTable2 GROUP BY id """
        order_qt_sql31 """ SELECT CAST(id AS VARCHAR) as a, count(*) FROM $dorisExTable2 GROUP BY CAST(id AS VARCHAR) order by a """
        order_qt_sql32 """ SELECT NULLIF(k7, k8), count(*) as c FROM $jdbcPg14Table1 GROUP BY NULLIF(k7, k8) order by c desc"""
        order_qt_sql33 """ SELECT id + 1, id + 2, id + 3, id + 4, id + 5, id + 6, id + 7,id + 8, id + 9, id + 10, COUNT(*) AS c
                            FROM $dorisExTable2 GROUP BY id + 1, id + 2, id + 3, id + 4, id + 5, id + 6, id + 7,id + 8, id + 9, id + 10
                            ORDER BY c desc """
        order_qt_sql35 """ 
                        SELECT name,SUM(CAST(id AS BIGINT))
                        FROM $dorisExTable2
                        WHERE name = 'abc'
                        GROUP BY name
                        UNION
                        SELECT NULL, SUM(CAST(id AS BIGINT))
                        FROM $dorisExTable2
                        WHERE name = 'abd' """


        // test for distribute queries
        sql """ drop table if exists $dorisExTable1 """
        sql  """
            CREATE EXTERNAL TABLE `$dorisExTable1` (
                `id` int NULL COMMENT "",
                `name` varchar(20) NULL COMMENT ""
            ) ENGINE=JDBC
            PROPERTIES (
            "resource" = "$jdbcResourcePg14",
            "table" = "test3", 
            "table_type"="postgresql"
            );
        """
        order_qt_sql38 """ SELECT count(*) FROM ${dorisExTable1} WHERE id IN (SELECT k8 FROM $jdbcPg14Table1 WHERE k8 > 111); """
        sql """ create view if not exists aview as select k7, k8 from $jdbcPg14Table1; """
        order_qt_sql39 """ SELECT * FROM aview a JOIN aview b on a.k8 = b.k8 order by a.k8 desc limit 5 """
        order_qt_sql42 """ SELECT * FROM (SELECT * FROM $jdbcPg14Table1 WHERE k8 % 8 = 0) l JOIN ${dorisExTable1} o ON l.k8 = o.id """
        order_qt_sql43 """ SELECT * FROM (SELECT * FROM $jdbcPg14Table1 WHERE k8 % 8 = 0) l LEFT JOIN ${dorisExTable1} o ON l.k8 = o.id order by k8 limit 5"""
        order_qt_sql44 """ SELECT * FROM (SELECT * FROM $jdbcPg14Table1 WHERE k8 % 8 = 0) l RIGHT JOIN ${dorisExTable1} o ON l.k8 = o.id"""
        order_qt_sql45 """ SELECT * FROM (SELECT * FROM $jdbcPg14Table1 WHERE k8 % 8 = 0) l JOIN 
                            (SELECT id, COUNT(*) FROM ${dorisExTable1} WHERE id > 111 GROUP BY id ORDER BY id) o ON l.k8 = o.id """
        order_qt_sql46 """ SELECT * FROM (SELECT * FROM $jdbcPg14Table1 WHERE k8 % 8 = 0) l JOIN ${dorisExTable1} o ON l.k8 = o.id + 1"""
        order_qt_sql47 """ SELECT * FROM (
                            SELECT k8 % 120 AS a, k8 % 3 AS b
                            FROM $jdbcPg14Table1) l JOIN 
                            (SELECT t1.a AS a, SUM(t1.b) AS b, SUM(LENGTH(t2.name)) % 3 AS d
                                FROM ( SELECT id AS a, id % 3 AS b FROM ${dorisExTable1}) t1
                            JOIN ${dorisExTable1} t2 ON t1.a = t2.id GROUP BY t1.a) o
                            ON l.b = o.d AND l.a = o.a order by l.a desc limit 3"""
        // this pr fixed, wait for merge: https://github.com/apache/doris/pull/16442   
        order_qt_sql48 """ SELECT x, y, COUNT(*) as c FROM (SELECT k8, 0 AS x FROM $jdbcPg14Table1) a
                            JOIN (SELECT k8, 1 AS y FROM $jdbcPg14Table1) b ON a.k8 = b.k8 group by x, y order by c desc limit 3 """
        order_qt_sql49 """ SELECT * FROM (SELECT * FROM $jdbcPg14Table1 WHERE k8 % 120 > 110) l
                            JOIN (SELECT *, COUNT(1) OVER (PARTITION BY id ORDER BY id) FROM ${dorisExTable1}) o ON l.k8 = o.id """
        order_qt_sql50 """ SELECT COUNT(*) FROM $jdbcPg14Table1 as a LEFT OUTER JOIN ${dorisExTable1} as b ON a.k8 = b.id AND a.k8 > 111 WHERE a.k8 < 114 """
        order_qt_sql51 """ SELECT count(*) > 0 FROM $jdbcPg14Table1 JOIN ${dorisExTable1} ON (cast(1.2 AS FLOAT) = CAST(1.2 AS decimal(2,1))) """
        order_qt_sql52 """ SELECT count(*) > 0 FROM $jdbcPg14Table1 JOIN ${dorisExTable1} ON CAST((CASE WHEN (TRUE IS NOT NULL) THEN '1.2' ELSE '1.2' END) AS FLOAT) = CAST(1.2 AS decimal(2,1)) """
        order_qt_sql53 """ SELECT SUM(k8) FROM $jdbcPg14Table1 as a JOIN ${dorisExTable1} as b ON a.k8 = CASE WHEN b.id % 2 = 0 and b.name = 'abc' THEN b.id ELSE NULL END """
        order_qt_sql54 """ SELECT COUNT(*) FROM $jdbcPg14Table1 a JOIN ${dorisExTable1} b on not (a.k8 <> b.id) """
        order_qt_sql55 """ SELECT COUNT(*) FROM $jdbcPg14Table1 a JOIN ${dorisExTable1} b on not not not (a.k8 = b.id)  """
        order_qt_sql56 """ SELECT x + y FROM (
                           SELECT id, COUNT(*) x FROM ${dorisExTable1} GROUP BY id) a JOIN 
                            (SELECT k8, COUNT(*) y FROM $jdbcPg14Table1 GROUP BY k8) b ON a.id = b.k8 """
        order_qt_sql57 """ SELECT COUNT(*) FROM ${dorisExTable1} as a JOIN $jdbcPg14Table1 as b ON a.id = b.k8 AND a.name LIKE '%ab%' """
        order_qt_sql58 """ 
                        SELECT COUNT(*) FROM
                        (SELECT a.k8 AS o1, b.id AS o2 FROM $jdbcPg14Table1 as a LEFT OUTER JOIN ${dorisExTable1} as b 
                        ON a.k8 = b.id AND b.id < 114
                            UNION ALL
                        SELECT a.k8 AS o1, b.id AS o2 FROM $jdbcPg14Table1 as a RIGHT OUTER JOIN ${dorisExTable1} as b 
                        ON a.k8 = b.id AND b.id < 114 WHERE a.k8 IS NULL) as t1
                         WHERE o1 IS NULL OR o2 IS NULL """
        order_qt_sql59 """ SELECT COUNT(*) FROM $jdbcPg14Table1 as a JOIN ${dorisExTable1} as b ON a.k8 = 112 AND b.id = 112 """
        order_qt_sql60 """ WITH x AS (SELECT DISTINCT k8 FROM $jdbcPg14Table1 ORDER BY k8 LIMIT 10)
                            SELECT count(*) FROM x a JOIN x b on a.k8 = b.k8 """
        order_qt_sql61 """ SELECT COUNT(*) FROM (SELECT * FROM $jdbcPg14Table1 ORDER BY k8 desc LIMIT 5) a 
                            CROSS JOIN (SELECT * FROM $jdbcPg14Table1 ORDER BY k8 desc LIMIT 5) b """
        order_qt_sql62 """ SELECT a.k8 FROM (SELECT * FROM $jdbcPg14Table1 WHERE k8 < 119) a
                            CROSS JOIN (SELECT * FROM $jdbcPg14Table1 WHERE k8 > 100) b order by a.k8 desc limit 3"""
        order_qt_sql63 """ SELECT * FROM (SELECT 1 a) x CROSS JOIN (SELECT 2 b) y """
        order_qt_sql65 """ SELECT t.c FROM (SELECT 1) as t1 CROSS JOIN (SELECT 0 AS c UNION ALL SELECT 1) t """
        order_qt_sql66 """ SELECT t.c FROM (SELECT 1) as a CROSS JOIN (SELECT 0 AS c UNION ALL SELECT 1) t """
        order_qt_sql67 """ SELECT * FROM (SELECT * FROM $jdbcPg14Table1 ORDER BY k8 LIMIT 5) a
                            JOIN (SELECT * FROM $jdbcPg14Table1 ORDER BY k8 LIMIT 5) b ON 123 = 123
                            order by a.k8 desc limit 5"""
        order_qt_sql68 """ SELECT id, count(1) as c FROM $dorisExTable1 GROUP BY id
                            HAVING c IN (select k8 from $jdbcPg14Table1 where k8 = 2) """


        // test for order by
        order_qt_sql70 """ WITH t AS (SELECT 1 x, 2 y) SELECT x, y FROM t ORDER BY x, y """
        order_qt_sql71 """ WITH t AS (SELECT k8 x, k7 y FROM $jdbcPg14Table1) SELECT x, y FROM t ORDER BY x, y LIMIT 1 """
        order_qt_sql72 """ SELECT id X FROM ${dorisExTable1} ORDER BY x """


	// test for queries
        order_qt_sql73 """ SELECT k7, k8 FROM $jdbcPg14Table1 LIMIT 0 """
        order_qt_sql74 """ SELECT COUNT(k8) FROM $jdbcPg14Table1 """
        order_qt_sql75 """ SELECT COUNT(CAST(NULL AS BIGINT)) FROM $jdbcPg14Table1 """
        order_qt_sql76 """ SELECT k8 FROM $jdbcPg14Table1 WHERE k8 < 120 INTERSECT SELECT id as k8 FROM ${dorisExTable1}  """
        order_qt_sql77 """ SELECT k8 FROM $jdbcPg14Table1 WHERE k8 < 120 INTERSECT DISTINCT SELECT id as k8 FROM ${dorisExTable1}  """
        order_qt_sql78 """ WITH wnation AS (SELECT k7, k8 FROM $jdbcPg14Table1) 
                            SELECT k8 FROM wnation WHERE k8 < 100
                            INTERSECT SELECT k8 FROM wnation WHERE k8 > 98 """
        order_qt_sql79 """ SELECT num FROM (SELECT 1 AS num FROM $jdbcPg14Table1 WHERE k8=10 
                            INTERSECT SELECT 1 FROM $jdbcPg14Table1 WHERE k8=20) T """
        order_qt_sql80 """ SELECT k8 FROM (SELECT k8 FROM $jdbcPg14Table1 WHERE k8 < 100
                            INTERSECT SELECT k8 FROM $jdbcPg14Table1 WHERE k8 > 95) as t1
                            UNION SELECT 4 """
        order_qt_sql81 """ SELECT k8, k8 / 2 FROM (SELECT k8 FROM $jdbcPg14Table1 WHERE k8 < 10
                            INTERSECT SELECT k8 FROM $jdbcPg14Table1 WHERE k8 > 4) T WHERE k8 % 2 = 0 order by k8 limit 3 """
        order_qt_sql82 """ SELECT k8 FROM (SELECT k8 FROM $jdbcPg14Table1 WHERE k8 < 7
                            UNION SELECT k8 FROM $jdbcPg14Table1 WHERE k8 > 21) as t1
                            INTERSECT SELECT 1 """
        order_qt_sql83 """ SELECT k8 FROM (SELECT k8 FROM $jdbcPg14Table1 WHERE k8 < 100
                            INTERSECT SELECT k8 FROM $jdbcPg14Table1 WHERE k8 > 95) as t1
                            UNION ALL SELECT 4 """
        order_qt_sql84 """ SELECT NULL, NULL INTERSECT SELECT NULL, NULL FROM $jdbcPg14Table1 """
        order_qt_sql85 """ SELECT COUNT(*) FROM $jdbcPg14Table1 INTERSECT SELECT COUNT(k8) FROM $jdbcPg14Table1 HAVING SUM(k7) IS NOT NULL """
        order_qt_sql86 """ SELECT k8 FROM $jdbcPg14Table1 WHERE k8 < 7 EXCEPT SELECT k8 FROM $jdbcPg14Table1 WHERE k8 > 21 """
        order_qt_sql87 """ SELECT row_number() OVER (PARTITION BY k7) rn, k8 FROM $jdbcPg14Table1 LIMIT 3 """
        order_qt_sql88 """ SELECT row_number() OVER (PARTITION BY k7 ORDER BY k8) rn FROM $jdbcPg14Table1 LIMIT 3 """
        order_qt_sql89 """ SELECT row_number() OVER (ORDER BY k8) rn FROM $jdbcPg14Table1 LIMIT 3 """
        order_qt_sql90 """ SELECT row_number() OVER () FROM $jdbcPg14Table1 as a JOIN ${dorisExTable1} as b ON a.k8 = b.id WHERE a.k8 > 111 LIMIT 2 """
        order_qt_sql91 """ SELECT k7, k8, SUM(rn) OVER (PARTITION BY k8) c
                            FROM ( SELECT k7, k8, row_number() OVER (PARTITION BY k8) rn
                             FROM (SELECT * FROM $jdbcPg14Table1 ORDER BY k8 desc LIMIT 10) as t1) as t2 limit 3 """


        // test for create external table use different type with original type
        sql  """ drop table if exists ${exMysqlTypeTable} """
        sql  """
            CREATE EXTERNAL TABLE `$exMysqlTypeTable` (
                `id` int NULL COMMENT "",
                `name` varchar(120) NULL COMMENT ""
            ) ENGINE=JDBC
            PROPERTIES (
            "resource" = "$jdbcResourcePg14",
            "table" = "test3", 
            "table_type"="postgresql"
            );
        """
        order_qt_sql """ select * from ${exMysqlTypeTable} order by id """
        sql  """ drop table if exists ${exMysqlTypeTable2} """
        sql  """
               CREATE EXTERNAL TABLE ${exMysqlTypeTable2} (
                id1 smallint,
                id2 int,
                id3 boolean,
                id4 varchar(10),
                id5 bigint
               ) ENGINE=JDBC
               COMMENT "JDBC Mysql 外部表"
            PROPERTIES (
            "resource" = "$jdbcResourcePg14",
            "table" = "test9", 
            "table_type"="postgresql"
            );
        """
        order_qt_sql """ select * from ${exMysqlTypeTable2} order by id1 """


        order_qt_sql92 """ WITH a AS (SELECT k8 from $jdbcPg14Table1), b AS (WITH a AS (SELECT k8 from $jdbcPg14Table1) SELECT * FROM a) 
                            SELECT * FROM b order by k8 desc limit 5 """
        order_qt_sql93 """ SELECT CASE k8 WHEN 1 THEN CAST(1 AS decimal(4,1)) WHEN 2 THEN CAST(1 AS decimal(4,2)) 
                            ELSE CAST(1 AS decimal(4,3)) END FROM $jdbcPg14Table1 limit 3"""
        order_qt_sql95 """ SELECT * from (SELECT k8 FROM $jdbcPg14Table1 UNION (SELECT id as k8 FROM ${dorisExTable1}  UNION SELECT k7 as k8 FROM $jdbcPg14Table1) 
                            UNION ALL SELECT id as k8 FROM $exMysqlTypeTable ORDER BY id limit 3) as a order by k8 limit 3"""
        order_qt_sql100 """ SELECT COUNT(*) FROM $jdbcPg14Table1 WHERE EXISTS(SELECT max(id) FROM ${dorisExTable1}) """
        order_qt_sql103 """ SELECT count(*) FROM $jdbcPg14Table1 n WHERE (SELECT count(*) FROM ${dorisExTable1} r WHERE n.k8 = r.id) > 1 """
        order_qt_sql105 """ SELECT count(*) AS numwait FROM $jdbcPg14Table1 l1 WHERE
                            EXISTS(SELECT * FROM $jdbcPg14Table1 l2 WHERE l2.k8 = l1.k8 )
                            AND NOT EXISTS(SELECT * FROM $jdbcPg14Table1 l3 WHERE l3.k8= l1.k8) """
        order_qt_sql106 """ SELECT AVG(x) FROM (SELECT 1 AS x, k7 FROM $jdbcPg14Table1) as a GROUP BY x, k7 """
        order_qt_sql107 """ WITH lineitem_ex AS (
                                SELECT k8,CAST(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CAST((k8 % 255) AS VARCHAR), '.'),
                                    CAST((k8 % 255) AS VARCHAR)), '.'),CAST(k8 AS VARCHAR)), '.' ),
                                    CAST(k8 AS VARCHAR)) as varchar) AS ip FROM $jdbcPg14Table1)
                            SELECT SUM(length(l.ip)) FROM lineitem_ex l, ${dorisExTable1} p WHERE l.k8 = p.id """
        order_qt_sql108 """ SELECT RANK() OVER (PARTITION BY k7 ORDER BY COUNT(DISTINCT k8)) rnk
                            FROM $jdbcPg14Table1 GROUP BY k7, k8 ORDER BY rnk limit 3"""
        order_qt_sql109 """ SELECT sum(k7) OVER(PARTITION BY k7 ORDER BY k8), count(k7) OVER(PARTITION BY k7 ORDER BY k7),
                            min(k8) OVER(PARTITION BY k9, k10 ORDER BY k8) FROM $jdbcPg14Table1 ORDER BY 1, 2  limit 3 """
        order_qt_sql110 """ WITH t1 AS (SELECT k8 FROM $jdbcPg14Table1 ORDER BY k7, k8 desc LIMIT 2),
                                    t2 AS (SELECT k8, sum(k8) OVER() AS x FROM t1),
                                    t3 AS (SELECT max(x) OVER() FROM t2) SELECT * FROM t3 limit 3"""
        order_qt_sql111 """ SELECT rank() OVER () FROM (SELECT k8 FROM $jdbcPg14Table1 LIMIT 10) as t1 LIMIT 3 """
        order_qt_sql112 """ SELECT k7, count(DISTINCT k8) FROM $jdbcPg14Table1 WHERE k8 > 110 GROUP BY GROUPING SETS ((), (k7)) """

    }
}


