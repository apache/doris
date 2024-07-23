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

suite("test_mysql_jdbc_catalog", "p0,external,mysql,external_docker,external_docker_mysql") {
    qt_sql """select current_catalog()"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String user = "test_jdbc_user";
        String pwd = '123456';
        def tokens = context.config.jdbcUrl.split('/')
        def url = tokens[0] + "//" + tokens[2] + "/" + "information_schema" + "?"
        String catalog_name = "mysql_jdbc_catalog";
        String internal_db_name = "regression_test_jdbc_catalog_p0";
        String ex_db_name = "doris_test";
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        String inDorisTable = "test_mysql_jdbc_doris_in_tb";
        String ex_tb0 = "ex_tb0";
        String ex_tb1 = "ex_tb1";
        String ex_tb2 = "ex_tb2";
        String ex_tb3 = "ex_tb3";
        String ex_tb4 = "ex_tb4";
        String ex_tb5 = "ex_tb5";
        String ex_tb6 = "ex_tb6";
        String ex_tb7 = "ex_tb7";
        String ex_tb8 = "ex_tb8";
        String ex_tb9 = "ex_tb9";
        String ex_tb10 = "ex_tb10";
        String ex_tb11 = "ex_tb11";
        String ex_tb12 = "ex_tb12";
        String ex_tb13 = "ex_tb13";
        String ex_tb14 = "ex_tb14";
        String ex_tb15 = "ex_tb15";
        String ex_tb16 = "ex_tb16";
        String ex_tb17 = "ex_tb17";
        String ex_tb18 = "ex_tb18";
        String ex_tb19 = "ex_tb19";
        String ex_tb20 = "ex_tb20";
        String ex_tb21 = "test_key_word";
        String test_insert = "test_insert";
        String test_insert2 = "test_insert2";
        String test_insert_all_types = "test_mysql_insert_all_types";
        String test_ctas = "test_mysql_ctas";
        String auto_default_t = "auto_default_t";
        String dt = "dt";
        String dt_null = "dt_null";
        String test_zd = "test_zd"

        try_sql("DROP USER ${user}")
        sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""

        sql """create database if not exists ${internal_db_name}; """

        sql """drop catalog if exists ${catalog_name} """

        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false&zeroDateTimeBehavior=convertToNull",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver"
        );"""

        sql """use ${internal_db_name}"""
        sql  """ drop table if exists ${internal_db_name}.${inDorisTable} """
        sql  """
              CREATE TABLE ${internal_db_name}.${inDorisTable} (
                `id` INT NULL COMMENT "主键id",
                `name` string NULL COMMENT "名字"
                ) DISTRIBUTED BY HASH(id) BUCKETS 10
                PROPERTIES("replication_num" = "1");
        """

        // used for testing all types
        sql  """ drop table if exists ${internal_db_name}.${test_insert_all_types} """
        sql  """
                CREATE TABLE ${internal_db_name}.${test_insert_all_types} (
                    `tinyint_u` SMALLINT,
                    `smallint_u` INT,
                    `mediumint_u` INT,
                    `int_u` BIGINT,
                    `bigint_u` LARGEINT,
                    `decimal_u` DECIMAL(18, 5),
                    `double_u` DOUBLE,
                    `float_u` FLOAT,
                    `boolean` TINYINT,
                    `tinyint` TINYINT,
                    `smallint` SMALLINT,
                    `year` SMALLINT,
                    `mediumint` INT,
                    `int` INT,
                    `bigint` BIGINT,
                    `date` DATE,
                    `timestamp` DATETIME(4) null,
                    `datetime` DATETIME,
                    `float` FLOAT,
                    `double` DOUBLE,
                    `decimal` DECIMAL(12, 4),
                    `char` CHAR(5),
                    `varchar` VARCHAR(10),
                    `time` STRING,
                    `text` STRING,
                    `blob` STRING,
                    `json` JSON,
                    `set` STRING,
                    `bit` STRING,
                    `binary` STRING,
                    `varbinary` STRING,
                    `enum` STRING
                ) DISTRIBUTED BY HASH(tinyint_u) BUCKETS 10
                PROPERTIES("replication_num" = "1");
        """

        qt_sql """select current_catalog()"""
        sql """switch ${catalog_name}"""
        qt_sql """select current_catalog()"""
        def res_dbs_log = sql "show databases;"
		for(int i = 0;i < res_dbs_log.size();i++) {
			def tbs = sql "show tables from  `${res_dbs_log[i][0]}`"
			log.info( "database = ${res_dbs_log[i][0]} => tables = "+tbs.toString())
		}
        try {
        
            sql """ use ${ex_db_name}"""

            order_qt_ex_tb0  """ select id, name from ${ex_tb0} order by id; """
            sql  """ insert into internal.${internal_db_name}.${inDorisTable} select id, name from ${ex_tb0}; """
            order_qt_in_tb  """ select id, name from internal.${internal_db_name}.${inDorisTable} order by id; """

            order_qt_ex_tb1  """ select * from ${ex_tb1} order by id; """
            order_qt_ex_tb2  """ select * from ${ex_tb2} order by id; """
            order_qt_ex_tb3  """ select * from ${ex_tb3} order by game_code; """
            order_qt_ex_tb4  """ select * from ${ex_tb4} order by products_id; """
            order_qt_ex_tb5  """ select * from ${ex_tb5} order by id; """
            order_qt_ex_tb6  """ select * from ${ex_tb6} order by id; """
            order_qt_ex_tb7  """ select * from ${ex_tb7} order by id; """
            order_qt_ex_tb8  """ select * from ${ex_tb8} order by uid; """
            order_qt_ex_tb9  """ select * from ${ex_tb9} order by c_date; """
            order_qt_ex_tb10  """ select * from ${ex_tb10} order by aa; """
            order_qt_ex_tb11  """ select * from ${ex_tb11} order by aa; """
            order_qt_ex_tb12  """ select * from ${ex_tb12} order by cc; """
            order_qt_ex_tb13  """ select * from ${ex_tb13} order by name; """
            order_qt_ex_tb14  """ select * from ${ex_tb14} order by tid; """
            order_qt_ex_tb15  """ select * from ${ex_tb15} order by col1; """
            order_qt_ex_tb16  """ select * from ${ex_tb16} order by id; """
            order_qt_ex_tb17  """ select * from ${ex_tb17} order by id; """
            order_qt_ex_tb18  """ select * from ${ex_tb18} order by num_tinyint; """
            order_qt_ex_tb19  """ select * from ${ex_tb19} order by date_value; """
            order_qt_ex_tb20  """ select * from ${ex_tb20} order by decimal_normal; """
            order_qt_ex_tb21_1  """ select `key`, `id` from ${ex_tb21} where `key` = 2 order by id;"""
            order_qt_ex_tb21_2  """ select `key`, `id` from ${ex_tb21} where `key` like 2 order by id;"""
            order_qt_ex_tb21_3  """ select `key`, `id` from ${ex_tb21} where `key` in (1,2) order by id;"""
            order_qt_ex_tb21_4  """ select `key`, `id` from ${ex_tb21} where abs(`key`) = 2 order by id;"""
            order_qt_ex_tb21_5  """ select `key`, `id` from ${ex_tb21} where `key` between 1 and 2 order by id;"""
            order_qt_ex_tb21_6  """ select `key`, `id` from ${ex_tb21} where `key` = case when id = 1 then 1 else 0 end order by id;"""
            order_qt_ex_tb21_7  """ select (`key` +1) as k, `id` from ${ex_tb21} having abs(k) = 2 order by id;"""
            order_qt_ex_tb21_8  """ select `key` as k, `id` from ${ex_tb21} having abs(k) = 2 order by id;"""
            order_qt_information_schema """ show tables from information_schema; """
            order_qt_dt """select * from ${dt}; """
            order_qt_dt_null """select * from ${dt_null} order by 1; """
            order_qt_test_dz """select * from ${test_zd} order by 1; """
            order_qt_test_filter_not """select * from ${ex_tb13} where name not like '%张三0%' order by 1; """
            explain {
                sql("select `datetime` from all_types where to_date(`datetime`) = '2012-10-25';")
                contains """ SELECT `datetime` FROM `doris_test`.`all_types` WHERE (date(`datetime`) = '2012-10-25')"""
            }

            explain {
                sql("select /*+ SET_VAR(enable_ext_func_pred_pushdown = false) */ `datetime` from all_types where to_date(`datetime`) = '2012-10-25';")
                contains """SELECT `datetime` FROM `doris_test`.`all_types`"""
            }

            // test insert
            String uuid1 = UUID.randomUUID().toString();
            connect(user=user, password="${pwd}", url=url) {
                try {
                    sql """ insert into ${catalog_name}.${ex_db_name}.${test_insert} values ('${uuid1}', 'doris1', 18) """
                    fail()
                } catch (Exception e) {
                    log.info(e.getMessage())
                }
            }

            sql """GRANT LOAD_PRIV ON ${catalog_name}.${ex_db_name}.${test_insert} TO ${user}"""

            connect(user=user, password="${pwd}", url=url) {
                try {
                    sql """ insert into ${catalog_name}.${ex_db_name}.${test_insert} values ('${uuid1}', 'doris1', 18) """
                } catch (Exception e) {
                    fail();
                }
            }
            order_qt_test_insert1 """ select name, age from ${test_insert} where id = '${uuid1}' order by age """

            String uuid2 = UUID.randomUUID().toString();
            sql """ insert into ${test_insert} values ('${uuid2}', 'doris2', 19), ('${uuid2}', 'doris3', 20) """
            order_qt_test_insert2 """ select name, age from ${test_insert} where id = '${uuid2}' order by age """

            sql """ insert into ${test_insert} select * from ${test_insert} where id = '${uuid2}' """
            order_qt_test_insert3 """ select name, age from ${test_insert} where id = '${uuid2}' order by age """

            String uuid3 = UUID.randomUUID().toString();
            sql """ INSERT INTO ${test_insert2} VALUES
                    ('${uuid3}', true, 'abcHa1.12345', '1.123450xkalowadawd', '2022-10-01', 3.14159, 1, 2, 0, 100000, 1.2345678, 24.000, '07:09:51', '2022', '2022-11-27 07:09:51', '2022-11-27 07:09:51'); """
            order_qt_test_insert4 """ select k1,k2,k3,k4,k5,k6,k7,k8,k9,k10,k11,k12,k13,k14,k15 from ${test_insert2} where id = '${uuid3}' """
        } finally {
			res_dbs_log = sql "show databases;"
			for(int i = 0;i < res_dbs_log.size();i++) {
				def tbs = sql "show tables from  `${res_dbs_log[i][0]}`"
				log.info( "database = ${res_dbs_log[i][0]} => tables = "+tbs.toString())
			}
		}
        sql """ drop catalog if exists ${catalog_name} """

        // test only_specified_database argument
        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "only_specified_database" = "true"
        );"""

        sql """switch ${catalog_name}"""

        qt_specified_database_1   """ show databases; """

        sql """ drop catalog if exists ${catalog_name} """

        // test only_specified_database and include_database_list argument
        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}?useSSL=false",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "only_specified_database" = "true",
            "include_database_list" = "doris_test"
        );"""

        sql """switch ${catalog_name}"""

        qt_specified_database_2   """ show databases; """

        sql """ drop catalog if exists ${catalog_name} """

        // test only_specified_database and exclude_database_list argument
        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}?useSSL=false",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "only_specified_database" = "true",
            "exclude_database_list" = "doris_test"
        );"""

        sql """switch ${catalog_name}"""

        qt_specified_database_3   """ show databases; """

        sql """ drop catalog if exists ${catalog_name} """

        // test include_database_list and exclude_database_list have overlapping items case
        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}?useSSL=false",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "only_specified_database" = "true",
            "include_database_list" = "doris_test",
            "exclude_database_list" = "doris_test"
        );"""

        sql """switch ${catalog_name}"""

        qt_specified_database_4   """ show databases; """

        sql """ drop catalog if exists ${catalog_name} """

        // test old create-catalog syntax for compatibility
        sql """ CREATE CATALOG ${catalog_name} PROPERTIES (
            "type"="jdbc",
            "jdbc.user"="root",
            "jdbc.password"="123456",
            "jdbc.jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false",
            "jdbc.driver_url" = "${driver_url}",
            "jdbc.driver_class" = "com.mysql.cj.jdbc.Driver");
        """
        sql """ switch ${catalog_name} """
        
        res_dbs_log = sql "show databases;"
		for(int i = 0;i < res_dbs_log.size();i++) {
			def tbs = sql "show tables from  `${res_dbs_log[i][0]}`"
			log.info( "database = ${res_dbs_log[i][0]} => tables = "+tbs.toString())
		}
        try {
            sql """ use ${ex_db_name} """
            order_qt_ex_tb1  """ select * from ${ex_tb1} order by id; """

            // test all types supported by MySQL
            sql """use doris_test;"""
            qt_mysql_all_types """select * from all_types order by tinyint_u;"""

            // test insert into internal.db.table select * from all_types
            sql """ insert into internal.${internal_db_name}.${test_insert_all_types} select * from all_types; """
            order_qt_select_insert_all_types """ select * from internal.${internal_db_name}.${test_insert_all_types} order by tinyint_u; """

            // test CTAS
            sql  """ drop table if exists internal.${internal_db_name}.${test_ctas} """
            sql """ create table internal.${internal_db_name}.${test_ctas}
                    PROPERTIES("replication_num" = "1")
                    AS select * from all_types;
                """

            order_qt_ctas """select * from internal.${internal_db_name}.${test_ctas} order by tinyint_u;"""

            order_qt_ctas_desc """desc internal.${internal_db_name}.${test_ctas};"""
        } finally {
			res_dbs_log = sql "show databases;"
			for(int i = 0;i < res_dbs_log.size();i++) {
				def tbs = sql "show tables from  `${res_dbs_log[i][0]}`"
				log.info( "database = ${res_dbs_log[i][0]} => tables = "+tbs.toString())
			}
		}
        sql """ drop catalog if exists ${catalog_name} """

        // test mysql view
        sql """ drop catalog if exists view_catalog """
        sql """ CREATE CATALOG view_catalog PROPERTIES (
            "type"="jdbc",
            "jdbc.user"="root",
            "jdbc.password"="123456",
            "jdbc.jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false",
            "jdbc.driver_url" = "${driver_url}",
            "jdbc.driver_class" = "com.mysql.cj.jdbc.Driver");
        """
        qt_mysql_view """ select * from view_catalog.doris_test.mysql_view order by col_1;"""
        sql """ drop catalog if exists view_catalog; """

        sql """ drop catalog if exists mysql_fun_push_catalog """
        sql """ CREATE CATALOG mysql_fun_push_catalog PROPERTIES (
            "type"="jdbc",
            "jdbc.user"="root",
            "jdbc.password"="123456",
            "jdbc.jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false",
            "jdbc.driver_url" = "${driver_url}",
            "jdbc.driver_class" = "com.mysql.cj.jdbc.Driver");
        """

        sql """switch mysql_fun_push_catalog"""
        sql """ use ${ex_db_name}"""
        sql """ set enable_ext_func_pred_pushdown = "true"; """
        order_qt_filter1 """select * from ${ex_tb17} where id = 1; """
        order_qt_filter2 """select * from ${ex_tb17} where 1=1 order by 1; """
        order_qt_filter3 """select * from ${ex_tb17} where id = 1 and 1 = 1; """
        order_qt_date_trunc """ SELECT timestamp0  from dt where DATE_TRUNC(date_sub(timestamp0,INTERVAL 9 HOUR),'hour') > '2011-03-03 17:39:05'; """
        order_qt_money_format """ select k8 from test1 where money_format(k8) = '1.00'; """
        explain {
            sql("select k8 from test1 where money_format(k8) = '1.00';")

            contains "QUERY: SELECT `k8` FROM `doris_test`.`test1`"
        }
        explain {
            sql("select k12 from test1 where negative(k12) = 1;")

            contains "QUERY: SELECT `k12` FROM `doris_test`.`test1`"
        }
        explain {
            sql ("SELECT timestamp0  from dt where DATE_TRUNC(date_sub(timestamp0,INTERVAL 9 HOUR),'hour') > '2011-03-03 17:39:05';")

            contains "QUERY: SELECT `timestamp0` FROM `doris_test`.`dt`"
        }
        explain {
            sql ("SELECT timestamp0  from dt where DATE_TRUNC(date_sub(timestamp0,INTERVAL 9 HOUR),'hour') > '2011-03-03 17:39:05' and timestamp0 > '2022-01-01';")

            contains "QUERY: SELECT `timestamp0` FROM `doris_test`.`dt` WHERE (`timestamp0` > '2022-01-01 00:00:00')"
        }
        explain {
            sql ("select k6, k8 from test1 where nvl(k6, null) = 1;")

            contains "QUERY: SELECT `k6`, `k8` FROM `doris_test`.`test1` WHERE ((ifnull(`k6`, NULL) = 1))"
        }
        explain {
            sql ("select k6, k8 from test1 where nvl(nvl(k6, null),null) = 1;")

            contains "QUERY: SELECT `k6`, `k8` FROM `doris_test`.`test1` WHERE ((ifnull(ifnull(`k6`, NULL), NULL) = 1))"
        }
        sql """ set enable_ext_func_pred_pushdown = "false"; """
        explain {
            sql ("select k6, k8 from test1 where nvl(k6, null) = 1 and k8 = 1;")

            contains "QUERY: SELECT `k6`, `k8` FROM `doris_test`.`test1` WHERE ((`k8` = 1))"
        }
        sql """ set enable_ext_func_pred_pushdown = "true"; """
        // test date_add
        order_qt_date_add_year """ select * from test_zd where date_add(d_z,interval 1 year) = '2023-01-01' order by 1; """
        explain {
            sql("select * from test_zd where date_add(d_z,interval 1 year) = '2023-01-01' order by 1;")

            contains " QUERY: SELECT `id`, `d_z` FROM `doris_test`.`test_zd` WHERE (`d_z` = '2022-01-01')"
        }
        order_qt_date_add_month """ select * from test_zd where date_add(d_z,interval 1 month) = '2022-02-01' order by 1; """
        explain {
            sql("select * from test_zd where date_add(d_z,interval 1 month) = '2022-02-01' order by 1;")

            contains " QUERY: SELECT `id`, `d_z` FROM `doris_test`.`test_zd` WHERE (`d_z` = '2022-01-01')"
        }
        order_qt_date_add_week """ select * from test_zd where date_add(d_z,interval 1 week) = '2022-01-08' order by 1; """
        explain {
            sql("select * from test_zd where date_add(d_z,interval 1 week) = '2022-01-08' order by 1;")

            contains " QUERY: SELECT `id`, `d_z` FROM `doris_test`.`test_zd` WHERE (`d_z` = '2022-01-01')"
        }
        order_qt_date_add_day """ select * from test_zd where date_add(d_z,interval 1 day) = '2022-01-02' order by 1; """
        explain {
            sql("select * from test_zd where date_add(d_z,interval 1 day) = '2022-01-02' order by 1;")

            contains " QUERY: SELECT `id`, `d_z` FROM `doris_test`.`test_zd` WHERE (`d_z` = '2022-01-01')"
        }
        order_qt_date_add_hour """ select * from test_zd where date_add(d_z,interval 1 hour) = '2022-01-01 01:00:00' order by 1; """
        explain {
            sql("select * from test_zd where date_add(d_z,interval 1 hour) = '2022-01-01 01:00:00' order by 1;")

            contains " QUERY: SELECT `id`, `d_z` FROM `doris_test`.`test_zd` WHERE (`d_z` = '2022-01-01')"
        }
        order_qt_date_add_min """ select * from test_zd where date_add(d_z,interval 1 minute) = '2022-01-01 00:01:00' order by 1; """
        explain {
            sql("select * from test_zd where date_add(d_z,interval 1 minute) = '2022-01-01 00:01:00' order by 1;")

            contains " QUERY: SELECT `id`, `d_z` FROM `doris_test`.`test_zd` WHERE (`d_z` = '2022-01-01')"
        }
        order_qt_date_add_sec """ select * from test_zd where date_add(d_z,interval 1 second) = '2022-01-01 00:00:01' order by 1; """
        explain {
            sql("select * from test_zd where date_add(d_z,interval 1 second) = '2022-01-01 00:00:01' order by 1;")

            contains " QUERY: SELECT `id`, `d_z` FROM `doris_test`.`test_zd` WHERE (`d_z` = '2022-01-01')"
        }
        // date_sub
        order_qt_date_sub_year """ select * from test_zd where date_sub(d_z,interval 1 year) = '2021-01-01' order by 1; """
        explain {
            sql("select * from test_zd where date_sub(d_z,interval 1 year) = '2021-01-01' order by 1;")

            contains " QUERY: SELECT `id`, `d_z` FROM `doris_test`.`test_zd` WHERE (`d_z` = '2022-01-01')"
        }
        order_qt_date_sub_month """ select * from test_zd where date_sub(d_z,interval 1 month) = '2021-12-01' order by 1; """
        explain {
            sql("select * from test_zd where date_sub(d_z,interval 1 month) = '2021-12-01' order by 1;")

            contains " QUERY: SELECT `id`, `d_z` FROM `doris_test`.`test_zd` WHERE (`d_z` = '2022-01-01')"
        }
        order_qt_date_sub_week """ select * from test_zd where date_sub(d_z,interval 1 week) = '2021-12-25' order by 1; """
        explain {
            sql("select * from test_zd where date_sub(d_z,interval 1 week) = '2021-12-25' order by 1;")

            contains " QUERY: SELECT `id`, `d_z` FROM `doris_test`.`test_zd` WHERE (`d_z` = '2022-01-01')"
        }
        order_qt_date_sub_day """ select * from test_zd where date_sub(d_z,interval 1 day) = '2021-12-31' order by 1; """
        explain {
            sql("select * from test_zd where date_sub(d_z,interval 1 day) = '2021-12-31' order by 1;")

            contains " QUERY: SELECT `id`, `d_z` FROM `doris_test`.`test_zd` WHERE (`d_z` = '2022-01-01')"
        }
        order_qt_date_sub_hour """ select * from test_zd where date_sub(d_z,interval 1 hour) = '2021-12-31 23:00:00' order by 1; """
        explain {
            sql("select * from test_zd where date_sub(d_z,interval 1 hour) = '2021-12-31 23:00:00' order by 1;")

            contains " QUERY: SELECT `id`, `d_z` FROM `doris_test`.`test_zd` WHERE (`d_z` = '2022-01-01')"
        }
        order_qt_date_sub_min """ select * from test_zd where date_sub(d_z,interval 1 minute) = '2021-12-31 23:59:00' order by 1; """
        explain {
            sql("select * from test_zd where date_sub(d_z,interval 1 minute) = '2021-12-31 23:59:00' order by 1;")

            contains " QUERY: SELECT `id`, `d_z` FROM `doris_test`.`test_zd` WHERE (`d_z` = '2022-01-01')"
        }
        order_qt_date_sub_sec """ select * from test_zd where date_sub(d_z,interval 1 second) = '2021-12-31 23:59:59' order by 1; """
        explain {
            sql("select * from test_zd where date_sub(d_z,interval 1 second) = '2021-12-31 23:59:59' order by 1;")

            contains " QUERY: SELECT `id`, `d_z` FROM `doris_test`.`test_zd` WHERE (`d_z` = '2022-01-01')"
        }

        sql """ drop catalog if exists mysql_fun_push_catalog; """

        // test insert null

        sql """drop catalog if exists ${catalog_name} """

        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver"
        );"""

        sql """switch ${catalog_name}"""
        sql """ use ${ex_db_name}"""

        order_qt_auto_default_t1 """insert into ${auto_default_t}(name) values('a'); """
        test {
            sql "insert into ${auto_default_t}(name,dt) values('a', null);"
            exception "Column `dt` is not nullable, but the inserted value is nullable."
        }
        test {
            sql "insert into ${auto_default_t}(name,dt) select '1', null;"
            exception "Column `dt` is not nullable, but the inserted value is nullable."
        }
        explain {
            sql "insert into ${auto_default_t}(name,dt) select col1,col12 from ex_tb15;"
            contains "PreparedStatement SQL: INSERT INTO `doris_test`.`auto_default_t`(`name`,`dt`) VALUES (?, ?)"
        }
        order_qt_auto_default_t2 """insert into ${auto_default_t}(name,dt) select col1, coalesce(col12,'2022-01-01 00:00:00') from ex_tb15 limit 1;"""
        sql """drop catalog if exists ${catalog_name} """

        // test lower_case_meta_names

        sql """ drop catalog if exists mysql_lower_case_catalog """
        sql """ CREATE CATALOG mysql_lower_case_catalog PROPERTIES (
                    "type"="jdbc",
                    "user"="root",
                    "password"="123456",
                    "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "com.mysql.cj.jdbc.Driver",
                    "lower_case_meta_names" = "true",
                    "meta_names_mapping" = '{"databases": [{"remoteDatabase": "DORIS","mapping": "doris_1"},{"remoteDatabase": "Doris","mapping": "doris_2"},{"remoteDatabase": "doris","mapping": "doris_3"}],"tables": [{"remoteDatabase": "Doris","remoteTable": "DORIS","mapping": "doris_1"},{"remoteDatabase": "Doris","remoteTable": "Doris","mapping": "doris_2"},{"remoteDatabase": "Doris","remoteTable": "doris","mapping": "doris_3"}]}'
            );
        """

        qt_sql "show databases from mysql_lower_case_catalog;"
        qt_sql "show tables from mysql_lower_case_catalog.doris_2;"
        qt_sql "select * from mysql_lower_case_catalog.doris_2.doris_1 order by id;"
        qt_sql "select * from mysql_lower_case_catalog.doris_2.doris_2 order by id;"
        qt_sql "select * from mysql_lower_case_catalog.doris_2.doris_3 order by id;"

        sql """ drop catalog if exists mysql_lower_case_catalog; """
        sql """ drop catalog if exists mysql_lower_case_catalog2; """
        test {
            sql """ CREATE CATALOG mysql_lower_case_catalog2 PROPERTIES (
                        "type"="jdbc",
                        "user"="root",
                        "password"="123456",
                        "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false",
                        "driver_url" = "${driver_url}",
                        "driver_class" = "com.mysql.cj.jdbc.Driver",
                        "lower_case_table_names" = "true",
                        "meta_names_mapping" = '{"databases": [{"remoteDatabase": "DORIS","mapping": "doris_1"},{"remoteDatabase": "Doris","mapping": "doris_2"},{"remoteDatabase": "doris","mapping": "doris_3"}],"tables": [{"remoteDatabase": "Doris","remoteTable": "DORIS","mapping": "doris_1"},{"remoteDatabase": "Doris","remoteTable": "Doris","mapping": "doris_2"},{"remoteDatabase": "Doris","remoteTable": "doris","mapping": "doris_3"}]}'
                    );
                """
            exception "Jdbc catalog property lower_case_table_names is not supported, please use lower_case_meta_names instead"
            }
        sql """ drop catalog if exists mysql_lower_case_catalog2; """
        sql """ drop catalog if exists mysql_lower_case_catalog3; """
        sql """ CREATE CATALOG mysql_lower_case_catalog3 PROPERTIES (
                    "type"="jdbc",
                    "user"="root",
                    "password"="123456",
                    "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "com.mysql.cj.jdbc.Driver",
                    "lower_case_meta_names" = "true",
                    "meta_names_mapping" = "{\\\"databases\\\": [{\\\"remoteDatabase\\\": \\\"DORIS\\\",\\\"mapping\\\": \\\"doris_1\\\"},{\\\"remoteDatabase\\\": \\\"Doris\\\",\\\"mapping\\\": \\\"doris_2\\\"},{\\\"remoteDatabase\\\": \\\"doris\\\",\\\"mapping\\\": \\\"doris_3\\\"}],\\\"tables\\\": [{\\\"remoteDatabase\\\": \\\"Doris\\\",\\\"remoteTable\\\": \\\"DORIS\\\",\\\"mapping\\\": \\\"doris_1\\\"},{\\\"remoteDatabase\\\": \\\"Doris\\\",\\\"remoteTable\\\": \\\"Doris\\\",\\\"mapping\\\": \\\"doris_2\\\"},{\\\"remoteDatabase\\\": \\\"Doris\\\",\\\"remoteTable\\\": \\\"doris\\\",\\\"mapping\\\": \\\"doris_3\\\"}]}"
                    );
                """
        sql """ drop catalog if exists mysql_lower_case_catalog3; """

        sql """drop catalog if exists mysql_refresh_property;"""

        sql """create catalog if not exists mysql_refresh_property properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false&zeroDateTimeBehavior=convertToNull",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "metadata_refresh_interval_sec" = "5"
        );"""

        sql """drop catalog if exists mysql_rename1;"""

        sql """create catalog if not exists mysql_rename1 properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false&zeroDateTimeBehavior=convertToNull",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver"
        );"""

        qt_sql """select count(*) from mysql_rename1.doris_test.ex_tb1;"""

        sql """alter catalog mysql_rename1 rename mysql_rename2"""

        qt_sql """select count(*) from mysql_rename2.doris_test.ex_tb1;"""

        sql """drop catalog if exists mysql_rename2;"""

    }
}

