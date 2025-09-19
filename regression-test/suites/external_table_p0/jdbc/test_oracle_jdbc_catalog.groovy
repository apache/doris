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

suite("test_oracle_jdbc_catalog", "p0,external,oracle,external_docker,external_docker_oracle") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest");
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/ojdbc8.jar"
    String driver6_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/ojdbc6.jar"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "oracle_catalog";
        String internal_db_name = "regression_test_jdbc_catalog_p0";
        String ex_db_name = "DORIS_TEST";
        String ex_db_name_lower_case = ex_db_name.toLowerCase();
        String oracle_port = context.config.otherConfigs.get("oracle_11_port");
        String SID = "XE";
        String test_insert = "TEST_INSERT";
        String test_all_types = "TEST_ALL_TYPES";
        String test_insert_all_types = "test_oracle_insert_all_types";
        String test_ctas = "test_oracle_ctas";

        String inDorisTable = "doris_in_tb";

        sql """create database if not exists ${internal_db_name}; """

        sql """drop catalog if exists ${catalog_name} """

        sql """create catalog if not exists ${catalog_name} properties(
                    "type"="jdbc",
                    "user"="doris_test",
                    "password"="123456",
                    "jdbc_url" = "jdbc:oracle:thin:@${externalEnvIp}:${oracle_port}:${SID}",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "oracle.jdbc.driver.OracleDriver"
        );"""
        order_qt_show_db """ show databases from ${catalog_name}; """
        sql """use ${internal_db_name}"""
        sql  """ drop table if exists ${internal_db_name}.${inDorisTable} """
        sql  """
              CREATE TABLE ${internal_db_name}.${inDorisTable} (
                `id` INT NULL COMMENT "主键id",
                `name` string NULL COMMENT "名字",
                `age` INT NULL COMMENT "年龄"
                ) DISTRIBUTED BY HASH(id) BUCKETS 10
                PROPERTIES("replication_num" = "1");
        """

        sql """ drop table if exists ${internal_db_name}.${test_insert_all_types} """
        sql """
            CREATE TABLE ${internal_db_name}.${test_insert_all_types} (
                `ID` LARGEINT NULL,
                `N1` TEXT NULL,
                `N2` LARGEINT NULL,
                `N3` DECIMAL(9, 2) NULL,
                `N4` LARGEINT NULL,
                `N5` LARGEINT NULL,
                `N6` DECIMAL(5, 2) NULL,
                `N7` DOUBLE NULL,
                `N8` DOUBLE NULL,
                `N9` DOUBLE NULL,
                `TINYINT_VALUE1` TINYINT NULL,
                `SMALLINT_VALUE1` SMALLINT NULL,
                `INT_VALUE1` INT NULL,
                `BIGINT_VALUE1` BIGINT NULL,
                `TINYINT_VALUE2` SMALLINT NULL,
                `SMALLINT_VALUE2` INT NULL,
                `INT_VALUE2` BIGINT NULL,
                `BIGINT_VALUE2` LARGEINT NULL,
                `COUNTRY` TEXT NULL,
                `CITY` TEXT NULL,
                `ADDRESS` TEXT NULL,
                `NAME` TEXT NULL,
                `REMARK` TEXT NULL,
                `NUM1` DECIMAL(5, 2) NULL,
                `NUM2` INT NULL,
                `NUM4` DECIMAL(7, 7) NULL,
                `T1` DATETIME NULL,
                `T2` DATETIME(3) NULL,
                `T3` DATETIME(6) NULL,
                `T4` DATETIME(6) NULL,
                `T5` DATETIME(6) NULL,
                `T6` TEXT NULL,
                `T7` TEXT NULL
                )
            DISTRIBUTED BY HASH(`ID`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
        """

        sql """switch ${catalog_name}"""
        sql """ use ${ex_db_name}"""

        order_qt_test0  """ select * from STUDENT order by ID; """
        sql  """ insert into internal.${internal_db_name}.${inDorisTable} select ID, NAME, AGE from STUDENT; """
        order_qt_in_tb  """ select id, name, age from internal.${internal_db_name}.${inDorisTable} order by id; """

        order_qt_test1  """ select * from TEST_NUM order by ID; """
        order_qt_test2  """ select * from TEST_CHAR order by ID; """
        order_qt_test3  """ select * from TEST_INT order by ID; """
        order_qt_test5  """ select * from TEST_DATE order by ID; """
        order_qt_test6  """ select * from TEST_TIMESTAMP order by ID; """
        order_qt_test7  """ select * from TEST_NUMBER order by ID; """
        order_qt_test8  """ select * from TEST_NUMBER2 order by ID; """
        order_qt_test9  """ select * from TEST_NUMBER3 order by ID; """
        order_qt_test10  """ select * from TEST_NUMBER4 order by ID; """
        order_qt_filter1  """ select * from TEST_CHAR where ID = 1 order by ID; """
        order_qt_filter2  """ select * from TEST_CHAR where 1 = 1 order by ID; """
        order_qt_filter3  """ select * from TEST_CHAR where ID = 1 and 1 = 1  order by ID; """
        order_qt_filter4  """ select * from STUDENT where NAME NOT like '%bob%' order by ID; """
        order_qt_filter5  """ select * from STUDENT where NAME NOT like '%bob%' or NAME NOT LIKE '%jerry%' order by ID; """
        order_qt_filter6  """ select * from STUDENT where NAME NOT like '%bob%' and NAME NOT LIKE '%jerry%' order by ID; """
        order_qt_filter7  """ select * from STUDENT where NAME NOT like '%bob%' and NAME LIKE '%jerry%' order by ID; """
        order_qt_filter8  """ select * from STUDENT where NAME NOT like '%bob%' and ID = 4 order by ID; """
        order_qt_filter9  """ SELECT * FROM STUDENT WHERE (NAME NOT LIKE '%bob%' AND AGE > 20) OR (SCORE < 90 AND NOT (NAME = 'alice' OR AGE <= 18)) order by ID; """
        order_qt_date1  """ select * from TEST_DATE where T1 > '2022-01-21 00:00:00' or T1 < '2022-01-22 00:00:00'; """
        order_qt_date2  """ select * from TEST_DATE where T1 > '2022-01-21 00:00:00' and T1 < '2022-01-22 00:00:00'; """
        order_qt_date3  """ select * from TEST_DATE where (T1 > '2022-01-21 00:00:00' and T1 < '2022-01-22 00:00:00') or T1 > '2022-01-20 00:00:00'; """
        order_qt_date4  """ select * from TEST_DATE where (T1 > '2022-01-21 00:00:00' and T1 < '2022-01-22 00:00:00') or (T1 > '2022-01-20 00:00:00' and T1 < '2022-01-23 00:00:00'); """
        order_qt_date5  """ select * from TEST_DATE where T1 < '2022-01-22 00:00:00' or T1 = '2022-01-21 05:23:01'; """
        order_qt_date6  """ select * from TEST_DATE where (T1 < '2022-01-22 00:00:00' or T1 > '2022-01-20 00:00:00') and (T1 < '2022-01-23 00:00:00' or T1 > '2022-01-19 00:00:00'); """
        order_qt_date7  """select * from TEST_TIMESTAMP where T2 < str_to_date('2020-12-21 12:34:56', '%Y-%m-%d %H:%i:%s');"""

        // Test compound predicates processing - Multiple test cases for NOT, AND, OR combinations and nesting
        order_qt_compound1 """select * from TEST_TIMESTAMP where T1 is not null order by ID;"""
        order_qt_compound2 """select * from TEST_TIMESTAMP where NOT (T1 is null) order by ID;"""
        order_qt_compound3 """select * from TEST_TIMESTAMP where T1 is not null AND T1 < '2013-02-01 00:00:00' order by ID;"""
        order_qt_compound4 """select * from TEST_TIMESTAMP where T1 is not null OR T2 is not null order by ID;"""
        order_qt_compound5 """select * from TEST_TIMESTAMP where NOT (T1 is null OR T2 is null) order by ID;"""
        order_qt_compound6 """select * from TEST_TIMESTAMP where (T1 is not null AND T1 < '2013-02-01 00:00:00') OR (T2 is not null AND T2 > '2019-01-01 00:00:00') order by ID;"""
        order_qt_compound7 """select * from TEST_TIMESTAMP where NOT ((T1 is null AND T2 is null) OR (T3 is null AND T4 is null)) order by ID;"""

        // Test IN operator with date/time values
        order_qt_in_date1 """select * from TEST_TIMESTAMP where T1 IN ('2013-01-21 05:23:01', '2013-11-12 20:32:56') order by ID;"""
        order_qt_in_date2 """select * from TEST_TIMESTAMP where T1 NOT IN ('2013-01-21 05:23:01') order by ID;"""
        
        // Test date/time comparisons and combinations
        order_qt_date_complex1 """select * from TEST_TIMESTAMP where T1 < '2013-05-01 00:00:00' AND T1 > '2013-01-01 00:00:00' order by ID;"""
        order_qt_date_complex2 """select * from TEST_TIMESTAMP where T1 BETWEEN '2013-01-01 00:00:00' AND '2013-12-31 23:59:59' order by ID;"""
        order_qt_date_complex3 """select * from TEST_TIMESTAMP where T2 >= '2019-01-01 00:00:00' OR T3 >= '2019-01-01 00:00:00' order by ID;"""
        order_qt_date_complex4 """select * from TEST_TIMESTAMP where NOT (T1 < '2013-01-01 00:00:00' OR T1 > '2014-01-01 00:00:00') order by ID;"""
        
        // Test NOT operator in date/time expressions
        order_qt_time_not1 """select * from TEST_TIMESTAMP where NOT (T1 > '2013-06-01 00:00:00') order by ID;"""
        order_qt_time_not2 """select * from TEST_TIMESTAMP where NOT (T1 BETWEEN '2013-05-01 00:00:00' AND '2013-12-31 23:59:59') order by ID;"""
        order_qt_time_not3 """select * from TEST_TIMESTAMP where T1 IS NOT NULL AND NOT (T1 > '2013-10-01 00:00:00') order by ID;"""
        
        // Test complex date/time comparisons and multiple time field combinations
        order_qt_time_multi1 """select * from TEST_TIMESTAMP where (T1 < '2013-06-01 00:00:00' OR T2 > '2019-01-01 00:00:00') order by ID;"""
        order_qt_time_multi2 """select * from TEST_TIMESTAMP where (T1 IS NOT NULL AND T1 < '2013-06-01 00:00:00') OR (T2 IS NOT NULL AND T2 > '2019-01-01 00:00:00') order by ID;"""
        order_qt_time_multi3 """select * from TEST_TIMESTAMP where NOT ((T1 IS NULL) OR (T2 IS NULL AND T3 IS NULL)) order by ID;"""
        
        // Test date comparisons using to_date function
        order_qt_time_func1 """select * from TEST_TIMESTAMP where T1 > str_to_date('2013-01-01 00:00:00', '%Y-%m-%d %H:%i:%s') order by ID;"""
        order_qt_time_func2 """select * from TEST_TIMESTAMP where T1 BETWEEN str_to_date('2013-01-01 00:00:00', '%Y-%m-%d %H:%i:%s') AND str_to_date('2013-12-31 23:59:59', '%Y-%m-%d %H:%i:%s') order by ID;"""
        order_qt_time_func3 """select * from TEST_TIMESTAMP where NOT (T1 < str_to_date('2013-01-01 00:00:00', '%Y-%m-%d %H:%i:%s')) order by ID;"""
        
        // Test complex AND/OR/NOT combinations with date/time comparisons
        order_qt_time_complex1 """select * from TEST_TIMESTAMP where (T1 > '2013-01-01 00:00:00' AND T1 < '2013-12-31 23:59:59') OR (T2 > '2018-01-01 00:00:00' AND T2 < '2020-01-01 00:00:00') order by ID;"""
        order_qt_time_complex2 """select * from TEST_TIMESTAMP where NOT ((T1 < '2013-01-01 00:00:00' OR T1 > '2014-01-01 00:00:00') AND (T2 IS NULL OR T2 < '2018-01-01 00:00:00')) order by ID;"""
        order_qt_time_complex3 """select * from TEST_TIMESTAMP where (T1 IN ('2013-01-21 05:23:01', '2013-11-12 20:32:56') OR T2 IS NOT NULL) AND NOT (T3 IS NULL AND T4 IS NULL) order by ID;"""
        
        // Test deeply nested date/time conditions
        order_qt_nested1 """select * from TEST_TIMESTAMP where NOT (NOT (T1 > '2013-01-01 00:00:00')) order by ID;"""
        order_qt_nested2 """select * from TEST_TIMESTAMP where NOT ((T1 IS NULL OR T1 > '2013-10-01 00:00:00') AND (T2 IS NULL OR T2 < '2019-01-01 00:00:00')) order by ID;"""
        
        // Test date format edge cases
        order_qt_edge1 """select * from TEST_TIMESTAMP where T1 = '2013-01-21 05:23:01' order by ID;"""
        order_qt_edge2 """select * from TEST_TIMESTAMP where T2 = '2019-11-12 20:33:57.999' order by ID;"""
        order_qt_edge3 """select * from TEST_TIMESTAMP where T3 = '2019-11-12 20:33:57.999998' order by ID;"""
        
        // Test CURDATE() function and CAST combinations
        order_qt_curdate1 """select * from TEST_TIMESTAMP where ID = 1 OR (T2 >= CAST(CURDATE() AS DATETIME) AND T1 >= CAST(CURDATE() AS DATETIME) AND T3 NOT IN (CAST(CURDATE() AS DATETIME))) order by ID;"""
        order_qt_curdate2 """select * from TEST_TIMESTAMP where T1 >= CAST(CURDATE() AS DATETIME) order by ID;"""
        order_qt_curdate3 """select * from TEST_TIMESTAMP where T1 IN (CAST(CURDATE() AS DATETIME), CAST(CURDATE() + INTERVAL 1 DAY AS DATETIME)) order by ID;"""
        order_qt_curdate4 """select * from TEST_TIMESTAMP where T2 NOT IN (CAST(CURDATE() AS DATETIME), CAST(CURDATE() + INTERVAL 1 DAY AS DATETIME)) order by ID;"""
        
        // Test complex nested conditions with date functions
        order_qt_complex1 """select * from TEST_TIMESTAMP where (T1 < CAST(CURDATE() AS DATETIME) OR T2 > CAST(CURDATE() AS DATETIME)) AND NOT (T3 = CAST(CURDATE() AS DATETIME)) order by ID;"""
        order_qt_complex2 """select * from TEST_TIMESTAMP where NOT ((T1 < CAST(CURDATE() AS DATETIME) AND T2 IS NULL) OR (T3 > CAST(CURDATE() AS DATETIME))) order by ID;"""
        
        // Test CAST, CURDATE with multiple time conditions
        order_qt_complex3 """select * from TEST_TIMESTAMP where (ID = 1 AND T1 >= CAST(CURDATE() AS DATETIME)) OR (ID = 2 AND T2 BETWEEN CAST(CURDATE() AS DATETIME) AND CAST(CURDATE() + INTERVAL 7 DAY AS DATETIME)) order by ID;"""
        
        // Test mixed date functions and string dates
        order_qt_mixed """select * from TEST_TIMESTAMP where T1 >= CAST(CURDATE() AS DATETIME) AND T2 < '2023-01-01 00:00:00' order by ID;"""

        // test nvl
        explain {
            sql("SELECT * FROM STUDENT WHERE nvl(score, 0) < 95;")
            contains """SELECT "ID", "NAME", "AGE", "SCORE" FROM "DORIS_TEST"."STUDENT" WHERE ((nvl("SCORE", 0.0) < 95.0))"""
        }

        order_qt_raw  """ select * from TEST_RAW order by ID; """

        // test insert
        String uuid1 = UUID.randomUUID().toString();
        sql """ insert into ${test_insert} values ('${uuid1}', 'doris1', 18) """
        order_qt_test_insert1 """ select name, age from ${test_insert} where id = '${uuid1}' order by age """

        String uuid2 = UUID.randomUUID().toString();
        sql """ insert into ${test_insert} values ('${uuid2}', 'doris2', 19), ('${uuid2}', 'doris3', 20) """
        order_qt_test_insert2 """ select name, age from ${test_insert} where id = '${uuid2}' order by age """

        sql """ insert into ${test_insert} select * from ${test_insert} where id = '${uuid2}' """
        order_qt_test_insert3 """ select name, age from ${test_insert} where id = '${uuid2}' order by age """

        // test select all types
        order_qt_select_all_types """select * from ${test_all_types}; """

        order_qt_select_all_types_tvf """select * from query('catalog' = '${catalog_name}', 'query' ='select * from ${test_all_types}') order by 1; """

        // test test ctas
        sql """ drop table if exists internal.${internal_db_name}.${test_ctas} """
        sql """ create table internal.${internal_db_name}.${test_ctas}
                PROPERTIES("replication_num" = "1")
                AS select * from ${test_all_types};
            """

        order_qt_ctas """select * from internal.${internal_db_name}.${test_ctas};"""

        order_qt_ctas_desc """desc internal.${internal_db_name}.${test_ctas};"""

        // test insert into internal.db.tbl
        sql """ insert into internal.${internal_db_name}.${test_insert_all_types} select * from ${test_all_types}; """
        order_qt_select_insert_all_types """ select * from internal.${internal_db_name}.${test_insert_all_types} order by id; """

        sql """drop catalog if exists ${catalog_name} """

        // test only_specified_database argument
        sql """create catalog if not exists ${catalog_name} properties(
                    "type"="jdbc",
                    "user"="doris_test",
                    "password"="123456",
                    "jdbc_url" = "jdbc:oracle:thin:@${externalEnvIp}:${oracle_port}:${SID}",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "oracle.jdbc.driver.OracleDriver",
                    "only_specified_database" = "true"
        );"""
        sql """ switch ${catalog_name} """

        qt_specified_database   """ show databases; """
        sql """drop catalog if exists ${catalog_name} """

        // test only_specified_database and specified_database_list argument
        sql """create catalog if not exists ${catalog_name} properties(
                    "type"="jdbc",
                    "user"="doris_test",
                    "password"="123456",
                    "jdbc_url" = "jdbc:oracle:thin:@${externalEnvIp}:${oracle_port}:${SID}",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "oracle.jdbc.driver.OracleDriver",
                    "only_specified_database" = "true",
                    "include_database_list" = "${ex_db_name}"
        );"""
        sql """ switch ${catalog_name} """

        qt_specified_database   """ show databases; """
        sql """drop catalog if exists ${catalog_name} """

        // test lower_case_meta_names argument
        sql """create catalog if not exists ${catalog_name} properties(
                    "type"="jdbc",
                    "user"="doris_test",
                    "password"="123456",
                    "jdbc_url" = "jdbc:oracle:thin:@${externalEnvIp}:${oracle_port}:${SID}",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "oracle.jdbc.driver.OracleDriver",
                    "lower_case_meta_names" = "true"
        );"""
        sql """ switch ${catalog_name} """
        sql """ use ${ex_db_name_lower_case}"""

        qt_lower_case_table_names1  """ select * from test_num order by ID; """
        qt_lower_case_table_names2  """ select * from test_char order by ID; """
        qt_lower_case_table_names3  """ select * from test_int order by ID; """

        // test lower case name
        order_qt_lower_case_table_names4  """ select * from student2 order by id; """
        order_qt_lower_case_column_names1  """ select * from student3 order by id; """
        order_qt_lower_case_column_names2  """ select * from student3  where id = 1 order by id; """
        order_qt_lower_case_column_names3  """ select * from student3  where id = 1 and name = 'doris' order by id; """

        sql """drop catalog if exists ${catalog_name} """

        // test for clob type
        sql """create catalog if not exists ${catalog_name} properties(
                    "type"="jdbc",
                    "user"="doris_test",
                    "password"="123456",
                    "jdbc_url" = "jdbc:oracle:thin:@${externalEnvIp}:${oracle_port}:${SID}",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "oracle.jdbc.driver.OracleDriver",
                    "lower_case_meta_names" = "true"
        );"""
        sql """ switch ${catalog_name} """
        qt_query_clob """ select * from doris_test.test_clob order by id; """

        sql """drop catalog if exists ${catalog_name} """

        // test for `AA/D`
        sql """create catalog if not exists ${catalog_name} properties(
                    "type"="jdbc",
                    "user"="doris_test",
                    "password"="123456",
                    "jdbc_url" = "jdbc:oracle:thin:@${externalEnvIp}:${oracle_port}:${SID}",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "oracle.jdbc.driver.OracleDriver",
                    "lower_case_meta_names" = "true"
        );"""
        sql """ switch ${catalog_name} """
        qt_query_ad1 """ select * from doris_test.`aa/d` order by id; """
        qt_query_ad2 """ select * from doris_test.aaad order by id; """

        sql """drop catalog if exists ${catalog_name} """

        // test for suffix column name
        sql """create catalog if not exists ${catalog_name} properties(
                    "type"="jdbc",
                    "user"="doris_test",
                    "password"="123456",
                    "jdbc_url" = "jdbc:oracle:thin:@${externalEnvIp}:${oracle_port}:${SID}",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "oracle.jdbc.driver.OracleDriver",
                    "lower_case_meta_names" = "true",
                    "meta_names_mapping" = '{"columns": [{"remoteDatabase": "DORIS_TEST","remoteTable": "LOWER_TEST","remoteColumn": "DORIS","mapping": "doris_1"},{"remoteDatabase": "DORIS_TEST","remoteTable": "LOWER_TEST","remoteColumn": "Doris","mapping": "doris_2"},{"remoteDatabase": "DORIS_TEST","remoteTable": "LOWER_TEST","remoteColumn": "doris","mapping": "doris_3"}]}'
        );"""
        sql """ switch ${catalog_name} """
        qt_query_lower_desc """ desc doris_test.lower_test; """
        qt_query_lower_all """ select * from doris_test.lower_test; """
        qt_query_lower_1 """ select doris_1 from doris_test.lower_test; """
        qt_query_lower_2 """ select doris_2 from doris_test.lower_test; """
        qt_query_lower_3 """ select doris_3 from doris_test.lower_test; """

        sql """drop catalog if exists ${catalog_name} """

        // test for ojdbc6
        sql """drop catalog if exists oracle_ojdbc6; """
        sql """create catalog if not exists oracle_ojdbc6 properties(
                    "type"="jdbc",
                    "user"="doris_test",
                    "password"="123456",
                    "jdbc_url" = "jdbc:oracle:thin:@${externalEnvIp}:${oracle_port}:${SID}",
                    "driver_url" = "${driver6_url}",
                    "driver_class" = "oracle.jdbc.OracleDriver"
        );"""
        sql """ use oracle_ojdbc6.DORIS_TEST; """
        qt_query_ojdbc6_all_types """ select * from oracle_ojdbc6.DORIS_TEST.TEST_ALL_TYPES order by 1; """

        sql """drop catalog if exists oracle_ojdbc6; """

        // test oracle null operator
        sql """ drop catalog if exists oracle_null_operator; """
        sql """ create catalog if not exists oracle_null_operator properties(
                    "type"="jdbc",
                    "user"="doris_test",
                    "password"="123456",
                    "jdbc_url" = "jdbc:oracle:thin:@${externalEnvIp}:${oracle_port}:${SID}",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "oracle.jdbc.driver.OracleDriver"
        );"""

        sql """ use oracle_null_operator.DORIS_TEST; """
        order_qt_null_operator1 """ SELECT * FROM STUDENT WHERE (id IS NOT NULL OR NULL); """
        order_qt_null_operator2 """ SELECT * FROM STUDENT WHERE (age > 20 OR NULL); """
        order_qt_null_operator3 """ SELECT * FROM STUDENT WHERE (name = 'alice' AND age = 20); """
        order_qt_null_operator4 """ SELECT * FROM STUDENT WHERE (LENGTH(name) > 3 AND NULL); """
        order_qt_null_operator5 """ SELECT * FROM STUDENT WHERE (age = NULL); """
        order_qt_null_operator6 """ SELECT * FROM STUDENT WHERE (score IS NULL); """
        order_qt_null_operator7 """ SELECT * FROM STUDENT WHERE ((age > 20 AND score < 90) OR NULL); """
        order_qt_null_operator8 """ SELECT * FROM STUDENT WHERE (age BETWEEN 20 AND 25) AND (name LIKE 'a%'); """
        order_qt_null_operator9 """ SELECT * FROM STUDENT WHERE (id IS NOT NULL AND NULL); """
        order_qt_null_operator10 """ SELECT * FROM STUDENT WHERE (name IS NULL OR age IS NOT NULL); """

        // test function rules
        // test push down
        sql """ drop catalog if exists oracle_function_rules"""
        // test invalid config
        test {
            sql """create catalog if not exists oracle_function_rules properties(
                "type"="jdbc",
                "user"="doris_test",
                "password"="123456",
                "jdbc_url" = "jdbc:oracle:thin:@${externalEnvIp}:${oracle_port}:${SID}",
                "driver_url" = "${driver_url}",
                "driver_class" = "oracle.jdbc.driver.OracleDriver",
                "function_rules" = '{"pushdown" : {"supported" : [null]}}'
            );"""

            exception """Failed to parse push down rules: {"pushdown" : {"supported" : [null]}}"""
        }

        sql """create catalog if not exists oracle_function_rules properties(
            "type"="jdbc",
            "user"="doris_test",
            "password"="123456",
            "jdbc_url" = "jdbc:oracle:thin:@${externalEnvIp}:${oracle_port}:${SID}",
            "driver_url" = "${driver_url}",
            "driver_class" = "oracle.jdbc.driver.OracleDriver",
            "function_rules" = '{"pushdown" : {"supported" : ["abs"]}}'
        );"""

        sql "use oracle_function_rules.DORIS_TEST"
        explain {
            sql """select id from STUDENT where abs(id) > 0 and ifnull(id, 3) = 3;"""
            contains """QUERY: SELECT "ID" FROM "DORIS_TEST"."STUDENT" WHERE ((abs("ID") > 0)) AND ((nvl("ID", 3) = 3))"""
            contains """PREDICATES: ((abs(ID[#0]) > 0) AND (ifnull(ID[#0], 3) = 3))"""
        }
        sql """alter catalog oracle_function_rules set properties("function_rules" = '');"""
        explain {
            sql """select id from STUDENT where abs(id) > 0 and ifnull(id, 3) = 3;"""
            contains """QUERY: SELECT "ID" FROM "DORIS_TEST"."STUDENT" WHERE ((nvl("ID", 3) = 3))"""
            contains """PREDICATES: ((abs(ID[#0]) > 0) AND (ifnull(ID[#0], 3) = 3))"""
        }

        sql """alter catalog oracle_function_rules set properties("function_rules" = '{"pushdown" : {"supported": ["abs"], "unsupported" : []}}')"""
        explain {
            sql """select id from STUDENT where abs(id) > 0 and ifnull(id, 3) = 3;"""
            contains """QUERY: SELECT "ID" FROM "DORIS_TEST"."STUDENT" WHERE ((abs("ID") > 0)) AND ((nvl("ID", 3) = 3))"""
            contains """PREDICATES: ((abs(ID[#0]) > 0) AND (ifnull(ID[#0], 3) = 3))"""
        }

        // test rewrite
        sql """alter catalog oracle_function_rules set properties("function_rules" = '{"pushdown" : {"supported": ["abs"]}, "rewrite" : {"abs" : "abs2"}}');"""
        explain {
            sql """select id from STUDENT where abs(id) > 0 and ifnull(id, 3) = 3;"""
            contains """QUERY: SELECT "ID" FROM "DORIS_TEST"."STUDENT" WHERE ((abs2("ID") > 0)) AND ((nvl("ID", 3) = 3))"""
            contains """PREDICATES: ((abs(ID[#0]) > 0) AND (ifnull(ID[#0], 3) = 3))"""
        }

        // reset function rules
        sql """alter catalog oracle_function_rules set properties("function_rules" = '');"""
        explain {
            sql """select id from STUDENT where abs(id) > 0 and ifnull(id, 3) = 3;"""
            contains """QUERY: SELECT "ID" FROM "DORIS_TEST"."STUDENT" WHERE ((nvl("ID", 3) = 3))"""
            contains """PREDICATES: ((abs(ID[#0]) > 0) AND (ifnull(ID[#0], 3) = 3))"""
        }

        // test invalid config
        test {
            sql """alter catalog oracle_function_rules set properties("function_rules" = 'invalid_json')"""
            exception """Failed to parse push down rules: invalid_json"""
        }

        // test synonym
        sql """drop catalog if exists oracle_test_synonym"""
        sql """create catalog oracle_test_synonym properties(
            "type"="jdbc",
            "user"="doris_test",
            "password"="123456",
            "jdbc_url" = "jdbc:oracle:thin:@${externalEnvIp}:${oracle_port}:${SID}",
            "driver_url" = "${driver_url}",
            "driver_class" = "oracle.jdbc.driver.OracleDriver"
        );"""

        // TEST_SYNONYM_STUDENT is a synonym of "student" table in DORIS_TEST
        order_qt_sql_syn01 """select * from oracle_test_synonym.DORIS_TEST.TEST_SYNONYM_STUDENT"""
        // should be empty, because user "doris_test" has no priv on SYNONYM_TEST_USER
        qt_sql_syn02 """show tables from oracle_test_synonym.SYNONYM_TEST_USER;"""

        // create catalog with admin priv
        sql """drop catalog if exists oracle_test_synonym_sys"""
        sql """create catalog oracle_test_synonym_sys properties(
            "type"="jdbc",
            "user"="system",
            "password"="oracle",
            "jdbc_url" = "jdbc:oracle:thin:@${externalEnvIp}:${oracle_port}:${SID}",
            "driver_url" = "${driver_url}",
            "driver_class" = "oracle.jdbc.driver.OracleDriver"
        );"""

        // can still see synonym in DORIS_TEST
        order_qt_sql_syn01 """select * from oracle_test_synonym_sys.DORIS_TEST.TEST_SYNONYM_STUDENT"""
        // should has priv to see 2 synonym in SYNONYM_TEST_USER
        qt_sql_syn02 """show tables from oracle_test_synonym_sys.SYNONYM_TEST_USER;"""
        order_qt_sql_syn03 """select * from oracle_test_synonym_sys.SYNONYM_TEST_USER.TEST_SYNONYM_STUDENT"""
        order_qt_sql_syn04 """select * from oracle_test_synonym_sys.SYNONYM_TEST_USER.TEST_SYNONYM_STUDENT2"""
        

        // sql """ drop catalog if exists oracle_null_operator; """

    }
}
