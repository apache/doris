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

suite("test_sqlserver_jdbc_catalog", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest");
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mssql-jdbc-11.2.3.jre8.jar"
    // String driver_url = "mssql-jdbc-11.2.3.jre8.jar"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "sqlserver_catalog";
        String internal_db_name = "sqlserver_jdbc_catalog_p0";
        String ex_db_name = "dbo";
        String sqlserver_port = context.config.otherConfigs.get("sqlserver_2022_port");

        String inDorisTable = "test_sqlserver_doris_in_tb";

        sql """ drop catalog if exists ${catalog_name} """

        sql """ create catalog if not exists ${catalog_name} properties(
                    "type"="jdbc",
                    "user"="sa",
                    "password"="Doris123456",
                    "jdbc_url" = "jdbc:sqlserver://${externalEnvIp}:${sqlserver_port};encrypt=false;databaseName=doris_test;",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        );"""
        order_qt_show_db """ show databases from ${catalog_name}; """
        sql """drop database if exists ${internal_db_name}"""
        sql """create database if not exists ${internal_db_name}"""
        sql """use ${internal_db_name}"""
        sql  """ drop table if exists ${inDorisTable} """
        sql  """
              CREATE TABLE ${inDorisTable} (
                `id` INT NULL COMMENT "主键id",
                `name` string NULL COMMENT "名字",
                `age` INT NULL COMMENT "年龄"
                ) DISTRIBUTED BY HASH(id) BUCKETS 10
                PROPERTIES("replication_num" = "1");
        """

        sql """ switch ${catalog_name} """
        sql """ use ${ex_db_name} """

        order_qt_test0  """ select * from student order by id; """
        sql  """ insert into internal.${internal_db_name}.${inDorisTable} select * from student; """
        order_qt_in_tb  """ select id, name, age from internal.${internal_db_name}.${inDorisTable} order by id; """

        order_qt_test1  """ select * from test_int order by id; """
        order_qt_test2  """ select * from test_float order by id; """
        order_qt_test3  """ select * from test_char order by id; """
        order_qt_test5  """ select * from test_time order by id; """
        order_qt_test6  """ select * from test_money order by id; """
        order_qt_test7  """ select * from test_decimal order by id; """
        order_qt_test8  """ select * from test_text order by id; """
        order_qt_dt  """ select * from DateAndTime; """
        order_qt_filter1  """ select * from test_char where 1 = 1 order by id; """
        order_qt_filter2  """ select * from test_char where 1 = 1 and id = 1  order by id; """
        order_qt_filter3  """ select * from test_char where id = 1  order by id; """
        order_qt_filter4  """ select * from student where name not like '%doris%'  order by id; """
        order_qt_id """ select count(*) from (select * from t_id) as a; """
        order_qt_all_type """ select * from all_type order by id; """
        sql """ drop table if exists internal.${internal_db_name}.all_type; """
        order_qt_ctas """ create table internal.${internal_db_name}.ctas_all_type PROPERTIES("replication_num" = "1") as select * from all_type; """
        qt_desc_query_ctas """ desc internal.${internal_db_name}.ctas_all_type; """
        order_qt_query_ctas """ select * from internal.${internal_db_name}.ctas_all_type order by id; """
        order_qt_desc_timestamp """desc dbo.test_timestamp; """
        order_qt_query_timestamp """select count(timestamp_col) from dbo.test_timestamp; """

        order_qt_all_types_tvf """ select * from query('catalog' = '${catalog_name}', 'query' = 'select * from all_type;') order by 1"""

        order_qt_identity_decimal """ select * from test_identity_decimal order by id; """

        // Test cases for SQL Server date format pushdown (handleSQLServerDateFormat)
        // Uses test_date_filter table which has diverse date/datetime values across rows
        // to verify that filters genuinely include/exclude the correct rows.

        // Case 1: datetime equality — BinaryPredicate triggers CONVERT(DATETIME, '...', 121)
        // Should return only rows matching '2023-01-17 10:30:00' (id=1)
        order_qt_datetime_eq """ select * from test_date_filter where datetime_value = '2023-01-17 10:30:00' order by id; """
        // Case 2: datetime range — two BinaryPredicates trigger CONVERT(DATETIME, ..., 121)
        // Should return rows with datetime between '2023-06-25 14:30:45' and '2024-12-31 23:59:59' (id=2,3)
        order_qt_datetime_range """ select * from test_date_filter where datetime_value >= '2023-06-25 14:30:45' and datetime_value <= '2024-12-31 23:59:59' order by id; """
        // Case 3: datetime IN — InPredicate triggers CONVERT(DATETIME, ..., 121) for each item
        // Should return rows matching either value (id=1,5)
        order_qt_datetime_in """ select * from test_date_filter where datetime_value in ('2023-01-17 10:30:00', '2025-03-15 12:00:00') order by id; """
        // Case 4: date equality — BinaryPredicate triggers CONVERT(DATE, '...', 23)
        // Should return rows with date_value='2023-01-17' (id=1,4)
        order_qt_date_eq """ select * from test_date_filter where date_value = '2023-01-17' order by id; """
        // Case 5: date range — two BinaryPredicates trigger CONVERT(DATE, ..., 23)
        // Should return rows with date between '2023-06-25' and '2024-12-31' (id=2,3)
        order_qt_date_range """ select * from test_date_filter where date_value >= '2023-06-25' and date_value <= '2024-12-31' order by id; """
        // Case 6: date IN — InPredicate triggers CONVERT(DATE, ..., 23) for each item
        // Should return rows with date_value in the list (id=1,3,4)
        order_qt_date_in """ select * from test_date_filter where date_value in ('2023-01-17', '2024-12-31') order by id; """

        sql """ drop catalog if exists ${catalog_name} """

        sql """ create catalog if not exists ${catalog_name} properties(
                    "type"="jdbc",
                    "user"="sa",
                    "password"="Doris123456",
                    "jdbc_url" = "jdbc:sqlserver://${externalEnvIp}:${sqlserver_port};encrypt=false;databaseName=doris_test;trustServerCertificate=false",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        );"""

        order_qt_sql """ show databases from ${catalog_name} """


        sql """ drop catalog if exists test_sqlserver_jdbc_catalog_binary """

        sql """ create catalog if not exists test_sqlserver_jdbc_catalog_binary properties(
                    "type"="jdbc",
                    "user"="sa",
                    "password"="Doris123456",
                    "jdbc_url" = "jdbc:sqlserver://${externalEnvIp}:${sqlserver_port};encrypt=false;databaseName=doris_test;trustServerCertificate=false",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                    "enable.mapping.varbinary" = "true"
        );"""
        sql """ switch test_sqlserver_jdbc_catalog_binary """
        sql """ use ${ex_db_name} """

        order_qt_desc """ desc test_binary;  """
        sql """ CALL EXECUTE_STMT("test_sqlserver_jdbc_catalog_binary", "DELETE FROM dbo.test_binary WHERE id = 4") """
        order_qt_query """ select * from test_binary order by id; """
        sql """ insert into test_binary values (4, 4, X"ABAB", X"AB") """
        order_qt_query_after_insert """ select * from test_binary order by id; """
        sql """ drop catalog if exists ${catalog_name} """
    }
}
