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

suite("test_clickhouse_jdbc_catalog", "p0,external,clickhouse,external_docker,external_docker_clickhouse") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "clickhouse_catalog";
        String internal_db_name = "regression_test_jdbc_catalog_p0";
        String ex_db_name = "doris_test";
        String clickhouse_port = context.config.otherConfigs.get("clickhouse_22_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/clickhouse-jdbc-0.4.2-all.jar"
        String driver_url_7 = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/clickhouse-jdbc-0.7.1-patch1-all.jar"

        String inDorisTable = "test_clickhouse_jdbc_doris_in_tb";

        sql """create database if not exists ${internal_db_name}; """

        sql """ drop catalog if exists ${catalog_name} """

        sql """ create catalog if not exists ${catalog_name} properties(
                    "type"="jdbc",
                    "user"="default",
                    "password"="123456",
                    "jdbc_url" = "jdbc:clickhouse://${externalEnvIp}:${clickhouse_port}/doris_test",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "com.clickhouse.jdbc.ClickHouseDriver"
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

        sql """ switch ${catalog_name} """
        sql """ use ${ex_db_name} """

        order_qt_type  """ select * from type order by k1; """
        order_qt_type_null  """ select * from type_null order by id; """
        sql """drop table if exists internal.${internal_db_name}.ck_type_null """
        order_qt_ctas_type_null """create table internal.${internal_db_name}.ck_type_null PROPERTIES("replication_num" = "1") as select * from type_null """;
        order_qt_query_ctas_type_null """ select * from internal.${internal_db_name}.ck_type_null order by id; """
        order_qt_number  """ select * from number order by k6; """
        order_qt_arr  """ select * from arr order by id; """
        order_qt_arr_null  """ select * from arr_null order by id; """
        sql """ drop table if exists internal.${internal_db_name}.ck_arr_null"""
        order_qt_ctas_arr_null """create table internal.${internal_db_name}.ck_arr_null PROPERTIES("replication_num" = "1") as select * from arr_null """;
        order_qt_query_ctas_arr_null """ select * from internal.${internal_db_name}.ck_arr_null order by id; """
        sql  """ insert into internal.${internal_db_name}.${inDorisTable} select * from student; """
        order_qt_in_tb  """ select id, name, age from internal.${internal_db_name}.${inDorisTable} order by id; """

        order_qt_filter  """ select k1,k2 from type where 1 = 1 order by 1 ; """
        order_qt_filter2  """ select k1,k2 from type where 1 = 1 and  k1 = true order by 1 ; """
        order_qt_filter3  """ select k1,k2 from type where k1 = true order by 1 ; """
        order_qt_filter4  """ select k28 from type where k28 not like '%String%' order by 1 ; """
        sql "set jdbc_clickhouse_query_final = true;"
        order_qt_final1 """select * from final_test"""
        sql "set jdbc_clickhouse_query_final = false;"
        order_qt_final2 """select * from final_test"""
        order_qt_func_push """select * from ts where from_unixtime(ts,'yyyyMMdd') >= '2022-01-01';"""
        explain {
            sql("select * from ts where from_unixtime(ts,'yyyyMMdd') >= '2022-01-01';")
            contains """QUERY: SELECT "id", "ts" FROM "doris_test"."ts" WHERE ((FROM_UNIXTIME("ts", '%Y%m%d') >= '2022-01-01'))"""
        }
        order_qt_func_push2 """select * from ts where ts <= unix_timestamp(from_unixtime(ts,'yyyyMMdd'));"""
        explain {
            sql("select * from ts where ts <= unix_timestamp(from_unixtime(ts,'yyyy-MM-dd'));")
            contains """QUERY: SELECT "id", "ts" FROM "doris_test"."ts" WHERE (("ts" <= toUnixTimestamp(FROM_UNIXTIME("ts", '%Y-%m-%d'))))"""
        }

        order_qt_dt_with_tz """ select * from dt_with_tz order by id; """

        sql """ drop catalog if exists ${catalog_name} """


        sql """ drop catalog if exists clickhouse_7_default """
        sql """ create catalog if not exists clickhouse_7_default properties(
                    "type"="jdbc",
                    "user"="default",
                    "password"="123456",
                    "jdbc_url" = "jdbc:clickhouse://${externalEnvIp}:${clickhouse_port}/doris_test",
                    "driver_url" = "${driver_url_7}",
                    "driver_class" = "com.clickhouse.jdbc.ClickHouseDriver"
        );"""

        order_qt_clickhouse_7_default """ select * from clickhouse_7_default.doris_test.type; """
        order_qt_clickhouse_7_default_tvf """ select * from query('catalog' = 'clickhouse_7_default', 'query' = 'select * from doris_test.type;') order by 1; """
        order_qt_clickhouse_7_default_tvf_arr """ select * from query('catalog' = 'clickhouse_7_default', 'query' = 'select * from doris_test.arr;') order by 1; """

        sql """ drop catalog if exists clickhouse_7_default """

        sql """ drop catalog if exists clickhouse_7_catalog """

        sql """ create catalog if not exists clickhouse_7_catalog properties(
                    "type"="jdbc",
                    "user"="default",
                    "password"="123456",
                    "jdbc_url" = "jdbc:clickhouse://${externalEnvIp}:${clickhouse_port}/doris_test?databaseTerm=catalog",
                    "driver_url" = "${driver_url_7}",
                    "driver_class" = "com.clickhouse.jdbc.ClickHouseDriver"
        );"""

        order_qt_clickhouse_7_catalog """ select * from clickhouse_7_catalog.doris_test.type; """
        order_qt_clickhouse_7_catalog_tvf """ select * from query('catalog' = 'clickhouse_7_catalog', 'query' = 'select * from doris_test.type;') order by 1; """
        order_qt_clickhouse_7_catalog_tvf_arr """ select * from query('catalog' = 'clickhouse_7_catalog', 'query' = 'select * from doris_test.arr;') order by 1; """

        sql """ drop catalog if exists clickhouse_7_catalog """

        sql """ drop catalog if exists clickhouse_7_schema """

        sql """ create catalog if not exists clickhouse_7_schema properties(
                    "type"="jdbc",
                    "user"="default",
                    "password"="123456",
                    "jdbc_url" = "jdbc:clickhouse://${externalEnvIp}:${clickhouse_port}/doris_test?databaseTerm=schema",
                    "driver_url" = "${driver_url_7}",
                    "driver_class" = "com.clickhouse.jdbc.ClickHouseDriver"
        );"""

        order_qt_clickhouse_7_schema """ select * from clickhouse_7_schema.doris_test.type; """
        order_qt_clickhouse_7_schema_tvf """ select * from query('catalog' = 'clickhouse_7_schema', 'query' = 'select * from doris_test.type;') order by 1; """
        order_qt_clickhouse_7_schema_tvf_arr """ select * from query('catalog' = 'clickhouse_7_schema', 'query' = 'select * from doris_test.arr;') order by 1; """

        // test function rules
        // test push down
        sql """ drop catalog if exists clickhouse_7_catalog """
        // test invalid config
        test {
            sql """ create catalog if not exists clickhouse_7_catalog properties(
                        "type"="jdbc",
                        "user"="default",
                        "password"="123456",
                        "jdbc_url" = "jdbc:clickhouse://${externalEnvIp}:${clickhouse_port}/doris_test?databaseTerm=schema",
                        "driver_url" = "${driver_url_7}",
                        "driver_class" = "com.clickhouse.jdbc.ClickHouseDriver",
                        "function_rules" = '{"pushdown" : {"supported" : [null]}}'
            );"""

            exception """Failed to parse push down rules: {"pushdown" : {"supported" : [null]}}"""
        }

        sql """ create catalog if not exists clickhouse_7_catalog properties(
                    "type"="jdbc",
                    "user"="default",
                    "password"="123456",
                    "jdbc_url" = "jdbc:clickhouse://${externalEnvIp}:${clickhouse_port}/doris_test?databaseTerm=schema",
                    "driver_url" = "${driver_url_7}",
                    "driver_class" = "com.clickhouse.jdbc.ClickHouseDriver",
                    "function_rules" = '{"pushdown" : {"supported": ["abs"]}}'
        );"""
        sql "use clickhouse_7_catalog.doris_test"
        explain {
            sql("select k4 from type where abs(k4) > 0 and unix_timestamp(k4) > 0")
            contains """SELECT "k4" FROM "doris_test"."type" WHERE ((abs("k4") > 0)) AND ((toUnixTimestamp("k4") > 0))"""
            contains """PREDICATES: ((abs(CAST(k4[#3] AS double)) > 0) AND (unix_timestamp(k4[#3]) > 0))"""
        }
        sql """alter catalog clickhouse_7_catalog set properties("function_rules" = '');"""
        explain {
            sql("select k4 from type where abs(k4) > 0 and unix_timestamp(k4) > 0")
            contains """QUERY: SELECT "k4" FROM "doris_test"."type" WHERE ((toUnixTimestamp("k4") > 0))"""
            contains """PREDICATES: ((abs(CAST(k4[#3] AS double)) > 0) AND (unix_timestamp(k4[#3]) > 0))"""
        }

        sql """alter catalog clickhouse_7_catalog set properties("function_rules" = '{"pushdown" : {"supported": ["abs"]}}')"""         
        explain {
            sql("select k4 from type where abs(k4) > 0 and unix_timestamp(k4) > 0")
            contains """SELECT "k4" FROM "doris_test"."type" WHERE ((abs("k4") > 0)) AND ((toUnixTimestamp("k4") > 0))"""
            contains """PREDICATES: ((abs(CAST(k4[#3] AS double)) > 0) AND (unix_timestamp(k4[#3]) > 0))"""
        }

        // test rewrite
        sql """alter catalog clickhouse_7_catalog set properties("function_rules" = '{"pushdown" : {"supported": ["abs"]}, "rewrite" : {"unix_timestamp" : "rewrite_func"}}')"""
        explain {
            sql("select k4 from type where abs(k4) > 0 and unix_timestamp(k4) > 0")
            contains """QUERY: SELECT "k4" FROM "doris_test"."type" WHERE ((abs("k4") > 0)) AND ((rewrite_func("k4") > 0))"""
            contains """((abs(CAST(k4[#3] AS double)) > 0) AND (unix_timestamp(k4[#3]) > 0))"""
        }

        // reset function rules
        sql """alter catalog clickhouse_7_catalog set properties("function_rules" = '');"""
        explain {
            sql("select k4 from type where abs(k4) > 0 and unix_timestamp(k4) > 0")
            contains """QUERY: SELECT "k4" FROM "doris_test"."type" WHERE ((toUnixTimestamp("k4") > 0))"""
            contains """PREDICATES: ((abs(CAST(k4[#3] AS double)) > 0) AND (unix_timestamp(k4[#3]) > 0))"""
        }

        // test invalid config
        test {
            sql """alter catalog clickhouse_7_catalog set properties("function_rules" = 'invalid_json')"""
            exception """Failed to parse push down rules: invalid_json"""
        }

        // sql """ drop catalog if exists clickhouse_7_schema """
    }
}
