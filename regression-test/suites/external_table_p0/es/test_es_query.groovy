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

suite("test_es_query", "p0,external,es,external_docker,external_docker_es") {
    String enabled = context.config.otherConfigs.get("enableEsTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String es_5_port = context.config.otherConfigs.get("es_5_port")
        String es_6_port = context.config.otherConfigs.get("es_6_port")
        String es_7_port = context.config.otherConfigs.get("es_7_port")
        String es_8_port = context.config.otherConfigs.get("es_8_port")

        sql """drop catalog if exists test_es_query_es5;"""
        sql """drop catalog if exists test_es_query_es6;"""
        sql """drop catalog if exists test_es_query_es7;"""
        sql """drop catalog if exists test_es_query_es8;"""
        sql """drop table if exists test_v1;"""
        sql """drop table if exists test_v2;"""

        // test old create-catalog syntax for compatibility
        sql """
            create catalog test_es_query_es5
            properties (
                "type"="es",
                "elasticsearch.hosts"="http://${externalEnvIp}:$es_5_port",
                "elasticsearch.nodes_discovery"="false",
                "elasticsearch.keyword_sniff"="true"
            );
        """
        sql """
            create catalog test_es_query_es6
            properties (
                "type"="es",
                "elasticsearch.hosts"="http://${externalEnvIp}:$es_6_port",
                "elasticsearch.nodes_discovery"="false",
                "elasticsearch.keyword_sniff"="true"
            );
        """

        // test new create catalog syntax
        sql """create catalog if not exists test_es_query_es7 properties(
            "type"="es",
            "hosts"="http://${externalEnvIp}:$es_7_port",
            "nodes_discovery"="false",
            "enable_keyword_sniff"="true"
        );
        """

        sql """create catalog if not exists test_es_query_es8 properties(
            "type"="es",
            "hosts"="http://${externalEnvIp}:$es_8_port",
            "nodes_discovery"="false",
            "enable_keyword_sniff"="true"
        );
        """

        sql """create catalog if not exists es6_hide properties(
            "type"="es",
            "hosts"="http://${externalEnvIp}:$es_6_port",
            "nodes_discovery"="false",
            "enable_keyword_sniff"="true",
            "include_hidden_index"="true"
        );
        """

        sql """create catalog if not exists es7_hide properties(
            "type"="es",
            "hosts"="http://${externalEnvIp}:$es_7_port",
            "nodes_discovery"="false",
            "enable_keyword_sniff"="true",
            "include_hidden_index"="true"
        );
        """

        // test external table for datetime
        sql """
            CREATE TABLE `test_v1` (
                `c_datetime` array<datetimev2> NULL,
                `c_long` array<bigint(20)> NULL,
                `c_unsigned_long` array<largeint(40)> NULL,
                `c_text` array<text> NULL,
                `c_short` array<smallint(6)> NULL,
                `c_ip` array<text> NULL,
                `test1` text NULL,
                `c_half_float` array<float> NULL,
                `test4` date NULL,
                `test5` datetime NULL,
                `test2` text NULL,
                `c_date` array<datev2> NULL,
                `test3` double NULL,
                `c_scaled_float` array<double> NULL,
                `c_float` array<float> NULL,
                `c_double` array<double> NULL,
                `c_keyword` array<text> NULL,
                `c_person` json NULL,
                `test6` datetime NULL,
                `test7` datetime NULL,
                `test8` datetime NULL,
                `c_byte` array<tinyint(4)> NULL,
                `c_bool` array<boolean> NULL,
                `c_integer` array<int(11)> NULL,
                `message` text NULL,
                `c_user` json NULL
            ) ENGINE=ELASTICSEARCH
            COMMENT 'ELASTICSEARCH'
            PROPERTIES (
                "hosts" = "http://${externalEnvIp}:$es_8_port",
                "index" = "test1",
                "nodes_discovery"="false",
                "enable_keyword_sniff"="true",
                "http_ssl_enabled"="false"
            );
        """
        order_qt_sql01 """select * from test_v1 where test2='text#1'"""
        order_qt_sql02 """select * from test_v1 where esquery(test2, '{"match":{"test2":"text#1"}}')"""
        order_qt_sql03 """select test4,test5,test6,test7,test8 from test_v1 order by test8"""
        order_qt_sql04 """select message from test_v1 where message != ''"""
        order_qt_sql05 """select message from test_v1 where message is not null"""
        order_qt_sql06 """select message from test_v1 where not_null_or_empty(message)"""
        order_qt_sql07 """select * from test_v1 where esquery(c_datetime, '{"term":{"c_datetime":"2020-01-01 12:00:00"}}');"""
        order_qt_sql08 """select c_person, c_user, json_extract(c_person, '\$.[0].name'), json_extract(c_user, '\$.[1].last') from test_v1;"""
       sql """
            CREATE TABLE `test_v2` (
                `c_datetime` array<datetimev2> NULL,
                `c_long` array<bigint(20)> NULL,
                `c_unsigned_long` array<largeint(40)> NULL,
                `c_text` array<text> NULL,
                `c_short` array<smallint(6)> NULL,
                `c_ip` array<text> NULL,
                `test1` text NULL,
                `c_half_float` array<float> NULL,
                `test4` datev2 NULL,
                `test5` datetimev2 NULL,
                `test2` text NULL,
                `c_date` array<datev2> NULL,
                `test3` double NULL,
                `c_scaled_float` array<double> NULL,
                `c_float` array<float> NULL,
                `c_double` array<double> NULL,
                `c_keyword` array<text> NULL,
                `c_person` json NULL,
                `test6` datetimev2 NULL,
                `test7` datetimev2 NULL,
                `test8` datetimev2 NULL,
                `c_byte` array<tinyint(4)> NULL,
                `c_bool` array<boolean> NULL,
                `c_integer` array<int(11)> NULL,
                `c_user` json NULL
            ) ENGINE=ELASTICSEARCH
            COMMENT 'ELASTICSEARCH'
            PROPERTIES (
                "hosts" = "http://${externalEnvIp}:$es_8_port",
                "index" = "test1",
                "nodes_discovery"="false",
                "enable_keyword_sniff"="true",
                "http_ssl_enabled"="false"
            );
        """
        order_qt_sql20 """select * from test_v2 where test2='text#1'"""
        order_qt_sql21 """select * from test_v2 where esquery(test2, '{"match":{"test2":"text#1"}}')"""
        order_qt_sql22 """select test4,test5,test6,test7,test8 from test_v2 order by test8"""
        order_qt_sql23 """select * from test_v2 where esquery(c_long, '{"term":{"c_long":"-1"}}');"""
        order_qt_sql24 """select c_person, c_user, json_extract(c_person, '\$.[0].name'), json_extract(c_user, '\$.[1].last') from test_v2;"""

        def query_catalogs = { -> 
            sql """switch internal"""
            sql """use regression_test_external_table_p0_es"""
            order_qt_sql01 """select * from test_v1 where test2='text#1'"""
            order_qt_sql02 """select * from test_v1 where esquery(test2, '{"match":{"test2":"text#1"}}')"""
            order_qt_sql03 """select test4,test5,test6,test7,test8 from test_v1 order by test8"""
            order_qt_sql04 """select message from test_v1 where message != ''"""
            order_qt_sql05 """select message from test_v1 where message is not null"""
            order_qt_sql06 """select message from test_v1 where not_null_or_empty(message)"""
            order_qt_sql07 """select * from test_v1 where esquery(c_datetime, '{"term":{"c_datetime":"2020-01-01 12:00:00"}}');"""
            order_qt_sql08 """select c_person, c_user, json_extract(c_person, '\$.[0].name'), json_extract(c_user, '\$.[1].last') from test_v1;"""
            
            order_qt_sql20 """select * from test_v2 where test2='text#1'"""
            order_qt_sql21 """select * from test_v2 where esquery(test2, '{"match":{"test2":"text#1"}}')"""
            order_qt_sql22 """select test4,test5,test6,test7,test8 from test_v2 order by test8"""
            order_qt_sql23 """select * from test_v2 where esquery(c_long, '{"term":{"c_long":"-1"}}');"""
            order_qt_sql24 """select c_person, c_user, json_extract(c_person, '\$.[0].name'), json_extract(c_user, '\$.[1].last') from test_v2;"""

            sql """switch test_es_query_es5"""
            order_qt_sql_5_02 """select * from test1 where test2='text#1'"""
            order_qt_sql_5_03 """select * from test2_20220808 where test4 >= '2022-08-08 00:00:00' and test4 < '2022-08-08 23:59:59'"""
            order_qt_sql_5_04 """select * from test2_20220808 where substring(test2, 2) = 'ext2'"""
            order_qt_sql_5_05 """select c_bool[1], c_byte[1], c_short[1], c_integer[1], c_long[1], c_unsigned_long[1], c_float[1], c_half_float[1], c_double[1], c_scaled_float[1], c_date[1], c_datetime[1], c_keyword[1], c_text[1], c_ip[1], json_extract(c_person, '\$.[0]') from test1"""
            order_qt_sql_5_06 """select c_bool[1], c_byte[1], c_short[1], c_integer[1], c_long[1], c_unsigned_long[1], c_float[1], c_half_float[1], c_double[1], c_scaled_float[1], c_date[1], c_datetime[1], c_keyword[1], c_text[1], c_ip[1], json_extract(c_person, '\$.[0]') from test2_20220808"""
            order_qt_sql_5_07 """select * from test1 where esquery(test2, '{"match":{"test2":"text#1"}}')"""
            order_qt_sql_5_08 """select c_bool, c_byte, c_short, c_integer, c_long, c_unsigned_long, c_float, c_half_float, c_double, c_scaled_float, c_date, c_datetime, c_keyword, c_text, c_ip, c_person from test1"""
            order_qt_sql_5_09 """select c_bool, c_byte, c_short, c_integer, c_long, c_unsigned_long, c_float, c_half_float, c_double, c_scaled_float, c_date, c_datetime, c_keyword, c_text, c_ip, c_person from test2_20220808"""
            order_qt_sql_5_10 """select * from test1 where test1='string1'"""
            order_qt_sql_5_11 """select * from test1 where test1='string2'"""
            order_qt_sql_5_12 """select * from test1 where test1='string3'"""
            order_qt_sql_5_13 """select test6 from test1 where test1='string1'"""
            order_qt_sql_5_14 """select test6 from test1 where test1='string2'"""
            order_qt_sql_5_15 """select test6 from test1 where test1='string3'"""
            order_qt_sql_5_16 """select message from test1 where message != ''"""
            order_qt_sql_5_17 """select message from test1 where message is not null"""
            order_qt_sql_5_18 """select message from test1 where not_null_or_empty(message)"""
            order_qt_sql_5_19 """select * from test1 where esquery(c_unsigned_long, '{"match":{"c_unsigned_long":0}}')"""
            order_qt_sql_5_20 """select c_person, c_user, json_extract(c_person, '\$.[0].name'), json_extract(c_user, '\$.[1].last') from test1;"""

            sql """switch test_es_query_es6"""
            // order_qt_sql_6_01 """show tables"""
            order_qt_sql_6_02 """select * from test1 where test2='text#1'"""
            order_qt_sql_6_03 """select * from test2_20220808 where test4 >= '2022-08-08 00:00:00' and test4 < '2022-08-08 23:59:59'"""
            order_qt_sql_6_04 """select * from test2_20220808 where substring(test2, 2) = 'ext2'"""
            order_qt_sql_6_05 """select c_bool[1], c_byte[1], c_short[1], c_integer[1], c_long[1], c_unsigned_long[1], c_float[1], c_half_float[1], c_double[1], c_scaled_float[1], c_date[1], c_datetime[1], c_keyword[1], c_text[1], c_ip[1], json_extract(c_person, '\$.[0]') from test1"""
            order_qt_sql_6_06 """select c_bool[1], c_byte[1], c_short[1], c_integer[1], c_long[1], c_unsigned_long[1], c_float[1], c_half_float[1], c_double[1], c_scaled_float[1], c_date[1], c_datetime[1], c_keyword[1], c_text[1], c_ip[1], json_extract(c_person, '\$.[0]') from test2_20220808"""
            order_qt_sql_6_07 """select * from test1 where esquery(test2, '{"match":{"test2":"text#1"}}')"""
            order_qt_sql_6_08 """select c_bool, c_byte, c_short, c_integer, c_long, c_unsigned_long, c_float, c_half_float, c_double, c_scaled_float, c_date, c_datetime, c_keyword, c_text, c_ip, c_person from test1"""
            order_qt_sql_6_09 """select c_bool, c_byte, c_short, c_integer, c_long, c_unsigned_long, c_float, c_half_float, c_double, c_scaled_float, c_date, c_datetime, c_keyword, c_text, c_ip, c_person from test2_20220808"""
            order_qt_sql_6_10 """select * from test1 where test1='string1'"""
            order_qt_sql_6_11 """select * from test1 where test1='string2'"""
            order_qt_sql_6_12 """select * from test1 where test1='string3'"""
            order_qt_sql_6_13 """select test6 from test1 where test1='string1'"""
            order_qt_sql_6_14 """select test6 from test1 where test1='string2'"""
            order_qt_sql_6_15 """select test6 from test1 where test1='string3'"""
            order_qt_sql_6_16 """select message from test1 where message != ''"""
            order_qt_sql_6_17 """select message from test1 where message is not null"""
            order_qt_sql_6_18 """select message from test1 where not_null_or_empty(message)"""
            order_qt_sql_6_19 """select * from test1 where esquery(c_person, '{"match":{"c_person.name":"Andy"}}')"""
            order_qt_sql_6_20 """select c_person, c_user, json_extract(c_person, '\$.[0].name'), json_extract(c_user, '\$.[1].last') from test1;"""

            List<List<String>> tables6N = sql """show tables"""
            boolean notContainHide = true
            tables6N.forEach {
                if (it[0] == ".hide"){
                    notContainHide = false
                }
            }
            assertTrue(notContainHide)

            sql """switch es6_hide"""
            List<List<String>> tables6Y = sql """show tables"""
            boolean containHide = false
            tables6Y.forEach {
                if (it[0] == ".hide"){
                    containHide = true
                }
            }
            assertTrue(containHide)

            sql """switch test_es_query_es7"""
            // order_qt_sql_7_01 """show tables"""
            order_qt_sql_7_02 """select * from test1 where test2='text#1'"""
            order_qt_sql_7_03 """select * from test2_20220808 where test4 >= '2022-08-08 00:00:00' and test4 < '2022-08-08 23:59:59'"""
            order_qt_sql_7_04 """select * from test2_20220808 where substring(test2, 2) = 'ext2'"""
            order_qt_sql_7_05 """select c_bool[1], c_byte[1], c_short[1], c_integer[1], c_long[1], c_unsigned_long[1], c_float[1], c_half_float[1], c_double[1], c_scaled_float[1], c_date[1], c_datetime[1], c_keyword[1], c_text[1], c_ip[1], json_extract(c_person, '\$.[0]') from test1"""
            order_qt_sql_7_06 """select c_bool[1], c_byte[1], c_short[1], c_integer[1], c_long[1], c_unsigned_long[1], c_float[1], c_half_float[1], c_double[1], c_scaled_float[1], c_date[1], c_datetime[1], c_keyword[1], c_text[1], c_ip[1], json_extract(c_person, '\$.[0]') from test2"""
            order_qt_sql_7_07 """select * from test1 where esquery(test2, '{"match":{"test2":"text#1"}}')"""
            order_qt_sql_7_08 """select c_bool, c_byte, c_short, c_integer, c_long, c_unsigned_long, c_float, c_half_float, c_double, c_scaled_float, c_date, c_datetime, c_keyword, c_text, c_ip, c_person from test1"""
            order_qt_sql_7_09 """select c_bool, c_byte, c_short, c_integer, c_long, c_unsigned_long, c_float, c_half_float, c_double, c_scaled_float, c_date, c_datetime, c_keyword, c_text, c_ip, c_person from test2"""
            order_qt_sql_7_10 """select * from test3_20231005"""
            order_qt_sql_7_11 """select * from test1 where test1='string1'"""
            order_qt_sql_7_12 """select * from test1 where test1='string2'"""
            order_qt_sql_7_13 """select * from test1 where test1='string3'"""
            order_qt_sql_7_14 """select * from test1 where test1='string4'"""
            order_qt_sql_7_15 """select test10 from test1 where test1='string1'"""
            order_qt_sql_7_16 """select test10 from test1 where test1='string2'"""
            order_qt_sql_7_17 """select test10 from test1 where test1='string3'"""
            order_qt_sql_7_18 """select test10 from test1 where test1='string4'"""
            order_qt_sql_7_19 """select message from test1 where message != ''"""
            order_qt_sql_7_20 """select message from test1 where message is not null"""
            order_qt_sql_7_21 """select message from test1 where not_null_or_empty(message)"""
            order_qt_sql_7_22 """select * from test1 where esquery(my_wildcard, '{ "wildcard": { "my_wildcard": { "value":"*quite*lengthy" } } }');"""
            order_qt_sql_7_23 """select * from test1 where level = 'debug'"""
            order_qt_sql_7_24 """select * from test1 where esquery(c_float, '{"match":{"c_float":1.1}}')"""
            order_qt_sql_7_25 """select c_person, c_user, json_extract(c_person, '\$.[0].name'), json_extract(c_user, '\$.[1].last') from test1;"""

            List<List<String>> tables7N = sql """show tables"""
            boolean notContainHide7 = true
            tables7N.forEach {
                if (it[0] == ".hide"){
                    notContainHide7 = false
                }
            }
            assertTrue(notContainHide7)

            sql """switch es7_hide"""
            List<List<String>> tables7Y = sql """show tables"""
            boolean containeHide7 = false
            tables7Y.forEach {
                if (it[0] == (".hide")){
                    containeHide7 = true
                }
            }
            assertTrue(containeHide7)

            order_qt_sql_7_26 """select * from test3_20231005"""

            sql """switch test_es_query_es8"""
            order_qt_sql_8_01 """select * from test1 where test2='text#1'"""
            order_qt_sql_8_02 """select * from test2_20220808 where test4 >= '2022-08-08 00:00:00' and test4 < '2022-08-08 23:59:59'"""
            order_qt_sql_8_03 """select c_bool[1], c_byte[1], c_short[1], c_integer[1], c_long[1], c_unsigned_long[1], c_float[1], c_half_float[1], c_double[1], c_scaled_float[1], c_date[1], c_datetime[1], c_keyword[1], c_text[1], c_ip[1], json_extract(c_person, '\$.[0]') from test1"""
            order_qt_sql_8_04 """select c_bool[1], c_byte[1], c_short[1], c_integer[1], c_long[1], c_unsigned_long[1], c_float[1], c_half_float[1], c_double[1], c_scaled_float[1], c_date[1], c_datetime[1], c_keyword[1], c_text[1], c_ip[1], json_extract(c_person, '\$.[0]') from test2"""
            order_qt_sql_8_05 """select * from test1 where esquery(test2, '{"match":{"test2":"text#1"}}')"""
            order_qt_sql_8_06 """select c_bool, c_byte, c_short, c_integer, c_long, c_unsigned_long, c_float, c_half_float, c_double, c_scaled_float, c_date, c_datetime, c_keyword, c_text, c_ip, c_person from test1"""
            order_qt_sql_8_07 """select c_bool, c_byte, c_short, c_integer, c_long, c_unsigned_long, c_float, c_half_float, c_double, c_scaled_float, c_date, c_datetime, c_keyword, c_text, c_ip, c_person from test2"""
            order_qt_sql_8_08 """select * from test3_20231005"""
            order_qt_sql_8_09 """select * from test1 where test1='string1'"""
            order_qt_sql_8_10 """select * from test1 where test1='string2'"""
            order_qt_sql_8_11 """select * from test1 where test1='string3'"""
            order_qt_sql_8_12 """select * from test1 where test1='string4'"""
            order_qt_sql_8_13 """select test10 from test1 where test1='string1'"""
            order_qt_sql_8_14 """select test10 from test1 where test1='string2'"""
            order_qt_sql_8_15 """select test10 from test1 where test1='string3'"""
            order_qt_sql_8_16 """select test10 from test1 where test1='string4'"""
            order_qt_sql_8_17 """select message from test1 where message != ''"""
            order_qt_sql_8_18 """select message from test1 where message is not null"""
            order_qt_sql_8_19 """select message from test1 where not_null_or_empty(message)"""
            order_qt_sql_8_20 """select * from test1 where esquery(my_wildcard, '{ "wildcard": { "my_wildcard": { "value":"*quite*lengthy" } } }');"""
            order_qt_sql_8_21 """select * from test1 where level = 'debug'"""
            order_qt_sql_8_22 """select * from test1 where esquery(c_ip, '{"match":{"c_ip":"192.168.0.1"}}')"""
            order_qt_sql_8_23 """select c_person, c_user, json_extract(c_person, '\$.[0].name'), json_extract(c_user, '\$.[1].last') from test1;"""
        
        }

        sql """set enable_es_parallel_scroll=true"""
        query_catalogs()

        sql """set enable_es_parallel_scroll=false"""
        query_catalogs()
    }
}
