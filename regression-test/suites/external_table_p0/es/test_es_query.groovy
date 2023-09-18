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
        String es_6_port = context.config.otherConfigs.get("es_6_port")
        String es_7_port = context.config.otherConfigs.get("es_7_port")
        String es_8_port = context.config.otherConfigs.get("es_8_port")

        sql """drop catalog if exists es6;"""
        sql """drop catalog if exists es7;"""
        sql """drop catalog if exists es8;"""
        sql """drop table if exists test_v1;"""
        sql """drop table if exists test_v2;"""

        // test old create-catalog syntax for compatibility
        sql """
            create catalog es6
            properties (
                "type"="es",
                "elasticsearch.hosts"="http://${externalEnvIp}:$es_6_port",
                "elasticsearch.nodes_discovery"="false",
                "elasticsearch.keyword_sniff"="true"
            );
        """

        // test new create catalog syntax
        sql """create catalog if not exists es7 properties(
            "type"="es",
            "hosts"="http://${externalEnvIp}:$es_7_port",
            "nodes_discovery"="false",
            "enable_keyword_sniff"="true"
        );
        """

        sql """create catalog if not exists es8 properties(
            "type"="es",
            "hosts"="http://${externalEnvIp}:$es_8_port",
            "nodes_discovery"="false",
            "enable_keyword_sniff"="true"
        );
        """

        // test external table for datetime
        sql """
            CREATE TABLE `test_v1` (
                `c_datetime` array<datev2> NULL,
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
                `c_person` array<text> NULL,
                `test6` datetime NULL,
                `test7` datetime NULL,
                `test8` datetime NULL,
                `c_byte` array<tinyint(4)> NULL,
                `c_bool` array<boolean> NULL,
                `c_integer` array<int(11)> NULL
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
        order_qt_sql51 """select * from test_v1 where test2='text#1'"""
        order_qt_sql52 """select * from test_v1 where esquery(test2, '{"match":{"test2":"text#1"}}')"""
        order_qt_sql53 """select test4,test5,test6,test7,test8 from test_v1 order by test8"""

       sql """
            CREATE TABLE `test_v2` (
                `c_datetime` array<datev2> NULL,
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
                `c_person` array<text> NULL,
                `test6` datetimev2 NULL,
                `test7` datetimev2 NULL,
                `test8` datetimev2 NULL,
                `c_byte` array<tinyint(4)> NULL,
                `c_bool` array<boolean> NULL,
                `c_integer` array<int(11)> NULL
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
        order_qt_sql53 """select * from test_v2 where test2='text#1'"""
        order_qt_sql54 """select * from test_v2 where esquery(test2, '{"match":{"test2":"text#1"}}')"""
        order_qt_sql55 """select test4,test5,test6,test7,test8 from test_v2 order by test8"""

        sql """switch es6"""
        // order_qt_sql61 """show tables"""
        order_qt_sql62 """select * from test1 where test2='text#1'"""
        order_qt_sql63 """select * from test2_20220808 where test4 >= '2022-08-08 00:00:00' and test4 < '2022-08-08 23:59:59'"""
        order_qt_sql64 """select * from test2_20220808 where substring(test2, 2) = 'ext2'"""
        order_qt_sql65 """select c_bool[1], c_byte[1], c_short[1], c_integer[1], c_long[1], c_unsigned_long[1], c_float[1], c_half_float[1], c_double[1], c_scaled_float[1], c_date[1], c_datetime[1], c_keyword[1], c_text[1], c_ip[1], c_person[1] from test1"""
        order_qt_sql66 """select c_bool[1], c_byte[1], c_short[1], c_integer[1], c_long[1], c_unsigned_long[1], c_float[1], c_half_float[1], c_double[1], c_scaled_float[1], c_date[1], c_datetime[1], c_keyword[1], c_text[1], c_ip[1], c_person[1] from test2_20220808"""
        order_qt_sql67 """select * from test1 where esquery(test2, '{"match":{"test2":"text#1"}}')"""
        order_qt_sql68 """select c_bool, c_byte, c_short, c_integer, c_long, c_unsigned_long, c_float, c_half_float, c_double, c_scaled_float, c_date, c_datetime, c_keyword, c_text, c_ip, c_person from test1"""
        order_qt_sql69 """select c_bool, c_byte, c_short, c_integer, c_long, c_unsigned_long, c_float, c_half_float, c_double, c_scaled_float, c_date, c_datetime, c_keyword, c_text, c_ip, c_person from test2_20220808"""
        sql """switch es7"""
        // order_qt_sql71 """show tables"""
        order_qt_sql72 """select * from test1 where test2='text#1'"""
        order_qt_sql73 """select * from test2_20220808 where test4 >= '2022-08-08 00:00:00' and test4 < '2022-08-08 23:59:59'"""
        order_qt_sql74 """select * from test2_20220808 where substring(test2, 2) = 'ext2'"""
        order_qt_sql75 """select c_bool[1], c_byte[1], c_short[1], c_integer[1], c_long[1], c_unsigned_long[1], c_float[1], c_half_float[1], c_double[1], c_scaled_float[1], c_date[1], c_datetime[1], c_keyword[1], c_text[1], c_ip[1], c_person[1] from test1"""
        order_qt_sql76 """select c_bool[1], c_byte[1], c_short[1], c_integer[1], c_long[1], c_unsigned_long[1], c_float[1], c_half_float[1], c_double[1], c_scaled_float[1], c_date[1], c_datetime[1], c_keyword[1], c_text[1], c_ip[1], c_person[1] from test2"""
        order_qt_sql77 """select * from test1 where esquery(test2, '{"match":{"test2":"text#1"}}')"""
        order_qt_sql78 """select c_bool, c_byte, c_short, c_integer, c_long, c_unsigned_long, c_float, c_half_float, c_double, c_scaled_float, c_date, c_datetime, c_keyword, c_text, c_ip, c_person from test1"""
        order_qt_sql79 """select c_bool, c_byte, c_short, c_integer, c_long, c_unsigned_long, c_float, c_half_float, c_double, c_scaled_float, c_date, c_datetime, c_keyword, c_text, c_ip, c_person from test2"""
        sql """switch es8"""
        order_qt_sql81 """select * from test1 where test2='text#1'"""
        order_qt_sql82 """select * from test2_20220808 where test4 >= '2022-08-08 00:00:00' and test4 < '2022-08-08 23:59:59'"""
        order_qt_sql83 """select c_bool[1], c_byte[1], c_short[1], c_integer[1], c_long[1], c_unsigned_long[1], c_float[1], c_half_float[1], c_double[1], c_scaled_float[1], c_date[1], c_datetime[1], c_keyword[1], c_text[1], c_ip[1], c_person[1] from test1"""
        order_qt_sql84 """select c_bool[1], c_byte[1], c_short[1], c_integer[1], c_long[1], c_unsigned_long[1], c_float[1], c_half_float[1], c_double[1], c_scaled_float[1], c_date[1], c_datetime[1], c_keyword[1], c_text[1], c_ip[1], c_person[1] from test2"""
        order_qt_sql85 """select * from test1 where esquery(test2, '{"match":{"test2":"text#1"}}')"""
        order_qt_sql86 """select c_bool, c_byte, c_short, c_integer, c_long, c_unsigned_long, c_float, c_half_float, c_double, c_scaled_float, c_date, c_datetime, c_keyword, c_text, c_ip, c_person from test1"""
        order_qt_sql87 """select c_bool, c_byte, c_short, c_integer, c_long, c_unsigned_long, c_float, c_half_float, c_double, c_scaled_float, c_date, c_datetime, c_keyword, c_text, c_ip, c_person from test2"""

    }
}
