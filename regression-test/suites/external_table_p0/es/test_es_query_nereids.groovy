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

suite("test_es_query_nereids", "p0,external,es,external_docker,external_docker_es") {
    String enabled = context.config.otherConfigs.get("enableEsTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String es_6_port = context.config.otherConfigs.get("es_6_port")
        String es_7_port = context.config.otherConfigs.get("es_7_port")
        String es_8_port = context.config.otherConfigs.get("es_8_port")

        sql """drop catalog if exists es6_nereids;"""
        sql """drop catalog if exists es7_nereids;"""
        sql """drop catalog if exists es8_nereids;"""
        sql """drop table if exists test_v1_nereids;"""
        sql """drop table if exists test_v2_nereids;"""
        sql """set enable_nereids_planner=true;"""

        // test old create-catalog syntax for compatibility
        sql """
            create catalog es6_nereids
            properties (
                "type"="es",
                "elasticsearch.hosts"="http://${externalEnvIp}:$es_6_port",
                "elasticsearch.nodes_discovery"="false",
                "elasticsearch.keyword_sniff"="true"
            );
        """

        // test new create catalog syntax
        sql """create catalog if not exists es7_nereids properties(
            "type"="es",
            "hosts"="http://${externalEnvIp}:$es_7_port",
            "nodes_discovery"="false",
            "enable_keyword_sniff"="true"
        );
        """

        sql """create catalog if not exists es8_nereids properties(
            "type"="es",
            "hosts"="http://${externalEnvIp}:$es_8_port",
            "nodes_discovery"="false",
            "enable_keyword_sniff"="true"
        );
        """

        // test external table for datetime
        sql """
            CREATE TABLE `test_v1_nereids` (
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
        // TODO(ftw): should open these annotation when nereids support es external table
        // order_qt_sql52 """select * from test_v1_nereids where test2='text#1'"""

       sql """
            CREATE TABLE `test_v2_nereids` (
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

        sql """set enable_fallback_to_original_planner=false;"""

        // TODO(ftw): should open these annotation when nereids support es external table
        // order_qt_sql51 """select * from test_v2_nereids where test2='text#1'"""


        sql """switch es6_nereids"""
        order_qt_sql62 """select test1, test2, test3, test4 from test1 where test2='text#1'"""
        order_qt_sql63 """select test1, test2, test3, test4 from test2_20220808 where test4='2022-08-08 08:00:00'"""
        order_qt_sql64 """select test1, test2, test3, test4 from test2_20220808 where substring(test2, 2) = 'ext2'"""
        // TODO(ftw): should open these annotation when nereids support ARRAY
        // order_qt_sql62 """select * from test1 where test2='text#1'"""
        // order_qt_sql63 """select * from test2_20220808 where test4='2022-08-08'"""
        // order_qt_sql64 """select * from test2_20220808 where substring(test2, 2) = 'ext2'"""
        // order_qt_sql65 """select c_bool[1], c_byte[1], c_short[1], c_integer[1], c_long[1], c_unsigned_long[1], c_float[1], c_half_float[1], c_double[1], c_scaled_float[1], c_date[1], c_datetime[1], c_keyword[1], c_text[1], c_ip[1], c_person[1] from test1"""
        // order_qt_sql66 """select c_bool[1], c_byte[1], c_short[1], c_integer[1], c_long[1], c_unsigned_long[1], c_float[1], c_half_float[1], c_double[1], c_scaled_float[1], c_date[1], c_datetime[1], c_keyword[1], c_text[1], c_ip[1], c_person[1] from test2_20220808"""


        sql """switch es7_nereids"""
        order_qt_sql72 """select test1, test2, test3, test4, test5, test6, test7, test8 from test1"""
        order_qt_sql73 """select test1, test2, test3, test4, test5, test6, test7, test8 from test2_20220808"""
        order_qt_sql74 """select test1, test2, test3, test4, test5, test6, test7, test8 from test2_20220808"""
        // TODO(ftw): should open these annotation when nereids support ARRAY
        // order_qt_sql72 """select * from test1 where test2='text#1'"""
        // order_qt_sql73 """select * from test2_20220808 where test4='2022-08-08'"""
        // order_qt_sql74 """select * from test2_20220808 where substring(test2, 2) = 'ext2'"""
        // order_qt_sql75 """select c_bool[1], c_byte[1], c_short[1], c_integer[1], c_long[1], c_unsigned_long[1], c_float[1], c_half_float[1], c_double[1], c_scaled_float[1], c_date[1], c_datetime[1], c_keyword[1], c_text[1], c_ip[1], c_person[1] from test1"""
        // order_qt_sql76 """select c_bool[1], c_byte[1], c_short[1], c_integer[1], c_long[1], c_unsigned_long[1], c_float[1], c_half_float[1], c_double[1], c_scaled_float[1], c_date[1], c_datetime[1], c_keyword[1], c_text[1], c_ip[1], c_person[1] from test2"""
        
        
        sql """switch es8_nereids"""
        order_qt_sql81 """select test1, test2, test3, test4, test5, test6, test7, test8 from test1"""
        order_qt_sql82 """select test1, test2, test3, test4, test5, test6, test7, test8 from test2_20220808"""
        // TODO(ftw): should open these annotation when nereids support ARRAY
        // order_qt_sql81 """select * from test1 where test2='text#1'"""
        // order_qt_sql82 """select * from test2_20220808 where test4='2022-08-08'"""
        // order_qt_sql83 """select c_bool[1], c_byte[1], c_short[1], c_integer[1], c_long[1], c_unsigned_long[1], c_float[1], c_half_float[1], c_double[1], c_scaled_float[1], c_date[1], c_datetime[1], c_keyword[1], c_text[1], c_ip[1], c_person[1] from test1"""
        // order_qt_sql84 """select c_bool[1], c_byte[1], c_short[1], c_integer[1], c_long[1], c_unsigned_long[1], c_float[1], c_half_float[1], c_double[1], c_scaled_float[1], c_date[1], c_datetime[1], c_keyword[1], c_text[1], c_ip[1], c_person[1] from test2"""
    }
}
