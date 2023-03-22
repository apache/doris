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

suite("test_es_query_no_http_url", "p0") {

    String enabled = context.config.otherConfigs.get("enableEsTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String es_6_port = context.config.otherConfigs.get("es_6_port")
        String es_7_port = context.config.otherConfigs.get("es_7_port")
        String es_8_port = context.config.otherConfigs.get("es_8_port")

        sql """drop catalog if exists es6;"""
        sql """drop catalog if exists es7;"""
        sql """drop catalog if exists es8;"""
        sql """drop resource if exists es6_resource;"""
        sql """drop resource if exists es7_resource;"""
        sql """drop resource if exists es8_resource;"""
        sql """drop table if exists test_v1;"""
        sql """drop table if exists test_v2;"""

        // test old create-catalog syntax for compatibility
        sql """
            create catalog es6
            properties (
                "type"="es",
                "elasticsearch.hosts"="127.0.0.1:$es_6_port",
                "elasticsearch.nodes_discovery"="false",
                "elasticsearch.keyword_sniff"="true"
            );
        """

        // test new create catalog syntax
        sql """create resource if not exists es7_resource properties(
            "type"="es",
            "hosts"="127.0.0.1:$es_7_port",
            "nodes_discovery"="false",
            "enable_keyword_sniff"="true"
        );
        """

        sql """create resource if not exists es8_resource properties(
            "type"="es",
            "hosts"="127.0.0.1:$es_8_port",
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
                "hosts" = "127.0.0.1:$es_7_port",
                "index" = "test1",
                "nodes_discovery"="false",
                "enable_keyword_sniff"="true",
                "http_ssl_enabled"="false"
            );
        """
        order_qt_sql51 """select * from test_v1 where test2='text#1'"""

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
                "hosts" = "127.0.0.1:$es_8_port",
                "index" = "test1",
                "nodes_discovery"="false",
                "enable_keyword_sniff"="true",
                "http_ssl_enabled"="false"
            );
        """
        order_qt_sql52 """select * from test_v2 where test2='text#1'"""

        sql """create catalog if not exists es6 with resource es6_resource;"""
        sql """create catalog if not exists es7 with resource es7_resource;"""
        sql """create catalog if not exists es8 with resource es8_resource;"""
        // es6
        sql """switch es6"""
        order_qt_sql61 """select * from test1 where test2='text#1'"""
        // es7
        sql """switch es7"""
        order_qt_sql71 """select * from test1 where test2='text#1'"""
        // es8
        sql """switch es8"""
        order_qt_sql81 """select * from test1 where test2='text#1'"""

        sql """drop catalog if exists es6;"""
        sql """drop catalog if exists es7;"""
        sql """drop catalog if exists es8;"""
        sql """drop resource if exists es6_resource;"""
        sql """drop resource if exists es7_resource;"""
        sql """drop resource if exists es8_resource;"""
    }
}
