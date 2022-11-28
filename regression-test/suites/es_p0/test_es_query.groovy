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

suite("test_es_query", "p0") {

    String enabled = context.config.otherConfigs.get("enableEsTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String es_6_port = context.config.otherConfigs.get("es_6_port")
        String es_7_port = context.config.otherConfigs.get("es_7_port")
        String es_8_port = context.config.otherConfigs.get("es_8_port")

        sql """drop catalog if exists es6;"""
        sql """drop catalog if exists es7;"""
        sql """drop catalog if exists es8;"""
        sql """
            create catalog es6
            properties (
                "type"="es",
                "elasticsearch.hosts"="http://127.0.0.1:$es_6_port",
                "elasticsearch.nodes_discovery"="false",
                "elasticsearch.keyword_sniff"="true"
            );
            """
        sql """
            create catalog es7
            properties (
                "type"="es",
                "elasticsearch.hosts"="http://127.0.0.1:$es_7_port",
                "elasticsearch.nodes_discovery"="false",
                "elasticsearch.keyword_sniff"="true"
            );
            """
        sql """
            create catalog es8
            properties (
                "type"="es",
                "elasticsearch.hosts"="http://127.0.0.1:$es_8_port",
                "elasticsearch.nodes_discovery"="false",
                "elasticsearch.keyword_sniff"="false"
            );
            """
        sql """switch es6"""
        // order_qt_sql61 """show tables"""
        order_qt_sql62 """select * from test1 where test2='text#1'"""
        order_qt_sql63 """select * from test2_20220808 where test4='2022-08-08'"""
        order_qt_sql64 """select * from test2_20220808 where substring(test2, 2) = 'ext2'"""
        sql """switch es7"""
        // order_qt_sql71 """show tables"""
        order_qt_sql72 """select * from test1 where test2='text#1'"""
        order_qt_sql73 """select * from test2_20220808 where test4='2022-08-08'"""
        order_qt_sql74 """select * from test2_20220808 where substring(test2, 2) = 'ext2'"""
        // es8 has some problem, need fix
        // sql """switch es8"""
        // order_qt_sql1 """select * from test1 where test2='text'"""
        // order_qt_sql2 """select * from test2_20220808 where test4='2022-08-08'"""
    }
}
