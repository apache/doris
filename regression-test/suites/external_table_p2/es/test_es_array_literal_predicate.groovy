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

// An ES catalog evaluates in Doris a comparison between an ARRAY column and an ARRAY constant, because a term or
// range query matches any one value of a multi-valued field rather than the whole array. A docvalue scan used to
// push the comparison as a term query on the constant's text, such as {"term":{"c_keyword":"[a, b, c]"}}, and to
// drop it from what Doris evaluated, so an equality matched no document and an inequality matched every one.
suite("test_es_array_literal_predicate", "p2,external") {
    String enabled = context.config.otherConfigs.get("enableEsTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String es_7_port = context.config.otherConfigs.get("es_7_port")
        String es_8_port = context.config.otherConfigs.get("es_8_port")

        sql """drop catalog if exists test_es_array_literal_predicate_es7"""
        sql """create catalog test_es_array_literal_predicate_es7 properties (
            "type" = "es",
            "hosts" = "http://${externalEnvIp}:${es_7_port}",
            "nodes_discovery" = "false",
            "enable_keyword_sniff" = "true"
        )"""
        sql """drop catalog if exists test_es_array_literal_predicate_es8"""
        sql """create catalog test_es_array_literal_predicate_es8 properties (
            "type" = "es",
            "hosts" = "http://${externalEnvIp}:${es_8_port}",
            "nodes_discovery" = "false",
            "enable_keyword_sniff" = "true"
        )"""

        // Every document of test1 has c_keyword ["a", "b", "c"] and c_long [-1, 0, 1, 2]. The scan reads only
        // docvalue fields (test3 and the filtered column), so it is a docvalue scan, the one that pushed these.
        sql """switch test_es_array_literal_predicate_es7"""
        order_qt_es7_keyword_eq """select test3 from test1 where c_keyword = ['a', 'b', 'c']"""
        order_qt_es7_keyword_ne """select test3 from test1 where c_keyword != ['a', 'b', 'c']"""
        // On a long field the text failed the whole query: failed to create query: For input string.
        order_qt_es7_long_eq """select test3 from test1 where c_long = [-1, 0, 1, 2]"""
        order_qt_es7_and """select test3 from test1 where c_keyword = ['a', 'b', 'c'] and test3 > 4"""
        // The comparison is left for Doris, so the scan does not stop early at the LIMIT.
        explain {
            sql """select test3 from test1 where c_keyword = ['a', 'b', 'c'] limit 3"""
            contains "PREDICATES: (c_keyword"
            notContains "ES terminate_after"
        }

        sql """switch test_es_array_literal_predicate_es8"""
        order_qt_es8_keyword_eq """select test3 from test1 where c_keyword = ['a', 'b', 'c']"""
        order_qt_es8_keyword_ne """select test3 from test1 where c_keyword != ['a', 'b', 'c']"""
        order_qt_es8_long_eq """select test3 from test1 where c_long = [-1, 0, 1, 2]"""
        order_qt_es8_and """select test3 from test1 where c_keyword = ['a', 'b', 'c'] and test3 > 4"""
        explain {
            sql """select test3 from test1 where c_keyword = ['a', 'b', 'c'] limit 3"""
            contains "PREDICATES: (c_keyword"
            notContains "ES terminate_after"
        }
        sql """switch internal"""
    }
}
