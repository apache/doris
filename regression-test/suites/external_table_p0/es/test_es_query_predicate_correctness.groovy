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

// Regression tests for ES connector predicate pushdown correctness.
// Covers fixes:
//   P0-4: REGEXP must generate ES 'regexp' query, not 'wildcard'
//   P1-3: IS NULL must generate 'must_not exists', not 'term null'

suite("test_es_query_predicate_correctness", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableEsTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String es_7_port = context.config.otherConfigs.get("es_7_port")
        String es_8_port = context.config.otherConfigs.get("es_8_port")

        sql """drop catalog if exists es7_pred_test;"""
        sql """drop catalog if exists es8_pred_test;"""

        sql """create catalog if not exists es7_pred_test properties(
            "type"="es",
            "hosts"="http://${externalEnvIp}:$es_7_port",
            "nodes_discovery"="false",
            "enable_keyword_sniff"="true"
        );"""

        sql """create catalog if not exists es8_pred_test properties(
            "type"="es",
            "hosts"="http://${externalEnvIp}:$es_8_port",
            "nodes_discovery"="false",
            "enable_keyword_sniff"="true"
        );"""

        // ======================================================================
        // ES 7 tests
        // ======================================================================
        sql """switch es7_pred_test"""

        // P0-4: REGEXP with character class [12] → ES 'regexp' query
        // Before fix: generated 'wildcard' query, returning 0 rows
        // After fix: generates 'regexp' query, correctly matching string1 and string2
        order_qt_es7_regexp_charclass """select test1 from test1 where test1 regexp 'string[12]' order by test1"""

        // REGEXP with dot-star
        order_qt_es7_regexp_dotstar """select test1 from test1 where test1 regexp 'string.*' order by test1"""

        // REGEXP with alternation
        order_qt_es7_regexp_alternation """select test1 from test1 where test1 regexp 'string1|string3' order by test1"""

        // LIKE should still work (uses wildcard query, correct for LIKE)
        order_qt_es7_like """select test1 from test1 where test1 like 'string%' order by test1"""

        // P1-3: IS NULL / IS NOT NULL
        // Before fix: IS NULL generated {term: {field: null}} returning 0 rows
        // After fix: generates {bool: {must_not: [{exists: {field: ...}}]}}
        order_qt_es7_is_null """select count(*) from test1 where message is null"""
        order_qt_es7_is_not_null """select count(*) from test1 where message is not null"""
        order_qt_es7_total_count """select count(*) from test1"""

        // ======================================================================
        // ES 8 tests (same queries, verifying both versions)
        // ======================================================================
        sql """switch es8_pred_test"""

        order_qt_es8_regexp_charclass """select test1 from test1 where test1 regexp 'string[12]' order by test1"""
        order_qt_es8_regexp_dotstar """select test1 from test1 where test1 regexp 'string.*' order by test1"""
        order_qt_es8_regexp_alternation """select test1 from test1 where test1 regexp 'string1|string3' order by test1"""
        order_qt_es8_like """select test1 from test1 where test1 like 'string%' order by test1"""
        order_qt_es8_is_null """select count(*) from test1 where message is null"""
        order_qt_es8_is_not_null """select count(*) from test1 where message is not null"""
        order_qt_es8_total_count """select count(*) from test1"""

        // cleanup
        sql """drop catalog if exists es7_pred_test;"""
        sql """drop catalog if exists es8_pred_test;"""
    }
}
