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

// Check SQL predicate results independently of whether ES index semantics permit pushdown.

suite("test_es_query_predicate_correctness", "p2,external") {
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

        def verifySourcePredicates = { String version ->
            // SQL predicates apply to materialized source values even when ES omits their keyword index values.
            def expected = "internal.${context.dbName}.es_predicate_source_expected"
            sql "drop table if exists ${expected}"
            sql """create table ${expected} (row_id int, test1 string, message string)
                distributed by hash(row_id) buckets 1 properties("replication_num"="1")"""
            sql "insert into ${expected} select 0,test1,message from test1"
            ["test1 regexp 'string[12]'", "test1 regexp 'string.*'",
             "test1 regexp 'string1|string3'", "test1 like 'string%'",
             "message is null", "message is not null"].eachWithIndex { predicate, i ->
                quickTest("es_${version}_source_predicate_${i}",
                        "select test1 from test1 where ${predicate} order by test1",
                        "select test1 from ${expected} where ${predicate} order by test1")
            }
        }

        // ======================================================================
        // ES 7 tests
        // ======================================================================
        sql """switch es7_pred_test"""
        verifySourcePredicates("7")

        // Character classes and alternation must retain SQL regular-expression semantics.
        order_qt_es7_regexp_charclass """select test1 from test1 where test1 regexp 'string[12]' order by test1"""

        // REGEXP with dot-star
        order_qt_es7_regexp_dotstar """select test1 from test1 where test1 regexp 'string.*' order by test1"""

        // REGEXP with alternation
        order_qt_es7_regexp_alternation """select test1 from test1 where test1 regexp 'string1|string3' order by test1"""

        // LIKE must include matching source values omitted by ignore_above.
        order_qt_es7_like """select test1 from test1 where test1 like 'string%' order by test1"""

        // NULL predicates must agree with the source values used by the local oracle.
        order_qt_es7_is_null """select count(*) from test1 where message is null"""
        order_qt_es7_is_not_null """select count(*) from test1 where message is not null"""
        order_qt_es7_total_count """select count(*) from test1"""

        // ======================================================================
        // ES 8 tests (same queries, verifying both versions)
        // ======================================================================
        sql """switch es8_pred_test"""
        verifySourcePredicates("8")

        order_qt_es8_regexp_charclass """select test1 from test1 where test1 regexp 'string[12]' order by test1"""
        order_qt_es8_regexp_dotstar """select test1 from test1 where test1 regexp 'string.*' order by test1"""
        order_qt_es8_regexp_alternation """select test1 from test1 where test1 regexp 'string1|string3' order by test1"""
        order_qt_es8_like """select test1 from test1 where test1 like 'string%' order by test1"""
        order_qt_es8_is_null """select count(*) from test1 where message is null"""
        order_qt_es8_is_not_null """select count(*) from test1 where message is not null"""
        order_qt_es8_total_count """select count(*) from test1"""

        // ======================================================================
        // Limit pushdown / terminate_after tests (ES 7)
        // Verifies that LIMIT queries push terminate_after to ES when all
        // predicates are pushed down, and that EXPLAIN shows the optimization.
        // ======================================================================
        sql """switch es7_pred_test"""

        // Case 1: Simple LIMIT without WHERE - all predicates trivially pushed
        // EXPLAIN should show "ES terminate_after: 5"
        explain {
            sql("select * from test1 limit 5")
            contains "ES terminate_after: 5"
        }

        // A raw keyword with ignore_above needs a residual, so it cannot terminate the ES scan early.
        explain {
            sql("select * from test1 where test1 = 'string1' limit 3")
            notContains "ES terminate_after"
        }

        // An unchanged numeric docvalue remains eligible for predicate and LIMIT pushdown.
        explain {
            sql("select test3 from test1 where test3 = 3.14 limit 3")
            contains "ES terminate_after: 3"
        }

        // Case 3: No LIMIT → no terminate_after
        explain {
            sql("select * from test1")
            notContains "ES terminate_after"
        }

        // Case 4: Verify data correctness with LIMIT
        // The result count should be <= the limit value
        def limitResult = sql """select count(*) from (select * from test1 limit 5) t"""
        assertTrue(limitResult[0][0] as int <= 5, "LIMIT 5 should return at most 5 rows")

        // ======================================================================
        // Limit pushdown / terminate_after tests (ES 8)
        // ======================================================================
        sql """switch es8_pred_test"""

        explain {
            sql("select * from test1 limit 5")
            contains "ES terminate_after: 5"
        }

        explain {
            sql("select * from test1 where test1 = 'string1' limit 3")
            notContains "ES terminate_after"
        }

        // An unchanged numeric docvalue remains eligible for predicate and LIMIT pushdown.
        explain {
            sql("select test3 from test1 where test3 = 3.14 limit 3")
            contains "ES terminate_after: 3"
        }

        explain {
            sql("select * from test1")
            notContains "ES terminate_after"
        }

        def limitResult8 = sql """select count(*) from (select * from test1 limit 5) t"""
        assertTrue(limitResult8[0][0] as int <= 5, "LIMIT 5 should return at most 5 rows")

        // cleanup
        sql """drop catalog if exists es7_pred_test;"""
        sql """drop catalog if exists es8_pred_test;"""
    }
}
