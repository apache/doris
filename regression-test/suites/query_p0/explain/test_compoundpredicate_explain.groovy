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

suite("test_compoundpredicate_explain") {
    sql "drop table if exists test_compoundpredicate_explain"
    sql """create table test_compoundpredicate_explain
         (k1 int, k2 int)
         distributed by hash(k1) buckets 3 properties('replication_num' = '1');"""

    sql """INSERT INTO test_compoundpredicate_explain (k1, k2) VALUES (500, 450), (1100, 400), (300, 600), (700, 650), (800, 800), (1500, 300);"""

    def testQueries = [
        "select * from test_compoundpredicate_explain where k1 > 500 and k2 < 700 or k1 < 3000",
        "select * from test_compoundpredicate_explain where k1 > 500 or k2 < 700 and k1 < 3000",
        "select * from test_compoundpredicate_explain where not (k1 > 500 and k2 < 700) or k1 < 3000",
        "select * from test_compoundpredicate_explain where k1 > 500 and (k2 < 700 or k1 < 3000)",
        "select * from test_compoundpredicate_explain where not (k1 > 500 or k2 < 700) and k1 < 3000",
        "select * from test_compoundpredicate_explain where (k1 > 500 and not k2 < 700) or k1 < 3000",
        "select * from test_compoundpredicate_explain where (k1 > 500 and k2 < 700) and (k1 < 3000 or k2 > 400)",
        "select * from test_compoundpredicate_explain where not (k1 > 500 or (k2 < 700 and k1 < 3000))",
        "select * from test_compoundpredicate_explain where k1 > 500 or not (k2 < 700 and k1 < 3000)",
        "select * from test_compoundpredicate_explain where k1 < 1000 and (k2 < 700 or k1 > 500) and not (k2 > 300)",
        "select * from test_compoundpredicate_explain where not ((k1 > 500 and k2 < 700) or k1 < 3000)",
        "select * from test_compoundpredicate_explain where k1 > 500 and not (k2 < 700 or k1 < 3000)",
        "select * from test_compoundpredicate_explain where (k1 > 500 or k2 < 700) and (k1 < 3000 and k2 > 200)",
        "select * from test_compoundpredicate_explain where (k1 > 500 and k2 < 700) or not (k1 < 3000 and k2 > 200)"
    ]

    testQueries.each { query ->
        def explainResult1 = sql "explain all plan ${query}"
        def explainResult2 = sql "explain ${query}"

        def predicates2Line = explainResult2.find { line ->
            line[0].toString().trim().startsWith("PREDICATES:")
        }

        if (predicates2Line != null) {
            def predicates2 = predicates2Line[0].split("PREDICATES:").last().trim()

            predicates2 = predicates2?.replaceAll(/\[\#(\d+)\]/) { match, group1 -> "#" + group1 }

            def isMatch = explainResult1.any { line ->
                line.toString().contains(predicates2)
            }

            log.info("Testing query: " + query)
            log.info("Standardized Predicates from PREDICATES: " + predicates2)
            log.info("Match found in OPTIMIZED PLAN: " + isMatch)

            assert isMatch : "Predicates are not equal for query: ${query}"
        } else {
            logger.error("PREDICATES: not found in explain result for query: ${query}")
            assert false : "PREDICATES: not found in explain result"
        }
    }

    sql "drop table if exists test_compoundpredicate_explain"
}