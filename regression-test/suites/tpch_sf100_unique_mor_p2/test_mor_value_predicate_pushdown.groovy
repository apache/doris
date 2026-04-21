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

// This test runs all 22 TPC-H queries on MOR (Merge-On-Read) unique key tables
// with enable_mor_value_predicate_pushdown_tables enabled, and compares the
// results against standard MOR queries (pushdown disabled).
// On insert-only data (no updates/deletes), standard MOR merge produces the
// same results as MOW. Enabling value predicate pushdown should not change
// query results — this validates correctness at scale against TPC-H SF10.

suite("test_mor_value_predicate_pushdown") {
    sql "SET query_timeout = 1800"

    def sqlDir = new File("${context.file.parent}/sql")
    def queryFiles = sqlDir.listFiles().findAll { it.name.endsWith('.sql') }.sort { it.name }
    for (def queryFile : queryFiles) {
        def queryName = queryFile.name.replace('.sql', '')
        def querySql = queryFile.text

        // Run on MOR tables without pushdown (standard merge = same as MOW)
        sql "SET enable_mor_value_predicate_pushdown_tables = ''"
        def mowResult = sql querySql

        // Run on MOR tables with pushdown enabled
        sql "SET enable_mor_value_predicate_pushdown_tables = '*'"
        def pushdownResult = sql querySql

        // Compare — pushdown should produce identical results to standard MOR/MOW
        logger.info("Query ${queryName}: MOW rows=${mowResult.size()}, pushdown rows=${pushdownResult.size()}")
        assertEquals(mowResult.size(), pushdownResult.size(),
                "Query ${queryName}: row count mismatch, MOW=${mowResult.size()}, pushdown=${pushdownResult.size()}")
        assertEquals(mowResult, pushdownResult,
                "Query ${queryName}: MOR pushdown result differs from standard MOW result")
    }
}
