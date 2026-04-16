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
// with read_mor_as_dup_tables enabled, and compares the results against actual
// DUPLICATE KEY tables loaded with the same data (loaded twice).
// MOR tables with read_mor_as_dup should produce identical results to DUP tables
// since both skip merge and expose all row versions.

suite("test_read_mor_as_dup") {
    sql "SET query_timeout = 1800"

    // Table name mapping: replace longest names first to avoid partial matches
    // (e.g. "partsupp" must be replaced before "part")
    def tableNames = ["partsupp", "lineitem", "customer", "supplier", "orders", "nation", "region", "part"]

    def sqlDir = new File("${context.file.parent}/sql")
    def queryFiles = sqlDir.listFiles().findAll { it.name.endsWith('.sql') }.sort { it.name }
    for (def queryFile : queryFiles) {
        def queryName = queryFile.name.replace('.sql', '')
        def querySql = queryFile.text

        // Run on MOR tables with read_mor_as_dup_tables enabled
        sql "SET read_mor_as_dup_tables = '*'"
        def morResult = sql querySql

        // Run the same query on actual DUP tables (replace table names with _dup suffix)
        sql "SET read_mor_as_dup_tables = ''"
        def dupSql = querySql
        for (def tbl : tableNames) {
            dupSql = dupSql.replaceAll("\\b${tbl}\\b", "${tbl}_dup")
        }
        def dupResult = sql dupSql

        // Compare results — MOR-as-DUP should produce identical results to actual DUP tables
        logger.info("Query ${queryName}: MOR-as-DUP rows=${morResult.size()}, DUP rows=${dupResult.size()}")
        assertEquals(dupResult.size(), morResult.size(),
                "Query ${queryName}: row count mismatch, MOR-as-DUP=${morResult.size()}, DUP=${dupResult.size()}")
        assertEquals(dupResult, morResult,
                "Query ${queryName}: MOR-as-DUP result differs from actual DUP table result")
    }
}
