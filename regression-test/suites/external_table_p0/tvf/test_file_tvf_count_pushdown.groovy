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

suite("test_file_tvf_count_pushdown", "p0,external") {
    def backends = sql "show backends"
    def backendId = backends[0][0]
    def dataPath = context.config.dataPath + "/external_table_p0/tvf"
    def predeployedPath = context.config.otherConfigs.get("fileTvfCountFixturePath")
    def remotePath = predeployedPath ?: "/tmp/file_tvf_count_pushdown"
    def fixtures = ["tvf_count_header.csv"]
    for (def format : ["csv", "json", "parquet", "orc"]) {
        fixtures.add("tvf_count_data_rows.${format}")
        fixtures.add("tvf_count_data_empty.${format}")
        fixtures.add("tvf_count_data_more.${format}")
    }
    if (predeployedPath == null) {
        mkdirRemotePathOnAllBE("root", remotePath)
        for (def backend : backends) {
            for (def fixture : fixtures) {
                scpFiles("root", backend[1], "${dataPath}/${fixture}", remotePath, false)
            }
        }
    }

    // The same snapshots exercise both the record-reading and row-count paths.
    // Each nonempty fixture contains four rows, with one NULL and a duplicate value.
    for (def scannerV2 : [false, true]) {
        sql "set enable_file_scanner_v2 = ${scannerV2}"
        for (def pushdown : [false, true]) {
            sql "set enable_push_down_no_group_agg = ${pushdown}"
            def mode = "v2_${scannerV2}_push_${pushdown}"
            for (def format : ["csv", "json", "parquet", "orc"]) {
                def options = ""
                if (format == "csv") {
                    options = ', "column_separator" = ",", "csv_schema" = "id:int;value:int"'
                } else if (format == "json") {
                    options = ', "read_json_by_line" = "true"'
                }
                def table = """local("file_path" = "${remotePath}/tvf_count_data_rows.${format}",
                    "backend_id" = "${backendId}", "format" = "${format}" ${options})"""
                explain {
                    sql "SELECT COUNT(*) FROM ${table}"
                    contains "pushdown agg=${pushdown ? 'COUNT' : 'NONE'}"
                }
                for (def query : ["SELECT COUNT(value) FROM ${table}",
                        "SELECT COUNT(DISTINCT value) FROM ${table}",
                        "SELECT COUNT(*) FROM ${table} WHERE id > 2",
                        "SELECT value, COUNT(*) FROM ${table} GROUP BY value"]) {
                    explain {
                        sql query
                        contains "pushdown agg=NONE"
                    }
                }
                quickTest("${mode}_${format}_star", "SELECT COUNT(*) FROM ${table}")
                quickTest("${mode}_${format}_one", "SELECT COUNT(1) FROM ${table}")
                quickTest("${mode}_${format}_nullable", "SELECT COUNT(value) FROM ${table}")
                quickTest("${mode}_${format}_distinct", "SELECT COUNT(DISTINCT value) FROM ${table}")
                quickTest("${mode}_${format}_filter", "SELECT COUNT(*) FROM ${table} WHERE id > 2")
                quickTest("${mode}_${format}_group", "SELECT value, COUNT(*) FROM ${table} GROUP BY value", true)

                if (format == "parquet" || format == "orc") {
                    // Retained assertions must see file values, not the COUNT reader's synthetic columns.
                    def assertedCount = """SELECT COUNT(*) FROM (
                        SELECT assert_true(id > 0, 'positive id') AS checked FROM ${table}) t"""
                    explain {
                        sql assertedCount
                        contains "pushdown agg=NONE"
                    }
                    test {
                        sql assertedCount
                    }
                    test {
                        sql """SELECT COUNT(*) FROM (
                            SELECT assert_true(id = 0, 'zero id required') AS checked FROM ${table}) t"""
                        exception "zero id required"
                    }
                }

                // CSV has an explicit schema; empty Parquet/ORC files contain schema metadata.
                // A zero-byte JSON file has no schema for standalone TVF inference.
                if (format != "json") {
                    def emptyTable = table.replace("tvf_count_data_rows", "tvf_count_data_empty")
                    quickTest("${mode}_${format}_empty", "SELECT COUNT(*) FROM ${emptyTable}")
                }
                // Two nonempty files and one empty file exercise multiple physical ranges.
                def multipleTable = table.replace("tvf_count_data_rows", "tvf_count_data_*")
                quickTest("${mode}_${format}_multiple", "SELECT COUNT(*) FROM ${multipleTable}")
            }
            def headerTable = """local("file_path" = "${remotePath}/tvf_count_header.csv",
                "backend_id" = "${backendId}", "format" = "csv_with_names",
                "column_separator" = ",", "csv_schema" = "id:int;value:int")"""
            // Header, empty record and final record without a newline use normal CSV semantics.
            quickTest("${mode}_csv_header", "SELECT COUNT(*) FROM ${headerTable}")
            quickTest("${mode}_csv_header_nullable", "SELECT COUNT(value) FROM ${headerTable}")
        }
    }
}
