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

suite("test_local_tvf_csv_enclose_consistency", "p0,external") {
    def backends = sql "show backends"
    assertTrue(backends.size() > 0)
    def beId = backends[0][0]
    def dataPath = context.config.dataPath + "/external_table_p0/tvf"
    def filePath = "/"

    def files = [
        "csv_enclose_state.csv",
        "csv_matching_escape_enclose.csv",
        "csv_quoted_null.csv"
    ]
    for (def backend : backends) {
        for (def file : files) {
            scpFiles("root", backend[1], "${dataPath}/${file}", filePath, false)
        }
    }

    sql "set enable_file_scanner_v2 = true"

    order_qt_enclose_state """
        select id, score, extra
        from local(
            "file_path" = "${filePath}/csv_enclose_state.csv",
            "backend_id" = "${beId}",
            "format" = "csv_with_names",
            "column_separator" = ",",
            "enclose" = "\\\"",
            "escape" = "\\\\")
        order by id
    """

    order_qt_matching_escape_enclose """
        select id, name, score
        from local(
            "file_path" = "${filePath}/csv_matching_escape_enclose.csv",
            "backend_id" = "${beId}",
            "format" = "csv_with_names",
            "column_separator" = ",",
            "enclose" = "\\\"",
            "escape" = "\\\"")
        order by id
    """

    order_qt_quoted_null """
        select id, hex(name), name is null
        from local(
            "file_path" = "${filePath}/csv_quoted_null.csv",
            "backend_id" = "${beId}",
            "format" = "csv_with_names",
            "column_separator" = ",",
            "trim_double_quotes" = "true",
            "null_format" = "\\\\N")
        order by id
    """
}
