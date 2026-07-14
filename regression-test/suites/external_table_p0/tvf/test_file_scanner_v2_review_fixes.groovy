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

suite("test_file_scanner_v2_review_fixes", "p0,external") {
    def backends = sql "show backends"
    assertTrue(backends.size() > 0)
    def backendId = backends[0][0]
    def dataPath = context.config.dataPath + "/external_table_p0/tvf"
    // A developer may point this at fixtures already visible to every BE (for example, a shared
    // checkout mounted at /tmp). CI leaves it unset and exercises the normal per-BE distribution.
    def predeployedFixturePath = context.config.otherConfigs.get("fileScannerV2FixturePath")
    def remotePath = predeployedFixturePath ?: "/tmp/file_scanner_v2_review_fixes"
    def fixtureNames = [
        "file_scanner_v2_empty.csv",
        "file_scanner_v2_struct_0_bigint.parquet",
        "file_scanner_v2_struct_1_int.parquet",
        "file_scanner_v2_unsupported_time.parquet"
    ]

    if (predeployedFixturePath == null) {
        mkdirRemotePathOnAllBE("root", remotePath)
        for (def backend : backends) {
            for (def fixtureName : fixtureNames) {
                scpFiles("root", backend[1], "${dataPath}/${fixtureName}", remotePath, false)
            }
        }
    }

    sql "set enable_file_scanner_v2 = true"

    // A zero-byte CSV is a valid zero-row split when the schema is supplied explicitly. The EOF
    // raised during reader preparation must skip this split instead of failing the query.
    order_qt_empty_csv """
        SELECT id, name
        FROM local(
            "file_path" = "${remotePath}/file_scanner_v2_empty.csv",
            "backend_id" = "${backendId}",
            "format" = "csv",
            "csv_schema" = "id:int;name:string")
        ORDER BY id
    """

    // The first file defines STRUCT<a: BIGINT>, while the second stores a as INT. The predicate is
    // localized independently for each split, so the INT leaf must be cast back to BIGINT before
    // comparison. Repeating the multi-file read also exercises per-reader page-cache range state:
    // closing one file must not leak its range directory into the next file or the warm scan.
    def evolvedStructQuery = """
        SELECT id, col.a, col.b
        FROM local(
            "file_path" = "${remotePath}/file_scanner_v2_struct_*.parquet",
            "backend_id" = "${backendId}",
            "format" = "parquet")
        WHERE col.a = CAST(10 AS BIGINT)
        ORDER BY id
    """
    order_qt_struct_leaf_promotion_cold evolvedStructQuery
    order_qt_struct_leaf_promotion_warm evolvedStructQuery

    // COUNT(col) sees a non-trivial mapping for the file whose STRUCT leaf is INT while the merged
    // table type is BIGINT. It must fall back to the normal scan so schema-evolution casts and
    // nullability checks run instead of counting footer values directly.
    order_qt_count_evolved_struct """
        SELECT COUNT(col.a)
        FROM local(
            "file_path" = "${remotePath}/file_scanner_v2_struct_*.parquet",
            "backend_id" = "${backendId}",
            "format" = "parquet")
    """

    // TIME_MILLIS is intentionally unsupported by the record reader. It must not prevent schema
    // construction when only the supported id column is projected.
    order_qt_unprojected_unsupported_time """
        SELECT id
        FROM local(
            "file_path" = "${remotePath}/file_scanner_v2_unsupported_time.parquet",
            "backend_id" = "${backendId}",
            "format" = "parquet")
        ORDER BY id
    """

    // A requested unsupported leaf must fail before metadata pruning. The fixture stores positive
    // TIME_MILLIS values, so `unsupported_time < 0` can be eliminated from INT32 row-group
    // statistics. Without the early validation, this query incorrectly returns an empty result
    // instead of preserving the unsupported logical-type contract.
    test {
        sql """
            SELECT unsupported_time
            FROM local(
                "file_path" = "${remotePath}/file_scanner_v2_unsupported_time.parquet",
                "backend_id" = "${backendId}",
                "format" = "parquet")
            WHERE unsupported_time < 0
        """
        exception "Parquet TIME with isAdjustedToUTC=true is not supported"
    }

}
