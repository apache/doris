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

suite("test_variant_desc_completeness", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }
    sql "DROP TABLE IF EXISTS variant_desc_completeness"
    sql """CREATE TABLE variant_desc_completeness (id INT, payload VARIANT<PROPERTIES(
            "variant_enable_doc_mode" = "false")>)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true",
                    "storage_format" = "V3")"""
    sql """INSERT INTO variant_desc_completeness VALUES (1, PARSE_TO_VARIANT('{"expected_key":10}'))"""
    sql "SET describe_extend_variant_column = true"
    def tablet = sql_return_maparray("SHOW TABLETS FROM variant_desc_completeness")[0].TabletId
    def point = "LocalFileReader::read_at_impl.io_error"
    GetDebugPoint().enableDebugPointForAllBEs(point, [sub_path: "/${tablet}/", timeout: "60"])
    try {
        test {
            sql "DESC variant_desc_completeness"
            exception "IO_ERROR"
        }
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(point)
    }
    def columns = sql("DESC variant_desc_completeness").collect { it[0] }
    assertTrue(columns.contains("payload.expected_key"), "Recovered DESC returned ${columns}")
    sql "SET max_fetch_remote_schema_tablet_count = 0"
    test {
        sql "DESC variant_desc_completeness"
        exception "exceeding max_fetch_remote_schema_tablet_count"
    }
}
