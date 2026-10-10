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

suite("test_file_type_spill") {
    def publicStruct = "STRUCT<uri:VARCHAR(65533),offset:BIGINT,size:BIGINT," +
            "content_type:VARCHAR(1024),checksum:VARCHAR(1024),inline:VARBINARY>"
    sql "DROP TABLE IF EXISTS test_file_type_spill_values"
    sql """
        CREATE TABLE test_file_type_spill_values (
            id INT NOT NULL, category INT NOT NULL, f FILE, files ARRAY<FILE>)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES("replication_num"="1")
    """
    sql """
        INSERT INTO test_file_type_spill_values
        SELECT id, id % 4, f, CAST(ARRAY(CAST(f AS ${publicStruct}), NULL) AS ARRAY<FILE>) FROM (
            SELECT CAST(number AS INT) AS id,
                   CAST(NAMED_STRUCT('uri', CONCAT('urn:spill:', CAST(number AS STRING)), 'size', number, 'checksum', 'ETAG:spill', 'offset', NULL, 'content_type', NULL, 'inline', NULL) AS FILE) AS f
            FROM NUMBERS("number"="512")) source
    """
    sql "SET enable_spill = true"
    sql "SET enable_force_spill = true"
    sql "SET spill_min_revocable_mem = 1"
    sql "SET force_sort_algorithm = 'full'"
    sql "SET topn_lazy_materialization_threshold = -1"

    // Project complete FILE values so sort and shuffle retain the value payload.
    qt_sort """
        SELECT id, f, files FROM test_file_type_spill_values
        ORDER BY id DESC LIMIT 8 OFFSET 500
    """
    qt_join """
        SELECT a.id, a.f, b.files FROM test_file_type_spill_values a
        JOIN [shuffle] test_file_type_spill_values b ON a.id = b.id
        ORDER BY a.id LIMIT 8 OFFSET 500
    """
    qt_aggregate """
        SELECT category, COUNT(f), MIN(id), MAX(id)
        FROM test_file_type_spill_values GROUP BY category ORDER BY category
    """
    qt_window_payload """
        SELECT id, f, files, ROW_NUMBER() OVER (PARTITION BY category ORDER BY id)
        FROM test_file_type_spill_values WHERE id >= 504 ORDER BY id
    """
    for (def expression in ["FIRST_VALUE(f)", "LAST_VALUE(f)", "NTH_VALUE(f, 1)",
                            "LAG(f)", "LEAD(f)"]) {
        test {
            sql "SELECT ${expression} OVER (PARTITION BY category ORDER BY id) FROM test_file_type_spill_values"
            exception "FILE"
        }
    }
}
