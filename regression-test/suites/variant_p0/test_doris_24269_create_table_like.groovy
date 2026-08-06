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

// DORIS-24269: creating a variant column without an explicit
// variant_sparse_hash_shard_count used to serialize the column as
// `"variant_sparse_hash_shard_count" = "0"` in SHOW CREATE TABLE,
// which was then rejected when feeding that DDL back through
// CREATE TABLE LIKE. The schema serializer now clamps the emitted
// value to >= 1 for backward compatibility; this test guards the
// round-trip.
suite("test_doris_24269_create_table_like", "p0") {
    def src = "test_doris_24269_objects"
    def dst = "test_doris_24269_objects_like"

    sql "DROP TABLE IF EXISTS ${src}"
    sql "DROP TABLE IF EXISTS ${dst}"

    sql """
        CREATE TABLE ${src} (
            `col_0` int NULL,
            `col_1` varchar(64) NULL,
            `col_2` tinyint NULL,
            `col_3` bigint NULL,
            `col_7` variant NULL,
            INDEX objects_properties_idx (`col_7`) USING INVERTED
        ) ENGINE = OLAP
        UNIQUE KEY (`col_0`, `col_1`, `col_2`, `col_3`)
        DISTRIBUTED BY HASH (`col_0`, `col_1`, `col_2`) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """

    // CREATE TABLE LIKE must succeed even though the source table left
    // variant_sparse_hash_shard_count at its default value.
    sql "CREATE TABLE ${dst} LIKE ${src}"

    def srcSchema = sql "DESC ${src}"
    def dstSchema = sql "DESC ${dst}"
    assertEquals(srcSchema.size(), dstSchema.size())

    sql "DROP TABLE IF EXISTS ${dst}"
    sql "DROP TABLE IF EXISTS ${src}"
}
