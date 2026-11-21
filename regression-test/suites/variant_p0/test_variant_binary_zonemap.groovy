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

suite("test_variant_binary_zonemap", "p0,nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_variant_binary_zonemap"
    sql """
        CREATE TABLE test_variant_binary_zonemap (
            k INT,
            v VARIANT<PROPERTIES("variant_max_subcolumns_count" = "10")>
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1", "disable_auto_compaction" = "true",
                   "storage_page_size" = "4096")
    """

    // Each insert creates a segment with scalar a, object a, missing a and SQL NULL runs.
    // Keep enough rows to exercise valid physical integer page bounds, including object-only pages.
    [false, true].eachWithIndex { binary, batch ->
        setBeConfigTemporary([enable_binary_zone_map: binary.toString()]) {
            sql """
                INSERT INTO test_variant_binary_zonemap
                SELECT number + ${batch * 32768},
                       CASE WHEN number < 16384
                            THEN parse_to_variant(concat('{"a":', cast(number + 100000 as string), '}'))
                            WHEN number < 24576
                            THEN parse_to_variant(concat('{"a":{"b":', cast(number + 100000 as string), '}}'))
                            WHEN number < 28672 THEN parse_to_variant('{}')
                            ELSE NULL END
                FROM numbers("number" = "32768")
                ORDER BY number
            """
        }
    }

    // Both formats must remain readable after binary writes are disabled. Disable expression
    // pruning first to generate the reference results through ordinary row evaluation.
    setBeConfigTemporary([enable_binary_zone_map: "false"]) {
        [false, true].each { pruning ->
            sql "set enable_expr_zonemap_filter = ${pruning}"
            def prefix = pruning ? "enabled" : "disabled"
            def predicates = [
                "is_null": "v['a'] IS NULL",
                "is_not_null": "v['a'] IS NOT NULL",
                "or_null": "v['a'] IS NULL OR k = -1",
                "or_not_null": "v['a'] IS NOT NULL OR k = -1",
                "and_object": "v['a'] IS NOT NULL AND k % 32768 >= 16384",
                "leaf_not_null": "v['a']['b'] IS NOT NULL",
                "leaf_range": "cast(v['a']['b'] AS INT) >= 120000"
            ]
            predicates.each { name, predicate ->
                quickTest("${prefix}_${name}", """
                    SELECT k DIV 32768 AS batch, count(*), min(k), max(k), sum(k)
                    FROM test_variant_binary_zonemap
                    WHERE ${predicate}
                    GROUP BY batch ORDER BY batch
                """)
            }
        }
    }
}
