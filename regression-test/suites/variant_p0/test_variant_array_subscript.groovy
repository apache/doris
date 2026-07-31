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

suite("test_variant_array_subscript", "p0") {
    sql "set enable_nereids_planner = true"
    sql "set enable_fallback_to_original_planner = false"
    sql "set default_variant_enable_nested_group = false"
    sql "set default_variant_max_subcolumns_count = 100"

    sql "DROP TABLE IF EXISTS test_variant_array_subscript"
    sql """
        CREATE TABLE test_variant_array_subscript (
            id BIGINT,
            v VARIANT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        )
    """

    sql """
        INSERT INTO test_variant_array_subscript VALUES
        (1, '{"items":{"type":["e2e_QC","platform_QC"]}}')
    """
    sql "sync"

    explain {
        verbose true
        sql """
            SELECT CAST(v['items']['type'] AS ARRAY<STRING>)[1]
            FROM test_variant_array_subscript
        """
        contains "subColPath=[items, type]"
        contains "element_at(CAST(v"
        notContains "element_at(CAST(element_at(element_at("
    }
}
