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

suite("test_query_json_set_jsonb_nullable_value", "query") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    def tableName = "test_query_json_set_jsonb_nullable_value"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
            CREATE TABLE ${tableName} (
              `id` int not null,
              `j` string null,
              `v` int null
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
        """

    def rows = (1..5000).collect { i ->
        "(${i}, '{}', ${i % 2 == 1 ? 1 : "NULL"})"
    }.join(",")
    sql "INSERT INTO ${tableName} VALUES ${rows}"

    order_qt_jsonb_nullable_value """
        SELECT id, json_set(j, '\$.a',
                            CAST(CASE WHEN v > 0 THEN '"分层2-近期转化"' END AS JSONB))
        FROM ${tableName}
        WHERE id <= 2
        ORDER BY id
    """

    qt_invalid_jsonb_case_value """
        SELECT COUNT(*)
        FROM (
            SELECT json_set(j, '\$.a',
                            CASE WHEN id % 2 = 1
                                 THEN CAST('分层2-近期转化' AS JSONB)
                            END) AS x
            FROM ${tableName}
        ) t
        WHERE x IS NOT NULL
    """
}
