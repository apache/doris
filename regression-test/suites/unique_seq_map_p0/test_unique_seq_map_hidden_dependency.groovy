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

suite("test_unique_seq_map_hidden_dependency") {
    sql "DROP TABLE IF EXISTS test_unique_seq_map_hidden_dependency"
    sql """
        CREATE TABLE test_unique_seq_map_hidden_dependency (
            k BIGINT NOT NULL,
            v1 INT NULL,
            v2 INT NULL,
            s1 BIGINT NULL,
            s2 BIGINT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "false",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "true",
            "sequence_mapping.s1" = "v1",
            "sequence_mapping.s2" = "v2"
        )
    """

    // Keep each write in a separate rowset. For key 1, v1 comes from the
    // second rowset while v2 comes from the third. For key 2 the sources are
    // reversed, so neither query can accidentally use the newest whole row.
    sql "INSERT INTO test_unique_seq_map_hidden_dependency VALUES (1, 10, 100, 10, 10), (2, 50, 500, 50, 50)"
    sql "INSERT INTO test_unique_seq_map_hidden_dependency VALUES (1, 20, 200, 20, 5), (2, 60, 600, 40, 60)"
    sql "INSERT INTO test_unique_seq_map_hidden_dependency VALUES (1, 30, 300, 15, 30), (2, 70, 700, 70, 55)"
    sql "sync"

    // s1 and the key are internal merge dependencies for this projection.
    order_qt_hidden_s1 """
        SELECT v1
        FROM test_unique_seq_map_hidden_dependency
        ORDER BY v1
    """

    // s2 and the key are internal merge dependencies for this projection.
    order_qt_hidden_s2 """
        SELECT v2
        FROM test_unique_seq_map_hidden_dependency
        ORDER BY v2
    """

    sql "SET topn_lazy_materialization_threshold = 1024"
    def topnByValueQuery = """
        SELECT v1, v2
        FROM test_unique_seq_map_hidden_dependency
        ORDER BY v1 DESC
        LIMIT 1
    """
    explain {
        sql "SHAPE PLAN ${topnByValueQuery}"
        contains("PhysicalTopN")
        notContains("PhysicalLazyMaterialize")
    }
    order_qt_topn_sequence_map_by_value topnByValueQuery

    def topnByKeyQuery = """
        SELECT k, v1, v2
        FROM test_unique_seq_map_hidden_dependency
        ORDER BY k DESC
        LIMIT 1
    """
    explain {
        sql "SHAPE PLAN ${topnByKeyQuery}"
        contains("PhysicalTopN")
        notContains("PhysicalLazyMaterialize")
    }
    order_qt_topn_sequence_map_by_key topnByKeyQuery
}
