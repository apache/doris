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

suite("ann_range_search_source_index_status_regression", "nonConcurrent") {
    sql "unset variable all;"
    sql "set enable_common_expr_pushdown=true;"
    sql "set experimental_enable_virtual_slot_for_cse=true;"
    sql "set enable_no_need_read_data_opt=true;"
    sql "set parallel_pipeline_task_num=1;"
    sql "set enable_sql_cache=false;"
    sql "set enable_condition_cache=false;"

    // The source column's index in the scan block differs from its storage
    // ColumnId. ANN range search should mark the common expression status by
    // scan-block source column index, so the embedding column can be skipped
    // when it is only used by the satisfied ANN predicate.
    sql "drop table if exists ann_range_source_index_status"
    sql """
        create table ann_range_source_index_status (
            id int not null,
            pad_int int not null,
            pad_text string not null,
            embedding array<float> not null,
            value int not null,
            index idx_embedding(`embedding`) using ann properties(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="3"
            )
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num"="1");
    """

    sql """
        insert into ann_range_source_index_status values
        (1, 10, 'a', [0.0, 0.0, 0.0], 100),
        (2, 20, 'b', [0.1, 0.0, 0.0], 200),
        (3, 30, 'c', [0.2, 0.0, 0.0], 300),
        (4, 40, 'd', [0.3, 0.0, 0.0], 400),
        (5, 50, 'e', [0.4, 0.0, 0.0], 500),
        (6, 60, 'f', [0.5, 0.0, 0.0], 600),
        (7, 70, 'g', [0.6, 0.0, 0.0], 700),
        (8, 80, 'h', [0.7, 0.0, 0.0], 800),
        (9, 90, 'i', [0.8, 0.0, 0.0], 900),
        (10, 100, 'j', [0.9, 0.0, 0.0], 1000);
    """

    try {
        GetDebugPoint().enableDebugPointForAllBEs(
                "segment_iterator._read_columns_by_index", [column_name: "embedding"])
        def indexOnlyRows = sql """
            select id
            from ann_range_source_index_status
            where l2_distance_approximate(embedding, [0.0, 0.0, 0.0]) < 1.0
            order by id;
        """
        assertEquals([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], indexOnlyRows.collect { it[0] })
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("segment_iterator._read_columns_by_index")
    }

    def readEmbeddingRows = sql """
        select id, embedding
        from ann_range_source_index_status
        where l2_distance_approximate(embedding, [0.0, 0.0, 0.0]) < 1.0
        order by id;
    """
    assertEquals([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], readEmbeddingRows.collect { it[0] })
}
