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

suite("ann_index_build_min_segment_rows", "nonConcurrent") {
    sql "unset variable all;"
    sql "set enable_common_expr_pushdown=true;"
    sql "set experimental_enable_virtual_slot_for_cse=true;"
    sql "set enable_no_need_read_data_opt=true;"
    sql "set parallel_pipeline_task_num=1;"
    sql "set enable_sql_cache=false;"
    sql "set enable_condition_cache=false;"

    setBeConfigTemporary([ann_index_build_min_segment_rows: 100]) {
        sql "drop table if exists ann_index_build_min_segment_rows"
        sql """
            create table ann_index_build_min_segment_rows (
                id int not null,
                embedding array<float> not null,
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
            insert into ann_index_build_min_segment_rows values
            (1, [0.0, 0.0, 0.0]),
            (2, [0.1, 0.0, 0.0]),
            (3, [0.2, 0.0, 0.0]);
        """

        try {
            GetDebugPoint().enableDebugPointForAllBEs(
                    "segment_iterator._read_columns_by_index", [column_name: "embedding"])
            test {
                sql """
                    select id
                    from ann_index_build_min_segment_rows
                    where l2_distance_approximate(embedding, [0.0, 0.0, 0.0]) < 1.0
                    order by id;
                """
                exception "does not need to read data"
            }
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("segment_iterator._read_columns_by_index")
        }
    }
}
