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

suite("ivf_pq_recall", "nonConcurrent") {
    sql "set enable_common_expr_pushdown=true;"
    sql "set enable_ann_index_result_cache=false;"
    sql "set ivf_nprobe=8;"

    setBeConfigTemporary([ann_index_build_chunk_size: 400]) {
        sql "drop table if exists ivf_pq_recall"
        sql """
            create table ivf_pq_recall (
                id int not null,
                embedding array<float> not null,
                index idx_embedding (`embedding`) using ann properties(
                    "index_type" = "ivf",
                    "metric_type" = "l2_distance",
                    "nlist" = "8",
                    "dim" = "4",
                    "quantizer" = "pq",
                    "pq_m" = "2",
                    "pq_nbits" = "2"
                )
            ) engine=olap
            duplicate key(id)
            distributed by hash(id) buckets 1
            properties(
                "replication_num" = "1",
                "disable_auto_compaction" = "true"
            );
        """

        def formatFloat = { double value ->
            String.format(java.util.Locale.ROOT, "%.3f", value)
        }
        def vector = { double x ->
            "[${formatFloat(x)}, ${formatFloat(x * 2)}, ${formatFloat(x * 3)}, ${formatFloat(x * 4)}]"
        }
        def rows = []
        for (int i = 1; i <= 400; i++) {
            double x = (i - 1) / 1000.0
            rows.add("(${i}, ${vector(x)})")
        }
        for (int i = 401; i <= 800; i++) {
            double x = 1000.0 + (i - 401) / 1000.0
            rows.add("(${i}, ${vector(x)})")
        }
        sql "insert into ivf_pq_recall values ${rows.join(',')};"
        sql "sync"

        qt_row_count "select count(*) from ivf_pq_recall;"

        qt_first_cluster_recall """
            select count(*) from (
                select id
                from ivf_pq_recall
                order by l2_distance_approximate(embedding, [0.0, 0.0, 0.0, 0.0])
                limit 20
            ) t
            where id between 1 and 400;
        """

        qt_second_cluster_recall """
            select count(*) from (
                select id
                from ivf_pq_recall
                order by l2_distance_approximate(embedding, [1000.0, 2000.0, 3000.0, 4000.0])
                limit 20
            ) t
            where id between 401 and 800;
        """
    }
}
