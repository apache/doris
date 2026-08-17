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

suite("ivf_pq_full_buffer_train_recall", "nonConcurrent") {
    sql "set enable_common_expr_pushdown=true;"
    sql "set ivf_nprobe=8;"

    sql "drop table if exists tbl_ivf_pq_full_buffer_train_recall"
    sql """
    CREATE TABLE tbl_ivf_pq_full_buffer_train_recall (
        id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="ivf",
                "metric_type"="l2_distance",
                "nlist"="8",
                "dim"="4",
                "quantizer"="pq",
                "pq_m"="2",
                "pq_nbits"="1"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num" = "1");
    """

    def insertData = []
    for (int i = 1; i <= 400; i++) {
        if (i == 250) {
            insertData.add("(${i}, [0.0, 0.0, 0.0, 0.0])")
        } else if (i <= 200) {
            insertData.add("(${i}, [1000.0, ${i}.0, ${(i % 17)}.0, ${(i % 19)}.0])")
        } else {
            insertData.add(
                    "(${i}, [${(i - 250) / 50.0}, ${(250 - i) / 50.0}, "
                            + "${(i % 7 - 3) / 10.0}, ${(i % 5 - 2) / 10.0}])")
        }
    }
    sql "INSERT INTO tbl_ivf_pq_full_buffer_train_recall VALUES ${insertData.join(', ')};"
    sql "sync"

    qt_target_in_top20 """
        select count(*)
        from (
            select id
            from tbl_ivf_pq_full_buffer_train_recall
            order by l2_distance_approximate(embedding, [0.0, 0.0, 0.0, 0.0]), id
            limit 20
        ) t
        where id = 250;
    """
}
