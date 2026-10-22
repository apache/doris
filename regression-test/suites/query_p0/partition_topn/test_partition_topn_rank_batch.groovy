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

suite("test_partition_topn_rank_batch") {
    sql "set batch_size = 4"
    sql "set parallel_pipeline_task_num = 1"
    sql "set enable_partition_topn = true"

    sql "drop table if exists test_partition_topn_rank_batch"
    sql """
        create table test_partition_topn_rank_batch (id int, p int, k int)
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_partition_topn_rank_batch values
        (1,0,0),(2,0,0),(3,0,0),(4,0,0),(5,0,0),(6,0,1),
        (7,1,0),(8,1,0),(9,1,0),(10,1,0),(11,1,0),
        (12,1,0),(13,1,0),(14,1,0),(15,1,0),(16,1,1)
    """

    for (func in ["rank", "dense_rank"]) {
        def globalQuery = """
            select id, k, r from (
                select id, k, ${func}() over(order by k) r
                from test_partition_topn_rank_batch where p = 0
            ) t where r <= 1 order by id
        """
        explain {
            sql globalQuery
            contains "VPartitionTopN"
        }
        "qt_${func}_global"(globalQuery)
        "qt_${func}_partitioned"("""
            select id, p, k, r from (
                select id, p, k, ${func}() over(partition by p order by k) r
                from test_partition_topn_rank_batch
            ) t where r <= 1 order by id
        """)
        "qt_${func}_second_rank"("""
            select id, p, k, r from (
                select id, p, k, ${func}() over(partition by p order by k) r
                from test_partition_topn_rank_batch
            ) t where r <= 2 order by id
        """)
    }

    qt_row_number """
        select p, k, r from (
            select p, k, row_number() over(partition by p order by k) r
            from test_partition_topn_rank_batch
        ) t where r <= 1 order by p
    """

    // Keep more than 20,000 peers before the final input batch to exercise intermediate pruning.
    sql "set batch_size = 1000"
    sql """
        insert into test_partition_topn_rank_batch
        select number + 100, 2, if(number < 21005, 0, 1) from numbers("number" = "21006")
    """
    for (func in ["rank", "dense_rank"]) {
        "qt_${func}_intermediate_pruning"("""
            select p, count(*) from (
                select p, ${func}() over(partition by p order by k) r
                from test_partition_topn_rank_batch
            ) t where r <= 1 group by p order by p
        """)
    }

    // A two-row boundary group also spans batches when three rows precede it.
    sql "set batch_size = 4"
    sql """
        insert into test_partition_topn_rank_batch values
        (30000,3,0),(30001,3,0),(30002,3,0),(30003,3,1),(30004,3,1),(30005,3,2)
    """
    for (func in ["rank", "dense_rank"]) {
        def rankLimit = func == "rank" ? 4 : 2
        def shortGroupQuery = """
            select id, k, r from (
                select id, k, ${func}() over(order by k) r
                from test_partition_topn_rank_batch where p = 3
            ) t where r <= ${rankLimit} order by id
        """
        explain {
            sql shortGroupQuery
            contains "VPartitionTopN"
        }
        "qt_${func}_short_boundary_group"(shortGroupQuery)
    }
}
