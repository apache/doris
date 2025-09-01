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

suite("index_scan_opt") {
    sql "drop table if exists index_scan_opt"
    sql "drop table if exists index_scan_opt_index"

    sql """create table index_scan_opt (
            k1 int,
            embedding ARRAY<FLOAT> NOT NULL COMMENT ""
            )
        distributed by hash(k1)
        buckets 1
        properties('replication_num' = '1');"""

    sql """
        create table index_scan_opt_index (
            k1 int,
            embedding ARRAY<FLOAT> NOT NULL COMMENT "",
            INDEX ann_idx (embedding) USING ANN PROPERTIES(
                    "index_type" = "hnsw",
                    "metric_type" = "l2_distance",
                    "dim" = "10"
                ))
        distributed by hash(k1)
        buckets 1
        properties('replication_num' = '1');"""

    sql """
        insert into index_scan_opt values (1, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]),
                                             (2, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]),
                                             (3, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]),
                                             (4, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]),
                                             (5, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]),
                                             (6, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]),
                                             (7, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]),
                                             (8, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]),
                                             (9, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]),
                                             (10, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]);
    """
    sql "insert into index_scan_opt_index select * from index_scan_opt;"

    sql "set optimize_index_scan_parallelism=true;"

    qt_opt_index_scan """
        select k1 from index_scan_opt_index 
            order by l2_distance_approximate(embedding, [1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]) 
            limit 2;
    """
}