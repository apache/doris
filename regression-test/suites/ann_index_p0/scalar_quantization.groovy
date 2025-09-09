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

suite("scalar_quantization") {
    sql "drop table if exists scalar_quantization"
    sql "create table scalar_quantization (
            id int not null,
            vec array<float> not null,
            index ann_idx (vec)
                using ann properties ('index_type'='hnsw', 'metric_type'='l2_distance', 'dim'='3', 'quantizer'='sq4'))
            engine=olap
            duplicate key(id)
            distributed by hash(id) buckets 1
            properties('replication_num' = '1');"
    sql "insert into scalar_quantization values (1, [1.0, 2.0, 3.0])"
    qt_sql "select * from scalar_quantization order by id"

    sql "drop table if exists scalar_quantization"
    sql "create table scalar_quantization (
            id int not null,
            vec array<float> not null,
            index ann_idx (vec)
                using ann properties ('index_type'='hnsw', 'metric_type'='l2_distance', 'dim'='3', 'quantizer'='sq8'))
            engine=olap
            duplicate key(id)
            distributed by hash(id) buckets 1
            properties('replication_num' = '1');"
    sql "insert into scalar_quantization values (1, [1.0, 2.0, 3.0])"
    qt_sql "select * from scalar_quantization order by id"

    sql "drop table if exists scalar_quantization"
    test {
        sql "create table scalar_quantization (
            id int not null,
            vec array<float> not null,
            index ann_idx (vec)
                using ann properties ('index_type'='hnsw', 'metric_type'='l2_distance', 'dim'='3', 'quantizer'='sq1'))
            engine=olap
            duplicate key(id)
            distributed by hash(id) buckets 1
            properties('replication_num' = '1');"
        exception "only support ann index with quantizer flat, sq4 or sq8"
    }
    

}