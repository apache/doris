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

suite("product_quantization") {
    sql "drop table if exists product_quantization"
    sql """create table product_quantization (
            id int not null,
            vec array<float> not null,
            index ann_idx (vec)
                using ann properties ('index_type'='hnsw', 'metric_type'='l2_distance', 'dim'='4', 'quantizer'='pq', 'pq_m'='2', 'pq_nbits'='2'))
            engine=olap
            duplicate key(id)
            distributed by hash(id) buckets 1
            properties('replication_num' = '1');"""
    sql """insert into product_quantization values
        (1, [1.0, 2.0, 3.0, 4.0]),
        (2, [2.0, 3.0, 4.0, 5.0]),
        (3, [3.0, 4.0, 5.0, 6.0]),
        (4, [4.0, 5.0, 6.0, 7.0])
        """
    qt_sql """select * from product_quantization order by id"""

    sql "drop table if exists product_quantization"
    sql """create table product_quantization (
            id int not null,
            vec array<float> not null,
            index ann_idx (vec)
                using ann properties ('index_type'='hnsw', 'metric_type'='l2_distance', 'dim'='4', 'quantizer'='pq', 'pq_m'='2', 'pq_nbits'='2'))
            engine=olap
            duplicate key(id)
            distributed by hash(id) buckets 1
            properties('replication_num' = '1');"""
    test {
        sql """insert into product_quantization values (1, [1.0, 2.0, 3.0, 4.0])"""
        exception """exception occurred during training"""
    }

    sql """drop table if exists product_quantization"""
    test {
        sql """create table product_quantization (
            id int not null,
            vec array<float> not null,
            index ann_idx (vec)
                using ann properties ('index_type'='hnsw', 'metric_type'='l2_distance', 'dim'='4', 'quantizer'='pq', 'pq_m'='5', 'pq_nbits'='2'))
            engine=olap
            duplicate key(id)
            distributed by hash(id) buckets 1
            properties('replication_num' = '1');"""
        exception """The dimension of the vector (dim) should be a multiple of the number of subquantizers (pq_m)"""
    }

    sql """drop table if exists product_quantization"""
    test {
        sql """create table product_quantization (
            id int not null,
            vec array<float> not null,
            index ann_idx (vec)
                using ann properties ('index_type'='hnsw', 'metric_type'='l2_distance', 'dim'='4', 'quantizer'='pqx', 'pq_m'='2', 'pq_nbits'='2'))
            engine=olap
            duplicate key(id)
            distributed by hash(id) buckets 1
            properties('replication_num' = '1');"""
        exception """only support ann index with quantizer flat, sq4, sq8 or pq"""
    }

    sql """drop table if exists product_quantization"""
    test {
        sql """create table product_quantization (
            id int not null,
            vec array<float> not null,
            index ann_idx (vec)
                using ann properties ('index_type'='hnsw', 'metric_type'='l2_distance', 'dim'='4', 'quantizer'='pq'))
            engine=olap
            duplicate key(id)
            distributed by hash(id) buckets 1
            properties('replication_num' = '1');"""
        exception """The dimension of the vector (dim) or the number of subquantizers (pq_m) cannot be zero"""
    }

    sql """drop table if exists product_quantization"""
    test {
        sql """create table product_quantization (
            id int not null,
            vec array<float> not null,
            index ann_idx (vec)
                using ann properties ('index_type'='hnsw', 'metric_type'='l2_distance', 'dim'='4', 'quantizer'='pq', 'pq_m'='2', 'pq_nbits'='25'))
            engine=olap
            duplicate key(id)
            distributed by hash(id) buckets 1
            properties('replication_num' = '1');"""
        exception """pq_nbits larger than 24 is not practical"""
    }
}
