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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/aggregate
// and modified by Doris.

suite("test_hash_join_local_shuffle") {

    sql """drop table if exists test_hash_join_local_shuffle1;"""
    sql """drop table if exists test_hash_join_local_shuffle2;"""
    sql """drop table if exists test_hash_join_local_shuffle3;"""
    sql """drop table if exists test_hash_join_local_shuffle4;"""
    sql """
    CREATE TABLE `test_hash_join_local_shuffle1` (
              `id1` bigint,
              `id2` bigint,
              `id3` bigint,
              `id4` bigint,
            ) ENGINE=OLAP
            DUPLICATE KEY(`id1`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id1`) BUCKETS 96
            PROPERTIES (
              "replication_allocation" = "tag.location.default: 1"
            ); """
    sql """
    CREATE TABLE `test_hash_join_local_shuffle2` (
              `id1` bigint,
              `id2` bigint,
              `id3` bigint,
              `id4` bigint,
            ) ENGINE=OLAP
            DUPLICATE KEY(`id1`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id1`) BUCKETS 96
            PROPERTIES (
              "replication_allocation" = "tag.location.default: 1"
            ); """
    sql """
    CREATE TABLE `test_hash_join_local_shuffle3` (
              `id1` bigint,
              `id2` bigint,
              `id3` bigint,
              `id4` bigint,
            ) ENGINE=OLAP
            DUPLICATE KEY(`id1`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id1`) BUCKETS 96
            PROPERTIES (
              "replication_allocation" = "tag.location.default: 1"
            );  """
    sql """
    CREATE TABLE `test_hash_join_local_shuffle4` (
              `id1` bigint,
              `id2` bigint,
              `id3` bigint,
              `id4` bigint,
            ) ENGINE=OLAP
            DUPLICATE KEY(`id1`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id1`) BUCKETS 96
            PROPERTIES (
              "replication_allocation" = "tag.location.default: 1"
            ); """
    sql """ insert into test_hash_join_local_shuffle1 values(1,2,3,4); """
    sql """ insert into test_hash_join_local_shuffle2 values(1,2,3,4); """
    sql """ insert into test_hash_join_local_shuffle3 values(1,2,3,4); """
    sql """ insert into test_hash_join_local_shuffle4 values(1,2,3,4); """

    qt_select """
    select /*+ SET_VAR(disable_join_reorder=true)*/ * from   (select tmp2.id1,tmp2.id3,tmp2.id4,count(distinct tmp2.id2) from (select tmp1.id1, tmp1.id2, tmp1.id3, tmp1.id4 from   (select test_hash_join_local_shuffle3.id1,test_hash_join_local_shuffle3.id2,test_hash_join_local_shuffle3.id3,test_hash_join_local_shuffle3.id4 from test_hash_join_local_shuffle2 join[shuffle] test_hash_join_local_shuffle3 on test_hash_join_local_shuffle2.id3 = test_hash_join_local_shuffle3.id3) tmp1 join [broadcast] test_hash_join_local_shuffle4 on test_hash_join_local_shuffle4.id2 = tmp1.id2) tmp2 group by tmp2.id1, tmp2.id3, tmp2.id4) tmp join [shuffle]  test_hash_join_local_shuffle1 on test_hash_join_local_shuffle1.id3 = tmp.id3;
    """
     
}
