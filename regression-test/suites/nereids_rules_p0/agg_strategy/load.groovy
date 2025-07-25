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

suite("agg_strategy") {
    sql "set global enable_auto_analyze=false"
    // ndv is high
    sql "drop table if exists t_gbykey_10_dstkey_10_1000_id"
    sql """create table t_gbykey_10_dstkey_10_1000_id(id int, gby_key int, dst_key1 int, dst_key2 int) duplicate key(id) distributed by hash(id)
    buckets 32 properties('replication_num' = '1');"""
    sql """INSERT INTO t_gbykey_10_dstkey_10_1000_id VALUES
        (1, 3, 7, 42),
        (2, 5, 9, 18),
        (3, 2, 4, 76),
        (4, 8, 1, 33),
        (5, 6, 3, 91),
        (6, 1, 5, 27),
        (7, 4, 8, 64),
        (8, 9, 2, 55),
        (9, 7, 6, 13),
        (10, 10, 10, 100);"""

    // ndv is low
    sql "drop table if exists t_gbykey_2_dstkey_10_30_id"
    sql """create table t_gbykey_2_dstkey_10_30_id(id int, gby_key int, dst_key1 int, dst_key2 int) duplicate key(id) distributed by hash(id)
        buckets 32 properties('replication_num' = '1');"""
    sql """
        INSERT INTO t_gbykey_2_dstkey_10_30_id (id, gby_key, dst_key1, dst_key2) VALUES 
        (0, 0, 0, 0),(1, 1, 1, 1),(0, 0, 0, 2),(1, 1, 1, 3),(0, 0, 0, 4),(1, 1, 1, 5),(0, 0, 0, 6),
        (1, 1, 1, 7),(0, 0, 0, 8),(1, 1, 1, 9),(0, 0, 1, 0),(1, 1, 0, 1),(0, 0, 1, 2),(0, 1, 0, 3),
        (1, 0, 1, 4),(0, 1, 0, 5),(1, 0, 1, 6),(0, 1, 0, 7),(1, 0, 1, 8),(0, 1, 0, 9),(1, 0, 0, 0),
        (0, 1, 1, 1),(1, 0, 0, 2),(0, 1, 1, 3),(1, 0, 0, 4),(0, 1, 1, 5),(0, 0, 0, 6),(1, 1, 1, 7),
        (0, 0, 0, 8),(1, 1, 1, 9);"""

    // agg father give add hash request
    multi_sql """
        drop table if exists orders_left;
        CREATE TABLE `orders_left` (
          `o_orderkey` bigint NULL,
          `o_orderdate` date NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`o_orderkey`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        insert into orders_left values(1,'2024-01-02');
        drop table if exists lineitem_left;
        CREATE TABLE `lineitem_left` (
          `l_orderkey` bigint NULL,
          `l_shipdate` date NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`l_orderkey`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        insert into lineitem_left values(1,'2024-01-02');
    """

    multi_sql """
    drop table if exists t_gbykey_10_dstkey_10_1000_dst_key1;
    CREATE TABLE `t_gbykey_10_dstkey_10_1000_dst_key1` (
    `id` int NULL,
    `gby_key` int NULL,
    `dst_key1` varchar(10) NULL,
    `dst_key2` int NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`dst_key1`) BUCKETS 32
    PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
    );
    INSERT INTO t_gbykey_10_dstkey_10_1000_dst_key1 VALUES
    (1, 3, 7, 42),
    (2, 5, 9, 18),
    (3, 2, 4, 76),
    (4, 8, 1, 33),
    (5, 6, 3, 91),
    (6, 1, 5, 27),
    (7, 4, 8, 64),
    (8, 9, 2, 55),
    (9, 7, 6, 13),
    (10, 10, 10, 100);"""

    multi_sql """
    drop table if exists t1000_2;
    create table t1000_2(a_1 int, b_5 int, c_10 int, d_200 int) distributed by hash(c_10) properties('replication_num'='1');
    insert into t1000_2 select 1, number%5 , number%10, number%200 from numbers("number"="1003");
    """

    multi_sql """
    drop table if exists t1000;
    create table t1000(a_1 int, b_5 int, c_10 int, d_20 int) distributed by hash(c_10) properties('replication_num'='1');
    insert into t1000 select 1, number%5 , number%10, number%20 from numbers("number"="1003");
    set multi_distinct_strategy=0;
    """

    multi_sql """
    drop table if exists t1025_skew5000;
    create table t1025_skew5000(a_1 int, b_5 int, c_10 int, d_1025 int) distributed by hash(c_10) properties('replication_num'='1');
    insert into t1025_skew5000 select 1, number%5 , number%10, number from numbers("number"="1025");
    insert into t1025_skew5000 select 1, number%5 , number%10, 100 from numbers("number"="5000");
    """

    multi_sql """
    drop table if exists t1025;
    create table t1025(a_1 int, b_5 int, c_10 int, d_1025 int) distributed by hash(c_10) properties('replication_num'='1');
    insert into t1025 select 1, number%5 , number%10, number from numbers("number"="1025");
    """
}
