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

suite("mv_up_down_load") {

    String db = context.config.getDbNameByFile(context.file)
    sql """use ${db}"""

    sql """
    drop table if exists orders_2_agg
    """

    sql """CREATE TABLE `orders_2_agg` (
      `o_orderkey` BIGINT NULL,
      `o_custkey` INT NULL,
      `o_orderdate` DATE not NULL,
      `o_orderstatus` VARCHAR(1) replace,
      `o_totalprice` DECIMAL(15, 2) sum,
      `o_orderpriority` VARCHAR(15) replace,
      `o_clerk` VARCHAR(15) replace,
      `o_shippriority` INT sum,
      `o_comment` VARCHAR(79) replace
    ) ENGINE=OLAP
    aggregate KEY(`o_orderkey`, `o_custkey`, `o_orderdate`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    insert into orders_2_agg values 
    (null, 1, '2023-10-17', 'o', 99.5, 'a', 'b', 1, 'yy'),
    (1, null, '2023-10-17', 'k', 109.2, 'c','d',2, 'mm'),
    (3, 3, '2023-10-19', null, 99.5, 'a', 'b', 1, 'yy'),
    (1, 2, '2023-10-20', 'o', null, 'a', 'b', 1, 'yy'),
    (2, 3, '2023-10-21', 'k', 109.2, null,'d',2, 'mm'),
    (3, 1, '2023-10-22', 'o', 99.5, 'a', null, 1, 'yy'),
    (1, 3, '2023-10-19', 'k', 99.5, 'a', 'b', null, 'yy'),
    (2, 1, '2023-10-18', 'o', 109.2, 'c','d',2, null),
    (3, 2, '2023-10-17', 'k', 99.5, 'a', 'b', 1, 'yy'),
    (4, 5, '2023-10-19', 'o', 99.5, 'a', 'b', 1, 'yy');
    """
    sql """analyze table orders_2_agg with sync;"""

    def mv_name_agg = "agg_mv"
    def mv_stmt_agg = """
            select
            o_orderkey,
            sum(O_TOTALPRICE) as sum_total  
            from orders_2_agg
            where o_orderdate >= '2023-10-17' 
            group by o_orderkey"""
    createMV(getMVStmt(mv_name_agg, mv_stmt_agg))
    sql """analyze table orders_2_agg with sync;"""

    sql """
    drop table if exists orders_2_3
    """

    sql """CREATE TABLE `orders_2_3` (
      `o_orderkey` BIGINT NULL,
      `o_custkey` INT NULL,
      `o_orderstatus` VARCHAR(1) NULL,
      `o_totalprice` DECIMAL(15, 2)  NULL,
      `o_orderpriority` VARCHAR(15) NULL,
      `o_clerk` VARCHAR(15) NULL,
      `o_shippriority` INT NULL,
      `o_comment` VARCHAR(79) NULL,
      `o_orderdate` DATE not NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`o_orderkey`, `o_custkey`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    insert into orders_2_3 values 
    (null, 1, 'o', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
    (1, null, 'k', 109.2, 'c','d',2, 'mm', '2023-10-17'),
    (3, 3, null, 99.5, 'a', 'b', 1, 'yy', '2023-10-19'),
    (1, 2, 'o', null, 'a', 'b', 1, 'yy', '2023-10-20'),
    (2, 3, 'k', 109.2, null,'d',2, 'mm', '2023-10-21'),
    (3, 1, 'o', 99.5, 'a', null, 1, 'yy', '2023-10-22'),
    (1, 3, 'k', 99.5, 'a', 'b', null, 'yy', '2023-10-19'),
    (2, 1, 'o', 109.2, 'c','d',2, null, '2023-10-18'),
    (3, 2, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
    (4, 5, 'o', 99.5, 'a', 'b', 1, 'yy', '2023-10-19');
    """
    sql """analyze table orders_2_3 with sync;"""

    def mv_name_dup = "dup_mv"
    def mv_stmt_dup = """select
            o_orderkey,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2
            from orders_2_3 group by o_orderkey
            """
    createMV(getMVStmt(mv_name_dup, mv_stmt_dup))
    sql """analyze table orders_2_3 with sync;"""

    sql """
    drop table if exists orders_2_uniq
    """

    sql """CREATE TABLE `orders_2_uniq` (
      `o_orderkey` BIGINT NULL,
      `o_custkey` INT NULL,
      `o_orderdate` DATE not NULL,
      `o_orderstatus` VARCHAR(1) NULL,
      `o_totalprice` DECIMAL(15, 2)  NULL,
      `o_orderpriority` VARCHAR(15) NULL,
      `o_clerk` VARCHAR(15) NULL,
      `o_shippriority` INT NULL,
      `o_comment` VARCHAR(79) NULL
    ) ENGINE=OLAP
    unique KEY(`o_orderkey`, `o_custkey`, `o_orderdate`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    insert into orders_2_uniq values 
    (null, 1, '2023-10-17', 'o', 99.5, 'a', 'b', 1, 'yy'),
    (1, null, '2023-10-17', 'k', 109.2, 'c','d',2, 'mm'),
    (3, 3, '2023-10-19', null, 99.5, 'a', 'b', 1, 'yy'),
    (1, 2, '2023-10-20', 'o', null, 'a', 'b', 1, 'yy'),
    (2, 3, '2023-10-21', 'k', 109.2, null,'d',2, 'mm'),
    (3, 1, '2023-10-22', 'o', 99.5, 'a', null, 1, 'yy'),
    (1, 3, '2023-10-19', 'k', 99.5, 'a', 'b', null, 'yy'),
    (2, 1, '2023-10-18', 'o', 109.2, 'c','d',2, null),
    (3, 2, '2023-10-17', 'k', 99.5, 'a', 'b', 1, 'yy'),
    (4, 5, '2023-10-19', 'o', 99.5, 'a', 'b', 1, 'yy');
    """
    sql """analyze table orders_2_uniq with sync;"""

    def mv_name_uniq = "uniq_mv"
    def mv_stmt_uniq = """
        select o_orderkey, o_custkey, o_orderdate,  
            case when o_shippriority > 1 then 1 else 2 end cnt_1  
            from orders_2_uniq  
            where o_orderkey > 1 + 1 
        """
    createMV(getMVStmt(mv_name_uniq, mv_stmt_uniq))
    sql """analyze table orders_2_uniq with sync;"""

}
