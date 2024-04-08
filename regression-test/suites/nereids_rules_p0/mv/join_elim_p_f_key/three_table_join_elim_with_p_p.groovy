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

suite("three_table_join_elim_with_p_p_key") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_materialized_view_rewrite=true"
    sql "SET enable_nereids_timeout = false"

    sql """
    drop table if exists orders_2_p_p
    """

    sql """CREATE TABLE `orders_2_p_p` (
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
    DUPLICATE KEY(`o_orderkey`)
    COMMENT 'OLAP'
    auto partition by range (date_trunc(`o_orderdate`, 'day')) ()
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    drop table if exists lineitem_2_p_p
    """

    sql """CREATE TABLE `lineitem_2_p_p` (
      `l_linenumber` INT NULL,
      `l_partkey` INT not NULL,
      `l_orderkey` BIGINT not NULL,
      `l_suppkey` INT not NULL,
      `l_quantity` DECIMAL(15, 2) NULL,
      `l_extendedprice` DECIMAL(15, 2) NULL,
      `l_discount` DECIMAL(15, 2) NULL,
      `l_tax` DECIMAL(15, 2) NULL,
      `l_returnflag` VARCHAR(1) NULL,
      `l_linestatus` VARCHAR(1) NULL,
      `l_commitdate` DATE NULL,
      `l_receiptdate` DATE NULL,
      `l_shipinstruct` VARCHAR(25) NULL,
      `l_shipmode` VARCHAR(10) NULL,
      `l_comment` VARCHAR(44) NULL,
      `l_shipdate` DATE not NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(l_linenumber)
    COMMENT 'OLAP'
    auto partition by range (date_trunc(`l_shipdate`, 'day')) ()
    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    drop table if exists partsupp_2_p_p
    """

    sql """CREATE TABLE `partsupp_2_p_p` (
      `ps_partkey` INT NULL,
      `ps_suppkey` INT NULL,
      `ps_availqty` INT NULL,
      `ps_supplycost` DECIMAL(15, 2) NULL,
      `ps_comment` VARCHAR(199) NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`ps_partkey`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`ps_partkey`) BUCKETS 24
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    insert into orders_2_p_p values 
    (1, null, 'o', 109.2, 'c','d',2, 'mm', '2023-10-17'),
    (3, 3, null, 99.5, 'a', 'b', 1, 'yy', '2023-10-19'),
    (2, 3, 'k', 109.2, null,'d',2, 'mm', '2023-10-21'),
    (4, 5, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-19'); 
    """

    sql """
    insert into lineitem_2_p_p values 
    (null, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (1, 1, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (3, 3, 3, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 2, 3, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (2, 1, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18'),
    (3, 1, 3, 1, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 2, 1, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (2, 2, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18'),
    (3, 3, 3, 3, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 1, 1, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17');
    """

    sql"""
    insert into partsupp_2_p_p values 
    (1, 1, 1, 99.5, 'yy'),
    (2, 2, 2, 109.2, 'mm'),
    (3, 3, 1, 99.5, 'yy'),
    (3, null, 1, 99.5, 'yy'); 
    """

    sql """analyze table lineitem_2_p_p with sync;"""
    sql """analyze table orders_2_p_p with sync;"""
    sql """analyze table partsupp_2_p_p with sync;"""

    def create_mv_all = { mv_name, mv_sql ->
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""
        sql """DROP TABLE IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL 
        DISTRIBUTED BY RANDOM BUCKETS 2 
        PROPERTIES ('replication_num' = '1') 
        AS  
        ${mv_sql}
        """
    }

    def compare_res = { def stmt ->
        sql "SET enable_materialized_view_rewrite=false"
        def origin_res = sql stmt
        logger.info("origin_res: " + origin_res)
        sql "SET enable_materialized_view_rewrite=true"
        def mv_origin_res = sql stmt
        logger.info("mv_origin_res: " + mv_origin_res)
        assertTrue((mv_origin_res == [] && origin_res == []) || (mv_origin_res.size() == origin_res.size()))
        for (int row = 0; row < mv_origin_res.size(); row++) {
            assertTrue(mv_origin_res[row].size() == origin_res[row].size())
            for (int col = 0; col < mv_origin_res[row].size(); col++) {
                assertTrue(mv_origin_res[row][col] == origin_res[row][col])
            }
        }
    }

    def mv_name = "mv_elim_2_p_p"

    // 都不为null，和只有一个为null
    sql """alter table partsupp_2_p_p add constraint pk primary key(ps_partkey, ps_suppkey)"""
    sql """alter table lineitem_2_p_p add constraint fk1 foreign key (l_partkey, l_suppkey) references partsupp_2_p_p(ps_partkey, ps_suppkey)"""
    sql """alter table orders_2_p_p add constraint pk primary key(o_orderkey)"""
    sql """alter table lineitem_2_p_p add constraint fk foreign key (l_orderkey) references orders_2_p_p(o_orderkey)"""

    // base mtmv + not on combined pk + inner/left join + group by/distinct in diff position
    // base mtmv + inner/left join
    def mv_stmt_1 = """
        select * from lineitem_2_p_p
        inner join orders_2_p_p
        on orders_2_p_p.o_orderkey = lineitem_2_p_p.l_orderkey
        inner join partsupp_2_p_p
        on partsupp_2_p_p.ps_partkey = lineitem_2_p_p.l_partkey
        """
    def mv_stmt_2 = """
        select * from lineitem_2_p_p
        left join orders_2_p_p
        on orders_2_p_p.o_orderkey = lineitem_2_p_p.l_orderkey
        left join partsupp_2_p_p
        on partsupp_2_p_p.ps_partkey = lineitem_2_p_p.l_partkey
        """
    // base mtmv + group by in one primary key + inner/left join
    def mv_stmt_3 = """
        select * from lineitem_2_p_p
        inner join (select o_orderkey from orders_2_p_p group by o_orderkey) as t1
        on t1.o_orderkey = lineitem_2_p_p.l_orderkey
        inner join partsupp_2_p_p
        on partsupp_2_p_p.ps_partkey = lineitem_2_p_p.l_partkey
        """
    def mv_stmt_4 = """
        select * from lineitem_2_p_p
        left join (select o_orderkey from orders_2_p_p group by o_orderkey) as t1
        on t1.o_orderkey = lineitem_2_p_p.l_orderkey
        left join partsupp_2_p_p
        on partsupp_2_p_p.ps_partkey = lineitem_2_p_p.l_partkey
        """
    // base mtmv + group by in combine primary key + inner/left join
    def mv_stmt_5 = """
        select * from lineitem_2_p_p
        inner join orders_2_p_p
        on orders_2_p_p.o_orderkey = lineitem_2_p_p.l_orderkey
        inner join (select ps_partkey, ps_suppkey from partsupp_2_p_p group by ps_partkey, ps_suppkey) as t2
        on t2.ps_partkey = lineitem_2_p_p.l_partkey
        """
    def mv_stmt_6 = """
        select * from lineitem_2_p_p
        left join orders_2_p_p
        on orders_2_p_p.o_orderkey = lineitem_2_p_p.l_orderkey
        left join (select ps_partkey, ps_suppkey from partsupp_2_p_p group by ps_partkey, ps_suppkey) as t2
        on t2.ps_partkey = lineitem_2_p_p.l_partkey
        """
    // base mtmv + group by + inner/left join
    def mv_stmt_7 = """
        select * from lineitem_2_p_p
        inner join (select o_orderkey from orders_2_p_p group by o_orderkey) as t1
        on t1.o_orderkey = lineitem_2_p_p.l_orderkey
        inner join (select ps_partkey, ps_suppkey from partsupp_2_p_p group by ps_partkey, ps_suppkey) as t2
        on t2.ps_partkey = lineitem_2_p_p.l_partkey
        """
    def mv_stmt_8 = """
        select * from lineitem_2_p_p
        left join (select o_orderkey from orders_2_p_p group by o_orderkey) as t1
        on t1.o_orderkey = lineitem_2_p_p.l_orderkey
        left join (select ps_partkey, ps_suppkey from partsupp_2_p_p group by ps_partkey, ps_suppkey) as t2
        on t2.ps_partkey = lineitem_2_p_p.l_partkey
        """
    // base mtmv + distinct in one primary key + inner/left join
    def mv_stmt_9 = """
        select * from lineitem_2_p_p
        inner join (select distinct(o_orderkey) from orders_2_p_p) as t1
        on t1.o_orderkey = lineitem_2_p_p.l_orderkey
        inner join partsupp_2_p_p
        on partsupp_2_p_p.ps_partkey = lineitem_2_p_p.l_partkey
        """
    def mv_stmt_10 = """
        select * from lineitem_2_p_p
        left join (select distinct(o_orderkey) from orders_2_p_p) as t1
        on t1.o_orderkey = lineitem_2_p_p.l_orderkey
        left join partsupp_2_p_p
        on partsupp_2_p_p.ps_partkey = lineitem_2_p_p.l_partkey
        """
    // base mtmv + distinct in combine primary key + inner/left join
    def mv_stmt_11 = """
        select * from lineitem_2_p_p
        inner join orders_2_p_p
        on orders_2_p_p.o_orderkey = lineitem_2_p_p.l_orderkey
        inner join (select distinct(ps_partkey) from partsupp_2_p_p) as t2
        on t2.ps_partkey = lineitem_2_p_p.l_partkey
        """
    def mv_stmt_12 = """
        select * from lineitem_2_p_p
        left join orders_2_p_p
        on orders_2_p_p.o_orderkey = lineitem_2_p_p.l_orderkey
        left join (select distinct(ps_partkey) from partsupp_2_p_p) as t2
        on t2.ps_partkey = lineitem_2_p_p.l_partkey
        """
    // base mtmv + distinct + inner/left join
    def mv_stmt_13 = """
        select * from lineitem_2_p_p
        inner join (select distinct(o_orderkey) from orders_2_p_p) as t1
        on t1.o_orderkey = lineitem_2_p_p.l_orderkey
        inner join (select distinct(ps_partkey) from partsupp_2_p_p) as t2
        on t2.ps_partkey = lineitem_2_p_p.l_partkey
        """
    def mv_stmt_14 = """
        select * from lineitem_2_p_p
        left join (select distinct(o_orderkey) from orders_2_p_p) as t1
        on t1.o_orderkey = lineitem_2_p_p.l_orderkey
        left join (select distinct(ps_partkey) from partsupp_2_p_p) as t2
        on t2.ps_partkey = lineitem_2_p_p.l_partkey
        """

    // base mtmv + on combined pk + inner/left join + group by/distinct in diff position
    // base mtmv + inner/left join + on combined pk
    def mv_stmt_15 = """
        select * from lineitem_2_p_p
        inner join orders_2_p_p
        on orders_2_p_p.o_orderkey = lineitem_2_p_p.l_orderkey
        inner join partsupp_2_p_p
        on partsupp_2_p_p.ps_partkey = lineitem_2_p_p.l_partkey and partsupp_2_p_p.ps_suppkey = lineitem_2_p_p.l_suppkey
        """
    def mv_stmt_16 = """
        select * from lineitem_2_p_p
        left join orders_2_p_p
        on orders_2_p_p.o_orderkey = lineitem_2_p_p.l_orderkey
        left join partsupp_2_p_p
        on partsupp_2_p_p.ps_partkey = lineitem_2_p_p.l_partkey and partsupp_2_p_p.ps_suppkey = lineitem_2_p_p.l_suppkey
        """
    // base mtmv + group by in one primary key + inner/left join + on combined pk
    def mv_stmt_17 = """
        select * from lineitem_2_p_p
        inner join (select o_orderkey from orders_2_p_p group by o_orderkey) as t1
        on t1.o_orderkey = lineitem_2_p_p.l_orderkey
        inner join partsupp_2_p_p
        on partsupp_2_p_p.ps_partkey = lineitem_2_p_p.l_partkey and partsupp_2_p_p.ps_suppkey = lineitem_2_p_p.l_suppkey
        """
    def mv_stmt_18 = """
        select * from lineitem_2_p_p
        left join (select o_orderkey from orders_2_p_p group by o_orderkey) as t1
        on t1.o_orderkey = lineitem_2_p_p.l_orderkey
        left join partsupp_2_p_p
        on partsupp_2_p_p.ps_partkey = lineitem_2_p_p.l_partkey and partsupp_2_p_p.ps_suppkey = lineitem_2_p_p.l_suppkey
        """
    // base mtmv + group by in combine primary key + inner/left join + on combined pk
    def mv_stmt_19 = """
        select * from lineitem_2_p_p
        inner join orders_2_p_p
        on orders_2_p_p.o_orderkey = lineitem_2_p_p.l_orderkey
        inner join (select ps_partkey, ps_suppkey from partsupp_2_p_p group by ps_partkey, ps_suppkey) as t2
        on t2.ps_partkey = lineitem_2_p_p.l_partkey and t2.ps_suppkey = lineitem_2_p_p.l_suppkey
        """
    def mv_stmt_20 = """
        select * from lineitem_2_p_p
        left join orders_2_p_p
        on orders_2_p_p.o_orderkey = lineitem_2_p_p.l_orderkey
        left join (select ps_partkey, ps_suppkey from partsupp_2_p_p group by ps_partkey, ps_suppkey) as t2
        on t2.ps_partkey = lineitem_2_p_p.l_partkey and t2.ps_suppkey = lineitem_2_p_p.l_suppkey
        """
    // base mtmv + group by + inner/left join + on combined pk
    def mv_stmt_21 = """
        select * from lineitem_2_p_p
        inner join (select o_orderkey from orders_2_p_p group by o_orderkey) as t1
        on t1.o_orderkey = lineitem_2_p_p.l_orderkey
        inner join (select ps_partkey, ps_suppkey from partsupp_2_p_p group by ps_partkey, ps_suppkey) as t2
        on t2.ps_partkey = lineitem_2_p_p.l_partkey and t2.ps_suppkey = lineitem_2_p_p.l_suppkey
        """
    def mv_stmt_22 = """
        select * from lineitem_2_p_p
        left join (select o_orderkey from orders_2_p_p group by o_orderkey) as t1
        on t1.o_orderkey = lineitem_2_p_p.l_orderkey
        left join (select ps_partkey, ps_suppkey from partsupp_2_p_p group by ps_partkey, ps_suppkey) as t2
        on t2.ps_partkey = lineitem_2_p_p.l_partkey and t2.ps_suppkey = lineitem_2_p_p.l_suppkey
        """
    // base mtmv + distinct in one primary key + inner/left join + on combined pk
    def mv_stmt_23 = """
        select * from lineitem_2_p_p
        inner join (select distinct(o_orderkey) from orders_2_p_p) as t1
        on t1.o_orderkey = lineitem_2_p_p.l_orderkey
        inner join partsupp_2_p_p
        on partsupp_2_p_p.ps_partkey = lineitem_2_p_p.l_partkey and partsupp_2_p_p.ps_suppkey = lineitem_2_p_p.l_suppkey
        """
    def mv_stmt_24 = """
        select * from lineitem_2_p_p
        left join (select distinct(o_orderkey) from orders_2_p_p) as t1
        on t1.o_orderkey = lineitem_2_p_p.l_orderkey
        left join partsupp_2_p_p
        on partsupp_2_p_p.ps_partkey = lineitem_2_p_p.l_partkey and partsupp_2_p_p.ps_suppkey = lineitem_2_p_p.l_suppkey
        """
    // base mtmv + distinct in combine primary key + inner/left join + on combined pk
    def mv_stmt_25 = """
        select * from lineitem_2_p_p
        inner join orders_2_p_p
        on orders_2_p_p.o_orderkey = lineitem_2_p_p.l_orderkey
        inner join (select distinct(ps_partkey), ps_suppkey from partsupp_2_p_p) as t2
        on t2.ps_partkey = lineitem_2_p_p.l_partkey and t2.ps_suppkey = lineitem_2_p_p.l_suppkey
        """
    def mv_stmt_26 = """
        select * from lineitem_2_p_p
        left join orders_2_p_p
        on orders_2_p_p.o_orderkey = lineitem_2_p_p.l_orderkey
        left join (select distinct(ps_partkey), ps_suppkey from partsupp_2_p_p) as t2
        on t2.ps_partkey = lineitem_2_p_p.l_partkey and t2.ps_suppkey = lineitem_2_p_p.l_suppkey
        """
    // base mtmv + distinct + inner/left join + on combined pk
    def mv_stmt_27 = """
        select * from lineitem_2_p_p
        inner join (select distinct(o_orderkey) from orders_2_p_p) as t1
        on t1.o_orderkey = lineitem_2_p_p.l_orderkey
        inner join (select distinct(ps_partkey), ps_suppkey from partsupp_2_p_p) as t2
        on t2.ps_partkey = lineitem_2_p_p.l_partkey and t2.ps_suppkey = lineitem_2_p_p.l_suppkey
        """
    def mv_stmt_28 = """
        select * from lineitem_2_p_p
        left join (select distinct(o_orderkey) from orders_2_p_p) as t1
        on t1.o_orderkey = lineitem_2_p_p.l_orderkey
        left join (select distinct(ps_partkey), ps_suppkey from partsupp_2_p_p) as t2
        on t2.ps_partkey = lineitem_2_p_p.l_partkey and t2.ps_suppkey = lineitem_2_p_p.l_suppkey
        """

    // single base table + group by + filter/not null
    def query_1 = """select lineitem_2_p_p.l_shipdate from lineitem_2_p_p;"""
    def query_2 = """select orders_2_p_p.o_orderdate from orders_2_p_p;"""
    def query_3 = """select lineitem_2_p_p.l_shipdate from lineitem_2_p_p where lineitem_2_p_p.l_orderkey is not null group by l_shipdate"""
    def query_4 = """select orders_2_p_p.o_orderdate from lineitem_2_p_p where orders_2_p_p.o_orderkey is not null group by o_orderdate"""
    def query_5 = """select lineitem_2_p_p.l_shipdate from lineitem_2_p_p where lineitem_2_p_p.l_orderkey = 1 group by l_shipdate"""
    def query_6 = """select orders_2_p_p.o_orderdate from lineitem_2_p_p where orders_2_p_p.o_orderkey = 1 group by o_orderdate"""

    // inner/left join + group by/distinct
    def query_7 = """select *  from lineitem_2_p_p inner join orders_2_p_p on orders_2_p_p.o_orderkey = lineitem_2_p_p.l_orderkey"""
    def query_8 = """select * from lineitem_2_p_p left join orders_2_p_p on orders_2_p_p.o_orderkey = lineitem_2_p_p.l_orderkey"""
    def query_9 = """select * from lineitem_2_p_p inner join (select o_orderkey from orders_2_p_p group by o_orderkey) as t1 on t1.o_orderkey = lineitem_2_p_p.l_orderkey"""
    def query_10 = """select * from lineitem_2_p_p left join (select o_orderkey from orders_2_p_p group by o_orderkey) as t1 on t1.o_orderkey = lineitem_2_p_p.l_orderkey"""
    def query_11 = """select * from lineitem_2_p_p inner join (select distinct(o_orderkey) from orders_2_p_p) as t1 on t1.o_orderkey = lineitem_2_p_p.l_orderkey"""
    def query_12 = """select * from lineitem_2_p_p left join (select distinct(o_orderkey) from orders_2_p_p) as t1 on t1.o_orderkey = lineitem_2_p_p.l_orderkey"""

    // inner/left join + group by/distinct + combined primary key
    def query_13 = """select * from lineitem_2_p_p inner join partsupp_2_p_p on partsupp_2_p_p.ps_partkey = lineitem_2_p_p.l_partkey"""
    def query_14 = """select * from lineitem_2_p_p left join partsupp_2_p_p on partsupp_2_p_p.ps_partkey = lineitem_2_p_p.l_partkey"""
    def query_15 = """select * from lineitem_2_p_p inner join partsupp_2_p_p on partsupp_2_p_p.ps_partkey = lineitem_2_p_p.l_partkey and partsupp_2_p_p.ps_suppkey = lineitem_2_p_p.l_suppkey"""
    def query_16 = """select * from lineitem_2_p_p left join partsupp_2_p_p on partsupp_2_p_p.ps_partkey = lineitem_2_p_p.l_partkey and partsupp_2_p_p.ps_suppkey = lineitem_2_p_p.l_suppkey"""

    // inner/left join + group by/distinct + combined primary key + group by
    def query_17 = """select * from lineitem_2_p_p inner join (select ps_partkey from partsupp_2_p_p group by ps_partkey) as t2 on t2.ps_partkey = lineitem_2_p_p.l_partkey"""
    def query_18 = """select * from lineitem_2_p_p left join (select ps_partkey from partsupp_2_p_p group by ps_partkey) as t2 on t2.ps_partkey = lineitem_2_p_p.l_partkey"""
    def query_19 = """select * from lineitem_2_p_p inner join (select ps_partkey from partsupp_2_p_p group by ps_partkey) as t2 on t2.ps_partkey = lineitem_2_p_p.l_partkey and t2.ps_suppkey = lineitem_2_p_p.l_suppkey"""
    def query_20 = """select * from lineitem_2_p_p left join (select ps_partkey from partsupp_2_p_p group by ps_partkey) as t2 on t2.ps_partkey = lineitem_2_p_p.l_partkey and t2.ps_suppkey = lineitem_2_p_p.l_suppkey"""

    // inner/left join + group by/distinct + combined primary key + distinct
    def query_21 = """select * from lineitem_2_p_p inner join (select distinct(ps_partkey) from partsupp_2_p_p) as t2 on t2.ps_partkey = lineitem_2_p_p.l_partkey"""
    def query_22 = """select * from lineitem_2_p_p left join (select distinct(ps_partkey) from partsupp_2_p_p) as t2 on t2.ps_partkey = lineitem_2_p_p.l_partkey"""
    def query_23 = """select * from lineitem_2_p_p inner join (select distinct(ps_partkey) from partsupp_2_p_p) as t2 on t2.ps_partkey = lineitem_2_p_p.l_partkey and t2.ps_suppkey = lineitem_2_p_p.l_suppkey"""
    def query_24 = """select * from lineitem_2_p_p left join (select distinct(ps_partkey) from partsupp_2_p_p) as t2 on t2.ps_partkey = lineitem_2_p_p.l_partkey and t2.ps_suppkey = lineitem_2_p_p.l_suppkey"""

    // single base table + group by + filter/not null + combined primary key
    def query_25 = """select ps_partkey from partsupp_2 where ps_partkey is not null group by ps_partkey"""
    def query_26 = """select ps_partkey from partsupp_2 where ps_partkey = 1 group by ps_partkey"""
    def query_27 = """select ps_partkey, ps_suppkey from partsupp_2 where ps_partkey is not null and ps_suppkey is not null group by ps_partkey, ps_suppkey"""
    def query_28 = """select ps_partkey, ps_suppkey from partsupp_2 where ps_partkey = 1 and ps_suppkey = 1 group by ps_partkey, ps_suppkey"""

    def mtmv_list = [mv_stmt_1, mv_stmt_2, mv_stmt_3, mv_stmt_4, mv_stmt_5, mv_stmt_6, mv_stmt_7, mv_stmt_8,
                     mv_stmt_9, mv_stmt_10, mv_stmt_11, mv_stmt_12, mv_stmt_13, mv_stmt_14, mv_stmt_15, mv_stmt_16, mv_stmt_17,
                     mv_stmt_18, mv_stmt_19, mv_stmt_20, mv_stmt_21, mv_stmt_22, mv_stmt_23, mv_stmt_24, mv_stmt_25, mv_stmt_26,
                     mv_stmt_27, mv_stmt_28]
    def query_list = [query_1, query_2, query_3, query_4, query_5, query_6, query_7, query_8, query_9, query_10,
                      query_11, query_12, query_13, query_14, query_15, query_16, query_17, query_18, query_19, query_20,
                      query_21, query_22, query_23, query_24, query_25, query_26, query_27, query_28]
    def order_stmt1 = " order by 1"
    def order_stmt2 = " order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25"
    def order_stmt3 = " order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21"
    def order_stmt4 = " order by 1, 2"
    for (int i = 0; i < mtmv_list.size(); i++) {
        logger.info("mtmv_number: " + i)
        def res = sql mtmv_list[i]
        assertTrue(res.size() > 0)
    }
    for (int i = 0; i < query_list.size(); i++) {
        logger.info("query_number: " + i)
        def res = sql mtmv_list[i]
        assertTrue(res.size() > 0)
    }
    for (int i = 0; i < mtmv_list.size(); i++) {
        logger.info("i: " + i)
        create_mv_all(mv_name, mtmv_list[i])
        def job_name = getJobName(db, mv_name)
        waitingMTMVTaskFinished(job_name)
        if (i == 0) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 1) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 2) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 3) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 4) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 5) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 6) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 7) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 8) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 9) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 10) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 11) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 12) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 13) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 14) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 15) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 16) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 17) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 18) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 19) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 20) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 21) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 22) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 23) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 24) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 25) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 26) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 27) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + (j < 6 ? order_stmt1 : (j < 12 ? order_stmt2 : (j < 24 ? order_stmt3 : (j < 26 ? order_stmt1 : order_stmt4)))))
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        }
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""
    }

}
