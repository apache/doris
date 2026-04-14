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

suite("dist_expr_list") {
    sql """
        drop table if exists orders_1;
        CREATE TABLE `orders_1` (
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
            );

        drop table if exists lineitem_1;
        CREATE TABLE `lineitem_1` (
            `l_orderkey` BIGINT NULL,
            `l_linenumber` INT NULL,
            `l_partkey` INT NULL,
            `l_suppkey` INT NULL,
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
            DUPLICATE KEY(l_orderkey, l_linenumber, l_partkey, l_suppkey )
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );

        drop table if exists partsupp_1;
        CREATE TABLE `partsupp_1` (
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
            );
        insert into orders_1 values 
        (null, 1, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
        (1, null, 'o', 109.2, 'c','d',2, 'mm', '2023-10-17'),
        (3, 3, null, 99.5, 'a', 'b', 1, 'yy', '2023-10-19'),
        (1, 2, 'o', null, 'a', 'b', 1, 'yy', '2023-10-20'),
        (2, 3, 'k', 109.2, null,'d',2, 'mm', '2023-10-21'),
        (3, 1, 'k', 99.5, 'a', null, 1, 'yy', '2023-10-22'),
        (1, 3, 'o', 99.5, 'a', 'b', null, 'yy', '2023-10-19'),
        (2, 1, 'o', 109.2, 'c','d',2, null, '2023-10-18'),
        (3, 2, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
        (4, 5, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-19'); 

        insert into lineitem_1 values 
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
        
        insert into partsupp_1 values 
        (1, 1, 1, 99.5, 'yy'),
        (2, 2, 2, 109.2, 'mm'),
        (3, 3, 1, 99.5, 'yy'),
        (3, null, 1, 99.5, 'yy'); 
        """

    sql """
        set enable_aggregate_cse=true;
        set enable_local_shuffle=true;
        """
    // test the query result should be the same when enable_local_shuffle=true or false
    // set enable_local_shuffle=false, then generate result.out 
    // then set enable_local_shuffle=true, test the result is the same 
    def query = """
        select 
        t1.l_orderkey, 
        t2.l_partkey, 
        t1.l_suppkey, 
        t2.o_orderkey, 
        t1.o_custkey, 
        t2.ps_partkey, 
        t1.ps_suppkey, 
        t2.agg1, 
        t1.agg2, 
        t2.agg3, 
        t1.agg4, 
        t2.agg5, 
        t1.agg6 
        from 
        (
            select 
            l_orderkey, 
            l_partkey, 
            l_suppkey, 
            o_orderkey, 
            o_custkey, 
            ps_partkey, 
            ps_suppkey, 
            t.agg1 as agg1, 
            t.sum_total as agg3, 
            t.max_total as agg4, 
            t.min_total as agg5, 
            t.count_all as agg6, 
            cast(
                sum(
                IFNULL(ps_suppkey, 0) * IFNULL(ps_partkey, 0)
                ) as decimal(28, 8)
            ) as agg2 
            from 
            (
                select 
                l_orderkey, 
                l_partkey, 
                l_suppkey, 
                o_orderkey, 
                o_custkey, 
                cast(
                    sum(
                    IFNULL(o_orderkey, 0) * IFNULL(o_custkey, 0)
                    ) as decimal(28, 8)
                ) as agg1, 
                sum(o_totalprice) as sum_total, 
                max(o_totalprice) as max_total, 
                min(o_totalprice) as min_total, 
                count(*) as count_all, 
                bitmap_union(
                    to_bitmap(
                    case when o_shippriority > 1 
                    and o_orderkey IN (1, 3) then o_custkey else null end
                    )
                ) cnt_1, 
                bitmap_union(
                    to_bitmap(
                    case when o_shippriority > 2 
                    and o_orderkey IN (2) then o_custkey else null end
                    )
                ) as cnt_2 
                from 
                lineitem_1 
                inner join orders_1 on lineitem_1.l_orderkey = orders_1.o_orderkey 
                where 
                lineitem_1.l_shipdate >= "2023-10-17" 
                group by 
                l_orderkey, 
                l_partkey, 
                l_suppkey, 
                o_orderkey, 
                o_custkey
            ) as t 
            inner join partsupp_1 on t.l_partkey = partsupp_1.ps_partkey 
            and t.l_suppkey = partsupp_1.ps_suppkey 
            where 
            partsupp_1.ps_suppkey > 1 
            group by 
            l_orderkey, 
            l_partkey, 
            l_suppkey, 
            o_orderkey, 
            o_custkey, 
            ps_partkey, 
            ps_suppkey, 
            agg1, 
            agg3, 
            agg4, 
            agg5, 
            agg6
        ) as t1 
        left join (
            select 
            l_orderkey, 
            l_partkey, 
            l_suppkey, 
            o_orderkey, 
            o_custkey, 
            ps_partkey, 
            ps_suppkey, 
            t.agg1 as agg1, 
            t.sum_total as agg3, 
            t.max_total as agg4, 
            t.min_total as agg5, 
            t.count_all as agg6, 
            cast(
                sum(
                IFNULL(ps_suppkey, 0) * IFNULL(ps_partkey, 0)
                ) as decimal(28, 8)
            ) as agg2 
            from 
            (
                select 
                l_orderkey, 
                l_partkey, 
                l_suppkey, 
                o_orderkey, 
                o_custkey, 
                cast(
                    sum(
                    IFNULL(o_orderkey, 0) * IFNULL(o_custkey, 0)
                    ) as decimal(28, 8)
                ) as agg1, 
                sum(o_totalprice) as sum_total, 
                max(o_totalprice) as max_total, 
                min(o_totalprice) as min_total, 
                count(*) as count_all, 
                bitmap_union(
                    to_bitmap(
                    case when o_shippriority > 1 
                    and o_orderkey IN (1, 3) then o_custkey else null end
                    )
                ) cnt_1, 
                bitmap_union(
                    to_bitmap(
                    case when o_shippriority > 2 
                    and o_orderkey IN (2) then o_custkey else null end
                    )
                ) as cnt_2 
                from 
                lineitem_1 
                inner join orders_1 on lineitem_1.l_orderkey = orders_1.o_orderkey 
                where 
                lineitem_1.l_shipdate >= "2023-10-17" 
                group by 
                l_orderkey, 
                l_partkey, 
                l_suppkey, 
                o_orderkey, 
                o_custkey
            ) as t 
            inner join partsupp_1 on t.l_partkey = partsupp_1.ps_partkey 
            and t.l_suppkey = partsupp_1.ps_suppkey 
            where 
            partsupp_1.ps_suppkey > 1 
            group by 
            l_orderkey, 
            l_partkey, 
            l_suppkey, 
            o_orderkey, 
            o_custkey, 
            ps_partkey, 
            ps_suppkey, 
            agg1, 
            agg3, 
            agg4, 
            agg5, 
            agg6
        ) as t2 on t1.l_orderkey = t2.l_orderkey 
        where 
        t1.l_orderkey > 1 
        group by 
        t1.l_orderkey, 
        t2.l_partkey, 
        t1.l_suppkey, 
        t2.o_orderkey, 
        t1.o_custkey, 
        t2.ps_partkey, 
        t1.ps_suppkey, 
        t2.agg1, 
        t1.agg2, 
        t2.agg3, 
        t1.agg4, 
        t2.agg5, 
        t1.agg6
        order by 1, 2, 3, 4, 5, 6,7, 8, 9;
        """
    order_qt_shuffle "${query}"
}