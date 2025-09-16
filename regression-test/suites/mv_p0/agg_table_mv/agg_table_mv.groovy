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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite ("agg_table_mv") {

    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"

    sql """set enable_nereids_planner=true;"""
    sql """ DROP TABLE IF EXISTS orders_agg; """
    // this mv rewrite would not be rewritten in RBO phase, so set TRY_IN_RBO explicitly to make case stable
    sql "set pre_materialized_view_rewrite_strategy = TRY_IN_RBO"

    sql """
        CREATE TABLE `orders_agg` (
          `o_orderkey` BIGINT not NULL,
          `o_custkey` INT not NULL,
          `o_orderdate` DATE not null,
          `o_orderstatus` VARCHAR(1) replace,
          `o_totalprice` DECIMAL(15, 2) sum,
          `o_orderpriority` VARCHAR(15) replace,
          `o_clerk` VARCHAR(15) replace,
          `o_shippriority` INT sum,
          `o_comment` VARCHAR(79) replace
        ) ENGINE=OLAP
        aggregate KEY(`o_orderkey`, `o_custkey`, `o_orderdate`)
        COMMENT 'OLAP'
        auto partition by range (date_trunc(`o_orderdate`, 'day')) ()
        DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    );
        """

    sql """
    insert into orders_agg values 
    (2, 1, '2023-10-17', 'k', 99.5, 'a', 'b', 1, 'yy'),
    (1, 2, '2023-10-17', 'o', 109.2, 'c','d',2, 'mm'),
    (3, 3, '2023-10-19', null, 99.5, 'a', 'b', 1, 'yy'),
    (1, 2, '2023-10-20', 'o', null, 'a', 'b', 1, 'yy'),
    (2, 3, '2023-10-21', 'k', 109.2, null,'d',2, 'mm'),
    (3, 1, '2023-10-22', 'k', 99.5, 'a', null, 1, 'yy'),
    (1, 3, '2023-10-19', 'o', 99.5, 'a', 'b', null, 'yy'),
    (2, 1, '2023-10-18', 'o', 109.2, 'c','d',2, null),
    (3, 2, '2023-10-17', 'k', 99.5, 'a', 'b', 1, 'yy'),
    (4, 5, '2023-10-19', 'k', 99.5, 'a', 'b', 1, 'yy');
    """

    create_sync_mv(db, "orders_agg", "agg_mv_name_3", """
        select o_orderdatE as a_o_orderdatE, o_custkey as a_o_custkey, o_orderkey as a_o_orderkey, 
            sum(o_totalprice) as sum_total 
            from orders_agg 
            group by 
            o_orderdatE, 
            o_custkey, 
            o_orderkey;
    """)

    mv_rewrite_success("""
            select o_custkey, o_orderkey, 
            sum(o_totalprice) as sum_total 
            from orders_agg 
            group by 
            o_custkey, 
            o_orderkey 
    """, "agg_mv_name_3")
}
