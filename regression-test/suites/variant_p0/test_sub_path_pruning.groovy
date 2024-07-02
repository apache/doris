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

suite("variant_sub_path_pruning", "variant_type"){

    sql "DROP TABLE IF EXISTS pruning_test"

    sql """
        CREATE TABLE `pruning_test` (
          `id` INT NULL,
          `dt` VARIANT NULL
        ) 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id)
        PROPERTIES("replication_num"="1")
    """

    sql """
        insert into pruning_test values(1, '{"a":{"b":{"c":{"d":{"e":11}}},"c":{"d":{"e":12}},"d":{"e":13},"e":14},"b":{"c":{"d":{"e":21}},"d":{"e":22},"e":23},"c":{"d":{"e":31},"e":32},"d":{"e":4},"e":5}')
    """

    sql "sync"

    // project

    // simple case
    order_qt_sql """select dt['a'] from pruning_test order by id limit 100;"""

    // one level
    order_qt_sql """select id, c1['a'] from (select id, dt as c1 from pruning_test order by id limit 100) tmp;"""
    order_qt_sql """select id, c1 from (select id, dt['a'] as c1 from pruning_test order by id limit 100) tmp;"""

    // two level, one level sub path
    order_qt_sql """select id, c2 as c3 from (select id, c1 as c2 from (select id, dt['a'] as c1 from pruning_test order by id limit 100) tmp order by id limit 100) tmp;"""
    order_qt_sql """select id, c2 as c3 from (select id, c1['a'] as c2 from (select id, dt as c1 from pruning_test order by id limit 100) tmp order by id limit 100) tmp;"""
    order_qt_sql """select id, c2['a'] as c3 from (select id, c1 as c2 from (select id, dt as c1 from pruning_test order by id limit 100) tmp order by id limit 100) tmp;"""

    // two level, two level sub paths
    order_qt_sql """select id, c2 as c3 from (select id, c1['b'] as c2 from (select id, dt['a'] as c1 from pruning_test order by id limit 100) tmp order by id limit 100) tmp;"""
    order_qt_sql """select id, c2['b'] as c3 from (select id, c1 as c2 from (select id, dt['a'] as c1 from pruning_test order by id limit 100) tmp order by id limit 100) tmp;"""
    order_qt_sql """select id, c2['b'] as c3 from (select id, c1['a'] as c2 from (select id, dt as c1 from pruning_test order by id limit 100) tmp order by id limit 100) tmp;"""

    // two level, two sub path on diff slot
    order_qt_sql """select id, c2 as c3, d2 as d3 from (select id, c1 as c2, d1 as d2 from (select id, dt['a'] as c1, dt['b'] as d1 from pruning_test order by id limit 100) tmp order by id limit 100) tmp;"""
    order_qt_sql """select id, c2 as c3, d2 as d3 from (select id, c1 as c2, d1['b'] as d2 from (select id, dt['a'] as c1, dt as d1 from pruning_test order by id limit 100) tmp order by id limit 100) tmp;"""
    order_qt_sql """select id, c2 as c3, d2['b'] as d3 from (select id, c1 as c2, d1 as d2 from (select id, dt['a'] as c1, dt as d1 from pruning_test order by id limit 100) tmp order by id limit 100) tmp;"""

    order_qt_sql """select id, c2 as c3, d2 as d3 from (select id, c1['a'] as c2, d1['b'] as d2 from (select id, dt as c1, dt as d1 from pruning_test order by id limit 100) tmp order by id limit 100) tmp;"""
    order_qt_sql """select id, c2['a'] as c3, d2 as d3 from (select id, c1 as c2, d1['b'] as d2 from (select id, dt as c1, dt as d1 from pruning_test order by id limit 100) tmp order by id limit 100) tmp;"""

    order_qt_sql """select id, c2['a'] as c3, d2['b'] as d3 from (select id, c1 as c2, d1 as d2 from (select id, dt as c1, dt as d1 from pruning_test order by id limit 100) tmp order by id limit 100) tmp;"""

    // two level, two level sub path on diff slot
    order_qt_sql """select id, c2 as c3, d2 as d3 from (select id, c1['b'] as c2, d1 as d2 from (select id, dt['a'] as c1, dt['b'] as d1 from pruning_test order by id limit 100) tmp order by id limit 100) tmp;"""
    order_qt_sql """select id, c2['b'] as c3, d2 as d3 from (select id, c1 as c2, d1 as d2 from (select id, dt['a'] as c1, dt['b'] as d1 from pruning_test order by id limit 100) tmp order by id limit 100) tmp;"""

    order_qt_sql """select id, c2 as c3, d2 as d3 from (select id, c1['b'] as c2, d1['b'] as d2 from (select id, dt['a'] as c1, dt['b'] as d1 from pruning_test order by id limit 100) tmp order by id limit 100) tmp;"""
    order_qt_sql """select id, c2 as c3, d2['b'] as d3 from (select id, c1['b'] as c2, d1 as d2 from (select id, dt['a'] as c1, dt['b'] as d1 from pruning_test order by id limit 100) tmp order by id limit 100) tmp;"""

    order_qt_sql """select id, c2 as c3 from (select id, c1 as c2 from (select id, dt['a']['b'] as c1 from pruning_test order by id limit 100) tmp order by id limit 100) tmp;"""
    order_qt_sql """select id, c2 as c3 from (select id, c1['a']['b'] as c2 from (select id, dt as c1 from pruning_test order by id limit 100) tmp order by id limit 100) tmp;"""
    order_qt_sql """select id, c2['a']['b'] as c3 from (select id, c1 as c2 from (select id, dt as c1 from pruning_test order by id limit 100) tmp order by id limit 100) tmp;"""

    // two level, three level sub paths
    order_qt_sql """select id, c2 as c3 from (select id, c1['c'] as c2 from (select id, dt['a']['b'] as c1 from pruning_test order by id limit 100) tmp order by id limit 100) tmp;"""
    order_qt_sql """select id, c2['c'] as c3 from (select id, c1['a']['b'] as c2 from (select id, dt as c1 from pruning_test order by id limit 100) tmp order by id limit 100) tmp;"""


    // filter
    order_qt_sql """select id, dt['e'] as c1 from pruning_test where dt['e'] > 1;"""
    order_qt_sql """select id, dt['a'] as c1 from pruning_test where dt['e'] > 1;"""
    order_qt_sql """select id, dt['a'] as c1, dt['e'] as c2 from pruning_test where dt['e'] > 1;"""
    order_qt_sql """select id, dt['a'] as c1, dt['e'] as c2 from pruning_test where dt['d']['e'] > 1;"""
    order_qt_sql """select id, c1, c2 from (select id, dt['a'] as c1, dt['e'] as c2 from pruning_test)tmp where c2 > 1;"""


    // cte

    sql """set inline_cte_referenced_threshold = 0;"""

    // one level, sub path on consumer
    order_qt_sql """with cte1 as (select id, dt as c1 from pruning_test) select id, c1['a'] as c2 from cte1;"""
    order_qt_sql """with cte1 as (select id, dt as c1 from pruning_test) select id, c1['a']['b'] as c2 from cte1;"""

    // one level sub path on producer
    order_qt_sql """with cte1 as (select id, dt['a'] as c1 from pruning_test) select id, c1 as c2 from cte1;"""
    order_qt_sql """with cte1 as (select id, dt['a']['b'] as c1 from pruning_test) select id, c1 as c2 from cte1;"""

    // two level, sub path on both producer and consumer
    order_qt_sql """with cte1 as (select id, dt['a'] as c1 from pruning_test) select id, c1['b'] as c2 from cte1;"""
    order_qt_sql """with cte1 as (select id, dt['a']['b'] as c1 from pruning_test) select id, c1['c'] as c2 from cte1;"""
    order_qt_sql """with cte1 as (select id, dt['a'] as c1 from pruning_test) select id, c1['b']['c'] as c2 from cte1;"""

    // two level, sub path on both producer and consumer on diff slots
    order_qt_sql """with cte1 as (select id, dt['a'] as c1 from pruning_test) select id, c1['b'] as c2, c1['c'] as c3 from cte1;"""
    order_qt_sql """with cte1 as (select id, dt['a'] as c1, dt['b'] as c2 from pruning_test) select id, c1['b'] as c3 from cte1;"""
    order_qt_sql """with cte1 as (select id, dt['a'] as c1, dt['b'] as c2 from pruning_test) select id, c1['b'] as c3, c2['a'] as c4 from cte1;"""
    order_qt_sql """with cte1 as (select id, dt['a'] as c1, dt['b'] as c2 from pruning_test) select id, c1['b'] as c3, c1['a'] as c4 from cte1;"""

    // more consumers
    order_qt_sql """with cte1 as (select id, dt as c1 from pruning_test) select * from (select id, c1['a'] as c2 from cte1) v1 cross join (select id, c1['a'] as c2 from cte1) v2;"""
    order_qt_sql """with cte1 as (select id, dt as c1 from pruning_test) select * from (select id, c1['a'] as c2 from cte1) v1 cross join (select id, c1['b'] as c2 from cte1) v2;"""
    order_qt_sql """with cte1 as (select id, dt as c1 from pruning_test) select * from (select id, c1['a']['b'] as c2 from cte1) v1 cross join (select id, c1 as c2 from cte1) v2;"""
    order_qt_sql """with cte1 as (select id, dt as c1 from pruning_test) select * from (select id, c1['a'] as c2, c1['b'] as c3 from cte1) v1 cross join (select id, c1 as c2 from cte1) v2;"""

    // more producers
    order_qt_sql """with cte1 as (select id, dt as c1 from pruning_test), cte2 as (select id, dt as c1 from pruning_test) select cte1.c1['a'], cte2.c1['b'] from cte1, cte2;"""
    order_qt_sql """with cte1 as (select id, dt['a'] as c1 from pruning_test), cte2 as (select id, dt['a'] as c1 from pruning_test) select cte1.c1['b'], cte2.c1['b'] from cte1, cte2;"""

    // two level cte
    order_qt_sql """with cte1 as (select id, dt as c1 from pruning_test), cte2 as (select id, c1 as c2 from cte1) select c2['a'] from cte2;"""
    order_qt_sql """with cte1 as (select id, dt as c1 from pruning_test), cte2 as (select id, c1['a'] as c2 from cte1) select c2 from cte2;"""
    order_qt_sql """with cte1 as (select id, dt['a'] as c1 from pruning_test), cte2 as (select id, c1 as c2 from cte1) select c2 from cte2;"""

    order_qt_sql """with cte1 as (select id, dt['a'] as c1 from pruning_test), cte2 as (select id, c1['b'] as c2, c1['a'] as c3 from cte1) select id, c2['c'] as c4 from cte2;"""
    order_qt_sql """with cte1 as (select id, dt['a']['b'] as c1 from pruning_test), cte2 as (select id, c1 as c2, c1['a'] as c3 from cte1) select id, c2['c'] as c4 from cte2;"""
    order_qt_sql """with cte1 as (select id, dt['a']['b'] as c1 from pruning_test), cte2 as (select id, c1 as c2, c1['a'] as c3 from cte1) select id, c2['c'] as c4, c3['b'] as c5 from cte2;"""

    // two level cte + one level cte
    order_qt_sql """with cte1 as (select id, dt['a'] as c1 from pruning_test), cte2 as (select id, c1['b'] as c2, c1['a'] as c3 from cte1) select cte2.id, cte2.c2['c'], cte1.c1['b'] from cte1, cte2;"""


    // set operation
    // distinct could not push down, only push down union all

    // two children
    order_qt_sql """
        select  /*+SET_VAR(batch_size=50,disable_streaming_preaggregations=false,enable_distinct_streaming_aggregation=true,parallel_fragment_exec_instance_num=6,parallel_pipeline_task_num=2,profile_level=1,enable_pipeline_engine=true,enable_parallel_scan=false,parallel_scan_max_scanners_count=16,parallel_scan_min_rows_per_scanner=128,enable_fold_constant_by_be=false,enable_rewrite_element_at_to_slot=true,runtime_filter_type=2,enable_nereids_planner=true,rewrite_or_to_in_predicate_threshold=2,enable_function_pushdown=true,enable_common_expr_pushdown=true,enable_local_exchange=false,partitioned_hash_join_rows_threshold=1048576,partitioned_hash_agg_rows_threshold=8,partition_pruning_expand_threshold=10,enable_share_hash_table_for_broadcast_join=false,enable_two_phase_read_opt=true,enable_delete_sub_predicate_v2=true,min_revocable_mem=33554432,fetch_remote_schema_timeout_seconds=120,enable_join_spill=false,enable_sort_spill=false,enable_agg_spill=false,enable_force_spill=false,data_queue_max_blocks=1,external_agg_bytes_threshold=0,spill_streaming_agg_mem_limit=268435456,external_agg_partition_bits=5) */ dt['a'] as c1 from pruning_test union all select dt['a'] as c1 from pruning_test;
        """
    order_qt_sql """select c1['a'] from (select dt as c1 from pruning_test union all select dt as c1 from pruning_test) v1;"""
    order_qt_sql """select c1['b'] from (select dt['a'] as c1 from pruning_test union all select dt['a'] as c1 from pruning_test) v1;"""
    order_qt_sql """select c1['b'] from (select dt['a'] as c1 from pruning_test union all select dt as c1 from pruning_test) v1;"""
    order_qt_sql """select c1['c']['d'] from (select dt['a']['b'] as c1 from pruning_test union all select dt as c1 from pruning_test) v1;"""
    order_qt_sql """select c1['c']['d'] from (select dt['a']['b'] as c1 from pruning_test union all select dt['a'] as c1 from pruning_test) v1;"""

    // two children with diff sub path in children
    order_qt_sql """select c1['d'] from (select dt['c'] as c1 from pruning_test where cast(dt['a']['b']['c']['d']['e'] as int) = 11 union all select dt['b'] as c1 from pruning_test where cast(dt['a']['c']['d']['e'] as int) = 12) tmp;"""
    order_qt_sql """select c1['d'] from (select dt['c'] as c1 from pruning_test where cast(dt['c']['d']['e'] as int) = 31 union all select dt['b'] as c1 from pruning_test where cast(dt['c']['e'] as int) = 32) tmp;"""
    order_qt_sql """select c1['d'], c1['e'] from (select dt['c'] as c1 from pruning_test where cast(dt['c']['d']['e'] as int) = 31 union all select dt['b'] as c1 from pruning_test where cast(dt['c']['e'] as int) = 32) tmp;"""

    // three children
    order_qt_sql """select dt['a'] as c1 from pruning_test union all select dt['a'] as c1 from pruning_test union all select dt['a'] as c1 from pruning_test;"""
    order_qt_sql """select c1['a'] from (select dt as c1 from pruning_test union all select dt as c1 from pruning_test union all select dt as c1 from pruning_test) v1;"""
    order_qt_sql """select c1['b'] from (select dt['a'] as c1 from pruning_test union all select dt['a'] as c1 from pruning_test union all select dt['a'] as c1 from pruning_test) v1;"""
    order_qt_sql """select c1['d'] from (select dt['a'] as c1 from pruning_test union all select dt['b'] as c1 from pruning_test union all select dt['c'] as c1 from pruning_test) v1;"""
    order_qt_sql """select c1['d'] from (select dt['a'] as c1 from pruning_test union all select dt['b'] as c1 from pruning_test union all select dt['b'] as c1 from pruning_test) v1;"""
    order_qt_sql """select c1['c']['d'] from (select dt['a']['b'] as c1 from pruning_test union all select dt['a'] as c1 from pruning_test union all select dt as c1 from pruning_test) v1;"""

    // one table + one const list
    order_qt_sql """select id, cast(c1['a'] as text) from (select cast('{"a":1}' as variant) as c1, 1 as id union all select dt as c1, id from pruning_test) tmp order by id limit 100;"""
    order_qt_sql """select c1['a'] from (select id, c1 from (select cast('{"a":1}' as variant) as c1, 1 as id union all select dt as c1, id from pruning_test) tmp order by id limit 100) tmp;"""
    order_qt_sql """select c2['b'] from (select id, cast(c1['a'] as text) as c2 from (select cast('{"a":{"b":1}}' as variant) as c1, 1 as id union all select dt as c1, id from pruning_test) tmp order by id limit 100) tmp;"""
    // order_qt_sql """select c2['a']['b'] from (select id, c1 as c2 from (select cast('1' as variant) as c1, 1 as id union all select dt as c1, id from pruning_test) tmp order by id limit 100) tmp;"""
    order_qt_sql """select id, cast(c1['c'] as text) from (select cast('{"c":1}' as variant) as c1, 1 as id union all select dt['a']['b'] as c1, id from pruning_test) tmp order by 1, 2 limit 100;"""
    order_qt_sql """select c1['c'] from (select id, c1 from (select cast('{"c":1}' as variant) as c1, 1 as id union all select dt['a']['b'] as c1, id from pruning_test) tmp order by id limit 100) tmp;"""
    order_qt_sql """select  cast(c2['d'] as text)  from (select id, c1['a'] as c2 from (select cast('{"c":{"d":1}}' as variant) as c1, 1 as id union all select dt['a']['b'] as c1, id from pruning_test) tmp order by id limit 100) tmp;"""
    // order_qt_sql """select c2['c']['d'] from (select id, c1 as c2 from (select cast('{"c":{"d":1}}' as variant) as c1, 1 as id union all select dt['a']['b'] as c1, id from pruning_test) tmp order by id limit 100) tmp;"""

    // two const list
    order_qt_sql """select id, cast(c1['a'] as text) from (select cast('{"a":1}' as variant) as c1, 1 as id union all select cast('{"a":1}' as variant) as c1, 2 as id) tmp order by id limit 100;"""
    order_qt_sql """select c1['a'] from (select id, c1 from (select cast('{"a":1}' as variant) as c1, 1 as id union all select cast('{"a":1}' as variant) as c1, 2 as id) tmp order by id limit 100) tmp;"""
    order_qt_sql """select cast(c2['b'] as text) from (select id, c1['a'] as c2 from (select cast('{"a":{"b":1}}' as variant) as c1, 1 as id union all select cast('{"a":{"b":1}}' as variant) as c1, 2 as id) tmp order by id limit 100) tmp;"""
    order_qt_sql """select c2['a']['b'] from (select id, c1 as c2 from (select cast('{"a":{"b":1}}' as variant) as c1, 1 as id union all select cast('{"a":{"b":1}}' as variant) as c1, 2 as id) tmp order by id limit 100) tmp;"""


    // join
    order_qt_sql """select v1.id, v2.dt['a']['b'] from pruning_test v1 join pruning_test v2 on cast(v1.dt['b']['c']['d']['e'] as int) = cast(v2.dt['a']['d']['e'] as int);"""
    order_qt_sql """select v1.id, v2.dt['a']['b'] from pruning_test v1 join pruning_test v2 on case when cast(v1.dt['b']['c']['d']['e'] as int) > cast(v2.dt['a']['d']['e'] as int) then true when cast(v1.dt['b']['c']['d']['e'] as int) < cast(v2.dt['a']['d']['e'] as int) then false else true end;"""
    order_qt_sql """select v1.id, v2.dt['a']['b'] from pruning_test v1 join pruning_test v2 on v1.id = v2.id and cast(v1.dt['b']['c']['d']['e'] as int) > cast(v2.dt['a']['d']['e'] as int);"""


    // sort / topn
    order_qt_sql """select id, dt['a']['c'] from pruning_test order by cast(dt['a']['e'] as int), cast(dt['e'] as int);"""


    // partition topn
    order_qt_sql """select * from (select id, dt['c']['d'], row_number() over(partition by id order by cast(dt['c']['e'] as int)) as c1 from pruning_test) tmp where c1 < 10;"""


    // generate
    order_qt_sql """select id, dt['a']['c'] as c1, c2 from pruning_test lateral view explode_split(cast(dt['a']['b'] as string), ':') v1 as c2 order by c2;"""


    // window
    order_qt_sql """select id, dt['c']['d'], sum(cast(dt['c']['e'] as int)) over(partition by id) as c1 from pruning_test;"""


    // aggregate
    order_qt_sql """select id, cast(dt['c']['d']['e'] as int) c1, sum(cast(dt['c']['e'] as int)) as c2 from pruning_test group by 1, 2;"""


    // variant in project / one row relation

    // variant in project
    order_qt_sql """select c1['a'] from (select id, cast('{"a":1}' as variant) as c1 from pruning_test order by id limit 100) tmp;"""
    order_qt_sql """select c1['a'] as c2, c1['b'] as c3 from (select id, cast('{"a":1, "b":2}' as variant) as c1 from pruning_test order by id limit 100) tmp;"""
    order_qt_sql """select c1['a'] from (select id, cast('{"b":{"a":1}}' as variant)["b"] as c1 from pruning_test order by id limit 100) tmp;"""
    order_qt_sql """select c1['a'] as c2, c1['b'] as c3 from (select id, cast('{"b":{"a":1, "b":2}}' as variant)["b"] as c1 from pruning_test order by id limit 100) tmp;"""

    // varaint in one row relation
    order_qt_sql """select c1['a'] from (select 1 as id, cast('{"a":1}' as variant) as c1 order by id limit 100) tmp;"""
    order_qt_sql """select c1['a'] as c2, c1['b'] as c3 from (select 1 as id, cast('{"a":1, "b":2}' as variant) as c1 order by id limit 100) tmp;"""
    order_qt_sql """select c1['a'] from (select  1 as id, cast('{"b":{"a":1}}' as variant)["b"] as c1 order by id limit 100) tmp;"""
    order_qt_sql """select c1['a'] as c2, c1['b'] as c3 from (select 1 as id, cast('{"b":{"a":1, "b":2}}' as variant)["b"] as c1 order by id limit 100) tmp;"""
}