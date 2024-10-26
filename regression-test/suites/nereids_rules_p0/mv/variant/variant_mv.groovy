package mv.variant
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

suite("variant_mv") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "SET enable_agg_state = true"

    sql """
    drop table if exists github_events1
    """

    sql """
    CREATE TABLE IF NOT EXISTS github_events1 (
        id BIGINT NOT NULL,
        type VARCHAR(30) NULL,
        actor VARIANT NULL,
        repo VARIANT NULL,
        payload VARIANT NULL,
        public BOOLEAN NULL,
        created_at DATETIME NULL,
        INDEX idx_payload (`payload`) USING INVERTED PROPERTIES("parser" = "english") COMMENT 'inverted index for payload'
    )
    DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(id) BUCKETS 10
    properties("replication_num" = "1");
    """

    sql """
    drop table if exists github_events2
    """

    sql """
    CREATE TABLE IF NOT EXISTS github_events2 (
        id BIGINT NOT NULL,
        type VARCHAR(30) NULL,
        actor VARIANT NULL,
        repo VARIANT NULL,
        payload VARIANT NULL,
        public BOOLEAN NULL,
        created_at DATETIME NULL,
        INDEX idx_payload (`payload`) USING INVERTED PROPERTIES("parser" = "english") COMMENT 'inverted index for payload'
    )
    DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(id) BUCKETS 10
    properties("replication_num" = "1");
    """

    streamLoad {
        table "github_events1"
        set 'columns', 'id, type, actor, repo, payload, public, created_at'
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'strip_outer_array', 'false'
        file context.config.dataPath + "/nereids_rules_p0/mv/variant/variant_data.json"
        time 10000
    }

    streamLoad {
        table "github_events2"
        set 'read_json_by_line', 'true'
        set 'format', 'json'
        set 'columns', 'id, type, actor, repo, payload, public, created_at'
        set 'strip_outer_array', 'false'
        file context.config.dataPath + "/nereids_rules_p0/mv/variant/variant_data.json"
        time 10000 // limit inflight 10s
    }

    sql "sync"
    sql """analyze table github_events1 with sync;"""
    sql """analyze table github_events2 with sync;"""

    // variant appear in where both slot and in expression
    def mv1_0 = """
    SELECT
    id,
    type,
    actor,
    payload,
    payload['issue']
    FROM github_events1
    where actor['id'] > 34259289 and cast(actor['id'] as int) + cast(repo['id'] as int) > 80000000;
    """
    def query1_0 = """
    SELECT
    id,
    type,
    floor(cast(actor['id'] as int) + 100.5),
    actor['display_login'],
    payload['issue']['href']
    FROM github_events1
    where actor['id'] > 64259289 and cast(actor['id'] as int) + cast(repo['id'] as int) > 80000000;
    """
    order_qt_query1_0_before "${query1_0}"
    async_mv_rewrite_success(db, mv1_0, query1_0, "mv1_0")
    order_qt_query1_0_after "${query1_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_0"""


    def mv1_1 = """
    SELECT
    id,
    type,
    actor,
    payload,
    payload['pull_request']
    FROM github_events1;
    """
    def query1_1 = """
    SELECT
    id,
    type,
    floor(cast(actor['id'] as int) + 100.5),
    actor['display_login'],
    payload['pull_request']['id']
    FROM github_events1
    """
    order_qt_query1_1_before "${query1_1}"
    async_mv_rewrite_success_without_check_chosen(db, mv1_1, query1_1, "mv1_1")
    order_qt_query1_1_after "${query1_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_1"""

    // floor expression is different, should fail
    def mv1_2 = """
    SELECT
    id,
    type,
    floor(cast(actor['id'] as int) + 200.5),
    payload,
    payload['pull_request']
    FROM github_events1;
    """
    def query1_2 = """
    SELECT
    id,
    type,
    floor(cast(actor['id'] as int) + 100.5),
    payload['pull_request']['id']
    FROM github_events1;
    """
    order_qt_query1_2_before "${query1_2}"
    // the expression floor(cast(actor['id'] as int) + 200.5) in query and view is different
    async_mv_rewrite_fail(db, mv1_2, query1_2, "mv1_2")
    order_qt_query1_2_after "${query1_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_2"""

    def mv1_3 = """
    SELECT
    id,
    type,
    actor['type'],
    payload,
    payload['issue']
    FROM github_events1
    where actor['id'] > 34259289 and cast(actor['id'] as int) + cast(repo['id'] as int) > 80000000;
    """
    def query1_3 = """
    SELECT
    id,
    type,
    floor(cast(actor['id'] as int) + 100.5),
    actor['display_login'],
    payload['issue']['href']
    FROM github_events1
    where actor['id'] > 64259289 and cast(actor['id'] as int) + cast(repo['id'] as int) > 80000000;
    """
    order_qt_query1_3_before "${query1_3}"
    // the query repo['id'] expression in compensatory filter is not in mv
    async_mv_rewrite_fail(db, mv1_3, query1_3, "mv1_3")
    order_qt_query1_3_after "${query1_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_3"""


    def mv1_4 = """
    SELECT
    id,
    type,
    actor,
    repo['id'],
    payload,
    payload['issue']
    FROM github_events1
    where actor['id'] > 34259289 and cast(actor['id'] as int) + cast(repo['id'] as int) > 80000000;
    """
    def query1_4 = """
    SELECT
    id,
    type,
    floor(cast(actor['id'] as int) + 100.5),
    actor['display_login'],
    payload['issue']['href']
    FROM github_events1
    where actor['id'] > 64259289 and cast(actor['id'] as int) + cast(repo['id'] as int) > 80000000;
    """
    order_qt_query1_4_before "${query1_4}"
    async_mv_rewrite_success_without_check_chosen(db, mv1_4, query1_4, "mv1_4")
    order_qt_query1_4_after "${query1_4}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_4"""


    // variant appear in agg both slot and in expression
    // not roll up
    def mv2_0 = """
    SELECT
    id,
    type,
    cast(repo['name'] as varchar(100)),
    count(*),
    max(floor(cast(actor['id'] as int) + 100.5))
    FROM github_events1
    where actor['id'] > 34259289 and cast(actor['id'] as int) + cast(repo['id'] as int) > 80000000
    group by 
    id,
    type,
    cast(repo['name'] as varchar(100));
    """
    def query2_0 = """
    SELECT
    id,
    type,
    cast(repo['name'] as varchar(100)),
    count(*),
    max(floor(cast(actor['id'] as int) + 100.5))
    FROM github_events1
    where actor['id'] > 34259289 and cast(actor['id'] as int) + cast(repo['id'] as int) > 80000000
    group by 
    id,
    type,
    cast(repo['name'] as varchar(100));
    """
    order_qt_query2_0_before "${query2_0}"
    async_mv_rewrite_success_without_check_chosen(db, mv2_0, query2_0, "mv2_0")
    order_qt_query2_0_after "${query2_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_0"""


    // roll up
    def mv2_1 = """
    SELECT
    id,
    type,
    cast(repo['name'] as varchar(100)),
    count(*),
    max(floor(cast(actor['id'] as int) + 100.5))
    FROM github_events1
    where actor['id'] > 34259289 and cast(actor['id'] as int) + cast(repo['id'] as int) > 80000000
    group by 
    id,
    type,
    cast(repo['name'] as varchar(100));
    """
    def query2_1 = """
    SELECT
    id,
    cast(repo['name'] as varchar(100)),
    count(*),
    max(floor(cast(actor['id'] as int) + 100.5))
    FROM github_events1
    where actor['id'] > 34259289 and cast(actor['id'] as int) + cast(repo['id'] as int) > 80000000
    group by 
    id,
    cast(repo['name'] as varchar(100));
    """
    order_qt_query2_1_before "${query2_1}"
    async_mv_rewrite_success_without_check_chosen(db, mv2_1, query2_1, "mv2_1")
    order_qt_query2_1_after "${query2_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_1"""


    def mv2_2 = """
    SELECT
    id,
    type,
    cast(repo as varchar(100)),
    count(*),
    max(floor(cast(actor['id'] as int) + 100.5))
    FROM github_events1
    where actor['id'] > 34259289 and cast(actor['id'] as int) + cast(repo['id'] as int) > 80000000
    group by 
    id,
    type,
    cast(repo as varchar(100));
    """
    def query2_2 = """
    SELECT
    id,
    type,
    cast(repo['name'] as varchar(100)),
    count(*),
    max(floor(cast(actor['id'] as int) + 100.5))
    FROM github_events1
    where actor['id'] > 34259289 and cast(actor['id'] as int) + cast(repo['id'] as int) > 80000000
    group by 
    id,
    type,
    cast(repo['name'] as varchar(100));
    """
    order_qt_query2_2_before "${query2_2}"
    // cast(repo) expression is different, should fail
    async_mv_rewrite_fail(db, mv2_2, query2_2, "mv2_2")
    order_qt_query2_2_after "${query2_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_2"""


    def mv2_3 = """
    SELECT
    id,
    type,
    cast(repo['name'] as varchar(100)),
    count(*),
    max(floor(cast(actor['id'] as int) + 100.5))
    FROM github_events1
    where actor['id'] > 34259289 and cast(actor['id'] as int) + cast(repo['id'] as int) > 80000000
    group by 
    id,
    type,
    cast(actor['id'] as int),
    cast(repo['name'] as varchar(100));
    """
    def query2_3 = """
    SELECT
    id,
    type,
    cast(repo['name'] as varchar(100)),
    count(*),
    max(floor(cast(actor['id'] as int) + 100.5))
    FROM github_events1
    where cast(actor['id'] as int) > 34259300 and cast(actor['id'] as int) + cast(repo['id'] as int) > 80000000
    group by 
    id,
    type,
    cast(repo['name'] as varchar(100));
    """
    order_qt_query2_3_before "${query2_3}"
    // compensatory filter (actor['id'] is not in mv output should fail
    async_mv_rewrite_fail(db, mv2_3, query2_3, "mv2_3")
    order_qt_query2_3_after "${query2_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_3"""


    def mv2_4 = """
    SELECT
    id,
    type,
    cast(actor['id'] as int),
    cast(repo['name'] as varchar(100)),
    count(*),
    max(floor(cast(actor['id'] as int) + 100.5))
    FROM github_events1
    where actor['id'] > 34259289 and cast(actor['id'] as int) + cast(repo['id'] as int) > 80000000
    group by 
    id,
    type,
    cast(repo['name'] as varchar(100)),
    cast(actor['id'] as int);
    """
    def query2_4 = """
    SELECT
    id,
    type,
    cast(repo['name'] as varchar(100)),
    count(*),
    max(floor(cast(actor['id'] as int) + 100.5))
    FROM github_events1
    where cast(actor['id'] as int) > 34259300 and cast(actor['id'] as int) + cast(repo['id'] as int) > 80000000
    group by 
    id,
    type,
    cast(repo['name'] as varchar(100));
    """
    order_qt_query2_4_before "${query2_4}"
    async_mv_rewrite_success_without_check_chosen(db, mv2_4, query2_4, "mv2_4")
    order_qt_query2_4_after "${query2_4}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_4"""


    // variant appear in join both slot and in expression
    def mv3_0 = """
    SELECT
    g1.id,
    g2.type,
    g1.actor,
    g2.payload,
    g1.payload['issue']
    FROM github_events1 g1
    left join github_events2 g2 on g1.id = g2.id
    where g2.actor['id'] > 34259289 and cast(g1.actor['id'] as int) + cast(g2.repo['id'] as int) > 80000000;
    """
    def query3_0 = """
    SELECT
    g1.id,
    g2.type,
    floor(cast(g1.actor['id'] as int) + 100.5),
    g1.actor['display_login'],
    g2.payload['issue']['href']
    FROM github_events1 g1
    left join github_events2 g2 on g1.id = g2.id
    where g2.actor['id'] > 34259289 and cast(g1.actor['id'] as int) + cast(g2.repo['id'] as int) > 80000000;
    """
    order_qt_query3_0_before "${query3_0}"
    // condition in join other conjuects is not supported now, suppport later
//    async_mv_rewrite_success(db, mv3_0, query3_0, "mv3_0")
    order_qt_query3_0_after "${query3_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_0"""


    def mv3_5 = """
    SELECT
    g1.id,
    g2.type,
    g1.actor,
    g2.actor as actor_g2,
    g2.payload,
    g1.payload['issue']
    FROM github_events1 g1
    left join github_events2 g2 on g1.id = g2.id
    where g2.actor['id'] > 34259289;
    """
    def query3_5 = """
    SELECT
    g1.id,
    g2.type,
    floor(cast(g1.actor['id'] as int) + 100.5),
    g1.actor['display_login'],
    g2.payload['issue']['href']
    FROM github_events1 g1
    left join github_events2 g2 on g1.id = g2.id
    where g2.actor['id'] > 34259300;
    """
    order_qt_query3_5_before "${query3_5}"
    async_mv_rewrite_success_without_check_chosen(db, mv3_5, query3_5, "mv3_5")
    order_qt_query3_5_after "${query3_5}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_5"""


    def mv3_1 = """
    SELECT
    g1.id,
    g2.type,
    g1.actor,
    g2.payload,
    g1.payload['pull_request']
    FROM github_events1 g1
    left join github_events2 g2 on g1.id = g2.id;
    """
    def query3_1 = """
    SELECT
    g1.id,
    g2.type,
    floor(cast(g1.actor['id'] as int) + 100.5),
    g1.actor['display_login'],
    g1.payload['pull_request']['id']
    FROM github_events1 g1
    left join github_events2 g2 on g1.id = g2.id;
    """
    order_qt_query3_1_before "${query3_1}"
    async_mv_rewrite_success_without_check_chosen(db, mv3_1, query3_1, "mv3_1")
    order_qt_query3_1_after "${query3_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_1"""


    def mv3_2 = """
    SELECT
    g1.id,
    g2.type,
    floor(cast(g1.actor['id'] as int) + 200.5),
    g1.payload['pull_request']
    FROM github_events1 g1
    left join github_events2 g2 on g1.id = g2.id;
    """
    def query3_2 = """
    SELECT
    g1.id,
    g2.type,
    floor(cast(g1.actor['id'] as int) + 100.5),
    g1.payload['pull_request']['id']
    FROM github_events1 g1
    left join github_events2 g2 on g1.id = g2.id;
    """
    order_qt_query3_2_before "${query3_2}"
    // floor expression is different, should fail
    async_mv_rewrite_fail(db, mv3_2, query3_2, "mv3_2")
    order_qt_query3_2_after "${query3_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_2"""


    def mv3_3 = """
    SELECT
    g1.id,
    g2.type,
    g1.actor,
    g2.payload,
    g1.payload['issue']
    FROM github_events1 g1
    left join github_events2 g2 on g1.id = g2.id
    where g2.actor['id'] > 34259289 and cast(g1.actor['id'] as int) + cast(g2.repo['id'] as int) > 80000000;
    """
    def query3_3 = """
    SELECT
    g1.id,
    g2.type,
    floor(cast(g1.actor['id'] as int) + 100.5),
    g1.actor['display_login'],
    g2.payload['issue']['href']
    FROM github_events1 g1
    left join github_events2 g2 on g1.id = g2.id
    where g2.actor['id'] > 34259300 and cast(g1.actor['id'] as int) + cast(g2.repo['id'] as int) > 80000000;
    """
    order_qt_query3_3_before "${query3_3}"
    // the query g2.actor['id'] expression in compensatory filter is not in mv
    async_mv_rewrite_fail(db, mv3_3, query3_3, "mv3_3")
    order_qt_query3_3_after "${query3_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_3"""


    def mv3_4 = """
    SELECT
    g1.id,
    g2.type,
    g1.actor,
    g2.payload,
    g2.actor['id'],
    g1.payload['issue']
    FROM github_events1 g1
    left join github_events2 g2 on g1.id = g2.id
    where g2.actor['id'] > 34259289 and cast(g1.actor['id'] as int) + cast(g2.repo['id'] as int) > 80000000;
    """
    def query3_4 = """
    SELECT
    g1.id,
    g2.type,
    floor(cast(g1.actor['id'] as int) + 100.5),
    g1.actor['display_login'],
    g2.payload['issue']['href']
    FROM github_events1 g1
    left join github_events2 g2 on g1.id = g2.id
    where g2.actor['id'] > 34259300 and cast(g1.actor['id'] as int) + cast(g2.repo['id'] as int) > 80000000;
    """
    order_qt_query3_4_before "${query3_4}"
    // condition in join other conjuects is not supported now, suppport later
//    async_mv_rewrite_success(db, mv3_4, query3_4, "mv3_4")
    order_qt_query3_4_after "${query3_4}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_4"""


    def mv3_6 = """
    SELECT
    g1.id,
    g2.type,
    g1.actor,
    g2.payload,
    g1.actor['id'],
    g1.payload['issue']
    FROM github_events1 g1
    left join github_events2 g2 on g1.id = g2.id
    where g2.actor['id'] > 34259289;
    """
    def query3_6 = """
    SELECT  /*+SET_VAR(batch_size=4064,broker_load_batch_size=16352,disable_streaming_preaggregations=false,enable_distinct_streaming_aggregation=true,parallel_fragment_exec_instance_num=3,parallel_pipeline_task_num=0,profile_level=1,enable_pipeline_engine=true,enable_parallel_scan=true,parallel_scan_max_scanners_count=32,parallel_scan_min_rows_per_scanner=64,enable_fold_constant_by_be=true,enable_rewrite_element_at_to_slot=true,runtime_filter_type=1,enable_parallel_result_sink=false,enable_nereids_planner=true,rewrite_or_to_in_predicate_threshold=100000,enable_function_pushdown=false,enable_common_expr_pushdown=false,enable_local_exchange=true,partitioned_hash_join_rows_threshold=8,partitioned_hash_agg_rows_threshold=8,partition_pruning_expand_threshold=10,enable_share_hash_table_for_broadcast_join=true,enable_two_phase_read_opt=true,enable_common_expr_pushdown_for_inverted_index=false,enable_delete_sub_predicate_v2=false,min_revocable_mem=33554432,fetch_remote_schema_timeout_seconds=120,max_fetch_remote_schema_tablet_count=512,enable_join_spill=false,enable_sort_spill=false,enable_agg_spill=false,enable_force_spill=false,data_queue_max_blocks=1,spill_streaming_agg_mem_limit=268435456,external_agg_partition_bits=5) */
    g1.id,
    g2.type,
    floor(cast(g1.actor['id'] as int) + 100.5),
    g1.actor['display_login'],
    g2.payload['issue']['href']
    FROM github_events1 g1
    left join github_events2 g2 on g1.id = g2.id
    where g2.actor['id'] > 34259300;
    """
    order_qt_query3_6_before "${query3_6}"
    // should success, should enable in future
    async_mv_rewrite_fail(db, mv3_6, query3_6, "mv3_6")
    order_qt_query3_6_after "${query3_6}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_6"""
}
