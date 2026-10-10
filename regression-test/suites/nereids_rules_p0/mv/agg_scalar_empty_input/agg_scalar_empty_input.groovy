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

suite("agg_scalar_empty_input") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set pre_materialized_view_rewrite_strategy = TRY_IN_RBO"
    sql "set runtime_filter_mode=OFF"
    sql "SET ignore_shape_nodes='PhysicalDistribute'"

    sql """DROP MATERIALIZED VIEW IF EXISTS scalar_agg_empty_mv"""
    sql """DROP MATERIALIZED VIEW IF EXISTS scalar_agg_uniform_mv"""
    sql """DROP TABLE IF EXISTS scalar_agg_empty_base"""

    sql """
    CREATE TABLE scalar_agg_empty_base (k INT NOT NULL, v INT NOT NULL)
    DUPLICATE KEY(k)
    DISTRIBUTED BY HASH(k) BUCKETS 1
    PROPERTIES ("replication_num" = "1")
    """

    // 2000 rows over 100 distinct k values, so the pre-aggregated mv is much cheaper to read than
    // the base table and the rewritten plan is the one the optimizer picks. k = 9999 matches nothing,
    // and k = 1 holds v = 1, 101, ... 1901.
    sql """INSERT INTO scalar_agg_empty_base SELECT number % 100, number FROM numbers("number" = "2000")"""
    sql """ANALYZE TABLE scalar_agg_empty_base WITH SYNC"""

    def mvSql = """select k, count(*) as cnt, sum(v) as s, min(v) as mn, max(v) as mx """ +
            """from scalar_agg_empty_base group by k"""

    def qCountEmpty = """select count(*) from scalar_agg_empty_base where k = 9999"""
    def qCountMatched = """select count(*) from scalar_agg_empty_base where k = 1"""
    def qCountAll = """select count(*) from scalar_agg_empty_base"""
    def qCountProjected = """select count(*) + 1 from scalar_agg_empty_base where k = 9999"""
    def qCountDistinct = """select count(distinct v) from scalar_agg_empty_base where k = 9999"""
    def qSumEmpty = """select sum(v) from scalar_agg_empty_base where k = 9999"""
    def qSumMatched = """select sum(v) from scalar_agg_empty_base where k = 1"""
    def qSumAll = """select sum(v) from scalar_agg_empty_base"""
    def qMinEmpty = """select min(v) from scalar_agg_empty_base where k = 9999"""
    def qMinMatched = """select min(v) from scalar_agg_empty_base where k = 1"""
    def qMaxEmpty = """select max(v) from scalar_agg_empty_base where k = 9999"""
    def qMaxMatched = """select max(v) from scalar_agg_empty_base where k = 1"""

    // Reference results, computed on the base table: a scalar aggregate always emits exactly one
    // row, COUNT of an empty input is 0 while SUM/MIN/MAX of an empty input are NULL.
    sql "set enable_materialized_view_rewrite=false"
    order_qt_count_off_empty "${qCountEmpty}"
    order_qt_count_off_matched "${qCountMatched}"
    order_qt_count_off_all "${qCountAll}"
    order_qt_count_off_projected "${qCountProjected}"
    order_qt_count_off_distinct "${qCountDistinct}"
    order_qt_sum_off_empty "${qSumEmpty}"
    order_qt_sum_off_matched "${qSumMatched}"
    order_qt_sum_off_all "${qSumAll}"
    order_qt_min_off_empty "${qMinEmpty}"
    order_qt_min_off_matched "${qMinMatched}"
    order_qt_max_off_empty "${qMaxEmpty}"
    order_qt_max_off_matched "${qMaxMatched}"

    // The mv groups by k while these queries are scalar, so the mv dimensions may only be dropped
    // if the aggregate node is kept: rolling COUNT up must yield 0 and SUM/MIN/MAX must yield NULL.
    sql "set enable_materialized_view_rewrite=true"
    async_mv_rewrite_success(db, mvSql, qCountEmpty, "scalar_agg_empty_mv")
    sql """ANALYZE TABLE scalar_agg_empty_mv WITH SYNC"""
    mv_rewrite_success(qCountMatched, "scalar_agg_empty_mv")
    mv_rewrite_success(qCountProjected, "scalar_agg_empty_mv")
    mv_rewrite_success(qSumEmpty, "scalar_agg_empty_mv")
    mv_rewrite_success(qSumMatched, "scalar_agg_empty_mv")
    mv_rewrite_success(qSumAll, "scalar_agg_empty_mv")
    mv_rewrite_success(qMinEmpty, "scalar_agg_empty_mv")
    mv_rewrite_success(qMinMatched, "scalar_agg_empty_mv")
    mv_rewrite_success(qMaxEmpty, "scalar_agg_empty_mv")
    mv_rewrite_success(qMaxMatched, "scalar_agg_empty_mv")
    // count(distinct v) is stored in the mv as an already computed value and can not be rolled up,
    // so the mv must not answer this query at all.
    mv_rewrite_fail(qCountDistinct, "scalar_agg_empty_mv")
    // count(*) without a predicate is answered straight from the FE table metadata by
    // REWRITE_SIMPLE_AGG_TO_CONSTANT, so the mv is not expected to be used for it.

    // The scalar aggregate must survive the rewrite: an mv scan alone would return no row at all.
    order_qt_count_on_empty "${qCountEmpty}"
    order_qt_count_on_matched "${qCountMatched}"
    order_qt_count_on_all "${qCountAll}"
    order_qt_count_on_projected "${qCountProjected}"
    order_qt_count_on_distinct "${qCountDistinct}"
    order_qt_sum_on_empty "${qSumEmpty}"
    order_qt_sum_on_matched "${qSumMatched}"
    order_qt_sum_on_all "${qSumAll}"
    order_qt_min_on_empty "${qMinEmpty}"
    order_qt_min_on_matched "${qMinMatched}"
    order_qt_max_on_empty "${qMaxEmpty}"
    order_qt_max_on_matched "${qMaxMatched}"
    qt_shape_count_on """explain shape plan ${qCountEmpty}"""
    // MIN rolls up to MIN, which is nullable already, so no empty-input compensation is needed here.
    qt_shape_min_on """explain shape plan ${qMinEmpty}"""

    // A query which keeps one of the mv dimensions may still drop the aggregate node: the equality
    // filter makes the remaining mv dimension uniform, so the mv holds exactly one row per group.
    def uniformMvSql = """select k, v, count(*) as cnt from scalar_agg_empty_base group by k, v"""
    def qUniform = """select k, count(*) from scalar_agg_empty_base where v = 1001 group by k"""
    def qUniformEmpty = """select k, count(*) from scalar_agg_empty_base where v = 999999 group by k"""
    sql """DROP MATERIALIZED VIEW IF EXISTS scalar_agg_empty_mv"""
    async_mv_rewrite_success(db, uniformMvSql, qUniform, "scalar_agg_uniform_mv")
    sql """ANALYZE TABLE scalar_agg_uniform_mv WITH SYNC"""
    mv_rewrite_success(qUniformEmpty, "scalar_agg_uniform_mv")

    sql "set enable_materialized_view_rewrite=false"
    order_qt_uniform_off "${qUniform}"
    order_qt_uniform_off_empty "${qUniformEmpty}"
    sql "set enable_materialized_view_rewrite=true"
    order_qt_uniform_on "${qUniform}"
    order_qt_uniform_on_empty "${qUniformEmpty}"
    qt_shape_uniform """explain shape plan ${qUniform}"""
}
