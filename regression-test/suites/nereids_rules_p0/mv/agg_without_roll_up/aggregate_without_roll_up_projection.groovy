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

suite("aggregate_without_roll_up_projection") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    // Pin the session time zone: the test inserts offset-less TIMESTAMPTZ values and the golden output
    // hard-codes +08:00, so on a non +08:00 runner the rendered values would differ for environmental reasons.
    sql "set time_zone = '+08:00'"
    sql "set pre_materialized_view_rewrite_strategy = TRY_IN_RBO"
    // Disable the plain aggregate rule so that the query is rewritten by the
    // MATERIALIZED_VIEW_PROJECT_FILTER_AGGREGATE rule only, which reproduces the bug that a
    // projection of the group by key in the query top plan is wrongly treated as a group by key.
    sql "set disable_nereids_rules='MATERIALIZED_VIEW_ONLY_AGGREGATE'"

    sql """ DROP TABLE IF EXISTS sync_tz_base; """

    sql """
        create table sync_tz_base(
            id int null,
            ts timestamptz(6) null,
            v int null
        )
        duplicate key (id)
        distributed BY hash(id) buckets 3
        properties("replication_num" = "1");
    """

    sql "insert into sync_tz_base values (1, '2024-01-01 10:00:00', 3), (2, '2024-01-01 12:00:00', 4), (3, '2024-01-02 08:00:00', 3), (4, null, 7);"

    // the sync mv contains the same `ts is not null` predicate as the query, so it can be covered
    // by the mv and no predicate compensation is needed
    create_sync_mv(db, "sync_tz_base", "sync_tz_day",
            "select date_trunc(ts, 'day') as day_ts, sum(v) as day_sum from sync_tz_base "
                    + "where ts is not null group by date_trunc(ts, 'day');")
    // The mv groups by (day, id), while a query selecting only `day` is grouped by (day, id) and its top
    // project is a strict matching prefix of the normalized aggregate outputs. The redundant aggregate
    // output must be projected away, otherwise the rewritten plan output count differs from the query
    // and the rewrite is rejected. The select aliases avoid column name conflicts with the base table
    // and the existing mv on the same table.
    create_sync_mv(db, "sync_tz_base", "sync_tz_day_id",
            "select date_trunc(ts, 'day') as day_ts2, id as id2, sum(v) as day_sum2 from sync_tz_base "
                    + "where ts is not null group by date_trunc(ts, 'day'), id;")

    sql "analyze table sync_tz_base with sync;"
    sql """alter table sync_tz_base modify column id set stats ('row_count'='4');"""

    // The query has a derived projection `cast(date_trunc(ts, 'day') as string)` of the group by key
    // `date_trunc(ts, 'day')` in the top project. The derived projection should be recomputed by a
    // project above the rewritten aggregate, and must not be treated as a group by key of the
    // rewritten aggregate.
    mv_rewrite_success("select cast(date_trunc(ts, 'day') as string) as day_ts, sum(v) "
            + "from sync_tz_base where ts is not null group by date_trunc(ts, 'day');", "sync_tz_day")
    order_qt_select_mv """select cast(date_trunc(ts, 'day') as string) as day_ts, sum(v)
            from sync_tz_base where ts is not null group by date_trunc(ts, 'day') order by 1;"""

    // The query top project is a strict matching prefix of the normalized aggregate outputs
    // (`select day from ... group by day, id`), and the leading subset exactly matches the mv
    // outputs, which reproduces the boundary where the redundant aggregate output must be
    // projected away by a top project above the rewritten aggregate.
    mv_rewrite_success("select date_trunc(ts, 'day') from sync_tz_base where ts is not null "
            + "group by date_trunc(ts, 'day'), id;", "sync_tz_day_id")
    order_qt_select_mv_leading_subset """select date_trunc(ts, 'day') from sync_tz_base
            where ts is not null group by date_trunc(ts, 'day'), id order by 1;"""

    // Multiple query top plan expressions can be rewritten to the same aggregate output slot
    // (`sum(v) as s1` and `sum(v) as s2` both reference the same bottom sum slot). Each top
    // project position must keep a distinct output expr id, otherwise the rewritten output set
    // collapses and the rewritten plan is skipped before normalization.
    mv_rewrite_success("select cast(date_trunc(ts, 'day') as string) as day_ts, sum(v) as s1, "
            + "sum(v) as s2 from sync_tz_base where ts is not null "
            + "group by date_trunc(ts, 'day');", "sync_tz_day")
    order_qt_select_mv_dup_sum """select cast(date_trunc(ts, 'day') as string) as day_ts, sum(v) as s1,
            sum(v) as s2 from sync_tz_base where ts is not null group by date_trunc(ts, 'day') order by 1;"""

    // The same duplicate-collapse can also happen for separately aliased duplicates of the group by
    // expression itself.
    mv_rewrite_success("select date_trunc(ts, 'day') as d1, date_trunc(ts, 'day') as d2, sum(v) as s "
            + "from sync_tz_base where ts is not null group by date_trunc(ts, 'day');", "sync_tz_day")
    order_qt_select_mv_dup_group """select date_trunc(ts, 'day') as d1, date_trunc(ts, 'day') as d2, sum(v) as s
            from sync_tz_base where ts is not null group by date_trunc(ts, 'day') order by 1;"""

    // A repeated unaliased bare output references the same original output slot twice
    // (`select day, day, sum(v) ... group by day`). The two positions rewrite to the same mv slot and
    // must keep the single original expr id multiplicity: forcing a fresh alias on the repeated
    // position would inflate the rewritten output set and skip the whole-tree normalization and
    // partition pruning in MaterializedViewUtils.rewriteByRules.
    mv_rewrite_success("select date_trunc(ts, 'day'), date_trunc(ts, 'day'), sum(v) "
            + "from sync_tz_base where ts is not null group by date_trunc(ts, 'day');", "sync_tz_day")
    order_qt_select_mv_dup_bare_group """select date_trunc(ts, 'day'), date_trunc(ts, 'day'), sum(v)
            from sync_tz_base where ts is not null group by date_trunc(ts, 'day') order by 1;"""
}
