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

/*
 * Constant propagation materializes a constant grouping key above the aggregate,
 * e.g. `2026 AS dt_year#4`, and reuses the exprId of the column. The predicate which
 * filters that column below the aggregate must still be compensated when the query is
 * rewritten by the materialized view, otherwise the rewritten plan aggregates the rows
 * of every year and returns a wrong result.
 */
suite("mv_rewrite_constant_group_key_predicate") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_sql_cache=false"
    sql "SET enable_materialized_view_rewrite=true"
    sql "SET pre_materialized_view_rewrite_strategy=NOT_IN_RBO"

    sql "drop materialized view if exists mv_constant_group_key"
    sql "drop table if exists fact_constant_group_key"
    sql "drop table if exists dim_constant_group_key_date"
    sql "drop table if exists dim_constant_group_key_account"

    sql """
        create table fact_constant_group_key (
            data_date date not null,
            account_id int not null,
            amount decimal(20, 2)
        ) engine=olap
        duplicate key(data_date, account_id)
        partition by range(data_date) (from ('2026-01-01') to ('2026-02-01') interval 1 day)
        distributed by hash(account_id) buckets 1
        properties("replication_num" = "1")
    """

    sql """
        create table dim_constant_group_key_date (
            data_date date not null,
            dt_year int,
            dt_quarter int
        ) engine=olap
        duplicate key(data_date)
        distributed by hash(data_date) buckets 1
        properties("replication_num" = "1")
    """

    sql """
        create table dim_constant_group_key_account (
            account_id int not null,
            org_group varchar(20),
            account_type varchar(20)
        ) engine=olap
        duplicate key(account_id)
        distributed by hash(account_id) buckets 1
        properties("replication_num" = "1")
    """

    // the same fact day carries the rows of two different years, so reading every partition
    // changes the aggregation result even though only one day is written
    sql """insert into dim_constant_group_key_date values ('2026-01-05', 2025, 1), ('2026-01-06', 2026, 1)"""
    sql """insert into dim_constant_group_key_account values (1, 'G0', 'NORMAL'), (2, 'G1', 'NORMAL')"""
    sql """insert into fact_constant_group_key values
        ('2026-01-05', 1, 100), ('2026-01-06', 1, 10), ('2026-01-06', 2, 20)"""

    sql """
        create materialized view mv_constant_group_key
        build immediate refresh complete on schedule every 1 hour
        partition by (data_date)
        distributed by hash(org_group) buckets 1
        properties("replication_num" = "1")
        as
        select f.data_date, d.dt_year, d.dt_quarter, a.org_group, a.account_type, sum(f.amount) as amount
        from fact_constant_group_key f
        left join dim_constant_group_key_date d on d.data_date = f.data_date
        left join dim_constant_group_key_account a on a.account_id = f.account_id
        group by f.data_date, d.dt_year, d.dt_quarter, a.org_group, a.account_type
    """
    waitingMTMVTaskFinishedByMvName("mv_constant_group_key")

    def query = """
        select dt_year, org_group, sum(amount)
        from (
            select f.amount, d.dt_year, d.dt_quarter, a.org_group, a.account_type
            from fact_constant_group_key f
            left join dim_constant_group_key_date d on d.data_date = f.data_date
            left join dim_constant_group_key_account a on a.account_id = f.account_id
        ) q
        where dt_year in (2026) and org_group in ('G0', 'G1')
            and dt_quarter <= 3 and account_type <> 'X'
        group by dt_year, org_group
    """

    def explain = sql("explain ${query}").collect { it[0] }.join("\n")
    def choseStart = explain.indexOf("MaterializedViewRewriteSuccessAndChose:")
    def choseEnd = explain.indexOf("MaterializedViewRewriteSuccessButNotChose:")
    assertTrue(choseStart != -1 && choseEnd > choseStart, explain)
    assertTrue(explain.substring(choseStart, choseEnd).contains("mv_constant_group_key"),
            "the query should be rewritten by mv_constant_group_key:\n${explain}")

    order_qt_query "${query}"

    // the same query without the materialized view is the reference result
    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_without_mv "${query}"
    sql "SET enable_materialized_view_rewrite=true"
}
