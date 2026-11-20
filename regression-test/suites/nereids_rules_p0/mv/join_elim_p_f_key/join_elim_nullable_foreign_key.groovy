package mv.join_elim_p_f_key
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

suite("join_elim_nullable_foreign_key") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set enable_nereids_planner = true"
    sql "set enable_fallback_to_original_planner = false"
    sql "set enable_materialized_view_rewrite = true"
    sql "set enable_nereids_timeout = false"

    sql "drop table if exists fk_nullable_f"
    sql "drop table if exists fk_nullable_p"
    sql """
        create table fk_nullable_p (
            id bigint not null
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties ("replication_num" = "1")
    """
    sql """
        create table fk_nullable_f (
            id bigint not null,
            parent_id bigint null
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties ("replication_num" = "1")
    """
    sql "alter table fk_nullable_p add constraint pk primary key(id)"
    sql "alter table fk_nullable_f add constraint fk foreign key(parent_id) references fk_nullable_p(id)"
    sql "insert into fk_nullable_p values (1)"
    sql "insert into fk_nullable_f values (1, 1), (2, null)"

    create_async_mv(db, "fk_nullable_mv", """
        select f.id, f.parent_id
        from fk_nullable_f f inner join fk_nullable_p p on f.parent_id = p.id
    """)

    def nullableQuery = """
        select /*+ use_mv(fk_nullable_mv) */ f.id, f.parent_id
        from fk_nullable_f f order by f.id
    """
    mv_rewrite_fail(nullableQuery, "fk_nullable_mv")
    order_qt_nullable_fk_query nullableQuery

    def nonNullQuery = """
        select /*+ use_mv(fk_nullable_mv) */ f.id, f.parent_id
        from fk_nullable_f f where f.parent_id is not null order by f.id
    """
    mv_rewrite_success(nonNullQuery, "fk_nullable_mv")
    order_qt_non_null_fk_query nonNullQuery
}
