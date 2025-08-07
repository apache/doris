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

suite ("agg_use_key_direct") {

    String db = context.config.getDbNameByFile(context.file)

    def tblName = "agg_use_key_direct"

    sql "drop table if exists ${tblName} force;"
    sql """
        create table ${tblName} (
            k1 int null,
            k2 int not null,
            k3 bigint null,
            k4 bigint null,
            k5 varchar(100) null
        )
        duplicate key (k1, k2, k3)
        distributed by hash(k1) buckets 3
        properties("replication_num" = "1");
    """
    sql "insert into ${tblName} select e1, -4, -4, -4, 'd' from (select 1 k1) as t lateral view explode_numbers(100) tmp1 as e1;"
    create_sync_mv(db, tblName, "common_mv", """select k1 as a1, k3 as a2, sum(k2), count(k4) from ${tblName} group by k1, k3;""")

    mv_rewrite_fail("""select count(k1) from agg_use_key_direct""", "common_mv")
    mv_rewrite_fail("""select sum(k1) from agg_use_key_direct""", "common_mv")
    mv_rewrite_fail("""select avg(k3) from agg_use_key_direct""", "common_mv")


    mv_rewrite_success("""select count(distinct k1) from agg_use_key_direct""", "common_mv")
    order_qt_select_count """select count(distinct k1) from agg_use_key_direct"""

    mv_rewrite_success("""select sum(distinct k1) from agg_use_key_direct""", "common_mv")
    order_qt_select_sum """select sum(distinct k1) from agg_use_key_direct"""

    mv_rewrite_success("""select max(distinct k3) from agg_use_key_direct""", "common_mv")
    order_qt_select_max """select max(distinct k3) from agg_use_key_direct"""

    mv_rewrite_success("""select min(distinct k3) from agg_use_key_direct""", "common_mv")
    order_qt_select_min """select min(distinct k3) from agg_use_key_direct"""

    mv_rewrite_success("""select avg(distinct k3) from agg_use_key_direct""", "common_mv")
    order_qt_select_avg """select avg(distinct k3) from agg_use_key_direct"""
}
