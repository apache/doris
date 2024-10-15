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

suite ("dup_mv_bm_hash") {
    sql """ DROP TABLE IF EXISTS dup_mv_bm_hash; """

    sql """
            create table dup_mv_bm_hash(
                k1 int null,
                k2 int null,
                k3 varchar(100) null
            )
            duplicate key (k1)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into dup_mv_bm_hash select 1,1,'a';"
    sql "insert into dup_mv_bm_hash select 2,2,'b';"
    sql "insert into dup_mv_bm_hash select 3,3,'c';"

    createMV( "create materialized view dup_mv_bm_hash_mv1 as select k1,bitmap_union(to_bitmap(k2)) from dup_mv_bm_hash group by k1;")

    sql "SET experimental_enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "analyze table dup_mv_bm_hash with sync;"
    sql """set enable_stats=false;"""

    explain {
        sql("select bitmap_union_count(to_bitmap(k2)) from dup_mv_bm_hash group by k1 order by k1;")
        contains "(dup_mv_bm_hash_mv1)"
    }
    order_qt_select_mv "select bitmap_union_count(to_bitmap(k2)) from dup_mv_bm_hash group by k1 order by k1;"

    sql """set enable_stats=true;"""
    explain {
        sql("select bitmap_union_count(to_bitmap(k2)) from dup_mv_bm_hash group by k1 order by k1;")
        contains "(dup_mv_bm_hash_mv1)"
    }

    createMV("create materialized view dup_mv_bm_hash_mv2 as select k1,bitmap_union(bitmap_hash(k3)) from dup_mv_bm_hash group by k1;")

    sql "insert into dup_mv_bm_hash select 2,2,'bb';"
    sql "insert into dup_mv_bm_hash select 3,3,'c';"

    order_qt_select_k1 "select k1 from dup_mv_bm_hash order by k1;"

    order_qt_select_star "select * from dup_mv_bm_hash order by k1,k2,k3;"

    explain {
        sql("select k1,bitmap_union_count(bitmap_hash(k3)) from dup_mv_bm_hash group by k1;")
        contains "(dup_mv_bm_hash_mv2)"
    }
    order_qt_select_mv_sub "select k1,bitmap_union_count(bitmap_hash(k3)) from dup_mv_bm_hash group by k1 order by k1;"

    sql """set enable_stats=false;"""
    explain {
        sql("select k1,bitmap_union_count(bitmap_hash(k3)) from dup_mv_bm_hash group by k1;")
        contains "(dup_mv_bm_hash_mv2)"
    }
}
