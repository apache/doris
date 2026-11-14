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

suite("test_ivm_empty_anti_join") {
    sql "set enable_materialized_view_rewrite = false"
    sql "set enable_dml_materialized_view_rewrite = false"
    sql "drop materialized view if exists ivm_empty_anti_mv"
    sql "drop table if exists ivm_empty_anti_l"
    sql "drop table if exists ivm_empty_anti_r"
    sql """create table ivm_empty_anti_l (k int, v int)
        unique key(k) distributed by hash(k) buckets 1
        properties ("replication_num" = "1", "changelog.enable" = "true",
            "changelog.need_historical_value" = "true", "enable_unique_key_merge_on_write" = "true")"""
    sql """create table ivm_empty_anti_r (k int, v int)
        unique key(k) distributed by hash(k) buckets 1
        properties ("replication_num" = "1", "changelog.enable" = "true",
            "changelog.need_historical_value" = "true", "enable_unique_key_merge_on_write" = "true")"""
    sql "insert into ivm_empty_anti_l values (1, 10), (2, 20), (3, 30), (null, 40)"
    sql "insert into ivm_empty_anti_r values (1, 100), (3, 300), (null, 400)"
    def fullQuery = """select l.k, l.v as lv, r.v as rv from ivm_empty_anti_l l
        left join ivm_empty_anti_r r on l.k = r.k"""
    sql """create materialized view ivm_empty_anti_mv
        build deferred refresh incremental on manual
        distributed by random buckets 1 properties ('replication_num' = '1')
        as ${fullQuery}"""
    sql "refresh materialized view ivm_empty_anti_mv complete"
    waitingMTMVTaskFinishedByMvName("ivm_empty_anti_mv")
    order_qt_initial_mv "select k, lv, rv from ivm_empty_anti_mv"
    order_qt_initial_full fullQuery

    // Only the retained side changes: both null-side repair key inputs are statically empty.
    sql "insert into ivm_empty_anti_l values (4, 40)"
    sql "refresh materialized view ivm_empty_anti_mv incremental"
    waitingMTMVTaskFinishedByMvName("ivm_empty_anti_mv")
    order_qt_left_delta_mv "select k, lv, rv from ivm_empty_anti_mv"
    order_qt_left_delta_full fullQuery

    def changes = [
        first_match: ["insert into ivm_empty_anti_r values (2, 200)"],
        last_match_deleted: ["delete from ivm_empty_anti_r where k = 1"],
        value_update: ["update ivm_empty_anti_r set v = 201 where k = 2"],
        key_change: ["delete from ivm_empty_anti_r where k = 3",
                     "insert into ivm_empty_anti_r values (4, 301)"],
        null_key: ["update ivm_empty_anti_l set v = 41 where k is null",
                   "update ivm_empty_anti_r set v = 401 where k is null"],
        both_sides: ["insert into ivm_empty_anti_l values (5, 50)",
                     "insert into ivm_empty_anti_r values (5, 500)",
                     "delete from ivm_empty_anti_l where k = 2"]
    ]
    changes.each { stage, statements ->
        statements.each { sql it }
        sql "refresh materialized view ivm_empty_anti_mv incremental"
        waitingMTMVTaskFinishedByMvName("ivm_empty_anti_mv")
        "order_qt_${stage}_mv"("select k, lv, rv from ivm_empty_anti_mv")
        "order_qt_${stage}_full"(fullQuery)
    }
}
