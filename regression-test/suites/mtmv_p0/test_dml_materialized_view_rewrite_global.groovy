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

suite("test_dml_materialized_view_rewrite_global", "p0,mtmv,nonConcurrent") {
    String dbName = context.config.getDbNameByFile(context.file)
    String originalDmlRewrite = sql_return_maparray(
            "show global variables like 'enable_dml_materialized_view_rewrite'")[0].Value.toString()

    sql "drop materialized view if exists dml_rewrite_global_candidate_mv"
    sql "drop materialized view if exists dml_rewrite_global_target_mv"
    sql "drop table if exists dml_rewrite_global_base"

    sql """
        create table dml_rewrite_global_base (
            k1 int,
            v1 int
        )
        duplicate key(k1)
        distributed by hash(k1) buckets 1
        properties ('replication_num' = '1')
    """
    sql "insert into dml_rewrite_global_base values (1, 10), (2, 20)"

    sql """
        create materialized view dml_rewrite_global_candidate_mv
        build deferred refresh complete on manual
        distributed by random buckets 1
        properties ('replication_num' = '1')
        as select k1, sum(v1) as total from dml_rewrite_global_base group by k1
    """
    sql "refresh materialized view dml_rewrite_global_candidate_mv complete"
    waitingMTMVTaskFinishedByMvName("dml_rewrite_global_candidate_mv", dbName)

    sql """
        create materialized view dml_rewrite_global_target_mv
        build deferred refresh complete on manual
        distributed by random buckets 1
        properties ('replication_num' = '1')
        as select k1, sum(v1) as total from dml_rewrite_global_base group by k1
    """

    setGlobalVarTemporary([enable_dml_materialized_view_rewrite: false], {
        connect(context.config.jdbcUser, context.config.jdbcPassword,
                context.config.buildUrlWithDb(context.config.jdbcUrl, dbName)) {
            def sessionValue = sql_return_maparray(
                    "show variables like 'enable_dml_materialized_view_rewrite'")[0].Value.toString()
            assertEquals("false", sessionValue.toLowerCase())
            sql "set enable_materialized_view_rewrite=true"
            sql "refresh materialized view dml_rewrite_global_target_mv complete"
        }
        waitingMTMVTaskFinishedByMvName("dml_rewrite_global_target_mv", dbName)
    })

    order_qt_target_data "select k1, total from dml_rewrite_global_target_mv order by k1"

    String restoredDmlRewrite = sql_return_maparray(
            "show global variables like 'enable_dml_materialized_view_rewrite'")[0].Value.toString()
    assertEquals(originalDmlRewrite, restoredDmlRewrite)
}
