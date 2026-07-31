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

suite("test_now_consistent_mtmv", "mtmv") {
    def mvName = "test_now_consistent_mtmv"
    def dbName = context.config.getDbNameByFile(context.file)

    sql "drop materialized view if exists ${mvName}"
    sql "drop table if exists test_now_consistent_mtmv_table"
    sql """
        create table test_now_consistent_mtmv_table (
            id int
        )
        distributed by hash(id) buckets 1
        properties ("replication_num" = "1")
    """
    sql "insert into test_now_consistent_mtmv_table values (1)"

    def nowColumns = (1..100).collect {
        def function = it % 2 == 0 ? "now(6)" : "current_timestamp(6)"
        "${function} as ts${it}"
    }.join(", ")
    sql """
        create materialized view ${mvName}
        build deferred refresh complete on manual
        distributed by random buckets 1
        properties (
            "replication_num" = "1",
            "enable_nondeterministic_function" = "true"
        )
        as select id, ${nowColumns} from test_now_consistent_mtmv_table
    """

    sql "refresh materialized view ${mvName} complete"
    waitingMTMVTaskFinished(getJobName(dbName, mvName))

    def equalPredicates = (2..100).collect { "ts1 = ts${it}" }.join(" and ")
    qt_first_refresh_consistent "select count(*) from ${mvName} where ${equalPredicates}"
    def firstRefreshTime = sql("select ts1 from ${mvName}")[0][0]

    sleep(100)
    sql "refresh materialized view ${mvName} complete"
    waitingMTMVTaskFinished(getJobName(dbName, mvName))

    qt_second_refresh_consistent "select count(*) from ${mvName} where ${equalPredicates}"
    def secondRefreshTime = sql("select ts1 from ${mvName}")[0][0]
    assertNotEquals(firstRefreshTime, secondRefreshTime)
}
