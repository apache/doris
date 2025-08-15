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

suite("test_physical_translator") {
    def tbl = "tbl_test_physical_translator"
    sql 'SET enable_nereids_planner=true'
    sql 'SET runtime_filter_mode=OFF'
    sql 'SET enable_fallback_to_original_planner=false'
    sql "SET ignore_shape_nodes='PhysicalDistribute'"
    sql "SET detail_shape_nodes='PhysicalProject'"
    sql 'SET disable_nereids_rules=PRUNE_EMPTY_PARTITION'
    sql "drop table if exists ${tbl} force"
    sql "create table ${tbl} (a int, b int) properties('replication_num' = '1')"
    sql "insert into ${tbl} values(1, 10), (2, 20)"

    def sql1 = """
        SELECT a, x as k1,  x as k2 FROM (SELECT a, random(100) as x FROM ${tbl}) t
    """

    explainAndOrderResult "continue_project", sql1
    explain {
        sql sql1
        contains "VSELECT"
    }

    def sql2 = """
        select * from (select a + 2 as x from ${tbl} where a > 5 limit 1)s where x > 5 and x < 100
    """

    explainAndOrderResult "continue_filter", sql2
    explain {
        sql sql2
        contains "VSELECT"
    }

    sql "drop table if exists ${tbl} force"
}
