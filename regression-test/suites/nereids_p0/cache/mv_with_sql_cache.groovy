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

suite("mv_with_sql_cache") {

    def assertHasCache = { String sqlStr ->
        explain {
            sql ("physical plan ${sqlStr}")
            contains("PhysicalSqlCache")
        }
    }

    def assertNoCache = { String sqlStr ->
        explain {
            sql ("physical plan ${sqlStr}")
            notContains("PhysicalSqlCache")
        }
    }

    String dbName = context.config.getDbNameByFile(context.file)
    def prefix_str = "mv_with_sql_cache_"

    sql "ADMIN SET ALL FRONTENDS CONFIG ('cache_last_version_interval_second' = '10')"
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql "set enable_sql_cache=true"

    def tb_name1 = prefix_str + "table1"
    createTestTable tb_name1
    sleep(10 * 1000)

    def mv_name1 = prefix_str + "mv1"
    def new_mv_name1 = prefix_str + "new_mv1"
    def mv_name2 = prefix_str + "mv2"

    def mv_sql1 = """select id as col1, sum(value) as col2 from ${tb_name1} group by id;"""
    def mv_sql2 = """select id as col3 from ${tb_name1} group by id;"""
    // select mv
    def mv_str = """select col1 from ${tb_name1} index ${mv_name1} group by col1"""
    def new_mv_str = """select col1 from ${tb_name1} index ${new_mv_name1} group by col1"""

    assertNoCache "select * from ${tb_name1}"
    sql "select * from ${tb_name1}"
    assertHasCache "select * from ${tb_name1}"

    assertNoCache mv_sql1
    sql mv_sql1
    assertHasCache mv_sql1

    assertNoCache mv_sql2
    sql mv_sql2
    assertHasCache mv_sql2

    // create mv
    create_sync_mv(dbName, tb_name1, mv_name1, mv_sql1)
    create_sync_mv(dbName, tb_name1, mv_name2, mv_sql2)
    sleep(10 * 1000)

    assertHasCache "select * from ${tb_name1}"
    assertHasCache mv_sql1
    assertHasCache mv_sql2

    assertNoCache mv_str
    sql mv_str
    assertHasCache mv_str

    // insert data
    sql """insert into ${tb_name1} values (1, 3);"""
    sleep(10 * 1000)
    assertNoCache "select * from ${tb_name1}"
    sql "select * from ${tb_name1}"
    assertHasCache "select * from ${tb_name1}"

    assertNoCache mv_sql1
    sql mv_sql1
    assertHasCache mv_sql1

    assertNoCache mv_sql2
    sql mv_sql2
    assertHasCache mv_sql2

    assertNoCache mv_str
    sql mv_str
    assertHasCache mv_str

    // alter rename
    sql """ALTER TABLE ${tb_name1} RENAME ROLLUP ${mv_name1} ${new_mv_name1};"""
    sleep(10 * 1000)

    assertNoCache "select * from ${tb_name1}"
    assertNoCache mv_sql1
    assertNoCache mv_sql2
    assertNoCache new_mv_str

    sql "select * from ${tb_name1}"
    sql mv_sql1
    sql mv_sql2
    sql new_mv_str

    assertHasCache "select * from ${tb_name1}"
    assertHasCache mv_sql1
    assertHasCache mv_sql2
    assertHasCache new_mv_str

    // drop mv
    sql """drop materialized view ${new_mv_name1} on ${tb_name1};"""
    sleep(20 * 1000)

    assertHasCache "select * from ${tb_name1}"
    assertHasCache mv_sql1
    assertHasCache mv_sql2



}
