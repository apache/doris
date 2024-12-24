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

suite("test_row_policy") {
    def dbName = context.config.getDbNameByFile(context.file)
    def tableName = "t1"
    def user = "test_row_policy"
    def tokens = context.config.jdbcUrl.split('/')
    def url=tokens[0] + "//" + tokens[2] + "/" + dbName + "?"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """CREATE TABLE ${tableName} (id INT) DISTRIBUTED BY HASH(id) PROPERTIES('replication_num'='1')"""
    sql """DROP USER IF EXISTS ${user}"""
    sql """CREATE USER ${user} IDENTIFIED BY '123456';"""
    sql """GRANT SELECT_PRIV ON ${dbName} TO ${user}"""
    sql """DROP ROW POLICY IF EXISTS policy_01 ON ${tableName} FOR ${user}"""
    sql """CREATE ROW POLICY IF NOT EXISTS policy_01 ON ${tableName} AS restrictive TO ${user} USING(id=1)"""

    connect(user, '123456', url) {
        sql "set enable_nereids_planner = false"
        sql "SELECT * FROM ${tableName} a JOIN ${tableName} b ON a.id = b.id"
    }

    connect(user, '123456', url) {
        sql "set enable_nereids_planner = true"
        sql "set enable_fallback_to_original_planner = false"
        sql "SELECT * FROM ${tableName} a JOIN ${tableName} b ON a.id = b.id"
    }
}