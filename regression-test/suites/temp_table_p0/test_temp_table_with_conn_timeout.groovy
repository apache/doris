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

suite("test_temp_table_with_conn_timeout", "p0") {
    String db = context.config.getDbNameByFile(context.file)
    def tableName = "t_test_temp_table_with_conn_timeout"
    String tempTableFullName
    sql "select 1" // ensure db is created
    connect(context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
        sql"use ${db}"
        sql """create temporary table ${tableName}(id int) properties("replication_num" = "1") """
        def show_result = sql_return_maparray("show data")

        show_result.each {  row ->
            if (row.TableName.contains(tableName)) {
                tempTableFullName = row.TableName
            }
        }
        assert tempTableFullName != null

        // set session variable for a short connection timeout
        sql "set interactive_timeout=5"
        sql "set wait_timeout=5"

        sleep(10*1000)
    }

    // temp table should not exist after session exit
    def tables = sql_return_maparray("show data")
    assert tables.find { it.TableName == tempTableFullName } == null
}
