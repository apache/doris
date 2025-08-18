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

suite("test_temp_table_with_view", "p0") {
    def tableName = "t_test_temp_table_with_view"
    def viewName = "v_test_temp_table_with_view"
    sql("""create table ${tableName}(a int) properties("replication_num" = "1") """)
    sql("create view ${viewName} as select * from ${tableName}")
    sql("""create temporary table ${tableName}(a int, b int, c int, d int) properties("replication_num" = "1") """)

    // following query should success
    sql("select * from ${viewName}")
}
