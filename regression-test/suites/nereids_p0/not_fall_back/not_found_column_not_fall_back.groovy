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

suite("not_found_column_not_fall_back") {
    sql "set enable_fallback_to_original_planner = true;"
    sql "drop table if exists t1"
    sql """create table t1(a int, b int) properties('replication_num'='1');"""
    sql "insert into t1 values(1,2);"
    test {
        // a==1 is not supported by legacy planner parser
        //enable_fallback_to_original_planner is true and we still should not fall back to legacy planner
        sql "select c from t1 where a==1;"
        exception "Unknown column 'c' in 'table list' in PROJECT clause"
    }
}