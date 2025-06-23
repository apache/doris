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

suite('test_disable_nereids_expression_rule') {
    def tbl = 'test_disable_nereids_expression_rule_tbl'
    sql "DROP TABLE IF EXISTS ${tbl}"
    sql "CREATE TABLE ${tbl}(a INT) PROPERTIES('replication_num' = '1')"
    sql "INSERT INTO ${tbl} VALUES(10)"
    sql "SET enable_parallel_result_sink=true"
    qt_shape_1 "EXPLAIN SHAPE PLAN SELECT * FROM ${tbl} WHERE a = 1.1"
    sql "SET disable_nereids_expression_rules='SIMPLIFY_COMPARISON_PREDICATE'"
    qt_shape_2 "EXPLAIN SHAPE PLAN SELECT * FROM ${tbl} WHERE a = 1.1"
    sql "DROP TABLE IF EXISTS ${tbl}"
}
