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

suite('test_adjust_nullable') {
    // test AjustNullable not throw exception:
    // 'AdjustNullable convert slot xx from not-nullable to nullable. You can disable check by set fe_debug = false.'
    // NOTICE: the pipeline need set global fe_debug = true
    def tbl = 'test_adjust_nullable_t'
    sql "SET detail_shape_nodes='PhysicalProject'"
    sql "DROP TABLE IF EXISTS ${tbl} FORCE"
    sql "CREATE TABLE ${tbl}(a int not null, b int, c int not null) distributed by hash(a) properties('replication_num' = '1')"
    sql "INSERT INTO ${tbl} VALUES(1, 2, 3)"

    // avg => sum / count
    // avg is not nullable,  while divide '/' is nullable, need add non_nullable
    // avg => non_nullable(sum / count)
    qt_avg_shape """explain shape plan
        SELECT AVG(distinct a), AVG(distinct b) FROM ${tbl} GROUP BY c
    """

    sql "DROP TABLE IF EXISTS ${tbl} FORCE"
}
