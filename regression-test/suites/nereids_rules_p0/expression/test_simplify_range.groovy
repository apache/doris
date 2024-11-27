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

suite('test_simplify_range') {
    def tbl_1 = 'test_simplify_range_tbl_1'
    sql "DROP TABLE IF EXISTS  ${tbl_1} FORCE"
    sql "CREATE TABLE ${tbl_1}(a DECIMAL(16,8), b INT) PROPERTIES ('replication_num' = '1')"
    sql "INSERT INTO ${tbl_1} VALUES(null, 10)"
    test {
        sql "SELECT a BETWEEN 100.02 and 40.123 OR a IN (54.0402) AND b < 10 FROM ${tbl_1}"
        result([[null]])
    }
    sql "DROP TABLE IF EXISTS  ${tbl_1} FORCE"
}
