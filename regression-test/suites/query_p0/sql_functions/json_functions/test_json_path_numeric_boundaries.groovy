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

suite("test_json_path_numeric_boundaries") {
    // A path column reaches BE parsing without constant folding.
    sql "drop table if exists test_json_path_numeric_boundaries"
    sql """create table test_json_path_numeric_boundaries (
        i int not null, p string not null
    ) duplicate key(i) distributed by hash(i) buckets 1
    properties("replication_num" = "1")"""
    sql """insert into test_json_path_numeric_boundaries values (1, '\$[last--1]')"""

    test {
        sql "select json_extract_int('[1]', p) from test_json_path_numeric_boundaries"
        exception "Invalid Json Path"
    }
    test {
        sql "select json_extract_int('[1]', '\$[last--2147483648]')"
        exception "Invalid Json Path"
    }
}
