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

suite("test_index_key_match_select", "inverted_index_select"){
    def indexTbName1 = "index_key_match_select"
    def varchar_colume1 = "user"
    def array_string_colume2 = "followers"

    sql "DROP TABLE IF EXISTS ${indexTbName1}"

    // create table with different index
    sql """
            CREATE TABLE IF NOT EXISTS ${indexTbName1} (
                ${varchar_colume1} varchar(500),
                ${array_string_colume2} array<string>,
                INDEX ${varchar_colume1}_idx(${varchar_colume1}) USING INVERTED PROPERTIES("parser"="english") COMMENT '${varchar_colume1} index',
                INDEX ${array_string_colume2}_idx(${array_string_colume2}) USING INVERTED PROPERTIES("parser"="none") COMMENT '${array_string_colume2} index'
            )
            DUPLICATE KEY(`user`)
            DISTRIBUTED BY HASH(`user`) BUCKETS 10
            properties("replication_num" = "1");
    """

    // insert data
    // ${varchar_colume1}, ${array_string_colume2}
    // user, followers
    sql """ insert into ${indexTbName1} VALUES
        ("u1", ["u2","u3"]),
        ("u2", ["u1","u3","u4"]),
        ("u3", ["u1"]),
        ("u4", ["u3"])
    """
    sql """ set enable_common_expr_pushdown = true """
    qt_sql "SELECT * FROM ${indexTbName1} WHERE user MATCH_ANY 'u1, u2' ORDER BY user LIMIT 10;"
    qt_sql "SELECT * FROM ${indexTbName1} WHERE user MATCH_ANY 'u1, u2, u3' ORDER BY user LIMIT 10;"
}