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

suite("test_query_json_insert", "query") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    qt_sql "select json_insert('{\"a\": 1, \"b\": [2, 3]}', '\$', null);"
    qt_sql "select json_insert('{\"k\": [1, 2]}', '\$.k[0]', null, '\$.[1]', null);"
    def tableName = "test_query_json_insert"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
            CREATE TABLE ${tableName} (
              `id` int(11) not null,
              `time` datetime,
              `k` int(11)
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`,`time`,`k`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
        """

    sql "insert into ${tableName} values(1,'2022-01-01 11:45:14',9);"
    sql "insert into ${tableName} values(2,'2022-01-01 11:45:14',null);"
    sql "insert into ${tableName} values(3,null,9);"
    sql "insert into ${tableName} values(4,null,null);"
    order_qt_sql1 "select json_insert('{\"id\": 0, \"time\": \"1970-01-01 00:00:00\", \"a1\": [1, 2], \"a2\": [1, 2]}', '\$.id', id, '\$.time', time, '\$.a1[1]', k, '\$.a2[3]', k) from ${tableName};"
    sql "DROP TABLE ${tableName};"
}
