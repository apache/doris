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

suite("test_query_json_object", "query") {
    sql "set enable_vectorized_engine = false;"
    def tableName = "test_query_json_object"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
            CREATE TABLE `${tableName}` (
              `k1` int(11) NULL COMMENT "user id"
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
        """
    sql "insert into ${tableName} values(null);"
    sql "insert into ${tableName} values(null);"
    sql "insert into ${tableName} values(null);"
    sql "insert into ${tableName} values(null);"
    sql "insert into ${tableName} values(null);"
    sql "insert into ${tableName} values(1);"
    qt_sql "select json_object(\"k1\",k1) from ${tableName};"
    sql "DROP TABLE ${tableName};"
}
