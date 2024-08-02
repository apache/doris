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

suite("test_new_delete") {
    def tableName = "test_new_delete"
    // test delete X inverted index
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id` bigint,
            `group_id` bigint,
            `id` bigint,
            `keyword` VARCHAR(128),
            INDEX idx_col1 (user_id) USING INVERTED
            ) ENGINE=OLAP
        UNIQUE KEY(user_id, group_id)
        DISTRIBUTED BY HASH (user_id) BUCKETS 1
        PROPERTIES(
                "replication_num" = "1"
                );
    """
    sql "insert into ${tableName} values(1,1,5,'a'),(1,1,4,'a'),(1,1,3,'a')"
    sql "delete from ${tableName} where user_id=1"

    qt_select1 "SELECT * from ${tableName}"

    // test delete X row store
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id` bigint,
            `group_id` bigint,
            `id` bigint,
            `keyword` VARCHAR(128)
            ) ENGINE=OLAP
        UNIQUE KEY(user_id, group_id)
        DISTRIBUTED BY HASH (user_id) BUCKETS 1
        PROPERTIES(
                "replication_num" = "1",
                "store_row_column" = "true"
                );
    """
    sql "insert into ${tableName} values(1,1,5,'a'),(1,1,4,'a'),(1,1,3,'a')"
    sql "delete from ${tableName} where user_id=1"

    qt_select2 "SELECT * from ${tableName}"
}
