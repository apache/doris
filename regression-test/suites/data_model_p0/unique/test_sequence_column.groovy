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

suite("test_sequence_column") {
    // test sequence X inverted index
    def tableName = "test_sequence_column"
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
                "function_column.sequence_col" = 'id',
                "replication_num" = "1"
                );
    """
    sql "insert into ${tableName} values(1,1,5,'a'),(1,1,4,'a'),(1,1,3,'a')"

    qt_select1 "SELECT * from ${tableName}"

    // test sequence X row store
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
                "function_column.sequence_col" = 'id',
                "replication_num" = "1",
                "store_row_column" = "true"
                );
    """
    sql "insert into ${tableName} values(1,1,5,'a'),(1,1,4,'a'),(1,1,3,'a')"

    order_qt_all "SELECT * from ${tableName}"

    // test sequence X variant
    sql "DROP TABLE IF EXISTS ${tableName}"
    try{
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `user_id` bigint,
                `group_id` bigint,
                `id` bigint,
                `keyword` VARCHAR(128),
                `var` variant,
                INDEX idx_col1 (user_id) USING INVERTED
                ) ENGINE=OLAP
            UNIQUE KEY(user_id, group_id)
            DISTRIBUTED BY HASH (user_id) BUCKETS 1
            PROPERTIES(
                    "function_column.sequence_col" = 'var',
                    "replication_num" = "1",
                    "store_row_column" = "true"
                    );
    """
    }catch(Exception e){
        assertTrue(e.getMessage().contains("Sequence type only support integer types and date types"))
    }

}

