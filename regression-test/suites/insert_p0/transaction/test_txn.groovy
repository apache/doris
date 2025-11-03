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

suite("test_txn") {
    def tableName = "test_txn"
    // test txn X inverted index
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id` bigint default 999,
            `group_id` bigint,
            `id` bigint,
            `vv` variant,
            INDEX idx_col1 (user_id) USING INVERTED
            ) ENGINE=OLAP
        UNIQUE KEY(user_id, group_id)
        DISTRIBUTED BY HASH (user_id) BUCKETS 1
        PROPERTIES(
                "store_row_column" = "true",
                "replication_num" = "1"
                );
    """
    sql "begin"
    sql """insert into ${tableName} values(1,1,5,'{"b":"b"}'),(1,1,4,'{"b":"b"}'),(1,1,3,'{"b":"b"}')"""
    sql "commit"

    qt_select1 "SELECT * from ${tableName}"

}
