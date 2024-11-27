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

suite("test_default_value") {
    def tableName = "test_default_value"
    // test default value X txn 
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id` bigint default 999,
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
    sql "begin"
    sql "insert into ${tableName} values(1,1,5,'a'),(1,1,4,'a'),(1,1,3,'a')"
    sql "commit"

    qt_select1 "SELECT * from ${tableName}"

    // test default value X variant
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id` bigint,
            `group_id` bigint,
            `id` bigint,
            `vv` variant default NULL 
            ) ENGINE=OLAP
        UNIQUE KEY(user_id, group_id)
        DISTRIBUTED BY HASH (user_id) BUCKETS 1
        PROPERTIES(
                "replication_num" = "1"
                );
    """
    sql """insert into ${tableName} (user_id, group_id, id) values (1,1,5),(2,2,4),(1,1,3)"""
    sql """insert into ${tableName} values(3,3,3,'{"b":"b"}')"""

    qt_select2 "SELECT * from ${tableName}"
}
