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

suite("test_queries_tvf","p0,external,tvf,external_docker") {
    def table_name = "test_queries_tvf"
    sql """ DROP TABLE IF EXISTS ${table_name} """
    sql """
    CREATE TABLE IF NOT EXISTS ${table_name} (
        `user_id` LARGEINT NOT NULL COMMENT "用户id",
        `name` STRING COMMENT "用户名称",
        `age` INT COMMENT "用户年龄"
        )
        DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
    """

    sql """insert into ${table_name} values (1, 'doris', 10);"""

    sql """select * from ${table_name};"""

    def res = sql """ select query_id from information_schema.active_queries where `sql` like "%${table_name}%"; """
    logger.info("res = " + res)
    assertTrue(res.size() >= 0 && res.size() <= 2);
}