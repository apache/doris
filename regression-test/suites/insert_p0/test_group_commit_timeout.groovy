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

suite("test_group_commit_timeout", "nonConcurrent") {
    def tableName = "test_group_commit_timeout"
    sql """
        CREATE TABLE if not exists ${tableName} (
            `id` int(11) NOT NULL,
            `name` varchar(100) NULL,
            `score` int(11) NULL default "-1"
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "group_commit_interval_ms" = "300000"
        );
    """

    def query_timeout = sql """show variables where variable_name = 'query_timeout';"""
    def insert_timeout = sql """show variables where variable_name = 'insert_timeout';"""
    logger.info("query_timeout: ${query_timeout}, insert_timeout: ${insert_timeout}")

    long start = System.currentTimeMillis()
    try {
        sql "SET global query_timeout = 5"
        sql "SET global insert_timeout = 5"

        sql "set group_commit = sync_mode"
        sql "insert into ${tableName} values(1, 'a', 10)"
        assertTrue(false)
    } catch (Exception e) {
        long end = System.currentTimeMillis()
        logger.info("failed " + e.getMessage())
        assertTrue(e.getMessage().contains("FragmentMgr cancel worker going to cancel timeout instance") || e.getMessage().contains("Execute timeout") || e.getMessage().contains("timeout"))
        assertTrue(end - start <= 60000)
    } finally {
        sql "SET global query_timeout = ${query_timeout[0][1]}"
        sql "SET global insert_timeout = ${insert_timeout[0][1]}"
    }
}
