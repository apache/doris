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

suite("test_show_data_all_db") {
    def dbName = "test_show_data_all_db";
    def tableName = "test_show_data_all_tb";
    sql """DROP DATABASE IF EXISTS ${dbName}"""
    sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""
    sql """USE ${dbName}"""
    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """CREATE TABLE IF NOT EXISTS ${tableName} (
            `id` int(11) NOT NULL,
            `name` varchar(50) NOT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 5
        PROPERTIES (
            "replication_num" = "1"
    )"""
    sql """insert into ${tableName} values(1, "test1"), (2, "test2"), (3, "test3");"""
    def result = sql"""show data all;"""
    logger.info("show data result:${result}");
    assertTrue(result.size() > 0);
    for (int i = 0; i < result.size(); i++) {
        assertTrue(result[i].size() == 9);
    }
}
