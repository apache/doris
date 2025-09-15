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

suite("table_replica_count_override", "p0") {
    sql """ set table_replica_count_override = 3; """
    def result = sql """ show variables like "%table_replica_count_override%"; """
    assertEquals(result.size(), 1)

    result = """ show backends; """
    if (result.size() <= 1) {
        try {
            sql """
                CREATE TABLE IF NOT EXISTS test_replica_count (
                    id int,
                    name varchar(255)
                )
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1"
                )
            """
        } catch (Exception e) {
            logger.info("result, ${e.getMessage()}")
        }
    } else {
        sql """set table_replica_count_override = 1;"""
        sql """ CREATE TABLE IF NOT EXISTS test_replica_count_2 (
                    id int,
                    name varchar(255)
                )
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "3"
                ) """
        result = """ show create table  test_replica_count_2 """
        logger.info(""" result: ${result}  """)

        sql """set table_replica_count_override = 3;"""
        sql """ CREATE TABLE IF NOT EXISTS test_replica_count_3 (
                    id int,
                    name varchar(255)
                )
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "3"
                ) """
        result = """ show create table  test_replica_count_3 """
        logger.info(""" result: ${result}  """)
    }
}