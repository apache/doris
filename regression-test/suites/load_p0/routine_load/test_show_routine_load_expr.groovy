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

suite("test_show_routine_load_expr", "p0") {
    def tableName = "test_show_routine_load"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` int(20) NULL,
            `k2` string NULL,
            `v1` date  NULL,
            `v2` string  NULL,
            `v3` datetime  NULL,
            `v4` string  NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    sql """
        CREATE ROUTINE LOAD testshow001 ON ${tableName}
        COLUMNS TERMINATED BY ","
        PROPERTIES
        (
            "max_batch_interval" = "5",
            "max_batch_rows" = "300000",
            "max_batch_size" = "209715200"
        )
        FROM KAFKA
        (
            "kafka_broker_list" = "test",
            "kafka_topic" = "test",
            "property.kafka_default_offsets" = "OFFSET_BEGINNING"
        );
    """

    sql """
        CREATE ROUTINE LOAD testshow002 ON ${tableName}
        COLUMNS TERMINATED BY ","
        PROPERTIES
        (
            "max_batch_interval" = "5",
            "max_batch_rows" = "300000",
            "max_batch_size" = "209715200"
        )
        FROM KAFKA
        (
            "kafka_broker_list" = "test",
            "kafka_topic" = "test",
            "property.kafka_default_offsets" = "OFFSET_BEGINNING"
        );
    """

    // Order by name descending
    def res = sql """SHOW ROUTINE LOAD ORDER BY Name DESC"""
    log.info("SHOW ROUTINE LOAD result: ${res[0][1]}")
    // Expect the first job to be testshow002
    assertTrue(res[0][1] == "testshow002")

    // Order by name descending
    res = sql """SHOW ROUTINE LOAD ORDER BY Name"""
    log.info("SHOW ROUTINE LOAD result: ${res[0][1]}")
    // Expect the first job to be testshow001
    assertTrue(res[0][1] == "testshow001")

    // Limit test to get only the first routine load
    res = sql """SHOW ROUTINE LOAD LIMIT 1"""
    log.info("SHOW ROUTINE LOAD LIMIT 1 result: ${res}")
    assertTrue(res.size() == 1)  // Ensure only one result is returned

    res = sql """SHOW ROUTINE LOAD LIMIT 1 , 1"""
    log.info("SHOW ROUTINE LOAD LIMIT 1 result: ${res}")
    assertTrue(res.size() == 1)  // Ensure only one result is returned

    sql """ DROP TABLE IF EXISTS ${tableName} """


}