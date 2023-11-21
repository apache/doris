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

suite("test_set_replica_status", "p0") {
    def tableName = "test_set_replica_status_table"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            `id` LARGEINT NOT NULL,
            `count` LARGEINT SUM DEFAULT "0")
        AGGREGATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "replication_num" = "1"
        )
        """

    List<String> values = []
    for (int i = 1; i <= 10; ++i) {
        values.add("(${i}, ${i})")
    }
    sql """INSERT INTO ${tableName} VALUES ${values.join(",")}"""

    def result = sql """show tablets from ${tableName}"""
    assertTrue(result != null)
    def tabletId = result[0][0]
    def backendId = result[0][2]
    sql """ADMIN SET REPLICA STATUS PROPERTIES("tablet_id" = "${tabletId}", "backend_id" = "${backendId}", "status" = "bad");"""
    result = sql_return_maparray """ADMIN SHOW REPLICA STATUS FROM ${tableName}"""
    for (def res : result) {
        if (res.TabletId == tabletId) {
            logger.info("admin show replica status ${res}")
            assertTrue("true".equalsIgnoreCase(res.IsBad)) 
        }
    }
    sql """ADMIN SET REPLICA STATUS PROPERTIES("tablet_id" = "${tabletId}", "backend_id" = "${backendId}", "status" = "ok");"""
    result = sql_return_maparray """ADMIN SHOW REPLICA STATUS FROM ${tableName}"""
    for (def res : result) {
        if (res.TabletId == tabletId) {
            logger.info("admin show replica status ${res}")
            assertTrue("false".equalsIgnoreCase(res.IsBad)) 
        }
    }
    sql """ADMIN SET REPLICA VERSION PROPERTIES("tablet_id" = "${tabletId}", "backend_id" = "${backendId}", "last_failed_version" = "10");"""
    result = sql_return_maparray """ADMIN SHOW REPLICA STATUS FROM ${tableName}"""
    for (def res : result) {
        if (res.TabletId == tabletId) {
            logger.info("admin show replica version ${res}")
            assertFalse(res.LastFailedVersion == 10)
        }
    }
    sql """ADMIN CLEAN TRASH"""
}
