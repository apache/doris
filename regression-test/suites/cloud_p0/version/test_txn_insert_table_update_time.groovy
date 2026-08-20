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

suite("test_txn_insert_table_update_time") {
    if (!isCloudMode()) {
        return
    }

    def tbl = "test_txn_insert_table_update_time"
    sql """ DROP TABLE IF EXISTS ${tbl} """
    sql """
        CREATE TABLE ${tbl} (
            id INT NOT NULL,
            value INT NOT NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    def beforeRows = sql """
        SELECT UNIX_TIMESTAMP(update_time)
        FROM information_schema.tables
        WHERE table_schema = DATABASE() AND table_name = '${tbl}'
    """
    assertEquals(1, beforeRows.size())
    assertTrue(beforeRows[0][0] != null)
    def beforeUpdateTime = beforeRows[0][0]

    Thread.sleep(1100)

    sql """ BEGIN """
    sql """ INSERT INTO ${tbl} VALUES (1, 10) """
    sql """ INSERT INTO ${tbl} VALUES (2, 20) """
    sql """ COMMIT """
    sql """ SYNC """

    def countRows = sql """ SELECT COUNT(*) FROM ${tbl} """
    assertEquals(2, countRows[0][0])

    def afterRows = sql """
        SELECT UNIX_TIMESTAMP(update_time)
        FROM information_schema.tables
        WHERE table_schema = DATABASE() AND table_name = '${tbl}'
    """
    assertEquals(1, afterRows.size())
    assertTrue(afterRows[0][0] != null)
    def afterUpdateTime = afterRows[0][0]
    assertTrue(afterUpdateTime > beforeUpdateTime)
}
