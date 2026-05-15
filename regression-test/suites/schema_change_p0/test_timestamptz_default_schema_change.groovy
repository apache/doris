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

suite("test_timestamptz_default_schema_change", "p0") {
    def tableName = "test_timestamptz_default_schema_change"
    def getTableStatusSql = """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """

    sql "DROP TABLE IF EXISTS ${tableName} FORCE"
    sql "SET time_zone = '+08:00'"

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT NOT NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "light_schema_change" = "false"
        )
    """

    sql """ INSERT INTO ${tableName} VALUES (1), (2), (3) """

    sql """
        ALTER TABLE ${tableName}
        ADD COLUMN ts TIMESTAMPTZ(6) DEFAULT CURRENT_TIMESTAMP(6)
    """

    waitForSchemaChangeDone {
        sql getTableStatusSql
        time 120
    }

    sql """ sync """

    def rows = sql """
        SELECT id, CAST(ts AS STRING) AS ts_str
        FROM ${tableName}
        ORDER BY id
    """

    assertEquals(3, rows.size())
    rows.each { row ->
        assertTrue(row[1] != null)
        assertTrue(row[1].toString() ==~ /\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}[+-]\d{2}:\d{2}/,
                "Unexpected TIMESTAMPTZ value: ${row[1]}")
    }

    sql "DROP TABLE IF EXISTS ${tableName} FORCE"
}