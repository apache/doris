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

suite("test_table_stream_base_table_unavailable", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    def dbName = "test_table_stream_base_unavail_db"
    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "CREATE DATABASE ${dbName}"
    sql "USE ${dbName}"

    try {
        sql """
            CREATE TABLE base_drop (
                k1 INT,
                v1 INT
            ) ENGINE=OLAP
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "binlog.enable" = "true",
                "binlog.format" = "ROW"
            )
        """

        sql """
            CREATE STREAM s_drop
            ON TABLE base_drop
            PROPERTIES ("type" = "append_only")
        """

        def beforeDrop = sql """
            SELECT BASE_TABLE_NAME, BASE_TABLE_DB, BASE_TABLE_CTL, BASE_TABLE_TYPE
            FROM information_schema.table_streams
            WHERE DB_NAME = '${dbName}' AND STREAM_NAME = 's_drop'
        """
        assertEquals(1, beforeDrop.size())
        assertEquals("base_drop", beforeDrop[0][0].toString())
        assertEquals(dbName, beforeDrop[0][1].toString())
        assertEquals("internal", beforeDrop[0][2].toString())
        assertEquals("OLAP", beforeDrop[0][3].toString())

        sql "DROP TABLE base_drop FORCE"

        def afterDrop = sql """
            SELECT BASE_TABLE_NAME, BASE_TABLE_DB, BASE_TABLE_CTL, BASE_TABLE_TYPE
            FROM information_schema.table_streams
            WHERE DB_NAME = '${dbName}' AND STREAM_NAME = 's_drop'
        """
        assertEquals(1, afterDrop.size())
        assertEquals("N/A", afterDrop[0][0].toString())
        assertEquals("N/A", afterDrop[0][1].toString())
        assertEquals("N/A", afterDrop[0][2].toString())
        assertEquals("N/A", afterDrop[0][3].toString())
    } finally {
        sql "DROP DATABASE IF EXISTS ${dbName}"
    }
}
