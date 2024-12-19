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

suite("test_show_sync_job_command", "query,sync_job") {
    try {
        sql """CREATE DATABASE IF NOT EXISTS test_db_sync_job;"""
        
        sql """CREATE TABLE IF NOT EXISTS test_db_sync_job.test_tbl1_sync_job (
                  id INT,
                  name STRING
              ) UNIQUE KEY(id)
              DISTRIBUTED BY HASH(id) BUCKETS 1
              PROPERTIES("replication_num" = "1");"""

        // Create the sync job
        sql """CREATE SYNC test_db_sync_job.job1
        (
            FROM mysql_db1.tbl1 INTO test_tbl1_sync_job
        )
        FROM BINLOG
        (
            "type" = "canal",
            "canal.server.ip" = "127.0.0.1",
            "canal.server.port" = "11111",
            "canal.destination" = "example",
            "canal.username" = "",
            "canal.password" = ""
        );"""

        checkNereidsExecute("SHOW SYNC JOB FROM test_db_sync_job")

    } catch (Exception e) {
        // Log any exceptions that occur during testing
        log.error("Failed to execute CREATE SYNC JOB command", e)
        throw e
    } finally {
        // Cleanup
        try_sql("STOP SYNC JOB IF EXISTS job1;")
        try_sql("DROP TABLE IF EXISTS test_db_sync_job.test_tbl1_sync_job;")
        try_sql("DROP DATABASE IF EXISTS test_db_sync_job;")
    }
}
