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

suite("test_show_analyze_task_status_cmd", "nereids_p0") {
    sql """ set enable_stats=true """

    def dbName = "regression_test_db"
    sql """ CREATE DATABASE IF NOT EXISTS ${dbName} """
    sql """ use ${dbName} """

    def tableName = "test_analyze_tbl"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `id` int(11) NULL,
            `name` varchar(255) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """

    // Insert some data
    sql """ INSERT INTO ${tableName} VALUES (1, 'a'), (2, 'b') """

    // Run analyze asynchronously
    def analyzeResult = sql """ ANALYZE TABLE ${tableName} """
    assertTrue(analyzeResult.size() == 1, "Unexpected analyze result size")
    def jobId = analyzeResult[0][0]

    // Wait for the job to finish
    def maxTries = 30
    def tries = 0
    def state = "PENDING"
    while (!state.equals("FINISHED") && tries < maxTries) {
        sleep(2000)  // Wait 2 seconds
        def statusResult = sql """ SHOW ANALYZE ${jobId} """
        if (statusResult.size() > 0) {
            state = statusResult[0][9]  // Assuming state is the 10th column based on docs
        }
        tries++
    }
    assertEquals("FINISHED", state, "Job did not finish in time")

    // Run SHOW ANALYZE TASK STATUS
    checkNereidsExecute(""" SHOW ANALYZE TASK STATUS ${jobId} """)

    // Run SHOW ANALYZE TASK STATUS for more validation as qt_cmd will not be suitable here 
    // due to random data/ids that will be generated
    def taskResult = sql """ SHOW ANALYZE TASK STATUS ${jobId} """

    // Verify the result
    assertTrue(taskResult.size() > 0, "No tasks found for job ${jobId}")
    // Check number of columns
    assertEquals(7, taskResult[0].size(), "Unexpected number of columns")
    // Check state is FINISHED (last column)
    taskResult.each { row ->
        assertEquals("FINISHED", row[6], "Task state not FINISHED")
    }

    // Clean up
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ DROP DATABASE IF EXISTS ${dbName} """
}
