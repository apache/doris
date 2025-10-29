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

suite("test_load_job_info_system_table", "p0") {
    def tableName = "test_load_job_info_table"
    def label = "test_load_job_label_" + UUID.randomUUID().toString().replace("-", "")
    sql """
    DROP TABLE IF EXISTS ${tableName}
    """
    sql """
    CREATE TABLE ${tableName}
    (
        k1 INT NOT NULL,
        k2 VARCHAR(50) NOT NULL,
        k3 DATETIME NOT NULL,
        v1 INT SUM DEFAULT '0'
    )
    AGGREGATE KEY(k1, k2, k3)
    DISTRIBUTED BY HASH(k1) BUCKETS 3
    PROPERTIES (
        "replication_num" = "1"
    )
    """

    sql """
    INSERT INTO ${tableName} with label ${label} select 1, 'test1', '2023-01-01 00:00:00', 10
    """

    def res = sql """
    SELECT 
        JOB_ID,
        LABEL,
        STATE,
        PROGRESS,
        TYPE,
        ETL_INFO,
        TASK_INFO,
        ERROR_MSG,
        CREATE_TIME,
        ETL_START_TIME,
        ETL_FINISH_TIME,
        LOAD_START_TIME,
        LOAD_FINISH_TIME,
        URL,
        JOB_DETAILS,
        TRANSACTION_ID,
        ERROR_TABLETS,
        USER,
        COMMENT,
        FIRST_ERROR_MSG
    FROM 
        information_schema.load_jobs
    WHERE 
        LABEL = '${label}'
    """

    log.info("Result size: ${res.size()}")
    assertTrue(res.size() > 0, "Job should appear in load_jobs system table after ${label} insert")
}

