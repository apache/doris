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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.Instant;
import java.time.ZoneId;

suite("test_batch_insert_job") {
    def tableName = "t_test_batch_inSert_job"
    def insertTargertTableName = "t_test_batch_insert_target_table"
    sql """drop table if exists `${tableName}` force"""
    sql """drop table if exists `${insertTargertTableName}` force"""
    def jobsResult = sql """
      SELECT * from jobs("type"="BATCH_INSERT") where splitColumn="regression_test_job_p0.${tableName}.user_id"
    """
    if (jobsResult.size() > 0) {
        for (def job in jobsResult) {
            sql """
                DROP JOB where jobName="${job[1]}"
            """
        }
    }
    sql """
        CREATE TABLE IF NOT EXISTS `${tableName}`
        (
            `timestamp` DATE NOT NULL COMMENT "['0000-01-01', '9999-12-31']",
            `type` TINYINT NOT NULL COMMENT "[-128, 127]",
            `user_id` BIGINT COMMENT "[-9223372036854775808, 9223372036854775807]"
        )
            DUPLICATE KEY(`timestamp`, `type`)
        DISTRIBUTED BY HASH(`type`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql """
        CREATE TABLE IF NOT EXISTS `${insertTargertTableName}`
        (
            `timestamp` DATE NOT NULL COMMENT "['0000-01-01', '9999-12-31']",
            `type` TINYINT NOT NULL COMMENT "[-128, 127]",
            `user_id` BIGINT COMMENT "[-9223372036854775808, 9223372036854775807]"
        )
            DUPLICATE KEY(`timestamp`, `type`)
        DISTRIBUTED BY HASH(`type`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql """ insert into ${tableName} values ('2022-03-18', 1, 1),
            ('2022-03-18', 1, 2),
            ('2022-03-18', 1, 3),
            ('2022-03-18', 1, 4),
            ('2022-03-18', 1, 5),
            ('2022-03-18', 1, 6),
            ('2022-03-18', 1, 7),
            ('2022-03-18', 1, 8),
            ('2022-03-18', 1, 9),
            ('2022-03-18', 1, 10);
    """
    sql """
       SET enable_nereids_planner=true;
    """
    sql """
       BATCH ON COLUMN ${tableName}.user_id starts 1 ends 10 limit 3 using insert into ${insertTargertTableName} select timestamp,
               type, user_id from ${tableName}
    """
    def batchJobResult = sql """
      SELECT * from jobs("type"="BATCH_INSERT") where splitColumn="regression_test_job_p0.${tableName}.user_id"
    """
    assert batchJobResult.size() == 1
    def jobName = batchJobResult[0][1];
    
    Thread.sleep(2000);
    def jobExecuteResult = sql """
      SELECT status from jobs("type"="BATCH_INSERT") where name="${jobName}"
    """
    assert jobExecuteResult[0][0] == "FINISHED"
    def executeResult = sql """
      SELECT status,splitRange from tasks("type"="BATCH_INSERT") where jobName='${jobName}'
    """
    println executeResult
    def expectedSplitRanges = new HashSet<String>(Arrays.asList("[1..3]", "[4..6]", "[7..10]"))
    assert executeResult.size() == 3
    for (def result in executeResult) {
        assert result[0] == "SUCCESS"
        assert expectedSplitRanges.contains(result[1])
        expectedSplitRanges.remove(result[1])
    }
    assert expectedSplitRanges.size() == 0
    //clear job
    sql """drop job where jobName='${jobName}'"""

}
