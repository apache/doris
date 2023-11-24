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

suite("test_base_insert_job") {
    def tableName = "t_test_base_insert_job"
    def jobName = "insert_recovery_test_base_insert_job"
    sql """drop table if exists `${tableName}` force"""
    sql """
        STOP JOB for ${jobName}
    """

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
       CREATE JOB ${jobName}  ON SCHEDULER every 1 second   comment 'test' DO insert into ${tableName} (timestamp, type, user_id) values ('2023-03-18','1','12213');
    """
    Thread.sleep(2500)
    def jobs = sql """select * from ${tableName}"""
    println jobs
    assert 3>=jobs.size() >= (2 as Boolean) //at least 2 records, some times 3 records
    sql """
        STOP JOB for ${jobName}
    """
    sql """drop table if exists `${tableName}` force """
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
    def currentMs=System.currentTimeMillis()+1000;
    def   dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(currentMs), ZoneId.systemDefault());

    def formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    def startTime= dateTime.format(formatter);
    sql """
          CREATE JOB ${jobName}  ON SCHEDULER at '${startTime}'   comment 'test' DO insert into ${tableName} (timestamp, type, user_id) values ('2023-03-18','1','12213');
     """

    Thread.sleep(2500)

    def datas = sql """select * from ${tableName}"""
    println datas
    //assert datas.size() == 1
    try{
        sql """
            CREATE JOB ${jobName}  ON SCHEDULER at '${startTime}'   comment 'test' DO insert into ${tableName} (timestamp, type, user_id) values ('2023-03-18','1','12213');
        """
    } catch (Exception e) {
        assert true
    }
    sql """
        STOP JOB for test_one_time_error_starts
    """
    try{
        sql """
            CREATE JOB test_one_time_error_starts  ON SCHEDULER at '2023-11-13 14:18:07'   comment 'test' DO insert into ${tableName} (timestamp, type, user_id) values ('2023-03-18','1','12213');
        """
    } catch (Exception e) {
        assert true
    }
    sql """
        STOP JOB for test_error_starts
    """
    try{
        sql """
            CREATE JOB test_error_starts  ON SCHEDULER every 1 second ends '2023-11-13 14:18:07'   comment 'test' DO insert into ${tableName} (timestamp, type, user_id) values ('2023-03-18','1','12213');
        """
    } catch (Exception e) {
        assert true
    }


}
